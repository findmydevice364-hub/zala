
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phoenix Hub "Baryshnikov" — Shirt.uz Collector

FULL BUILD

DOWNLOAD : 40 workers
PARSING  : 58 workers
VALIDATE : 58 workers

VALIDATION:
    HEAD
    GET
    FFPROBE

FFPROBE:
    Mandatory
    Automatic download
    Automatic extraction
    Binary verification

LOGGING:
    SCALA / DREG — general
    TITAN / DREG — extended

OUTPUT:
    Main/donors_uz.m3u
    Main/stable_uz.m3u
    Shirt_uz/donors_uz.m3u
    Shirt_uz/stable_uz.m3u
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import threading
import time
import uuid
import zipfile

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests


# ============================================================
# CONFIGURATION
# ============================================================

INDEX_URL = "https://shirtv.uz/playlists.html"

SCRIPT_DIR = Path(__file__).resolve().parent

if SCRIPT_DIR.name.lower() == "main":
    ROOT_DIR = SCRIPT_DIR.parent
    MAIN_DIR = SCRIPT_DIR
else:
    ROOT_DIR = SCRIPT_DIR
    MAIN_DIR = ROOT_DIR / "Main"

SHIRT_DIR = ROOT_DIR / "Shirt_uz"

MAIN_LOG_DIR = MAIN_DIR / "logs"
SHIRT_LOG_DIR = SHIRT_DIR / "logs"

SCALA_LOG = MAIN_LOG_DIR / "SCALA_DREG.log"
TITAN_LOG = MAIN_LOG_DIR / "TITAN_DREG.log"

MAIN_DONORS = MAIN_DIR / "donors_uz.m3u"
MAIN_STABLE = MAIN_DIR / "stable_uz.m3u"

SHIRT_DONORS = SHIRT_DIR / "donors_uz.m3u"
SHIRT_STABLE = SHIRT_DIR / "stable_uz.m3u"

TOOLS_DIR = ROOT_DIR / ".tools" / "ffmpeg"
FFPROBE_DIR = TOOLS_DIR

FFPROBE_ARCHIVE = TOOLS_DIR / "ffprobe_download"

# Multithreading

MAX_DOWNLOAD_WORKERS = 40
MAX_PARSE_WORKERS = 58
MAX_VALIDATION_WORKERS = 58

# HTTP

REQUEST_TIMEOUT = 25
HEAD_TIMEOUT = 12
GET_TIMEOUT = 15

MAX_RETRIES = 3
RETRY_DELAY = 1.5

GET_RANGE = "bytes=0-4095"

# FFprobe

ENABLE_HEAD_CHECK = True
ENABLE_GET_CHECK = True
ENABLE_FFPROBE = True

REQUIRE_FFPROBE = True
AUTO_DOWNLOAD_FFPROBE = True

FFPROBE_TIMEOUT = 30
FFPROBE_RW_TIMEOUT = 15000000

FFPROBE_ANALYZE_DURATION = 5000000
FFPROBE_PROBE_SIZE = 5000000

# Validation policy

KEEP_INCONCLUSIVE = True

# Adult filter

ADULT_MARKERS = [
    "18+",
    "xxx",
    "porn",
    "adult",
    "эрот",
    "для взрослых",
]

# GitHub release source for FFmpeg builds

FFPROBE_RELEASE_API = (
    "https://api.github.com/repos/"
    "BtbN/FFmpeg-Builds/releases/latest"
)

# ============================================================
# RUNTIME
# ============================================================

RUN_ID = uuid.uuid4().hex[:12].upper()

STOP_EVENT = threading.Event()

HTTP_LOCAL = threading.local()

LOG_LOCK = threading.RLock()
COUNTER_LOCK = threading.Lock()
SOURCE_ERRORS_LOCK = threading.Lock()

DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(
    MAX_DOWNLOAD_WORKERS
)

VALIDATION_SEMAPHORE = threading.BoundedSemaphore(
    MAX_VALIDATION_WORKERS
)

FFPROBE_SEMAPHORE = threading.BoundedSemaphore(
    MAX_VALIDATION_WORKERS
)

SOURCE_ERRORS = []

FFPROBE_BINARY = None

START_TIME = time.time()

COUNTERS = {
    "sources_discovered": 0,
    "sources_downloaded": 0,
    "source_errors": 0,

    "donor_entries": 0,
    "adult_filtered": 0,
    "duplicates_removed": 0,

    "checked": 0,
    "accepted": 0,
    "rejected": 0,
    "inconclusive": 0,

    "head_ok": 0,
    "head_failed": 0,
    "head_inconclusive": 0,

    "get_ok": 0,
    "get_failed": 0,
    "get_inconclusive": 0,

    "ffprobe_ok": 0,
    "ffprobe_failed": 0,
    "ffprobe_inconclusive": 0,

    "download_retries": 0,
    "parse_errors": 0,
    "validation_errors": 0,
}


# ============================================================
# COUNTERS
# ============================================================

def counter_add(name, value=1):

    with COUNTER_LOCK:
        COUNTERS[name] = COUNTERS.get(name, 0) + value


def counter_snapshot():

    with COUNTER_LOCK:
        return dict(COUNTERS)


# ============================================================
# DUAL LOGGING
# ============================================================

def initialize_logs():

    MAIN_LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    SHIRT_LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    header = (
        "\n"
        + "=" * 90
        + "\n"
        + 'Phoenix Hub "Baryshnikov" — Shirt.uz Collector'
        + "\n"
        + f"RUN ID: {RUN_ID}"
        + "\n"
        + f"START UTC: {datetime.now(timezone.utc).isoformat()}"
        + "\n"
        + "=" * 90
        + "\n"
    )

    with LOG_LOCK:

        with SCALA_LOG.open("a", encoding="utf-8") as f:
            f.write(header)

        with TITAN_LOG.open("a", encoding="utf-8") as f:
            f.write(header)


def log(message, level="INFO", detail=True):

    timestamp = datetime.now().astimezone().isoformat(
        timespec="seconds"
    )

    line = (
        f"[{timestamp}]"
        f"[{level}]"
        f"[{RUN_ID}] "
        f"{message}"
    )

    with LOG_LOCK:

        # Общий журнал: только основные события.
        if not detail or level in (
            "WARNING",
            "ERROR",
            "FATAL",
        ):

            with SCALA_LOG.open(
                "a",
                encoding="utf-8"
            ) as f:

                f.write(line + "\n")

        # Расширенный журнал: все события.
        with TITAN_LOG.open(
            "a",
            encoding="utf-8"
        ) as f:

            f.write(line + "\n")

    print(line, flush=True)


def scala(message, level="INFO"):

    log(
        message,
        level=level,
        detail=False
    )


def titan(message, level="INFO"):

    log(
        message,
        level=level,
        detail=True
    )


# ============================================================
# HTTP SESSION
# ============================================================

def get_session():

    session = getattr(
        HTTP_LOCAL,
        "session",
        None
    )

    if session is None:

        session = requests.Session()

        session.headers.update({

            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "Chrome/122.0.0.0 Safari/537.36"
            ),

            "Accept": "*/*",

            "Accept-Language": (
                "ru-RU,ru;q=0.9,en;q=0.8"
            ),

        })

        HTTP_LOCAL.session = session

    return session


# ============================================================
# HTTP FETCH
# ============================================================

def fetch(
    url,
    is_html=False,
    referer=None
):

    headers = {}

    if is_html:

        headers["Accept"] = (
            "text/html,"
            "application/xhtml+xml,"
            "application/xml;q=0.9,"
            "*/*;q=0.8"
        )

    else:

        headers["Accept"] = "*/*"

    if referer:
        headers["Referer"] = referer

    last_error = None

    for attempt in range(MAX_RETRIES + 1):

        if STOP_EVENT.is_set():
            raise RuntimeError("Interrupted")

        DOWNLOAD_SEMAPHORE.acquire()

        try:

            titan(
                f"DOWNLOAD START url={url} attempt={attempt + 1}"
            )

            response = get_session().get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True
            )

            response.raise_for_status()

            data = response.content

            titan(
                f"DOWNLOAD OK url={url} bytes={len(data)} "
                f"status={response.status_code}"
            )

            return data

        except Exception as error:

            last_error = error

            if attempt < MAX_RETRIES:

                counter_add("download_retries")

                titan(
                    f"DOWNLOAD RETRY url={url} "
                    f"attempt={attempt + 1} error={error}",
                    level="WARNING"
                )

                time.sleep(
                    RETRY_DELAY * (attempt + 1)
                )

        finally:

            DOWNLOAD_SEMAPHORE.release()

    raise RuntimeError(
        f"Download failed: {url}: {last_error}"
    )


# ============================================================
# FFPROBE PLATFORM DETECTION
# ============================================================

def get_platform_asset_pattern():

    system = platform.system().lower()

    machine = platform.machine().lower()

    if machine in ("x86_64", "amd64"):

        arch = "x64"

    elif machine in ("aarch64", "arm64"):

        arch = "arm64"

    else:

        raise RuntimeError(
            f"Unsupported architecture: {machine}"
        )

    if system == "windows":

        if arch == "x64":
            return "win64", ".zip", "ffprobe.exe"

        if arch == "arm64":
            return "winarm64", ".zip", "ffprobe.exe"

    elif system == "linux":

        if arch == "x64":
            return "linux64", ".tar.xz", "ffprobe"

        if arch == "arm64":
            return "linuxarm64", ".tar.xz", "ffprobe"

    raise RuntimeError(
        f"No configured FFprobe build for {system}/{arch}"
    )


# ============================================================
# FFPROBE ARCHIVE DOWNLOAD
# ============================================================

def download_ffprobe_archive():

    platform_name, extension, executable = (
        get_platform_asset_pattern()
    )

    scala(
        f"FFPROBE: поиск сборки {platform_name}"
    )

    titan(
        f"FFPROBE RELEASE API: {FFPROBE_RELEASE_API}"
    )

    response = get_session().get(
        FFPROBE_RELEASE_API,
        timeout=REQUEST_TIMEOUT,
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )

    response.raise_for_status()

    release = response.json()

    assets = release.get("assets", [])

    matches = []

    for asset in assets:

        name = asset.get("name", "").lower()

        if platform_name not in name:
            continue

        if extension == ".zip" and not name.endswith(".zip"):
            continue

        if extension == ".tar.xz" and not name.endswith(".tar.xz"):
            continue

        if "gpl" not in name:
            continue

        matches.append(asset)

    if not matches:

        raise RuntimeError(
            f"FFprobe asset not found for {platform_name}"
        )

    # Выбираем актуальную сборку нужной платформы.
    asset = max(
        matches,
        key=lambda item: item.get("size", 0)
    )

    download_url = asset.get("browser_download_url")

    if not download_url:
        raise RuntimeError("FFprobe download URL missing")

    FFPROBE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    archive_path = (
        FFPROBE_ARCHIVE.with_suffix(extension)
    )

    titan(
        f"FFPROBE DOWNLOAD: {asset['name']}"
    )

    with get_session().get(
        download_url,
        stream=True,
        timeout=REQUEST_TIMEOUT
    ) as response:

        response.raise_for_status()

        with archive_path.open("wb") as file:

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):

                if STOP_EVENT.is_set():
                    raise RuntimeError(
                        "Interrupted during FFprobe download"
                    )

                if chunk:
                    file.write(chunk)

    titan(
        f"FFPROBE ARCHIVE SAVED: {archive_path}"
    )

    return archive_path, executable


# ============================================================
# SAFE FFPROBE EXTRACTION
# ============================================================

def extract_ffprobe(archive_path, executable):

    destination = FFPROBE_DIR / executable

    extracted = False

    if zipfile.is_zipfile(archive_path):

        with zipfile.ZipFile(archive_path) as archive:

            for member in archive.infolist():

                if Path(member.filename).name.lower() != executable.lower():
                    continue

                if member.is_dir():
                    continue

                with archive.open(member) as source:

                    with destination.open("wb") as target:

                        shutil.copyfileobj(
                            source,
                            target
                        )

                extracted = True
                break

    else:

        with tarfile.open(
            archive_path,
            mode="r:*"
        ) as archive:

            for member in archive.getmembers():

                if Path(member.name).name.lower() != executable.lower():
                    continue

                if not member.isfile():
                    continue

                source = archive.extractfile(member)

                if source is None:
                    continue

                with source:

                    with destination.open("wb") as target:

                        shutil.copyfileobj(
                            source,
                            target
                        )

                extracted = True
                break

    if not extracted:

        raise RuntimeError(
            "FFprobe executable not found inside archive"
        )

    if platform.system().lower() != "windows":

        destination.chmod(0o755)

    try:
        archive_path.unlink()
    except OSError:
        pass

    return destination


# ============================================================
# FFPROBE VERIFICATION
# ============================================================

def verify_ffprobe(binary):

    result = subprocess.run(
        [
            str(binary),
            "-version"
        ],

        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,

        text=True,
        encoding="utf-8",
        errors="replace",

        timeout=15,
        shell=False
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"FFprobe verification failed: {result.stderr[:500]}"
        )

    first_line = (
        result.stdout.splitlines()[0]
        if result.stdout
        else "FFprobe version unknown"
    )

    scala(
        f"FFPROBE READY: {first_line}"
    )

    titan(
        f"FFPROBE VERIFIED: {binary} | {first_line}"
    )

    return True


# ============================================================
# FFPROBE INITIALIZATION
# ============================================================

def initialize_ffprobe():

    global FFPROBE_BINARY

    if not ENABLE_FFPROBE:
        raise RuntimeError(
            "FFPROBE IS REQUIRED BUT DISABLED"
        )

    executable = (
        "ffprobe.exe"
        if platform.system().lower() == "windows"
        else "ffprobe"
    )

    local_binary = FFPROBE_DIR / executable

    system_binary = shutil.which("ffprobe")

    # Приоритет локальной сборке.
    if local_binary.is_file():

        try:

            verify_ffprobe(local_binary)

            FFPROBE_BINARY = str(local_binary)

            return

        except Exception as error:

            titan(
                f"LOCAL FFPROBE INVALID: {error}",
                level="WARNING"
            )

            try:
                local_binary.unlink()
            except OSError:
                pass

    # Системный бинарник.
    if system_binary:

        try:

            verify_ffprobe(system_binary)

            FFPROBE_BINARY = system_binary

            return

        except Exception as error:

            titan(
                f"SYSTEM FFPROBE INVALID: {error}",
                level="WARNING"
            )

    if not AUTO_DOWNLOAD_FFPROBE:

        raise RuntimeError(
            "FFPROBE MISSING AND AUTO DOWNLOAD DISABLED"
        )

    # Автоматическая загрузка.
    scala(
        "FFPROBE отсутствует — начинаю автоматическую загрузку",
        level="WARNING"
    )

    archive, executable = download_ffprobe_archive()

    binary = extract_ffprobe(
        archive,
        executable
    )

    verify_ffprobe(binary)

    FFPROBE_BINARY = str(binary)

    scala(
        "FFPROBE успешно установлен"
    )


# ============================================================
# PLAYLIST DISCOVERY
# ============================================================

def discover_playlist_urls():

    scala(
        "Загрузка каталога Shirt.uz"
    )

    html_bytes = fetch(
        INDEX_URL,
        is_html=True
    )

    html = html_bytes.decode(
        "utf-8",
        errors="replace"
    )

    patterns = [

        r'href=["\'](playlists/[^"\']+\.m3u8?)["\']',

        r'data-url=["\'](https?://shirtv\.uz/playlists/[^"\']+\.m3u8?)["\']',

        r'(https?://shirtv\.uz/playlists/[^"\'\s<>]+\.m3u8?)',

        r'(playlists/[^"\'\s<>]+\.m3u8?)',

    ]

    candidates = []

    for pattern in patterns:

        candidates.extend(
            re.findall(
                pattern,
                html,
                flags=re.IGNORECASE
            )
        )

    result = []
    seen = set()

    for href in candidates:

        href = href.strip().strip("'\"")

        if not href:
            continue

        absolute = urljoin(
            INDEX_URL,
            href
        )

        if absolute.startswith("http://"):

            absolute = (
                "https://" + absolute[7:]
            )

        key = absolute.lower().split("?")[0]

        if key in seen:
            continue

        if not key.endswith((".m3u", ".m3u8")):
            continue

        seen.add(key)

        result.append(absolute)

    counter_add(
        "sources_discovered",
        len(result)
    )

    scala(
        f"Найдено источников: {len(result)}"
    )

    for index, url in enumerate(result, 1):

        titan(
            f"SOURCE DISCOVERED [{index}/{len(result)}] {url}"
        )

    if not result:

        MAIN_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        (MAIN_DIR / "debug_page.html").write_text(
            html,
            encoding="utf-8"
        )

        raise RuntimeError(
            "No M3U/M3U8 playlists discovered"
        )

    return result


# ============================================================
# PARALLEL DOWNLOAD STAGE
# ============================================================

def download_one_source(url):

    try:

        data = fetch(
            url,
            referer=INDEX_URL
        )

        counter_add("sources_downloaded")

        return {
            "url": url,
            "content": data,
            "error": None,
        }

    except Exception as error:

        counter_add("source_errors")

        with SOURCE_ERRORS_LOCK:

            SOURCE_ERRORS.append({
                "url": url,
                "error": str(error),
            })

        log(
            f"SOURCE DOWNLOAD FAILED: {url} | {error}",
            level="ERROR"
        )

        return {
            "url": url,
            "content": None,
            "error": str(error),
        }


def download_all_sources(sources):

    scala(
        f"DOWNLOAD STAGE: {len(sources)} sources / {MAX_DOWNLOAD_WORKERS} workers"
    )

    downloaded = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_DOWNLOAD_WORKERS,
        thread_name_prefix="SHIRT-DOWNLOAD"
    ) as pool:

        futures = [
            pool.submit(
                download_one_source,
                url
            )
            for url in sources
        ]

        for future in concurrent.futures.as_completed(futures):

            if STOP_EVENT.is_set():
                break

            try:

                result = future.result()

                if result["content"] is not None:
                    downloaded.append(result)

            except Exception as error:

                log(
                    f"DOWNLOAD WORKER ERROR: {error}",
                    level="ERROR"
                )

    scala(
        f"DOWNLOAD COMPLETE: {len(downloaded)}/{len(sources)}"
    )

    return downloaded


# ============================================================
# M3U PARSING
# ============================================================

def parse_m3u(content, source):

    entries = []

    current_extinf = None

    for raw_line in content.splitlines():

        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF:"):

            current_extinf = line
            continue

        if line.startswith("#"):
            continue

        url = urljoin(
            source,
            line
        )

        if not url.startswith(
            ("http://", "https://")
        ):
            continue

        name = "Без названия"

        if current_extinf and "," in current_extinf:

            name = (
                current_extinf.split(",", 1)[1].strip()
                or name
            )

        entries.append({

            "extinf": (
                current_extinf
                or f"#EXTINF:-1,{name}"
            ),

            "url": url,

            "source": source,

        })

        current_extinf = None

    return entries


def parse_one_source(item):

    source = item["url"]

    try:

        content = item["content"].decode(
            "utf-8-sig",
            errors="replace"
        )

        entries = parse_m3u(
            content,
            source
        )

        clean = []

        for entry in entries:

            if is_adult(entry):

                counter_add("adult_filtered")

                titan(
                    f"ADULT FILTER: {entry['url']}"
                )

                continue

            clean.append(entry)

        counter_add(
            "donor_entries",
            len(clean)
        )

        titan(
            f"PARSED: {source} | entries={len(clean)}"
        )

        return clean

    except Exception as error:

        counter_add("parse_errors")

        log(
            f"PARSE ERROR: {source} | {error}",
            level="ERROR"
        )

        return []


def is_adult(entry):

    value = (
        entry.get("extinf", "")
        + " "
        + entry.get("url", "")
    ).lower()

    return any(
        marker in value
        for marker in ADULT_MARKERS
    )


# ============================================================
# PARALLEL PARSING STAGE
# ============================================================

def parse_all_sources(downloaded):

    scala(
        f"PARSING STAGE: {len(downloaded)} sources / {MAX_PARSE_WORKERS} workers"
    )

    all_entries = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_PARSE_WORKERS,
        thread_name_prefix="SHIRT-PARSE"
    ) as pool:

        futures = [
            pool.submit(
                parse_one_source,
                item
            )
            for item in downloaded
        ]

        for future in concurrent.futures.as_completed(futures):

            if STOP_EVENT.is_set():
                break

            try:

                all_entries.extend(
                    future.result()
                )

            except Exception as error:

                counter_add("parse_errors")

                log(
                    f"PARSER WORKER ERROR: {error}",
                    level="ERROR"
                )

    scala(
        f"PARSING COMPLETE: {len(all_entries)} entries"
    )

    return all_entries


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_entries(entries):

    result = []

    seen = set()

    for entry in entries:

        key = entry["url"].split("#")[0]

        if key in seen:
            continue

        seen.add(key)

        result.append(entry)

    removed = len(entries) - len(result)

    counter_add(
        "duplicates_removed",
        removed
    )

    scala(
        f"DEDUPLICATION: removed={removed}, remaining={len(result)}"
    )

    return result


# ============================================================
# HEAD VALIDATION
# ============================================================

def check_head(url):

    if not ENABLE_HEAD_CHECK:
        return None

    if STOP_EVENT.is_set():
        return None

    VALIDATION_SEMAPHORE.acquire()

    try:

        started = time.monotonic()

        response = get_session().head(
            url,
            timeout=HEAD_TIMEOUT,
            allow_redirects=True
        )

        status = response.status_code

        elapsed = time.monotonic() - started

        if 200 <= status < 400:

            counter_add("head_ok")

            titan(
                f"HEAD OK | {status} | {elapsed:.2f}s | {url}"
            )

            return True

        if status in (404, 410):

            counter_add("head_failed")

            titan(
                f"HEAD DEAD | {status} | {url}",
                level="WARNING"
            )

            return False

        counter_add("head_inconclusive")

        titan(
            f"HEAD INCONCLUSIVE | {status} | {url}"
        )

        return None

    except Exception as error:

        counter_add("head_inconclusive")

        titan(
            f"HEAD ERROR | {url} | {error}"
        )

        return None

    finally:

        VALIDATION_SEMAPHORE.release()


# ============================================================
# GET VALIDATION
# ============================================================

def check_get(url):

    if not ENABLE_GET_CHECK:
        return None

    if STOP_EVENT.is_set():
        return None

    VALIDATION_SEMAPHORE.acquire()

    response = None

    try:

        started = time.monotonic()

        response = get_session().get(
            url,

            headers={
                "Range": GET_RANGE,
                "Accept": "*/*",
                "Referer": INDEX_URL,
            },

            timeout=GET_TIMEOUT,
            allow_redirects=True,
            stream=True
        )

        status = response.status_code

        if status in (404, 410):

            counter_add("get_failed")

            titan(
                f"GET DEAD | {status} | {url}",
                level="WARNING"
            )

            return False

        if not (200 <= status < 400):

            counter_add("get_inconclusive")

            titan(
                f"GET INCONCLUSIVE | {status} | {url}"
            )

            return None

        chunk = next(
            response.iter_content(
                chunk_size=4096
            ),
            b""
        )

        elapsed = time.monotonic() - started

        if chunk:

            counter_add("get_ok")

            titan(
                f"GET OK | {status} | bytes={len(chunk)} "
                f"| {elapsed:.2f}s | {url}"
            )

            return True

        counter_add("get_inconclusive")

        titan(
            f"GET EMPTY | {url}"
        )

        return None

    except Exception as error:

        counter_add("get_inconclusive")

        titan(
            f"GET ERROR | {url} | {error}"
        )

        return None

    finally:

        if response is not None:
            response.close()

        VALIDATION_SEMAPHORE.release()


# ============================================================
# FFPROBE STREAM VALIDATION
# ============================================================

def check_ffprobe(url):

    if not FFPROBE_BINARY:

        raise RuntimeError(
            "FFPROBE WAS NOT INITIALIZED"
        )

    if STOP_EVENT.is_set():
        return None

    FFPROBE_SEMAPHORE.acquire()

    started = time.monotonic()

    try:

        command = [

            FFPROBE_BINARY,

            "-hide_banner",
            "-v", "error",

            "-rw_timeout",
            str(FFPROBE_RW_TIMEOUT),

            "-analyzeduration",
            str(FFPROBE_ANALYZE_DURATION),

            "-probesize",
            str(FFPROBE_PROBE_SIZE),

            "-user_agent",
            "Mozilla/5.0",

            "-headers",
            f"Referer: {INDEX_URL}\r\n",

            "-i",
            url,

            "-show_entries",
            "stream=codec_type",

            "-of",
            "json",

        ]

        result = subprocess.run(

            command,

            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,

            text=True,
            encoding="utf-8",
            errors="replace",

            timeout=FFPROBE_TIMEOUT,

            shell=False

        )

        elapsed = time.monotonic() - started

        if result.returncode != 0:

            counter_add(
                "ffprobe_inconclusive"
            )

            titan(
                f"FFPROBE ERROR | {elapsed:.2f}s "
                f"| {url} | {result.stderr[:400]}"
            )

            return None

        data = json.loads(
            result.stdout
        )

        streams = data.get(
            "streams",
            []
        )

        media_types = [
            stream.get("codec_type")
            for stream in streams
        ]

        if any(
            media_type in ("video", "audio")
            for media_type in media_types
        ):

            counter_add("ffprobe_ok")

            titan(
                f"FFPROBE OK | streams={media_types} "
                f"| {elapsed:.2f}s | {url}"
            )

            return True

        counter_add("ffprobe_failed")

        titan(
            f"FFPROBE NO MEDIA STREAMS | {url}",
            level="WARNING"
        )

        return False

    except subprocess.TimeoutExpired:

        counter_add("ffprobe_inconclusive")

        titan(
            f"FFPROBE TIMEOUT | {url}",
            level="WARNING"
        )

        return None

    except Exception as error:

        counter_add("ffprobe_inconclusive")

        titan(
            f"FFPROBE EXCEPTION | {url} | {error}",
            level="WARNING"
        )

        return None

    finally:

        FFPROBE_SEMAPHORE.release()


# ============================================================
# COMBINED VALIDATION
# ============================================================

def check_stream(url):

    """
    True  = confirmed available media stream
    False = confirmed unavailable resource
    None  = inconclusive
    """

    if STOP_EVENT.is_set():
        return None

    head_result = check_head(url)

    if STOP_EVENT.is_set():
        return None

    get_result = check_get(url)

    if STOP_EVENT.is_set():
        return None

    ffprobe_result = check_ffprobe(url)

    # FFprobe has confirmed an audio/video stream.
    if ffprobe_result is True:
        return True

    # GET has confirmed that content is returned,
    # but FFprobe could not identify media.
    if ffprobe_result is False:

        if get_result is True:
            return None

        if head_result is True:
            return None

        if get_result is False and head_result is False:
            return False

        return None

    # FFprobe was inconclusive.
    if get_result is True:
        return True

    # Two independent HTTP methods confirm 404/410.
    if (
        head_result is False
        and get_result is False
    ):
        return False

    return None


# ============================================================
# VALIDATION WORKER
# ============================================================

def validation_worker(entry):

    started = time.monotonic()

    try:

        result = check_stream(
            entry["url"]
        )

        elapsed = time.monotonic() - started

        titan(
            f"CHANNEL RESULT | {result} "
            f"| {elapsed:.2f}s | {entry['url']}"
        )

        return entry, result

    except Exception as error:

        counter_add("validation_errors")

        titan(
            f"VALIDATION EXCEPTION | {entry['url']} | {error}",
            level="ERROR"
        )

        return entry, None


# ============================================================
# PARALLEL VALIDATION STAGE
# ============================================================

def validate_entries(entries):

    stable = []

    total = len(entries)

    scala(
        f"VALIDATION STAGE: {total} channels / {MAX_VALIDATION_WORKERS} workers"
    )

    completed = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_VALIDATION_WORKERS,
        thread_name_prefix="SHIRT-VALIDATE"
    ) as pool:

        futures = [

            pool.submit(
                validation_worker,
                entry
            )

            for entry in entries

        ]

        for future in concurrent.futures.as_completed(futures):

            if STOP_EVENT.is_set():
                break

            try:

                entry, result = future.result()

                completed += 1

                counter_add("checked")

                if result is True:

                    stable.append(entry)

                    counter_add("accepted")

                elif result is False:

                    counter_add("rejected")

                else:

                    counter_add("inconclusive")

                    if KEEP_INCONCLUSIVE:

                        stable.append(entry)

                        counter_add("accepted")

                if (
                    completed % 25 == 0
                    or completed == total
                ):

                    counts = counter_snapshot()

                    scala(
                        f"VALIDATION PROGRESS "
                        f"{completed}/{total} "
                        f"accepted={counts['accepted']} "
                        f"rejected={counts['rejected']} "
                        f"inconclusive={counts['inconclusive']}"
                    )

            except Exception as error:

                counter_add("validation_errors")

                log(
                    f"VALIDATION WORKER ERROR: {error}",
                    level="ERROR"
                )

    scala(
        f"VALIDATION COMPLETE: {len(stable)}/{total}"
    )

    return stable


# ============================================================
# ATOMIC M3U WRITER
# ============================================================

def write_m3u(path, entries):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    tmp = path.with_name(
        path.name + f".{RUN_ID}.tmp"
    )

    with tmp.open(
        "w",
        encoding="utf-8",
        newline="\n"
    ) as file:

        file.write("#EXTM3U\n")

        for entry in entries:

            file.write(
                entry["extinf"].rstrip() + "\n"
            )

            file.write(
                entry["url"].rstrip() + "\n"
            )

        file.flush()
        os.fsync(file.fileno())

    tmp.replace(path)

    scala(
        f"PLAYLIST WRITTEN: {path} | {len(entries)} entries"
    )

    titan(
        f"WRITE COMPLETE: {path} | entries={len(entries)}"
    )


# ============================================================
# ERROR REPORT
# ============================================================

def write_error_report():

    if not SOURCE_ERRORS:
        return

    report_path = (
        MAIN_LOG_DIR / f"source_errors_{RUN_ID}.json"
    )

    with SOURCE_ERRORS_LOCK:

        data = list(SOURCE_ERRORS)

    report_path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8"
    )

    titan(
        f"SOURCE ERROR REPORT: {report_path}"
    )


# ============================================================
# FINAL REPORT
# ============================================================

def print_report(elapsed, stable_count):

    counts = counter_snapshot()

    report = [

        "=" * 70,

        'PHOENIX HUB "BARYSHNIKOV" — SHIRT.UZ COLLECTOR',

        f"RUN ID: {RUN_ID}",

        "=" * 70,

        f"Sources discovered       : {counts['sources_discovered']}",
        f"Sources downloaded       : {counts['sources_downloaded']}",
        f"Source errors            : {counts['source_errors']}",

        "-" * 70,

        f"Donor entries            : {counts['donor_entries']}",
        f"Adult filtered           : {counts['adult_filtered']}",
        f"Duplicates removed       : {counts['duplicates_removed']}",

        "-" * 70,

        f"Checked                  : {counts['checked']}",

        f"HEAD OK                  : {counts['head_ok']}",
        f"HEAD FAILED              : {counts['head_failed']}",

        f"GET OK                   : {counts['get_ok']}",
        f"GET FAILED               : {counts['get_failed']}",

        f"FFprobe OK               : {counts['ffprobe_ok']}",
        f"FFprobe FAILED           : {counts['ffprobe_failed']}",
        f"FFprobe INCONCLUSIVE     : {counts['ffprobe_inconclusive']}",

        "-" * 70,

        f"Accepted                 : {counts['accepted']}",
        f"Rejected                 : {counts['rejected']}",
        f"Inconclusive             : {counts['inconclusive']}",

        f"Stable entries            : {stable_count}",

        "-" * 70,

        f"Download retries         : {counts['download_retries']}",
        f"Parse errors             : {counts['parse_errors']}",
        f"Validation errors        : {counts['validation_errors']}",

        "-" * 70,

        f"Download workers         : {MAX_DOWNLOAD_WORKERS}",
        f"Parse workers            : {MAX_PARSE_WORKERS}",
        f"Validation workers       : {MAX_VALIDATION_WORKERS}",

        f"FFprobe                  : {FFPROBE_BINARY}",

        f"Elapsed                  : {elapsed:.2f} seconds",

        "=" * 70,

    ]

    output = "\n".join(report)

    print("\n" + output, flush=True)

    scala(
        f"FINISHED: stable={stable_count}, elapsed={elapsed:.2f}s"
    )

    titan(output)


# ============================================================
# MAIN
# ============================================================

def main():

    global START_TIME

    START_TIME = time.time()

    initialize_logs()

    print("=" * 70)

    print(
        'Phoenix Hub "Baryshnikov" — Shirt.uz Collector'
    )

    print(
        f"RUN ID: {RUN_ID}"
    )

    print("=" * 70)

    scala("COLLECTOR STARTED")

    scala(
        f"THREAD CONFIG: DOWNLOAD={MAX_DOWNLOAD_WORKERS}, "
        f"PARSE={MAX_PARSE_WORKERS}, "
        f"VALIDATION={MAX_VALIDATION_WORKERS}"
    )

    try:

        MAIN_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        SHIRT_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        # ----------------------------------------------------
        # 1. Mandatory FFprobe initialization
        # ----------------------------------------------------

        scala(
            "STAGE 1: FFPROBE INITIALIZATION"
        )

        initialize_ffprobe()

        if REQUIRE_FFPROBE and not FFPROBE_BINARY:

            raise RuntimeError(
                "MANDATORY FFPROBE NOT AVAILABLE"
            )

        # ----------------------------------------------------
        # 2. Discover all playlist sources
        # ----------------------------------------------------

        scala(
            "STAGE 2: PLAYLIST DISCOVERY"
        )

        sources = discover_playlist_urls()

        # ----------------------------------------------------
        # 3. Download with 40 workers
        # ----------------------------------------------------

        scala(
            "STAGE 3: PARALLEL DOWNLOAD"
        )

        downloaded = download_all_sources(
            sources
        )

        if not downloaded:

            raise RuntimeError(
                "No playlist sources downloaded"
            )

        # ----------------------------------------------------
        # 4. Parse with 58 workers
        # ----------------------------------------------------

        scala(
            "STAGE 4: PARALLEL PARSING"
        )

        donors = parse_all_sources(
            downloaded
        )

        # Release downloaded source contents.
        del downloaded

        if not donors:

            raise RuntimeError(
                "No playlist entries collected"
            )

        # ----------------------------------------------------
        # 5. Deduplicate
        # ----------------------------------------------------

        scala(
            "STAGE 5: DEDUPLICATION"
        )

        donors = deduplicate_entries(
            donors
        )

        # ----------------------------------------------------
        # 6. HEAD + GET + FFprobe with 58 workers
        # ----------------------------------------------------

        scala(
            "STAGE 6: STREAM VALIDATION"
        )

        stable = validate_entries(
            donors
        )

        # ----------------------------------------------------
        # 7. Write four output playlists
        # ----------------------------------------------------

        scala(
            "STAGE 7: PLAYLIST OUTPUT"
        )

        write_m3u(
            MAIN_DONORS,
            donors
        )

        write_m3u(
            MAIN_STABLE,
            stable
        )

        write_m3u(
            SHIRT_DONORS,
            donors
        )

        write_m3u(
            SHIRT_STABLE,
            stable
        )

        # ----------------------------------------------------
        # 8. Error report
        # ----------------------------------------------------

        write_error_report()

        # ----------------------------------------------------
        # 9. Final statistics
        # ----------------------------------------------------

        elapsed = time.time() - START_TIME

        print_report(
            elapsed,
            len(stable)
        )

        scala(
            "COLLECTOR FINISHED SUCCESSFULLY"
        )

        return 0

    except KeyboardInterrupt:

        STOP_EVENT.set()

        scala(
            "COLLECTOR INTERRUPTED BY USER",
            level="WARNING"
        )

        return 130

    except Exception as error:

        STOP_EVENT.set()

        log(
            f"FATAL: {error}",
            level="FATAL"
        )

        titan(
            f"FATAL DETAILS: {type(error).__name__}: {error}",
            level="FATAL"
        )

        return 1

    finally:

        elapsed = time.time() - START_TIME

        titan(
            f"PROCESS TERMINATED | elapsed={elapsed:.2f}s"
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    sys.exit(main())
