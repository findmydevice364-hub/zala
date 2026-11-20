#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phoenix Hub "Baryshnikov" — Shirt.uz Collector

FULL MONOLITHIC BUILD

============================================================
WORKER ARCHITECTURE
============================================================

DOWNLOAD : 50 workers
PARSING  : 60 workers

HEAD     : 70 workers
GET      : 70 workers
FFPROBE  : 70 workers

TOTAL CONFIGURED WORKERS: 320

HEAD, GET and FFPROBE are three independent executors.
All three checks for each channel are submitted concurrently.

============================================================
VALIDATION
============================================================

HEAD:
    Independent HTTP HEAD validation

GET:
    Independent HTTP GET Range validation

FFPROBE:
    Mandatory media-stream inspection
    Temporary download for this run only
    Separate temporary run directory
    Process timeout
    Active-process registry
    Cleanup after all validation executors finish

============================================================
LOGGING
============================================================

SCALA / DREG:
    Main operational events

TITAN / DREG:
    Operational events + extended diagnostics

COMBINED:
    Complete SCALA first
    Complete TITAN second

============================================================
DEDUPLICATION
============================================================

PLAYLIST SOURCE DEDUPLICATION: ENABLED EXACTLY ONCE

Executed only:
    After all playlist discovery patterns have completed
    Before playlist downloads begin

Scope:
    Discovered M3U/M3U8 source URLs only

Policy:
    Preserve first occurrence
    Preserve first-occurrence order
    Download each unique playlist source once

DISABLED:
    Channel URL deduplication
    Channel-name deduplication
    M3U record deduplication

All duplicate channel records are preserved.

============================================================
OUTPUT
============================================================

Main/donors_uz.m3u
Main/stable_uz.m3u

Shirt_uz/donors_uz.m3u
Shirt_uz/stable_uz.m3u

UZ_data/donors_uz.m3u
UZ_data/stable_uz.m3u
UZ_data/Shirt_donors_uz.m3u
UZ_data/Shirt_stable_uz.m3u
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import threading
import time
import uuid
import zipfile

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import (
    urljoin,
    urlsplit,
    urlunsplit,
)

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
UZ_DATA_DIR = ROOT_DIR / "UZ_data"

MAIN_LOG_DIR = MAIN_DIR / "logs"
SHIRT_LOG_DIR = SHIRT_DIR / "logs"

SCALA_LOG = MAIN_LOG_DIR / "SCALA_DREG.log"
TITAN_LOG = MAIN_LOG_DIR / "TITAN_DREG.log"
COMBINED_LOG = MAIN_LOG_DIR / "SCALA_DREG_TITAN.log"

MAIN_DONORS = MAIN_DIR / "donors_uz.m3u"
MAIN_STABLE = MAIN_DIR / "stable_uz.m3u"

SHIRT_DONORS = SHIRT_DIR / "donors_uz.m3u"
SHIRT_STABLE = SHIRT_DIR / "stable_uz.m3u"

UZ_DONORS = UZ_DATA_DIR / "donors_uz.m3u"
UZ_STABLE = UZ_DATA_DIR / "stable_uz.m3u"

UZ_SHIRT_DONORS = UZ_DATA_DIR / "Shirt_donors_uz.m3u"
UZ_SHIRT_STABLE = UZ_DATA_DIR / "Shirt_stable_uz.m3u"

TOOLS_DIR = ROOT_DIR / ".tools" / "ffmpeg"

# ------------------------------------------------------------
# WORKER CONFIGURATION
# ------------------------------------------------------------

MAX_DOWNLOAD_WORKERS = 50
MAX_PARSE_WORKERS = 60

MAX_HEAD_WORKERS = 70
MAX_GET_WORKERS = 70
MAX_FFPROBE_WORKERS = 70

MAX_VALIDATION_WORKERS = (
    MAX_HEAD_WORKERS
    + MAX_GET_WORKERS
    + MAX_FFPROBE_WORKERS
)

TOTAL_CONFIGURED_WORKERS = (
    MAX_DOWNLOAD_WORKERS
    + MAX_PARSE_WORKERS
    + MAX_VALIDATION_WORKERS
)

# ------------------------------------------------------------
# HTTP CONFIGURATION
# ------------------------------------------------------------

REQUEST_TIMEOUT = 25
HEAD_TIMEOUT = 12
GET_TIMEOUT = 15

MAX_RETRIES = 3
RETRY_DELAY = 1.5

GET_RANGE = "bytes=0-4095"

HTTP_MAX_CONNECTIONS = 100
HTTP_MAX_KEEPALIVE = 50

# ------------------------------------------------------------
# VALIDATION SWITCHES
# ------------------------------------------------------------

ENABLE_HEAD_CHECK = True
ENABLE_GET_CHECK = True
ENABLE_FFPROBE = True

REQUIRE_FFPROBE = True
AUTO_DOWNLOAD_FFPROBE = True

# ------------------------------------------------------------
# FFPROBE CONFIGURATION
# ------------------------------------------------------------

FFPROBE_TIMEOUT = 30
FFPROBE_STARTUP_TIMEOUT = 15

FFPROBE_RW_TIMEOUT = 15000000
FFPROBE_ANALYZE_DURATION = 5000000
FFPROBE_PROBE_SIZE = 5000000

FFPROBE_RELEASE_API = (
    "https://api.github.com/repos/"
    "BtbN/FFmpeg-Builds/releases/latest"
)

# ------------------------------------------------------------
# VALIDATION POLICY
# ------------------------------------------------------------

KEEP_INCONCLUSIVE = True

# ------------------------------------------------------------
# ADULT FILTER
# ------------------------------------------------------------

ADULT_MARKERS = [
    "18+",
    "xxx",
    "porn",
    "adult",
    "эрот",
    "для взрослых",
]

# ------------------------------------------------------------
# DISCOVERY
# ------------------------------------------------------------

PLAYLIST_EXTENSIONS = (
    ".m3u",
    ".m3u8",
)

# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------

LOG_FLUSH_EVERY_EVENT = True

# ------------------------------------------------------------
# RUNTIME
# ------------------------------------------------------------

RUN_ID = uuid.uuid4().hex[:12].upper()

START_TIME = time.time()

FFPROBE_RUN_DIR = TOOLS_DIR / RUN_ID
FFPROBE_ARCHIVE_DIR = FFPROBE_RUN_DIR / "archive"
FFPROBE_EXTRACT_DIR = FFPROBE_RUN_DIR / "bin"

STOP_EVENT = threading.Event()

HTTP_LOCAL = threading.local()

LOG_LOCK = threading.RLock()
COUNTER_LOCK = threading.Lock()
SOURCE_ERRORS_LOCK = threading.Lock()
PROCESS_LOCK = threading.RLock()

ACTIVE_PROCESSES = set()
SOURCE_ERRORS = []

FFPROBE_BINARY = None

EXIT_CODE = 1


# ============================================================
# COUNTERS
# ============================================================

COUNTERS = {
    "sources_discovered": 0,
    "sources_unique": 0,
    "sources_duplicates_removed": 0,
    "sources_downloaded": 0,
    "source_errors": 0,

    "donor_entries": 0,
    "adult_filtered": 0,

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

    "head_tasks_submitted": 0,
    "get_tasks_submitted": 0,
    "ffprobe_tasks_submitted": 0,

    "head_tasks_completed": 0,
    "get_tasks_completed": 0,
    "ffprobe_tasks_completed": 0,

    "validation_tasks_submitted": 0,
    "validation_tasks_completed": 0,

    "validation_decisions_completed": 0,

    "validation_cancelled": 0,
    "download_cancelled": 0,
    "parse_cancelled": 0,
}


def counter_add(name, value=1):

    with COUNTER_LOCK:
        COUNTERS[name] = COUNTERS.get(name, 0) + value


def counter_set(name, value):

    with COUNTER_LOCK:
        COUNTERS[name] = value


def counter_snapshot():

    with COUNTER_LOCK:
        return dict(COUNTERS)


# ============================================================
# LOGGING
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
        + "=" * 100
        + "\n"
        + 'Phoenix Hub "Baryshnikov" — Shirt.uz Collector'
        + "\n"
        + f"RUN ID: {RUN_ID}"
        + "\n"
        + f"START UTC: {datetime.now(timezone.utc).isoformat()}"
        + "\n"
        + f"ROOT: {ROOT_DIR}"
        + "\n"
        + "-" * 100
        + "\n"
        + f"DOWNLOAD WORKERS: {MAX_DOWNLOAD_WORKERS}"
        + "\n"
        + f"PARSING WORKERS: {MAX_PARSE_WORKERS}"
        + "\n"
        + f"HEAD WORKERS: {MAX_HEAD_WORKERS}"
        + "\n"
        + f"GET WORKERS: {MAX_GET_WORKERS}"
        + "\n"
        + f"FFPROBE WORKERS: {MAX_FFPROBE_WORKERS}"
        + "\n"
        + f"VALIDATION WORKERS: {MAX_VALIDATION_WORKERS}"
        + "\n"
        + f"TOTAL CONFIGURED WORKERS: {TOTAL_CONFIGURED_WORKERS}"
        + "\n"
        + "-" * 100
        + "\n"
        + "PLAYLIST SOURCE DEDUPLICATION: ENABLED ONCE"
        + "\n"
        + "DEDUPLICATION STAGE: DISCOVERY BEFORE DOWNLOAD"
        + "\n"
        + "DEDUPLICATION SCOPE: PLAYLIST SOURCE URLS ONLY"
        + "\n"
        + "PRESERVE FIRST SOURCE OCCURRENCE: YES"
        + "\n"
        + "PRESERVE UNIQUE SOURCE ORDER: YES"
        + "\n"
        + "CHANNEL URL DEDUPLICATION: DISABLED"
        + "\n"
        + "CHANNEL NAME DEDUPLICATION: DISABLED"
        + "\n"
        + "M3U RECORD DEDUPLICATION: DISABLED"
        + "\n"
        + "HEAD + GET + FFPROBE: INDEPENDENT PARALLEL EXECUTORS"
        + "\n"
        + "=" * 100
        + "\n"
    )

    with LOG_LOCK:

        with SCALA_LOG.open(
            "a",
            encoding="utf-8"
        ) as file:
            file.write(header)

        with TITAN_LOG.open(
            "a",
            encoding="utf-8"
        ) as file:
            file.write(header)


def log(
    message,
    level="INFO",
    titan_only=False
):

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

        if not titan_only:

            with SCALA_LOG.open(
                "a",
                encoding="utf-8"
            ) as file:

                file.write(line + "\n")

                if LOG_FLUSH_EVERY_EVENT:
                    file.flush()

        with TITAN_LOG.open(
            "a",
            encoding="utf-8"
        ) as file:

            file.write(line + "\n")

            if LOG_FLUSH_EVERY_EVENT:
                file.flush()

    print(
        line,
        flush=True
    )


def scala(message, level="INFO"):

    log(
        message,
        level=level
    )


def titan(message, level="INFO"):

    log(
        message,
        level=level
    )


def titan_detail(message, level="DEBUG"):

    log(
        message,
        level=level,
        titan_only=True
    )


def write_combined_log():

    MAIN_LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = COMBINED_LOG.with_name(
        COMBINED_LOG.name + f".{RUN_ID}.tmp"
    )

    with LOG_LOCK:

        with temporary.open(
            "w",
            encoding="utf-8"
        ) as output:

            output.write(
                "=" * 100 + "\n"
            )

            output.write(
                "SCALA_DREG — COMPLETE OPERATIONAL LOG\n"
            )

            output.write(
                "=" * 100 + "\n\n"
            )

            if SCALA_LOG.exists():

                with SCALA_LOG.open(
                    "r",
                    encoding="utf-8",
                    errors="replace"
                ) as source:

                    shutil.copyfileobj(
                        source,
                        output
                    )

            output.write("\n\n")

            output.write(
                "=" * 100 + "\n"
            )

            output.write(
                "TITAN_DREG — EXTENDED DIAGNOSTIC LOG\n"
            )

            output.write(
                "=" * 100 + "\n\n"
            )

            if TITAN_LOG.exists():

                with TITAN_LOG.open(
                    "r",
                    encoding="utf-8",
                    errors="replace"
                ) as source:

                    shutil.copyfileobj(
                        source,
                        output
                    )

            output.flush()
            os.fsync(output.fileno())

        temporary.replace(
            COMBINED_LOG
        )

    print(
        f"COMBINED LOG WRITTEN: {COMBINED_LOG}",
        flush=True
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

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=HTTP_MAX_CONNECTIONS,
            pool_maxsize=HTTP_MAX_KEEPALIVE,
            max_retries=0,
        )

        session.mount(
            "http://",
            adapter
        )

        session.mount(
            "https://",
            adapter
        )

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


def close_thread_session():

    session = getattr(
        HTTP_LOCAL,
        "session",
        None
    )

    if session is not None:

        try:
            session.close()
        except Exception:
            pass

        HTTP_LOCAL.session = None


# ============================================================
# INTERRUPTIBLE SLEEP
# ============================================================

def interruptible_sleep(seconds):

    return STOP_EVENT.wait(seconds)


# ============================================================
# HTTP FETCH
# ============================================================

def fetch(
    url,
    is_html=False,
    referer=None
):

    headers = {
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,*/*;q=0.8"
            if is_html
            else "*/*"
        )
    }

    if referer:
        headers["Referer"] = referer

    last_error = None

    for attempt in range(MAX_RETRIES + 1):

        if STOP_EVENT.is_set():
            raise RuntimeError(
                "Interrupted"
            )

        try:

            titan_detail(
                f"DOWNLOAD START | "
                f"attempt={attempt + 1} | {url}"
            )

            started = time.monotonic()

            response = get_session().get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True
            )

            try:

                response.raise_for_status()

                data = response.content
                status = response.status_code

            finally:
                response.close()

            elapsed = time.monotonic() - started

            titan(
                f"DOWNLOAD OK | status={status} "
                f"| bytes={len(data)} "
                f"| elapsed={elapsed:.2f}s "
                f"| {url}"
            )

            return data

        except Exception as error:

            last_error = error

            if attempt < MAX_RETRIES:

                counter_add(
                    "download_retries"
                )

                titan(
                    f"DOWNLOAD RETRY | "
                    f"attempt={attempt + 1} "
                    f"| {url} | {error}",
                    level="WARNING"
                )

                if interruptible_sleep(
                    RETRY_DELAY * (attempt + 1)
                ):
                    raise RuntimeError(
                        "Interrupted during retry delay"
                    )

    raise RuntimeError(
        f"Download failed: {url}: {last_error}"
    )


# ============================================================
# PLATFORM DETECTION
# ============================================================

def get_platform_asset_pattern():

    system = platform.system().lower()
    machine = platform.machine().lower()

    if machine in (
        "x86_64",
        "amd64",
    ):
        arch = "x64"

    elif machine in (
        "aarch64",
        "arm64",
    ):
        arch = "arm64"

    else:

        raise RuntimeError(
            f"Unsupported architecture: {machine}"
        )

    if system == "windows":

        if arch == "x64":
            return (
                "win64",
                ".zip",
                "ffprobe.exe"
            )

        if arch == "arm64":
            return (
                "winarm64",
                ".zip",
                "ffprobe.exe"
            )

    elif system == "linux":

        if arch == "x64":
            return (
                "linux64",
                ".tar.xz",
                "ffprobe"
            )

        if arch == "arm64":
            return (
                "linuxarm64",
                ".tar.xz",
                "ffprobe"
            )

    raise RuntimeError(
        f"No configured FFprobe build for {system}/{arch}"
    )


# ============================================================
# FFPROBE RELEASE LOOKUP
# ============================================================

def download_ffprobe_archive():

    (
        platform_name,
        extension,
        executable
    ) = get_platform_asset_pattern()

    scala(
        f"FFPROBE RELEASE SEARCH | {platform_name}"
    )

    titan(
        f"FFPROBE RELEASE API | {FFPROBE_RELEASE_API}"
    )

    response = get_session().get(
        FFPROBE_RELEASE_API,
        timeout=REQUEST_TIMEOUT,
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )

    try:

        response.raise_for_status()

        release = response.json()

    finally:
        response.close()

    assets = release.get(
        "assets",
        []
    )

    matches = []

    for asset in assets:

        name = asset.get(
            "name",
            ""
        ).lower()

        if platform_name not in name:
            continue

        if extension == ".zip":

            if not name.endswith(".zip"):
                continue

        elif extension == ".tar.xz":

            if not name.endswith(".tar.xz"):
                continue

        if "gpl" not in name:
            continue

        matches.append(asset)

    if not matches:

        raise RuntimeError(
            f"FFprobe asset not found for {platform_name}"
        )

    asset = max(
        matches,
        key=lambda item: item.get(
            "size",
            0
        )
    )

    download_url = asset.get(
        "browser_download_url"
    )

    if not download_url:

        raise RuntimeError(
            "FFprobe download URL missing"
        )

    FFPROBE_ARCHIVE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    archive_path = (
        FFPROBE_ARCHIVE_DIR
        / ("ffprobe_archive" + extension)
    )

    titan(
        f"FFPROBE ASSET SELECTED | {asset.get('name')}"
    )

    titan(
        f"FFPROBE ASSET SIZE | "
        f"{asset.get('size', 0)} bytes"
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

            file.flush()
            os.fsync(file.fileno())

    scala(
        f"FFPROBE ARCHIVE DOWNLOADED | {archive_path}"
    )

    return (
        archive_path,
        executable
    )


# ============================================================
# SAFE ARCHIVE PATH
# ============================================================

def safe_archive_destination(
    root,
    member_name
):

    root = Path(root).resolve()

    normalized = member_name.replace(
        "\\",
        "/"
    )

    parts = [
        part
        for part in normalized.split("/")
        if part not in ("", ".")
    ]

    if any(
        part == ".."
        for part in parts
    ):
        raise RuntimeError(
            f"Unsafe archive member: {member_name}"
        )

    destination = (
        root.joinpath(*parts)
    ).resolve()

    if (
        destination != root
        and root not in destination.parents
    ):

        raise RuntimeError(
            f"Archive path escapes destination: {member_name}"
        )

    return destination


# ============================================================
# FFPROBE ARCHIVE EXTRACTION
# ============================================================

def extract_ffprobe(
    archive_path,
    executable
):

    FFPROBE_EXTRACT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    extracted_binary = None
    extracted_files = 0

    # --------------------------------------------------------
    # ZIP extraction
    # --------------------------------------------------------

    if zipfile.is_zipfile(archive_path):

        with zipfile.ZipFile(
            archive_path
        ) as archive:

            for member in archive.infolist():

                if member.is_dir():
                    continue

                base_name = Path(
                    member.filename
                ).name.lower()

                is_binary = (
                    base_name == executable.lower()
                )

                is_runtime_library = (
                    base_name.endswith(".dll")
                )

                if not (
                    is_binary
                    or is_runtime_library
                ):
                    continue

                destination = safe_archive_destination(
                    FFPROBE_EXTRACT_DIR,
                    member.filename
                )

                destination.parent.mkdir(
                    parents=True,
                    exist_ok=True
                )

                with archive.open(member) as source:

                    with destination.open("wb") as target:

                        shutil.copyfileobj(
                            source,
                            target
                        )

                extracted_files += 1

                if is_binary:
                    extracted_binary = destination

    # --------------------------------------------------------
    # TAR extraction
    # --------------------------------------------------------

    else:

        with tarfile.open(
            archive_path,
            mode="r:*"
        ) as archive:

            for member in archive.getmembers():

                if not member.isfile():
                    continue

                base_name = Path(
                    member.name
                ).name.lower()

                is_binary = (
                    base_name == executable.lower()
                )

                normalized = member.name.replace(
                    "\\",
                    "/"
                )

                is_runtime_library = (
                    "/lib/" in normalized
                    and (
                        ".so" in base_name
                        or base_name.endswith(".dylib")
                    )
                )

                if not (
                    is_binary
                    or is_runtime_library
                ):
                    continue

                destination = safe_archive_destination(
                    FFPROBE_EXTRACT_DIR,
                    member.name
                )

                destination.parent.mkdir(
                    parents=True,
                    exist_ok=True
                )

                source = archive.extractfile(
                    member
                )

                if source is None:
                    continue

                with source:

                    with destination.open("wb") as target:

                        shutil.copyfileobj(
                            source,
                            target
                        )

                extracted_files += 1

                if is_binary:
                    extracted_binary = destination

    if extracted_binary is None:

        raise RuntimeError(
            "FFprobe executable not found inside archive"
        )

    if platform.system().lower() != "windows":

        extracted_binary.chmod(
            0o755
        )

    try:

        archive_path.unlink()

    except OSError as error:

        titan(
            f"ARCHIVE DELETE WARNING | {error}",
            level="WARNING"
        )

    scala(
        f"FFPROBE EXTRACTED | files={extracted_files}"
    )

    titan(
        f"FFPROBE BINARY | {extracted_binary}"
    )

    return extracted_binary


# ============================================================
# FFPROBE VERIFICATION
# ============================================================

def verify_ffprobe(binary):

    result = subprocess.run(
        [
            str(binary),
            "-version",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=FFPROBE_STARTUP_TIMEOUT,
        shell=False
    )

    if result.returncode != 0:

        raise RuntimeError(
            "FFprobe verification failed: "
            + result.stderr[:1000]
        )

    first_line = (
        result.stdout.splitlines()[0]
        if result.stdout
        else "FFprobe version unknown"
    )

    scala(
        f"FFPROBE READY | {first_line}"
    )

    titan_detail(
        f"FFPROBE VERIFIED | {binary} | {first_line}"
    )

    return True


# ============================================================
# FFPROBE INITIALIZATION
# ============================================================

def initialize_ffprobe():

    global FFPROBE_BINARY

    if not ENABLE_FFPROBE:

        if REQUIRE_FFPROBE:

            raise RuntimeError(
                "FFPROBE IS REQUIRED BUT DISABLED"
            )

        return

    if not AUTO_DOWNLOAD_FFPROBE:

        raise RuntimeError(
            "TEMPORARY FFPROBE DOWNLOAD IS DISABLED"
        )

    scala(
        "FFPROBE INITIALIZATION STARTED"
    )

    FFPROBE_RUN_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    (
        archive,
        executable
    ) = download_ffprobe_archive()

    binary = extract_ffprobe(
        archive,
        executable
    )

    verify_ffprobe(binary)

    FFPROBE_BINARY = str(
        binary.resolve()
    )

    scala(
        "FFPROBE TEMPORARY TOOL READY"
    )

    titan_detail(
        f"FFPROBE RUN DIRECTORY | {FFPROBE_RUN_DIR}"
    )


# ============================================================
# PLAYLIST SOURCE URL NORMALIZATION
# ============================================================

def normalize_playlist_source_url(url):
    """
    Нормализация исключительно адресов плейлистов.

    Используется только на этапе DISCOVERY.

    Не применяется:
        - к каналам;
        - к URL внутри M3U;
        - к названиям каналов;
        - к результатам парсинга;
        - к stable/donors.

    Сохраняет query-параметры, поскольку они могут быть
    значимыми для получения содержимого плейлиста.

    Удаляет fragment, который не передаётся HTTP-серверу.

    Приводит схему и hostname к нижнему регистру.
    Удаляет стандартные порты HTTP и HTTPS.
    Сохраняет регистр пути и query-параметры.
    """

    value = str(url).strip()

    if not value:
        return None

    try:

        parsed = urlsplit(value)

        scheme = parsed.scheme.lower()

        hostname = (
            parsed.hostname.lower()
            if parsed.hostname
            else ""
        )

        if not scheme or not hostname:
            return None

        username = parsed.username
        password = parsed.password

        auth = ""

        if username is not None:

            auth = username

            if password is not None:
                auth += ":" + password

            auth += "@"

        port = ""

        try:

            if parsed.port is not None:

                default_port = (
                    scheme == "https"
                    and parsed.port == 443
                ) or (
                    scheme == "http"
                    and parsed.port == 80
                )

                if not default_port:
                    port = f":{parsed.port}"

        except ValueError:
            return None

        # IPv6 hostnames require square brackets in netloc.
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"

        netloc = (
            auth
            + hostname
            + port
        )

        normalized = urlunsplit((
            scheme,
            netloc,
            parsed.path,
            parsed.query,
            "",
        ))

        return normalized

    except Exception:
        return None


# ============================================================
# PLAYLIST SOURCE DEDUPLICATION
# ============================================================

def deduplicate_discovered_playlists(candidates):
    """
    ЕДИНСТВЕННАЯ ТОЧКА ДЕДУПЛИКАЦИИ ПРОЕКТА.

    Работает только со списком обнаруженных M3U/M3U8.

    Алгоритм:
        1. Получает весь список найденных ссылок.
        2. Нормализует адрес каждой ссылки.
        3. Сохраняет первое появление ссылки.
        4. Пропускает последующие повторы.
        5. Не изменяет порядок уникальных источников.

    ВАЖНО:
        Здесь не обрабатываются каналы.
        Здесь не обрабатываются названия.
        Здесь не обрабатываются записи M3U.

    Функция вызывается ровно один раз из
    discover_playlist_urls().
    """

    unique_sources = []

    seen_sources = set()

    duplicates_removed = 0
    invalid_sources = 0

    total_candidates = len(candidates)

    for index, original_url in enumerate(
        candidates,
        1
    ):

        normalized_url = normalize_playlist_source_url(
            original_url
        )

        if not normalized_url:

            invalid_sources += 1

            titan_detail(
                f"SOURCE DEDUP INVALID | "
                f"index={index} | {original_url}"
            )

            continue

        if normalized_url in seen_sources:

            duplicates_removed += 1

            titan_detail(
                f"SOURCE DUPLICATE REMOVED | "
                f"index={index} | {normalized_url}"
            )

            continue

        seen_sources.add(
            normalized_url
        )

        unique_sources.append(
            normalized_url
        )

        titan_detail(
            f"SOURCE UNIQUE PRESERVED | "
            f"index={index} | {normalized_url}"
        )

    counter_set(
        "sources_discovered",
        total_candidates
    )

    counter_set(
        "sources_unique",
        len(unique_sources)
    )

    counter_set(
        "sources_duplicates_removed",
        duplicates_removed
    )

    scala(
        "PLAYLIST SOURCE DEDUPLICATION COMPLETE | "
        f"discovered={total_candidates} | "
        f"unique={len(unique_sources)} | "
        f"duplicates_removed={duplicates_removed} | "
        f"invalid={invalid_sources}"
    )

    titan(
        "SOURCE DEDUPLICATION STATISTICS | "
        f"input={total_candidates} | "
        f"output={len(unique_sources)} | "
        f"removed={duplicates_removed} | "
        f"invalid={invalid_sources}"
    )

    if total_candidates != (
        len(unique_sources)
        + duplicates_removed
        + invalid_sources
    ):

        raise RuntimeError(
            "Playlist deduplication accounting mismatch"
        )

    return unique_sources


# ============================================================
# PLAYLIST DISCOVERY
# ============================================================

def discover_playlist_urls():

    scala(
        "PLAYLIST DISCOVERY STARTED"
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

        r'data-url=["\']'
        r'(https?://shirtv\.uz/playlists/[^"\']+\.m3u8?)'
        r'["\']',

        r'(https?://shirtv\.uz/playlists/[^"\'\s<>]+\.m3u8?)',

        r'(playlists/[^"\'\s<>]+\.m3u8?)',
    ]

    # Each item:
    # (HTML position, pattern index, discovery sequence, URL)
    #
    # Sorting by HTML position preserves the order in which
    # matching URLs first appear in the source document.
    # Pattern index and sequence provide deterministic ordering
    # when several patterns match at the same position.

    located_candidates = []

    discovery_sequence = 0

    # --------------------------------------------------------
    # PHASE 1 — COLLECT ALL MATCHES
    #
    # No deduplication is performed here.
    #
    # The same URL can occur:
    #     - in href;
    #     - in data-url;
    #     - in an absolute link;
    #     - in a relative link;
    #     - several times in the HTML.
    #
    # All valid matches are collected before deduplication.
    # --------------------------------------------------------

    for pattern_index, pattern in enumerate(
        patterns,
        1
    ):

        found = list(
            re.finditer(
                pattern,
                html,
                flags=re.IGNORECASE
            )
        )

        scala(
            f"DISCOVERY PATTERN COMPLETE | "
            f"pattern={pattern_index} | "
            f"matches={len(found)}"
        )

        for match in found:

            href = (
                match.group(1)
                .strip()
                .strip("'\"")
            )

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

            check_path = (
                absolute.split("#", 1)[0]
                .split("?", 1)[0]
            )

            if not check_path.lower().endswith(
                PLAYLIST_EXTENSIONS
            ):
                continue

            discovery_sequence += 1

            located_candidates.append((
                match.start(1),
                pattern_index,
                discovery_sequence,
                absolute,
            ))

    # --------------------------------------------------------
    # PHASE 2 — RESTORE HTML DOCUMENT ORDER
    #
    # The patterns are evaluated independently.
    # Sorting by the source position prevents pattern grouping
    # from changing the order of first appearance in HTML.
    #
    # If multiple patterns match the same source position,
    # pattern index and discovery sequence make the order stable.
    # --------------------------------------------------------

    located_candidates.sort(
        key=lambda item: (
            item[0],
            item[1],
            item[2],
        )
    )

    candidates = [
        item[3]
        for item in located_candidates
    ]

    # --------------------------------------------------------
    # PHASE 3 — RAW DISCOVERY REPORT
    #
    # This is the total number of valid playlist matches
    # across all patterns, including repeated matches.
    # --------------------------------------------------------

    raw_count = len(candidates)

    counter_set(
        "sources_discovered",
        raw_count
    )

    scala(
        f"RAW PLAYLIST MATCHES COLLECTED | {raw_count}"
    )

    titan(
        "DISCOVERY RAW RESULT | "
        f"all_matches={raw_count}"
    )

    for index, url in enumerate(
        candidates,
        1
    ):

        titan_detail(
            f"RAW DISCOVERY MATCH "
            f"[{index}/{raw_count}] | {url}"
        )

    # --------------------------------------------------------
    # PHASE 4 — THE ONLY SOURCE DEDUPLICATION
    #
    # Exactly one call.
    #
    # Before download.
    # Before parsing.
    # Only for playlist source addresses.
    # --------------------------------------------------------

    result = deduplicate_discovered_playlists(
        candidates
    )

    # --------------------------------------------------------
    # PHASE 5 — FINAL UNIQUE SOURCE LIST
    #
    # This list alone is passed to the downloader.
    # First-occurrence order is preserved.
    # --------------------------------------------------------

    scala(
        f"UNIQUE PLAYLIST SOURCES READY | {len(result)}"
    )

    for index, url in enumerate(
        result,
        1
    ):

        titan(
            f"UNIQUE SOURCE "
            f"[{index}/{len(result)}] | {url}"
        )

    if not result:

        MAIN_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        debug_file = (
            MAIN_DIR / "debug_page.html"
        )

        debug_file.write_text(
            html,
            encoding="utf-8"
        )

        raise RuntimeError(
            "No unique M3U/M3U8 playlists discovered"
        )

    return result


# ============================================================
# SOURCE ERROR REGISTRY
# ============================================================

def register_source_error(
    url,
    error
):

    with SOURCE_ERRORS_LOCK:

        SOURCE_ERRORS.append({
            "url": url,
            "error": str(error),
            "run_id": RUN_ID,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
        })

    counter_add(
        "source_errors"
    )


# ============================================================
# DOWNLOAD ONE SOURCE
# ============================================================

def download_one_source(url):

    try:

        if STOP_EVENT.is_set():

            counter_add(
                "download_cancelled"
            )

            return {
                "url": url,
                "content": None,
                "error": "Interrupted",
            }

        data = fetch(
            url,
            referer=INDEX_URL
        )

        counter_add(
            "sources_downloaded"
        )

        return {
            "url": url,
            "content": data,
            "error": None,
        }

    except Exception as error:

        register_source_error(
            url,
            error
        )

        log(
            f"SOURCE DOWNLOAD FAILED | {url} | {error}",
            level="ERROR"
        )

        return {
            "url": url,
            "content": None,
            "error": str(error),
        }


# ============================================================
# PARALLEL DOWNLOAD STAGE
# ============================================================

def download_all_sources(sources):

    total = len(sources)

    counts = counter_snapshot()

    scala(
        f"DOWNLOAD STAGE | "
        f"sources={total} | "
        f"workers={MAX_DOWNLOAD_WORKERS}"
    )

    scala(
        "DOWNLOAD INPUT VERIFICATION | "
        f"discovered={counts['sources_discovered']} | "
        f"unique={counts['sources_unique']} | "
        f"duplicates_removed={counts['sources_duplicates_removed']} | "
        f"queued={total}"
    )

    if total != counts["sources_unique"]:

        raise RuntimeError(
            "Download queue does not match unique source counter"
        )

    results = [None] * total

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_DOWNLOAD_WORKERS,
        thread_name_prefix="SHIRT-DOWNLOAD"
    ) as pool:

        future_map = {}

        for index, url in enumerate(sources):

            future = pool.submit(
                download_one_source,
                url
            )

            future_map[future] = index

        completed = 0

        for future in concurrent.futures.as_completed(
            future_map
        ):

            index = future_map[future]

            try:

                results[index] = future.result()

            except Exception as error:

                register_source_error(
                    sources[index],
                    error
                )

                results[index] = {
                    "url": sources[index],
                    "content": None,
                    "error": str(error),
                }

            completed += 1

            if (
                completed % 25 == 0
                or completed == total
            ):

                titan_detail(
                    f"DOWNLOAD PROGRESS | "
                    f"{completed}/{total}"
                )

    # Preserve unique source discovery order.
    # Every unique source is submitted once.
    # Source duplicates have already been removed.
    #
    # No channel-level deduplication is performed here.

    downloaded = [
        item
        for item in results
        if item is not None
        and item["content"] is not None
    ]

    scala(
        f"DOWNLOAD COMPLETE | "
        f"{len(downloaded)}/{total}"
    )

    titan(
        "DOWNLOAD SOURCE ACCOUNTING | "
        f"unique_sources={total} | "
        f"downloaded={len(downloaded)} | "
        f"errors={total - len(downloaded)}"
    )

    return downloaded


# ============================================================
# M3U PARSING
# ============================================================

def parse_m3u(
    content,
    source
):

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

        if (
            current_extinf
            and "," in current_extinf
        ):

            name = (
                current_extinf.split(
                    ",",
                    1
                )[1].strip()
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


# ============================================================
# ADULT FILTER
# ============================================================

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
# PARSE ONE SOURCE
# ============================================================

def parse_one_source(item):

    source = item["url"]

    try:

        if STOP_EVENT.is_set():

            counter_add(
                "parse_cancelled"
            )

            return []

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

                counter_add(
                    "adult_filtered"
                )

                titan_detail(
                    f"ADULT FILTER | {entry['url']}"
                )

                continue

            clean.append(
                entry
            )

        counter_add(
            "donor_entries",
            len(clean)
        )

        titan(
            f"PARSED SOURCE | {source} "
            f"| entries={len(clean)}"
        )

        return clean

    except Exception as error:

        counter_add(
            "parse_errors"
        )

        log(
            f"PARSE ERROR | {source} | {error}",
            level="ERROR"
        )

        return []


# ============================================================
# PARALLEL PARSING STAGE
# ============================================================

def parse_all_sources(downloaded):

    total = len(downloaded)

    scala(
        f"PARSING STAGE | "
        f"sources={total} | workers={MAX_PARSE_WORKERS}"
    )

    results = [None] * total

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_PARSE_WORKERS,
        thread_name_prefix="SHIRT-PARSE"
    ) as pool:

        future_map = {}

        for index, item in enumerate(downloaded):

            future = pool.submit(
                parse_one_source,
                item
            )

            future_map[future] = index

        completed = 0

        for future in concurrent.futures.as_completed(
            future_map
        ):

            index = future_map[future]

            try:

                results[index] = future.result()

            except Exception as error:

                counter_add(
                    "parse_errors"
                )

                log(
                    f"PARSER WORKER ERROR | {error}",
                    level="ERROR"
                )

                results[index] = []

            completed += 1

            if (
                completed % 25 == 0
                or completed == total
            ):

                titan_detail(
                    f"PARSING PROGRESS | "
                    f"{completed}/{total}"
                )

    all_entries = []

    # Preserve original unique source order.
    # Do not deduplicate entries.
    # Duplicate channel URLs remain.
    # Duplicate channel names remain.
    # Duplicate M3U records remain.

    for source_entries in results:

        if source_entries:
            all_entries.extend(
                source_entries
            )

    scala(
        f"PARSING COMPLETE | entries={len(all_entries)}"
    )

    titan(
        "CHANNEL RECORD PRESERVATION | "
        "URL deduplication=DISABLED | "
        "name deduplication=DISABLED | "
        "M3U record deduplication=DISABLED"
    )

    return all_entries


# ============================================================
# HEAD VALIDATION
# ============================================================

def check_head(url):

    if not ENABLE_HEAD_CHECK:
        return None

    if STOP_EVENT.is_set():
        return None

    started = time.monotonic()

    try:

        response = get_session().head(
            url,
            timeout=HEAD_TIMEOUT,
            allow_redirects=True
        )

        try:

            status = response.status_code

        finally:
            response.close()

        elapsed = time.monotonic() - started

        if 200 <= status < 400:

            counter_add(
                "head_ok"
            )

            titan_detail(
                f"HEAD OK | status={status} "
                f"| elapsed={elapsed:.2f}s | {url}"
            )

            return True

        if status in (
            404,
            410,
        ):

            counter_add(
                "head_failed"
            )

            titan(
                f"HEAD DEAD | status={status} | {url}",
                level="WARNING"
            )

            return False

        counter_add(
            "head_inconclusive"
        )

        titan_detail(
            f"HEAD INCONCLUSIVE | "
            f"status={status} | {url}"
        )

        return None

    except Exception as error:

        counter_add(
            "head_inconclusive"
        )

        titan(
            f"HEAD ERROR | {url} | {error}",
            level="WARNING"
        )

        return None


# ============================================================
# GET VALIDATION
# ============================================================

def check_get(url):

    if not ENABLE_GET_CHECK:
        return None

    if STOP_EVENT.is_set():
        return None

    response = None
    started = time.monotonic()

    try:

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

        if status in (
            404,
            410,
        ):

            counter_add(
                "get_failed"
            )

            titan(
                f"GET DEAD | status={status} | {url}",
                level="WARNING"
            )

            return False

        if not 200 <= status < 400:

            counter_add(
                "get_inconclusive"
            )

            titan_detail(
                f"GET INCONCLUSIVE | "
                f"status={status} | {url}"
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

            counter_add(
                "get_ok"
            )

            titan_detail(
                f"GET OK | status={status} "
                f"| bytes={len(chunk)} "
                f"| elapsed={elapsed:.2f}s | {url}"
            )

            return True

        counter_add(
            "get_inconclusive"
        )

        titan(
            f"GET EMPTY RESPONSE | {url}",
            level="WARNING"
        )

        return None

    except Exception as error:

        counter_add(
            "get_inconclusive"
        )

        titan(
            f"GET ERROR | {url} | {error}",
            level="WARNING"
        )

        return None

    finally:

        if response is not None:

            try:
                response.close()
            except Exception:
                pass


# ============================================================
# PROCESS REGISTRY
# ============================================================

def register_process(process):

    with PROCESS_LOCK:

        ACTIVE_PROCESSES.add(
            process
        )


def unregister_process(process):

    with PROCESS_LOCK:

        ACTIVE_PROCESSES.discard(
            process
        )


def terminate_active_processes():

    with PROCESS_LOCK:

        processes = list(
            ACTIVE_PROCESSES
        )

    for process in processes:

        try:

            if process.poll() is None:

                titan_detail(
                    f"TERMINATING ACTIVE FFPROBE PID={process.pid}"
                )

                process.kill()

        except Exception as error:

            titan(
                f"PROCESS TERMINATION ERROR | {error}",
                level="ERROR"
            )


# ============================================================
# FFPROBE STREAM VALIDATION
# ============================================================

def check_ffprobe(url):

    if not ENABLE_FFPROBE:
        return None

    if not FFPROBE_BINARY:

        raise RuntimeError(
            "FFPROBE WAS NOT INITIALIZED"
        )

    if STOP_EVENT.is_set():
        return None

    started = time.monotonic()

    process = None

    try:

        command = [
            FFPROBE_BINARY,

            "-hide_banner",
            "-v",
            "error",

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

        titan_detail(
            f"FFPROBE START | {url}"
        )

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False
        )

        register_process(
            process
        )

        try:

            stdout, stderr = process.communicate(
                timeout=FFPROBE_TIMEOUT
            )

        except subprocess.TimeoutExpired:

            try:
                process.kill()
            except Exception:
                pass

            stdout, stderr = process.communicate()

            counter_add(
                "ffprobe_inconclusive"
            )

            titan(
                f"FFPROBE TIMEOUT | {url}",
                level="WARNING"
            )

            return None

        elapsed = time.monotonic() - started

        if process.returncode != 0:

            counter_add(
                "ffprobe_inconclusive"
            )

            titan(
                f"FFPROBE PROCESS ERROR | "
                f"returncode={process.returncode} "
                f"| elapsed={elapsed:.2f}s "
                f"| {url} | {stderr[:500]}",
                level="WARNING"
            )

            return None

        try:

            data = json.loads(
                stdout
            )

        except json.JSONDecodeError as error:

            counter_add(
                "ffprobe_inconclusive"
            )

            titan(
                f"FFPROBE INVALID JSON | {url} | {error}",
                level="WARNING"
            )

            return None

        streams = data.get(
            "streams",
            []
        )

        media_types = [
            stream.get("codec_type")
            for stream in streams
        ]

        has_media = any(
            media_type in (
                "video",
                "audio",
            )
            for media_type in media_types
        )

        if has_media:

            counter_add(
                "ffprobe_ok"
            )

            titan(
                f"FFPROBE MEDIA OK | "
                f"streams={media_types} "
                f"| elapsed={elapsed:.2f}s | {url}"
            )

            return True

        counter_add(
            "ffprobe_failed"
        )

        titan(
            f"FFPROBE NO MEDIA STREAMS | {url}",
            level="WARNING"
        )

        return False

    except Exception as error:

        counter_add(
            "ffprobe_inconclusive"
        )

        titan(
            f"FFPROBE EXCEPTION | {url} | {error}",
            level="WARNING"
        )

        return None

    finally:

        if process is not None:

            try:

                if process.poll() is None:

                    process.kill()

                    process.communicate(
                        timeout=5
                    )

            except Exception:
                pass

            unregister_process(
                process
            )


# ============================================================
# VALIDATION DECISION ENGINE
# ============================================================

def evaluate_validation_results(
    head_result,
    get_result,
    ffprobe_result
):

    """
    Validation decision policy.

    True:
        Confirmed available stream.

    False:
        Confirmed unavailable resource.

    None:
        Inconclusive.

    Rules:

    1. FFprobe confirms audio/video:
       ACCEPT.

    2. FFprobe confirms no media:
       REJECT only if both HEAD and GET confirm dead resource.
       Otherwise INCONCLUSIVE.

    3. FFprobe inconclusive:
       GET success confirms accessibility.
       Both HEAD and GET dead means REJECT.
       Remaining combinations are INCONCLUSIVE.
    """

    if ffprobe_result is True:
        return True

    if ffprobe_result is False:

        if (
            head_result is False
            and get_result is False
        ):
            return False

        return None

    if ffprobe_result is None:

        if get_result is True:
            return True

        if (
            head_result is False
            and get_result is False
        ):
            return False

        return None

    return None


# ============================================================
# VALIDATION TASK DISPATCHER
# ============================================================

def validation_task(
    kind,
    entry
):

    url = entry["url"]

    started = time.monotonic()

    try:

        if STOP_EVENT.is_set():
            return None

        if kind == "HEAD":

            return check_head(
                url
            )

        if kind == "GET":

            return check_get(
                url
            )

        if kind == "FFPROBE":

            return check_ffprobe(
                url
            )

        raise ValueError(
            f"Unknown validation task: {kind}"
        )

    except Exception as error:

        counter_add(
            "validation_errors"
        )

        titan(
            f"VALIDATION TASK ERROR | "
            f"type={kind} | {url} | {error}",
            level="ERROR"
        )

        return None

    finally:

        elapsed = time.monotonic() - started

        if kind == "HEAD":

            counter_add(
                "head_tasks_completed"
            )

        elif kind == "GET":

            counter_add(
                "get_tasks_completed"
            )

        elif kind == "FFPROBE":

            counter_add(
                "ffprobe_tasks_completed"
            )

        counter_add(
            "validation_tasks_completed"
        )

        titan_detail(
            f"TASK TERMINATED | "
            f"type={kind} | elapsed={elapsed:.2f}s | {url}"
        )


# ============================================================
# VALIDATION TASK SUBMISSION
# ============================================================

def submit_validation_task(
    executor,
    kind,
    entry
):

    return executor.submit(
        validation_task,
        kind,
        entry
    )


# ============================================================
# PARALLEL VALIDATION STAGE
# ============================================================

def validate_entries(entries):

    total = len(entries)

    if not entries:
        return []

    if REQUIRE_FFPROBE and not FFPROBE_BINARY:

        raise RuntimeError(
            "Mandatory FFprobe is unavailable"
        )

    if not ENABLE_HEAD_CHECK:
        raise RuntimeError(
            "HEAD validation is disabled"
        )

    if not ENABLE_GET_CHECK:
        raise RuntimeError(
            "GET validation is disabled"
        )

    if not ENABLE_FFPROBE:
        raise RuntimeError(
            "FFprobe validation is disabled"
        )

    scala(
        f"VALIDATION START | channels={total}"
    )

    scala(
        f"HEAD POOL | workers={MAX_HEAD_WORKERS}"
    )

    scala(
        f"GET POOL | workers={MAX_GET_WORKERS}"
    )

    scala(
        f"FFPROBE POOL | workers={MAX_FFPROBE_WORKERS}"
    )

    scala(
        f"TOTAL VALIDATION WORKERS | "
        f"{MAX_VALIDATION_WORKERS}"
    )

    # --------------------------------------------------------
    # Independent result arrays.
    # --------------------------------------------------------

    head_results = [None] * total
    get_results = [None] * total
    ffprobe_results = [None] * total

    head_completed = [False] * total
    get_completed = [False] * total
    ffprobe_completed = [False] * total

    accepted_flags = [False] * total

    decisions_completed = 0

    # --------------------------------------------------------
    # All three pools are created independently.
    #
    # There is no parent validation executor.
    # No worker waits for another worker's Future.
    # No nested executor deadlock.
    # --------------------------------------------------------

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_HEAD_WORKERS,
        thread_name_prefix="SHIRT-HEAD"
    ) as head_pool:

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_GET_WORKERS,
            thread_name_prefix="SHIRT-GET"
        ) as get_pool:

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=MAX_FFPROBE_WORKERS,
                thread_name_prefix="SHIRT-FFPROBE"
            ) as ffprobe_pool:

                futures = {}

                # --------------------------------------------
                # Submit every HEAD task.
                # --------------------------------------------

                for index, entry in enumerate(entries):

                    future = submit_validation_task(
                        head_pool,
                        "HEAD",
                        entry
                    )

                    futures[future] = (
                        index,
                        "HEAD"
                    )

                counter_add(
                    "head_tasks_submitted",
                    total
                )

                # --------------------------------------------
                # Submit every GET task.
                # --------------------------------------------

                for index, entry in enumerate(entries):

                    future = submit_validation_task(
                        get_pool,
                        "GET",
                        entry
                    )

                    futures[future] = (
                        index,
                        "GET"
                    )

                counter_add(
                    "get_tasks_submitted",
                    total
                )

                # --------------------------------------------
                # Submit every FFprobe task.
                # --------------------------------------------

                for index, entry in enumerate(entries):

                    future = submit_validation_task(
                        ffprobe_pool,
                        "FFPROBE",
                        entry
                    )

                    futures[future] = (
                        index,
                        "FFPROBE"
                    )

                counter_add(
                    "ffprobe_tasks_submitted",
                    total
                )

                total_tasks = total * 3

                counter_add(
                    "validation_tasks_submitted",
                    total_tasks
                )

                scala(
                    f"VALIDATION QUEUE CREATED | "
                    f"HEAD={total} | "
                    f"GET={total} | "
                    f"FFPROBE={total} | "
                    f"TOTAL={total_tasks}"
                )

                titan(
                    f"VALIDATION DISPATCH COMPLETE | "
                    f"independent_tasks={total_tasks}"
                )

                # --------------------------------------------
                # Collect all results.
                #
                # IMPORTANT:
                # Do not break when STOP_EVENT is set.
                # Every submitted Future must be accounted for.
                # --------------------------------------------

                completed_tasks = 0

                for future in concurrent.futures.as_completed(
                    futures
                ):

                    index, kind = futures[future]

                    try:

                        result = future.result()

                    except Exception as error:

                        result = None

                        counter_add(
                            "validation_errors"
                        )

                        titan(
                            f"VALIDATION FUTURE ERROR | "
                            f"type={kind} "
                            f"| {entries[index]['url']} "
                            f"| {error}",
                            level="ERROR"
                        )

                    completed_tasks += 1

                    # ----------------------------------------
                    # Store the individual result.
                    # ----------------------------------------

                    if kind == "HEAD":

                        head_results[index] = result
                        head_completed[index] = True

                    elif kind == "GET":

                        get_results[index] = result
                        get_completed[index] = True

                    elif kind == "FFPROBE":

                        ffprobe_results[index] = result
                        ffprobe_completed[index] = True

                    # ----------------------------------------
                    # Wait for all three checks for this
                    # specific channel.
                    # ----------------------------------------

                    if not (
                        head_completed[index]
                        and get_completed[index]
                        and ffprobe_completed[index]
                    ):
                        continue

                    # ----------------------------------------
                    # One final decision per input record.
                    # ----------------------------------------

                    decisions_completed += 1

                    counter_add(
                        "checked"
                    )

                    entry = entries[index]

                    decision = evaluate_validation_results(
                        head_results[index],
                        get_results[index],
                        ffprobe_results[index]
                    )

                    titan(
                        f"CHANNEL DECISION | "
                        f"HEAD={head_results[index]} | "
                        f"GET={get_results[index]} | "
                        f"FFPROBE={ffprobe_results[index]} | "
                        f"FINAL={decision} | "
                        f"{entry['url']}"
                    )

                    # ----------------------------------------
                    # ACCEPTED
                    # ----------------------------------------

                    if decision is True:

                        accepted_flags[index] = True

                        counter_add(
                            "accepted"
                        )

                    # ----------------------------------------
                    # REJECTED
                    # ----------------------------------------

                    elif decision is False:

                        counter_add(
                            "rejected"
                        )

                    # ----------------------------------------
                    # INCONCLUSIVE
                    # ----------------------------------------

                    else:

                        counter_add(
                            "inconclusive"
                        )

                        if KEEP_INCONCLUSIVE:

                            accepted_flags[index] = True

                            counter_add(
                                "accepted"
                            )

                    counter_add(
                        "validation_decisions_completed"
                    )

                    # ----------------------------------------
                    # Progress reporting.
                    # ----------------------------------------

                    if (
                        decisions_completed % 25 == 0
                        or decisions_completed == total
                    ):

                        counts = counter_snapshot()

                        scala(
                            f"VALIDATION PROGRESS | "
                            f"channels={decisions_completed}/{total} "
                            f"| tasks={completed_tasks}/{total_tasks} "
                            f"| accepted={counts['accepted']} "
                            f"| rejected={counts['rejected']} "
                            f"| inconclusive={counts['inconclusive']}"
                        )

                # --------------------------------------------
                # Executor contexts close after all Futures.
                # FFprobe cleanup is not performed here.
                # It happens in main() after this function
                # and all other processing have finished.
                # --------------------------------------------

    stable = [
        entries[index]
        for index in range(total)
        if accepted_flags[index]
    ]

    counts = counter_snapshot()

    scala(
        f"VALIDATION TASKS COMPLETE | "
        f"{counts['validation_tasks_completed']}/"
        f"{counts['validation_tasks_submitted']}"
    )

    scala(
        f"VALIDATION DECISIONS COMPLETE | "
        f"{decisions_completed}/{total}"
    )

    scala(
        f"VALIDATION RESULT | "
        f"stable={len(stable)} | input={total}"
    )

    if STOP_EVENT.is_set():

        raise RuntimeError(
            "Validation interrupted; partial output will not be written"
        )

    return stable


# ============================================================
# ATOMIC M3U WRITER
# ============================================================

def write_m3u(
    path,
    entries
):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary = path.with_name(
        path.name + f".{RUN_ID}.tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="\n"
    ) as file:

        file.write(
            "#EXTM3U\n"
        )

        for entry in entries:

            file.write(
                entry["extinf"].rstrip() + "\n"
            )

            file.write(
                entry["url"].rstrip() + "\n"
            )

        file.flush()

        os.fsync(
            file.fileno()
        )

    temporary.replace(
        path
    )

    scala(
        f"PLAYLIST WRITTEN | "
        f"{path} | entries={len(entries)}"
    )

    titan_detail(
        f"WRITE COMPLETE | {path} | entries={len(entries)}"
    )


# ============================================================
# COPY OUTPUTS TO UZ_DATA
# ============================================================

def copy_output_to_uz_data():

    UZ_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    copies = [
        (
            MAIN_DONORS,
            UZ_DONORS
        ),
        (
            MAIN_STABLE,
            UZ_STABLE
        ),
        (
            SHIRT_DONORS,
            UZ_SHIRT_DONORS
        ),
        (
            SHIRT_STABLE,
            UZ_SHIRT_STABLE
        ),
    ]

    for source, destination in copies:

        if not source.is_file():

            raise FileNotFoundError(
                f"Required output missing: {source}"
            )

        temporary = destination.with_name(
            destination.name + f".{RUN_ID}.tmp"
        )

        shutil.copy2(
            source,
            temporary
        )

        temporary.replace(
            destination
        )

        scala(
            f"UZ_DATA COPY COMPLETE | {destination}"
        )


# ============================================================
# SOURCE ERROR REPORT
# ============================================================

def write_error_report():

    with SOURCE_ERRORS_LOCK:

        data = list(
            SOURCE_ERRORS
        )

    if not data:
        return

    report_path = (
        MAIN_LOG_DIR
        / f"source_errors_{RUN_ID}.json"
    )

    temporary = report_path.with_name(
        report_path.name + ".tmp"
    )

    temporary.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8"
    )

    temporary.replace(
        report_path
    )

    titan(
        f"SOURCE ERROR REPORT WRITTEN | {report_path}"
    )


# ============================================================
# VALIDATION DIAGNOSTIC REPORT
# ============================================================

def write_validation_report():

    counts = counter_snapshot()

    report = {
        "run_id": RUN_ID,
        "timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "workers": {
            "download": MAX_DOWNLOAD_WORKERS,
            "parsing": MAX_PARSE_WORKERS,
            "head": MAX_HEAD_WORKERS,
            "get": MAX_GET_WORKERS,
            "ffprobe": MAX_FFPROBE_WORKERS,
            "validation_total": MAX_VALIDATION_WORKERS,
            "all_pools_total": TOTAL_CONFIGURED_WORKERS,
        },

        "deduplication": {
            "enabled": True,

            "playlist_sources": {
                "enabled": True,
                "execution_count": 1,
                "stage": "discovery_before_download",
                "preserve_first_occurrence": True,
                "preserve_order": True,
                "normalized_url_comparison": True,
            },

            "channel_urls": {
                "enabled": False,
            },

            "channel_names": {
                "enabled": False,
            },

            "m3u_records": {
                "enabled": False,
            },

            "source_counters": {
                "discovered": counts["sources_discovered"],
                "unique": counts["sources_unique"],
                "duplicates_removed": counts[
                    "sources_duplicates_removed"
                ],
                "downloaded": counts["sources_downloaded"],
            },
        },

        "counters": counts,

        "ffprobe": {
            "binary": FFPROBE_BINARY,
            "temporary_directory": str(
                FFPROBE_RUN_DIR
            ),
            "required": REQUIRE_FFPROBE,
        },
    }

    report_path = (
        MAIN_LOG_DIR
        / f"validation_report_{RUN_ID}.json"
    )

    temporary = report_path.with_name(
        report_path.name + ".tmp"
    )

    temporary.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8"
    )

    temporary.replace(
        report_path
    )

    titan(
        f"VALIDATION REPORT WRITTEN | {report_path}"
    )


# ============================================================
# FINAL REPORT
# ============================================================

def print_report(
    elapsed,
    stable_count
):

    counts = counter_snapshot()

    report = [
        "=" * 90,

        'PHOENIX HUB "BARYSHNIKOV" — SHIRT.UZ COLLECTOR',

        f"RUN ID: {RUN_ID}",

        "=" * 90,

        "SOURCE DISCOVERY AND DOWNLOAD",

        f"Raw source matches       : {counts['sources_discovered']}",
        f"Unique playlist sources : {counts['sources_unique']}",
        f"Duplicate sources removed: {counts['sources_duplicates_removed']}",
        f"Sources downloaded      : {counts['sources_downloaded']}",
        f"Source errors           : {counts['source_errors']}",

        "-" * 90,

        "PARSING",

        f"Donor entries            : {counts['donor_entries']}",
        f"Adult filtered           : {counts['adult_filtered']}",
        f"Parse errors             : {counts['parse_errors']}",

        "-" * 90,

        "DEDUPLICATION POLICY",

        "Playlist source dedup    : ENABLED",
        "Source dedup executions  : 1",
        "Source dedup stage      : DISCOVERY BEFORE DOWNLOAD",
        "Source first occurrence : PRESERVED",
        "Unique source order     : PRESERVED",

        "Channel URL dedup       : DISABLED",
        "Channel name dedup      : DISABLED",
        "M3U record dedup        : DISABLED",

        "-" * 90,

        "HEAD VALIDATION",

        f"HEAD OK                  : {counts['head_ok']}",
        f"HEAD FAILED              : {counts['head_failed']}",
        f"HEAD INCONCLUSIVE        : {counts['head_inconclusive']}",

        f"HEAD tasks submitted     : {counts['head_tasks_submitted']}",
        f"HEAD tasks completed     : {counts['head_tasks_completed']}",

        "-" * 90,

        "GET VALIDATION",

        f"GET OK                   : {counts['get_ok']}",
        f"GET FAILED               : {counts['get_failed']}",
        f"GET INCONCLUSIVE         : {counts['get_inconclusive']}",

        f"GET tasks submitted      : {counts['get_tasks_submitted']}",
        f"GET tasks completed      : {counts['get_tasks_completed']}",

        "-" * 90,

        "FFPROBE VALIDATION",

        f"FFprobe OK               : {counts['ffprobe_ok']}",
        f"FFprobe FAILED           : {counts['ffprobe_failed']}",
        f"FFprobe INCONCLUSIVE     : {counts['ffprobe_inconclusive']}",

        f"FFprobe tasks submitted  : {counts['ffprobe_tasks_submitted']}",
        f"FFprobe tasks completed  : {counts['ffprobe_tasks_completed']}",

        "-" * 90,

        "VALIDATION DISPATCHER",

        f"Validation submitted     : {counts['validation_tasks_submitted']}",
        f"Validation completed     : {counts['validation_tasks_completed']}",
        f"Validation decisions     : {counts['validation_decisions_completed']}",

        f"Checked                  : {counts['checked']}",
        f"Accepted                 : {counts['accepted']}",
        f"Rejected                 : {counts['rejected']}",
        f"Inconclusive             : {counts['inconclusive']}",

        f"Stable entries           : {stable_count}",

        "-" * 90,

        "WORKER CONFIGURATION",

        f"Download workers         : {MAX_DOWNLOAD_WORKERS}",
        f"Parsing workers          : {MAX_PARSE_WORKERS}",

        f"HEAD workers             : {MAX_HEAD_WORKERS}",
        f"GET workers              : {MAX_GET_WORKERS}",
        f"FFprobe workers          : {MAX_FFPROBE_WORKERS}",

        f"Validation workers       : {MAX_VALIDATION_WORKERS}",
        f"All configured workers  : {TOTAL_CONFIGURED_WORKERS}",

        "-" * 90,

        "ERRORS AND RETRIES",

        f"Download retries         : {counts['download_retries']}",
        f"Validation errors        : {counts['validation_errors']}",

        "-" * 90,

        "FFPROBE TEMPORARY INSTALLATION",

        f"FFprobe binary           : {FFPROBE_BINARY}",
        f"FFprobe temporary dir    : {FFPROBE_RUN_DIR}",

        "-" * 90,

        f"Elapsed                  : {elapsed:.2f} seconds",

        "=" * 90,
    ]

    output = "\n".join(
        report
    )

    print(
        "\n" + output,
        flush=True
    )

    scala(
        f"FINISHED | stable={stable_count} "
        f"| elapsed={elapsed:.2f}s"
    )

    titan(
        output
    )


# ============================================================
# TEMPORARY FFPROBE CLEANUP
# ============================================================

def cleanup_ffprobe():

    global FFPROBE_BINARY

    scala(
        "FFPROBE CLEANUP STARTED"
    )

    # All validation executor contexts must already be closed.
    # Only processes belonging to this run are registered.

    terminate_active_processes()

    old_binary = FFPROBE_BINARY

    FFPROBE_BINARY = None

    try:

        if FFPROBE_RUN_DIR.exists():

            shutil.rmtree(
                FFPROBE_RUN_DIR
            )

        if FFPROBE_RUN_DIR.exists():

            raise RuntimeError(
                "Temporary FFprobe directory still exists"
            )

        scala(
            "FFPROBE CLEANUP SUCCESS"
        )

        titan_detail(
            f"FFPROBE REMOVED | {old_binary}"
        )

    except Exception as error:

        log(
            f"FFPROBE CLEANUP FAILED | {error}",
            level="ERROR"
        )

        raise


# ============================================================
# RUNTIME CONFIGURATION VALIDATION
# ============================================================

def validate_configuration():

    if MAX_DOWNLOAD_WORKERS < 1:
        raise ValueError(
            "MAX_DOWNLOAD_WORKERS must be >= 1"
        )

    if MAX_PARSE_WORKERS < 1:
        raise ValueError(
            "MAX_PARSE_WORKERS must be >= 1"
        )

    if MAX_HEAD_WORKERS < 1:
        raise ValueError(
            "MAX_HEAD_WORKERS must be >= 1"
        )

    if MAX_GET_WORKERS < 1:
        raise ValueError(
            "MAX_GET_WORKERS must be >= 1"
        )

    if MAX_FFPROBE_WORKERS < 1:
        raise ValueError(
            "MAX_FFPROBE_WORKERS must be >= 1"
        )

    if REQUIRE_FFPROBE and not ENABLE_FFPROBE:

        raise ValueError(
            "FFprobe is mandatory but disabled"
        )

    if MAX_RETRIES < 0:
        raise ValueError(
            "MAX_RETRIES cannot be negative"
        )

    scala(
        "CONFIGURATION VALIDATED"
    )

    titan_detail(
        f"CONFIGURATION | "
        f"DOWNLOAD={MAX_DOWNLOAD_WORKERS} "
        f"| PARSE={MAX_PARSE_WORKERS} "
        f"| HEAD={MAX_HEAD_WORKERS} "
        f"| GET={MAX_GET_WORKERS} "
        f"| FFPROBE={MAX_FFPROBE_WORKERS}"
    )


# ============================================================
# INTERRUPT HANDLER
# ============================================================

def handle_signal(
    signum,
    frame
):

    signal_name = str(
        signum
    )

    try:

        scala(
            f"STOP SIGNAL RECEIVED | {signal_name}",
            level="WARNING"
        )

    except Exception:
        pass

    STOP_EVENT.set()

    terminate_active_processes()


# ============================================================
# MAIN
# ============================================================

def main():

    global START_TIME
    global EXIT_CODE

    START_TIME = time.time()

    EXIT_CODE = 1

    try:

        initialize_logs()

        validate_configuration()

        print("=" * 90)

        print(
            'Phoenix Hub "Baryshnikov" — Shirt.uz Collector'
        )

        print(
            f"RUN ID: {RUN_ID}"
        )

        print(
            f"DOWNLOAD: {MAX_DOWNLOAD_WORKERS}"
        )

        print(
            f"PARSING: {MAX_PARSE_WORKERS}"
        )

        print(
            f"HEAD: {MAX_HEAD_WORKERS}"
        )

        print(
            f"GET: {MAX_GET_WORKERS}"
        )

        print(
            f"FFPROBE: {MAX_FFPROBE_WORKERS}"
        )

        print(
            f"TOTAL CONFIGURED WORKERS: {TOTAL_CONFIGURED_WORKERS}"
        )

        print("=" * 90)

        scala(
            "COLLECTOR STARTED"
        )

        scala(
            "PLAYLIST SOURCE DEDUPLICATION ENABLED"
        )

        scala(
            "DEDUPLICATION SCOPE: DISCOVERED PLAYLIST URLS ONLY"
        )

        scala(
            "DEDUPLICATION EXECUTION COUNT: EXACTLY ONE"
        )

        scala(
            "DEDUPLICATION STAGE: AFTER DISCOVERY, BEFORE DOWNLOAD"
        )

        scala(
            "CHANNEL URL DEDUPLICATION: DISABLED"
        )

        scala(
            "CHANNEL NAME DEDUPLICATION: DISABLED"
        )

        scala(
            "M3U RECORD DEDUPLICATION: DISABLED"
        )

        titan_detail(
            "SOURCE DEDUPLICATION WILL RUN ONCE AFTER ALL "
            "DISCOVERY PATTERNS HAVE COMPLETED"
        )

        titan_detail(
            "CHANNEL URL, CHANNEL NAME AND M3U RECORD "
            "DUPLICATES WILL BE PRESERVED"
        )

        MAIN_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        SHIRT_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        UZ_DATA_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        # ----------------------------------------------------
        # STAGE 1 — FFPROBE INITIALIZATION
        # ----------------------------------------------------

        scala(
            "STAGE 1: TEMPORARY FFPROBE INITIALIZATION"
        )

        initialize_ffprobe()

        if REQUIRE_FFPROBE and not FFPROBE_BINARY:

            raise RuntimeError(
                "MANDATORY FFPROBE NOT AVAILABLE"
            )

        # ----------------------------------------------------
        # STAGE 2 — DISCOVERY AND SOURCE DEDUPLICATION
        # ----------------------------------------------------

        scala(
            "STAGE 2: PLAYLIST DISCOVERY"
        )

        scala(
            "STAGE 2.1: COLLECT ALL PLAYLIST MATCHES"
        )

        scala(
            "STAGE 2.2: EXECUTE SOURCE DEDUPLICATION ONCE"
        )

        scala(
            "STAGE 2.3: PRESERVE FIRST OCCURRENCE ORDER"
        )

        sources = discover_playlist_urls()

        source_counts = counter_snapshot()

        scala(
            "DISCOVERY FINAL ACCOUNTING | "
            f"raw={source_counts['sources_discovered']} | "
            f"unique={source_counts['sources_unique']} | "
            f"removed={source_counts['sources_duplicates_removed']}"
        )

        if STOP_EVENT.is_set():

            raise RuntimeError(
                "Interrupted after discovery"
            )

        # ----------------------------------------------------
        # STAGE 3 — DOWNLOAD UNIQUE SOURCES ONLY
        # ----------------------------------------------------

        scala(
            "STAGE 3: PARALLEL DOWNLOAD OF UNIQUE SOURCES"
        )

        downloaded = download_all_sources(
            sources
        )

        if STOP_EVENT.is_set():

            raise RuntimeError(
                "Interrupted during download"
            )

        if not downloaded:

            raise RuntimeError(
                "No playlist sources downloaded"
            )

        # ----------------------------------------------------
        # STAGE 4 — PARSING
        # ----------------------------------------------------

        scala(
            "STAGE 4: PARALLEL PARSING"
        )

        donors = parse_all_sources(
            downloaded
        )

        del downloaded

        if STOP_EVENT.is_set():

            raise RuntimeError(
                "Interrupted during parsing"
            )

        if not donors:

            raise RuntimeError(
                "No playlist entries collected"
            )

        # ----------------------------------------------------
        # STAGE 5 — NO CHANNEL DEDUPLICATION
        # ----------------------------------------------------

        scala(
            "STAGE 5: CHANNEL RECORDS PRESERVED"
        )

        scala(
            f"ALL PARSED RECORDS PRESERVED | {len(donors)}"
        )

        scala(
            "CHANNEL URL DEDUPLICATION: DISABLED"
        )

        scala(
            "CHANNEL NAME DEDUPLICATION: DISABLED"
        )

        scala(
            "M3U RECORD DEDUPLICATION: DISABLED"
        )

        titan_detail(
            f"RECORDS ENTERING VALIDATION | {len(donors)}"
        )

        titan_detail(
            "NO CHANNEL-LEVEL DEDUPLICATION HAS BEEN PERFORMED"
        )

        # ----------------------------------------------------
        # STAGE 6 — THREE INDEPENDENT VALIDATION POOLS
        # ----------------------------------------------------

        scala(
            "STAGE 6: PARALLEL HEAD + GET + FFPROBE"
        )

        stable = validate_entries(
            donors
        )

        if STOP_EVENT.is_set():

            raise RuntimeError(
                "Interrupted after validation"
            )

        # ----------------------------------------------------
        # STAGE 7 — WRITE OUTPUTS
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
        # STAGE 8 — UZ_DATA
        # ----------------------------------------------------

        scala(
            "STAGE 8: UZ_DATA OUTPUT"
        )

        copy_output_to_uz_data()

        # ----------------------------------------------------
        # STAGE 9 — ERROR REPORTS
        # ----------------------------------------------------

        scala(
            "STAGE 9: ERROR REPORTS"
        )

        write_error_report()

        write_validation_report()

        # ----------------------------------------------------
        # STAGE 10 — FINAL REPORT
        # ----------------------------------------------------

        elapsed = (
            time.time() - START_TIME
        )

        print_report(
            elapsed,
            len(stable)
        )

        scala(
            "COLLECTOR FINISHED SUCCESSFULLY"
        )

        EXIT_CODE = 0

    except KeyboardInterrupt:

        STOP_EVENT.set()

        scala(
            "COLLECTOR INTERRUPTED BY USER",
            level="WARNING"
        )

        EXIT_CODE = 130

    except Exception as error:

        STOP_EVENT.set()

        log(
            f"FATAL | {error}",
            level="FATAL"
        )

        titan_detail(
            f"FATAL DETAILS | "
            f"{type(error).__name__}: {error}",
            level="FATAL"
        )

        EXIT_CODE = 1

    finally:

        # ----------------------------------------------------
        # STOP ALL NEW WORK
        # ----------------------------------------------------

        STOP_EVENT.set()

        # ----------------------------------------------------
        # TERMINATE REGISTERED FFPROBE PROCESSES
        # ----------------------------------------------------

        terminate_active_processes()

        # ----------------------------------------------------
        # REMOVE TEMPORARY FFPROBE
        #
        # validate_entries() uses nested executor contexts.
        # Returning from it means all its workers have exited.
        # ----------------------------------------------------

        try:

            cleanup_ffprobe()

        except Exception as error:

            EXIT_CODE = 1

            print(
                f"FFPROBE CLEANUP ERROR: {error}",
                file=sys.stderr,
                flush=True
            )

        # ----------------------------------------------------
        # FINAL COUNTERS
        # ----------------------------------------------------

        elapsed = (
            time.time() - START_TIME
        )

        try:

            titan(
                f"PROCESS TERMINATED | "
                f"exit_code={EXIT_CODE} "
                f"| elapsed={elapsed:.2f}s"
            )

        except Exception:
            pass

        # ----------------------------------------------------
        # COMBINED LOG
        # ----------------------------------------------------

        try:

            write_combined_log()

        except Exception as error:

            print(
                f"COMBINED LOG ERROR: {error}",
                file=sys.stderr,
                flush=True
            )

            EXIT_CODE = 1

    return EXIT_CODE


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        signal.signal(
            signal.SIGINT,
            handle_signal
        )

        if hasattr(signal, "SIGTERM"):

            signal.signal(
                signal.SIGTERM,
                handle_signal
            )

    except Exception:
        pass

    sys.exit(
        main()
    )