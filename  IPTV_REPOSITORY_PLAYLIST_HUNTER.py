
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV REPOSITORY PLAYLIST HUNTER
FULL MULTITHREADED VERSION

Repository:
    Rafail1082GitHUB/rafail1982.ru

PIPELINE:
    28 workers - playlist downloading
    34 workers - playlist parsing
    45 workers - stream validation

AUDIT:
    HTTP HEAD
    HTTP GET
    Response time
    First data timing
    Redirects
    Content-Type
    Content-Length
    FFprobe
    Video tracks
    Audio tracks
    Subtitle tracks
    Resolution
    FPS
    Codec
    Bitrate

REQUIREMENTS:
    pip install requests

OPTIONAL:
    FFmpeg + FFprobe installed and available in PATH.
"""

import os
import re
import csv
import json
import gzip
import time
import shutil
import logging
import threading
import subprocess
import requests

from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode


# ============================================================
# CONFIGURATION
# ============================================================

OWNER = "Rafail1082GitHUB"
REPOSITORY = "rafail1982.ru"
BRANCH = "main"

GITHUB_API = (
    f"https://api.github.com/repos/"
    f"{OWNER}/{REPOSITORY}/git/trees/{BRANCH}?recursive=1"
)

RAW_BASE = (
    f"https://raw.githubusercontent.com/"
    f"{OWNER}/{REPOSITORY}/{BRANCH}/"
)

PLAYLIST_EXTENSIONS = (
    ".m3u",
    ".m3u8",
    ".m3u.gz",
    ".m3u8.gz",
)

# Independent worker pools
DOWNLOAD_WORKERS = 28
PARSER_WORKERS = 34
CHECK_WORKERS = 45

REQUEST_TIMEOUT = 20
FFPROBE_TIMEOUT = 30
FFPROBE_READ_SECONDS = 8

MAX_PLAYLIST_SIZE = 50 * 1024 * 1024

MAX_STREAMS_PER_PLAYLIST = 0
# 0 = unlimited

GET_SAMPLE_BYTES = 4096

FAST_RESPONSE_MS = 500
NORMAL_RESPONSE_MS = 1500

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()

USER_AGENT = "IPTV-Repository-Playlist-Hunter/3.0"


# ============================================================
# OUTPUT DIRECTORIES
# ============================================================

OUTPUT_DIR = os.path.abspath(
    "IPTV_REPOSITORY_AUDIT"
)

PLAYLIST_DIR = os.path.join(
    OUTPUT_DIR,
    "playlists",
)

DATABASE_DIR = os.path.join(
    OUTPUT_DIR,
    "channel_database",
)

LOG_DIR = os.path.join(
    OUTPUT_DIR,
    "logs",
)

for directory in (
    OUTPUT_DIR,
    PLAYLIST_DIR,
    DATABASE_DIR,
    LOG_DIR,
):
    os.makedirs(directory, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

LOG_FILE = os.path.join(
    LOG_DIR,
    "Stream_Audit.log",
)

AUDIT_JSONL = os.path.join(
    LOG_DIR,
    "Stream_Audit.jsonl",
)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        "%(asctime)s.%(msecs)03d "
        "[%(levelname)s] "
        "[%(threadName)s] "
        "%(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("IPTV_HUNTER")

audit_lock = threading.Lock()


def log_event(event, **data):

    record = {
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "event": event,
        **data,
    }

    with audit_lock:

        with open(
            AUDIT_JSONL,
            "a",
            encoding="utf-8",
        ) as file:

            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    default=str,
                ) + "\n"
            )


# ============================================================
# THREAD-LOCAL HTTP SESSION
# ============================================================

_thread_local = threading.local()


def get_session():

    if not hasattr(
        _thread_local,
        "session",
    ):

        session = requests.Session()

        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "keep-alive",
        })

        if GITHUB_TOKEN:

            session.headers["Authorization"] = (
                f"Bearer {GITHUB_TOKEN}"
            )

        _thread_local.session = session

    return _thread_local.session


# ============================================================
# GENERAL HELPERS
# ============================================================

def now_string():

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def safe_url(url):

    try:

        parsed = urlparse(url)

        hostname = parsed.hostname or ""

        if parsed.port:
            hostname += f":{parsed.port}"

        query = []

        for key, value in parse_qsl(
            parsed.query,
            keep_blank_values=True,
        ):

            if re.search(
                r"token|key|auth|password|passwd|secret|signature|credential",
                key,
                re.IGNORECASE,
            ):

                value = "***"

            query.append((key, value))

        return urlunparse((
            parsed.scheme,
            hostname,
            parsed.path,
            parsed.params,
            urlencode(query),
            "",
        ))

    except Exception:

        return "<URL_REDACTED>"


def safe_int(value):

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def safe_float(value):

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def human_bytes(value):

    size = safe_int(value)

    if size <= 0:
        return "UNKNOWN"

    for unit in ("B", "KB", "MB", "GB", "TB"):

        if size < 1024:
            return f"{size:.2f} {unit}"

        size /= 1024

    return f"{size:.2f} PB"


def bitrate_text(value):

    value = safe_int(value)

    if value <= 0:
        return "UNKNOWN"

    return f"{value / 1000:.0f} kbps"


def quality_from_resolution(width, height):

    width = safe_int(width)
    height = safe_int(height)

    if not width or not height:
        return "UNKNOWN"

    if height >= 2160 or width >= 3840:
        return "4K / UHD"

    if height >= 1440:
        return "QHD"

    if height >= 1080:
        return "FULL HD"

    if height >= 720:
        return "HD"

    if height >= 576:
        return "SD 576"

    if height >= 480:
        return "SD 480"

    return "LOW RESOLUTION"


def response_class(response_ms):

    if response_ms is None:
        return "UNKNOWN"

    if response_ms <= FAST_RESPONSE_MS:
        return "FAST RESPONSE"

    if response_ms <= NORMAL_RESPONSE_MS:
        return "NORMAL RESPONSE"

    return "SLOW RESPONSE"


# ============================================================
# GITHUB REPOSITORY SCANNER
# ============================================================

def github_headers():

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github+json",
    }

    if GITHUB_TOKEN:

        headers["Authorization"] = (
            f"Bearer {GITHUB_TOKEN}"
        )

    return headers


def get_repository_tree():

    logger.info("=" * 75)
    logger.info("GITHUB REPOSITORY SCAN")
    logger.info("OWNER: %s", OWNER)
    logger.info("REPOSITORY: %s", REPOSITORY)
    logger.info("BRANCH: %s", BRANCH)
    logger.info("API: %s", GITHUB_API)
    logger.info("=" * 75)

    started = time.perf_counter()

    response = requests.get(
        GITHUB_API,
        headers=github_headers(),
        timeout=REQUEST_TIMEOUT,
    )

    elapsed = (
        time.perf_counter() - started
    ) * 1000

    logger.info(
        "[GITHUB] HTTP %s | %.2f ms",
        response.status_code,
        elapsed,
    )

    log_event(
        "GITHUB_TREE_REQUEST",
        url=safe_url(GITHUB_API),
        status=response.status_code,
        response_ms=elapsed,
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("truncated"):

        raise RuntimeError(
            "GitHub recursive tree is truncated."
        )

    tree = payload.get("tree", [])

    logger.info(
        "[GITHUB] Tree entries: %s",
        len(tree),
    )

    return tree


def discover_playlists(tree):

    playlists = []

    for item in tree:

        if item.get("type") != "blob":
            continue

        path = item.get("path", "")

        if path.lower().endswith(
            PLAYLIST_EXTENSIONS
        ):

            playlists.append({
                "path": path,
                "size": item.get("size", 0),
                "sha": item.get("sha", ""),
                "url": RAW_BASE + path,
            })

    playlists.sort(
        key=lambda item: item["path"].lower()
    )

    logger.info(
        "[DISCOVERY] Found playlists: %s",
        len(playlists),
    )

    for item in playlists:

        logger.info(
            "[DISCOVERY] %s | size=%s | sha=%s",
            item["path"],
            human_bytes(item["size"]),
            item["sha"],
        )

    log_event(
        "PLAYLIST_DISCOVERY",
        count=len(playlists),
        paths=[
            item["path"]
            for item in playlists
        ],
    )

    return playlists


def save_playlist_index(playlists):

    filename = os.path.join(
        OUTPUT_DIR,
        "All_Playlists.txt",
    )

    with open(
        filename,
        "w",
        encoding="utf-8",
    ) as file:

        file.write(
            f"Repository: {OWNER}/{REPOSITORY}\n"
        )

        file.write(
            f"Branch: {BRANCH}\n"
        )

        file.write(
            f"Generated: {now_string()}\n\n"
        )

        for item in playlists:

            file.write(
                f"{item['path']}\n"
            )

            file.write(
                f"URL: {item['url']}\n"
            )

            file.write(
                f"Size: {item['size']}\n\n"
            )

    logger.info(
        "[OUTPUT] Playlist index: %s",
        filename,
    )


# ============================================================
# PARALLEL PLAYLIST DOWNLOAD
# ============================================================

def download_playlist(item):

    path = item["path"]
    url = item["url"]

    logger.info(
        "[DOWNLOAD START] %s | %s",
        path,
        safe_url(url),
    )

    started = time.perf_counter()

    result = {
        "path": path,
        "url": url,
        "success": False,
        "status_code": None,
        "download_ms": None,
        "size_bytes": 0,
        "channel_count": 0,
        "error": "",
    }

    try:

        response = get_session().get(
            url,
            timeout=REQUEST_TIMEOUT,
        )

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result["status_code"] = (
            response.status_code
        )

        result["download_ms"] = elapsed

        logger.info(
            "[DOWNLOAD HTTP] %s | HTTP=%s | %.2f ms",
            path,
            response.status_code,
            elapsed,
        )

        response.raise_for_status()

        raw_data = response.content

        if len(raw_data) > MAX_PLAYLIST_SIZE:

            raise ValueError(
                "Playlist exceeds MAX_PLAYLIST_SIZE"
            )

        if path.lower().endswith(".gz"):

            raw_data = gzip.decompress(
                raw_data
            )

        text_data = raw_data.decode(
            "utf-8-sig",
            errors="replace",
        )

        local_path = os.path.join(
            PLAYLIST_DIR,
            path,
        )

        os.makedirs(
            os.path.dirname(local_path),
            exist_ok=True,
        )

        with open(
            local_path,
            "w",
            encoding="utf-8",
        ) as file:

            file.write(text_data)

        result["success"] = True
        result["size_bytes"] = len(raw_data)
        result["local_path"] = local_path

        logger.info(
            "[DOWNLOAD OK] %s | %s | %.2f ms",
            path,
            human_bytes(len(raw_data)),
            elapsed,
        )

        log_event(
            "PLAYLIST_DOWNLOAD_OK",
            path=path,
            status=response.status_code,
            download_ms=elapsed,
            size_bytes=len(raw_data),
        )

        return result, text_data

    except Exception as error:

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result["download_ms"] = elapsed
        result["error"] = str(error)

        logger.exception(
            "[DOWNLOAD FAILED] %s",
            path,
        )

        log_event(
            "PLAYLIST_DOWNLOAD_FAILED",
            path=path,
            error=str(error),
            download_ms=elapsed,
        )

        return result, ""


def download_all_playlists(playlists):

    logger.info("=" * 75)

    logger.info(
        "[DOWNLOAD POOL] START | workers=%s | tasks=%s",
        DOWNLOAD_WORKERS,
        len(playlists),
    )

    logger.info("=" * 75)

    downloaded = []
    results = []

    completed = 0
    total = len(playlists)

    with ThreadPoolExecutor(
        max_workers=DOWNLOAD_WORKERS,
        thread_name_prefix="PLAYLIST_DOWNLOAD",
    ) as executor:

        futures = {
            executor.submit(
                download_playlist,
                item,
            ): item

            for item in playlists
        }

        for future in as_completed(futures):

            item = futures[future]

            completed += 1

            try:

                download_result, text_data = (
                    future.result()
                )

            except Exception as error:

                logger.exception(
                    "[DOWNLOAD WORKER ERROR] %s",
                    item["path"],
                )

                download_result = {
                    "path": item["path"],
                    "url": item["url"],
                    "success": False,
                    "error": str(error),
                    "channel_count": 0,
                }

                text_data = ""

            results.append(
                download_result
            )

            if download_result.get("success"):

                downloaded.append({
                    "item": item,
                    "text": text_data,
                    "result": download_result,
                })

            logger.info(
                "[DOWNLOAD PROGRESS] %s/%s | %s | success=%s",
                completed,
                total,
                item["path"],
                download_result.get("success"),
            )

    logger.info(
        "[DOWNLOAD POOL COMPLETE] successful=%s failed=%s",
        sum(
            1 for item in results
            if item.get("success")
        ),
        sum(
            1 for item in results
            if not item.get("success")
        ),
    )

    return downloaded, results


# ============================================================
# M3U PARSER
# ============================================================

URL_PATTERN = re.compile(
    r"^(https?|rtsp|rtmp|udp)://",
    re.IGNORECASE,
)


def parse_extinf(line):

    title = ""
    attributes = {}

    if not line.startswith("#EXTINF"):
        return title, attributes

    if "," in line:

        title = line.split(
            ",",
            1,
        )[1].strip()

    for match in re.finditer(
        r'([\w-]+)="([^"]*)"',
        line,
    ):

        attributes[
            match.group(1)
        ] = match.group(2)

    return title, attributes


def parse_playlist(text_data, playlist_path):

    channels = []

    lines = text_data.splitlines()

    pending_title = ""
    pending_attributes = {}
    pending_extinf = ""

    for line_number, original_line in enumerate(
        lines,
        start=1,
    ):

        line = original_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF"):

            pending_extinf = line

            (
                pending_title,
                pending_attributes,
            ) = parse_extinf(line)

            continue

        if line.startswith("#"):
            continue

        if not URL_PATTERN.match(line):
            continue

        if (
            MAX_STREAMS_PER_PLAYLIST
            and len(channels) >= MAX_STREAMS_PER_PLAYLIST
        ):
            break

        title = (
            pending_title
            or pending_attributes.get("tvg-name")
            or "UNKNOWN CHANNEL"
        )

        channels.append({
            "name": title,
            "url": line,
            "playlist": playlist_path,
            "line": line_number,

            "tvg_id": pending_attributes.get(
                "tvg-id", ""
            ),

            "tvg_name": pending_attributes.get(
                "tvg-name", ""
            ),

            "tvg_logo": pending_attributes.get(
                "tvg-logo", ""
            ),

            "group_title": pending_attributes.get(
                "group-title", ""
            ),

            "language": pending_attributes.get(
                "tvg-language", ""
            ),

            "extinf": pending_extinf,

            "attributes": dict(
                pending_attributes
            ),

            "validation": {},
        })

        pending_title = ""
        pending_attributes = {}
        pending_extinf = ""

    logger.info(
        "[PARSER] %s | lines=%s | channels=%s",
        playlist_path,
        len(lines),
        len(channels),
    )

    log_event(
        "PLAYLIST_PARSED",
        playlist=playlist_path,
        total_lines=len(lines),
        channels=len(channels),
    )

    return channels


# ============================================================
# PARALLEL PLAYLIST PARSING
# ============================================================

def parse_all_playlists(downloaded):

    logger.info("=" * 75)

    logger.info(
        "[PARSER POOL] START | workers=%s | tasks=%s",
        PARSER_WORKERS,
        len(downloaded),
    )

    logger.info("=" * 75)

    all_channels = []

    completed = 0
    total = len(downloaded)

    with ThreadPoolExecutor(
        max_workers=PARSER_WORKERS,
        thread_name_prefix="PLAYLIST_PARSER",
    ) as executor:

        futures = {
            executor.submit(
                parse_playlist,
                item["text"],
                item["item"]["path"],
            ): item

            for item in downloaded
        }

        for future in as_completed(futures):

            item = futures[future]

            completed += 1

            try:

                channels = future.result()

            except Exception as error:

                logger.exception(
                    "[PARSER ERROR] %s",
                    item["item"]["path"],
                )

                channels = []

            item["result"]["channel_count"] = (
                len(channels)
            )

            all_channels.extend(channels)

            logger.info(
                "[PARSER PROGRESS] %s/%s | %s | channels=%s",
                completed,
                total,
                item["item"]["path"],
                len(channels),
            )

    logger.info(
        "[PARSER COMPLETE] Total records=%s",
        len(all_channels),
    )

    return all_channels


# ============================================================
# HTTP HEAD AUDIT
# ============================================================

def check_head(url, channel_name):

    started = time.perf_counter()

    result = {
        "attempted": True,
        "status_code": None,
        "response_ms": None,
        "content_type": "",
        "content_length": "",
        "final_url": "",
        "redirects": [],
        "error": "",
    }

    logger.info(
        "[HEAD START] channel=%s | url=%s",
        channel_name,
        safe_url(url),
    )

    try:

        response = get_session().head(
            url,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result.update({
            "status_code": response.status_code,
            "response_ms": elapsed,
            "content_type": response.headers.get(
                "Content-Type", ""
            ),
            "content_length": response.headers.get(
                "Content-Length", ""
            ),
            "final_url": safe_url(response.url),

            "redirects": [
                {
                    "status": item.status_code,
                    "url": safe_url(item.url),
                    "location": safe_url(
                        item.headers.get("Location", "")
                    ),
                }
                for item in response.history
            ],
        })

        logger.info(
            "[HEAD RESULT] %s | HTTP=%s | %.2f ms | type=%s | size=%s",
            channel_name,
            response.status_code,
            elapsed,
            result["content_type"],
            result["content_length"],
        )

        for redirect in result["redirects"]:

            logger.info(
                "[HEAD REDIRECT] HTTP=%s | %s -> %s",
                redirect["status"],
                redirect["url"],
                redirect["location"],
            )

        log_event(
            "HTTP_HEAD",
            channel=channel_name,
            url=safe_url(url),
            **result,
        )

        response.close()

        return result

    except Exception as error:

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result["response_ms"] = elapsed
        result["error"] = str(error)

        logger.warning(
            "[HEAD ERROR] %s | %.2f ms | %s",
            channel_name,
            elapsed,
            error,
        )

        log_event(
            "HTTP_HEAD_ERROR",
            channel=channel_name,
            url=safe_url(url),
            error=str(error),
            response_ms=elapsed,
        )

        return result


# ============================================================
# HTTP GET AUDIT
# ============================================================

def check_get(url, channel_name):

    started = time.perf_counter()

    result = {
        "attempted": True,
        "status_code": None,
        "headers_ms": None,
        "first_chunk_ms": None,
        "total_ms": None,
        "bytes_read": 0,
        "content_type": "",
        "content_length": "",
        "content_range": "",
        "accept_ranges": "",
        "final_url": "",
        "redirects": [],
        "http_ok": False,
        "error": "",
    }

    logger.info(
        "[GET START] channel=%s | url=%s",
        channel_name,
        safe_url(url),
    )

    try:

        response = get_session().get(
            url,
            headers={
                "Range": (
                    f"bytes=0-{GET_SAMPLE_BYTES - 1}"
                ),
            },
            stream=True,
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )

        headers_ms = (
            time.perf_counter() - started
        ) * 1000

        result.update({
            "status_code": response.status_code,
            "headers_ms": headers_ms,

            "content_type": response.headers.get(
                "Content-Type", ""
            ),

            "content_length": response.headers.get(
                "Content-Length", ""
            ),

            "content_range": response.headers.get(
                "Content-Range", ""
            ),

            "accept_ranges": response.headers.get(
                "Accept-Ranges", ""
            ),

            "final_url": safe_url(response.url),

            "redirects": [
                {
                    "status": item.status_code,
                    "url": safe_url(item.url),
                    "location": safe_url(
                        item.headers.get("Location", "")
                    ),
                }
                for item in response.history
            ],
        })

        logger.info(
            "[GET HEADERS] %s | HTTP=%s | headers=%.2f ms | type=%s",
            channel_name,
            response.status_code,
            headers_ms,
            result["content_type"],
        )

        if response.status_code not in (200, 206):

            response.raise_for_status()

        first_chunk_started = time.perf_counter()

        for chunk in response.iter_content(
            chunk_size=GET_SAMPLE_BYTES
        ):

            if chunk:

                result["bytes_read"] += len(chunk)

                result["first_chunk_ms"] = (
                    time.perf_counter()
                    - first_chunk_started
                ) * 1000

                break

        result["http_ok"] = bool(
            response.status_code in (200, 206)
            and result["bytes_read"] > 0
        )

        result["total_ms"] = (
            time.perf_counter() - started
        ) * 1000

        logger.info(
            "[GET DATA] %s | bytes=%s | first_chunk=%.2f ms | total=%.2f ms | range=%s",
            channel_name,
            result["bytes_read"],
            result["first_chunk_ms"] or 0,
            result["total_ms"],
            result["content_range"],
        )

        for redirect in result["redirects"]:

            logger.info(
                "[GET REDIRECT] HTTP=%s | %s -> %s",
                redirect["status"],
                redirect["url"],
                redirect["location"],
            )

        log_event(
            "HTTP_GET",
            channel=channel_name,
            url=safe_url(url),
            **result,
        )

        response.close()

        return result

    except Exception as error:

        result["total_ms"] = (
            time.perf_counter() - started
        ) * 1000

        result["error"] = str(error)

        logger.warning(
            "[GET ERROR] %s | %.2f ms | %s",
            channel_name,
            result["total_ms"],
            error,
        )

        log_event(
            "HTTP_GET_ERROR",
            channel=channel_name,
            url=safe_url(url),
            error=str(error),
            total_ms=result["total_ms"],
        )

        return result


# ============================================================
# FFPROBE
# ============================================================

def find_ffprobe():

    return shutil.which("ffprobe")


def run_ffprobe(url, channel_name):

    ffprobe = find_ffprobe()

    if not ffprobe:

        logger.warning(
            "[FFPROBE] Not installed."
        )

        return {
            "available": False,
            "success": False,
            "error": "ffprobe not found",
            "video_tracks": [],
            "audio_tracks": [],
            "subtitle_tracks": [],
            "other_tracks": [],
        }

    command = [
        ffprobe,
        "-v", "error",
        "-rw_timeout", "12000000",
        "-read_intervals",
        f"%+{FFPROBE_READ_SECONDS}",
        "-show_streams",
        "-show_format",
        "-of", "json",
        url,
    ]

    logger.info(
        "[FFPROBE START] %s | %s",
        channel_name,
        safe_url(url),
    )

    started = time.perf_counter()

    result = {
        "available": True,
        "success": False,
        "duration": "",
        "format_name": "",
        "format_long_name": "",
        "format_bitrate": 0,
        "video_tracks": [],
        "audio_tracks": [],
        "subtitle_tracks": [],
        "other_tracks": [],
        "error": "",
        "probe_ms": None,
    }

    try:

        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT,
            encoding="utf-8",
            errors="replace",
        )

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result["probe_ms"] = elapsed

        if process.returncode != 0:

            error = (
                process.stderr.strip()
                or f"FFprobe exit code {process.returncode}"
            )

            result["error"] = error

            logger.warning(
                "[FFPROBE FAILED] %s | %.2f ms | %s",
                channel_name,
                elapsed,
                error[:1500],
            )

            log_event(
                "FFPROBE_FAILED",
                channel=channel_name,
                url=safe_url(url),
                probe_ms=elapsed,
                error=error[:3000],
            )

            return result

        data = json.loads(
            process.stdout or "{}"
        )

        media_format = data.get(
            "format",
            {},
        )

        result["format_name"] = (
            media_format.get("format_name", "")
        )

        result["format_long_name"] = (
            media_format.get("format_long_name", "")
        )

        result["duration"] = (
            media_format.get("duration", "")
        )

        result["format_bitrate"] = safe_int(
            media_format.get("bit_rate", 0)
        )

        for stream in data.get("streams", []):

            codec_type = stream.get(
                "codec_type",
                "unknown",
            )

            tags = stream.get(
                "tags",
                {},
            )

            common = {
                "index": stream.get("index", ""),
                "codec_type": codec_type,
                "codec_name": stream.get("codec_name", ""),
                "codec_long_name": stream.get("codec_long_name", ""),
                "profile": stream.get("profile", ""),
                "language": tags.get("language", ""),
                "title": tags.get("title", ""),
                "bitrate": safe_int(
                    stream.get("bit_rate", 0)
                ),
            }

            if codec_type == "video":

                width = safe_int(
                    stream.get("width", 0)
                )

                height = safe_int(
                    stream.get("height", 0)
                )

                fps = (
                    stream.get("avg_frame_rate")
                    or stream.get("r_frame_rate")
                    or ""
                )

                try:

                    if "/" in fps:

                        numerator, denominator = (
                            fps.split("/", 1)
                        )

                        denominator = float(
                            denominator
                        )

                        fps_value = (
                            float(numerator) / denominator
                            if denominator
                            else 0
                        )

                    else:

                        fps_value = safe_float(fps)

                except Exception:

                    fps_value = 0

                track = {
                    **common,

                    "width": width,
                    "height": height,

                    "resolution": (
                        f"{width}x{height}"
                        if width and height
                        else ""
                    ),

                    "quality": quality_from_resolution(
                        width,
                        height,
                    ),

                    "fps": round(
                        fps_value,
                        3,
                    ),

                    "pixel_format": stream.get(
                        "pix_fmt", ""
                    ),

                    "level": stream.get(
                        "level", ""
                    ),

                    "color_space": stream.get(
                        "color_space", ""
                    ),

                    "color_transfer": stream.get(
                        "color_transfer", ""
                    ),

                    "color_primaries": stream.get(
                        "color_primaries", ""
                    ),
                }

                result["video_tracks"].append(track)

                logger.info(
                    "[VIDEO TRACK] %s | index=%s | codec=%s | resolution=%s | quality=%s | FPS=%s | bitrate=%s",
                    channel_name,
                    track["index"],
                    track["codec_name"],
                    track["resolution"],
                    track["quality"],
                    track["fps"],
                    bitrate_text(track["bitrate"]),
                )

            elif codec_type == "audio":

                track = {
                    **common,

                    "sample_rate": stream.get(
                        "sample_rate", ""
                    ),

                    "channels": stream.get(
                        "channels", ""
                    ),

                    "channel_layout": stream.get(
                        "channel_layout", ""
                    ),

                    "sample_format": stream.get(
                        "sample_fmt", ""
                    ),

                    "bits_per_sample": stream.get(
                        "bits_per_sample", ""
                    ),
                }

                result["audio_tracks"].append(track)

                logger.info(
                    "[AUDIO TRACK] %s | index=%s | codec=%s | language=%s | channels=%s | layout=%s | sample_rate=%s | bitrate=%s",
                    channel_name,
                    track["index"],
                    track["codec_name"],
                    track["language"],
                    track["channels"],
                    track["channel_layout"],
                    track["sample_rate"],
                    bitrate_text(track["bitrate"]),
                )

            elif codec_type == "subtitle":

                track = {
                    **common,
                    "subtitle_codec": stream.get(
                        "codec_name", ""
                    ),
                }

                result["subtitle_tracks"].append(track)

                logger.info(
                    "[SUBTITLE TRACK] %s | index=%s | codec=%s | language=%s | title=%s",
                    channel_name,
                    track["index"],
                    track["codec_name"],
                    track["language"],
                    track["title"],
                )

            else:

                result["other_tracks"].append(
                    common
                )

                logger.info(
                    "[OTHER TRACK] %s | index=%s | type=%s | codec=%s",
                    channel_name,
                    common["index"],
                    codec_type,
                    common["codec_name"],
                )

        result["success"] = bool(
            result["video_tracks"]
            or result["audio_tracks"]
        )

        logger.info(
            "[FFPROBE SUMMARY] %s | format=%s | video=%s | audio=%s | subtitles=%s | duration=%s | bitrate=%s | %.2f ms",
            channel_name,
            result["format_name"],
            len(result["video_tracks"]),
            len(result["audio_tracks"]),
            len(result["subtitle_tracks"]),
            result["duration"],
            bitrate_text(result["format_bitrate"]),
            elapsed,
        )

        log_event(
            "FFPROBE_SUCCESS",
            channel=channel_name,
            url=safe_url(url),
            probe_ms=elapsed,
            format=result["format_name"],
            duration=result["duration"],
            format_bitrate=result["format_bitrate"],
            video_tracks=result["video_tracks"],
            audio_tracks=result["audio_tracks"],
            subtitle_tracks=result["subtitle_tracks"],
            success=result["success"],
        )

        return result

    except subprocess.TimeoutExpired:

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result["probe_ms"] = elapsed
        result["error"] = "FFprobe timeout"

        logger.warning(
            "[FFPROBE TIMEOUT] %s | %.2f ms",
            channel_name,
            elapsed,
        )

        log_event(
            "FFPROBE_TIMEOUT",
            channel=channel_name,
            url=safe_url(url),
            probe_ms=elapsed,
        )

        return result

    except Exception as error:

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result["probe_ms"] = elapsed
        result["error"] = str(error)

        logger.exception(
            "[FFPROBE ERROR] %s",
            channel_name,
        )

        log_event(
            "FFPROBE_ERROR",
            channel=channel_name,
            url=safe_url(url),
            error=str(error),
        )

        return result


# ============================================================
# STREAM VALIDATION
# ============================================================

def validate_url(url, channel_name):

    total_started = time.perf_counter()

    logger.info("=" * 75)

    logger.info(
        "[STREAM CHECK START] %s",
        channel_name,
    )

    logger.info(
        "[STREAM URL] %s",
        safe_url(url),
    )

    head = check_head(
        url,
        channel_name,
    )

    get = check_get(
        url,
        channel_name,
    )

    if get["http_ok"]:

        probe = run_ffprobe(
            url,
            channel_name,
        )

    else:

        probe = {
            "available": bool(find_ffprobe()),
            "success": False,
            "error": "Skipped: HTTP GET failed",
            "video_tracks": [],
            "audio_tracks": [],
            "subtitle_tracks": [],
            "other_tracks": [],
        }

        logger.warning(
            "[FFPROBE SKIPPED] %s | GET failed",
            channel_name,
        )

    total_ms = (
        time.perf_counter() - total_started
    ) * 1000

    video_tracks = probe.get(
        "video_tracks",
        [],
    )

    audio_tracks = probe.get(
        "audio_tracks",
        [],
    )

    subtitle_tracks = probe.get(
        "subtitle_tracks",
        [],
    )

    best_video = None

    if video_tracks:

        best_video = max(
            video_tracks,
            key=lambda item: (
                safe_int(item.get("height")),
                safe_int(item.get("width")),
            ),
        )

    verified = bool(
        get["http_ok"]
        and probe.get("success")
        and (video_tracks or audio_tracks)
    )

    response_ms = get.get("headers_ms")

    result = {
        "url": url,

        "verified": verified,

        "status": (
            "VERIFIED"
            if verified
            else "FAILED"
        ),

        "head": head,
        "get": get,
        "probe": probe,

        "total_check_ms": total_ms,

        "response_class": response_class(
            response_ms
        ),

        "response_ms": response_ms,

        "video_track_count": len(video_tracks),
        "audio_track_count": len(audio_tracks),
        "subtitle_track_count": len(subtitle_tracks),

        "video_tracks": video_tracks,
        "audio_tracks": audio_tracks,
        "subtitle_tracks": subtitle_tracks,

        "format_name": probe.get(
            "format_name", ""
        ),

        "format_bitrate": probe.get(
            "format_bitrate", 0
        ),

        "duration": probe.get(
            "duration", ""
        ),

        "resolution": (
            best_video.get("resolution", "")
            if best_video
            else ""
        ),

        "quality": (
            best_video.get("quality", "")
            if best_video
            else (
                "AUDIO ONLY"
                if audio_tracks
                else "UNKNOWN"
            )
        ),

        "fps": (
            best_video.get("fps", 0)
            if best_video
            else 0
        ),

        "video_codec": (
            best_video.get("codec_name", "")
            if best_video
            else ""
        ),

        "audio_codecs": ", ".join(
            sorted({
                item.get("codec_name", "")
                for item in audio_tracks
                if item.get("codec_name")
            })
        ),

        "live_latency": "NOT MEASURED",
    }

    logger.info(
        "[STREAM RESULT] %s | %s | HTTP=%s | RESPONSE=%s | RESOLUTION=%s | QUALITY=%s | VIDEO=%s | AUDIO=%s | SUBTITLES=%s | TOTAL=%.2f ms",
        channel_name,
        result["status"],
        get.get("status_code"),
        result["response_class"],
        result["resolution"] or "UNKNOWN",
        result["quality"],
        result["video_track_count"],
        result["audio_track_count"],
        result["subtitle_track_count"],
        total_ms,
    )

    logger.info(
        "[LIVE LATENCY] %s | NOT MEASURED",
        channel_name,
    )

    log_event(
        "STREAM_VALIDATION_FINISHED",
        channel=channel_name,
        url=safe_url(url),
        verified=verified,
        status=result["status"],
        total_check_ms=total_ms,
        response_class=result["response_class"],
        response_ms=response_ms,
        resolution=result["resolution"],
        quality=result["quality"],
        video_tracks=len(video_tracks),
        audio_tracks=len(audio_tracks),
        subtitle_tracks=len(subtitle_tracks),
        live_latency="NOT MEASURED",
    )

    logger.info(
        "[STREAM CHECK FINISHED] %s",
        channel_name,
    )

    logger.info("=" * 75)

    return result


# ============================================================
# PARALLEL STREAM VALIDATION
# ============================================================

def validate_unique_streams(channels):

    unique_urls = {}

    for channel in channels:

        url = channel["url"]

        if url not in unique_urls:

            unique_urls[url] = {
                "name": channel["name"],
                "url": url,
            }

    logger.info("=" * 75)

    logger.info(
        "[CHECK POOL] START | workers=%s | unique_urls=%s",
        CHECK_WORKERS,
        len(unique_urls),
    )

    logger.info(
        "[CHECK POOL] Total channel records=%s | aliases=%s",
        len(channels),
        len(channels) - len(unique_urls),
    )

    logger.info("=" * 75)

    results = {}

    total = len(unique_urls)
    completed = 0

    with ThreadPoolExecutor(
        max_workers=CHECK_WORKERS,
        thread_name_prefix="STREAM_CHECK",
    ) as executor:

        futures = {
            executor.submit(
                validate_url,
                item["url"],
                item["name"],
            ): item["url"]

            for item in unique_urls.values()
        }

        for future in as_completed(futures):

            url = futures[future]

            completed += 1

            try:

                result = future.result()

            except Exception as error:

                logger.exception(
                    "[CHECK WORKER ERROR] %s",
                    safe_url(url),
                )

                result = {
                    "url": url,
                    "verified": False,
                    "status": "FAILED",
                    "error": str(error),
                    "live_latency": "NOT MEASURED",
                    "video_tracks": [],
                    "audio_tracks": [],
                    "subtitle_tracks": [],
                    "video_track_count": 0,
                    "audio_track_count": 0,
                    "subtitle_track_count": 0,
                }

            results[url] = result

            logger.info(
                "[CHECK PROGRESS] %s/%s | %s | %s",
                completed,
                total,
                result.get("status", "UNKNOWN"),
                safe_url(url),
            )

    # Keep all aliases, attach shared validation result.

    for channel in channels:

        channel["validation"] = results.get(
            channel["url"],
            {},
        )

    logger.info(
        "[CHECK POOL COMPLETE] verified=%s failed=%s",
        sum(
            1 for item in results.values()
            if item.get("verified")
        ),
        sum(
            1 for item in results.values()
            if not item.get("verified")
        ),
    )

    return results


# ============================================================
# DATABASE
# ============================================================

def build_channel_database(channels):

    unique = {}

    for channel in channels:

        url = channel["url"]

        if url not in unique:

            unique[url] = {
                **channel,
                "aliases": [],
            }

        alias = channel["name"]

        if alias not in unique[url]["aliases"]:

            unique[url]["aliases"].append(alias)

    result = list(unique.values())

    logger.info(
        "[DATABASE] Unique streams=%s | records=%s",
        len(result),
        len(channels),
    )

    return result


# ============================================================
# M3U EXPORT
# ============================================================

def export_m3u(channels, filename, mode="all"):

    output_path = os.path.join(
        OUTPUT_DIR,
        filename,
    )

    count = 0

    with open(
        output_path,
        "w",
        encoding="utf-8",
        newline="\n",
    ) as file:

        file.write("#EXTM3U\n")

        for channel in channels:

            validation = (
                channel.get("validation", {})
                or {}
            )

            verified = validation.get(
                "verified",
                False,
            )

            if mode == "verified" and not verified:
                continue

            if mode == "failed" and verified:
                continue

            attributes = channel.get(
                "attributes",
                {},
            )

            attrs = []

            for key in (
                "tvg-id",
                "tvg-name",
                "tvg-logo",
                "group-title",
                "tvg-language",
            ):

                value = attributes.get(key)

                if value:

                    attrs.append(
                        f'{key}="{value}"'
                    )

            attr_text = (
                " " + " ".join(attrs)
                if attrs
                else ""
            )

            file.write(
                f'#EXTINF:-1{attr_text},{channel["name"]}\n'
            )

            file.write(
                f'{channel["url"]}\n'
            )

            count += 1

    logger.info(
        "[EXPORT M3U] %s | records=%s | mode=%s",
        output_path,
        count,
        mode,
    )


# ============================================================
# CSV EXPORT
# ============================================================

def export_csv(channels):

    filename = os.path.join(
        DATABASE_DIR,
        "channels.csv",
    )

    fields = [
        "name",
        "url",
        "aliases",
        "playlist",
        "line",
        "tvg_id",
        "tvg_name",
        "tvg_logo",
        "group_title",
        "language",
        "verified",
        "status",
        "http_status",
        "response_ms",
        "response_class",
        "get_total_ms",
        "bytes_read",
        "content_type",
        "format_name",
        "resolution",
        "quality",
        "fps",
        "video_codec",
        "audio_codecs",
        "video_track_count",
        "audio_track_count",
        "subtitle_track_count",
        "format_bitrate",
        "duration",
        "total_check_ms",
        "live_latency",
    ]

    with open(
        filename,
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fields,
        )

        writer.writeheader()

        for channel in channels:

            validation = (
                channel.get("validation", {})
                or {}
            )

            get = validation.get(
                "get",
                {},
            )

            writer.writerow({
                "name": channel["name"],
                "url": channel["url"],
                "aliases": " | ".join(
                    channel.get("aliases", [])
                ),
                "playlist": channel["playlist"],
                "line": channel["line"],
                "tvg_id": channel["tvg_id"],
                "tvg_name": channel["tvg_name"],
                "tvg_logo": channel["tvg_logo"],
                "group_title": channel["group_title"],
                "language": channel["language"],
                "verified": validation.get("verified", False),
                "status": validation.get("status", "UNKNOWN"),
                "http_status": get.get("status_code", ""),
                "response_ms": validation.get("response_ms", ""),
                "response_class": validation.get("response_class", ""),
                "get_total_ms": get.get("total_ms", ""),
                "bytes_read": get.get("bytes_read", ""),
                "content_type": get.get("content_type", ""),
                "format_name": validation.get("format_name", ""),
                "resolution": validation.get("resolution", ""),
                "quality": validation.get("quality", ""),
                "fps": validation.get("fps", ""),
                "video_codec": validation.get("video_codec", ""),
                "audio_codecs": validation.get("audio_codecs", ""),
                "video_track_count": validation.get("video_track_count", 0),
                "audio_track_count": validation.get("audio_track_count", 0),
                "subtitle_track_count": validation.get("subtitle_track_count", 0),
                "format_bitrate": validation.get("format_bitrate", 0),
                "duration": validation.get("duration", ""),
                "total_check_ms": validation.get("total_check_ms", ""),
                "live_latency": "NOT MEASURED",
            })

    logger.info(
        "[EXPORT CSV] %s",
        filename,
    )


# ============================================================
# JSON EXPORT
# ============================================================

def export_json(channels):

    filename = os.path.join(
        DATABASE_DIR,
        "channels.json",
    )

    export_data = []

    for channel in channels:

        validation = (
            channel.get("validation", {})
            or {}
        )

        export_data.append({
            "name": channel["name"],
            "url": channel["url"],
            "aliases": channel.get("aliases", []),
            "playlist": channel["playlist"],
            "line": channel["line"],
            "tvg_id": channel["tvg_id"],
            "tvg_name": channel["tvg_name"],
            "tvg_logo": channel["tvg_logo"],
            "group_title": channel["group_title"],
            "language": channel["language"],
            "verified": validation.get("verified", False),
            "validation": validation,
        })

    with open(
        filename,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            export_data,
            file,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    logger.info(
        "[EXPORT JSON] %s",
        filename,
    )


# ============================================================
# TRACK REPORT
# ============================================================

def export_track_report(channels):

    filename = os.path.join(
        DATABASE_DIR,
        "Stream_Tracks.csv",
    )

    fields = [
        "channel",
        "url",
        "playlist",
        "track_type",
        "track_index",
        "codec",
        "profile",
        "language",
        "title",
        "resolution",
        "quality",
        "fps",
        "bitrate",
        "sample_rate",
        "channels",
        "channel_layout",
        "pixel_format",
    ]

    with open(
        filename,
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fields,
        )

        writer.writeheader()

        for channel in channels:

            validation = (
                channel.get("validation", {})
                or {}
            )

            for track_type in (
                "video",
                "audio",
                "subtitle",
            ):

                tracks = validation.get(
                    f"{track_type}_tracks",
                    [],
                )

                for track in tracks:

                    writer.writerow({
                        "channel": channel["name"],
                        "url": channel["url"],
                        "playlist": channel["playlist"],
                        "track_type": track_type,
                        "track_index": track.get("index", ""),
                        "codec": track.get("codec_name", ""),
                        "profile": track.get("profile", ""),
                        "language": track.get("language", ""),
                        "title": track.get("title", ""),
                        "resolution": track.get("resolution", ""),
                        "quality": track.get("quality", ""),
                        "fps": track.get("fps", ""),
                        "bitrate": track.get("bitrate", ""),
                        "sample_rate": track.get("sample_rate", ""),
                        "channels": track.get("channels", ""),
                        "channel_layout": track.get("channel_layout", ""),
                        "pixel_format": track.get("pixel_format", ""),
                    })

    logger.info(
        "[EXPORT TRACK REPORT] %s",
        filename,
    )


# ============================================================
# PLAYLIST REPORT
# ============================================================

def export_playlist_report(playlist_results):

    filename = os.path.join(
        OUTPUT_DIR,
        "Playlist_Report.csv",
    )

    fields = [
        "playlist",
        "url",
        "download_success",
        "http_status",
        "download_ms",
        "size_bytes",
        "channel_count",
        "error",
    ]

    with open(
        filename,
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fields,
        )

        writer.writeheader()

        for item in playlist_results:

            writer.writerow({
                "playlist": item.get("path", ""),
                "url": item.get("url", ""),
                "download_success": item.get("success", False),
                "http_status": item.get("status_code", ""),
                "download_ms": item.get("download_ms", ""),
                "size_bytes": item.get("size_bytes", ""),
                "channel_count": item.get("channel_count", 0),
                "error": item.get("error", ""),
            })

    logger.info(
        "[EXPORT PLAYLIST REPORT] %s",
        filename,
    )


# ============================================================
# FULL CHECK REPORT
# ============================================================

def export_check_report(
    channels,
    unique_results,
    playlist_results,
):

    filename = os.path.join(
        OUTPUT_DIR,
        "Check_Report.txt",
    )

    total_records = len(channels)
    total_unique = len(unique_results)

    verified = sum(
        1 for result in unique_results.values()
        if result.get("verified")
    )

    failed = total_unique - verified

    video_streams = sum(
        1 for result in unique_results.values()
        if result.get("video_track_count", 0)
    )

    audio_streams = sum(
        1 for result in unique_results.values()
        if result.get("audio_track_count", 0)
    )

    subtitle_streams = sum(
        1 for result in unique_results.values()
        if result.get("subtitle_track_count", 0)
    )

    fast = sum(
        1 for result in unique_results.values()
        if result.get("response_class") == "FAST RESPONSE"
    )

    normal = sum(
        1 for result in unique_results.values()
        if result.get("response_class") == "NORMAL RESPONSE"
    )

    slow = sum(
        1 for result in unique_results.values()
        if result.get("response_class") == "SLOW RESPONSE"
    )

    playlist_ok = sum(
        1 for item in playlist_results
        if item.get("success")
    )

    playlist_failed = (
        len(playlist_results) - playlist_ok
    )

    with open(
        filename,
        "w",
        encoding="utf-8",
    ) as file:

        file.write("=" * 75 + "\n")
        file.write("IPTV REPOSITORY PLAYLIST HUNTER\n")
        file.write("DETAILED AUDIT REPORT\n")
        file.write("=" * 75 + "\n\n")

        file.write(
            f"Generated: {now_string()}\n"
        )

        file.write(
            f"Repository: {OWNER}/{REPOSITORY}\n"
        )

        file.write(
            f"Branch: {BRANCH}\n\n"
        )

        file.write("--- MULTITHREADING ---\n")

        file.write(
            f"Download workers: {DOWNLOAD_WORKERS}\n"
        )

        file.write(
            f"Parser workers: {PARSER_WORKERS}\n"
        )

        file.write(
            f"Check workers: {CHECK_WORKERS}\n\n"
        )

        file.write("--- PLAYLISTS ---\n")

        file.write(
            f"Discovered: {len(playlist_results)}\n"
        )

        file.write(
            f"Downloaded: {playlist_ok}\n"
        )

        file.write(
            f"Download failed: {playlist_failed}\n\n"
        )

        file.write("--- CHANNELS ---\n")

        file.write(
            f"Total channel records: {total_records}\n"
        )

        file.write(
            f"Unique stream URLs: {total_unique}\n"
        )

        file.write(
            f"Duplicate URL aliases: {total_records - total_unique}\n"
        )

        file.write(
            f"Verified streams: {verified}\n"
        )

        file.write(
            f"Failed streams: {failed}\n\n"
        )

        file.write("--- HTTP RESPONSE CLASSIFICATION ---\n")

        file.write(
            f"Fast response (<= {FAST_RESPONSE_MS} ms): {fast}\n"
        )

        file.write(
            f"Normal response (<= {NORMAL_RESPONSE_MS} ms): {normal}\n"
        )

        file.write(
            f"Slow response (> {NORMAL_RESPONSE_MS} ms): {slow}\n\n"
        )

        file.write("--- MEDIA TRACKS ---\n")

        file.write(
            f"Streams with video: {video_streams}\n"
        )

        file.write(
            f"Streams with audio: {audio_streams}\n"
        )

        file.write(
            f"Streams with subtitles: {subtitle_streams}\n\n"
        )

        file.write("--- STREAM DETAILS ---\n\n")

        for url, result in unique_results.items():

            file.write(
                f"URL: {safe_url(url)}\n"
            )

            file.write(
                f"Status: {result.get('status', 'UNKNOWN')}\n"
            )

            file.write(
                f"Verified: {result.get('verified', False)}\n"
            )

            file.write(
                f"HTTP response: {result.get('response_ms', '')} ms\n"
            )

            file.write(
                f"Response class: {result.get('response_class', '')}\n"
            )

            file.write(
                f"Resolution: {result.get('resolution', '')}\n"
            )

            file.write(
                f"Quality: {result.get('quality', '')}\n"
            )

            file.write(
                f"FPS: {result.get('fps', '')}\n"
            )

            file.write(
                f"Video codec: {result.get('video_codec', '')}\n"
            )

            file.write(
                f"Audio codecs: {result.get('audio_codecs', '')}\n"
            )

            file.write(
                f"Video tracks: {result.get('video_track_count', 0)}\n"
            )

            file.write(
                f"Audio tracks: {result.get('audio_track_count', 0)}\n"
            )

            file.write(
                f"Subtitle tracks: {result.get('subtitle_track_count', 0)}\n"
            )

            file.write(
                f"Total check: {result.get('total_check_ms', '')} ms\n"
            )

            file.write(
                "Live broadcast latency: NOT MEASURED\n"
            )

            file.write("-" * 75 + "\n")

        file.write("\n")
        file.write("--- IMPORTANT ---\n")

        file.write(
            "HTTP response time is not broadcast latency.\n"
        )

        file.write(
            "Fast HTTP response does not prove zero live delay.\n"
        )

        file.write(
            "True live-edge latency requires an independent reference timestamp.\n"
        )

    logger.info(
        "[EXPORT CHECK REPORT] %s",
        filename,
    )


# ============================================================
# FINAL STATISTICS
# ============================================================

def print_final_statistics(
    channels,
    unique_results,
    playlist_results,
):

    total = len(unique_results)

    verified = sum(
        1 for item in unique_results.values()
        if item.get("verified")
    )

    failed = total - verified

    logger.info("")
    logger.info("=" * 75)
    logger.info("FINAL IPTV AUDIT STATISTICS")
    logger.info("=" * 75)

    logger.info(
        "Repository: %s/%s",
        OWNER,
        REPOSITORY,
    )

    logger.info(
        "Download workers: %s",
        DOWNLOAD_WORKERS,
    )

    logger.info(
        "Parser workers: %s",
        PARSER_WORKERS,
    )

    logger.info(
        "Check workers: %s",
        CHECK_WORKERS,
    )

    logger.info(
        "Playlists discovered: %s",
        len(playlist_results),
    )

    logger.info(
        "Channel records: %s",
        len(channels),
    )

    logger.info(
        "Unique stream URLs: %s",
        total,
    )

    logger.info(
        "Verified streams: %s",
        verified,
    )

    logger.info(
        "Failed streams: %s",
        failed,
    )

    logger.info(
        "Success rate: %.2f%%",
        (verified / total * 100) if total else 0,
    )

    logger.info("=" * 75)

    log_event(
        "FINAL_STATISTICS",
        playlists=len(playlist_results),
        channel_records=len(channels),
        unique_streams=total,
        verified=verified,
        failed=failed,
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    started = time.perf_counter()

    logger.info("")
    logger.info("#" * 75)
    logger.info("IPTV REPOSITORY PLAYLIST HUNTER STARTED")
    logger.info("#" * 75)

    logger.info(
        "Repository: %s/%s",
        OWNER,
        REPOSITORY,
    )

    logger.info(
        "Branch: %s",
        BRANCH,
    )

    logger.info(
        "DOWNLOAD WORKERS: %s",
        DOWNLOAD_WORKERS,
    )

    logger.info(
        "PARSER WORKERS: %s",
        PARSER_WORKERS,
    )

    logger.info(
        "CHECK WORKERS: %s",
        CHECK_WORKERS,
    )

    logger.info(
        "FFprobe: %s",
        find_ffprobe() or "NOT INSTALLED",
    )

    log_event(
        "RUN_STARTED",
        owner=OWNER,
        repository=REPOSITORY,
        branch=BRANCH,
        download_workers=DOWNLOAD_WORKERS,
        parser_workers=PARSER_WORKERS,
        check_workers=CHECK_WORKERS,
        ffprobe=find_ffprobe(),
    )

    # --------------------------------------------------------
    # STEP 1: GITHUB TREE
    # --------------------------------------------------------

    logger.info("[MAIN] STEP 1: GITHUB TREE")

    tree = get_repository_tree()

    # --------------------------------------------------------
    # STEP 2: PLAYLIST DISCOVERY
    # --------------------------------------------------------

    logger.info("[MAIN] STEP 2: PLAYLIST DISCOVERY")

    playlists = discover_playlists(tree)

    save_playlist_index(playlists)

    if not playlists:

        logger.warning(
            "[MAIN] No playlists found."
        )

        return

    # --------------------------------------------------------
    # STEP 3: PARALLEL DOWNLOAD - 28 WORKERS
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 3: PARALLEL DOWNLOAD | workers=%s",
        DOWNLOAD_WORKERS,
    )

    downloaded, playlist_results = (
        download_all_playlists(playlists)
    )

    export_playlist_report(
        playlist_results
    )

    if not downloaded:

        logger.warning(
            "[MAIN] No playlists downloaded successfully."
        )

        export_check_report(
            [],
            {},
            playlist_results,
        )

        return

    # --------------------------------------------------------
    # STEP 4: PARALLEL PARSING - 34 WORKERS
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 4: PARALLEL PARSING | workers=%s",
        PARSER_WORKERS,
    )

    all_channels = parse_all_playlists(
        downloaded
    )

    logger.info(
        "[MAIN] Channel records collected: %s",
        len(all_channels),
    )

    if not all_channels:

        logger.warning(
            "[MAIN] No channel entries found."
        )

        export_check_report(
            [],
            {},
            playlist_results,
        )

        return

    # --------------------------------------------------------
    # STEP 5: PARALLEL STREAM CHECK - 45 WORKERS
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 5: STREAM VALIDATION | workers=%s",
        CHECK_WORKERS,
    )

    unique_results = validate_unique_streams(
        all_channels
    )

    # --------------------------------------------------------
    # STEP 6: BUILD UNIQUE DATABASE
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 6: BUILD CHANNEL DATABASE"
    )

    unique_channels = build_channel_database(
        all_channels
    )

    # --------------------------------------------------------
    # STEP 7: M3U EXPORT
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 7: EXPORT M3U"
    )

    export_m3u(
        all_channels,
        "All_Channels.m3u",
        mode="all",
    )

    export_m3u(
        all_channels,
        "Verified_Channels.m3u",
        mode="verified",
    )

    export_m3u(
        all_channels,
        "Failed_Channels.m3u",
        mode="failed",
    )

    # --------------------------------------------------------
    # STEP 8: DATABASE EXPORT
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 8: EXPORT DATABASE"
    )

    export_csv(
        unique_channels
    )

    export_json(
        unique_channels
    )

    # --------------------------------------------------------
    # STEP 9: TRACK REPORT
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 9: EXPORT TRACK REPORT"
    )

    export_track_report(
        unique_channels
    )

    # --------------------------------------------------------
    # STEP 10: FULL REPORT
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 10: EXPORT AUDIT REPORT"
    )

    export_check_report(
        all_channels,
        unique_results,
        playlist_results,
    )

    # --------------------------------------------------------
    # STEP 11: STATISTICS
    # --------------------------------------------------------

    print_final_statistics(
        all_channels,
        unique_results,
        playlist_results,
    )

    # --------------------------------------------------------
    # FINISH
    # --------------------------------------------------------

    elapsed = (
        time.perf_counter() - started
    )

    logger.info("")
    logger.info("#" * 75)

    logger.info(
        "IPTV REPOSITORY PLAYLIST HUNTER FINISHED"
    )

    logger.info(
        "Total execution time: %.2f seconds",
        elapsed,
    )

    logger.info(
        "Output directory: %s",
        OUTPUT_DIR,
    )

    logger.info(
        "Detailed log: %s",
        LOG_FILE,
    )

    logger.info(
        "Structured audit: %s",
        AUDIT_JSONL,
    )

    logger.info("#" * 75)

    log_event(
        "RUN_FINISHED",
        execution_seconds=elapsed,
        output_directory=OUTPUT_DIR,
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        logger.warning(
            "[MAIN] Interrupted by user."
        )

    except Exception as error:

        logger.exception(
            "[FATAL ERROR] %s",
            error,
        )

        log_event(
            "FATAL_ERROR",
            error=str(error),
        )

        raise
