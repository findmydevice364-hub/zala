
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV REPOSITORY PLAYLIST HUNTER
EXTENDED MULTITHREADED STREAM AUDITOR

Repository:
    Rafail1082GitHUB/rafail1982.ru

WORKERS:
    28 - playlist downloads
    34 - playlist parsing
    45 - unique stream validation

MAIN ANALYZER:
    FFprobe

INDEPENDENT DIAGNOSTICS:
    HTTP HEAD / GET
    HLS manifest and segment analysis
    TSDuck / tsanalyze
    TShark (local capture only, opt-in)
    PyAV

INSTALLATION:
    requests
    Optional: ffmpeg/ffprobe, TSDuck, TShark, PyAV

IMPORTANT:
    HTTP response time is not live broadcast latency.
    TShark cannot measure packet loss on an arbitrary remote URL.
    TSDuck and PyAV failures do not invalidate FFprobe results.
"""

import os
import re
import io
import csv
import sys
import json
import gzip
import time
import math
import shutil
import socket
import logging
import threading
import subprocess
import platform
import tempfile
import zipfile
import tarfile
import hashlib
import importlib.util
import requests

from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import (
    urlparse,
    urlunparse,
    parse_qsl,
    urlencode,
    urljoin,
)

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

DOWNLOAD_WORKERS = 28
PARSER_WORKERS = 34
CHECK_WORKERS = 45

REQUEST_TIMEOUT = 20
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 20

FFPROBE_TIMEOUT = 35
FFPROBE_READ_SECONDS = 8
FFPROBE_PACKET_COUNT = 300

MAX_PLAYLIST_SIZE = 50 * 1024 * 1024
MAX_DECOMPRESSED_SIZE = 100 * 1024 * 1024

MAX_STREAMS_PER_PLAYLIST = 0
GET_SAMPLE_BYTES = 4096

FAST_RESPONSE_MS = 500
NORMAL_RESPONSE_MS = 1500

HLS_SEGMENT_SAMPLE = 3
HLS_MAX_SEGMENT_BYTES = 2 * 1024 * 1024

# Automatic installation can use apt, brew or winget.
# Set IPTV_AUTO_INSTALL=0 to disable it.
AUTO_INSTALL_TOOLS = (
    os.getenv("IPTV_AUTO_INSTALL", "1").lower()
    not in ("0", "false", "no")
)

# TShark capture is disabled by default.
# It requires local visibility of the traffic and often privileges.
TSHARK_CAPTURE_ENABLED = (
    os.getenv("IPTV_TSHARK_CAPTURE", "0").lower()
    in ("1", "true", "yes")
)

TSHARK_INTERFACE = os.getenv(
    "IPTV_TSHARK_INTERFACE",
    "any",
)

TSHARK_CAPTURE_SECONDS = 8

GITHUB_TOKEN = os.getenv(
    "GITHUB_TOKEN",
    "",
).strip()

USER_AGENT = "IPTV-Repository-Playlist-Hunter/4.0"

HTTP_SCHEMES = ("http", "https")
SUPPORTED_SCHEMES = (
    "http",
    "https",
    "rtsp",
    "rtmp",
    "rtmps",
    "udp",
    "srt",
    "rist",
    "mmsh",
)

SENSITIVE_QUERY = re.compile(
    r"token|key|auth|password|passwd|secret|signature|credential|"
    r"session|jwt|access",
    re.IGNORECASE,
)

# ============================================================
# DIRECTORIES
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

TOOLS_DIR = os.path.join(
    OUTPUT_DIR,
    "tools",
)

for directory in (
    OUTPUT_DIR,
    PLAYLIST_DIR,
    DATABASE_DIR,
    LOG_DIR,
    TOOLS_DIR,
):
    os.makedirs(directory, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR,
    "Stream_Audit.log",
)

AUDIT_JSONL = os.path.join(
    LOG_DIR,
    "Stream_Audit.jsonl",
)

# ============================================================
# LOGGING
# ============================================================

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
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("IPTV_HUNTER")

audit_lock = threading.Lock()
tool_lock = threading.Lock()
_thread_local = threading.local()


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
# HTTP SESSIONS
# ============================================================

def get_session(github=False):
    """
    Separate sessions prevent GitHub credentials from leaking
    into requests to arbitrary IPTV hosts.
    """

    attr = (
        "github_session"
        if github
        else "public_session"
    )

    if not hasattr(_thread_local, attr):
        session = requests.Session()

        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        })

        if github and GITHUB_TOKEN:
            session.headers["Authorization"] = (
                f"Bearer {GITHUB_TOKEN}"
            )

        setattr(_thread_local, attr, session)

    return getattr(_thread_local, attr)


# ============================================================
# GENERAL HELPERS
# ============================================================

def now_string():
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def safe_url(url):
    try:
        parsed = urlparse(str(url))

        hostname = parsed.hostname or ""

        if parsed.port:
            hostname += f":{parsed.port}"

        query = []

        for key, value in parse_qsl(
            parsed.query,
            keep_blank_values=True,
        ):
            if SENSITIVE_QUERY.search(key):
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
    except (TypeError, ValueError, OverflowError):
        return 0


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0


def human_bytes(value):
    size = safe_int(value)

    if size < 0:
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


def is_http_url(url):
    return urlparse(url).scheme.lower() in HTTP_SCHEMES


def is_hls_url(url, content_type=""):
    path = urlparse(url).path.lower()
    content_type = (content_type or "").lower()

    return (
        ".m3u8" in path
        or "application/vnd.apple.mpegurl" in content_type
        or "application/x-mpegurl" in content_type
    )


def parse_fraction(value):
    if not value:
        return 0.0

    try:
        if "/" in str(value):
            numerator, denominator = str(value).split("/", 1)
            denominator = float(denominator)

            if denominator == 0:
                return 0.0

            return float(numerator) / denominator

        return float(value)

    except (ValueError, TypeError, ZeroDivisionError):
        return 0.0


def classify_protocol(url):
    scheme = urlparse(url).scheme.lower()

    if scheme in HTTP_SCHEMES:
        return "HTTP"

    if scheme in ("rtsp",):
        return "RTSP"

    if scheme in ("rtmp", "rtmps"):
        return "RTMP"

    if scheme == "udp":
        return "UDP"

    if scheme == "srt":
        return "SRT"

    if scheme == "rist":
        return "RIST"

    return scheme.upper() or "UNKNOWN"


def csv_safe(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        )

    return value


# ============================================================
# TOOL DISCOVERY AND OPTIONAL INSTALLATION
# ============================================================

TOOL_PATHS = {}


def find_tool(name):
    if name in TOOL_PATHS:
        path = TOOL_PATHS[name]

        if path and os.path.isfile(path):
            return path

    found = shutil.which(name)

    if found:
        TOOL_PATHS[name] = found
        return found

    local_candidates = [
        os.path.join(TOOLS_DIR, name),
        os.path.join(TOOLS_DIR, name + ".exe"),
        os.path.join(TOOLS_DIR, "bin", name),
        os.path.join(TOOLS_DIR, "bin", name + ".exe"),
    ]

    for candidate in local_candidates:
        if os.path.isfile(candidate):
            TOOL_PATHS[name] = candidate
            return candidate

    return None


def run_install_command(command, label):
    logger.info(
        "[INSTALL] Attempting %s: %s",
        label,
        " ".join(command),
    )

    try:
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=600,
            encoding="utf-8",
            errors="replace",
        )

        logger.info(
            "[INSTALL] %s | exit=%s",
            label,
            process.returncode,
        )

        if process.stdout:
            logger.debug(
                "[INSTALL STDOUT] %s",
                process.stdout[-4000:],
            )

        if process.stderr:
            logger.debug(
                "[INSTALL STDERR] %s",
                process.stderr[-4000:],
            )

        return process.returncode == 0

    except Exception as error:
        logger.warning(
            "[INSTALL ERROR] %s | %s",
            label,
            error,
        )
        return False


def safe_extract_tar(archive_path, destination):
    """
    Reject absolute paths, parent traversal, links and special files.
    """

    destination_real = os.path.realpath(destination)

    with tarfile.open(
        archive_path,
        "r:*",
    ) as archive:

        for member in archive.getmembers():
            name = member.name.replace("\\", "/")

            if name.startswith("/"):
                raise ValueError(
                    f"Unsafe absolute archive path: {name}"
                )

            if ".." in name.split("/"):
                raise ValueError(
                    f"Unsafe archive traversal: {name}"
                )

            if member.issym() or member.islnk():
                raise ValueError(
                    f"Archive link rejected: {name}"
                )

            if not (
                member.isfile()
                or member.isdir()
            ):
                continue

            target = os.path.realpath(
                os.path.join(destination, name)
            )

            if not (
                target == destination_real
                or target.startswith(
                    destination_real + os.sep
                )
            ):
                raise ValueError(
                    f"Archive target outside destination: {name}"
                )

        archive.extractall(destination)


def download_ffprobe_bundle():
    """
    Best-effort FFmpeg portable download.
    Asset names vary by platform and upstream release.
    """

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "linux":
        if machine in ("x86_64", "amd64"):
            asset_name = "linux64-gpl.tar.xz"
        elif machine in ("aarch64", "arm64"):
            asset_name = "linuxarm64-gpl.tar.xz"
        else:
            logger.warning(
                "[FFPROBE INSTALL] Unsupported Linux architecture: %s",
                machine,
            )
            return False

        archive_type = "tar"

    elif system == "windows":
        if machine not in ("amd64", "x86_64"):
            return False

        asset_name = "win64-gpl.zip"
        archive_type = "zip"

    else:
        logger.warning(
            "[FFPROBE INSTALL] Automatic portable download "
            "is not configured for %s.",
            system,
        )
        return False

    api_url = (
        "https://api.github.com/repos/"
        "BtbN/FFmpeg-Builds/releases/latest"
    )

    try:
        response = get_session(github=True).get(
            api_url,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        release = response.json()
        assets = release.get("assets", [])

        selected = None

        for asset in assets:
            name = asset.get("name", "").lower()

            if (
                asset_name in name
                and name.endswith(
                    (".tar.xz", ".zip")
                )
            ):
                selected = asset
                break

        if not selected:
            logger.warning(
                "[FFPROBE INSTALL] Compatible release asset not found."
            )
            return False

        download_url = selected.get(
            "browser_download_url",
            "",
        )

        if not download_url:
            return False

        archive_path = os.path.join(
            TOOLS_DIR,
            selected["name"],
        )

        logger.info(
            "[FFPROBE INSTALL] Downloading %s",
            selected["name"],
        )

        with get_session().get(
            download_url,
            stream=True,
            timeout=(CONNECT_TIMEOUT, 60),
        ) as download:

            download.raise_for_status()

            with open(archive_path, "wb") as output:
                for chunk in download.iter_content(
                    1024 * 1024
                ):
                    if chunk:
                        output.write(chunk)

        expected_size = safe_int(
            selected.get("size")
        )

        actual_size = os.path.getsize(
            archive_path
        )

        if expected_size and actual_size != expected_size:
            raise ValueError(
                "Downloaded archive size does not match GitHub metadata."
            )

        extract_dir = os.path.join(
            TOOLS_DIR,
            "ffmpeg_bundle",
        )

        os.makedirs(
            extract_dir,
            exist_ok=True,
        )

        if archive_type == "tar":
            safe_extract_tar(
                archive_path,
                extract_dir,
            )

        else:
            with zipfile.ZipFile(archive_path) as archive:
                for member in archive.infolist():
                    name = member.filename.replace("\\", "/")

                    if (
                        name.startswith("/")
                        or ".." in name.split("/")
                    ):
                        raise ValueError(
                            f"Unsafe ZIP member: {name}"
                        )

                archive.extractall(extract_dir)

        executable_name = (
            "ffprobe.exe"
            if system == "windows"
            else "ffprobe"
        )

        found = None

        for root, _, files in os.walk(extract_dir):
            if executable_name in files:
                found = os.path.join(
                    root,
                    executable_name,
                )
                break

        if not found:
            raise RuntimeError(
                "ffprobe executable not found after extraction."
            )

        if system != "windows":
            os.chmod(
                found,
                0o755,
            )

        TOOL_PATHS["ffprobe"] = found

        logger.info(
            "[FFPROBE INSTALL] Installed locally: %s",
            found,
        )

        return True

    except Exception:
        logger.exception(
            "[FFPROBE INSTALL] Portable installation failed."
        )
        return False


def install_optional_tools():
    if not AUTO_INSTALL_TOOLS:
        logger.info(
            "[TOOLS] Automatic installation disabled."
        )
        return

    with tool_lock:
        system = platform.system().lower()

        missing = []

        for name in (
            "ffprobe",
            "tsanalyze",
            "tshark",
        ):
            if not find_tool(name):
                missing.append(name)

        if not missing:
            logger.info(
                "[TOOLS] All external tools are available."
            )
            return

        logger.info(
            "[TOOLS] Missing tools: %s",
            ", ".join(missing),
        )

        if system == "linux":
            apt = shutil.which("apt-get")

            if apt:
                prefix = []

                if (
                    hasattr(os, "geteuid")
                    and os.geteuid() != 0
                ):
                    sudo = shutil.which("sudo")

                    if sudo:
                        prefix = [sudo]
                    else:
                        logger.warning(
                            "[TOOLS] apt-get requires root or sudo."
                        )

                if not prefix and (
                    not hasattr(os, "geteuid")
                    or os.geteuid() == 0
                ):
                    prefix = []

                if prefix or (
                    hasattr(os, "geteuid")
                    and os.geteuid() == 0
                ):
                    run_install_command(
                        prefix + [apt, "update", "-qq"],
                        "apt update",
                    )

                    packages = []

                    if "ffprobe" in missing:
                        packages.append("ffmpeg")

                    if "tsanalyze" in missing:
                        packages.append("tstools")

                    if "tshark" in missing:
                        packages.append("tshark")

                    if packages:
                        run_install_command(
                            prefix + [
                                apt,
                                "install",
                                "-y",
                                *packages,
                            ],
                            "apt packages",
                        )

        elif system == "darwin":
            brew = shutil.which("brew")

            if brew:
                packages = []

                if "ffprobe" in missing:
                    packages.append("ffmpeg")

                if "tsanalyze" in missing:
                    packages.append("tstools")

                if "tshark" in missing:
                    packages.append("wireshark")

                if packages:
                    run_install_command(
                        [brew, "install", *packages],
                        "Homebrew",
                    )

        elif system == "windows":
            winget = shutil.which("winget")

            if winget:
                if "ffprobe" in missing:
                    run_install_command(
                        [
                            winget,
                            "install",
                            "--id",
                            "Gyan.FFmpeg",
                            "-e",
                            "--silent",
                            "--accept-package-agreements",
                            "--accept-source-agreements",
                        ],
                        "FFmpeg",
                    )

                if "tshark" in missing:
                    run_install_command(
                        [
                            winget,
                            "install",
                            "--id",
                            "WiresharkFoundation.Wireshark",
                            "-e",
                            "--silent",
                            "--accept-package-agreements",
                            "--accept-source-agreements",
                        ],
                        "Wireshark",
                    )

        if not find_tool("ffprobe"):
            download_ffprobe_bundle()

        logger.info(
            "[TOOLS] Installation attempt finished. "
            "Some tools may still be unavailable."
        )


def tool_inventory():
    inventory = {}

    for name in (
        "ffprobe",
        "ffmpeg",
        "tsanalyze",
        "tshark",
    ):
        inventory[name] = find_tool(name) or ""

    inventory["pyav"] = bool(
        importlib.util.find_spec("av")
    )

    for name, path in inventory.items():
        logger.info(
            "[TOOL INVENTORY] %s=%s",
            name,
            path or (
                "AVAILABLE"
                if path is True
                else "NOT INSTALLED"
            ),
        )

    log_event(
        "TOOL_INVENTORY",
        tools=inventory,
    )

    return inventory


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
    logger.info("=" * 75)

    started = time.perf_counter()

    response = requests.get(
        GITHUB_API,
        headers=github_headers(),
        timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT),
    )

    elapsed = (
        time.perf_counter() - started
    ) * 1000

    logger.info(
        "[GITHUB] HTTP=%s | %.2f ms",
        response.status_code,
        elapsed,
    )

    logger.debug(
        "[GITHUB HEADERS] %s",
        dict(response.headers),
    )

    log_event(
        "GITHUB_TREE_REQUEST",
        url=safe_url(GITHUB_API),
        status=response.status_code,
        response_ms=elapsed,
        headers=dict(response.headers),
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
                f"Size: {item['size']}\n"
            )
            file.write(
                f"SHA: {item['sha']}\n\n"
            )

    logger.info(
        "[OUTPUT] Playlist index: %s",
        filename,
    )


# ============================================================
# SAFE PLAYLIST DOWNLOAD
# ============================================================

def read_bounded_gzip(data, max_size):
    """
    Bounded decompression prevents gzip bombs from allocating
    unlimited memory.
    """

    output = io.BytesIO()

    with gzip.GzipFile(
        fileobj=io.BytesIO(data),
        mode="rb",
    ) as compressed:

        total = 0

        while True:
            chunk = compressed.read(
                min(1024 * 1024, max_size + 1 - total)
            )

            if not chunk:
                break

            total += len(chunk)

            if total > max_size:
                raise ValueError(
                    "Decompressed playlist exceeds size limit."
                )

            output.write(chunk)

    return output.getvalue()


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
        "headers": {},
        "redirects": [],
    }

    response = None

    try:
        response = get_session().get(
            url,
            stream=True,
            allow_redirects=True,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )

        result["status_code"] = response.status_code
        result["headers"] = dict(response.headers)

        result["redirects"] = [
            {
                "status": redirect.status_code,
                "url": safe_url(redirect.url),
                "location": safe_url(
                    redirect.headers.get("Location", "")
                ),
            }
            for redirect in response.history
        ]

        logger.info(
            "[DOWNLOAD HTTP] %s | HTTP=%s | type=%s | length=%s",
            path,
            response.status_code,
            response.headers.get("Content-Type", ""),
            response.headers.get("Content-Length", ""),
        )

        logger.debug(
            "[DOWNLOAD HEADERS] %s | %s",
            path,
            dict(response.headers),
        )

        response.raise_for_status()

        raw = bytearray()

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):
            if not chunk:
                continue

            raw.extend(chunk)

            if len(raw) > MAX_PLAYLIST_SIZE:
                raise ValueError(
                    "Compressed playlist exceeds MAX_PLAYLIST_SIZE."
                )

        raw_data = bytes(raw)

        if path.lower().endswith(".gz"):
            raw_data = read_bounded_gzip(
                raw_data,
                MAX_DECOMPRESSED_SIZE,
            )

        elif len(raw_data) > MAX_DECOMPRESSED_SIZE:
            raise ValueError(
                "Playlist exceeds decompressed size limit."
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

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result.update({
            "success": True,
            "download_ms": elapsed,
            "size_bytes": len(raw_data),
            "local_path": local_path,
            "compressed_size_bytes": len(raw),
            "final_url": safe_url(response.url),
        })

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
            compressed_size_bytes=len(raw),
            headers=dict(response.headers),
            redirects=result["redirects"],
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

    finally:
        if response is not None:
            response.close()


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
                download_result, text_data = future.result()

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

            results.append(download_result)

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
        "[DOWNLOAD COMPLETE] successful=%s failed=%s",
        sum(1 for item in results if item.get("success")),
        sum(1 for item in results if not item.get("success")),
    )

    return downloaded, results


# ============================================================
# M3U PARSER
# ============================================================

URL_PATTERN = re.compile(
    r"^(https?|rtsp|rtmp|rtmps|udp|srt|rist)://",
    re.IGNORECASE,
)


def parse_extinf(line):
    title = ""
    attributes = {}

    if not line.startswith("#EXTINF"):
        return title, attributes

    if "," in line:
        title = line.split(",", 1)[1].strip()

    for match in re.finditer(
        r'([\w-]+)="([^"]*)"',
        line,
    ):
        attributes[match.group(1)] = match.group(2)

    return title, attributes


def parse_playlist(text_data, playlist_path):
    channels = []

    lines = text_data.splitlines()

    pending_title = ""
    pending_attributes = {}
    pending_extinf = ""
    pending_headers = {}

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

        if line.startswith("#EXTVLCOPT:"):
            option = line.split(":", 1)[1]

            if "=" in option:
                key, value = option.split("=", 1)
                pending_headers[key.strip()] = value.strip()

            continue

        if line.startswith("#KODIPROP:"):
            pending_attributes[
                "kodi:" + line.split(":", 1)[1].split("=", 1)[0]
            ] = line.split("=", 1)[-1]

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
            "tvg_id": pending_attributes.get("tvg-id", ""),
            "tvg_name": pending_attributes.get("tvg-name", ""),
            "tvg_logo": pending_attributes.get("tvg-logo", ""),
            "group_title": pending_attributes.get("group-title", ""),
            "language": pending_attributes.get("tvg-language", ""),
            "extinf": pending_extinf,
            "attributes": dict(pending_attributes),
            "request_headers": dict(pending_headers),
            "validation": {},
        })

        pending_title = ""
        pending_attributes = {}
        pending_extinf = ""
        pending_headers = {}

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

            except Exception:
                logger.exception(
                    "[PARSER ERROR] %s",
                    item["item"]["path"],
                )
                channels = []

            item["result"]["channel_count"] = len(channels)
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
# HTTP HEAD
# ============================================================

def check_head(url, channel_name, headers=None):
    started = time.perf_counter()

    result = {
        "attempted": True,
        "applicable": True,
        "status_code": None,
        "response_ms": None,
        "content_type": "",
        "content_length": "",
        "accept_ranges": "",
        "final_url": "",
        "redirects": [],
        "response_headers": {},
        "error": "",
    }

    if not is_http_url(url):
        result.update({
            "applicable": False,
            "attempted": False,
            "error": "HEAD is not applicable to this protocol.",
        })
        return result

    logger.info(
        "[HEAD START] channel=%s | url=%s",
        channel_name,
        safe_url(url),
    )

    response = None

    try:
        response = get_session().head(
            url,
            headers=headers or {},
            allow_redirects=True,
            timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT),
        )

        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result.update({
            "status_code": response.status_code,
            "response_ms": elapsed,
            "content_type": response.headers.get("Content-Type", ""),
            "content_length": response.headers.get("Content-Length", ""),
            "accept_ranges": response.headers.get("Accept-Ranges", ""),
            "final_url": safe_url(response.url),
            "response_headers": dict(response.headers),
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

    finally:
        if response is not None:
            response.close()


# ============================================================
# HTTP GET
# ============================================================

def check_get(url, channel_name, headers=None):
    started = time.perf_counter()

    result = {
        "attempted": True,
        "applicable": True,
        "status_code": None,
        "headers_ms": None,
        "first_byte_total_ms": None,
        "first_chunk_ms": None,
        "total_ms": None,
        "bytes_read": 0,
        "content_type": "",
        "content_length": "",
        "content_range": "",
        "accept_ranges": "",
        "final_url": "",
        "redirects": [],
        "response_headers": {},
        "http_ok": False,
        "range_supported": False,
        "error": "",
    }

    if not is_http_url(url):
        result.update({
            "applicable": False,
            "attempted": False,
            "error": "GET is not applicable to this protocol.",
        })
        return result

    logger.info(
        "[GET START] channel=%s | url=%s",
        channel_name,
        safe_url(url),
    )

    response = None

    request_headers = dict(headers or {})
    request_headers["Range"] = (
        f"bytes=0-{GET_SAMPLE_BYTES - 1}"
    )

    try:
        response = get_session().get(
            url,
            headers=request_headers,
            stream=True,
            allow_redirects=True,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )

        headers_ms = (
            time.perf_counter() - started
        ) * 1000

        result.update({
            "status_code": response.status_code,
            "headers_ms": headers_ms,
            "content_type": response.headers.get("Content-Type", ""),
            "content_length": response.headers.get("Content-Length", ""),
            "content_range": response.headers.get("Content-Range", ""),
            "accept_ranges": response.headers.get("Accept-Ranges", ""),
            "final_url": safe_url(response.url),
            "response_headers": dict(response.headers),
            "range_supported": response.status_code == 206,
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
            "[GET HEADERS] %s | HTTP=%s | headers=%.2f ms | type=%s | length=%s | range=%s",
            channel_name,
            response.status_code,
            headers_ms,
            result["content_type"],
            result["content_length"],
            result["content_range"],
        )

        logger.debug(
            "[GET RESPONSE HEADERS] %s | %s",
            channel_name,
            dict(response.headers),
        )

        if response.status_code not in (200, 206):
            response.raise_for_status()

        for chunk in response.iter_content(
            chunk_size=GET_SAMPLE_BYTES
        ):
            if chunk:
                result["bytes_read"] += len(chunk)

                result["first_byte_total_ms"] = (
                    time.perf_counter() - started
                ) * 1000

                result["first_chunk_ms"] = (
                    result["first_byte_total_ms"] - headers_ms
                )

                break

        result["http_ok"] = bool(
            response.status_code in (200, 206)
            and result["bytes_read"] > 0
        )

        result["total_ms"] = (
            time.perf_counter() - started
        ) * 1000

        for redirect in result["redirects"]:
            logger.info(
                "[GET REDIRECT] HTTP=%s | %s -> %s",
                redirect["status"],
                redirect["url"],
                redirect["location"],
            )

        logger.info(
            "[GET DATA] %s | bytes=%s | headers=%.2f ms | first_byte=%.2f ms | total=%.2f ms",
            channel_name,
            result["bytes_read"],
            headers_ms,
            result["first_byte_total_ms"] or 0,
            result["total_ms"],
        )

        log_event(
            "HTTP_GET",
            channel=channel_name,
            url=safe_url(url),
            **result,
        )

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

    finally:
        if response is not None:
            response.close()


# ============================================================
# HLS ANALYSIS
# ============================================================

def analyze_hls_manifest(url, channel_name, request_headers=None):
    result = {
        "applicable": False,
        "success": False,
        "is_master": False,
        "is_media": False,
        "manifest_url": safe_url(url),
        "manifest_status": None,
        "manifest_content_type": "",
        "variant_count": 0,
        "segment_count": 0,
        "sampled_segments": [],
        "encryption_methods": [],
        "key_urls": [],
        "media_sequence": "",
        "target_duration": "",
        "endlist": False,
        "nested_manifests": [],
        "errors": [],
    }

    if not is_http_url(url):
        return result

    response = None

    try:
        response = get_session().get(
            url,
            headers=request_headers or {},
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )

        result["manifest_status"] = response.status_code
        result["manifest_content_type"] = (
            response.headers.get("Content-Type", "")
        )

        response.raise_for_status()

        body = response.content[:MAX_DECOMPRESSED_SIZE]

        text_data = body.decode(
            "utf-8-sig",
            errors="replace",
        )

        if not text_data.lstrip().startswith("#EXTM3U"):
            return result

        result["applicable"] = True

        lines = [
            line.strip()
            for line in text_data.splitlines()
            if line.strip()
        ]

        result["is_master"] = any(
            line.startswith("#EXT-X-STREAM-INF")
            for line in lines
        )

        result["is_media"] = any(
            line.startswith("#EXTINF")
            for line in lines
        )

        result["media_sequence"] = next(
            (
                line.split(":", 1)[1]
                for line in lines
                if line.startswith("#EXT-X-MEDIA-SEQUENCE:")
            ),
            "",
        )

        result["target_duration"] = next(
            (
                line.split(":", 1)[1]
                for line in lines
                if line.startswith("#EXT-X-TARGETDURATION:")
            ),
            "",
        )

        result["endlist"] = (
            "#EXT-X-ENDLIST" in lines
        )

        variants = []
        segments = []

        for index, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                if index + 1 < len(lines):
                    next_line = lines[index + 1]

                    if not next_line.startswith("#"):
                        variants.append(
                            urljoin(url, next_line)
                        )

            if line.startswith("#EXTINF"):
                if index + 1 < len(lines):
                    next_line = lines[index + 1]

                    if not next_line.startswith("#"):
                        segments.append(
                            urljoin(url, next_line)
                        )

            if line.startswith("#EXT-X-KEY:"):
                attributes = {}

                for match in re.finditer(
                    r'([\w-]+)=("[^"]*"|[^,]*)',
                    line.split(":", 1)[1],
                ):
                    attributes[match.group(1)] = (
                        match.group(2).strip('"')
                    )

                method = attributes.get("METHOD", "UNKNOWN")
                key_uri = attributes.get("URI", "")

                if method not in result["encryption_methods"]:
                    result["encryption_methods"].append(method)

                if key_uri:
                    result["key_urls"].append(
                        safe_url(urljoin(url, key_uri))
                    )

        result["variant_count"] = len(variants)
        result["segment_count"] = len(segments)

        result["nested_manifests"] = [
            safe_url(item)
            for item in variants[:10]
        ]

        sample_urls = segments[:HLS_SEGMENT_SAMPLE]

        for segment_url in sample_urls:
            segment_result = {
                "url": safe_url(segment_url),
                "success": False,
                "status_code": None,
                "bytes_read": 0,
                "content_type": "",
                "error": "",
            }

            segment_response = None

            try:
                segment_response = get_session().get(
                    segment_url,
                    headers=request_headers or {},
                    stream=True,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )

                segment_result["status_code"] = (
                    segment_response.status_code
                )

                segment_result["content_type"] = (
                    segment_response.headers.get(
                        "Content-Type",
                        "",
                    )
                )

                segment_response.raise_for_status()

                for chunk in segment_response.iter_content(
                    chunk_size=65536
                ):
                    if chunk:
                        segment_result["bytes_read"] += len(chunk)

                    if (
                        segment_result["bytes_read"]
                        >= HLS_MAX_SEGMENT_BYTES
                    ):
                        break

                segment_result["success"] = (
                    segment_result["bytes_read"] > 0
                )

            except Exception as error:
                segment_result["error"] = str(error)

            finally:
                if segment_response is not None:
                    segment_response.close()

            result["sampled_segments"].append(
                segment_result
            )

        result["success"] = bool(
            result["applicable"]
            and (
                result["is_master"]
                or result["is_media"]
            )
        )

        logger.info(
            "[HLS] %s | master=%s | media=%s | variants=%s | segments=%s | sampled=%s | encryption=%s | ENDLIST=%s",
            channel_name,
            result["is_master"],
            result["is_media"],
            result["variant_count"],
            result["segment_count"],
            len(result["sampled_segments"]),
            result["encryption_methods"],
            result["endlist"],
        )

        for segment in result["sampled_segments"]:
            logger.info(
                "[HLS SEGMENT] %s | HTTP=%s | bytes=%s | success=%s | error=%s",
                channel_name,
                segment["status_code"],
                segment["bytes_read"],
                segment["success"],
                segment["error"],
            )

        log_event(
            "HLS_ANALYSIS",
            channel=channel_name,
            **result,
        )

        return result

    except Exception as error:
        result["errors"].append(str(error))

        logger.warning(
            "[HLS ERROR] %s | %s",
            channel_name,
            error,
        )

        log_event(
            "HLS_ANALYSIS_ERROR",
            channel=channel_name,
            url=safe_url(url),
            error=str(error),
        )

        return result

    finally:
        if response is not None:
            response.close()


# ============================================================
# FFPROBE
# ============================================================

def find_ffprobe():
    return find_tool("ffprobe")


def ffprobe_input_options(url, request_headers=None):
    options = []

    if request_headers:
        header_lines = []

        for key, value in request_headers.items():
            if key.lower() in (
                "authorization",
                "cookie",
                "referer",
                "user-agent",
                "origin",
            ):
                header_lines.append(
                    f"{key}: {value}"
                )

        if header_lines:
            options.extend([
                "-headers",
                "\r\n".join(header_lines) + "\r\n",
            ])

    if is_http_url(url):
        options.extend([
            "-rw_timeout",
            "15000000",
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_delay_max",
            "3",
        ])

    return options


def run_ffprobe(url, channel_name, request_headers=None):
    ffprobe = find_ffprobe()

    result = {
        "available": bool(ffprobe),
        "attempted": bool(ffprobe),
        "success": False,
        "timed_out": False,
        "return_code": None,
        "duration": "",
        "format_name": "",
        "format_long_name": "",
        "format_bitrate": 0,
        "size_bytes": 0,
        "start_time": "",
        "probe_ms": None,
        "video_tracks": [],
        "audio_tracks": [],
        "subtitle_tracks": [],
        "data_tracks": [],
        "attachment_tracks": [],
        "other_tracks": [],
        "error": "",
        "stderr": "",
    }

    if not ffprobe:
        result["error"] = "ffprobe not found"
        logger.warning(
            "[FFPROBE] Not installed."
        )
        return result

    command = [
        ffprobe,
        "-hide_banner",
        "-v", "error",
        "-print_format", "json",
        "-show_error",
        "-show_format",
        "-show_streams",
        "-show_programs",
        "-show_chapters",
        "-count_packets",
        "-count_frames",
        "-read_intervals",
        f"%+{FFPROBE_READ_SECONDS}",
        *ffprobe_input_options(
            url,
            request_headers,
        ),
        url,
    ]

    logger.info(
        "[FFPROBE START] %s | %s",
        channel_name,
        safe_url(url),
    )

    logger.debug(
        "[FFPROBE COMMAND] %s",
        " ".join(
            "<REDACTED>" if SENSITIVE_QUERY.search(part) else part
            for part in command
        ),
    )

    started = time.perf_counter()

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
        result["return_code"] = process.returncode
        result["stderr"] = process.stderr[-5000:]

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
                error[:2000],
            )

            log_event(
                "FFPROBE_FAILED",
                channel=channel_name,
                url=safe_url(url),
                probe_ms=elapsed,
                return_code=process.returncode,
                error=error[:5000],
            )

            return result

        data = json.loads(
            process.stdout or "{}"
        )

        media_format = data.get("format", {})

        result.update({
            "format_name": media_format.get("format_name", ""),
            "format_long_name": media_format.get("format_long_name", ""),
            "duration": media_format.get("duration", ""),
            "format_bitrate": safe_int(
                media_format.get("bit_rate", 0)
            ),
            "size_bytes": safe_int(
                media_format.get("size", 0)
            ),
            "start_time": media_format.get("start_time", ""),
        })

        for stream in data.get("streams", []):
            codec_type = stream.get(
                "codec_type",
                "unknown",
            )

            tags = stream.get("tags", {}) or {}
            disposition = stream.get("disposition", {}) or {}

            common = {
                "index": stream.get("index", ""),
                "id": stream.get("id", ""),
                "codec_type": codec_type,
                "codec_name": stream.get("codec_name", ""),
                "codec_long_name": stream.get("codec_long_name", ""),
                "profile": stream.get("profile", ""),
                "level": stream.get("level", ""),
                "language": tags.get("language", ""),
                "title": tags.get("title", ""),
                "bitrate": safe_int(stream.get("bit_rate", 0)),
                "time_base": stream.get("time_base", ""),
                "start_time": stream.get("start_time", ""),
                "duration": stream.get("duration", ""),
                "nb_frames": stream.get("nb_frames", ""),
                "nb_read_frames": stream.get("nb_read_frames", ""),
                "nb_read_packets": stream.get("nb_read_packets", ""),
                "is_default": bool(disposition.get("default", 0)),
                "is_forced": bool(disposition.get("forced", 0)),
            }

            if codec_type == "video":
                width = safe_int(stream.get("width", 0))
                height = safe_int(stream.get("height", 0))

                fps = parse_fraction(
                    stream.get("avg_frame_rate")
                    or stream.get("r_frame_rate")
                )

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
                    "fps": round(fps, 3),
                    "pixel_format": stream.get("pix_fmt", ""),
                    "sample_aspect_ratio": stream.get("sample_aspect_ratio", ""),
                    "display_aspect_ratio": stream.get("display_aspect_ratio", ""),
                    "field_order": stream.get("field_order", ""),
                    "color_space": stream.get("color_space", ""),
                    "color_transfer": stream.get("color_transfer", ""),
                    "color_primaries": stream.get("color_primaries", ""),
                    "bits_per_raw_sample": stream.get("bits_per_raw_sample", ""),
                    "is_attached_picture": bool(
                        disposition.get("attached_pic", 0)
                    ),
                }

                result["video_tracks"].append(track)

                logger.info(
                    "[VIDEO TRACK] %s | index=%s | codec=%s | resolution=%s | quality=%s | FPS=%s | bitrate=%s | pixel=%s | packets=%s",
                    channel_name,
                    track["index"],
                    track["codec_name"],
                    track["resolution"],
                    track["quality"],
                    track["fps"],
                    bitrate_text(track["bitrate"]),
                    track["pixel_format"],
                    track["nb_read_packets"],
                )

            elif codec_type == "audio":
                track = {
                    **common,
                    "sample_rate": stream.get("sample_rate", ""),
                    "channels": stream.get("channels", ""),
                    "channel_layout": stream.get("channel_layout", ""),
                    "sample_format": stream.get("sample_fmt", ""),
                    "bits_per_sample": stream.get("bits_per_sample", ""),
                    "initial_padding": stream.get("initial_padding", ""),
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
                result["subtitle_tracks"].append({
                    **common,
                    "subtitle_codec": stream.get("codec_name", ""),
                })

            elif codec_type == "data":
                result["data_tracks"].append(common)

            elif codec_type == "attachment":
                result["attachment_tracks"].append(common)

            else:
                result["other_tracks"].append(common)

        result["success"] = bool(
            result["video_tracks"]
            or result["audio_tracks"]
        )

        logger.info(
            "[FFPROBE SUMMARY] %s | format=%s | duration=%s | bitrate=%s | video=%s | audio=%s | subtitles=%s | data=%s | %.2f ms",
            channel_name,
            result["format_name"],
            result["duration"],
            bitrate_text(result["format_bitrate"]),
            len(result["video_tracks"]),
            len(result["audio_tracks"]),
            len(result["subtitle_tracks"]),
            len(result["data_tracks"]),
            elapsed,
        )

        log_event(
            "FFPROBE_SUCCESS",
            channel=channel_name,
            url=safe_url(url),
            probe_ms=elapsed,
            return_code=process.returncode,
            **{
                key: result[key]
                for key in (
                    "format_name",
                    "duration",
                    "format_bitrate",
                    "video_tracks",
                    "audio_tracks",
                    "subtitle_tracks",
                    "data_tracks",
                    "success",
                )
            },
        )

        return result

    except subprocess.TimeoutExpired:
        elapsed = (
            time.perf_counter() - started
        ) * 1000

        result.update({
            "probe_ms": elapsed,
            "timed_out": True,
            "error": "FFprobe timeout",
        })

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
            probe_ms=elapsed,
            error=str(error),
        )

        return result


# ============================================================
# TSDUCK / TSANALYZE
# ============================================================

def run_tsanalyze(url, channel_name, ffprobe_result):
    """
    tsanalyze is primarily a local transport-stream analyzer.
    A remote URL is not assumed to be supported by every build.
    """

    tool = find_tool("tsanalyze")

    result = {
        "available": bool(tool),
        "attempted": False,
        "success": False,
        "input_mode": "",
        "command": [],
        "output": "",
        "error": "",
        "analysis_file": "",
    }

    if not tool:
        result["error"] = "tsanalyze not installed"
        return result

    if not ffprobe_result.get("success"):
        result["error"] = (
            "Skipped: FFprobe did not identify a readable media stream."
        )
        return result

    # tsanalyze does not universally accept network URLs.
    # Download a bounded sample only for HTTP transport streams.
    if not is_http_url(url):
        result["error"] = (
            "Remote non-HTTP input is not supported by this wrapper."
        )
        return result

    temp_path = None
    response = None

    try:
        response = get_session().get(
            url,
            stream=True,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )

        response.raise_for_status()

        fd, temp_path = tempfile.mkstemp(
            suffix=".ts",
            prefix="iptv_ts_",
            dir=TOOLS_DIR,
        )

        total = 0

        with os.fdopen(fd, "wb") as output:
            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):
                if not chunk:
                    continue

                output.write(chunk)
                total += len(chunk)

                if total >= 16 * 1024 * 1024:
                    break

        if total < 188:
            result["error"] = (
                "Insufficient transport stream sample."
            )
            return result

        result["analysis_file"] = temp_path
        result["input_mode"] = "HTTP DOWNLOADED SAMPLE"

        command = [
            tool,
            temp_path,
        ]

        result["command"] = command
        result["attempted"] = True

        logger.info(
            "[TSDUCK START] %s | sample=%s",
            channel_name,
            human_bytes(total),
        )

        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=45,
            encoding="utf-8",
            errors="replace",
        )

        output_text = (
            process.stdout
            + "\n"
            + process.stderr
        ).strip()

        result["output"] = output_text[-15000:]
        result["success"] = process.returncode == 0

        if process.returncode != 0:
            result["error"] = (
                f"tsanalyze exit code {process.returncode}"
            )

        logger.info(
            "[TSDUCK RESULT] %s | success=%s | exit=%s",
            channel_name,
            result["success"],
            process.returncode,
        )

        if output_text:
            logger.info(
                "[TSDUCK OUTPUT] %s\n%s",
                channel_name,
                output_text[-5000:],
            )

        log_event(
            "TSDUCK_ANALYSIS",
            channel=channel_name,
            url=safe_url(url),
            success=result["success"],
            return_code=process.returncode,
            output=result["output"],
            error=result["error"],
        )

        return result

    except subprocess.TimeoutExpired:
        result["error"] = "tsanalyze timeout"

        logger.warning(
            "[TSDUCK TIMEOUT] %s",
            channel_name,
        )

        return result

    except Exception as error:
        result["error"] = str(error)

        logger.warning(
            "[TSDUCK ERROR] %s | %s",
            channel_name,
            error,
        )

        log_event(
            "TSDUCK_ERROR",
            channel=channel_name,
            url=safe_url(url),
            error=str(error),
        )

        return result

    finally:
        if response is not None:
            response.close()

        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                logger.warning(
                    "[TSDUCK] Could not remove temporary file: %s",
                    temp_path,
                )


# ============================================================
# TSHARK
# ============================================================

def run_tshark_capture(url, channel_name):
    """
    Optional local capture.
    It can only inspect traffic visible on the selected interface.
    It does not claim packet loss/jitter for an arbitrary URL.
    """

    tool = find_tool("tshark")

    result = {
        "available": bool(tool),
        "attempted": False,
        "enabled": TSHARK_CAPTURE_ENABLED,
        "success": False,
        "interface": TSHARK_INTERFACE,
        "capture_seconds": TSHARK_CAPTURE_SECONDS,
        "packet_count": 0,
        "rtp_streams": "",
        "jitter": "NOT_MEASURED",
        "packet_loss": "NOT_MEASURED",
        "output": "",
        "error": "",
    }

    if not TSHARK_CAPTURE_ENABLED:
        result["error"] = "Capture disabled by configuration."
        return result

    if not tool:
        result["error"] = "tshark not installed"
        return result

    parsed = urlparse(url)

    if not parsed.hostname:
        result["error"] = "Cannot resolve stream host."
        return result

    try:
        resolved = socket.gethostbyname(
            parsed.hostname
        )

        capture_filter = (
            f"host {resolved} and udp"
        )

        command = [
            tool,
            "-i",
            TSHARK_INTERFACE,
            "-a",
            f"duration:{TSHARK_CAPTURE_SECONDS}",
            "-f",
            capture_filter,
            "-q",
            "-z",
            "io,stat,1",
            "-z",
            "rtp,streams",
        ]

        result["attempted"] = True

        logger.info(
            "[TSHARK START] %s | interface=%s | host=%s",
            channel_name,
            TSHARK_INTERFACE,
            resolved,
        )

        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=TSHARK_CAPTURE_SECONDS + 20,
            encoding="utf-8",
            errors="replace",
        )

        output_text = (
            process.stdout
            + "\n"
            + process.stderr
        ).strip()

        result["output"] = output_text[-12000:]

        result["success"] = (
            process.returncode == 0
        )

        # This is a textual packet count estimate only.
        # It is not an RTP loss calculation.
        packet_match = re.search(
            r"(\d+)\s+packets?",
            output_text,
            re.IGNORECASE,
        )

        if packet_match:
            result["packet_count"] = safe_int(
                packet_match.group(1)
            )

        if "RTP Streams" in output_text or "SSRC" in output_text:
            result["rtp_streams"] = "RTP DETECTED"
        else:
            result["rtp_streams"] = "RTP NOT CONFIRMED"

        result["jitter"] = (
            "NOT_MEASURED_UNLESS_RTP_STATS_AVAILABLE"
        )

        result["packet_loss"] = (
            "NOT_MEASURED_UNLESS_RTP_STATS_AVAILABLE"
        )

        logger.info(
            "[TSHARK RESULT] %s | success=%s | RTP=%s | packets=%s",
            channel_name,
            result["success"],
            result["rtp_streams"],
            result["packet_count"],
        )

        log_event(
            "TSHARK_CAPTURE",
            channel=channel_name,
            url=safe_url(url),
            **result,
        )

        return result

    except subprocess.TimeoutExpired:
        result["error"] = "TShark capture timeout"

        logger.warning(
            "[TSHARK TIMEOUT] %s",
            channel_name,
        )

        return result

    except Exception as error:
        result["error"] = str(error)

        logger.warning(
            "[TSHARK ERROR] %s | %s",
            channel_name,
            error,
        )

        log_event(
            "TSHARK_ERROR",
            channel=channel_name,
            url=safe_url(url),
            error=str(error),
        )

        return result


# ============================================================
# PYAV
# ============================================================

def run_pyav(url, channel_name, request_headers=None):
    result = {
        "available": False,
        "attempted": False,
        "success": False,
        "format": "",
        "streams": [],
        "packets_checked": 0,
        "error": "",
    }

    if not importlib.util.find_spec("av"):
        result["error"] = "PyAV not installed"
        return result

    result["available"] = True

    try:
        import av

        options = {
            "rw_timeout": "12000000",
        }

        if request_headers:
            header_lines = []

            for key, value in request_headers.items():
                if key.lower() in (
                    "authorization",
                    "cookie",
                    "referer",
                    "user-agent",
                ):
                    header_lines.append(
                        f"{key}: {value}"
                    )

            if header_lines:
                options["headers"] = (
                    "\r\n".join(header_lines) + "\r\n"
                )

        logger.info(
            "[PYAV START] %s | %s",
            channel_name,
            safe_url(url),
        )

        result["attempted"] = True

        container = av.open(
            url,
            mode="r",
            options=options,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )

        result["format"] = str(
            container.format.name
            if container.format
            else ""
        )

        for stream in container.streams:
            codec_context = stream.codec_context

            track = {
                "index": stream.index,
                "type": stream.type,
                "codec": (
                    codec_context.name
                    if codec_context
                    else ""
                ),
                "width": getattr(
                    codec_context,
                    "width",
                    0,
                ),
                "height": getattr(
                    codec_context,
                    "height",
                    0,
                ),
                "sample_rate": getattr(
                    codec_context,
                    "sample_rate",
                    0,
                ),
                "channels": getattr(
                    codec_context,
                    "channels",
                    0,
                ),
                "time_base": str(stream.time_base),
            }

            result["streams"].append(track)

            logger.info(
                "[PYAV TRACK] %s | %s",
                channel_name,
                track,
            )

        # A small packet sample is used.
        # Some live sources can still block during demux.
        for packet in container.demux():
            if packet.size:
                result["packets_checked"] += 1

            if result["packets_checked"] >= 40:
                break

        container.close()

        result["success"] = bool(
            result["streams"]
        )

        logger.info(
            "[PYAV RESULT] %s | success=%s | streams=%s | packets=%s",
            channel_name,
            result["success"],
            len(result["streams"]),
            result["packets_checked"],
        )

        log_event(
            "PYAV_ANALYSIS",
            channel=channel_name,
            url=safe_url(url),
            **result,
        )

        return result

    except Exception as error:
        result["error"] = str(error)

        logger.warning(
            "[PYAV ERROR] %s | %s",
            channel_name,
            error,
        )

        log_event(
            "PYAV_ERROR",
            channel=channel_name,
            url=safe_url(url),
            error=str(error),
        )

        return result


# ============================================================
# STREAM VALIDATION
# ============================================================

def validate_url(url, channel_name, request_headers=None):
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

    protocol = classify_protocol(url)

    # HEAD and GET are mandatory diagnostics for HTTP/HTTPS.
    # They do not determine whether FFprobe is allowed to run.
    head = check_head(
        url,
        channel_name,
        request_headers,
    )

    get = check_get(
        url,
        channel_name,
        request_headers,
    )

    # FFprobe is independent of GET.
    # A failed HTTP sample must not suppress a separate media probe.
    probe = run_ffprobe(
        url,
        channel_name,
        request_headers,
    )

    content_type = (
        get.get("content_type")
        or head.get("content_type")
        or ""
    )

    hls = {
        "applicable": False,
        "success": False,
        "errors": [],
    }

    if (
        is_http_url(url)
        and is_hls_url(url, content_type)
    ):
        hls = analyze_hls_manifest(
            get.get("final_url")
            or url,
            channel_name,
            request_headers,
        )

    # Independent optional analyzers.
    tsduck = run_tsanalyze(
        url,
        channel_name,
        probe,
    )

    tshark = run_tshark_capture(
        url,
        channel_name,
    )

    pyav = run_pyav(
        url,
        channel_name,
        request_headers,
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

    http_applicable = is_http_url(url)

    http_ok = (
        get.get("http_ok", False)
        if http_applicable
        else None
    )

    media_ok = bool(
        probe.get("success")
    )

    # VERIFIED means HTTP transport is reachable (for HTTP URLs)
    # and FFprobe identifies at least one audio/video stream.
    verified = bool(
        media_ok
        and (
            http_ok
            if http_applicable
            else True
        )
    )

    response_ms = get.get("headers_ms")

    if not http_applicable:
        response_ms = None

    result = {
        "url": url,
        "protocol": protocol,
        "verified": verified,
        "status": "VERIFIED" if verified else "FAILED",
        "http_ok": http_ok,
        "media_ok": media_ok,
        "head": head,
        "get": get,
        "probe": probe,
        "hls": hls,
        "tsduck": tsduck,
        "tshark": tshark,
        "pyav": pyav,
        "total_check_ms": total_ms,
        "response_class": response_class(response_ms),
        "response_ms": response_ms,
        "video_track_count": len(video_tracks),
        "audio_track_count": len(audio_tracks),
        "subtitle_track_count": len(subtitle_tracks),
        "data_track_count": len(probe.get("data_tracks", [])),
        "video_tracks": video_tracks,
        "audio_tracks": audio_tracks,
        "subtitle_tracks": subtitle_tracks,
        "data_tracks": probe.get("data_tracks", []),
        "format_name": probe.get("format_name", ""),
        "format_bitrate": probe.get("format_bitrate", 0),
        "duration": probe.get("duration", ""),
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
        "[STREAM RESULT] %s | %s | protocol=%s | HTTP=%s | MEDIA=%s | resolution=%s | quality=%s | video=%s | audio=%s | subtitles=%s | total=%.2f ms",
        channel_name,
        result["status"],
        protocol,
        http_ok,
        media_ok,
        result["resolution"] or "UNKNOWN",
        result["quality"],
        result["video_track_count"],
        result["audio_track_count"],
        result["subtitle_track_count"],
        total_ms,
    )

    logger.info(
        "[STREAM DIAGNOSTICS] %s | HEAD=%s | GET=%s | FFPROBE=%s | HLS=%s | TSDUCK=%s | TSHARK=%s | PYAV=%s",
        channel_name,
        head.get("status_code", "N/A"),
        get.get("status_code", "N/A"),
        probe.get("success"),
        hls.get("success"),
        tsduck.get("success"),
        tshark.get("success"),
        pyav.get("success"),
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
        protocol=protocol,
        http_ok=http_ok,
        media_ok=media_ok,
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
# PARALLEL UNIQUE STREAM VALIDATION
# ============================================================

def validate_unique_streams(channels):
    unique_urls = {}

    for channel in channels:
        url = channel["url"]

        if url not in unique_urls:
            unique_urls[url] = {
                "name": channel["name"],
                "url": url,
                "headers": channel.get(
                    "request_headers",
                    {},
                ),
                "aliases": [],
            }

        if channel["name"] not in unique_urls[url]["aliases"]:
            unique_urls[url]["aliases"].append(
                channel["name"]
            )

    logger.info("=" * 75)
    logger.info(
        "[CHECK POOL] START | workers=%s | unique_urls=%s",
        CHECK_WORKERS,
        len(unique_urls),
    )
    logger.info(
        "[CHECK POOL] Records=%s | duplicate aliases=%s",
        len(channels),
        len(channels) - len(unique_urls),
    )
    logger.info("=" * 75)

    results = {}
    completed = 0
    total = len(unique_urls)

    with ThreadPoolExecutor(
        max_workers=CHECK_WORKERS,
        thread_name_prefix="STREAM_CHECK",
    ) as executor:

        futures = {
            executor.submit(
                validate_url,
                item["url"],
                item["name"],
                item["headers"],
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

    # Every alias retains the same shared validation result.
    for channel in channels:
        channel["validation"] = results.get(
            channel["url"],
            {},
        )

    logger.info(
        "[CHECK COMPLETE] verified=%s failed=%s",
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
                "source_playlists": [],
            }

        alias = channel["name"]

        if alias not in unique[url]["aliases"]:
            unique[url]["aliases"].append(alias)

        playlist = channel["playlist"]

        if playlist not in unique[url]["source_playlists"]:
            unique[url]["source_playlists"].append(playlist)

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
            validation = channel.get(
                "validation",
                {},
            ) or {}

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
                    value = str(value).replace('"', "'")
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

            # Preserve per-entry request headers where possible.
            for key, value in channel.get(
                "request_headers",
                {},
            ).items():
                file.write(
                    f"#EXTVLCOPT:{key}={value}\n"
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
        "source_playlists",
        "playlist",
        "line",
        "tvg_id",
        "tvg_name",
        "tvg_logo",
        "group_title",
        "language",
        "protocol",
        "verified",
        "status",
        "http_ok",
        "media_ok",
        "head_status",
        "head_ms",
        "http_status",
        "response_ms",
        "response_class",
        "get_total_ms",
        "first_byte_total_ms",
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
        "data_track_count",
        "format_bitrate",
        "duration",
        "total_check_ms",
        "hls_success",
        "hls_segment_count",
        "tsduck_success",
        "tshark_success",
        "pyav_success",
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
            validation = channel.get(
                "validation",
                {},
            ) or {}

            get = validation.get("get", {})
            head = validation.get("head", {})
            hls = validation.get("hls", {})
            tsduck = validation.get("tsduck", {})
            tshark = validation.get("tshark", {})
            pyav = validation.get("pyav", {})

            writer.writerow({
                "name": channel["name"],
                "url": channel["url"],
                "aliases": " | ".join(channel.get("aliases", [])),
                "source_playlists": " | ".join(
                    channel.get("source_playlists", [])
                ),
                "playlist": channel["playlist"],
                "line": channel["line"],
                "tvg_id": channel["tvg_id"],
                "tvg_name": channel["tvg_name"],
                "tvg_logo": channel["tvg_logo"],
                "group_title": channel["group_title"],
                "language": channel["language"],
                "protocol": validation.get("protocol", ""),
                "verified": validation.get("verified", False),
                "status": validation.get("status", "UNKNOWN"),
                "http_ok": validation.get("http_ok", ""),
                "media_ok": validation.get("media_ok", ""),
                "head_status": head.get("status_code", ""),
                "head_ms": head.get("response_ms", ""),
                "http_status": get.get("status_code", ""),
                "response_ms": validation.get("response_ms", ""),
                "response_class": validation.get("response_class", ""),
                "get_total_ms": get.get("total_ms", ""),
                "first_byte_total_ms": get.get("first_byte_total_ms", ""),
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
                "data_track_count": validation.get("data_track_count", 0),
                "format_bitrate": validation.get("format_bitrate", 0),
                "duration": validation.get("duration", ""),
                "total_check_ms": validation.get("total_check_ms", ""),
                "hls_success": hls.get("success", False),
                "hls_segment_count": hls.get("segment_count", 0),
                "tsduck_success": tsduck.get("success", False),
                "tshark_success": tshark.get("success", False),
                "pyav_success": pyav.get("success", False),
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
        validation = channel.get(
            "validation",
            {},
        ) or {}

        export_data.append({
            "name": channel["name"],
            "url": channel["url"],
            "aliases": channel.get("aliases", []),
            "source_playlists": channel.get("source_playlists", []),
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
        "time_base",
        "duration",
        "packets",
        "frames",
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
            validation = channel.get(
                "validation",
                {},
            ) or {}

            for track_type in (
                "video",
                "audio",
                "subtitle",
                "data",
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
                        "time_base": track.get("time_base", ""),
                        "duration": track.get("duration", ""),
                        "packets": track.get("nb_read_packets", ""),
                        "frames": track.get("nb_read_frames", ""),
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
        "compressed_size_bytes",
        "channel_count",
        "final_url",
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
                "compressed_size_bytes": item.get(
                    "compressed_size_bytes",
                    "",
                ),
                "channel_count": item.get("channel_count", 0),
                "final_url": item.get("final_url", ""),
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

    def count_if(predicate):
        return sum(
            1 for result in unique_results.values()
            if predicate(result)
        )

    video_streams = count_if(
        lambda item: item.get("video_track_count", 0) > 0
    )

    audio_streams = count_if(
        lambda item: item.get("audio_track_count", 0) > 0
    )

    subtitle_streams = count_if(
        lambda item: item.get("subtitle_track_count", 0) > 0
    )

    head_attempted = count_if(
        lambda item: item.get("head", {}).get("attempted", False)
    )

    get_ok = count_if(
        lambda item: item.get("get", {}).get("http_ok", False)
    )

    ffprobe_ok = count_if(
        lambda item: item.get("probe", {}).get("success", False)
    )

    hls_count = count_if(
        lambda item: item.get("hls", {}).get("applicable", False)
    )

    hls_ok = count_if(
        lambda item: item.get("hls", {}).get("success", False)
    )

    tsduck_ok = count_if(
        lambda item: item.get("tsduck", {}).get("success", False)
    )

    tshark_ok = count_if(
        lambda item: item.get("tshark", {}).get("success", False)
    )

    pyav_ok = count_if(
        lambda item: item.get("pyav", {}).get("success", False)
    )

    playlist_ok = sum(
        1 for item in playlist_results
        if item.get("success")
    )

    playlist_failed = len(playlist_results) - playlist_ok

    fast = count_if(
        lambda item: item.get("response_class") == "FAST RESPONSE"
    )

    normal = count_if(
        lambda item: item.get("response_class") == "NORMAL RESPONSE"
    )

    slow = count_if(
        lambda item: item.get("response_class") == "SLOW RESPONSE"
    )

    with open(
        filename,
        "w",
        encoding="utf-8",
    ) as file:

        file.write("=" * 85 + "\n")
        file.write("IPTV REPOSITORY PLAYLIST HUNTER\n")
        file.write("EXTENDED DIAGNOSTIC AUDIT REPORT\n")
        file.write("=" * 85 + "\n\n")

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
            f"Total records: {total_records}\n"
        )
        file.write(
            f"Unique stream URLs: {total_unique}\n"
        )
        file.write(
            f"Duplicate URL aliases: {total_records - total_unique}\n"
        )
        file.write(
            f"Verified: {verified}\n"
        )
        file.write(
            f"Failed: {failed}\n"
        )
        file.write(
            f"Success rate: {(verified / total_unique * 100) if total_unique else 0:.2f}%\n\n"
        )

        file.write("--- HTTP / MEDIA DIAGNOSTICS ---\n")
        file.write(
            f"HEAD attempted: {head_attempted}\n"
        )
        file.write(
            f"GET successful: {get_ok}\n"
        )
        file.write(
            f"FFprobe successful: {ffprobe_ok}\n\n"
        )

        file.write("--- RESPONSE CLASSIFICATION ---\n")
        file.write(
            f"Fast (<= {FAST_RESPONSE_MS} ms): {fast}\n"
        )
        file.write(
            f"Normal (<= {NORMAL_RESPONSE_MS} ms): {normal}\n"
        )
        file.write(
            f"Slow (> {NORMAL_RESPONSE_MS} ms): {slow}\n\n"
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

        file.write("--- ADDITIONAL ANALYZERS ---\n")
        file.write(
            f"HLS manifests found: {hls_count}\n"
        )
        file.write(
            f"HLS manifests analyzed: {hls_ok}\n"
        )
        file.write(
            f"TSDuck successful: {tsduck_ok}\n"
        )
        file.write(
            f"TShark successful: {tshark_ok}\n"
        )
        file.write(
            f"PyAV successful: {pyav_ok}\n\n"
        )

        file.write("--- STREAM DETAILS ---\n\n")

        for url, result in unique_results.items():
            head = result.get("head", {})
            get = result.get("get", {})
            probe = result.get("probe", {})
            hls = result.get("hls", {})
            tsduck = result.get("tsduck", {})
            tshark = result.get("tshark", {})
            pyav = result.get("pyav", {})

            file.write(
                f"URL: {safe_url(url)}\n"
            )
            file.write(
                f"Protocol: {result.get('protocol', '')}\n"
            )
            file.write(
                f"Status: {result.get('status', 'UNKNOWN')}\n"
            )
            file.write(
                f"Verified: {result.get('verified', False)}\n"
            )

            file.write(
                f"HEAD: {head.get('status_code', 'N/A')} | {head.get('response_ms', '')} ms | {head.get('error', '')}\n"
            )
            file.write(
                f"GET: {get.get('status_code', 'N/A')} | {get.get('headers_ms', '')} ms headers | {get.get('first_byte_total_ms', '')} ms first byte | {get.get('bytes_read', 0)} bytes\n"
            )
            file.write(
                f"GET error: {get.get('error', '')}\n"
            )

            file.write(
                f"FFprobe: success={probe.get('success', False)} | timeout={probe.get('timed_out', False)} | error={probe.get('error', '')}\n"
            )
            file.write(
                f"Format: {result.get('format_name', '')}\n"
            )
            file.write(
                f"Duration: {result.get('duration', '')}\n"
            )
            file.write(
                f"Bitrate: {bitrate_text(result.get('format_bitrate', 0))}\n"
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
                f"HLS: applicable={hls.get('applicable', False)} | success={hls.get('success', False)} | variants={hls.get('variant_count', 0)} | segments={hls.get('segment_count', 0)} | encryption={hls.get('encryption_methods', [])}\n"
            )

            file.write(
                f"TSDuck: {tsduck.get('success', False)} | {tsduck.get('error', '')}\n"
            )
            file.write(
                f"TShark: {tshark.get('success', False)} | {tshark.get('error', '')}\n"
            )
            file.write(
                f"PyAV: {pyav.get('success', False)} | {pyav.get('error', '')}\n"
            )

            file.write(
                f"Total check: {result.get('total_check_ms', '')} ms\n"
            )
            file.write(
                "Live broadcast latency: NOT MEASURED\n"
            )
            file.write("-" * 85 + "\n")

        file.write("\n--- INTERPRETATION ---\n")
        file.write(
            "HTTP response time is not broadcast latency.\n"
        )
        file.write(
            "A failed HEAD request does not necessarily mean the stream is unavailable.\n"
        )
        file.write(
            "GET and FFprobe are evaluated independently.\n"
        )
        file.write(
            "FFprobe timeout does not prove that a stream is dead.\n"
        )
        file.write(
            "TSDuck and PyAV are additional diagnostics, not replacements for FFprobe.\n"
        )
        file.write(
            "TShark analyzes traffic visible on the selected local interface only.\n"
        )
        file.write(
            "Packet loss and jitter are not reported unless valid RTP statistics are available.\n"
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
    logger.info("=" * 85)
    logger.info("FINAL IPTV AUDIT STATISTICS")
    logger.info("=" * 85)

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

    logger.info("=" * 85)

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
    logger.info("#" * 85)
    logger.info("IPTV REPOSITORY PLAYLIST HUNTER STARTED")
    logger.info("#" * 85)

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

    # --------------------------------------------------------
    # STEP 0: TOOL PREPARATION
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 0: TOOL PREPARATION"
    )

    install_optional_tools()
    inventory = tool_inventory()

    log_event(
        "RUN_STARTED",
        owner=OWNER,
        repository=REPOSITORY,
        branch=BRANCH,
        download_workers=DOWNLOAD_WORKERS,
        parser_workers=PARSER_WORKERS,
        check_workers=CHECK_WORKERS,
        tools=inventory,
    )

    # --------------------------------------------------------
    # STEP 1: GITHUB TREE
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 1: GITHUB TREE"
    )

    tree = get_repository_tree()

    # --------------------------------------------------------
    # STEP 2: PLAYLIST DISCOVERY
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 2: PLAYLIST DISCOVERY"
    )

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
    # STEP 5: STREAM VALIDATION - 45 WORKERS
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 5: STREAM VALIDATION | workers=%s",
        CHECK_WORKERS,
    )

    unique_results = validate_unique_streams(
        all_channels
    )

    # --------------------------------------------------------
    # STEP 6: UNIQUE DATABASE
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
    # STEP 10: PLAYLIST REPORT
    # --------------------------------------------------------

    export_playlist_report(
        playlist_results
    )

    # --------------------------------------------------------
    # STEP 11: FULL AUDIT REPORT
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 11: EXPORT AUDIT REPORT"
    )

    export_check_report(
        all_channels,
        unique_results,
        playlist_results,
    )

    # --------------------------------------------------------
    # STEP 12: FINAL STATISTICS
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
    logger.info("#" * 85)
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
    logger.info("#" * 85)

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

        log_event(
            "RUN_INTERRUPTED",
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
