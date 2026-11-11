
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
    45 - per-record stream validation (no deduplication)
    39 - concurrent audio-language analysis workers

MAIN ANALYZER:
    FFprobe

INDEPENDENT DIAGNOSTICS:
    HTTP HEAD / GET
    HLS manifest and segment analysis
    TSDuck / tsanalyze
    TShark (bounded per-stream local packet capture)
    PyAV
    faster-whisper language identification
    SpeechBrain VoxLingua107 (optional independent confirmation)

INSTALLATION:
    requests, faster-whisper, SpeechBrain, PyAV (auto-installed when missing)
    FFmpeg/FFprobe, TSDuck, TShark (auto-installed when missing)

IMPORTANT:
    HTTP response time is not live broadcast latency.
    TShark analyzes locally captured TCP/UDP packets; it does not infer media loss from HTTP timings.
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

# Audio analysis uses three independent execution levels:
#   channel pool: one coordinator task per playlist occurrence;
#   track pool: every FFprobe audio stream is analyzed independently;
#   engine pool: Whisper and SpeechBrain can evaluate the same sample in parallel.
# The extraction/inference semaphores remain the hard resource limits. They are
# deliberately separate from executor sizes so queue capacity never implies
# that dozens of heavy decoders/models run at once.
AUDIO_LANGUAGE_WORKERS = max(1, int(os.getenv("IPTV_AUDIO_LANGUAGE_WORKERS", "50")))
AUDIO_TRACK_WORKERS = max(1, int(os.getenv("IPTV_AUDIO_TRACK_WORKERS", "50")))
AUDIO_ENGINE_WORKERS = max(2, int(os.getenv("IPTV_AUDIO_ENGINE_WORKERS", "4")))
AUDIO_EXTRACTION_SLOTS = max(1, int(os.getenv("IPTV_AUDIO_EXTRACTION_SLOTS", "6")))
AUDIO_INFERENCE_SLOTS = max(1, int(os.getenv("IPTV_AUDIO_INFERENCE_SLOTS", "2")))
AUDIO_SAMPLE_SECONDS = max(5, int(os.getenv("IPTV_AUDIO_SAMPLE_SECONDS", "18")))
# Every FFprobe audio track is analyzed. No per-record track cap is applied.
AUDIO_MAX_TRACKS_PER_CHANNEL = 0
AUDIO_LANGUAGE_ENABLED = os.getenv("IPTV_AUDIO_LANGUAGE_ENABLED", "1").lower() not in ("0", "false", "no")
WHISPER_MODEL_SIZE = os.getenv("IPTV_WHISPER_MODEL", "small").strip() or "small"
ENABLE_SPEECHBRAIN = os.getenv("IPTV_ENABLE_SPEECHBRAIN", "1").lower() not in ("0", "false", "no")
# All available analyzers are enabled by default. Environment overrides are
# retained only as emergency switches for constrained runners.
ENABLE_PYAV = os.getenv("IPTV_ENABLE_PYAV", "1").lower() not in ("0", "false", "no")
ENABLE_TSDUCK = os.getenv("IPTV_ENABLE_TSDUCK", "1").lower() not in ("0", "false", "no")
ENABLE_TSHARK = os.getenv("IPTV_ENABLE_TSHARK", "1").lower() not in ("0", "false", "no")
ENABLE_WHISPER = os.getenv("IPTV_ENABLE_WHISPER", "1").lower() not in ("0", "false", "no")
AUTO_INSTALL_PYTHON_ANALYZERS = os.getenv("IPTV_AUTO_INSTALL_PYTHON", "1").lower() not in ("0", "false", "no")
SPEECHBRAIN_THRESHOLD = float(os.getenv("IPTV_SPEECHBRAIN_THRESHOLD", "0.55"))
WHISPER_THRESHOLD = float(os.getenv("IPTV_WHISPER_THRESHOLD", "0.60"))

# Intentionally no URL or record deduplication: every playlist occurrence
# is checked and exported independently.
DEDUPLICATE_STREAMS = False

CINERAMA_OLD_HOST = "stream8.cinerama.uz"
CINERAMA_NEW_HOST = "stream1.cinerama.uz"
URL_REWRITE_COUNT = 0
URL_REWRITE_LOCK = threading.Lock()
_AUDIO_EXECUTOR = None
_AUDIO_TRACK_EXECUTOR = None
_AUDIO_ENGINE_EXECUTOR = None
_AUDIO_EXTRACTION_SEMAPHORE = threading.BoundedSemaphore(max(1, AUDIO_EXTRACTION_SLOTS))
_AUDIO_INFERENCE_SEMAPHORE = threading.BoundedSemaphore(max(1, AUDIO_INFERENCE_SLOTS))
_WHISPER_MODEL = None
_WHISPER_MODEL_ERROR = ""
_WHISPER_MODEL_LOCK = threading.Lock()
_SPEECHBRAIN_MODEL = None
_SPEECHBRAIN_MODEL_ERROR = ""
_SPEECHBRAIN_MODEL_LOCK = threading.Lock()

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

# TShark capture is enabled by default. It captures TCP and UDP traffic
# for every checked stream host, subject to a bounded capture semaphore.
# Set IPTV_TSHARK_CAPTURE=0 only when the runner cannot capture packets.
TSHARK_CAPTURE_ENABLED = (
    os.getenv("IPTV_TSHARK_CAPTURE", "1").lower()
    not in ("0", "false", "no")
)
TSHARK_INTERFACE = os.getenv("IPTV_TSHARK_INTERFACE", "any")
TSHARK_CAPTURE_SECONDS = max(2, int(os.getenv("IPTV_TSHARK_CAPTURE_SECONDS", "8")))
TSHARK_CAPTURE_WORKERS = max(1, int(os.getenv("IPTV_TSHARK_CAPTURE_WORKERS", "6")))
TSHARK_CAPTURE_MAX_KB = max(256, int(os.getenv("IPTV_TSHARK_CAPTURE_MAX_KB", "4096")))
TSHARK_KEEP_PCAP = os.getenv("IPTV_TSHARK_KEEP_PCAP", "0").lower() in ("1", "true", "yes")

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

TSHARK_DIR = os.path.join(OUTPUT_DIR, "tshark")
TSHARK_PCAP_DIR = os.path.join(TSHARK_DIR, "pcap")

for directory in (
    OUTPUT_DIR,
    PLAYLIST_DIR,
    DATABASE_DIR,
    LOG_DIR,
    TOOLS_DIR,
    TSHARK_DIR,
    TSHARK_PCAP_DIR,
):
    os.makedirs(directory, exist_ok=True)

MODEL_CACHE_DIR = os.path.abspath(
    os.getenv(
        "IPTV_MODEL_CACHE_DIR",
        os.path.join(TOOLS_DIR, "models"),
    )
)
os.makedirs(MODEL_CACHE_DIR, exist_ok=True)

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
_TSHARK_SEMAPHORE = threading.BoundedSemaphore(max(1, TSHARK_CAPTURE_WORKERS))
_TSHARK_RECORDS = []
_TSHARK_RECORDS_LOCK = threading.Lock()
_TSHARK_SEQUENCE = 0
_TSHARK_SEQUENCE_LOCK = threading.Lock()


def log_event(event, **data):
    record = _sanitize_audit_value({
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "event": event,
        **data,
    })

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



def rewrite_stream_url(url):
    """
    Force the retired Cinerama hostname to the replacement hostname.
    Only the hostname is changed; scheme, port, path, query and fragment
    are preserved. The rewrite is applied before every stream check/export.
    """
    global URL_REWRITE_COUNT

    try:
        value = str(url or "")
        parsed = urlparse(value)
        hostname = (parsed.hostname or "").lower()

        if hostname != CINERAMA_OLD_HOST:
            return value

        auth = ""
        hostport = parsed.netloc.rsplit("@", 1)[-1]
        if "@" in parsed.netloc:
            auth = parsed.netloc.rsplit("@", 1)[0] + "@"

        port = parsed.port
        new_netloc = auth + CINERAMA_NEW_HOST
        if port:
            new_netloc += f":{port}"

        rewritten = urlunparse((
            parsed.scheme,
            new_netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        ))

        with URL_REWRITE_LOCK:
            URL_REWRITE_COUNT += 1

        logger.info(
            "[URL REWRITE] %s -> %s",
            safe_url(value),
            safe_url(rewritten),
        )
        return rewritten

    except Exception:
        return str(url or "")


_SECRET_TEXT_PATTERN = re.compile(
    r"(?i)(\b(?:token|access_token|refresh_token|api[_-]?key|key|auth|"
    r"authorization|password|passwd|secret|signature|credential|session|jwt)\b"
    r"\s*[=:]\s*)([^&\s,;\"'<>]+)"
)
_BEARER_PATTERN = re.compile(r"(?i)(\bBearer\s+)[A-Za-z0-9._~+/=-]+")


def redact_text(value):
    """Redact credentials embedded in URLs, errors, stderr and log messages."""
    text_value = str(value or "")
    text_value = _SECRET_TEXT_PATTERN.sub(r"\1***", text_value)
    text_value = _BEARER_PATTERN.sub(r"\1***", text_value)
    try:
        candidates = re.findall(r"https?://[^\s\"'<>]+", text_value)
        for candidate in candidates:
            text_value = text_value.replace(candidate, safe_url(candidate))
    except Exception:
        pass
    return text_value


def _sanitize_audit_value(value):
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in ("url", "final_url", "manifest_url"):
                cleaned[key] = safe_url(item) if item else item
            else:
                cleaned[key] = _sanitize_audit_value(item)
        return cleaned
    if isinstance(value, (list, tuple)):
        return [_sanitize_audit_value(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


class _SecretRedactionFilter(logging.Filter):
    def filter(self, record):
        try:
            rendered = record.getMessage()
            record.msg = redact_text(rendered)
            record.args = ()
            if record.exc_info:
                formatter = logging.Formatter()
                record.exc_text = redact_text(
                    formatter.formatException(record.exc_info)
                )
                record.exc_info = None
        except Exception:
            pass
        return True


_secret_redaction_filter = _SecretRedactionFilter()
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_secret_redaction_filter)
logger.addFilter(_secret_redaction_filter)


def rewrite_playlist_text(text_data):
    """Rewrite every literal occurrence of the retired Cinerama host in playlist text."""
    global URL_REWRITE_COUNT
    pattern = re.compile(re.escape(CINERAMA_OLD_HOST), re.IGNORECASE)
    matches = len(pattern.findall(text_data or ""))
    if matches:
        text_data = pattern.sub(CINERAMA_NEW_HOST, text_data)
        with URL_REWRITE_LOCK:
            URL_REWRITE_COUNT += matches
        logger.info(
            "[PLAYLIST URL REWRITE] replaced=%s | %s -> %s",
            matches,
            CINERAMA_OLD_HOST,
            CINERAMA_NEW_HOST,
        )
    return text_data


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
        install_env = os.environ.copy()
        install_env.setdefault("DEBIAN_FRONTEND", "noninteractive")
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=600,
            encoding="utf-8",
            errors="replace",
            env=install_env,
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

        executable_suffix = ".exe" if system == "windows" else ""
        found = {}
        for root, _, files in os.walk(extract_dir):
            for binary_name in ("ffprobe", "ffmpeg"):
                candidate_name = binary_name + executable_suffix
                if candidate_name in files and binary_name not in found:
                    found[binary_name] = os.path.join(
                        root,
                        candidate_name,
                    )

        if "ffprobe" not in found:
            raise RuntimeError(
                "ffprobe executable not found after extraction."
            )

        for binary_name, binary_path in found.items():
            if system != "windows":
                os.chmod(binary_path, 0o755)
            TOOL_PATHS[binary_name] = binary_path
            logger.info(
                "[FFMPEG INSTALL] %s installed locally: %s",
                binary_name,
                binary_path,
            )

        return True

    except Exception:
        logger.exception(
            "[FFPROBE INSTALL] Portable installation failed."
        )
        return False


def install_python_analyzers():
    """Install all required Python-side analyzers when they are missing."""
    if not AUTO_INSTALL_PYTHON_ANALYZERS:
        logger.warning("[PYTHON ANALYZERS] Automatic installation disabled by IPTV_AUTO_INSTALL_PYTHON=0")
        return

    required = [
        ("av", "av>=12.0.0", ENABLE_PYAV),
        ("faster_whisper", "faster-whisper>=1.1.0", ENABLE_WHISPER),
        ("speechbrain", "speechbrain>=1.0.0", ENABLE_SPEECHBRAIN),
    ]
    missing = [package for module, package, enabled in required
               if enabled and importlib.util.find_spec(module) is None]
    if not missing:
        logger.info("[PYTHON ANALYZERS] PyAV, faster-whisper and SpeechBrain dependencies are available.")
        return

    logger.info("[PYTHON ANALYZERS] Installing missing packages: %s", ", ".join(missing))
    command = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", *missing]
    ok = run_install_command(command, "Python media/audio analyzers")
    importlib.invalidate_caches()
    still_missing = [module for module, _package, enabled in required
                     if enabled and importlib.util.find_spec(module) is None]
    if still_missing:
        logger.error("[PYTHON ANALYZERS] Still unavailable after installation: %s", ", ".join(still_missing))
    else:
        logger.info("[PYTHON ANALYZERS] All requested Python analyzers are installed.")


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
            "ffmpeg",
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

                    if "ffprobe" in missing or "ffmpeg" in missing:
                        packages.append("ffmpeg")

                    if "tsanalyze" in missing:
                        packages.append("tsduck")

                    if "tshark" in missing:
                        packages.append("tshark")

                    if packages:
                        # Install packages independently: an unavailable TSDuck
                        # package must not prevent FFmpeg or TShark installation.
                        if "tshark" in packages:
                            run_install_command(
                                prefix + [apt, "install", "-y", "libcap2-bin"],
                                "install packet-capture capabilities helper",
                            )
                        for package in packages:
                            run_install_command(
                                prefix + [apt, "install", "-y", package],
                                f"apt package {package}",
                            )

        elif system == "darwin":
            brew = shutil.which("brew")

            if brew:
                packages = []

                if "ffprobe" in missing or "ffmpeg" in missing:
                    packages.append("ffmpeg")

                if "tsanalyze" in missing:
                    packages.append("tsduck")

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
                if "ffprobe" in missing or "ffmpeg" in missing:
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

        if not find_tool("ffprobe") or not find_tool("ffmpeg"):
            download_ffprobe_bundle()

        logger.info(
            "[TOOLS] Installation attempt finished. "
            "Some tools may still be unavailable."
        )


def configure_tshark_capture_permissions():
    """Best-effort configure dumpcap capabilities for non-root TShark capture."""
    if not (ENABLE_TSHARK and TSHARK_CAPTURE_ENABLED):
        return False
    dumpcap = shutil.which("dumpcap")
    setcap = shutil.which("setcap")
    if not dumpcap or not setcap:
        logger.warning("[TSHARK PERMISSIONS] dumpcap/setcap unavailable; capture may require elevated privileges.")
        return False
    command_prefix = []
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        sudo = shutil.which("sudo")
        if not sudo:
            logger.warning("[TSHARK PERMISSIONS] sudo unavailable; cannot grant dumpcap capabilities.")
            return False
        command_prefix = [sudo]
    ok = run_install_command(
        command_prefix + [setcap, "cap_net_raw,cap_net_admin+eip", dumpcap],
        "configure TShark dumpcap capabilities",
    )
    if ok:
        logger.info("[TSHARK PERMISSIONS] dumpcap packet capture capabilities configured.")
    return ok


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
    inventory["faster_whisper"] = bool(
        importlib.util.find_spec("faster_whisper")
    )
    inventory["speechbrain"] = bool(
        importlib.util.find_spec("speechbrain")
    )
    inventory["enabled"] = {
        "tshark": ENABLE_TSHARK and TSHARK_CAPTURE_ENABLED,
        "tsduck": ENABLE_TSDUCK,
        "pyav": ENABLE_PYAV,
        "whisper": ENABLE_WHISPER,
        "speechbrain": ENABLE_SPEECHBRAIN,
        "audio_language": AUDIO_LANGUAGE_ENABLED,
    }

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
    if not playlists:
        logger.info("[OUTPUT] Playlist index skipped: no playlists discovered")
        return
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
        text_data = rewrite_playlist_text(text_data)

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
            "final_url": rewrite_stream_url(response.url),
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

        line = rewrite_stream_url(line)

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
            "final_url": rewrite_stream_url(response.url),
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
            "final_url": rewrite_stream_url(response.url),
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
        "audio_renditions": [],
        "declared_languages": [],
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
                            rewrite_stream_url(urljoin(url, next_line))
                        )

            if line.startswith("#EXTINF"):
                if index + 1 < len(lines):
                    next_line = lines[index + 1]

                    if not next_line.startswith("#"):
                        segments.append(
                            rewrite_stream_url(urljoin(url, next_line))
                        )

            if line.startswith("#EXT-X-MEDIA:"):
                media_attributes = {}
                for match in re.finditer(
                    r'([\w-]+)=("[^"]*"|[^,]*)',
                    line.split(":", 1)[1],
                ):
                    media_attributes[match.group(1)] = (
                        match.group(2).strip('"')
                    )

                if media_attributes.get("TYPE", "").upper() == "AUDIO":
                    declared_language = normalize_language_code(
                        media_attributes.get("LANGUAGE", "")
                    )
                    rendition = {
                        "group_id": media_attributes.get("GROUP-ID", ""),
                        "name": media_attributes.get("NAME", ""),
                        "language": declared_language,
                        "default": media_attributes.get("DEFAULT", ""),
                        "autoselect": media_attributes.get("AUTOSELECT", ""),
                        "uri": safe_url(rewrite_stream_url(
                            urljoin(url, media_attributes.get("URI", ""))
                        )) if media_attributes.get("URI") else "",
                    }
                    result["audio_renditions"].append(rendition)
                    if (
                        declared_language
                        and declared_language not in result["declared_languages"]
                    ):
                        result["declared_languages"].append(declared_language)

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
                        safe_url(rewrite_stream_url(urljoin(url, key_uri)))
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

    tool = find_tool("tsanalyze") if ENABLE_TSDUCK else None

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
    """Capture and analyze real local packets for this stream host.

    A bounded semaphore prevents 45 validation workers from launching 45
    simultaneous packet captures. Both TCP and UDP are captured. The pcap is
    retained only when requested; otherwise it is parsed and removed.
    """
    global _TSHARK_SEQUENCE

    tool = find_tool("tshark")
    recorded = False
    result = {
        "available": bool(tool),
        "attempted": False,
        "enabled": TSHARK_CAPTURE_ENABLED,
        "success": False,
        "interface": TSHARK_INTERFACE,
        "capture_seconds": TSHARK_CAPTURE_SECONDS,
        "capture_workers": TSHARK_CAPTURE_WORKERS,
        "packet_count": 0,
        "tcp_conversations": 0,
        "udp_conversations": 0,
        "tcp_retransmissions": 0,
        "tcp_duplicate_acks": 0,
        "tcp_resets": 0,
        "tcp_zero_windows": 0,
        "pcap_file": "",
        "pcap_bytes": 0,
        "statistics": "",
        "jitter": "NOT_MEASURED",
        "packet_loss": "NOT_MEASURED",
        "error": "",
    }

    def store_record():
        nonlocal recorded
        if recorded or not TSHARK_CAPTURE_ENABLED:
            return
        with _TSHARK_RECORDS_LOCK:
            _TSHARK_RECORDS.append({
                "timestamp": now_string(),
                "channel": channel_name,
                "url": safe_url(url),
                **{key: value for key, value in result.items() if key != "statistics"},
                "statistics": result.get("statistics", ""),
            })
        recorded = True

    if not TSHARK_CAPTURE_ENABLED:
        result["error"] = "Capture disabled by IPTV_TSHARK_CAPTURE=0"
        return result
    if not tool:
        result["error"] = "tshark executable not found"
        store_record()
        return result

    parsed = urlparse(url)
    if not parsed.hostname:
        result["error"] = "Cannot resolve stream hostname"
        store_record()
        return result

    temporary_pcap = None
    retained_pcap = None
    try:
        addresses = socket.getaddrinfo(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
        resolved = []
        for item in addresses:
            address = item[4][0]
            if address not in resolved:
                resolved.append(address)
        if not resolved:
            raise RuntimeError("DNS returned no usable addresses")

        # Capture filter accepts both TCP and UDP. Use the first resolved
        # address to keep the BPF filter precise and avoid unrelated traffic.
        capture_filter = f"host {resolved[0]}"
        with _TSHARK_SEQUENCE_LOCK:
            _TSHARK_SEQUENCE += 1
            sequence = _TSHARK_SEQUENCE
        identity = hashlib.sha256(
            (str(sequence) + "|" + channel_name + "|" + safe_url(url)).encode("utf-8", errors="replace")
        ).hexdigest()[:12]
        safe_channel = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(channel_name)).strip("._-")[:48] or "channel"
        temporary_pcap = os.path.join(tempfile.gettempdir(), f"iptv_tshark_{sequence:07d}_{identity}.pcapng")

        result["attempted"] = True
        result["resolved_ip"] = resolved[0]
        logger.info(
            "[TSHARK START] #%s | %s | interface=%s | host=%s | filter=%s",
            sequence, channel_name, TSHARK_INTERFACE, resolved[0], capture_filter,
        )

        capture_command = [
            tool, "-i", TSHARK_INTERFACE,
            "-f", capture_filter,
            "-s", "256",
            "-a", f"duration:{TSHARK_CAPTURE_SECONDS}",
            "-a", f"filesize:{TSHARK_CAPTURE_MAX_KB}",
            "-w", temporary_pcap,
        ]

        with _TSHARK_SEMAPHORE:
            process = subprocess.run(
                capture_command,
                capture_output=True,
                text=True,
                timeout=TSHARK_CAPTURE_SECONDS + 25,
                encoding="utf-8",
                errors="replace",
            )
            first_error = (process.stderr or "").lower()
            if (
                process.returncode != 0
                and os.name == "posix"
                and shutil.which("sudo")
                and any(token in first_error for token in ("permission denied", "operation not permitted", "you don't have permission"))
            ):
                logger.warning("[TSHARK] Capture permissions denied; retrying with sudo -n")
                process = subprocess.run(
                    ["sudo", "-n"] + capture_command,
                    capture_output=True,
                    text=True,
                    timeout=TSHARK_CAPTURE_SECONDS + 25,
                    encoding="utf-8",
                    errors="replace",
                )

        capture_stderr = (process.stderr or "").strip()
        if process.returncode != 0:
            result["error"] = redact_text(capture_stderr[-3000:] or f"tshark exit code {process.returncode}")
            if not os.path.exists(temporary_pcap):
                temporary_pcap = None
                return result

        if not temporary_pcap or not os.path.isfile(temporary_pcap):
            result["error"] = result["error"] or "TShark did not create a capture file"
            return result

        result["pcap_bytes"] = os.path.getsize(temporary_pcap)
        if result["pcap_bytes"] <= 24:
            result["error"] = result["error"] or "Capture file contains no packet data"
            return result

        # Count frames directly from the capture; this is not inferred from
        # text output or HTTP response timing.
        count_command = [tool, "-r", temporary_pcap, "-T", "fields", "-e", "frame.number"]
        count_process = subprocess.run(
            count_command, capture_output=True, text=True,
            timeout=30, encoding="utf-8", errors="replace",
        )
        packet_count = sum(1 for line in (count_process.stdout or "").splitlines() if line.strip())
        result["packet_count"] = packet_count
        if packet_count <= 0:
            result["error"] = "No packets matched the stream host capture filter"
            return result

        stats_command = [
            tool, "-r", temporary_pcap, "-q",
            "-z", "io,stat,1",
            "-z", "conv,tcp",
            "-z", "conv,udp",
            "-z", "expert,tcp",
        ]
        stats_process = subprocess.run(
            stats_command, capture_output=True, text=True,
            timeout=45, encoding="utf-8", errors="replace",
        )
        stats_text = ((stats_process.stdout or "") + "\n" + (stats_process.stderr or "")).strip()
        result["statistics"] = stats_text[-16000:]
        if stats_process.returncode != 0:
            result["error"] = redact_text((stats_process.stderr or "TShark statistics command failed")[-3000:])

        # Parse actual Wireshark expert labels and conversation sections.
        for key, pattern in {
            "tcp_retransmissions": r"TCP Retransmission",
            "tcp_duplicate_acks": r"TCP Dup ACK",
            "tcp_resets": r"TCP RST",
            "tcp_zero_windows": r"TCP ZeroWindow",
        }.items():
            result[key] = len(re.findall(pattern, stats_text, re.IGNORECASE))
        tcp_section = re.split(r"UDP Conversations", stats_text, maxsplit=1, flags=re.IGNORECASE)[0]
        udp_section = stats_text.split("UDP Conversations", 1)[-1] if "UDP Conversations" in stats_text else ""
        result["tcp_conversations"] = len(re.findall(r"<->", tcp_section))
        result["udp_conversations"] = len(re.findall(r"<->", udp_section))
        result["success"] = bool(packet_count > 0 and stats_process.returncode == 0)

        if TSHARK_KEEP_PCAP:
            retained_pcap = os.path.join(TSHARK_PCAP_DIR, f"{sequence:07d}_{safe_channel}_{identity}.pcapng")
            shutil.move(temporary_pcap, retained_pcap)
            temporary_pcap = None
            result["pcap_file"] = os.path.relpath(retained_pcap, OUTPUT_DIR)
            result["pcap_bytes"] = os.path.getsize(retained_pcap)

        logger.info(
            "[TSHARK RESULT] #%s | success=%s | packets=%s | tcp_retrans=%s | dup_ack=%s | tcp_rst=%s | pcap=%s",
            sequence, result["success"], result["packet_count"],
            result["tcp_retransmissions"], result["tcp_duplicate_acks"],
            result["tcp_resets"], result["pcap_file"] or "temporary capture removed",
        )
        log_event(
            "TSHARK_CAPTURE",
            channel=channel_name,
            url=safe_url(url),
            **{key: value for key, value in result.items() if key != "statistics"},
        )
        return result

    except subprocess.TimeoutExpired:
        result["error"] = "TShark capture or analysis timed out"
        logger.warning("[TSHARK TIMEOUT] %s", channel_name)
        return result
    except Exception as error:
        result["error"] = redact_text(str(error))
        logger.warning("[TSHARK ERROR] %s | %s", channel_name, result["error"])
        log_event("TSHARK_ERROR", channel=channel_name, url=safe_url(url), error=result["error"])
        return result
    finally:
        if temporary_pcap and os.path.exists(temporary_pcap):
            try:
                os.remove(temporary_pcap)
            except OSError:
                logger.warning("[TSHARK] Could not remove temporary capture: %s", temporary_pcap)
        store_record()


def export_tshark_reports():
    """Write non-empty TShark reports only when captures were attempted."""
    with _TSHARK_RECORDS_LOCK:
        records = list(_TSHARK_RECORDS)
    if not records:
        logger.info("[TSHARK EXPORT] Skipped: no stream records were supplied to TShark")
        return []

    csv_path = os.path.join(TSHARK_DIR, "TShark_Network_Audit.csv")
    json_path = os.path.join(TSHARK_DIR, "TShark_Network_Audit.json")
    jsonl_path = os.path.join(TSHARK_DIR, "TShark_Network_Audit.jsonl")
    log_path = os.path.join(TSHARK_DIR, "TShark_Network_Audit.log")
    summary_path = os.path.join(TSHARK_DIR, "TShark_Summary.txt")

    fields = [
        "timestamp", "channel", "url", "available", "attempted", "enabled",
        "success", "interface", "resolved_ip", "capture_seconds", "capture_workers",
        "packet_count", "tcp_conversations", "udp_conversations",
        "tcp_retransmissions", "tcp_duplicate_acks", "tcp_resets", "tcp_zero_windows",
        "pcap_file", "pcap_bytes", "jitter", "packet_loss", "error",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    safe_records = _sanitize_audit_value(records)
    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(safe_records, file, ensure_ascii=False, indent=2, default=str)
    with open(jsonl_path, "w", encoding="utf-8") as file:
        for record in safe_records:
            file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    successful = [record for record in records if record.get("success")]
    packet_total = sum(safe_int(record.get("packet_count")) for record in records)
    lines = [
        "TSHARK NETWORK AUDIT",
        "=" * 76,
        f"Generated: {now_string()}",
        f"Stream capture requests: {len(records)}",
        f"Actual capture attempts: {sum(1 for record in records if record.get('attempted'))}",
        f"Successful captures: {len(successful)}",
        f"Unavailable/failed/no-packet records: {len(records) - len(successful)}",
        f"Total captured packets: {packet_total}",
        f"Capture workers: {TSHARK_CAPTURE_WORKERS}",
        f"Capture duration per stream: {TSHARK_CAPTURE_SECONDS}s",
        f"PCAP retention enabled: {TSHARK_KEEP_PCAP}",
        "",
        "PER-STREAM RESULTS",
        "-" * 76,
    ]
    for record in records:
        lines.append(
            f"{record.get('channel')} | packets={record.get('packet_count')} | "
            f"success={record.get('success')} | retrans={record.get('tcp_retransmissions')} | "
            f"dup_ack={record.get('tcp_duplicate_acks')} | rst={record.get('tcp_resets')} | "
            f"zero_window={record.get('tcp_zero_windows')} | error={record.get('error', '')}"
        )
    lines.extend([
        "",
        "INTERPRETATION",
        "-" * 76,
        "Packet counts and TCP events are taken from captured packets.",
        "TCP retransmissions, duplicate ACKs and resets are transport diagnostics, not proof of media loss.",
        "RTP jitter/loss are not claimed unless a valid RTP stream analysis provides them.",
        "Encrypted TLS payload contents are not decoded by this report.",
    ])
    with open(log_path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")
    with open(summary_path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines[:12]) + "\n")

    logger.info("[TSHARK EXPORT] attempts=%s success=%s reports=%s", len(records), len(successful), TSHARK_DIR)
    return [csv_path, json_path, jsonl_path, log_path, summary_path]


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

    if not ENABLE_PYAV:
        result["error"] = "PyAV disabled by IPTV_ENABLE_PYAV=0"
        return result

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
# AUDIO LANGUAGE IDENTIFICATION
# ============================================================

LANGUAGE_NAMES = {
    "ar": "Arabic", "az": "Azerbaijani", "be": "Belarusian",
    "bg": "Bulgarian", "bn": "Bengali", "bs": "Bosnian",
    "ca": "Catalan", "cs": "Czech", "cy": "Welsh",
    "da": "Danish", "de": "German", "el": "Greek",
    "en": "English", "es": "Spanish", "et": "Estonian",
    "fa": "Persian", "fi": "Finnish", "fr": "French",
    "ga": "Irish", "he": "Hebrew", "hi": "Hindi",
    "hr": "Croatian", "hu": "Hungarian", "hy": "Armenian",
    "id": "Indonesian", "is": "Icelandic", "it": "Italian",
    "ja": "Japanese", "ka": "Georgian", "kk": "Kazakh",
    "ko": "Korean", "lt": "Lithuanian", "lv": "Latvian",
    "mk": "Macedonian", "ms": "Malay", "mt": "Maltese",
    "nl": "Dutch", "no": "Norwegian", "pl": "Polish",
    "pt": "Portuguese", "ro": "Romanian", "ru": "Russian",
    "sk": "Slovak", "sl": "Slovenian", "sq": "Albanian",
    "sr": "Serbian", "sv": "Swedish", "sw": "Swahili",
    "ta": "Tamil", "te": "Telugu", "th": "Thai",
    "tr": "Turkish", "uk": "Ukrainian", "ur": "Urdu",
    "uz": "Uzbek", "vi": "Vietnamese", "zh": "Chinese",
}


LANGUAGE_CODE_ALIASES = {
    "eng": "en", "rus": "ru", "deu": "de", "ger": "de",
    "fra": "fr", "fre": "fr", "spa": "es", "ita": "it",
    "por": "pt", "zho": "zh", "chi": "zh", "jpn": "ja",
    "kor": "ko", "ukr": "uk", "ara": "ar", "hin": "hi",
    "tur": "tr", "pol": "pl", "nld": "nl", "dut": "nl",
    "ces": "cs", "cze": "cs", "ron": "ro", "rum": "ro",
    "swe": "sv", "nor": "no", "dan": "da", "fin": "fi",
    "ell": "el", "gre": "el", "heb": "he", "fas": "fa",
    "per": "fa", "urd": "ur", "vie": "vi", "tha": "th",
    "ind": "id", "msa": "ms", "may": "ms", "ben": "bn",
    "tam": "ta", "tel": "te",
}


def normalize_language_code(value):
    value = str(value or "").strip().lower().replace("_", "-")
    if not value or value in ("und", "unknown", "unk", "none", "n/a"):
        return ""
    match = re.search(r"\b([a-z]{2,3})(?:[-_:]|$)", value)
    if not match:
        return ""
    code = match.group(1)
    return LANGUAGE_CODE_ALIASES.get(code, code)


def language_name(code):
    code = normalize_language_code(code)
    return LANGUAGE_NAMES.get(code, code.upper() if code else "UNKNOWN")


def _audio_ffmpeg_headers(request_headers):
    allowed = {
        "authorization", "cookie", "referer", "user-agent", "origin"
    }
    lines = []
    for key, value in (request_headers or {}).items():
        if str(key).lower() in allowed and value:
            lines.append(f"{key}: {value}")
    return "\r\n".join(lines) + ("\r\n" if lines else "")


def extract_audio_sample(url, channel_name, track, request_headers=None):
    """
    Decode a short mono 16 kHz PCM sample from one FFprobe audio stream.
    Extraction is separately throttled so 39 queued workers do not spawn
    39 simultaneous full FFmpeg decoders on a small hosted runner.
    """
    ffmpeg = find_tool("ffmpeg")
    result = {
        "success": False,
        "path": "",
        "stream_index": track.get("index", ""),
        "bytes": 0,
        "duration_seconds": AUDIO_SAMPLE_SECONDS,
        "error": "",
    }

    if not ffmpeg:
        result["error"] = "ffmpeg not installed"
        return result

    temp_path = None
    try:
        with _AUDIO_EXTRACTION_SEMAPHORE:
            fd, temp_path = tempfile.mkstemp(
                suffix=".wav",
                prefix="iptv_language_",
                dir=TOOLS_DIR,
            )
            os.close(fd)

            command = [
                ffmpeg,
                "-nostdin",
                "-hide_banner",
                "-loglevel", "error",
                "-rw_timeout", "20000000",
            ]

            header_text = _audio_ffmpeg_headers(request_headers)
            if header_text:
                command.extend(["-headers", header_text])

            command.extend([
                "-i", rewrite_stream_url(url),
                "-map", f"0:{track.get('index', 0)}",
                "-t", str(AUDIO_SAMPLE_SECONDS),
                "-vn",
                "-ac", "1",
                "-ar", "16000",
                "-c:a", "pcm_s16le",
                "-f", "wav",
                "-y",
                temp_path,
            ])

            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=max(45, AUDIO_SAMPLE_SECONDS * 3),
                encoding="utf-8",
                errors="replace",
            )

            result["bytes"] = (
                os.path.getsize(temp_path)
                if os.path.exists(temp_path)
                else 0
            )

            if process.returncode != 0 or result["bytes"] < 16000:
                result["error"] = redact_text(
                    process.stderr[-1200:] or
                    f"FFmpeg audio extraction exit={process.returncode}"
                )
                return result

            result["success"] = True
            result["path"] = temp_path
            return result

    except subprocess.TimeoutExpired:
        result["error"] = "Audio extraction timeout"
        return result
    except Exception as error:
        result["error"] = redact_text(error)
        return result


def _get_whisper_model():
    global _WHISPER_MODEL, _WHISPER_MODEL_ERROR
    if _WHISPER_MODEL is not None:
        return _WHISPER_MODEL
    if _WHISPER_MODEL_ERROR:
        return None

    with _WHISPER_MODEL_LOCK:
        if _WHISPER_MODEL is not None:
            return _WHISPER_MODEL
        if _WHISPER_MODEL_ERROR:
            return None

        try:
            if not ENABLE_WHISPER:
                raise RuntimeError("Whisper disabled by IPTV_ENABLE_WHISPER=0")
            from faster_whisper import WhisperModel

            model_cache = os.path.join(MODEL_CACHE_DIR, "faster-whisper")
            os.makedirs(model_cache, exist_ok=True)

            logger.info(
                "[LANGUAGE] Loading faster-whisper model=%s on CPU/int8",
                WHISPER_MODEL_SIZE,
            )

            _WHISPER_MODEL = WhisperModel(
                WHISPER_MODEL_SIZE,
                device="cpu",
                compute_type="int8",
                cpu_threads=max(1, min(4, AUDIO_INFERENCE_SLOTS)),
                num_workers=1,
                download_root=model_cache,
            )
            return _WHISPER_MODEL

        except Exception as error:
            _WHISPER_MODEL_ERROR = redact_text(error)
            logger.warning(
                "[LANGUAGE] faster-whisper model initialization failed: %s",
                _WHISPER_MODEL_ERROR,
            )
            return None

def identify_with_whisper(wav_path):
    result = {
        "engine": "faster-whisper",
        "available": False,
        "attempted": False,
        "language": "",
        "language_name": "UNKNOWN",
        "confidence": 0.0,
        "error": "",
    }

    try:
        model = _get_whisper_model()
        if model is None:
            result["error"] = "faster-whisper not installed or model unavailable"
            return result

        result["available"] = True
        result["attempted"] = True

        with _AUDIO_INFERENCE_SEMAPHORE:
            segments, info = model.transcribe(
                wav_path,
                beam_size=1,
                best_of=1,
                vad_filter=True,
                without_timestamps=True,
            )
            # faster-whisper executes lazily; consume a few segments so
            # the language result and VAD/transcription path are exercised.
            for index, _segment in enumerate(segments):
                if index >= 4:
                    break

        code = normalize_language_code(getattr(info, "language", ""))
        confidence = safe_float(
            getattr(info, "language_probability", 0.0)
        )

        result.update({
            "language": code,
            "language_name": language_name(code),
            "confidence": round(confidence, 4),
        })

        if not code:
            result["error"] = "Whisper returned no language"
    except Exception as error:
        result["error"] = redact_text(error)
        logger.warning("[LANGUAGE WHISPER ERROR] %s", result["error"])

    return result


def _get_speechbrain_model():
    global _SPEECHBRAIN_MODEL, _SPEECHBRAIN_MODEL_ERROR
    if not ENABLE_SPEECHBRAIN:
        return None
    if _SPEECHBRAIN_MODEL is not None:
        return _SPEECHBRAIN_MODEL
    if _SPEECHBRAIN_MODEL_ERROR:
        return None

    with _SPEECHBRAIN_MODEL_LOCK:
        if _SPEECHBRAIN_MODEL is not None:
            return _SPEECHBRAIN_MODEL
        if _SPEECHBRAIN_MODEL_ERROR:
            return None

        try:
            try:
                from speechbrain.inference.classifiers import EncoderClassifier
            except ImportError:
                from speechbrain.pretrained import EncoderClassifier

            model_dir = os.path.join(
                MODEL_CACHE_DIR,
                "speechbrain-voxlingua107",
            )
            os.makedirs(model_dir, exist_ok=True)

            logger.info(
                "[LANGUAGE] Loading SpeechBrain VoxLingua107 model"
            )

            _SPEECHBRAIN_MODEL = EncoderClassifier.from_hparams(
                source="speechbrain/lang-id-voxlingua107-ecapa",
                savedir=model_dir,
                run_opts={"device": "cpu"},
            )
            return _SPEECHBRAIN_MODEL

        except Exception as error:
            _SPEECHBRAIN_MODEL_ERROR = redact_text(error)
            logger.warning(
                "[LANGUAGE] SpeechBrain initialization failed: %s",
                _SPEECHBRAIN_MODEL_ERROR,
            )
            return None

def identify_with_speechbrain(wav_path):
    result = {
        "engine": "SpeechBrain VoxLingua107",
        "available": False,
        "attempted": False,
        "language": "",
        "language_name": "UNKNOWN",
        "confidence": 0.0,
        "error": "",
    }

    try:
        model = _get_speechbrain_model()
        if model is None:
            result["error"] = (
                "SpeechBrain disabled, not installed, or model unavailable"
            )
            return result

        result["available"] = True
        result["attempted"] = True

        with _AUDIO_INFERENCE_SEMAPHORE:
            classifier_output = model.classify_file(wav_path)

        if len(classifier_output) == 4:
            out_prob, score, index, text_lab = classifier_output
        elif len(classifier_output) == 3:
            out_prob, index, text_lab = classifier_output
            score = None
        else:
            raise RuntimeError(
                f"Unexpected SpeechBrain classify_file result length: {len(classifier_output)}"
            )

        label = ""
        if text_lab is not None:
            try:
                label = str(text_lab[0])
            except Exception:
                label = str(text_lab)

        code = normalize_language_code(label)
        confidence = 0.0

        def scalar_probability(value):
            try:
                return float(value[0].detach().cpu().item())
            except Exception:
                try:
                    return float(value[0])
                except Exception:
                    try:
                        return float(value.detach().cpu().item())
                    except Exception:
                        try:
                            return float(value)
                        except Exception:
                            return 0.0

        if score is not None:
            confidence = scalar_probability(score)
        if confidence <= 0 and out_prob is not None:
            try:
                confidence = float(out_prob.max().detach().cpu().item())
            except Exception:
                confidence = scalar_probability(out_prob)

        result.update({
            "language": code,
            "language_name": language_name(code),
            "confidence": round(confidence, 4),
            "label": label,
        })

        if not code:
            result["error"] = "SpeechBrain returned an unrecognized label"

    except Exception as error:
        result["error"] = redact_text(error)
        logger.warning("[LANGUAGE SPEECHBRAIN ERROR] %s", result["error"])

    return result


def combine_language_engines(whisper_result, speechbrain_result):
    whisper_code = whisper_result.get("language", "")
    speechbrain_code = speechbrain_result.get("language", "")
    whisper_conf = safe_float(whisper_result.get("confidence", 0))
    speechbrain_conf = safe_float(speechbrain_result.get("confidence", 0))

    if whisper_code and speechbrain_code:
        if whisper_code == speechbrain_code:
            confidence = (
                whisper_conf * 0.55 + speechbrain_conf * 0.45
            )
            return {
                "detected_language": whisper_code,
                "language_name": language_name(whisper_code),
                "confidence": round(confidence, 4),
                "status": "CONFIRMED",
                "agreement": True,
            }

        return {
            "detected_language": "CONFLICT",
            "language_name": "CONFLICT",
            "confidence": round(max(whisper_conf, speechbrain_conf), 4),
            "status": "CONFLICT",
            "agreement": False,
        }

    if whisper_code:
        return {
            "detected_language": whisper_code,
            "language_name": language_name(whisper_code),
            "confidence": round(whisper_conf, 4),
            "status": (
                "PROVISIONAL" if whisper_conf >= WHISPER_THRESHOLD
                else "LOW_CONFIDENCE"
            ),
            "agreement": None,
        }

    if speechbrain_code:
        return {
            "detected_language": speechbrain_code,
            "language_name": language_name(speechbrain_code),
            "confidence": round(speechbrain_conf, 4),
            "status": (
                "PROVISIONAL" if speechbrain_conf >= SPEECHBRAIN_THRESHOLD
                else "LOW_CONFIDENCE"
            ),
            "agreement": None,
        }

    return {
        "detected_language": "UNKNOWN",
        "language_name": "UNKNOWN",
        "confidence": 0.0,
        "status": "UNKNOWN",
        "agreement": None,
    }


def analyze_audio_track(
    url,
    channel_name,
    request_headers,
    track,
    record_id="",
    track_number=0,
    engine_executor=None,
):
    """Analyze one FFprobe audio stream without blocking stream-check workers."""
    started = time.perf_counter()
    stream_index = track.get("index", "")
    track_id = f"{record_id or 'UNASSIGNED'}:audio:{stream_index}"

    logger.info(
        "[AUDIO TRACK START] record=%s | track_id=%s | channel=%s | stream_index=%s | worker=%s",
        record_id or "UNASSIGNED",
        track_id,
        channel_name,
        stream_index,
        threading.current_thread().name,
    )

    extracted = extract_audio_sample(
        url,
        channel_name,
        track,
        request_headers,
    )

    track_result = {
        "record_id": record_id,
        "track_id": track_id,
        "track_number": track_number,
        "stream_index": stream_index,
        "metadata_language": normalize_language_code(
            track.get("language", "")
        ),
        "codec_name": track.get("codec_name", ""),
        "channels": track.get("channels", 0),
        "channel_layout": track.get("channel_layout", ""),
        "sample_rate": track.get("sample_rate", ""),
        "bit_rate": track.get("bit_rate", ""),
        "detected_language": "UNKNOWN",
        "language_name": "UNKNOWN",
        "confidence": 0.0,
        "status": "UNKNOWN",
        "audio_sample_bytes": extracted.get("bytes", 0),
        "whisper": {},
        "speechbrain": {},
        "error": extracted.get("error", ""),
    }

    wav_path = extracted.get("path")
    if wav_path:
        try:
            if engine_executor is not None:
                whisper_future = engine_executor.submit(
                    identify_with_whisper,
                    wav_path,
                )
                speechbrain_future = engine_executor.submit(
                    identify_with_speechbrain,
                    wav_path,
                )
                try:
                    whisper_result = whisper_future.result()
                except Exception as error:
                    whisper_result = {
                        "engine": "faster-whisper",
                        "available": False,
                        "attempted": False,
                        "language": "",
                        "confidence": 0.0,
                        "error": redact_text(error),
                    }
                try:
                    speechbrain_result = speechbrain_future.result()
                except Exception as error:
                    speechbrain_result = {
                        "engine": "SpeechBrain VoxLingua107",
                        "available": False,
                        "attempted": False,
                        "language": "",
                        "confidence": 0.0,
                        "error": redact_text(error),
                    }
            else:
                whisper_result = identify_with_whisper(wav_path)
                speechbrain_result = identify_with_speechbrain(wav_path)

            combined = combine_language_engines(
                whisper_result,
                speechbrain_result,
            )
            track_result.update(combined)
            track_result["whisper"] = whisper_result
            track_result["speechbrain"] = speechbrain_result
        finally:
            try:
                os.remove(wav_path)
            except OSError:
                pass

    track_result["analysis_ms"] = round(
        (time.perf_counter() - started) * 1000,
        2,
    )

    logger.info(
        "[AUDIO TRACK FINISHED] record=%s | track_id=%s | language=%s | status=%s | confidence=%.3f | extraction_bytes=%s | total_ms=%.2f",
        record_id or "UNASSIGNED",
        track_id,
        track_result.get("detected_language", "UNKNOWN"),
        track_result.get("status", "UNKNOWN"),
        safe_float(track_result.get("confidence", 0)),
        track_result.get("audio_sample_bytes", 0),
        track_result["analysis_ms"],
    )

    log_event(
        "AUDIO_TRACK_ANALYSIS_FINISHED",
        record_id=record_id,
        track_id=track_id,
        channel=channel_name,
        stream_index=stream_index,
        metadata_language=track_result.get("metadata_language", ""),
        detected_language=track_result.get("detected_language", "UNKNOWN"),
        confidence=track_result.get("confidence", 0),
        status=track_result.get("status", "UNKNOWN"),
        audio_sample_bytes=track_result.get("audio_sample_bytes", 0),
        whisper=track_result.get("whisper", {}),
        speechbrain=track_result.get("speechbrain", {}),
        error=track_result.get("error", ""),
        analysis_ms=track_result["analysis_ms"],
    )
    return track_result


def detect_channel_language(
    url,
    channel_name,
    request_headers,
    audio_tracks,
    record_id="",
):
    """Coordinate independently scheduled track analyses for one record."""
    started = time.perf_counter()
    result = {
        "enabled": AUDIO_LANGUAGE_ENABLED,
        "workers": AUDIO_LANGUAGE_WORKERS,
        "track_workers": AUDIO_TRACK_WORKERS,
        "engine_workers": AUDIO_ENGINE_WORKERS,
        "record_id": record_id,
        "detected_language": "UNKNOWN",
        "language_name": "UNKNOWN",
        "confidence": 0.0,
        "status": "UNKNOWN",
        "metadata_languages": [],
        "track_results": [],
        "engines": ["faster-whisper", "SpeechBrain VoxLingua107"],
        "error": "",
    }

    if not AUDIO_LANGUAGE_ENABLED:
        result["status"] = "DISABLED"
        result["error"] = "Audio language detection disabled by configuration"
        return result

    if not audio_tracks:
        result["status"] = "NO_AUDIO_TRACK"
        result["error"] = "FFprobe did not find an audio track"
        return result

    result["metadata_languages"] = sorted({
        normalize_language_code(track.get("language", ""))
        for track in audio_tracks
        if normalize_language_code(track.get("language", ""))
    })

    selected_tracks = list(audio_tracks)
    result["tracks_discovered"] = len(audio_tracks)
    result["tracks_scheduled"] = len(selected_tracks)
    result["tracks_not_scheduled"] = len(audio_tracks) - len(selected_tracks)

    logger.info(
        "[AUDIO CHANNEL QUEUE] record=%s | channel=%s | discovered_tracks=%s | scheduled_tracks=%s | track_pool=%s | engine_pool=%s",
        record_id or "UNASSIGNED",
        channel_name,
        len(audio_tracks),
        len(selected_tracks),
        AUDIO_TRACK_WORKERS,
        AUDIO_ENGINE_WORKERS,
    )

    track_executor = _AUDIO_TRACK_EXECUTOR
    engine_executor = _AUDIO_ENGINE_EXECUTOR
    track_futures = {}

    if track_executor is not None:
        for track_number, track in enumerate(selected_tracks, 1):
            future = track_executor.submit(
                analyze_audio_track,
                url,
                channel_name,
                request_headers or {},
                track,
                record_id,
                track_number,
                engine_executor,
            )
            track_futures[future] = (track_number, track)

        completed_tracks = {}
        for future in as_completed(track_futures):
            track_number, track = track_futures[future]
            try:
                completed_tracks[track_number] = future.result()
            except Exception as error:
                stream_index = track.get("index", "")
                track_id = f"{record_id or 'UNASSIGNED'}:audio:{stream_index}"
                logger.exception(
                    "[AUDIO TRACK ERROR] record=%s | track_id=%s | channel=%s",
                    record_id,
                    track_id,
                    channel_name,
                )
                completed_tracks[track_number] = {
                    "record_id": record_id,
                    "track_id": track_id,
                    "track_number": track_number,
                    "stream_index": stream_index,
                    "metadata_language": normalize_language_code(
                        track.get("language", "")
                    ),
                    "detected_language": "UNKNOWN",
                    "language_name": "UNKNOWN",
                    "confidence": 0.0,
                    "status": "ERROR",
                    "audio_sample_bytes": 0,
                    "whisper": {},
                    "speechbrain": {},
                    "error": redact_text(error),
                }
        result["track_results"] = [
            completed_tracks[number]
            for number in sorted(completed_tracks)
        ]
    else:
        # Defensive fallback for direct unit calls outside the orchestrator.
        result["track_results"] = [
            analyze_audio_track(
                url,
                channel_name,
                request_headers or {},
                track,
                record_id,
                track_number,
                None,
            )
            for track_number, track in enumerate(selected_tracks, 1)
        ]

    accepted = [
        item for item in result["track_results"]
        if item.get("detected_language")
        not in ("", "UNKNOWN", "CONFLICT")
        and item.get("status") in ("CONFIRMED", "PROVISIONAL")
    ]
    languages = sorted({item["detected_language"] for item in accepted})

    if len(languages) == 1:
        selected = languages[0]
        selected_items = [
            item for item in accepted
            if item["detected_language"] == selected
        ]
        result.update({
            "detected_language": selected,
            "language_name": language_name(selected),
            "confidence": round(
                sum(safe_float(item.get("confidence")) for item in selected_items)
                / len(selected_items),
                4,
            ),
            "status": (
                "CONFIRMED"
                if all(item.get("status") == "CONFIRMED" for item in selected_items)
                else "PROVISIONAL"
            ),
        })
    elif len(languages) > 1:
        result.update({
            "detected_language": "MULTI_LANGUAGE",
            "language_name": "MULTI_LANGUAGE",
            "confidence": round(
                sum(safe_float(item.get("confidence")) for item in accepted)
                / len(accepted),
                4,
            ),
            "status": "MULTI_LANGUAGE",
        })
    elif any(
        item.get("status") == "CONFLICT"
        for item in result["track_results"]
    ):
        result["detected_language"] = "CONFLICT"
        result["language_name"] = "CONFLICT"
        result["status"] = "CONFLICT"
    elif any(
        item.get("status") == "LOW_CONFIDENCE"
        for item in result["track_results"]
    ):
        result["detected_language"] = "UNKNOWN"
        result["language_name"] = "UNKNOWN"
        result["status"] = "LOW_CONFIDENCE"
    else:
        errors = [
            item.get("error", "")
            for item in result["track_results"]
            if item.get("error")
        ]
        result["error"] = "; ".join(errors)

    result["analysis_ms"] = round(
        (time.perf_counter() - started) * 1000,
        2,
    )

    logger.info(
        "[AUDIO LANGUAGE] record=%s | %s | language=%s | status=%s | confidence=%.3f | tracks=%s/%s | %.2f ms",
        record_id or "UNASSIGNED",
        channel_name,
        result["detected_language"],
        result["status"],
        result["confidence"],
        len(result["track_results"]),
        len(audio_tracks),
        result["analysis_ms"],
    )

    log_event(
        "AUDIO_LANGUAGE_DETECTION",
        record_id=record_id,
        channel=channel_name,
        url=safe_url(url),
        detected_language=result["detected_language"],
        language_name=result["language_name"],
        confidence=result["confidence"],
        status=result["status"],
        metadata_languages=result["metadata_languages"],
        tracks_discovered=result["tracks_discovered"],
        tracks_scheduled=result["tracks_scheduled"],
        tracks_not_scheduled=result["tracks_not_scheduled"],
        track_results=result["track_results"],
        analysis_ms=result["analysis_ms"],
    )
    return result


def classify_channel_region(channel):
    """
    Conservative RU/WORLD classification.
    Explicit country metadata wins; then group/title; then detected audio
    language. Unknown/conflicting evidence is not silently labeled RU.
    """
    attrs = channel.get("attributes", {}) or {}
    validation = channel.get("validation", {}) or {}
    language = validation.get("language_detection", {}) or {}

    country_value = " ".join(str(attrs.get(key, "")) for key in (
        "tvg-country", "tvg-country-code", "country", "country-code"
    )).strip().lower()
    country_normalized = re.sub(r"[^a-zа-яё0-9]+", " ", country_value).strip()

    russian_country_values = {
        "ru", "rus", "russia", "russian", "russian federation",
        "россия", "российская федерация",
    }
    if country_normalized in russian_country_values:
        return "RU"

    if country_value:
        return "WORLD"

    group_and_name = (
        str(channel.get("group_title", "")) + " " +
        str(channel.get("name", ""))
    ).lower()

    if any(token in group_and_name for token in (
        "россия", "россий", "russia", "russian"
    )):
        return "RU"

    if any(token in group_and_name for token in (
        "world", "international", "international tv", "мир", "зарубеж"
    )):
        return "WORLD"

    detected = language.get("detected_language", "")
    metadata_candidates = {
        normalize_language_code(channel.get("language", "")),
        *(
            normalize_language_code(item)
            for item in language.get("metadata_languages", [])
        ),
    }
    metadata_candidates.discard("")

    if detected == "ru":
        return "RU"

    if detected in LANGUAGE_NAMES and detected != "ru":
        return "WORLD"

    if not detected or detected in ("UNKNOWN", "CONFLICT", "MULTI_LANGUAGE"):
        if metadata_candidates == {"ru"}:
            return "RU"
        if len(metadata_candidates) == 1 and next(iter(metadata_candidates)) != "ru":
            return "WORLD"

    return "UNKNOWN"


def export_language_reports(channels):
    if not channels:
        logger.info("[LANGUAGE EXPORT] Skipped: no channel records")
        return
    csv_path = os.path.join(DATABASE_DIR, "Language_Detection.csv")
    json_path = os.path.join(DATABASE_DIR, "Language_Detection.json")
    jsonl_path = os.path.join(LOG_DIR, "Audio_Language_Audit.jsonl")
    text_path = os.path.join(OUTPUT_DIR, "Language_Summary.txt")

    rows = []
    for index, channel in enumerate(channels, 1):
        validation = channel.get("validation", {}) or {}
        detection = validation.get("language_detection", {}) or {}
        row = {
            "record_number": index,
            "channel": channel.get("name", ""),
            "url": safe_url(channel.get("url", "")),
            "playlist": channel.get("playlist", ""),
            "metadata_language": channel.get("language", ""),
            "metadata_audio_languages": "|".join(
                detection.get("metadata_languages", [])
            ),
            "detected_language": detection.get("detected_language", "UNKNOWN"),
            "language_name": detection.get("language_name", "UNKNOWN"),
            "confidence": detection.get("confidence", 0),
            "language_status": detection.get("status", "UNKNOWN"),
            "region_class": classify_channel_region(channel),
            "audio_analysis_ms": detection.get("analysis_ms", ""),
            "audio_error": redact_text(detection.get("error", "")),
            "track_results_json": json.dumps(
                detection.get("track_results", []),
                ensure_ascii=False,
            ),
        }
        rows.append(row)

    fields = list(rows[0].keys()) if rows else [
        "record_number", "channel", "url", "playlist",
        "metadata_language", "metadata_audio_languages",
        "detected_language", "language_name", "confidence",
        "language_status", "region_class", "audio_analysis_ms",
        "audio_error", "track_results_json",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(rows, file, ensure_ascii=False, indent=2)

    tracks_csv_path = os.path.join(
        DATABASE_DIR,
        "Audio_Language_Tracks.csv",
    )
    track_fields = [
        "record_number", "channel", "playlist", "url",
        "audio_stream_index", "metadata_language",
        "whisper_language", "whisper_confidence",
        "speechbrain_language", "speechbrain_confidence",
        "combined_language", "combined_confidence",
        "language_status", "error",
    ]
    track_rows = []
    for index, channel in enumerate(channels, 1):
        detection = (channel.get("validation", {}) or {}).get("language_detection", {}) or {}
        for track in detection.get("track_results", []):
            whisper = track.get("whisper", {}) or {}
            speechbrain = track.get("speechbrain", {}) or {}
            track_rows.append({
                "record_number": index,
                "channel": channel.get("name", ""),
                "playlist": channel.get("playlist", ""),
                "url": safe_url(channel.get("url", "")),
                "audio_stream_index": track.get("stream_index", ""),
                "metadata_language": track.get("metadata_language", ""),
                "whisper_language": whisper.get("language", ""),
                "whisper_confidence": whisper.get("confidence", ""),
                "speechbrain_language": speechbrain.get("language", ""),
                "speechbrain_confidence": speechbrain.get("confidence", ""),
                "combined_language": track.get("detected_language", ""),
                "combined_confidence": track.get("confidence", ""),
                "language_status": track.get("status", ""),
                "error": redact_text(track.get("error", "")),
            })
    if track_rows:
        with open(tracks_csv_path, "w", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=track_fields)
            writer.writeheader()
            writer.writerows(track_rows)
    elif os.path.exists(tracks_csv_path):
        os.remove(tracks_csv_path)

    with open(jsonl_path, "w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")

    counts = defaultdict(int)
    for row in rows:
        counts[row["detected_language"]] += 1

    audio_log_path = os.path.join(LOG_DIR, "Audio_Language_Audit.log")
    with open(audio_log_path, "w", encoding="utf-8") as file:
        file.write("AUDIO LANGUAGE AUDIT\n")
        file.write("=" * 72 + "\n")
        for row in rows:
            file.write(
                f"#{row['record_number']} | {row['channel']} | "
                f"{row['detected_language']} ({row['language_name']}) | "
                f"confidence={row['confidence']} | status={row['language_status']} | "
                f"region={row['region_class']} | url={row['url']}\n"
            )
            if row["audio_error"]:
                file.write(f"    ERROR: {row['audio_error']}\n")

    with open(text_path, "w", encoding="utf-8") as file:
        file.write("IPTV AUDIO LANGUAGE DETECTION SUMMARY\n")
        file.write("=" * 72 + "\n")
        file.write(f"Generated: {now_string()}\n")
        file.write(f"Records analyzed: {len(rows)}\n")
        file.write(f"Audio channel workers: {AUDIO_LANGUAGE_WORKERS}\n")
        file.write(f"Audio track workers: {AUDIO_TRACK_WORKERS}\n")
        file.write(f"Audio engine workers: {AUDIO_ENGINE_WORKERS}\n")
        file.write(f"Audio max tracks per record: {AUDIO_MAX_TRACKS_PER_CHANNEL or 'ALL'}\n")
        file.write(f"Whisper model: {WHISPER_MODEL_SIZE}\n")
        file.write(f"SpeechBrain enabled: {ENABLE_SPEECHBRAIN}\n\n")
        file.write("LANGUAGE COUNTS\n")
        file.write("-" * 72 + "\n")
        for code, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
            file.write(f"{code}: {count}\n")
        file.write("\nREGION CLASSIFICATION\n")
        for region in ("RU", "WORLD", "UNKNOWN"):
            count = sum(
                1 for channel in channels
                if classify_channel_region(channel) == region
            )
            file.write(f"{region}: {count}\n")

        file.write("\nSTATUSES\n")
        for status in sorted({
            (channel.get("validation", {}) or {})
            .get("language_detection", {}).get("status", "UNKNOWN")
            for channel in channels
        }):
            count = sum(
                1 for channel in channels
                if (channel.get("validation", {}) or {})
                .get("language_detection", {}).get("status", "UNKNOWN") == status
            )
            file.write(f"{status}: {count}\n")

    logger.info(
        "[LANGUAGE EXPORT] CSV=%s | JSON=%s | TRACKS=%s | JSONL=%s | SUMMARY=%s",
        csv_path, json_path, tracks_csv_path, jsonl_path, text_path,
    )
    logger.info("[LANGUAGE EXPORT] Human-readable audio log=%s", audio_log_path)


def export_regional_playlists(channels):
    export_m3u(channels, "Rafail_donor_Ru.m3u", mode="region_ru")
    export_m3u(channels, "Rafail_donor_mir.m3u", mode="region_world")
    export_m3u(channels, "Rafail_stable.m3u", mode="stable")


# ============================================================
# STREAM VALIDATION
# ============================================================

def validate_url(url, channel_name, request_headers=None, record_id=None):
    url = rewrite_stream_url(url)
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

    # Submit audio work to its independent pool. The check worker must never
    # wait for language inference; it continues with HLS/TSDuck/TShark/PyAV.
    language_future = None
    if (
        AUDIO_LANGUAGE_ENABLED
        and probe.get("audio_tracks")
        and _AUDIO_EXECUTOR is not None
    ):
        language_future = _AUDIO_EXECUTOR.submit(
            detect_channel_language,
            rewrite_stream_url(url),
            channel_name,
            request_headers or {},
            probe.get("audio_tracks", []),
            record_id or "",
        )
        logger.info(
            "[AUDIO LANGUAGE QUEUED] record=%s | channel=%s | audio_tracks=%s | audio_worker_pool=%s",
            record_id or "UNASSIGNED",
            channel_name,
            len(probe.get("audio_tracks", [])),
            AUDIO_LANGUAGE_WORKERS,
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
        "language_detection": {},
    }

    if language_future is not None:
        # Keep the Future private to the in-memory coordinator. It is removed
        # before export/JSON serialization and resolved only after CHECK_POOL
        # has fully released its workers.
        result["language_detection"] = {
            "detected_language": "UNKNOWN",
            "language_name": "UNKNOWN",
            "confidence": 0.0,
            "status": "PENDING",
            "metadata_languages": sorted({
                normalize_language_code(item.get("language", ""))
                for item in audio_tracks
                if normalize_language_code(item.get("language", ""))
            }),
        }
        result["_language_future"] = language_future
    else:
        result["language_detection"] = {
            "detected_language": "UNKNOWN",
            "language_name": "UNKNOWN",
            "confidence": 0.0,
            "status": (
                "NO_AUDIO_TRACK"
                if not audio_tracks
                else "DISABLED_OR_NOT_SCHEDULED"
            ),
            "error": (
                "No audio track detected"
                if not audio_tracks
                else "Audio analysis was not scheduled"
            ),
            "metadata_languages": [],
        }

    hls_declared_languages = hls.get("declared_languages", []) or []
    current_metadata_languages = (
        result["language_detection"].get("metadata_languages", []) or []
    )
    result["language_detection"]["metadata_languages"] = sorted({
        normalize_language_code(item)
        for item in current_metadata_languages + hls_declared_languages
        if normalize_language_code(item)
    })
    result["language_detection"]["hls_audio_renditions"] = (
        hls.get("audio_renditions", []) or []
    )

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
        "[STREAM LANGUAGE] %s | %s | confidence=%s | status=%s",
        channel_name,
        result["language_detection"].get("detected_language", "UNKNOWN"),
        result["language_detection"].get("confidence", 0),
        result["language_detection"].get("status", "UNKNOWN"),
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
        language_detection=result.get("language_detection", {}),
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
    """Validate every record independently and coordinate audio asynchronously."""
    global _AUDIO_EXECUTOR
    global _AUDIO_TRACK_EXECUTOR
    global _AUDIO_ENGINE_EXECUTOR

    logger.info("=" * 75)
    logger.info(
        "[CHECK POOL] START | workers=%s | records=%s | deduplication=%s",
        CHECK_WORKERS,
        len(channels),
        DEDUPLICATE_STREAMS,
    )
    logger.info(
        "[AUDIO POOLS] channel_workers=%s | track_workers=%s | engine_workers=%s | extraction_slots=%s | inference_slots=%s | max_tracks_per_record=%s",
        AUDIO_LANGUAGE_WORKERS,
        AUDIO_TRACK_WORKERS,
        AUDIO_ENGINE_WORKERS,
        AUDIO_EXTRACTION_SLOTS,
        AUDIO_INFERENCE_SLOTS,
        AUDIO_MAX_TRACKS_PER_CHANNEL or "ALL",
    )
    logger.info("=" * 75)

    results = {}
    completed = 0
    total = len(channels)
    url_occurrences = defaultdict(int)
    record_context = []

    for index, channel in enumerate(channels, 1):
        channel["url"] = rewrite_stream_url(channel.get("url", ""))
        url = channel["url"]
        url_occurrences[url] += 1
        record_id = (
            f"{index:08d}:"
            + hashlib.sha256(
                (
                    str(channel.get("playlist", "")) + "|"
                    + str(channel.get("line", "")) + "|"
                    + str(channel.get("name", "")) + "|"
                    + str(url)
                ).encode("utf-8", errors="replace")
            ).hexdigest()[:16]
        )
        record_context.append((index, channel, record_id))

    duplicate_occurrences = sum(
        count - 1 for count in url_occurrences.values() if count > 1
    )

    logger.info(
        "[CHECK POOL] duplicate URL occurrences retained=%s | distinct URL count=%s",
        duplicate_occurrences,
        len(url_occurrences),
    )

    audio_pending = []
    audio_completed = 0
    audio_failed = 0
    channels_by_record_id = {}

    # Pool nesting is intentional: channel tasks submit track tasks, and track
    # tasks submit engine tasks. Each level has its own executor; no worker waits
    # on a Future from its own pool. CHECK_POOL never joins audio Futures.
    with ThreadPoolExecutor(
        max_workers=AUDIO_ENGINE_WORKERS,
        thread_name_prefix="AUDIO_ENGINE",
    ) as engine_executor:
        _AUDIO_ENGINE_EXECUTOR = engine_executor

        with ThreadPoolExecutor(
            max_workers=AUDIO_TRACK_WORKERS,
            thread_name_prefix="AUDIO_TRACK",
        ) as track_executor:
            _AUDIO_TRACK_EXECUTOR = track_executor

            with ThreadPoolExecutor(
                max_workers=AUDIO_LANGUAGE_WORKERS,
                thread_name_prefix="AUDIO_LANGUAGE",
            ) as audio_executor:
                _AUDIO_EXECUTOR = audio_executor

                with ThreadPoolExecutor(
                    max_workers=CHECK_WORKERS,
                    thread_name_prefix="STREAM_CHECK",
                ) as executor:
                    futures = {
                        executor.submit(
                            validate_url,
                            channel["url"],
                            channel["name"],
                            channel.get("request_headers", {}),
                            record_id,
                        ): (index, channel, record_id)
                        for index, channel, record_id in record_context
                    }

                    for future in as_completed(futures):
                        index, channel, record_id = futures[future]
                        completed += 1
                        url = channel["url"]

                        try:
                            result = future.result()
                        except Exception as error:
                            logger.exception(
                                "[CHECK WORKER ERROR] record=%s | %s",
                                record_id,
                                safe_url(url),
                            )
                            result = {
                                "url": url,
                                "verified": False,
                                "status": "FAILED",
                                "error": redact_text(error),
                                "live_latency": "NOT MEASURED",
                                "video_tracks": [],
                                "audio_tracks": [],
                                "subtitle_tracks": [],
                                "video_track_count": 0,
                                "audio_track_count": 0,
                                "subtitle_track_count": 0,
                                "language_detection": {
                                    "detected_language": "UNKNOWN",
                                    "language_name": "UNKNOWN",
                                    "confidence": 0.0,
                                    "status": "ERROR",
                                    "error": redact_text(error),
                                },
                            }

                        result["record_id"] = record_id
                        result["record_number"] = index
                        result["url"] = url
                        result["duplicate_url_occurrences"] = (
                            url_occurrences[url] - 1
                        )

                        language_future = result.pop("_language_future", None)
                        if language_future is not None:
                            audio_pending.append((
                                language_future,
                                record_id,
                                channel,
                                result,
                            ))

                        channel["validation"] = result
                        channel["record_id"] = record_id
                        channels_by_record_id[record_id] = channel
                        results[record_id] = result

                        logger.info(
                            "[CHECK PROGRESS] %s/%s | record=%s | %s | %s | language=%s",
                            completed,
                            total,
                            record_id,
                            result.get("status", "UNKNOWN"),
                            safe_url(url),
                            (result.get("language_detection", {}) or {}).get(
                                "detected_language", "UNKNOWN"
                            ),
                        )

                # CHECK_POOL is now fully drained. Only the coordinator thread
                # waits for audio completion; all 45 check workers are reusable.
                logger.info(
                    "[AUDIO COORDINATOR] CHECK_POOL released | pending_records=%s | check_workers=%s",
                    len(audio_pending),
                    CHECK_WORKERS,
                )

                audio_future_map = {
                    item[0]: item[1:]
                    for item in audio_pending
                }
                for audio_future in as_completed(audio_future_map):
                    record_id, channel, result = audio_future_map[audio_future]
                    try:
                        language_result = audio_future.result()
                        audio_completed += 1
                    except Exception as error:
                        audio_failed += 1
                        logger.exception(
                            "[AUDIO COORDINATOR ERROR] record=%s | channel=%s",
                            record_id,
                            channel.get("name", ""),
                        )
                        language_result = {
                            "record_id": record_id,
                            "detected_language": "UNKNOWN",
                            "language_name": "UNKNOWN",
                            "confidence": 0.0,
                            "status": "ERROR",
                            "metadata_languages": [],
                            "track_results": [],
                            "error": redact_text(error),
                        }

                    hls = result.get("hls", {}) or {}
                    current_metadata = language_result.get(
                        "metadata_languages", []
                    ) or []
                    hls_languages = hls.get("declared_languages", []) or []
                    language_result["metadata_languages"] = sorted({
                        normalize_language_code(value)
                        for value in current_metadata + hls_languages
                        if normalize_language_code(value)
                    })
                    language_result["hls_audio_renditions"] = (
                        hls.get("audio_renditions", []) or []
                    )
                    result["language_detection"] = language_result
                    channel["validation"] = result
                    result["region_class"] = classify_channel_region(channel)

                    logger.info(
                        "[AUDIO COORDINATOR PROGRESS] %s/%s | record=%s | channel=%s | language=%s | status=%s | tracks=%s",
                        audio_completed + audio_failed,
                        len(audio_pending),
                        record_id,
                        channel.get("name", ""),
                        language_result.get("detected_language", "UNKNOWN"),
                        language_result.get("status", "UNKNOWN"),
                        len(language_result.get("track_results", [])),
                    )

                    log_event(
                        "AUDIO_RECORD_RESULT_ATTACHED",
                        record_id=record_id,
                        channel=channel.get("name", ""),
                        detected_language=language_result.get("detected_language", "UNKNOWN"),
                        status=language_result.get("status", "UNKNOWN"),
                        track_count=len(language_result.get("track_results", [])),
                        error=language_result.get("error", ""),
                    )

            _AUDIO_EXECUTOR = None
        _AUDIO_TRACK_EXECUTOR = None
    _AUDIO_ENGINE_EXECUTOR = None

    # Records without a scheduled audio job still receive region classification
    # after all validation data is final. No record is dropped or merged.
    for record_id, result in results.items():
        channel = channels_by_record_id.get(record_id)
        if channel is not None:
            result["region_class"] = classify_channel_region(channel)

    verified = sum(
        1 for item in results.values()
        if item.get("verified")
    )

    logger.info(
        "[AUDIO COORDINATOR COMPLETE] scheduled_records=%s | completed=%s | failed=%s",
        len(audio_pending),
        audio_completed,
        audio_failed,
    )
    logger.info(
        "[CHECK COMPLETE] checked_records=%s verified=%s failed=%s duplicate_URL_occurrences_retained=%s audio_records=%s",
        len(results),
        verified,
        len(results) - verified,
        duplicate_occurrences,
        len(audio_pending),
    )

    return results


# ============================================================
# DATABASE
# ============================================================

def build_channel_database(channels):
    """
    Preserve every playlist record. Identical URLs are intentionally
    repeated so source provenance, aliases and per-entry headers survive.
    """
    result = []

    for index, channel in enumerate(channels, 1):
        item = dict(channel)
        item["record_number"] = index
        item["record_id"] = item.get("record_id") or (
            f"{index:08d}:"
            + hashlib.sha256(
                (
                    str(item.get("playlist", "")) + "|"
                    + str(item.get("line", "")) + "|"
                    + str(item.get("name", "")) + "|"
                    + str(item.get("url", ""))
                ).encode("utf-8", errors="replace")
            ).hexdigest()[:16]
        )
        item["aliases"] = [item.get("name", "")]
        item["source_playlists"] = [item.get("playlist", "")]
        item["url"] = rewrite_stream_url(item.get("url", ""))
        result.append(item)

    logger.info(
        "[DATABASE] Records retained=%s | deduplication=%s",
        len(result),
        DEDUPLICATE_STREAMS,
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
    temporary_path = output_path + ".tmp"

    with open(
        temporary_path,
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

            region = classify_channel_region(channel)
            if mode == "region_ru" and region != "RU":
                continue
            if mode == "region_world" and region != "WORLD":
                continue
            if mode == "stable" and not verified:
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

            validation_language = (
                (validation.get("language_detection", {}) or {})
                .get("detected_language", "")
            )
            if (
                validation_language in LANGUAGE_NAMES
                and not attributes.get("tvg-language")
            ):
                attrs.append(f'tvg-language="{validation_language}"')

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
                f'{rewrite_stream_url(channel["url"])}\n'
            )

            count += 1

    if count:
        os.replace(temporary_path, output_path)
    else:
        try:
            os.remove(temporary_path)
        except OSError:
            pass
        if os.path.exists(output_path):
            os.remove(output_path)

    logger.info(
        "[EXPORT M3U] %s | records=%s | mode=%s | %s",
        output_path,
        count,
        mode,
        "written" if count else "skipped: no matching records",
    )


# ============================================================
# CSV EXPORT
# ============================================================

def export_csv(channels):
    if not channels:
        logger.info("[EXPORT CSV] Skipped: no channel records")
        return
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
        "detected_language",
        "detected_language_name",
        "language_confidence",
        "language_status",
        "language_metadata",
        "region_class",
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
                "url": safe_url(channel["url"]),
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
                "detected_language": (
                    validation.get("language_detection", {}) or {}
                ).get("detected_language", "UNKNOWN"),
                "detected_language_name": (
                    validation.get("language_detection", {}) or {}
                ).get("language_name", "UNKNOWN"),
                "language_confidence": (
                    validation.get("language_detection", {}) or {}
                ).get("confidence", 0),
                "language_status": (
                    validation.get("language_detection", {}) or {}
                ).get("status", "UNKNOWN"),
                "language_metadata": channel.get("language", ""),
                "region_class": classify_channel_region(channel),
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
    if not channels:
        logger.info("[EXPORT JSON] Skipped: no channel records")
        return
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
            "url": safe_url(channel["url"]),
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
            "detected_language": (
                validation.get("language_detection", {}) or {}
            ).get("detected_language", "UNKNOWN"),
            "language_detection": validation.get("language_detection", {}),
            "region_class": classify_channel_region(channel),
            "record_id": channel.get("record_id", ""),
            "validation": _sanitize_audit_value(validation),
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
    track_total = sum(
        len((channel.get("validation", {}) or {}).get(f"{kind}_tracks", []) or [])
        for channel in channels
        for kind in ("video", "audio", "subtitle", "data")
    )
    if not track_total:
        logger.info("[EXPORT TRACK REPORT] Skipped: no media tracks")
        return
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
        "detected_channel_language",
        "language_confidence",
        "language_status",
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
                        "url": safe_url(channel["url"]),
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
                        "detected_channel_language": (
                            validation.get("language_detection", {}) or {}
                        ).get("detected_language", "UNKNOWN"),
                        "language_confidence": (
                            validation.get("language_detection", {}) or {}
                        ).get("confidence", 0),
                        "language_status": (
                            validation.get("language_detection", {}) or {}
                        ).get("status", "UNKNOWN"),
                    })

    logger.info(
        "[EXPORT TRACK REPORT] %s",
        filename,
    )


# ============================================================
# PLAYLIST REPORT
# ============================================================

def export_playlist_report(playlist_results):
    if not playlist_results:
        logger.info("[EXPORT PLAYLIST REPORT] Skipped: no playlist results")
        return
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
                "error": redact_text(item.get("error", "")),
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
    if not channels and not unique_results and not playlist_results:
        logger.info("[EXPORT CHECK REPORT] Skipped: no diagnostic records")
        return
    filename = os.path.join(
        OUTPUT_DIR,
        "Check_Report.txt",
    )

    total_records = len(channels)
    total_checked = len(unique_results)
    url_counts = defaultdict(int)
    for channel in channels:
        url_counts[channel.get("url", "")] += 1
    duplicate_url_occurrences = sum(
        count - 1 for count in url_counts.values() if count > 1
    )

    verified = sum(
        1 for result in unique_results.values()
        if result.get("verified")
    )

    failed = total_checked - verified

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
    response_unknown = count_if(
        lambda item: item.get("response_class") in (None, "", "UNKNOWN")
    )
    ffprobe_success_not_verified = count_if(
        lambda item: item.get("probe", {}).get("success", False)
        and not item.get("verified", False)
    )
    verified_without_ffprobe = count_if(
        lambda item: item.get("verified", False)
        and not item.get("probe", {}).get("success", False)
    )
    language_confirmed = count_if(
        lambda item: (item.get("language_detection", {}) or {}).get("status") == "CONFIRMED"
    )
    language_provisional = count_if(
        lambda item: (item.get("language_detection", {}) or {}).get("status") == "PROVISIONAL"
    )
    language_conflicts = count_if(
        lambda item: (item.get("language_detection", {}) or {}).get("status") == "CONFLICT"
    )
    language_unknown = count_if(
        lambda item: (item.get("language_detection", {}) or {}).get("detected_language")
        in ("UNKNOWN", "", None)
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
            f"Check workers: {CHECK_WORKERS}\n"
        )
        file.write(
            f"Deduplication enabled: {DEDUPLICATE_STREAMS}\n"
        )
        file.write(
            f"Audio language channel workers: {AUDIO_LANGUAGE_WORKERS}\n"
        )
        file.write(
            f"Audio track workers: {AUDIO_TRACK_WORKERS}\n"
        )
        file.write(
            f"Audio engine workers: {AUDIO_ENGINE_WORKERS}\n"
        )
        file.write(
            f"Audio max tracks per record: {AUDIO_MAX_TRACKS_PER_CHANNEL or 'ALL'}\n"
        )
        file.write(
            f"Audio extraction slots: {AUDIO_EXTRACTION_SLOTS}\n"
        )
        file.write(
            f"Audio inference slots: {AUDIO_INFERENCE_SLOTS}\n"
        )
        file.write(
            f"Whisper model: {WHISPER_MODEL_SIZE}\n"
        )
        file.write(
            f"SpeechBrain enabled: {ENABLE_SPEECHBRAIN}\n\n"
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
            f"Records checked (deduplication disabled): {total_checked}\n"
        )
        file.write(
            f"Distinct URL values (statistics only): {len(url_counts)}\n"
        )
        file.write(
            f"Duplicate URL occurrences retained and checked: {duplicate_url_occurrences}\n"
        )
        file.write(
            f"Verified: {verified}\n"
        )
        file.write(
            f"Failed: {failed}\n"
        )
        file.write(
            f"Success rate: {(verified / total_checked * 100) if total_checked else 0:.2f}%\n\n"
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
            f"Slow (> {NORMAL_RESPONSE_MS} ms): {slow}\n"
        )
        file.write(
            f"Unknown / not measured: {response_unknown}\n"
        )
        file.write(
            f"Response classes accounted: {fast + normal + slow + response_unknown}/{total_checked}\n"
        )
        file.write(
            f"FFprobe successful but not verified: {ffprobe_success_not_verified}\n"
        )
        file.write(
            f"Verified without FFprobe success: {verified_without_ffprobe}\n\n"
        )

        file.write("--- AUDIO LANGUAGE DETECTION ---\n")
        file.write(
            f"Audio channel workers: {AUDIO_LANGUAGE_WORKERS}\n"
        )
        file.write(
            f"Audio track workers: {AUDIO_TRACK_WORKERS}\n"
        )
        file.write(
            f"Audio engine workers: {AUDIO_ENGINE_WORKERS}\n"
        )
        file.write(
            f"Audio max tracks per record: {AUDIO_MAX_TRACKS_PER_CHANNEL or 'ALL'}\n"
        )
        file.write(
            f"Whisper model: {WHISPER_MODEL_SIZE}\n"
        )
        file.write(
            f"SpeechBrain enabled: {ENABLE_SPEECHBRAIN}\n"
        )
        file.write(
            f"Forced Cinerama host rewrites: {URL_REWRITE_COUNT}\n"
        )
        file.write(
            f"Confirmed: {language_confirmed}\n"
        )
        file.write(
            f"Provisional: {language_provisional}\n"
        )
        file.write(
            f"Conflicts: {language_conflicts}\n"
        )
        file.write(
            f"Unknown: {language_unknown}\n\n"
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

        for record_id, result in unique_results.items():
            url = result.get("url", record_id)
            language = result.get("language_detection", {}) or {}
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
                f"HEAD: {head.get('status_code', 'N/A')} | {head.get('response_ms', '')} ms | {redact_text(head.get('error', ''))}\n"
            )
            file.write(
                f"GET: {get.get('status_code', 'N/A')} | {get.get('headers_ms', '')} ms headers | {get.get('first_byte_total_ms', '')} ms first byte | {get.get('bytes_read', 0)} bytes\n"
            )
            file.write(
                f"GET error: {redact_text(get.get('error', ''))}\n"
            )

            file.write(
                f"FFprobe: success={probe.get('success', False)} | timeout={probe.get('timed_out', False)} | error={redact_text(probe.get('error', ''))}\n"
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
                f"TSDuck: {tsduck.get('success', False)} | {redact_text(tsduck.get('error', ''))}\n"
            )
            file.write(
                f"TShark: {tshark.get('success', False)} | {redact_text(tshark.get('error', ''))}\n"
            )
            file.write(
                f"PyAV: {pyav.get('success', False)} | {redact_text(pyav.get('error', ''))}\n"
            )
            file.write(
                f"Detected language: {language.get('detected_language', 'UNKNOWN')} | {language.get('language_name', 'UNKNOWN')} | confidence={language.get('confidence', 0)} | status={language.get('status', 'UNKNOWN')}\n"
            )
            file.write(
                f"Language metadata: {language.get('metadata_languages', [])}\n"
            )
            file.write(
                f"Language error: {redact_text(language.get('error', ''))}\n"
            )
            for language_track in language.get("track_results", []):
                file.write(
                    f"  Audio stream #{language_track.get('stream_index', '')}: "
                    f"{language_track.get('detected_language', 'UNKNOWN')} | "
                    f"{language_track.get('status', 'UNKNOWN')} | "
                    f"confidence={language_track.get('confidence', 0)}\n"
                )
            file.write(
                f"Region classification: {result.get('region_class', 'UNKNOWN')}\n"
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
    url_counts = defaultdict(int)
    for channel in channels:
        url_counts[channel.get("url", "")] += 1
    duplicate_url_occurrences = sum(
        count - 1 for count in url_counts.values() if count > 1
    )

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
        "Channel records checked (deduplication disabled): %s",
        total,
    )
    logger.info(
        "Distinct URL values (statistics only): %s",
        len(url_counts),
    )
    logger.info(
        "Duplicate URL occurrences retained and checked: %s",
        duplicate_url_occurrences,
    )
    logger.info(
        "Audio language channel workers: %s",
        AUDIO_LANGUAGE_WORKERS,
    )
    logger.info(
        "Audio track workers: %s",
        AUDIO_TRACK_WORKERS,
    )
    logger.info(
        "Audio engine workers: %s",
        AUDIO_ENGINE_WORKERS,
    )
    logger.info(
        "Audio max tracks per record: %s",
        AUDIO_MAX_TRACKS_PER_CHANNEL or "ALL",
    )
    logger.info(
        "Forced Cinerama hostname rewrites: %s",
        URL_REWRITE_COUNT,
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
        checked_records=total,
        distinct_url_values=len(url_counts),
        duplicate_url_occurrences_retained=duplicate_url_occurrences,
        audio_language_workers=AUDIO_LANGUAGE_WORKERS,
        audio_track_workers=AUDIO_TRACK_WORKERS,
        audio_engine_workers=AUDIO_ENGINE_WORKERS,
        audio_max_tracks_per_record=AUDIO_MAX_TRACKS_PER_CHANNEL or "ALL",
        verified=verified,
        failed=failed,
    )


def write_output_manifest():
    """Index only real, non-empty files produced in the audit directory."""
    manifest_path = os.path.join(OUTPUT_DIR, "OUTPUT_FILES_MANIFEST.json")
    entries = []
    for root, dirs, files in os.walk(OUTPUT_DIR):
        # Model/tool caches are not generated audit outputs.
        dirs[:] = [name for name in dirs if not (root == OUTPUT_DIR and name == "tools")]
        for name in sorted(files):
            path = os.path.join(root, name)
            if os.path.abspath(path) == os.path.abspath(manifest_path):
                continue
            try:
                size = os.path.getsize(path)
                if size <= 0:
                    continue
                digest = hashlib.sha256()
                with open(path, "rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                entries.append({
                    "path": os.path.relpath(path, OUTPUT_DIR).replace(os.sep, "/"),
                    "size_bytes": size,
                    "sha256": digest.hexdigest(),
                })
            except OSError as error:
                logger.warning("[MANIFEST] Cannot inspect %s: %s", path, error)

    payload = {
        "generated_at": now_string(),
        "repository": f"{OWNER}/{REPOSITORY}",
        "branch": BRANCH,
        "deduplication_enabled": False,
        "tshark_enabled": TSHARK_CAPTURE_ENABLED,
        "tshark_capture_workers": TSHARK_CAPTURE_WORKERS,
        "non_empty_files_count": len(entries),
        "files": sorted(entries, key=lambda item: item["path"]),
    }
    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    return manifest_path


def clean_previous_outputs():
    """Remove stale generated reports while preserving downloaded tool/model caches."""
    keep_paths = {os.path.abspath(LOG_FILE), os.path.abspath(AUDIT_JSONL)}
    for root, dirs, files in os.walk(OUTPUT_DIR, topdown=True):
        if os.path.abspath(root) == os.path.abspath(OUTPUT_DIR):
            dirs[:] = [name for name in dirs if name != "tools"]
        for name in files:
            path = os.path.abspath(os.path.join(root, name))
            try:
                if path in keep_paths:
                    # Stream_Audit FileHandler already has this file open.
                    with open(path, "w", encoding="utf-8"):
                        pass
                else:
                    os.remove(path)
            except OSError as error:
                logger.warning("[CLEANUP] Could not remove stale output %s: %s", path, error)
    for root, dirs, files in os.walk(OUTPUT_DIR, topdown=False):
        try:
            if os.path.commonpath([os.path.abspath(root), os.path.abspath(TOOLS_DIR)]) == os.path.abspath(TOOLS_DIR):
                continue
        except ValueError:
            pass
        try:
            if not os.listdir(root):
                os.rmdir(root)
        except OSError:
            pass
    for directory in (PLAYLIST_DIR, DATABASE_DIR, LOG_DIR, TSHARK_DIR, TSHARK_PCAP_DIR):
        os.makedirs(directory, exist_ok=True)


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    started = time.perf_counter()
    clean_previous_outputs()

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
    logger.info(
        "AUDIO LANGUAGE CHANNEL WORKERS: %s",
        AUDIO_LANGUAGE_WORKERS,
    )
    logger.info(
        "AUDIO TRACK WORKERS: %s",
        AUDIO_TRACK_WORKERS,
    )
    logger.info(
        "AUDIO ENGINE WORKERS: %s",
        AUDIO_ENGINE_WORKERS,
    )
    logger.info(
        "AUDIO MAX TRACKS PER RECORD: %s",
        AUDIO_MAX_TRACKS_PER_CHANNEL or "ALL",
    )
    logger.info(
        "DEDUPLICATION: DISABLED",
    )
    logger.info(
        "TSHARK: enabled=%s | interface=%s | capture_workers=%s | duration=%ss | keep_pcap=%s",
        TSHARK_CAPTURE_ENABLED, TSHARK_INTERFACE, TSHARK_CAPTURE_WORKERS,
        TSHARK_CAPTURE_SECONDS, TSHARK_KEEP_PCAP,
    )
    logger.info(
        "CINERAMA URL REWRITE: %s -> %s",
        CINERAMA_OLD_HOST,
        CINERAMA_NEW_HOST,
    )

    # --------------------------------------------------------
    # STEP 0: TOOL PREPARATION
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 0: TOOL PREPARATION"
    )

    install_optional_tools()
    configure_tshark_capture_permissions()
    install_python_analyzers()
    inventory = tool_inventory()

    log_event(
        "RUN_STARTED",
        owner=OWNER,
        repository=REPOSITORY,
        branch=BRANCH,
        download_workers=DOWNLOAD_WORKERS,
        parser_workers=PARSER_WORKERS,
        check_workers=CHECK_WORKERS,
        audio_language_workers=AUDIO_LANGUAGE_WORKERS,
        audio_track_workers=AUDIO_TRACK_WORKERS,
        audio_engine_workers=AUDIO_ENGINE_WORKERS,
        audio_max_tracks_per_record=AUDIO_MAX_TRACKS_PER_CHANNEL or "ALL",
        deduplication=DEDUPLICATE_STREAMS,
        analyzers_enabled={
            "ffmpeg_ffprobe": True,
            "tsduck": ENABLE_TSDUCK,
            "tshark": ENABLE_TSHARK and TSHARK_CAPTURE_ENABLED,
            "pyav": ENABLE_PYAV,
            "whisper": ENABLE_WHISPER,
            "speechbrain": ENABLE_SPEECHBRAIN,
            "audio_language": AUDIO_LANGUAGE_ENABLED,
        },
        tshark={
            "enabled": TSHARK_CAPTURE_ENABLED,
            "interface": TSHARK_INTERFACE,
            "capture_workers": TSHARK_CAPTURE_WORKERS,
            "capture_seconds": TSHARK_CAPTURE_SECONDS,
            "keep_pcap": TSHARK_KEEP_PCAP,
        },
        cinerama_rewrite={
            "from": CINERAMA_OLD_HOST,
            "to": CINERAMA_NEW_HOST,
        },
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
        logger.warning("[MAIN] No playlists found; continuing to final diagnostics without creating empty playlist outputs.")

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
        logger.warning("[MAIN] No playlists downloaded successfully; channel parsing will be skipped.")

    # --------------------------------------------------------
    # STEP 4: PARALLEL PARSING - 34 WORKERS
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 4: PARALLEL PARSING | workers=%s",
        PARSER_WORKERS,
    )

    all_channels = parse_all_playlists(
        downloaded
    ) if downloaded else []

    logger.info(
        "[MAIN] Channel records collected: %s",
        len(all_channels),
    )

    if not all_channels:
        logger.warning("[MAIN] No channel entries found; skipping channel-level analysis and exports.")

    # --------------------------------------------------------
    # STEP 5: STREAM VALIDATION - 45 WORKERS
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 5: STREAM VALIDATION | workers=%s",
        CHECK_WORKERS,
    )

    unique_results = validate_unique_streams(
        all_channels
    ) if all_channels else {}

    # --------------------------------------------------------
    # STEP 6: UNIQUE DATABASE
    # --------------------------------------------------------

    logger.info(
        "[MAIN] STEP 6: BUILD CHANNEL DATABASE"
    )

    unique_channels = build_channel_database(
        all_channels
    ) if all_channels else []

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

    export_regional_playlists(all_channels)

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

    export_language_reports(all_channels)
    export_tshark_reports()

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
    finally:
        try:
            write_output_manifest()
        except Exception:
            logger.exception("[MANIFEST] Failed to write output manifest")