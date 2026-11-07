
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ==============================================================================
# Проект:               findmydevice364-hub_lab
# Владелец:             #Phoenix89S_Lab©
# Публичное имя:        Phoenix Hub "Baryshnikov"
# Модуль:               Unified Playlist Hunter
#
# Версия:               0.0.0-alpha.001
# Код версии:           1000001
# Номер сборки:         001
# Идентификатор сборки: PHH-ALPHA-001-20261107
#
# Описание:             Модуль расширенного поиска, обнаружения, исследования
#                       и классификации IPTV-источников и плейлистов.
#                       Поддерживает автономную работу, ведущую роль при
#                       обнаружении новых источников и планируемую ведомую
#                       работу под управлением основного кода.
#
# Архитектура:          Автономная / Ведущая / Ведомая
#
# Автономная работа:    Самостоятельный поиск источников, анализ документов,
#                       извлечение URL, рекурсивное исследование, разбор
#                       плейлистов, проверка потоков и сохранение результатов.
#
# Ведущая роль:         Обнаружение новых документов, репозиториев, плейлистов
#                       и вложенных источников с формированием очередей
#                       кандидатов, сохранением происхождения ссылок
#                       и подготовкой результатов дальнейшей обработки.
#
# Ведомая роль:         Планируемая работа под управлением основного кода:
#                       получение параметров, выполнение назначенных задач
#                       и возврат структурированных результатов.
#
# Поиск и извлечение:   Python / TXT / Markdown / HTML / JSON / YAML / README;
#                       M3U / M3U8, включая заголовки и комментарии до EXTINF.
#                       Извлечение ссылок на источники, документы, потоки
#                       и неоднозначные кандидаты.
#
# Исследование:         Рекурсивный поиск вложенных источников, анализ GitHub,
#                       обработка ошибок, повторные попытки, учёт происхождения
#                       ссылок и подробная диагностика этапов обработки.
#
# Параллелизм:          Поиск >= 8; разбор >= 35; проверка потоков >= 40.
#
# Дедупликация:         Неагрессивная. Совпадение названий само по себе
#                       не является основанием для удаления каналов.
#                       Альтернативные ссылки и различия метаданных сохраняются.
#
# Текущий режим:        АВТОНОМНАЯ АЛЬФА-СБОРКА
#
# Передача результатов: ОТКЛЮЧЕНА
# Взаимодействие с
# основным кодом:       ОТКЛЮЧЕНО
# Запуск основным кодом: ЗАПЛАНИРОВАН / НЕ РЕАЛИЗОВАН
# Внешняя интеграция:   ОТКЛЮЧЕНА
#
# Цель сборки:          Альфа 001 — автономный поиск и исследование источников.
# Статус:               Экспериментальная разработка.
# ==============================================================================

# ==============================================================================
# ПЛАНИРУЕМЫЙ РЕЖИМ: ДВОЙНОЙ
#
# Назначение:           Предусмотреть два варианта эксплуатации Hunter:
#                       самостоятельную работу и работу под управлением
#                       основного кода.
#
# 1. АВТОНОМНЫЙ РЕЖИМ:
#                       Hunter самостоятельно запускает поиск, обнаруживает
#                       новые источники, исследует документы и вложенные
#                       ссылки, классифицирует кандидатов и формирует
#                       результаты. При обнаружении новых источников Hunter
#                       выполняет ведущую роль в процессе исследования.
#
# 2. ВЕДОМЫЙ РЕЖИМ:
#                       Основной код запускает Hunter, передаёт параметры
#                       и назначенные задачи, а Hunter выполняет поиск
#                       и анализ с последующей подготовкой структурированных
#                       результатов для вызывающего модуля.
#
# Состояние реализации: ПЛАНИРУЕТСЯ / НЕ РЕАЛИЗОВАНО
# Запуск основным кодом: ОТКЛЮЧЁН
# Межмодульное
# взаимодействие:       ОТКЛЮЧЕНО
# Передача результатов: ОТКЛЮЧЕНА
# Внешняя интеграция:   ОТКЛЮЧЕНА
# ============================================================================== 

Phoenix Hub — Unified Playlist Hunter / SKALA
Единый модуль поиска, загрузки, анализа и аудита IPTV-плейлистов.

Назначение основных модулей:
1. Configuration:
   Загрузка параметров из переменных окружения, настройка путей,
   лимитов параллелизма, HTTP, диагностики и выходных файлов.
2. HTTPClient:
   Единый HTTP-клиент на стандартной библиотеке Python с тайм-аутами,
   ограничением размера ответа, повторными запросами и обработкой ошибок.
3. SourceDiscovery:
   Поиск публичных источников через GitHub Search API, GitHub Gists
   и заданный список исходных URL.
4. PlaylistParser:
   Разбор M3U/M3U8, сохранение EXTINF-метаданных, обнаружение вложенных
   плейлистов, извлечение URL и безопасная дедупликация.
5. StreamChecker:
   Проверка URL с использованием HEAD, GET Range и необязательного ffprobe.
6. HunterStore:
   SQLite-хранилище запусков, источников, результатов и состояния проверок.
7. SKALA:
   Сравнение предыдущего и нового Donor.m3u, статистика BEFORE/CHANGES/AFTER,
   архивирование версий и отчёт об изменениях.
8. ReceiverBridge:
   Формирование локального JSON outbox для последующей интеграции.
   Исходный Python-файл никогда не используется как HTTP API.
9. UnifiedPlaylistHunter:
   Оркестрация полного процесса от поиска до формирования результатов.

Concurrency requirements:
- At least 8 workers for source discovery.
- At least 35 workers for source fetching and playlist parsing.
- At least 40 workers for stream checking.

The script uses only the Python standard library.
ffprobe is optional and its absence does not stop the entire run.

Important:
- Public URL availability is not a guarantee of stream playback.
- HTTP status checks and ffprobe checks are recorded separately.
- Unverified streams can remain in the donor playlist by default.
- Network failures are not interpreted as proof that a channel is permanently dead.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import dataclasses
import datetime as dt
import gzip
import hashlib
import ipaddress
import json
import logging
import os
import platform
import random
import re
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional


# ============================================================================
# 1. CONFIGURATION
# ============================================================================

APP_NAME = "Phoenix Hub — Unified Playlist Hunter"
APP_VERSION = "1.0.0"

BASE_DIR = Path(os.getenv("HUNTER_BASE_DIR", ".")).resolve()
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", str(BASE_DIR / "data"))).resolve()
TOOLS_DIR = Path(os.getenv("TOOLS_DIR", str(OUTPUT_DIR / "tools"))).resolve()

DONOR_PATH = Path(
    os.getenv("DONOR_PATH", str(OUTPUT_DIR / "Donor.m3u"))
).resolve()

SKALA_ARCHIVE_PATH = Path(
    os.getenv("SKALA_ARCHIVE_PATH", str(OUTPUT_DIR / "alt_m3u.zip"))
).resolve()

SKALA_REPORT_DIR = Path(
    os.getenv("SKALA_REPORT_DIR", str(OUTPUT_DIR / "skala"))
).resolve()

DATABASE_PATH = Path(
    os.getenv("DATABASE_PATH", str(OUTPUT_DIR / "hunter.sqlite3"))
).resolve()

RESULTS_JSON_PATH = Path(
    os.getenv("RESULTS_JSON_PATH", str(OUTPUT_DIR / "hunter_results.json"))
).resolve()

DIAGNOSTICS_JSON_PATH = Path(
    os.getenv("DIAGNOSTICS_JSON_PATH", str(OUTPUT_DIR / "diagnostics.json"))
).resolve()

RECEIVER_OUTBOX = Path(
    os.getenv("RECEIVER_OUTBOX", str(OUTPUT_DIR / "receiver_outbox"))
).resolve()

LOG_PATH = Path(
    os.getenv("LOG_PATH", str(OUTPUT_DIR / "hunter.log"))
).resolve()

# The URL is a raw Python source file, not an API endpoint.
# This script does not POST data to it and does not execute downloaded code.
RECEIVER_SOURCE_URL = os.getenv(
    "RECEIVER_SOURCE_URL",
    "https://raw.githubusercontent.com/findmydevice364-hub/zala/main/git_bot_for_stable.py",
)

RECEIVER_TRANSPORT_ENABLED = (
    os.getenv("RECEIVER_TRANSPORT_ENABLED", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

# Worker pools are separate, so discovery cannot consume every parsing worker.
SEARCH_WORKERS = max(8, int(os.getenv("SEARCH_WORKERS", "8")))
PARSING_WORKERS = max(35, int(os.getenv("PARSING_WORKERS", "35")))
STREAM_CHECK_WORKERS = max(
    40, int(os.getenv("STREAM_CHECK_WORKERS", "40"))
)

HTTP_MAX_CONNECTIONS = max(
    100, int(os.getenv("HTTP_MAX_CONNECTIONS", "100"))
)
HTTP_TIMEOUT_SECONDS = max(
    3, int(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
)
HTTP_RETRIES = max(0, int(os.getenv("HTTP_RETRIES", "2")))
HTTP_RETRY_BACKOFF = max(
    0.1, float(os.getenv("HTTP_RETRY_BACKOFF", "0.6"))
)
HTTP_MAX_BYTES = max(
    1024 * 1024, int(os.getenv("HTTP_MAX_BYTES", str(16 * 1024 * 1024)))
)
HTTP_USER_AGENT = os.getenv(
    "HTTP_USER_AGENT",
    "PhoenixHub-UnifiedPlaylistHunter/1.0 (+playlist-diagnostics)",
)

MAX_DISCOVERED_SOURCES = max(
    1, int(os.getenv("MAX_DISCOVERED_SOURCES", "300"))
)
MAX_NESTED_PLAYLISTS = max(
    0, int(os.getenv("MAX_NESTED_PLAYLISTS", "250"))
)
MAX_CHANNELS = max(1, int(os.getenv("MAX_CHANNELS", "50000")))
MAX_PLAYLIST_DEPTH = max(0, int(os.getenv("MAX_PLAYLIST_DEPTH", "2")))
MAX_GITHUB_PAGES = max(1, int(os.getenv("MAX_GITHUB_PAGES", "3")))
MAX_GIST_PAGES = max(1, int(os.getenv("MAX_GIST_PAGES", "2")))

STREAM_CACHE_TTL_SECONDS = max(
    0, int(os.getenv("STREAM_CACHE_TTL_SECONDS", "1800"))
)
STREAM_CHECK_FFPROBE = (
    os.getenv("STREAM_CHECK_FFPROBE", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
FFPROBE_BOOTSTRAP_ENABLED = (
    os.getenv("FFPROBE_BOOTSTRAP_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
FFPROBE_PATH = os.getenv("FFPROBE_PATH", "").strip()
FFPROBE_TIMEOUT_SECONDS = max(
    2, int(os.getenv("FFPROBE_TIMEOUT_SECONDS", "8"))
)
FFPROBE_DOWNLOAD_URL = os.getenv("FFPROBE_DOWNLOAD_URL", "").strip()

DONOR_INCLUDE_UNVERIFIED = (
    os.getenv("DONOR_INCLUDE_UNVERIFIED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
DONOR_MIN_CHANNELS = max(1, int(os.getenv("DONOR_MIN_CHANNELS", "1")))
DONOR_REJECT_EMPTY = (
    os.getenv("DONOR_REJECT_EMPTY", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)

# Nested URL retrieval is enabled for public HTTP(S) playlists only.
NESTED_PLAYLISTS_ENABLED = (
    os.getenv("NESTED_PLAYLISTS_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)

# Private, loopback, link-local, and reserved IP destinations are blocked by
# default. Set ALLOW_PRIVATE_NETWORKS=1 only for a trusted local environment.
ALLOW_PRIVATE_NETWORKS = (
    os.getenv("ALLOW_PRIVATE_NETWORKS", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_API_BASE = "https://api.github.com"
GITHUB_SEARCH_QUERIES = [
    "iptv m3u",
    "iptv playlist",
    "m3u8 tv channels",
    "russia iptv m3u",
    "country:RU extension:m3u",
    "extension:m3u8 iptv",
]

GITHUB_GIST_API = "https://api.github.com/gists"
GITHUB_RAW_HOSTS = {
    "raw.githubusercontent.com",
    "gist.githubusercontent.com",
}

# Optional configured seeds. Use a semicolon-separated environment variable:
# EXTRA_SOURCE_URLS="https://example.org/list.m3u;https://example.org/list.m3u8"
DEFAULT_SOURCE_URLS: list[str] = [
    "https://iptv-org.github.io/iptv/countries/ru.m3u",
    "https://iptv-org.github.io/iptv/countries/ua.m3u",
]

EXTRA_SOURCE_URLS = [
    item.strip()
    for item in os.getenv("EXTRA_SOURCE_URLS", "").split(";")
    if item.strip()
]

CONFIGURED_SOURCE_URLS = list(
    dict.fromkeys(DEFAULT_SOURCE_URLS + EXTRA_SOURCE_URLS)
)

# URLs can be added here without changing the discovery engine.
GITVERSE_SEED_URLS = [
    item.strip()
    for item in os.getenv("GITVERSE_SEED_URLS", "").split(";")
    if item.strip()
]

# Optional trusted archive download URL for ffprobe.
# The default installer tries the configured URL and known static builds.
FFPROBE_CANDIDATE_URLS = [
    FFPROBE_DOWNLOAD_URL,
    "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz",
]

# These are transport retries, not proof of permanent source failure.
RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}

PLAYLIST_SUFFIXES = (".m3u", ".m3u8", ".m3u.gz")
PLAYLIST_CONTENT_TYPES = (
    "mpegurl",
    "audio/x-mpegurl",
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "text/plain",
    "application/octet-stream",
)

# Document and source-code extensions that may contain playlist / stream URLs.
DOCUMENT_EXTENSIONS = (
    ".py", ".pyw", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx",
    ".java", ".kt", ".kts", ".go", ".rs", ".c", ".h", ".cpp", ".hpp",
    ".cs", ".php", ".rb", ".pl", ".sh", ".bash", ".ps1", ".bat", ".cmd",
    ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf",
    ".json", ".xml", ".html", ".htm", ".xhtml",
    ".md", ".markdown", ".rst", ".txt", ".log",
    ".csv", ".tsv", ".pls", ".xspf", ".w3u", ".list", ".lst",
    ".url", ".desktop", ".properties", ".env.example",
)

ARCHIVE_EXTENSIONS = (
    ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z",
)

# Combined set used by discovery scanners.
SCANNABLE_EXTENSIONS = PLAYLIST_SUFFIXES + DOCUMENT_EXTENSIONS + ARCHIVE_EXTENSIONS

# Archive processing safety limits
ARCHIVE_MAX_BYTES = max(
    1024 * 1024, int(os.getenv("ARCHIVE_MAX_BYTES", str(8 * 1024 * 1024)))
)
ARCHIVE_MAX_FILES = max(1, int(os.getenv("ARCHIVE_MAX_FILES", "40")))
ARCHIVE_MAX_DEPTH = max(0, int(os.getenv("ARCHIVE_MAX_DEPTH", "2")))
ARCHIVE_TIMEOUT_SECONDS = max(
    2, int(os.getenv("ARCHIVE_TIMEOUT_SECONDS", "12"))
)

# Discovery statistics counters (thread-safe via simple dict + lock later)
DISCOVERY_COUNTERS = {
    "documents_found": 0,
    "documents_processed": 0,
    "documents_skipped": 0,
    "documents_duplicate": 0,
    "documents_error": 0,
    "urls_extracted": 0,
    "urls_classified_playlist": 0,
    "urls_classified_stream": 0,
    "urls_classified_document": 0,
    "urls_classified_archive": 0,
    "urls_classified_other": 0,
}

VIDEO_SUFFIXES = (".m3u8", ".ts", ".mp4", ".mkv", ".mpd", ".m4s")
BLACKLIST_NAME_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in os.getenv(
        "CHANNEL_NAME_BLACKLIST",
        r"(^|\b)(18\+|xxx|adult|porn|erotic)(\b|$)",
    ).split(";;")
    if pattern.strip()
]

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ============================================================================
# 2. LOGGING AND GENERAL UTILITIES
# ============================================================================

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TOOLS_DIR.mkdir(parents=True, exist_ok=True)
SKALA_REPORT_DIR.mkdir(parents=True, exist_ok=True)
RECEIVER_OUTBOX.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)

LOGGER = logging.getLogger("phoenix_hunter")
SSL_CONTEXT = ssl.create_default_context()
DNS_CACHE: dict[str, tuple[float, list[str]]] = {}
DNS_CACHE_LOCK = threading.Lock()
URL_CHECK_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
URL_CHECK_CACHE_LOCK = threading.Lock()
URL_CHECK_LOCKS: dict[str, threading.Lock] = {}
URL_CHECK_LOCKS_LOCK = threading.Lock()


def utc_now() -> str:
    """Return the current UTC time in ISO 8601 format."""
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log_event(level: int, message: str, *args: Any) -> None:
    """Write a structured application log message."""
    LOGGER.log(level, message, *args)


def safe_int(value: Any, default: int = 0) -> int:
    """Convert a value to integer without raising."""
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def sha256_text(value: str) -> str:
    """Return a SHA-256 digest for text."""
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def atomic_write_text(path: Path, content: str) -> None:
    """Atomically replace a UTF-8 text file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def atomic_write_json(path: Path, value: Any) -> None:
    """Atomically write JSON."""
    atomic_write_text(
        path,
        json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
    )


def is_http_url(value: str) -> bool:
    """Return True for absolute HTTP(S) URLs."""
    try:
        parsed = urllib.parse.urlsplit(value.strip())
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.hostname)
    except Exception:
        return False


def normalize_url(value: str) -> str:
    """Normalize URL syntax without changing its query parameters."""
    value = value.strip()
    if not is_http_url(value):
        return ""
    parsed = urllib.parse.urlsplit(value)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    if not host:
        return ""
    try:
        port = parsed.port
    except ValueError:
        return ""
    netloc = host
    if ":" in host and not host.startswith("["):
        netloc = f"[{host}]"
    if port and not (
        (scheme == "http" and port == 80)
        or (scheme == "https" and port == 443)
    ):
        netloc += f":{port}"
    path = parsed.path or "/"
    return urllib.parse.urlunsplit(
        (scheme, netloc, path, parsed.query, "")
    )


def url_host(value: str) -> str:
    """Extract the host from a URL."""
    try:
        return (urllib.parse.urlsplit(value).hostname or "").lower()
    except Exception:
        return ""


def resolve_host_addresses(host: str, ttl: int = 300) -> list[str]:
    """Resolve a host and cache its IP addresses."""
    now = time.time()
    with DNS_CACHE_LOCK:
        cached = DNS_CACHE.get(host)
        if cached and cached[0] > now:
            return list(cached[1])

    addresses: set[str] = set()
    try:
        for item in socket.getaddrinfo(
            host, None, type=socket.SOCK_STREAM
        ):
            address = item[4][0]
            if address:
                addresses.add(address)
    except (socket.gaierror, OSError):
        pass

    result = sorted(addresses)
    with DNS_CACHE_LOCK:
        DNS_CACHE[host] = (now + ttl, result)
    return result


def is_public_address(address: str) -> bool:
    """Check whether an IP address is publicly routable."""
    try:
        parsed = ipaddress.ip_address(address)
        return bool(
            parsed.is_global
            and not parsed.is_private
            and not parsed.is_loopback
            and not parsed.is_link_local
            and not parsed.is_multicast
            and not parsed.is_reserved
            and not parsed.is_unspecified
        )
    except ValueError:
        return False


def url_safety_check(url: str) -> tuple[bool, str]:
    """Validate URL scheme and block private network targets by default."""
    normalized = normalize_url(url)
    if not normalized:
        return False, "INVALID_URL"

    host = url_host(normalized)
    if not host:
        return False, "MISSING_HOST"

    if ALLOW_PRIVATE_NETWORKS:
        return True, "ALLOWED_BY_CONFIGURATION"

    try:
        direct_ip = ipaddress.ip_address(host)
        if not is_public_address(str(direct_ip)):
            return False, "NON_PUBLIC_IP"
        return True, "PUBLIC_IP"
    except ValueError:
        pass

    addresses = resolve_host_addresses(host)
    if not addresses:
        return False, "DNS_RESOLUTION_FAILED"

    if any(not is_public_address(item) for item in addresses):
        return False, "HOST_RESOLVES_TO_NON_PUBLIC_ADDRESS"

    return True, "PUBLIC_HOST"


def content_type_looks_like_playlist(content_type: str, body: bytes) -> bool:
    """Check content type and initial bytes for playlist-like content."""
    lowered = (content_type or "").lower()
    if any(token in lowered for token in PLAYLIST_CONTENT_TYPES):
        return True
    prefix = body[:2048].decode("utf-8", errors="ignore").lstrip("\ufeff \r\n\t")
    return prefix.startswith("#EXTM3U") or "#EXTINF:" in prefix


def decode_body(body: bytes) -> str:
    """Decode a byte response using UTF-8 with fallback."""
    if body.startswith(b"\x1f\x8b"):
        try:
            body = gzip.decompress(body)
        except OSError:
            pass
    return body.decode("utf-8-sig", errors="replace")


def redact_url(url: str) -> str:
    """Redact common secret-like query parameters for logs and reports."""
    try:
        parsed = urllib.parse.urlsplit(url)
        secret_names = {
            "token", "key", "auth", "password", "pass", "secret",
            "signature", "sig", "access_token", "api_key",
        }
        query_items = urllib.parse.parse_qsl(
            parsed.query, keep_blank_values=True
        )
        cleaned = [
            (key, "***" if key.lower() in secret_names else value)
            for key, value in query_items
        ]
        query = urllib.parse.urlencode(cleaned)
        return urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, query, "")
        )
    except Exception:
        return url


# ============================================================================
# 3. HTTP CLIENT
# ============================================================================

@dataclasses.dataclass
class HttpResult:
    url: str
    final_url: str
    status: int
    headers: dict[str, str]
    body: bytes
    elapsed_seconds: float
    error: str = ""

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300 and not self.error


class HttpClient:
    """Thread-safe HTTP client with retries, size limits, and redirect checks."""

    def __init__(self) -> None:
        self.timeout = HTTP_TIMEOUT_SECONDS
        self.max_bytes = HTTP_MAX_BYTES

    def _request_once(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[dict[str, str]] = None,
        max_bytes: Optional[int] = None,
    ) -> HttpResult:
        started = time.monotonic()
        normalized = normalize_url(url)
        if not normalized:
            return HttpResult(url, url, 0, {}, b"", 0.0, "INVALID_URL")

        allowed, reason = url_safety_check(normalized)
        if not allowed:
            return HttpResult(
                normalized, normalized, 0, {}, b"",
                time.monotonic() - started, reason,
            )

        request_headers = {
            "User-Agent": HTTP_USER_AGENT,
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        }
        if headers:
            request_headers.update(headers)

        request = urllib.request.Request(
            normalized, headers=request_headers, method=method
        )
        limit = max_bytes if max_bytes is not None else self.max_bytes

        try:
            with urllib.request.urlopen(
                request,
                timeout=self.timeout,
                context=SSL_CONTEXT,
            ) as response:
                final_url = normalize_url(response.geturl())
                if not final_url:
                    final_url = response.geturl()

                final_allowed, final_reason = url_safety_check(final_url)
                if not final_allowed:
                    return HttpResult(
                        normalized,
                        final_url,
                        response.status,
                        dict(response.headers.items()),
                        b"",
                        time.monotonic() - started,
                        "UNSAFE_REDIRECT:" + final_reason,
                    )

                body = b""
                if method.upper() != "HEAD":
                    declared_length = response.headers.get("Content-Length")
                    if declared_length:
                        declared = safe_int(declared_length, -1)
                        if declared > limit:
                            return HttpResult(
                                normalized,
                                final_url,
                                response.status,
                                dict(response.headers.items()),
                                b"",
                                time.monotonic() - started,
                                "RESPONSE_TOO_LARGE",
                            )
                    body = response.read(limit + 1)
                    if len(body) > limit:
                        return HttpResult(
                            normalized,
                            final_url,
                            response.status,
                            dict(response.headers.items()),
                            b"",
                            time.monotonic() - started,
                            "RESPONSE_TOO_LARGE",
                        )

                return HttpResult(
                    normalized,
                    final_url,
                    safe_int(response.status),
                    dict(response.headers.items()),
                    body,
                    time.monotonic() - started,
                )

        except urllib.error.HTTPError as exc:
            try:
                body = exc.read(min(limit, 4096))
            except Exception:
                body = b""
            return HttpResult(
                normalized,
                normalized,
                safe_int(exc.code),
                dict(exc.headers.items()) if exc.headers else {},
                body,
                time.monotonic() - started,
                f"HTTP_ERROR:{exc.code}",
            )
        except (
            urllib.error.URLError,
            TimeoutError,
            socket.timeout,
            ssl.SSLError,
            OSError,
            ValueError,
        ) as exc:
            return HttpResult(
                normalized,
                normalized,
                0,
                {},
                b"",
                time.monotonic() - started,
                f"{type(exc).__name__}:{exc}",
            )

    def request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[dict[str, str]] = None,
        max_bytes: Optional[int] = None,
    ) -> HttpResult:
        """Request a URL with bounded retries."""
        last_result: Optional[HttpResult] = None

        for attempt in range(HTTP_RETRIES + 1):
            result = self._request_once(
                url, method=method, headers=headers, max_bytes=max_bytes
            )
            last_result = result

            if result.ok:
                return result

            if result.status and result.status not in RETRYABLE_HTTP_CODES:
                return result

            if attempt < HTTP_RETRIES:
                delay = HTTP_RETRY_BACKOFF * (2 ** attempt)
                delay += random.uniform(0.0, 0.25)
                time.sleep(delay)

        if last_result is not None:
            return last_result

        return HttpResult(url, url, 0, {}, b"", 0.0, "UNKNOWN_HTTP_FAILURE")

    def get_text(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        max_bytes: Optional[int] = None,
    ) -> tuple[HttpResult, str]:
        """Fetch text and decode the response."""
        result = self.request(
            url, method="GET", headers=headers, max_bytes=max_bytes
        )
        return result, decode_body(result.body)


HTTP = HttpClient()


# ============================================================================
# 4. FFPROBE BOOTSTRAP
# ============================================================================

def find_ffprobe() -> Optional[str]:
    """Find ffprobe in configuration, PATH, or local tools directories."""
    candidates = [
        FFPROBE_PATH,
        shutil.which("ffprobe") or "",
        str(TOOLS_DIR / "ffprobe"),
        str(TOOLS_DIR / "ffmpeg" / "ffprobe"),
        str(TOOLS_DIR / "bin" / "ffprobe"),
    ]

    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            try:
                completed = subprocess.run(
                    [str(path), "-version"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                    check=False,
                )
                if completed.returncode == 0:
                    return str(path.resolve())
            except (OSError, subprocess.SubprocessError):
                continue

    return None


def safe_extract_ffprobe_archive(archive_path: Path, destination: Path) -> Optional[str]:
    """Extract only a regular ffprobe executable from a trusted archive."""
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / "ffprobe"

    try:
        with tarfile.open(archive_path, mode="r:*") as archive:
            members = archive.getmembers()
            candidate = next(
                (
                    member
                    for member in members
                    if Path(member.name).name == "ffprobe"
                    and member.isfile()
                    and not member.issym()
                    and not member.islnk()
                ),
                None,
            )
            if candidate is None:
                return None

            extracted = archive.extractfile(candidate)
            if extracted is None:
                return None

            payload = extracted.read(100 * 1024 * 1024)
            if not payload:
                return None

            temporary = target.with_suffix(".tmp")
            temporary.write_bytes(payload)
            temporary.chmod(0o755)
            os.replace(temporary, target)

            try:
                result = subprocess.run(
                    [str(target), "-version"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                    check=False,
                )
                if result.returncode == 0:
                    return str(target.resolve())
            except (OSError, subprocess.SubprocessError):
                pass

    except (tarfile.TarError, OSError, EOFError):
        return None

    return None


def ensure_ffprobe() -> Optional[str]:
    """Find or best-effort install ffprobe without administrator privileges."""
    existing = find_ffprobe()
    if existing:
        log_event(logging.INFO, "ffprobe available: %s", existing)
        return existing

    if not FFPROBE_BOOTSTRAP_ENABLED:
        log_event(logging.WARNING, "ffprobe bootstrap is disabled")
        return None

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system != "linux" or machine not in {"x86_64", "amd64"}:
        log_event(
            logging.WARNING,
            "Automatic ffprobe bootstrap is unsupported on %s/%s",
            system,
            machine,
        )
        return None

    candidates = [
        item for item in FFPROBE_CANDIDATE_URLS if item and is_http_url(item)
    ]
    if not candidates:
        log_event(logging.WARNING, "No ffprobe download URL is configured")
        return None

    TOOLS_DIR.mkdir(parents=True, exist_ok=True)

    for url in dict.fromkeys(candidates):
        log_event(logging.INFO, "Attempting ffprobe bootstrap from %s", redact_url(url))
        result = HTTP.request(
            url,
            method="GET",
            max_bytes=250 * 1024 * 1024,
        )
        if not result.ok or not result.body:
            log_event(
                logging.WARNING,
                "ffprobe download failed: %s",
                result.error or result.status,
            )
            continue

        with tempfile.TemporaryDirectory(prefix="phoenix-ffprobe-") as temp_dir:
            archive_path = Path(temp_dir) / "ffmpeg-static.tar.xz"
            archive_path.write_bytes(result.body)
            installed = safe_extract_ffprobe_archive(archive_path, TOOLS_DIR)
            if installed:
                log_event(logging.INFO, "ffprobe installed: %s", installed)
                return installed

    log_event(
        logging.WARNING,
        "ffprobe could not be installed; HTTP diagnostics will continue",
    )
    return None


# ============================================================================
# 5. PLAYLIST DATA MODEL AND M3U PARSER
# ============================================================================

@dataclasses.dataclass
class PlaylistRecord:
    name: str
    url: str
    extinf: str = ""
    group_title: str = ""
    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    source_url: str = ""
    source_name: str = ""
    source_hash: str = ""
    stream_status: str = "NOT_CHECKED"
    http_status: int = 0
    ffprobe_status: str = "NOT_CHECKED"
    content_type: str = ""
    checked_at: str = ""
    error: str = ""
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def normalized_url(self) -> str:
        return normalize_url(self.url)

    @property
    def identity(self) -> str:
        return self.normalized_url or self.url.strip()

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def parse_extinf_attributes(extinf: str) -> dict[str, str]:
    """Parse quoted and unquoted EXTINF attributes."""
    result: dict[str, str] = {}
    if not extinf:
        return result

    pattern = re.compile(
        r"""([A-Za-z0-9_-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s,]+))"""
    )

    for match in pattern.finditer(extinf):
        key = match.group(1).lower()
        value = next(
            (group for group in match.groups()[1:] if group is not None), ""
        )
        result[key] = value

    return result


def extract_extinf_name(extinf: str) -> str:
    """Extract the display name after the final EXTINF comma."""
    if not extinf:
        return "Unknown channel"

    _, separator, name = extinf.partition(",")
    if not separator:
        return "Unknown channel"

    name = name.strip()
    return name or "Unknown channel"


def parse_m3u(
    content: str,
    source_url: str = "",
    source_name: str = "",
) -> list[PlaylistRecord]:
    """Parse M3U/M3U8 content while preserving the original EXTINF line."""
    records: list[PlaylistRecord] = []
    current_extinf = ""
    current_name = ""
    pending_metadata: dict[str, Any] = {}

    for raw_line in content.splitlines():
        line = raw_line.strip().lstrip("\ufeff")
        if not line:
            continue

        if line.startswith("#EXTINF:"):
            current_extinf = line
            current_name = extract_extinf_name(line)
            attributes = parse_extinf_attributes(line)

            pending_metadata = {
                "attributes": attributes,
                "group_title": attributes.get("group-title", ""),
                "tvg_id": attributes.get("tvg-id", ""),
                "tvg_name": attributes.get("tvg-name", ""),
                "tvg_logo": attributes.get("tvg-logo", ""),
            }
            continue

        if line.startswith("#"):
            if current_extinf:
                pending_metadata.setdefault("extra_tags", []).append(line)
            continue

        if not is_http_url(line):
            current_extinf = ""
            current_name = ""
            pending_metadata = {}
            continue

        normalized = normalize_url(line)
        if not normalized:
            current_extinf = ""
            current_name = ""
            pending_metadata = {}
            continue

        metadata = dict(pending_metadata)
        attributes = metadata.get("attributes", {})

        record = PlaylistRecord(
            name=current_name or attributes.get("tvg-name") or "Unknown channel",
            url=normalized,
            extinf=current_extinf,
            group_title=str(metadata.get("group_title", "")),
            tvg_id=str(metadata.get("tvg_id", "")),
            tvg_name=str(metadata.get("tvg_name", "")),
            tvg_logo=str(metadata.get("tvg_logo", "")),
            source_url=source_url,
            source_name=source_name,
            source_hash=sha256_text(source_url or source_name),
            metadata=metadata,
        )
        records.append(record)

        current_extinf = ""
        current_name = ""
        pending_metadata = {}

        if len(records) >= MAX_CHANNELS:
            log_event(
                logging.WARNING,
                "Playlist parsing stopped at MAX_CHANNELS=%s",
                MAX_CHANNELS,
            )
            break

    return records


def render_m3u(records: Iterable[PlaylistRecord]) -> str:
    """Render records as a UTF-8 M3U playlist."""
    lines = ["#EXTM3U"]

    for record in records:
        extinf = record.extinf.strip()
        if not extinf:
            name = (record.name or "Unknown channel").replace("\n", " ").strip()
            extinf = f"#EXTINF:-1,{name}"

        lines.append(extinf)
        lines.append(record.url)

    return "\n".join(lines) + "\n"


def extract_playlist_urls(content: str, base_url: str = "") -> list[str]:
    """Extract playlist candidates from text or HTML."""
    candidates: set[str] = set()

    for match in re.findall(
        r"""https?://[^\s"'<>]+""",
        content,
        flags=re.IGNORECASE,
    ):
        candidate = match.rstrip("),.;]} ")
        normalized = normalize_url(candidate)
        if normalized and any(
            suffix in urllib.parse.urlsplit(normalized).path.lower()
            for suffix in PLAYLIST_SUFFIXES
        ):
            candidates.add(normalized)

    for match in re.findall(
        r"""(?:href|src)\s*=\s*["']([^"']+)["']""",
        content,
        flags=re.IGNORECASE,
    ):
        candidate = urllib.parse.urljoin(base_url, match.strip())
        normalized = normalize_url(candidate)
        if normalized and any(
            suffix in urllib.parse.urlsplit(normalized).path.lower()
            for suffix in PLAYLIST_SUFFIXES
        ):
            candidates.add(normalized)

    return sorted(candidates)


def is_blacklisted_channel_name(name: str) -> bool:
    """Return True if a channel name matches the configured blacklist."""
    return any(pattern.search(name or "") for pattern in BLACKLIST_NAME_PATTERNS)



# ============================================================================
# 5b. UNIVERSAL LINK EXTRACTION AND CLASSIFICATION (Block 2)
# ============================================================================

# Broad URL regex that tolerates common escapes and HTML entities later.
_URL_RE = re.compile(
    r"""(?xi)
    (?:
        https?://[^\s<>"'`\)\]\}]+
        |
        //[^\s<>"'`\)\]\}]+
    )
    """,
)

# Common HTML entity replacements for URL recovery
_HTML_ENTITIES = {
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
}


def _unescape_html_entities(value: str) -> str:
    for entity, char in _HTML_ENTITIES.items():
        value = value.replace(entity, char)
    return value


def classify_url_kind(url: str, hint: str = "") -> str:
    """Classify a URL into playlist / stream / document / archive / page / other."""
    lowered = (url or "").lower().split("?", 1)[0].split("#", 1)[0]
    hint_l = (hint or "").lower()

    if any(lowered.endswith(ext) for ext in PLAYLIST_SUFFIXES):
        return "playlist"
    if any(lowered.endswith(ext) for ext in ARCHIVE_EXTENSIONS):
        return "archive"
    if any(lowered.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
        return "document"
    if any(token in hint_l for token in ("mpegurl", "m3u", "playlist")):
        return "playlist"
    if any(lowered.endswith(ext) for ext in (".ts", ".mp4", ".mkv", ".mpd", ".m4s", ".flv")):
        return "stream"
    if "/blob/" in lowered or "/tree/" in lowered or "github.com" in lowered:
        return "page"
    if "raw.githubusercontent.com" in lowered or "gist.githubusercontent.com" in lowered:
        # Could still be a document or playlist; prefer extension if present
        if any(lowered.endswith(ext) for ext in PLAYLIST_SUFFIXES):
            return "playlist"
        return "document"
    return "other"


def github_blob_to_raw(url: str) -> str:
    """Convert a GitHub blob / tree web URL into a raw.githubusercontent.com URL when possible."""
    try:
        parsed = urllib.parse.urlsplit(url.strip())
        host = (parsed.hostname or "").lower()
        if host not in {"github.com", "www.github.com"}:
            return ""
        parts = [p for p in parsed.path.split("/") if p]
        # Expected: owner/repo/blob/branch/path...  or tree
        if len(parts) < 5:
            return ""
        owner, repo, kind, branch = parts[0], parts[1], parts[2], parts[3]
        if kind not in {"blob", "tree", "raw"}:
            return ""
        rest = "/".join(parts[4:])
        if not rest:
            return ""
        return (
            f"https://raw.githubusercontent.com/"
            f"{owner}/{repo}/{urllib.parse.quote(branch, safe='')}/"
            f"{urllib.parse.quote(rest, safe='/')}"
        )
    except Exception:
        return ""


def extract_urls_from_text(
    content: str,
    *,
    base_url: str = "",
    origin_document: str = "",
) -> list[dict[str, Any]]:
    """
    Extract candidate URLs from arbitrary text (code, markdown, html, logs, m3u...).
    Returns list of dicts with url, line, fragment, kind.
    """
    if not content:
        return []

    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    lines = content.splitlines()
    for line_no, raw_line in enumerate(lines, start=1):
        line = _unescape_html_entities(raw_line)
        # Also try to recover URLs that were split by common escapes
        line = line.replace("\\/", "/").replace("\\u002F", "/")

        for match in _URL_RE.finditer(line):
            raw = match.group(0).rstrip(".,;:)]}>'\"")
            # Drop obvious non-http schemes that the regex might still catch via //
            if raw.startswith("//"):
                raw = "https:" + raw
            if not is_http_url(raw):
                continue

            normalized = normalize_url(raw)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)

            kind = classify_url_kind(normalized)
            # Prefer raw form for GitHub blob links when we can derive it
            raw_variant = github_blob_to_raw(normalized)
            if raw_variant and raw_variant not in seen:
                seen.add(raw_variant)
                results.append(
                    {
                        "url": raw_variant,
                        "line": line_no,
                        "fragment": raw_line.strip()[:240],
                        "kind": classify_url_kind(raw_variant),
                        "origin_document": origin_document,
                        "origin_url": base_url,
                        "from_blob": True,
                    }
                )

            results.append(
                {
                    "url": normalized,
                    "line": line_no,
                    "fragment": raw_line.strip()[:240],
                    "kind": kind,
                    "origin_document": origin_document,
                    "origin_url": base_url,
                    "from_blob": False,
                }
            )

    return results


def extract_urls_from_m3u_header(content: str, base_url: str = "") -> list[dict[str, Any]]:
    """
    Special handling for M3U/M3U8: collect URLs that appear before the first
    channel entry (comments, logos, group headers, nested playlist refs).
    """
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    lines = content.splitlines()
    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        # Stop when we hit a classic channel URL line (non-comment after EXTINF)
        if not stripped.startswith("#") and is_http_url(stripped):
            # This is likely a stream URL; still record it but mark as stream
            normalized = normalize_url(stripped)
            if normalized and normalized not in seen:
                seen.add(normalized)
                results.append(
                    {
                        "url": normalized,
                        "line": line_no,
                        "fragment": stripped[:240],
                        "kind": "stream",
                        "origin_document": "m3u-body",
                        "origin_url": base_url,
                    }
                )
            continue
        if stripped.startswith("#"):
            # Look for URLs inside comments / directives
            for item in extract_urls_from_text(stripped, base_url=base_url, origin_document="m3u-header"):
                if item["url"] not in seen:
                    seen.add(item["url"])
                    item["line"] = line_no
                    results.append(item)
    return results


def bump_discovery_counter(key: str, amount: int = 1) -> None:
    """Thread-safe-ish increment of discovery statistics."""
    if key in DISCOVERY_COUNTERS:
        DISCOVERY_COUNTERS[key] = DISCOVERY_COUNTERS.get(key, 0) + amount



# ============================================================================
# 6. SOURCE DISCOVERY
# ============================================================================

@dataclasses.dataclass
class SourceCandidate:
    url: str
    name: str
    provider: str
    discovered_at: str = dataclasses.field(default_factory=utc_now)
    discovery_query: str = ""
    score: float = 0.0
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)
    # Provenance / classification extensions (Block 2)
    origin_url: str = ""
    origin_document: str = ""
    origin_line: int = 0
    origin_fragment: str = ""
    candidate_kind: str = "unknown"  # playlist | stream | document | archive | page | other
    content_type_hint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class BaseHunter:
    """Base class for source discovery providers."""

    provider_name = "base"

    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def discover(self) -> list[SourceCandidate]:
        """Return discovered sources."""
        return []


class GitHubHunter(BaseHunter):
    """Search public GitHub repositories and code when API access permits."""

    provider_name = "github"

    def __init__(self, http: HttpClient) -> None:
        super().__init__(http)
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if GITHUB_TOKEN:
            self.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    def _search_query(self, query: str, max_pages: int | None = None) -> list[SourceCandidate]:
        """Search GitHub Code Search with controlled pagination and richer classification."""
        if not GITHUB_TOKEN:
            log_event(
                logging.WARNING,
                "GitHub API search may be restricted without GITHUB_TOKEN",
            )

        pages = max_pages or MAX_GITHUB_PAGES
        candidates: list[SourceCandidate] = []
        seen_urls: set[str] = set()

        for page in range(1, pages + 1):
            encoded = urllib.parse.urlencode(
                {"q": query, "per_page": 30, "page": page}
            )
            endpoint = f"{GITHUB_API_BASE}/search/code?{encoded}"
            result, body = self.http.get_text(endpoint, headers=self.headers)

            if not result.ok:
                log_event(
                    logging.WARNING,
                    "GitHub code search failed for query '%s' page %s: %s",
                    query,
                    page,
                    result.error or result.status,
                )
                # Continue with next page / query rather than aborting everything
                continue

            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                log_event(logging.WARNING, "GitHub returned invalid JSON for page %s", page)
                continue

            items = payload.get("items") or []
            if not items:
                break

            for item in items:
                html_url = str(item.get("html_url", "") or "")
                repository = item.get("repository") or {}
                path = str(item.get("path", "") or "")
                full_name = str(repository.get("full_name", "") or "")
                default_branch = str(repository.get("default_branch", "main") or "main")
                score = float(item.get("score") or 0.0)

                download_url = ""
                if full_name and path:
                    download_url = (
                        "https://raw.githubusercontent.com/"
                        f"{full_name}/{urllib.parse.quote(default_branch, safe='')}/"
                        f"{urllib.parse.quote(path, safe='/')}"
                    )

                # Prefer raw URL when available
                target = normalize_url(download_url) or normalize_url(html_url)
                if not target or target in seen_urls:
                    continue
                seen_urls.add(target)

                kind = classify_url_kind(target, hint=path)
                # Accept playlists, documents and archives; keep pages only as evidence
                if kind not in {"playlist", "document", "archive", "page", "other"}:
                    continue

                name = path.rsplit("/", 1)[-1] if path else target
                candidates.append(
                    SourceCandidate(
                        url=target,
                        name=name,
                        provider=self.provider_name,
                        discovery_query=query,
                        score=min(1.0, score / 100.0) if score else 0.4,
                        origin_url=html_url,
                        origin_document=path,
                        candidate_kind=kind,
                        metadata={
                            "html_url": html_url,
                            "repository": full_name,
                            "path": path,
                            "default_branch": default_branch,
                            "github_score": score,
                        },
                    )
                )
                bump_discovery_counter("documents_found")
                if kind == "playlist":
                    bump_discovery_counter("urls_classified_playlist")
                elif kind == "document":
                    bump_discovery_counter("urls_classified_document")
                elif kind == "archive":
                    bump_discovery_counter("urls_classified_archive")
                else:
                    bump_discovery_counter("urls_classified_other")

            # Honour secondary rate limits softly
            if page < pages:
                time.sleep(0.35)

        return candidates

    def discover(self) -> list[SourceCandidate]:
        """Run multiple search queries concurrently."""
        found: list[SourceCandidate] = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=SEARCH_WORKERS,
            thread_name_prefix="github-search",
        ) as executor:
            futures = {
                executor.submit(self._search_query, query): query
                for query in GITHUB_SEARCH_QUERIES
            }

            for future in concurrent.futures.as_completed(futures):
                query = futures[future]
                try:
                    found.extend(future.result())
                except Exception as exc:
                    log_event(
                        logging.WARNING,
                        "GitHub query failed '%s': %s",
                        query,
                        exc,
                    )

        return found


class GistHunter(BaseHunter):
    """Discover public Gists using the GitHub Gists API."""

    provider_name = "github-gist"

    def __init__(self, http: HttpClient) -> None:
        super().__init__(http)
        self.headers = {"Accept": "application/vnd.github+json"}
        if GITHUB_TOKEN:
            self.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    def _fetch_page(self, page: int) -> list[SourceCandidate]:
        endpoint = (
            f"{GITHUB_GIST_API}?per_page=100&page={page}"
        )
        result, body = self.http.get_text(endpoint, headers=self.headers)
        if not result.ok:
            log_event(
                logging.WARNING,
                "Gist page %s failed: %s",
                page,
                result.error or result.status,
            )
            return []

        try:
            items = json.loads(body)
        except json.JSONDecodeError:
            return []

        candidates: list[SourceCandidate] = []
        for gist in items if isinstance(items, list) else []:
            files = gist.get("files") or {}
            for filename, file_data in files.items():
                if not filename.lower().endswith(PLAYLIST_SUFFIXES):
                    continue

                raw_url = str(file_data.get("raw_url", ""))
                normalized = normalize_url(raw_url)
                if not normalized:
                    continue

                candidates.append(
                    SourceCandidate(
                        url=normalized,
                        name=filename,
                        provider=self.provider_name,
                        score=0.5,
                        metadata={
                            "gist_id": gist.get("id", ""),
                            "html_url": gist.get("html_url", ""),
                        },
                    )
                )

        return candidates

    def discover(self) -> list[SourceCandidate]:
        """Fetch multiple public Gist listing pages concurrently."""
        pages = range(1, MAX_GIST_PAGES + 1)
        found: list[SourceCandidate] = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=SEARCH_WORKERS,
            thread_name_prefix="gist-discovery",
        ) as executor:
            futures = [executor.submit(self._fetch_page, page) for page in pages]
            for future in concurrent.futures.as_completed(futures):
                try:
                    found.extend(future.result())
                except Exception as exc:
                    log_event(logging.WARNING, "Gist discovery failed: %s", exc)

        return found


class ConfiguredSourceHunter(BaseHunter):
    """Load user-configured source URLs and GitVerse seed URLs."""

    provider_name = "configured"

    def discover(self) -> list[SourceCandidate]:
        candidates: list[SourceCandidate] = []

        for url in CONFIGURED_SOURCE_URLS:
            normalized = normalize_url(url)
            if normalized:
                candidates.append(
                    SourceCandidate(
                        url=normalized,
                        name=urllib.parse.urlsplit(normalized).path.rsplit("/", 1)[-1]
                        or normalized,
                        provider="configured",
                        score=1.0,
                    )
                )

        for url in GITVERSE_SEED_URLS:
            normalized = normalize_url(url)
            if normalized:
                candidates.append(
                    SourceCandidate(
                        url=normalized,
                        name=urllib.parse.urlsplit(normalized).path.rsplit("/", 1)[-1]
                        or normalized,
                        provider="gitverse-seed",
                        score=0.6,
                    )
                )

        return candidates



class DocumentLinkHunter(BaseHunter):
    """
    Secondary pass: download a limited set of already-discovered documents
    (code, markdown, json, etc.) and extract additional playlist / stream URLs
    from their content. Bounded by size, time and count.
    """

    provider_name = "document-scan"

    def __init__(self, http: HttpClient, seed_candidates: list[SourceCandidate] | None = None) -> None:
        super().__init__(http)
        self.seed_candidates = seed_candidates or []

    def _safe_fetch_text(self, url: str) -> tuple[str, str]:
        """Return (final_url, text) or ("", "") on failure / non-text."""
        try:
            result, body = self.http.get_text(url)
            if not result.ok or not body:
                return "", ""
            # Reject obvious binary / huge content
            if len(body) > HTTP_MAX_BYTES:
                return "", ""
            text = body if isinstance(body, str) else decode_body(body.encode("utf-8", errors="replace") if isinstance(body, str) else body)
            return result.final_url or url, text
        except Exception:
            return "", ""

    def discover(self) -> list[SourceCandidate]:
        candidates: list[SourceCandidate] = []
        # Only scan documents / pages, not pure streams
        seeds = [
            c for c in self.seed_candidates
            if c.candidate_kind in {"document", "page", "other", "unknown"}
        ][: min(40, MAX_DISCOVERED_SOURCES // 3)]

        for seed in seeds:
            bump_discovery_counter("documents_found")
            final_url, content = self._safe_fetch_text(seed.url)
            if not content:
                bump_discovery_counter("documents_skipped")
                continue

            bump_discovery_counter("documents_processed")
            extracted = extract_urls_from_text(
                content,
                base_url=final_url or seed.url,
                origin_document=seed.name or seed.url,
            )
            # Also try M3U-header style extraction if content looks like playlist
            if content.lstrip().startswith("#EXTM3U") or "#EXTINF:" in content[:4096]:
                extracted.extend(
                    extract_urls_from_m3u_header(content, base_url=final_url or seed.url)
                )

            for item in extracted:
                kind = item.get("kind") or classify_url_kind(item["url"])
                if kind not in {"playlist", "document", "archive"}:
                    # We only promote playlist-like or further documents from this pass
                    if kind != "stream":
                        continue
                normalized = normalize_url(item["url"])
                if not normalized:
                    continue
                bump_discovery_counter("urls_extracted")
                if kind == "playlist":
                    bump_discovery_counter("urls_classified_playlist")
                elif kind == "document":
                    bump_discovery_counter("urls_classified_document")
                elif kind == "archive":
                    bump_discovery_counter("urls_classified_archive")
                else:
                    bump_discovery_counter("urls_classified_other")

                candidates.append(
                    SourceCandidate(
                        url=normalized,
                        name=normalized.rsplit("/", 1)[-1] or normalized,
                        provider=self.provider_name,
                        score=0.35,
                        origin_url=seed.url,
                        origin_document=item.get("origin_document", ""),
                        origin_line=int(item.get("line") or 0),
                        origin_fragment=str(item.get("fragment") or "")[:240],
                        candidate_kind=kind,
                        metadata={
                            "parent_provider": seed.provider,
                            "parent_url": seed.url,
                        },
                    )
                )

        return candidates


class SourceDiscovery:
    """Coordinate all configured discovery providers."""

    def __init__(self, http: HttpClient) -> None:
        self.http = http
        self.providers: list[BaseHunter] = [
            ConfiguredSourceHunter(http),
            GitHubHunter(http),
            GistHunter(http),
            # DocumentLinkHunter is added dynamically after the first wave
            # so that it can receive seeds. It is still registered for future use.
        ]
        self._document_hunter = DocumentLinkHunter(http)

    def discover(self) -> list[SourceCandidate]:
        """Run providers concurrently and return unique source candidates with provenance."""
        found: list[SourceCandidate] = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=SEARCH_WORKERS,
            thread_name_prefix="source-discovery",
        ) as executor:
            futures = {
                executor.submit(provider.discover): provider.provider_name
                for provider in self.providers
            }

            for future in concurrent.futures.as_completed(futures):
                provider_name = futures[future]
                try:
                    results = future.result()
                    found.extend(results)
                    log_event(
                        logging.INFO,
                        "Discovery provider %s returned %s candidates",
                        provider_name,
                        len(results),
                    )
                except Exception as exc:
                    bump_discovery_counter("documents_error")
                    log_event(
                        logging.WARNING,
                        "Discovery provider %s failed: %s",
                        provider_name,
                        exc,
                    )

        unique: dict[str, SourceCandidate] = {}
        for candidate in found:
            normalized = normalize_url(candidate.url)
            if not normalized:
                bump_discovery_counter("documents_skipped")
                continue
            if normalized in unique:
                # Keep provenance of the first occurrence, mark duplicate
                bump_discovery_counter("documents_duplicate")
                existing = unique[normalized]
                # Merge discovery sources into metadata without losing data
                origins = existing.metadata.setdefault("discovery_origins", [])
                origins.append(
                    {
                        "provider": candidate.provider,
                        "query": candidate.discovery_query,
                        "origin_url": candidate.origin_url,
                        "origin_document": candidate.origin_document,
                    }
                )
                continue

            candidate.url = normalized
            if not candidate.candidate_kind or candidate.candidate_kind == "unknown":
                candidate.candidate_kind = classify_url_kind(normalized)
            unique[normalized] = candidate
            bump_discovery_counter("documents_processed")

        result = list(unique.values())[:MAX_DISCOVERED_SOURCES]
        log_event(
            logging.INFO,
            "Total unique sources discovered: %s | counters=%s",
            len(result),
            dict(DISCOVERY_COUNTERS),
        )
        return result


# ============================================================================
# 7. SQLITE STORE
# ============================================================================

class HunterStore:
    """Persist source, run, and stream-check information in SQLite."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.database_path),
            timeout=30,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    summary_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS sources (
                    url TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    provider TEXT NOT NULL DEFAULT '',
                    discovered_at TEXT NOT NULL,
                    last_status TEXT NOT NULL DEFAULT '',
                    last_checked_at TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS streams (
                    url TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    http_status INTEGER NOT NULL DEFAULT 0,
                    ffprobe_status TEXT NOT NULL DEFAULT '',
                    checked_at TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '',
                    record_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_streams_status
                    ON streams(status);

                CREATE INDEX IF NOT EXISTS idx_sources_provider
                    ON sources(provider);
                """
            )

    def start_run(self, run_id: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO runs
                    (run_id, started_at, finished_at, status, summary_json)
                VALUES (?, ?, NULL, ?, '{}')
                """,
                (run_id, utc_now(), "RUNNING"),
            )

    def finish_run(
        self,
        run_id: str,
        status: str,
        summary: dict[str, Any],
    ) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE runs
                SET finished_at=?, status=?, summary_json=?
                WHERE run_id=?
                """,
                (
                    utc_now(),
                    status,
                    json.dumps(summary, ensure_ascii=False, default=str),
                    run_id,
                ),
            )

    def save_sources(self, sources: Iterable[SourceCandidate]) -> None:
        rows = [
            (
                normalize_url(source.url),
                source.name,
                source.provider,
                source.discovered_at,
                "",
                "",
                json.dumps(source.metadata, ensure_ascii=False, default=str),
            )
            for source in sources
            if normalize_url(source.url)
        ]

        if not rows:
            return

        with self._lock, self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO sources
                    (url, name, provider, discovered_at, last_status,
                     last_checked_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    name=excluded.name,
                    provider=excluded.provider,
                    metadata_json=excluded.metadata_json
                """,
                rows,
            )

    def save_records(self, records: Iterable[PlaylistRecord]) -> None:
        rows = [
            (
                record.identity,
                record.name,
                record.source_url,
                record.stream_status,
                record.http_status,
                record.ffprobe_status,
                record.checked_at,
                record.error,
                json.dumps(record.to_dict(), ensure_ascii=False, default=str),
            )
            for record in records
            if record.identity
        ]

        if not rows:
            return

        with self._lock, self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO streams
                    (url, name, source_url, status, http_status,
                     ffprobe_status, checked_at, error, record_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    name=excluded.name,
                    source_url=excluded.source_url,
                    status=excluded.status,
                    http_status=excluded.http_status,
                    ffprobe_status=excluded.ffprobe_status,
                    checked_at=excluded.checked_at,
                    error=excluded.error,
                    record_json=excluded.record_json
                """,
                rows,
            )

    def get_recent_stream_result(self, url: str) -> Optional[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT record_json FROM streams WHERE url=?",
                (normalize_url(url),),
            ).fetchone()

        if not row:
            return None

        try:
            value = json.loads(row["record_json"])
            return value if isinstance(value, dict) else None
        except json.JSONDecodeError:
            return None


# ============================================================================
# 8. SOURCE FETCHING AND NESTED PLAYLIST DISCOVERY
# ============================================================================

@dataclasses.dataclass
class SourceFetchResult:
    source: SourceCandidate
    status: str
    http_status: int
    content_type: str
    final_url: str
    body_text: str
    records: list[PlaylistRecord]
    nested_urls: list[str]
    error: str = ""
    elapsed_seconds: float = 0.0


def fetch_source_content(
    source: SourceCandidate,
    http: HttpClient,
) -> SourceFetchResult:
    """Fetch and parse one source; this function is safe for worker threads."""
    started = time.monotonic()
    result, body = http.get_text(source.url)

    if not result.ok:
        return SourceFetchResult(
            source=source,
            status="FETCH_FAILED",
            http_status=result.status,
            content_type=result.headers.get("Content-Type", ""),
            final_url=result.final_url,
            body_text="",
            records=[],
            nested_urls=[],
            error=result.error or f"HTTP_{result.status}",
            elapsed_seconds=time.monotonic() - started,
        )

    content_type = result.headers.get("Content-Type", "")
    if not content_type_looks_like_playlist(content_type, result.body):
        nested = extract_playlist_urls(body, result.final_url)
        return SourceFetchResult(
            source=source,
            status="NON_PLAYLIST_PAGE",
            http_status=result.status,
            content_type=content_type,
            final_url=result.final_url,
            body_text=body,
            records=[],
            nested_urls=nested,
            error="CONTENT_NOT_RECOGNIZED_AS_PLAYLIST",
            elapsed_seconds=time.monotonic() - started,
        )

    records = parse_m3u(
        body,
        source_url=result.final_url,
        source_name=source.name,
    )
    nested = extract_playlist_urls(body, result.final_url)

    return SourceFetchResult(
        source=source,
        status="PARSED" if records else "EMPTY_PLAYLIST",
        http_status=result.status,
        content_type=content_type,
        final_url=result.final_url,
        body_text=body,
        records=records,
        nested_urls=nested,
        error="" if records else "NO_VALID_ENTRIES",
        elapsed_seconds=time.monotonic() - started,
    )


class PlaylistFetcher:
    """Fetch and parse source playlists with bounded parallelism."""

    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def fetch_all(
        self,
        sources: list[SourceCandidate],
    ) -> list[SourceFetchResult]:
        results: list[SourceFetchResult] = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=PARSING_WORKERS,
            thread_name_prefix="playlist-fetch",
        ) as executor:
            futures = {
                executor.submit(fetch_source_content, source, self.http): source
                for source in sources
            }

            for future in concurrent.futures.as_completed(futures):
                source = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    log_event(
                        logging.INFO,
                        "Source %s: status=%s entries=%s elapsed=%.2fs",
                        redact_url(source.url),
                        result.status,
                        len(result.records),
                        result.elapsed_seconds,
                    )
                except Exception as exc:
                    log_event(
                        logging.WARNING,
                        "Source worker failed for %s: %s",
                        redact_url(source.url),
                        exc,
                    )
                    results.append(
                        SourceFetchResult(
                            source=source,
                            status="WORKER_ERROR",
                            http_status=0,
                            content_type="",
                            final_url=source.url,
                            body_text="",
                            records=[],
                            nested_urls=[],
                            error=str(exc),
                        )
                    )

        return results

    def fetch_nested(
        self,
        initial_results: list[SourceFetchResult],
        already_seen: set[str],
    ) -> list[SourceFetchResult]:
        """Fetch a bounded number of nested playlists in subsequent rounds."""
        if not NESTED_PLAYLISTS_ENABLED or MAX_NESTED_PLAYLISTS <= 0:
            return []

        discovered: list[SourceCandidate] = []
        seen = set(already_seen)

        for result in initial_results:
            for nested_url in result.nested_urls:
                normalized = normalize_url(nested_url)
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                discovered.append(
                    SourceCandidate(
                        url=normalized,
                        name=urllib.parse.urlsplit(normalized).path.rsplit("/", 1)[-1]
                        or "nested-playlist",
                        provider="nested-playlist",
                        score=0.4,
                        metadata={"parent_source": result.source.url},
                    )
                )
                if len(discovered) >= MAX_NESTED_PLAYLISTS:
                    break
            if len(discovered) >= MAX_NESTED_PLAYLISTS:
                break

        if not discovered:
            return []

        log_event(
            logging.INFO,
            "Fetching nested playlist candidates: %s",
            len(discovered),
        )
        return self.fetch_all(discovered)


# ============================================================================
# 9. STREAM CHECKER
# ============================================================================

def get_url_check_lock(url: str) -> threading.Lock:
    """Get a per-URL lock to prevent duplicate simultaneous checks."""
    normalized = normalize_url(url)
    with URL_CHECK_LOCKS_LOCK:
        lock = URL_CHECK_LOCKS.get(normalized)
        if lock is None:
            lock = threading.Lock()
            URL_CHECK_LOCKS[normalized] = lock
        return lock


class StreamChecker:
    """Check stream endpoints using HEAD, GET Range, and optional ffprobe."""

    def __init__(
        self,
        http: HttpClient,
        ffprobe_path: Optional[str],
        store: HunterStore,
    ) -> None:
        self.http = http
        self.ffprobe_path = ffprobe_path
        self.store = store

    def _cache_get(self, url: str) -> Optional[dict[str, Any]]:
        if STREAM_CACHE_TTL_SECONDS <= 0:
            return None

        now = time.time()
        with URL_CHECK_CACHE_LOCK:
            item = URL_CHECK_CACHE.get(url)
            if item and item[0] > now:
                return dict(item[1])

        stored = self.store.get_recent_stream_result(url)
        if stored:
            checked_at = str(stored.get("checked_at", ""))
            try:
                checked_dt = dt.datetime.fromisoformat(checked_at)
                age = (
                    dt.datetime.now(dt.timezone.utc)
                    - checked_dt.astimezone(dt.timezone.utc)
                ).total_seconds()
                if 0 <= age <= STREAM_CACHE_TTL_SECONDS:
                    return stored
            except (ValueError, TypeError):
                pass

        return None

    def _cache_set(self, url: str, value: dict[str, Any]) -> None:
        if STREAM_CACHE_TTL_SECONDS <= 0:
            return
        with URL_CHECK_CACHE_LOCK:
            URL_CHECK_CACHE[url] = (
                time.time() + STREAM_CACHE_TTL_SECONDS,
                dict(value),
            )

    def _ffprobe(self, url: str) -> tuple[str, str]:
        if not STREAM_CHECK_FFPROBE:
            return "DISABLED", ""

        if not self.ffprobe_path:
            return "UNAVAILABLE", "FFPROBE_NOT_INSTALLED"

        command = [
            self.ffprobe_path,
            "-v", "error",
            "-rw_timeout", str(FFPROBE_TIMEOUT_SECONDS * 1_000_000),
            "-show_entries", "format=format_name,duration",
            "-show_entries", "stream=codec_type,codec_name",
            "-of", "json",
            url,
        ]

        try:
            completed = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=FFPROBE_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return "TIMEOUT", "FFPROBE_TIMEOUT"
        except OSError as exc:
            return "ERROR", f"FFPROBE_EXECUTION_ERROR:{exc}"

        if completed.returncode == 0:
            try:
                payload = json.loads(completed.stdout or "{}")
            except json.JSONDecodeError:
                return "UNKNOWN", "FFPROBE_INVALID_JSON"

            streams = payload.get("streams") or []
            fmt = payload.get("format") or {}
            if streams or fmt:
                return "OK", ""
            return "NO_MEDIA_DATA", "FFPROBE_RETURNED_NO_MEDIA_DATA"

        error_text = (completed.stderr or "").strip()
        return "FAILED", error_text[:1000]

    def _check_uncached(self, record: PlaylistRecord) -> dict[str, Any]:
        started = time.monotonic()
        url = normalize_url(record.url)

        safe, safety_reason = url_safety_check(url)
        if not safe:
            return {
                "stream_status": "BLOCKED",
                "http_status": 0,
                "ffprobe_status": "NOT_RUN",
                "content_type": "",
                "checked_at": utc_now(),
                "error": safety_reason,
                "elapsed_seconds": time.monotonic() - started,
            }

        head = self.http.request(
            url,
            method="HEAD",
            headers={"Accept": "*/*"},
            max_bytes=1024,
        )

        http_status = head.status
        content_type = head.headers.get("Content-Type", "")
        transport_error = head.error

        # Some stream hosts reject HEAD, so fall back to a small GET.
        if not head.ok or head.status in {403, 405, 501}:
            get_result = self.http.request(
                url,
                method="GET",
                headers={
                    "Accept": "*/*",
                    "Range": "bytes=0-4095",
                },
                max_bytes=8192,
            )
            if get_result.status:
                http_status = get_result.status
            content_type = (
                get_result.headers.get("Content-Type", "") or content_type
            )
            if get_result.ok:
                transport_error = ""
            elif get_result.error:
                transport_error = get_result.error

        http_ok = 200 <= http_status < 400
        ffprobe_status = "NOT_RUN"
        ffprobe_error = ""

        if http_ok and STREAM_CHECK_FFPROBE:
            ffprobe_status, ffprobe_error = self._ffprobe(url)

        if http_ok:
            if ffprobe_status == "OK":
                stream_status = "PLAYABLE"
            elif ffprobe_status in {"FAILED", "TIMEOUT", "NO_MEDIA_DATA"}:
                stream_status = "HTTP_OK_FFPROBE_FAILED"
            elif not STREAM_CHECK_FFPROBE:
                stream_status = "HTTP_OK_NOT_PROBED"
            else:
                stream_status = "HTTP_OK_PROBE_UNAVAILABLE"
        else:
            stream_status = "HTTP_FAILED"

        error_parts = [
            item for item in (transport_error, ffprobe_error) if item
        ]

        return {
            "stream_status": stream_status,
            "http_status": http_status,
            "ffprobe_status": ffprobe_status,
            "content_type": content_type,
            "checked_at": utc_now(),
            "error": " | ".join(error_parts),
            "elapsed_seconds": time.monotonic() - started,
        }

    def check(self, record: PlaylistRecord) -> PlaylistRecord:
        """Check one record, using cache and a per-URL lock."""
        normalized = normalize_url(record.url)
        lock = get_url_check_lock(normalized)

        with lock:
            cached = self._cache_get(normalized)
            if cached:
                record.stream_status = str(
                    cached.get("stream_status", "UNKNOWN")
                )
                record.http_status = safe_int(cached.get("http_status"))
                record.ffprobe_status = str(
                    cached.get("ffprobe_status", "NOT_CHECKED")
                )
                record.content_type = str(cached.get("content_type", ""))
                record.checked_at = str(cached.get("checked_at", ""))
                record.error = str(cached.get("error", ""))
                return record

            result = self._check_uncached(record)
            record.stream_status = str(result["stream_status"])
            record.http_status = safe_int(result["http_status"])
            record.ffprobe_status = str(result["ffprobe_status"])
            record.content_type = str(result["content_type"])
            record.checked_at = str(result["checked_at"])
            record.error = str(result["error"])

            self._cache_set(normalized, result)
            return record

    def check_all(
        self,
        records: list[PlaylistRecord],
    ) -> list[PlaylistRecord]:
        """Run stream checks with at least 40 workers by default."""
        checked: list[PlaylistRecord] = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=STREAM_CHECK_WORKERS,
            thread_name_prefix="stream-check",
        ) as executor:
            futures = {
                executor.submit(self.check, record): record
                for record in records
            }

            total = len(futures)
            completed = 0

            for future in concurrent.futures.as_completed(futures):
                record = futures[future]
                try:
                    checked.append(future.result())
                except Exception as exc:
                    record.stream_status = "CHECK_ERROR"
                    record.checked_at = utc_now()
                    record.error = str(exc)
                    checked.append(record)

                completed += 1
                if completed % 100 == 0 or completed == total:
                    log_event(
                        logging.INFO,
                        "Stream checks: %s/%s completed",
                        completed,
                        total,
                    )

        return checked


# ============================================================================
# 10. DEDUPLICATION AND DONOR FILTERING
# ============================================================================

def deduplicate_records(
    records: Iterable[PlaylistRecord],
) -> list[PlaylistRecord]:
    """Deduplicate by normalized URL while preserving first-seen metadata."""
    unique: dict[str, PlaylistRecord] = {}

    for record in records:
        key = record.identity
        if not key:
            continue

        if key not in unique:
            unique[key] = record
            continue

        current = unique[key]

        # Prefer a meaningful name if the first entry was empty or generic.
        if (
            current.name.strip().lower() in {"", "unknown", "unknown channel"}
            and record.name.strip()
        ):
            current.name = record.name

        # Keep available metadata from later donor entries.
        if not current.extinf and record.extinf:
            current.extinf = record.extinf
        if not current.group_title and record.group_title:
            current.group_title = record.group_title
        if not current.tvg_logo and record.tvg_logo:
            current.tvg_logo = record.tvg_logo

    return list(unique.values())


def select_donor_records(
    records: list[PlaylistRecord],
) -> tuple[list[PlaylistRecord], dict[str, int]]:
    """Select donor records according to the configured verification policy."""
    selected: list[PlaylistRecord] = []
    counts: Counter[str] = Counter()

    for record in records:
        if is_blacklisted_channel_name(record.name):
            counts["blacklisted_name"] += 1
            continue

        if not is_http_url(record.url):
            counts["invalid_url"] += 1
            continue

        status = record.stream_status or "NOT_CHECKED"

        if status == "PLAYABLE":
            selected.append(record)
            counts["playable"] += 1
            continue

        if status in {"HTTP_OK_NOT_PROBED", "HTTP_OK_PROBE_UNAVAILABLE"}:
            selected.append(record)
            counts["http_ok_unprobed"] += 1
            continue

        if status == "HTTP_OK_FFPROBE_FAILED":
            # A failed probe is not sufficient evidence that a stream is dead.
            if DONOR_INCLUDE_UNVERIFIED:
                selected.append(record)
                counts["probe_failed_kept"] += 1
            else:
                counts["probe_failed_excluded"] += 1
            continue

        if status in {"NOT_CHECKED", "CHECK_ERROR", "UNKNOWN"}:
            if DONOR_INCLUDE_UNVERIFIED:
                selected.append(record)
                counts["unverified_kept"] += 1
            else:
                counts["unverified_excluded"] += 1
            continue

        if status in {"HTTP_FAILED", "BLOCKED"}:
            if DONOR_INCLUDE_UNVERIFIED and status == "HTTP_FAILED":
                selected.append(record)
                counts["http_failed_kept_by_policy"] += 1
            else:
                counts["http_failed_excluded"] += 1
            continue

        if DONOR_INCLUDE_UNVERIFIED:
            selected.append(record)
            counts["other_status_kept"] += 1
        else:
            counts["other_status_excluded"] += 1

    return selected, dict(counts)


# ============================================================================
# 11. SKALA VERSIONING AND AUDIT
# ============================================================================

def record_fingerprint(record: PlaylistRecord) -> str:
    """Build a stable fingerprint from channel identity and metadata."""
    data = {
        "url": record.identity,
        "name": record.name,
        "extinf": record.extinf,
        "group_title": record.group_title,
        "tvg_id": record.tvg_id,
        "tvg_name": record.tvg_name,
        "tvg_logo": record.tvg_logo,
    }
    return sha256_text(json.dumps(data, sort_keys=True, ensure_ascii=False))


def read_playlist_file(path: Path) -> list[PlaylistRecord]:
    """Read a local M3U file."""
    if not path.is_file():
        return []
    try:
        content = path.read_text(encoding="utf-8-sig", errors="replace")
        return parse_m3u(content, source_url=str(path), source_name=path.name)
    except OSError as exc:
        log_event(logging.WARNING, "Could not read %s: %s", path, exc)
        return []


class Skala:
    """Version and audit Donor.m3u without requiring Git history."""

    def __init__(
        self,
        donor_path: Path,
        archive_path: Path,
        report_dir: Path,
    ) -> None:
        self.donor_path = donor_path
        self.archive_path = archive_path
        self.report_dir = report_dir
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def read_previous(self) -> list[PlaylistRecord]:
        """Read the previous donor from the ZIP archive if available."""
        if not self.archive_path.is_file():
            return []

        try:
            with zipfile.ZipFile(self.archive_path, "r") as archive:
                names = archive.namelist()
                preferred = [
                    name for name in names
                    if Path(name).name.lower() in {
                        "donor.m3u", "donor.m3u8", "stable.m3u"
                    }
                ]
                if not preferred:
                    preferred = [
                        name for name in names
                        if name.lower().endswith((".m3u", ".m3u8"))
                    ]
                if not preferred:
                    return []

                payload = archive.read(preferred[0])
                content = payload.decode("utf-8-sig", errors="replace")
                return parse_m3u(
                    content,
                    source_url=str(self.archive_path),
                    source_name=preferred[0],
                )
        except (OSError, zipfile.BadZipFile, KeyError) as exc:
            log_event(logging.WARNING, "Previous SKALA archive is unreadable: %s", exc)
            return []

    def _map_records(
        self,
        records: list[PlaylistRecord],
    ) -> dict[str, PlaylistRecord]:
        mapped: dict[str, PlaylistRecord] = {}
        for record in records:
            if record.identity and record.identity not in mapped:
                mapped[record.identity] = record
        return mapped

    def compare(
        self,
        before: list[PlaylistRecord],
        after: list[PlaylistRecord],
    ) -> dict[str, Any]:
        """Compare previous and new donor snapshots."""
        old_map = self._map_records(before)
        new_map = self._map_records(after)

        old_keys = set(old_map)
        new_keys = set(new_map)

        added_keys = sorted(new_keys - old_keys)
        removed_keys = sorted(old_keys - new_keys)
        common_keys = sorted(old_keys & new_keys)

        changed: list[dict[str, Any]] = []
        for key in common_keys:
            old_record = old_map[key]
            new_record = new_map[key]
            if record_fingerprint(old_record) != record_fingerprint(new_record):
                changed.append(
                    {
                        "url": key,
                        "before": old_record.to_dict(),
                        "after": new_record.to_dict(),
                    }
                )

        return {
            "created_at": utc_now(),
            "before_count": len(before),
            "after_count": len(after),
            "added_count": len(added_keys),
            "removed_count": len(removed_keys),
            "changed_count": len(changed),
            "added": [new_map[key].to_dict() for key in added_keys],
            "removed": [old_map[key].to_dict() for key in removed_keys],
            "changed": changed,
            "before": [item.to_dict() for item in before],
            "after": [item.to_dict() for item in after],
        }

    def archive_current(self) -> bool:
        """Archive the current donor before replacing it."""
        if not self.donor_path.is_file():
            return False

        self.archive_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(
                self.archive_path,
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=6,
            ) as archive:
                archive.write(self.donor_path, arcname="Donor.m3u")
                archive.writestr("archive_metadata.json", json.dumps(
                    {
                        "archived_at": utc_now(),
                        "source": str(self.donor_path),
                        "size_bytes": self.donor_path.stat().st_size,
                    },
                    ensure_ascii=False,
                    indent=2,
                ))
            return True
        except OSError as exc:
            log_event(logging.ERROR, "Failed to archive current donor: %s", exc)
            return False

    def write_report(self, report: dict[str, Any]) -> Path:
        """Write JSON and text SKALA reports."""
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = self.report_dir / f"skala_{timestamp}.json"
        text_path = self.report_dir / f"skala_{timestamp}.txt"

        atomic_write_json(json_path, report)

        lines = [
            "Phoenix Hub — SKALA",
            f"Generated: {report.get('created_at', '')}",
            "",
            "BEFORE / CHANGES / AFTER",
            f"BEFORE: {report.get('before_count', 0)}",
            f"ADDED: {report.get('added_count', 0)}",
            f"REMOVED: {report.get('removed_count', 0)}",
            f"CHANGED: {report.get('changed_count', 0)}",
            f"AFTER: {report.get('after_count', 0)}",
            "",
            "ADDED CHANNELS",
        ]

        for item in report.get("added", []):
            lines.append(f"+ {item.get('name', '')} | {item.get('url', '')}")

        lines.extend(["", "REMOVED CHANNELS"])
        for item in report.get("removed", []):
            lines.append(f"- {item.get('name', '')} | {item.get('url', '')}")

        lines.extend(["", "CHANGED CHANNELS"])
        for item in report.get("changed", []):
            before_item = item.get("before", {})
            after_item = item.get("after", {})
            lines.append(
                f"* {before_item.get('name', '')} -> {after_item.get('name', '')}"
                f" | {item.get('url', '')}"
            )

        atomic_write_text(text_path, "\n".join(lines) + "\n")
        return json_path


# ============================================================================
# 12. RECEIVER BRIDGE
# ============================================================================

class ReceiverBridge:
    """Create a local outbox message for a separately managed receiver."""

    def __init__(self, outbox: Path, source_url: str) -> None:
        self.outbox = outbox
        self.source_url = source_url
        self.outbox.mkdir(parents=True, exist_ok=True)

    def create_payload(
        self,
        run_id: str,
        records: list[PlaylistRecord],
        results_path: Path,
    ) -> Path:
        """Write a local JSON payload; do not send it to the source URL."""
        payload = {
            "run_id": run_id,
            "created_at": utc_now(),
            "application": APP_NAME,
            "version": APP_VERSION,
            "receiver_source_url": self.source_url,
            "transport_enabled": RECEIVER_TRANSPORT_ENABLED,
            "transport_note": (
                "No network submission performed. The configured URL is a raw "
                "source file, not a documented receiver API."
            ),
            "results_path": str(results_path),
            "record_count": len(records),
            "records": [record.to_dict() for record in records],
        }

        path = self.outbox / f"receiver_payload_{run_id}.json"
        atomic_write_json(path, payload)
        return path


# ============================================================================
# 13. UNIFIED ORCHESTRATOR
# ============================================================================

class UnifiedPlaylistHunter:
    """Run discovery, fetch, parse, check, donor generation, and SKALA audit."""

    def __init__(self) -> None:
        self.run_id = dt.datetime.now().strftime("%Y%m%dT%H%M%SZ")
        self.http = HTTP
        self.store = HunterStore(DATABASE_PATH)
        self.discovery = SourceDiscovery(self.http)
        self.fetcher = PlaylistFetcher(self.http)
        self.ffprobe_path: Optional[str] = None
        self.stream_checker: Optional[StreamChecker] = None
        self.skala = Skala(
            donor_path=DONOR_PATH,
            archive_path=SKALA_ARCHIVE_PATH,
            report_dir=SKALA_REPORT_DIR,
        )
        self.summary: dict[str, Any] = {
            "run_id": self.run_id,
            "application": APP_NAME,
            "version": APP_VERSION,
            "started_at": utc_now(),
            "status": "RUNNING",
            "concurrency": {
                "search_workers": SEARCH_WORKERS,
                "parsing_workers": PARSING_WORKERS,
                "stream_check_workers": STREAM_CHECK_WORKERS,
                "http_max_connections": HTTP_MAX_CONNECTIONS,
            },
        }

    def _log_configuration(self) -> None:
        """Log the effective runtime configuration."""
        log_event(logging.INFO, "%s %s", APP_NAME, APP_VERSION)
        log_event(
            logging.INFO,
            "Concurrency configuration: search=%s parsing=%s stream_check=%s",
            SEARCH_WORKERS,
            PARSING_WORKERS,
            STREAM_CHECK_WORKERS,
        )
        log_event(
            logging.INFO,
            "Output directory: %s",
            OUTPUT_DIR,
        )
        log_event(
            logging.INFO,
            "Donor path: %s",
            DONOR_PATH,
        )
        log_event(
            logging.INFO,
            "Private network access: %s",
            ALLOW_PRIVATE_NETWORKS,
        )
        log_event(
            logging.INFO,
            "Unverified donor policy: %s",
            DONOR_INCLUDE_UNVERIFIED,
        )

    def discover_sources(self) -> list[SourceCandidate]:
        """Discover public and configured playlist sources."""
        sources = self.discovery.discover()
        self.store.save_sources(sources)
        self.summary["sources_discovered"] = len(sources)
        return sources

    def fetch_and_parse_sources(
        self,
        sources: list[SourceCandidate],
    ) -> tuple[list[SourceFetchResult], list[PlaylistRecord]]:
        """Fetch initial and nested playlists with bounded concurrency."""
        initial_results = self.fetcher.fetch_all(sources)
        seen = {normalize_url(source.url) for source in sources}

        nested_results: list[SourceFetchResult] = []
        if MAX_PLAYLIST_DEPTH > 0 and NESTED_PLAYLISTS_ENABLED:
            nested_results = self.fetcher.fetch_nested(initial_results, seen)

        all_results = initial_results + nested_results
        all_records: list[PlaylistRecord] = []

        for result in all_results:
            all_records.extend(result.records)

        self.summary["sources_fetched"] = len(all_results)
        self.summary["sources_parsed"] = sum(
            1 for result in all_results if result.status == "PARSED"
        )
        self.summary["source_failures"] = sum(
            1
            for result in all_results
            if result.status in {"FETCH_FAILED", "WORKER_ERROR"}
        )
        self.summary["records_parsed_before_dedup"] = len(all_records)

        return all_results, all_records

    def check_streams(
        self,
        records: list[PlaylistRecord],
    ) -> list[PlaylistRecord]:
        """Check all unique stream URLs using the dedicated stream pool."""
        if self.stream_checker is None:
            self.stream_checker = StreamChecker(
                self.http,
                self.ffprobe_path,
                self.store,
            )

        unique_records = deduplicate_records(records)
        checked = self.stream_checker.check_all(unique_records)
        self.store.save_records(checked)

        self.summary["records_checked"] = len(checked)
        self.summary["stream_status_counts"] = dict(
            Counter(record.stream_status for record in checked)
        )
        return checked

    def write_outputs(
        self,
        checked_records: list[PlaylistRecord],
    ) -> tuple[list[PlaylistRecord], dict[str, Any]]:
        """Write the donor playlist, JSON reports, and SKALA audit."""
        donor_records, donor_filter_counts = select_donor_records(checked_records)

        if DONOR_REJECT_EMPTY and len(donor_records) < DONOR_MIN_CHANNELS:
            raise RuntimeError(
                "Donor rejected: accepted channel count is below DONOR_MIN_CHANNELS"
            )

        donor_content = render_m3u(donor_records)
        if DONOR_REJECT_EMPTY and not donor_records:
            raise RuntimeError("Donor rejected: no accepted records")

        previous_records = self.skala.read_previous()

        # Archive the previous current donor before replacement.
        archived_current = self.skala.archive_current()

        atomic_write_text(DONOR_PATH, donor_content)

        # Compare against the archive snapshot if present; otherwise compare
        # against the previous on-disk donor when it existed before this run.
        if previous_records:
            before_records = previous_records
        else:
            # First archive may not exist. A separate before snapshot is kept
            # from the previous file only when available prior to this write.
            before_records = []

        skala_report = self.skala.compare(before_records, donor_records)
        skala_report["archive_created"] = archived_current
        skala_report["donor_path"] = str(DONOR_PATH)
        skala_report["archive_path"] = str(SKALA_ARCHIVE_PATH)
        skala_report["filter_counts"] = donor_filter_counts
        skala_report_path = self.skala.write_report(skala_report)

        atomic_write_json(
            RESULTS_JSON_PATH,
            {
                "run_id": self.run_id,
                "generated_at": utc_now(),
                "donor_path": str(DONOR_PATH),
                "donor_count": len(donor_records),
                "donor_filter_counts": donor_filter_counts,
                "records": [record.to_dict() for record in checked_records],
                "donor_records": [record.to_dict() for record in donor_records],
            },
        )

        receiver_path = ReceiverBridge(
            RECEIVER_OUTBOX,
            RECEIVER_SOURCE_URL,
        ).create_payload(
            self.run_id,
            donor_records,
            RESULTS_JSON_PATH,
        )

        self.summary["donor_count"] = len(donor_records)
        self.summary["donor_filter_counts"] = donor_filter_counts
        self.summary["skala_report_path"] = str(skala_report_path)
        self.summary["results_json_path"] = str(RESULTS_JSON_PATH)
        self.summary["receiver_outbox_path"] = str(receiver_path)

        return donor_records, skala_report

    def write_diagnostics(
        self,
        source_results: list[SourceFetchResult],
        checked_records: list[PlaylistRecord],
    ) -> None:
        """Write detailed diagnostic JSON for the current run."""
        diagnostics = {
            "run_id": self.run_id,
            "generated_at": utc_now(),
            "configuration": {
                "search_workers": SEARCH_WORKERS,
                "parsing_workers": PARSING_WORKERS,
                "stream_check_workers": STREAM_CHECK_WORKERS,
                "http_max_connections": HTTP_MAX_CONNECTIONS,
                "http_timeout_seconds": HTTP_TIMEOUT_SECONDS,
                "http_retries": HTTP_RETRIES,
                "max_response_bytes": HTTP_MAX_BYTES,
                "ffprobe_path": self.ffprobe_path,
                "ffprobe_enabled": STREAM_CHECK_FFPROBE,
                "allow_private_networks": ALLOW_PRIVATE_NETWORKS,
                "donor_include_unverified": DONOR_INCLUDE_UNVERIFIED,
            },
            "sources": [
                {
                    "source": item.source.to_dict(),
                    "status": item.status,
                    "http_status": item.http_status,
                    "content_type": item.content_type,
                    "final_url": item.final_url,
                    "record_count": len(item.records),
                    "nested_url_count": len(item.nested_urls),
                    "error": item.error,
                    "elapsed_seconds": item.elapsed_seconds,
                }
                for item in source_results
            ],
            "stream_status_counts": dict(
                Counter(record.stream_status for record in checked_records)
            ),
            "records": [record.to_dict() for record in checked_records],
            "summary": self.summary,
        }

        atomic_write_json(DIAGNOSTICS_JSON_PATH, diagnostics)

    def run(self) -> dict[str, Any]:
        """Execute the complete pipeline and persist run status."""
        self._log_configuration()
        self.store.start_run(self.run_id)

        final_status = "FAILED"
        error_message = ""

        try:
            self.ffprobe_path = ensure_ffprobe()
            self.summary["ffprobe_path"] = self.ffprobe_path or ""
            self.stream_checker = StreamChecker(
                self.http,
                self.ffprobe_path,
                self.store,
            )

            sources = self.discover_sources()
            if not sources:
                raise RuntimeError(
                    "No source candidates discovered. Configure EXTRA_SOURCE_URLS "
                    "or verify the discovery providers."
                )

            source_results, parsed_records = self.fetch_and_parse_sources(sources)
            self.summary["records_parsed"] = len(parsed_records)

            deduplicated = deduplicate_records(parsed_records)
            self.summary["records_after_dedup"] = len(deduplicated)

            if not deduplicated:
                raise RuntimeError(
                    "No valid playlist entries were parsed; existing donor was preserved."
                )

            checked_records = self.check_streams(deduplicated)
            donor_records, skala_report = self.write_outputs(checked_records)

            self.summary["skala"] = {
                "before_count": skala_report.get("before_count", 0),
                "added_count": skala_report.get("added_count", 0),
                "removed_count": skala_report.get("removed_count", 0),
                "changed_count": skala_report.get("changed_count", 0),
                "after_count": skala_report.get("after_count", 0),
            }

            self.write_diagnostics(source_results, checked_records)
            final_status = "SUCCESS"

        except Exception as exc:
            error_message = f"{type(exc).__name__}: {exc}"
            self.summary["error"] = error_message
            log_event(logging.ERROR, "Pipeline failed: %s", error_message)

            # Do not delete or overwrite the existing donor on a pipeline error.
            try:
                atomic_write_json(
                    DIAGNOSTICS_JSON_PATH,
                    {
                        "run_id": self.run_id,
                        "generated_at": utc_now(),
                        "status": "FAILED",
                        "error": error_message,
                        "summary": self.summary,
                    },
                )
            except Exception as diagnostics_exc:
                log_event(
                    logging.ERROR,
                    "Failed to write failure diagnostics: %s",
                    diagnostics_exc,
                )

        finally:
            self.summary["finished_at"] = utc_now()
            self.summary["status"] = final_status
            self.summary["error"] = error_message

            try:
                self.store.finish_run(
                    self.run_id,
                    final_status,
                    self.summary,
                )
            except Exception as store_exc:
                log_event(
                    logging.ERROR,
                    "Failed to persist run status: %s",
                    store_exc,
                )

            log_event(
                logging.INFO,
                "Pipeline finished: status=%s run_id=%s",
                final_status,
                self.run_id,
            )

        if final_status != "SUCCESS":
            raise RuntimeError(error_message or "Pipeline failed")

        return self.summary


# ============================================================================
# 14. ENTRY POINT
# ============================================================================

def log_concurrency_configuration() -> None:
    """Print effective worker pool sizes."""
    log_event(
        logging.INFO,
        "Effective concurrency: discovery=%d parsing=%d stream_checks=%d",
        SEARCH_WORKERS,
        PARSING_WORKERS,
        STREAM_CHECK_WORKERS,
    )


def main() -> int:
    """Application entry point."""
    log_concurrency_configuration()

    try:
        hunter = UnifiedPlaylistHunter()
        summary = hunter.run()
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        log_event(logging.WARNING, "Interrupted by user")
        return 130
    except Exception as exc:
        log_event(logging.ERROR, "Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
