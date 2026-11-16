#!/usr/bin/env python3
# coding: utf-8
"""
==============================================================================
Project:         findmydevice364-hub_lab
Owner:           #Phoenix89S_Lab©
Public Name:     Phoenix Hub "Baryshnikov"

Version:         3.8.0-alpha.1
VersionCode:     31.8.0.1
Build ID:        P3100-A1-20261116
Release channel: ALPHA
Revision:        16-SOURCE-DREG-DUAL

File:            git_bot_for_stable.py

16-source IPTV collector, validation pipeline, RTRS normalizer,
Chebur catch-up preservation, Disney HLS handling and DREG dual logging.

DREG:
    DREG_SKALA_4_0
    DREG_TITAN_2_8
    DREG_COMMON

POLICY:
- Ordinary donors are not globally deduplicated.
- HVK is a special unique-URL fallback donor.
- Only explicit 18+ blacklist filtering is applied.
- Entry-level M3U directives are preserved.
- HLS media segments are not interpreted as TV channels.
- Inconclusive stream checks are retained.
- Network validation is a point-in-time check, not a guarantee.
==============================================================================
"""

import os
import re
import sys
import json
import time
import uuid
import shutil
import tarfile
import platform
import tempfile
import threading
import subprocess
import urllib.request
import urllib.error

from urllib.parse import urlsplit, urlunsplit
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed


# =============================================================================
# BUILD METADATA
# =============================================================================

VERSION = "3.8.0-alpha.1"
VERSION_CODE = "31.8.0.1"
BUILD_ID = "P3100-A1-20261116"
RELEASE_CHANNEL = "ALPHA"
REVISION = "16-SOURCE-DREG-DUAL"

PROJECT_NAME = 'Phoenix Hub "Baryshnikov"'
SCRIPT_NAME = "git_bot_for_stable.py"


# =============================================================================
# DONOR URLS — 16 SOURCES
# =============================================================================

SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/IpTV_playlist_2026Ru/main/ngenix_found_1.m3u"
)
SECOND_SOURCE_URL = (
    "https://raw.githubusercontent.com/Free-TV/IPTV/"
    "refs/heads/master/playlists/playlist_russia.m3u8"
)
THIRD_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/Mir-IpTV-Ru-/main/Super_RTRS_2026.m3u"
)
TEST_CHANNELS_URL = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/IpTV_playlist_2026Ru/gh-pages/test_channels.m3u"
)
FIFTH_SOURCE_URL = "https://iptv.org.ua/iptv/avto-full.m3u"
SIXTH_SOURCE_URL = "https://smart-iptv.ru/russia.m3u"
SEVENTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "Dmitryi55/ruIPTV/main/IPTVru.m3u"
)
EIGHTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/zala_N.m3u"
)
NINTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/find_output/04_final/"
    "AGGREGATED_MASTER.m3u"
)

CHEBUR_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "spartvan/iptvrus/dh-pages/Чебур.NET_2.m3u"
)
ELEVENTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "vvv1negret/iptvru/main/IPTVru.m3u"
)
TWELFTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "artem-art998/IPTVru/main/iptv126.m3u"
)
DISNEY_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "kanaldisneyplus/Disney_iptvRU/main/hls/playlist.m3u8"
)
HVK_CONTROL_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/hvk.m3u"
)
CHANNELS_INVENTORY_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/find_output/03_channels/"
    "channels_inventory.m3u"
)
ALISKA_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/Aliska.m3u"
)


# Environment overrides for every donor.
for _source_name in (
    "SOURCE_URL",
    "SECOND_SOURCE_URL",
    "THIRD_SOURCE_URL",
    "TEST_CHANNELS_URL",
    "FIFTH_SOURCE_URL",
    "SIXTH_SOURCE_URL",
    "SEVENTH_SOURCE_URL",
    "EIGHTH_SOURCE_URL",
    "NINTH_SOURCE_URL",
    "CHEBUR_SOURCE_URL",
    "ELEVENTH_SOURCE_URL",
    "TWELFTH_SOURCE_URL",
    "DISNEY_SOURCE_URL",
    "HVK_CONTROL_SOURCE_URL",
    "CHANNELS_INVENTORY_SOURCE_URL",
    "ALISKA_SOURCE_URL",
):
    globals()[_source_name] = os.environ.get(
        _source_name, globals()[_source_name]
    )


# =============================================================================
# RUNTIME CONFIGURATION
# =============================================================================

SKIP_PUSH = os.environ.get(
    "SKIP_PUSH", "true"
).strip().lower() in ("1", "true", "yes", "on")

TARGET_PUSH_BRANCH = os.environ.get(
    "TARGET_PUSH_BRANCH", "gh-pages-2"
).strip()

DREG_INCLUDE_IN_GIT = os.environ.get(
    "DREG_INCLUDE_IN_GIT", "true"
).strip().lower() in ("1", "true", "yes", "on")

MAX_SOURCE_WORKERS = int(os.environ.get("MAX_SOURCE_WORKERS", "15"))
MAX_STREAM_WORKERS = int(os.environ.get("MAX_STREAM_WORKERS", "12"))
MAX_RTRS_WORKERS = int(os.environ.get("MAX_RTRS_WORKERS", "8"))

FETCH_TIMEOUT = int(os.environ.get("FETCH_TIMEOUT", "45"))
FETCH_ATTEMPTS = int(os.environ.get("FETCH_ATTEMPTS", "5"))
STREAM_TIMEOUT = int(os.environ.get("STREAM_TIMEOUT", "12"))
FFPROBE_TIMEOUT = int(os.environ.get("FFPROBE_TIMEOUT", "20"))


# =============================================================================
# OUTPUT FILES
# =============================================================================

TARGET_MIR = "IPTVmir.m3u8"
TARGET_STABLE_LEGACY = "legacy/IPTVstable.m3u8"
TARGET_STABLE = "stable.m3u"
TARGET_RTRS_STABLE = "RTRS_stable.m3u8"

LOG_DIR = "logs"
LOG_COMMON = os.path.join(LOG_DIR, "DREG_COMMON.txt")
LOG_SKALA = os.path.join(LOG_DIR, "DREG_SKALA_4_0.txt")
LOG_TITAN = os.path.join(LOG_DIR, "DREG_TITAN_2_8.txt")


# =============================================================================
# 18+ BLACKLIST — NO OTHER CONTENT FILTERS
# =============================================================================

BLACKLIST = {
    "playboy",
    "erox",
    "blue hustler",
    "barely legal",
    "adult",
    "private",
    "penthouse",
    "senstv",
    "o-la-la",
    "xxx",
    "18+",
    "porn",
    "hustler",
}


# =============================================================================
# RTRS CHANNEL MAP — SD/HD + ALIASES
# =============================================================================

_RTRS_NAMES = [
    "Первый канал",
    "Россия 1",
    "Матч ТВ",
    "НТВ",
    "Пятый канал",
    "Россия К",
    "Россия 24",
    "Карусель",
    "ОТР",
    "ТВ Центр",
    "РЕН ТВ",
    "Спас",
    "СТС",
    "Домашний",
    "ТВ-3",
    "Пятница!",
    "Звезда",
    "Мир",
    "ТНТ",
    "Муз-ТВ",
]

RTRS_CHANNELS_MAP = {}

for _index, _channel_name in enumerate(_RTRS_NAMES, 1):
    _key = _channel_name.casefold().replace("ё", "е")
    RTRS_CHANNELS_MAP[_key] = (
        "РТРС SD",
        f"{_index}.SD {_channel_name}",
        _index,
    )
    RTRS_CHANNELS_MAP[_key + " hd"] = (
        "РТРС HD",
        f"{_index}.HD {_channel_name} HD",
        100 + _index,
    )

RTRS_CHANNELS_MAP.update({
    "твц": ("РТРС SD", "10.SD ТВ Центр", 10),
    "твц hd": ("РТРС HD", "10.HD ТВ Центр HD", 110),
    "тв3": ("РТРС SD", "15.SD ТВ-3", 15),
    "тв 3": ("РТРС SD", "15.SD ТВ-3", 15),
    "тв3 hd": ("РТРС HD", "15.HD ТВ-3 HD", 115),
    "тв 3 hd": ("РТРС HD", "15.HD ТВ-3 HD", 115),
})


# =============================================================================
# THEMATIC CLASSIFICATION
# =============================================================================

CATEGORY_RULES = [
    ("Новости", [
        "новост", "news", "россия 24", "рбк", "rbc",
        "известия", "жизнь", "москва 24", "миротворец",
        "rt", "russia today", "bbc", "cnn", "euronews",
        "tv rain", "дождь", "current time", "наст. время",
        "360", "life", "life news",
    ]),
    ("Музыка", [
        "муз", "music", "радио", "radio", "hit", "mtv",
        "vh1", "jazz", "rock", "dance", "hip-hop",
        "hiphop", "shanson", "шансон", "europa plus",
        "love radio", "bridge", "bridge tv", "ru.tv",
        "music box",
    ]),
    ("Детские", [
        "дет", "kids", "cartoon", "мульт", "карусель",
        "disney", "nickelodeon", "baby", "детский",
        "мультик", "cartoon network", "nick jr",
        "мультиландия",
    ]),
    ("Кино", [
        "кино", "movie", "film", "cinema", "фильм",
        "кинохит", "кинопремьера", "premiere",
        "киносемья", "kino", "fox", "hbo", "amc",
        "sony", "кинопоказ", "кинокомедия",
        "индийское кино", "наше кино",
    ]),
    ("Еда", [
        "еда", "food", "кулинар", "кухня", "кухон",
        "рецепт", "рецепты", "cooking", "cook",
        "chef", "gastro", "гастроном", "food network",
        "food tv", "вкусно", "готовим", "готовка",
        "телекафе", "кухня тв", "кулинария",
    ]),
    ("Познавательные", [
        "познават", "discovery", "national geographic",
        "nat geo", "history", "animal", "наука", "doc",
        "докум", "viasat", "viasat history",
        "viasat nature", "animal planet",
        "discovery science",
    ]),
    ("Развлекательные", [
        "развлек", "comedy", "юмор", "тв3", "пятница",
        "тнт", "ctc", "стс", "юморист", "квн",
        "comedy club", "тнт4", "2x2",
    ]),
    ("Научные", [
        "науч", "science", "cosmos", "space", "tech",
        "техно", "наука 2.0", "science 2.0",
        "galaxy", "cosmos tv",
    ]),
]

CATEGORY_ORDER = [
    "Новости",
    "Музыка",
    "Детские",
    "Кино",
    "Еда",
    "Познавательные",
    "Развлекательные",
    "Научные",
    "Прочее",
]


# =============================================================================
# DREG — DUAL LOGGING + COMMON CHRONOLOGY
# =============================================================================

class DREGManager:
    """
    Thread-safe append-only logger.

    Every event receives one shared RUN_ID and monotonically increasing SEQ.
    SKALA/TITAN records are written byte-for-byte identically to their own
    journal and the common journal.
    """

    def __init__(self):
        self.run_id = uuid.uuid4().hex[:12].upper()
        self.seq = 0
        self.lock = threading.RLock()
        self.handles = {}
        self.enabled = True
        self._open_logs()

    def _open_logs(self):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            for path in (LOG_COMMON, LOG_SKALA, LOG_TITAN):
                self.handles[path] = open(
                    path, "a", encoding="utf-8", buffering=1
                )
        except Exception:
            self.enabled = False
            self._safe_close_handles()

    def _safe_close_handles(self):
        for handle in list(self.handles.values()):
            try:
                handle.close()
            except Exception:
                pass
        self.handles.clear()

    def _write_path(self, path, serialized_record):
        handle = self.handles.get(path)
        if not handle:
            return
        try:
            handle.write(serialized_record + "\n")
            handle.flush()
        except Exception:
            # Logging must never interrupt the IPTV pipeline.
            pass

    def log(self, system, event, level="INFO", **fields):
        if not self.enabled:
            return

        with self.lock:
            self.seq += 1
            record = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "run_id": self.run_id,
                "seq": self.seq,
                "system": system,
                "level": level,
                "event": event,
                "fields": fields,
            }

            serialized = json.dumps(
                record, ensure_ascii=False, default=str
            )

            if system == "DREG_SKALA_4_0":
                self._write_path(LOG_SKALA, serialized)
                self._write_path(LOG_COMMON, serialized)
            elif system == "DREG_TITAN_2_8":
                self._write_path(LOG_TITAN, serialized)
                self._write_path(LOG_COMMON, serialized)
            else:
                self._write_path(LOG_COMMON, serialized)

    def console(self, stream_name, value):
        if not value:
            return
        self.log(
            "DREG_COMMON",
            "CONSOLE",
            stream=stream_name,
            text=value.rstrip("\n"),
        )

    def flush(self):
        with self.lock:
            for handle in self.handles.values():
                try:
                    handle.flush()
                except Exception:
                    pass

    def close(self):
        with self.lock:
            for handle in self.handles.values():
                try:
                    handle.flush()
                    handle.close()
                except Exception:
                    pass
            self.handles.clear()


class DREGConsoleTee:
    """Mirror stdout/stderr to the original console and DREG_COMMON."""

    def __init__(self, original, manager, stream_name):
        self.original = original
        self.manager = manager
        self.stream_name = stream_name
        self.buffer = ""

    def write(self, value):
        result = self.original.write(value)

        try:
            self.buffer += value
            while "\n" in self.buffer:
                line, self.buffer = self.buffer.split("\n", 1)
                self.manager.console(self.stream_name, line)
        except Exception:
            pass

        return result

    def flush(self):
        try:
            self.original.flush()
        except Exception:
            pass

        if self.buffer:
            try:
                self.manager.console(self.stream_name, self.buffer)
            except Exception:
                pass
            self.buffer = ""

    def isatty(self):
        return (
            self.original.isatty()
            if hasattr(self.original, "isatty")
            else False
        )

    def fileno(self):
        return self.original.fileno()

    @property
    def encoding(self):
        return getattr(self.original, "encoding", "utf-8")

    def writable(self):
        return True


DREG = None
_ORIGINAL_STDOUT = sys.stdout
_ORIGINAL_STDERR = sys.stderr


def start_dreg():
    global DREG

    if DREG is not None:
        return

    DREG = DREGManager()

    if DREG.enabled:
        sys.stdout = DREGConsoleTee(
            _ORIGINAL_STDOUT, DREG, "stdout"
        )
        sys.stderr = DREGConsoleTee(
            _ORIGINAL_STDERR, DREG, "stderr"
        )

    DREG.log(
        "DREG_COMMON",
        "RUN_START",
        version=VERSION,
        version_code=VERSION_CODE,
        build_id=BUILD_ID,
        release_channel=RELEASE_CHANNEL,
        revision=REVISION,
        run_id=DREG.run_id,
        skip_push=SKIP_PUSH,
        target_branch=TARGET_PUSH_BRANCH,
        python=platform.python_version(),
        platform=platform.platform(),
        source_count=16,
    )

    DREG.log(
        "DREG_SKALA_4_0",
        "SKALA_INITIALIZED",
        description="Operational event chronology initialized",
    )

    DREG.log(
        "DREG_TITAN_2_8",
        "TITAN_INITIALIZED",
        description="Extended diagnostic journal initialized",
    )


def dlog(system, event, level="INFO", **fields):
    try:
        if DREG is not None:
            DREG.log(system, event, level, **fields)
    except Exception:
        pass


def stop_dreg_for_git():
    """Flush and close journals before Git stages their final snapshot."""
    global DREG

    if DREG is None:
        return

    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass

    sys.stdout = _ORIGINAL_STDOUT
    sys.stderr = _ORIGINAL_STDERR

    try:
        DREG.flush()
        DREG.close()
    except Exception:
        pass


# =============================================================================
# SHARED STREAM CHECK CACHE
# =============================================================================

_STREAM_CHECK_CACHE = {}
_STREAM_CHECK_IN_PROGRESS = {}
_STREAM_CHECK_LOCK = threading.Lock()


# =============================================================================
# GIT UTILITIES
# =============================================================================

def run(cmd, cwd=None, check=True):
    try:
        return subprocess.run(
            cmd,
            cwd=cwd,
            check=check,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        print(f"Команда {' '.join(cmd)} завершилась с ошибкой: {exc}")
        print(f"stderr:\n{exc.stderr}")

        dlog(
            "DREG_SKALA_4_0",
            "COMMAND_FAILED",
            "ERROR",
            command=cmd,
            stderr=exc.stderr,
            returncode=exc.returncode,
        )
        raise


def git_is_clean():
    result = run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        check=True,
    )

    remaining = []

    for line in result.stdout.splitlines():
        path = line[3:].replace("\\", "/") if len(line) >= 4 else ""

        # Append-only DREG logs must not prevent the next run.
        if path.startswith("logs/"):
            continue

        remaining.append(line)

    return not remaining


def ensure_on_branch(branch=TARGET_PUSH_BRANCH):
    if not branch or branch.startswith("-"):
        raise ValueError(f"Некорректное имя ветки: {branch!r}")

    run(["git", "fetch", "origin", branch])

    local_exists = subprocess.run(
        ["git", "show-ref", "--verify", f"refs/heads/{branch}"],
        capture_output=True,
        text=True,
    ).returncode == 0

    if local_exists:
        run(["git", "checkout", branch])
    else:
        run(["git", "checkout", "-b", branch, f"origin/{branch}"])

    run(["git", "pull", "--ff-only", "origin", branch])


# =============================================================================
# NETWORK FETCH
# =============================================================================

def _decode_response(raw_data):
    for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            return raw_data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_data.decode("utf-8", errors="replace")


def fetch_url(url, timeout=FETCH_TIMEOUT, attempts=FETCH_ATTEMPTS):
    if not url:
        return ""

    last_error = None

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PhoenixBaryshnikov/3.8)",
        "Accept": "*/*",
        "Cache-Control": "no-cache",
    }

    for attempt in range(1, attempts + 1):
        started = time.monotonic()

        dlog(
            "DREG_TITAN_2_8",
            "FETCH_ATTEMPT",
            url=url,
            attempt=attempt,
            max_attempts=attempts,
        )

        try:
            request = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(
                request, timeout=timeout
            ) as response:
                raw_data = response.read()
                content_length = response.headers.get("Content-Length")
                status = getattr(response, "status", None)

            if not raw_data:
                raise ValueError("Источник вернул пустое тело")

            if content_length:
                try:
                    expected = int(content_length)
                    if len(raw_data) != expected:
                        raise ValueError(
                            f"Неполная загрузка: "
                            f"{len(raw_data)}/{expected}"
                        )
                except ValueError:
                    raise

            decoded = _decode_response(raw_data)

            dlog(
                "DREG_SKALA_4_0",
                "DONOR_FETCH_SUCCESS",
                url=url,
                bytes=len(raw_data),
                status=status,
                elapsed=round(time.monotonic() - started, 3),
            )

            return decoded

        except Exception as exc:
            last_error = str(exc)

            dlog(
                "DREG_TITAN_2_8",
                "FETCH_RETRY",
                "WARNING",
                url=url,
                attempt=attempt,
                error=last_error,
            )

            print(
                f"Ошибка загрузки (попытка {attempt}/{attempts}): "
                f"{url}: {exc}"
            )

            if attempt < attempts:
                time.sleep(min(3 * attempt, 15))

    dlog(
        "DREG_SKALA_4_0",
        "DONOR_FETCH_FAILED",
        "ERROR",
        url=url,
        attempts=attempts,
        error=last_error,
    )

    print(f"Донор не загружен после {attempts} попыток: {url}")
    return ""


# =============================================================================
# HTTP VALIDATION — HEAD -> GET
# =============================================================================

def check_stream_alive_simple(url, timeout=STREAM_TIMEOUT):
    """
    Returns:
        True  — HTTP endpoint returned readable media data.
        False — both HTTP probes definitively returned 404/410.
        None  — inconclusive; do not reject the candidate.
    """

    if not url or not url.lower().startswith(("http://", "https://")):
        return None

    headers = {
        "User-Agent": "VLC/3.0.16 LibVLC/3.0.16",
        "Accept": "*/*",
    }

    definitive_statuses = []
    successful_http = False

    # HEAD probe.
    try:
        request = urllib.request.Request(
            url,
            headers=headers,
            method="HEAD",
        )

        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = getattr(response, "status", 200)

            if 200 <= status < 400:
                successful_http = True

            dlog(
                "DREG_TITAN_2_8",
                "HTTP_HEAD_RESULT",
                url=url,
                status=status,
            )

    except urllib.error.HTTPError as exc:
        if exc.code in (404, 410):
            definitive_statuses.append(exc.code)

        dlog(
            "DREG_TITAN_2_8",
            "HTTP_HEAD_ERROR",
            "WARNING",
            url=url,
            status=exc.code,
        )

    except Exception as exc:
        dlog(
            "DREG_TITAN_2_8",
            "HTTP_HEAD_INCONCLUSIVE",
            "WARNING",
            url=url,
            error=str(exc),
        )

    # GET range probe. HEAD failure is not enough to reject a stream.
    try:
        get_headers = dict(headers)
        get_headers["Range"] = "bytes=0-2047"

        request = urllib.request.Request(
            url,
            headers=get_headers,
            method="GET",
        )

        with urllib.request.urlopen(request, timeout=timeout) as response:
            chunk = response.read(2048)
            status = getattr(response, "status", 200)

            if 200 <= status < 400 and chunk:
                successful_http = True

                dlog(
                    "DREG_TITAN_2_8",
                    "HTTP_GET_OK",
                    url=url,
                    status=status,
                    bytes_read=len(chunk),
                )

    except urllib.error.HTTPError as exc:
        if exc.code in (404, 410):
            definitive_statuses.append(exc.code)

        dlog(
            "DREG_TITAN_2_8",
            "HTTP_GET_ERROR",
            "WARNING",
            url=url,
            status=exc.code,
        )

    except Exception as exc:
        dlog(
            "DREG_TITAN_2_8",
            "HTTP_GET_INCONCLUSIVE",
            "WARNING",
            url=url,
            error=str(exc),
        )

    if successful_http:
        return True

    # Only two independent definitive 404/410 responses qualify
    # as an HTTP-level dead endpoint candidate.
    if (
        len(definitive_statuses) >= 2
        and all(code in (404, 410) for code in definitive_statuses)
    ):
        return False

    return None


# =============================================================================
# FFPROBE BOOTSTRAP
# =============================================================================

def ensure_ffprobe():
    existing = shutil.which("ffprobe")

    if existing:
        dlog(
            "DREG_SKALA_4_0",
            "FFPROBE_FOUND",
            path=existing,
            bootstrap=False,
        )
        print(f"Найден системный ffprobe: {existing}")
        return True

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system != "linux" or machine not in ("x86_64", "amd64"):
        print(
            "Автозагрузка ffprobe поддерживается только Linux x86_64. "
            f"Текущая платформа: {system}/{machine}"
        )

        dlog(
            "DREG_SKALA_4_0",
            "FFPROBE_UNAVAILABLE",
            "ERROR",
            platform=f"{system}/{machine}",
        )
        return False

    cache_dir = os.path.join(
        os.path.expanduser("~"),
        ".cache",
        "iptv_bot_bin",
    )
    os.makedirs(cache_dir, exist_ok=True)

    target = os.path.join(cache_dir, "ffprobe")

    def activate(path):
        try:
            os.chmod(path, 0o755)

            result = subprocess.run(
                [path, "-version"],
                capture_output=True,
                text=True,
                timeout=8,
            )

            if result.returncode:
                return False

            os.environ["PATH"] = (
                cache_dir
                + os.pathsep
                + os.environ.get("PATH", "")
            )

            return True

        except Exception:
            return False

    if os.path.isfile(target) and activate(target):
        dlog(
            "DREG_SKALA_4_0",
            "FFPROBE_CACHE_READY",
            path=target,
        )
        return True

    try:
        if os.path.exists(target):
            os.remove(target)
    except OSError:
        pass

    download_url = (
        "https://johnvansickle.com/ffmpeg/releases/"
        "ffmpeg-release-amd64-static.tar.xz"
    )

    archive_path = None
    binary_path = None

    try:
        fd, archive_path = tempfile.mkstemp(
            prefix="phoenix-ffmpeg-",
            suffix=".tar.xz",
        )
        os.close(fd)

        request = urllib.request.Request(
            download_url,
            headers={
                "User-Agent": "PhoenixBaryshnikovBootstrap/3.8"
            },
        )

        with urllib.request.urlopen(request, timeout=60) as response:
            with open(archive_path, "wb") as output:
                shutil.copyfileobj(response, output)

        with tarfile.open(archive_path, mode="r:xz") as archive:
            member = next(
                (
                    item for item in archive.getmembers()
                    if item.isfile()
                    and item.name.rstrip("/").endswith("/ffprobe")
                ),
                None,
            )

            if member is None:
                raise RuntimeError("В архиве не найден ffprobe")

            source = archive.extractfile(member)

            if source is None:
                raise RuntimeError("Не удалось прочитать ffprobe")

            fd, binary_path = tempfile.mkstemp(
                prefix="ffprobe-",
                dir=cache_dir,
            )

            with os.fdopen(fd, "wb") as output, source:
                shutil.copyfileobj(source, output)

        os.chmod(binary_path, 0o755)

        check = subprocess.run(
            [binary_path, "-version"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if check.returncode:
            raise RuntimeError("Скачанный ffprobe не запускается")

        os.replace(binary_path, target)
        binary_path = None

        os.environ["PATH"] = (
            cache_dir
            + os.pathsep
            + os.environ.get("PATH", "")
        )

        dlog(
            "DREG_SKALA_4_0",
            "FFPROBE_BOOTSTRAP_SUCCESS",
            path=target,
        )

        return True

    except Exception as exc:
        dlog(
            "DREG_SKALA_4_0",
            "FFPROBE_BOOTSTRAP_FAILED",
            "ERROR",
            error=str(exc),
        )

        print(f"Не удалось подготовить ffprobe: {exc}")
        return False

    finally:
        for path in (archive_path, binary_path):
            if path:
                try:
                    os.remove(path)
                except OSError:
                    pass


# =============================================================================
# FFPROBE VALIDATION — TRI-STATE
# =============================================================================

def check_stream_quality_ffprobe(url, timeout=FFPROBE_TIMEOUT):
    """
    Returns:
        True  — ffprobe successfully found audio/video.
        False — ffprobe successfully parsed the resource but found no
                audio/video streams.
        None  — probe failed, timed out or was inconclusive.
    """

    if not url or not url.lower().startswith(("http://", "https://")):
        return None

    if not shutil.which("ffprobe"):
        return None

    command = [
        "ffprobe",
        "-v", "error",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        "-rw_timeout", str(int(timeout * 1_000_000)),
        url,
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout + 2,
        )

        # A non-zero exit can mean authorization, network, TLS,
        # throttling or unsupported transport. It is not proof of death.
        if result.returncode:
            dlog(
                "DREG_TITAN_2_8",
                "FFPROBE_INCONCLUSIVE",
                "WARNING",
                url=url,
                returncode=result.returncode,
                stderr=(result.stderr or "")[:500],
            )
            return None

        data = json.loads(result.stdout or "{}")
        streams = data.get("streams", [])

        valid = any(
            stream.get("codec_type") in ("audio", "video")
            for stream in streams
        )

        dlog(
            "DREG_TITAN_2_8",
            "FFPROBE_RESULT",
            url=url,
            valid=valid,
            stream_count=len(streams),
        )

        return valid

    except subprocess.TimeoutExpired:
        dlog(
            "DREG_TITAN_2_8",
            "FFPROBE_TIMEOUT",
            "WARNING",
            url=url,
            timeout=timeout,
        )
        return None

    except Exception as exc:
        dlog(
            "DREG_TITAN_2_8",
            "FFPROBE_ERROR",
            "WARNING",
            url=url,
            error=str(exc),
        )
        return None


# =============================================================================
# STREAM VALIDATION — ONE CHECK PER URL PER RUN
# =============================================================================

def check_stream_all(url):
    """
    Single validation per URL per process run.

    Conservative policy:
    - Positive HTTP or ffprobe result: retain.
    - Inconclusive result: retain.
    - Reject only when HTTP definitively reports 404/410 and ffprobe
      successfully confirms absence of audio/video streams.
    """

    if not url or not url.lower().startswith(("http://", "https://")):
        return False

    with _STREAM_CHECK_LOCK:
        if url in _STREAM_CHECK_CACHE:
            result = _STREAM_CHECK_CACHE[url]

            dlog(
                "DREG_TITAN_2_8",
                "STREAM_CACHE_HIT",
                url=url,
                result=result,
            )

            return result

        event = _STREAM_CHECK_IN_PROGRESS.get(url)

        if event is None:
            event = threading.Event()
            _STREAM_CHECK_IN_PROGRESS[url] = event
            owner = True

            dlog(
                "DREG_TITAN_2_8",
                "STREAM_CACHE_MISS",
                url=url,
            )
        else:
            owner = False

            dlog(
                "DREG_TITAN_2_8",
                "STREAM_WAIT_FOR_CHECK",
                url=url,
            )

    if not owner:
        event.wait()

        with _STREAM_CHECK_LOCK:
            result = _STREAM_CHECK_CACHE.get(url, True)

        dlog(
            "DREG_TITAN_2_8",
            "STREAM_WAIT_COMPLETE",
            url=url,
            result=result,
        )

        return result

    valid = True

    try:
        http_result = check_stream_alive_simple(url)

        if http_result is True:
            valid = True
            ffprobe_result = None

        else:
            ffprobe_result = check_stream_quality_ffprobe(url)

            if ffprobe_result is True:
                valid = True

            elif http_result is False and ffprobe_result is False:
                valid = False

            else:
                # Ambiguous HTTP or ffprobe result: preserve the candidate.
                valid = True

        dlog(
            "DREG_SKALA_4_0",
            "STREAM_VALIDATION_COMPLETE",
            url=url,
            http_result=http_result,
            ffprobe_result=ffprobe_result,
            valid=valid,
        )

        return valid

    except Exception as exc:
        valid = True

        dlog(
            "DREG_TITAN_2_8",
            "STREAM_VALIDATION_EXCEPTION",
            "WARNING",
            url=url,
            error=str(exc),
            policy="inconclusive_preserved",
        )

        return valid

    finally:
        # The waiter event is always released, even after an exception.
        with _STREAM_CHECK_LOCK:
            _STREAM_CHECK_CACHE[url] = valid

            completed = _STREAM_CHECK_IN_PROGRESS.pop(url, None)

            if completed is not None:
                completed.set()


# =============================================================================
# M3U PARSER — EXTINF + ENTRY DIRECTIVES + STANDALONE URLS
# =============================================================================

PRESERVED_DIRECTIVE_PREFIXES = (
    "#EXTVLCOPT:",
    "#EXTGRP:",
    "#KODIPROP:",
    "#EXTHTTP:",
    "#EXT-X-DEFINE:",
    "#EXT-X-SESSION-DATA:",
    "#EXT-X-SESSION-KEY:",
)


def _is_preserved_directive(line):
    upper = line.strip().upper()
    return upper.startswith(PRESERVED_DIRECTIVE_PREFIXES)


def has_digit_index_in_path(url):
    """Legacy helper retained from the original source."""
    try:
        path = urlsplit(url).path
    except Exception:
        path = url or ""

    return bool(re.search(r"(?:^|[/_.-])\d+(?:[/_.-]|$)", path))


def parse_m3u_content(content, source_name="unknown"):
    if not isinstance(content, str) or not content.strip():
        raise ValueError("Пустой источник")

    lines = content.splitlines()
    entries = []

    current_meta = None
    current_meta_line = None
    current_directives = []

    malformed = 0
    standalone = 0
    parsed = 0

    tvg_re = re.compile(
        r'\btvg-id\s*=\s*"([^"]*)"',
        re.IGNORECASE,
    )

    def save(meta, url, line_number, directives):
        nonlocal parsed, standalone

        url = (url or "").strip()

        if not url:
            return

        if meta:
            parts = meta.split(",", 1)
            name = parts[1].strip() if len(parts) > 1 else ""

            match = tvg_re.search(meta)

            key = (
                match.group(1).strip().casefold()
                if match and match.group(1).strip()
                else url.casefold()
            )
        else:
            name = ""
            key = url.casefold()
            standalone += 1

        entries.append({
            "extinf": meta or "",
            "url": url,
            "key": key,
            "name": name,
            "source_line": line_number,
            "source": source_name,
            "directives": list(directives or []),
        })

        parsed += 1

    for line_number, raw_line in enumerate(lines, 1):
        line = raw_line.strip()

        if not line:
            continue

        upper = line.upper()

        # HLS segment-duration EXTINF is not a TV channel record.
        if upper.startswith("#EXTINF:") and current_meta is None:
            if "#EXT-X-" in content.upper():
                # In an HLS manifest, EXTINF normally describes a segment.
                # A channel-style record is distinguished by attributes
                # such as tvg-id, tvg-name or group-title.
                if not re.search(
                    r'\b(?:tvg-id|tvg-name|group-title)\s*=',
                    line,
                    re.IGNORECASE,
                ):
                    continue

            current_meta = line
            current_meta_line = line_number
            current_directives = []
            continue

        if line.startswith("#"):
            if current_meta is not None and _is_preserved_directive(line):
                current_directives.append(line)
            continue

        save(
            current_meta,
            line,
            current_meta_line if current_meta else line_number,
            current_directives,
        )

        current_meta = None
        current_meta_line = None
        current_directives = []

    if current_meta is not None:
        malformed += 1

        dlog(
            "DREG_TITAN_2_8",
            "M3U_TRAILING_EXTINF_WITHOUT_URL",
            "WARNING",
            source=source_name,
            line=current_meta_line,
        )

    dlog(
        "DREG_SKALA_4_0",
        "M3U_PARSE_SUMMARY",
        source=source_name,
        lines=len(lines),
        parsed=parsed,
        standalone=standalone,
        malformed_extinf=malformed,
    )

    return entries


# =============================================================================
# DISNEY HLS HANDLER
# =============================================================================

def parse_disney_source(content, source_url):
    """
    Parse a channel-style playlist normally.

    If the file is an HLS media/master manifest rather than a channel list,
    retain its manifest URL as one logical Disney channel. Never convert
    individual .ts segments into channels.
    """

    upper = content.upper()

    hls_markers = (
        "#EXT-X-TARGETDURATION",
        "#EXT-X-MEDIA-SEQUENCE",
        "#EXT-X-STREAM-INF",
        "#EXT-X-PLAYLIST-TYPE",
        "#EXT-X-ENDLIST",
        "#EXT-X-I-FRAME-STREAM-INF",
    )

    is_hls_manifest = any(
        marker in upper for marker in hls_markers
    )

    has_channel_metadata = bool(re.search(
        r'#EXTINF:[^\r\n]*(?:tvg-id|tvg-name|group-title)\s*=',
        content,
        re.IGNORECASE,
    ))

    if is_hls_manifest and not has_channel_metadata:
        segment_count = sum(
            1 for line in content.splitlines()
            if line.strip()
            and not line.strip().startswith("#")
        )

        dlog(
            "DREG_TITAN_2_8",
            "DISNEY_HLS_MANIFEST_DETECTED",
            source_url=source_url,
            segment_lines=segment_count,
        )

        return [{
            "extinf": (
                '#EXTINF:-1 group-title="Детские",Disney IPTV RU'
            ),
            "url": source_url,
            "key": source_url.casefold(),
            "name": "Disney IPTV RU",
            "source_line": 1,
            "source": "DISNEY_SOURCE_URL",
            "directives": [],
            "hls_manifest": True,
        }]

    return parse_m3u_content(
        content,
        "DISNEY_SOURCE_URL",
    )


# =============================================================================
# TEXT NORMALIZATION / BLACKLIST / CATEGORIES / RTRS
# =============================================================================

def is_blacklisted(title):
    value = (title or "").strip().casefold()
    return any(word.casefold() in value for word in BLACKLIST)


def normalize_channel_text(value):
    clean = (
        (value or "")
        .casefold()
        .replace("ё", "е")
    )

    clean = re.sub(r"[‐-‒–—−_./\\:]+", " ", clean)
    clean = re.sub(r"[^\w\s]", " ", clean, flags=re.UNICODE)

    return re.sub(r"\s+", " ", clean).strip()


def rename_special_channel(name):
    name = (name or "").strip()

    if normalize_channel_text(name) == "scream hd":
        return "scream HD (Крик HD)"

    return name


def _contains_phrase(text, phrase):
    return bool(re.search(
        r"(?<!\w)" + re.escape(phrase) + r"(?!\w)",
        text,
        flags=re.UNICODE,
    ))


_NORMALIZED_RTRS_KEYS = [
    (normalize_channel_text(key), value, key)
    for key, value in RTRS_CHANNELS_MAP.items()
    if normalize_channel_text(key)
]


def match_rtrs(name):
    clean = normalize_channel_text(name)

    if not clean:
        return None

    ordered = sorted(
        _NORMALIZED_RTRS_KEYS,
        key=lambda item: len(item[0]),
        reverse=True,
    )

    for key, value, _original_key in ordered:
        if _contains_phrase(clean, key):
            return value

    return None


def _keyword_match(clean, keyword):
    normalized_keyword = normalize_channel_text(keyword)

    if not normalized_keyword:
        return False

    if normalized_keyword in {
        "rt", "rbc", "bbc", "cnn", "mtv",
        "vh1", "fox", "hbo", "amc", "ctc",
    }:
        return _contains_phrase(clean, normalized_keyword)

    return normalized_keyword in clean


def detect_category(name):
    clean = normalize_channel_text(name)

    for category, keywords in CATEGORY_RULES:
        if any(
            _keyword_match(clean, keyword)
            for keyword in keywords
        ):
            return category

    return "Прочее"


def smart_format_entry(
    name,
    extinf,
    default_group="Стабильные ТВ",
):
    name = rename_special_channel(name)

    if extinf and "," in extinf:
        meta, source_name = extinf.split(",", 1)
        extinf = (
            f"{meta},{rename_special_channel(source_name)}"
        )

    matched = match_rtrs(name)

    if matched:
        group, formatted_name, _order = matched
        meta = (extinf or "#EXTINF:-1").split(",", 1)[0]

        if re.search(r'\bgroup-title="[^"]*"', meta, re.I):
            meta = re.sub(
                r'\bgroup-title="[^"]*"',
                f'group-title="{group}"',
                meta,
                flags=re.I,
            )
        else:
            meta += f' group-title="{group}"'

        return f"{meta},{formatted_name}", group

    if not extinf:
        return (
            f'#EXTINF:-1 group-title="{default_group}",'
            f'{name or "Без названия"}',
            default_group,
        )

    if "," in extinf:
        meta, channel_name = extinf.split(",", 1)
    else:
        meta = extinf
        channel_name = name or "Без названия"

    channel_name = rename_special_channel(channel_name.strip())

    if re.search(r'\bgroup-title="[^"]*"', meta, re.I):
        meta = re.sub(
            r'\bgroup-title="[^"]*"',
            f'group-title="{default_group}"',
            meta,
            flags=re.I,
        )
    else:
        meta += f' group-title="{default_group}"'

    return f"{meta},{channel_name}", default_group


# =============================================================================
# PLAYLIST METADATA / BACKUPS
# =============================================================================

def get_build_metadata(
    playlist_schema="3.8",
    total_candidates=0,
    valid_added=0,
):
    return {
        "playlist_schema": playlist_schema,
        "builder_name": SCRIPT_NAME,
        "builder_version": VERSION,
        "builder_code": VERSION_CODE,
        "build_id": BUILD_ID,
        "release_channel": RELEASE_CHANNEL,
        "revision": REVISION,
        "total_candidates": total_candidates,
        "valid_added": valid_added,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def build_playlist_header(
    target_path,
    total_candidates=0,
    valid_added=0,
    playlist_schema="3.8",
):
    meta = get_build_metadata(
        playlist_schema,
        total_candidates,
        valid_added,
    )

    return [
        "#EXTM3U",
        "# ===== BUILD METADATA BEGIN =====",
        "# ==============================================================================",
        f"# Target File:      {os.path.basename(target_path)}",
        f"# Playlist Schema:  v{meta['playlist_schema']}",
        f"# Generated At:     {meta['timestamp']}",
        f"# Builder Engine:   {meta['builder_name']}",
        f"# Builder Version:  {meta['builder_version']}",
        f"# VersionCode:      {meta['builder_code']}",
        f"# Build ID:         {meta['build_id']}",
        f"# Release Channel:  {meta['release_channel']}",
        f"# Revision:         {meta['revision']}",
        f"# Pipeline Stats:   Candidates={meta['total_candidates']} "
        f"| Written={meta['valid_added']}",
        "# Validation:       HEAD/GET/ffprobe; inconclusive retained",
        "# Deduplication:    disabled for ordinary donor records",
        "# ==============================================================================",
        "# ===== BUILD METADATA END =====",
        "",
    ]


def remove_existing_metadata_header(content):
    result = []
    in_metadata = False
    at_start = True

    for line in content.splitlines():
        stripped = line.strip()

        if stripped == "# ===== BUILD METADATA BEGIN =====":
            in_metadata = True
            continue

        if in_metadata:
            if stripped == "# ===== BUILD METADATA END =====":
                in_metadata = False
            continue

        if at_start:
            if not stripped or stripped == "#EXTM3U":
                continue

            if stripped.startswith((
                "# Generated:",
                "# Project:",
                "# Version:",
                "# Validation:",
            )):
                continue

            at_start = False

        result.append(line)

    return "\n".join(result).lstrip("\n")


def backup_file(path):
    if not os.path.exists(path):
        return None

    root = os.path.join(
        tempfile.gettempdir(),
        "iptv_bot_backups",
    )

    os.makedirs(root, exist_ok=True)

    stamp = time.strftime("%Y%m%d_%H%M%S")

    safe = (
        os.path.abspath(path)
        .replace(":", "")
        .replace(os.sep, "_")
    )

    backup = os.path.join(
        root,
        f"{safe}.bak_{stamp}",
    )

    shutil.copy2(path, backup)

    dlog(
        "DREG_SKALA_4_0",
        "PLAYLIST_BACKUP_CREATED",
        path=path,
        backup=backup,
    )

    print(f"Создан backup вне Git: {backup}")

    return backup


def write_entry_lines(lines, extinf, entry):
    """Write EXTINF, its attached directives and the original URL."""
    lines.append((extinf or "#EXTINF:-1").rstrip())

    for directive in entry.get("directives", []):
        lines.append(directive.rstrip())

    lines.append(entry.get("url", "").rstrip())


# =============================================================================
# STREAM WORKERS
# =============================================================================

def process_single_stream_check_simple(entry):
    return (
        entry
        if check_stream_all(entry.get("url", ""))
        else None
    )


def process_single_stream_check_ffprobe(entry):
    return (
        entry
        if check_stream_all(entry.get("url", ""))
        else None
    )


# =============================================================================
# APPEND TO ORDINARY PLAYLISTS — NO GLOBAL DEDUP
# =============================================================================

def append_all_valid(target_path, donor_entries):
    if not donor_entries:
        dlog(
            "DREG_SKALA_4_0",
            "PLAYLIST_NO_NEW_CANDIDATES",
            path=target_path,
        )

        if not os.path.exists(target_path):
            os.makedirs(
                os.path.dirname(target_path) or ".",
                exist_ok=True,
            )

            header = build_playlist_header(target_path)

            with open(
                target_path,
                "w",
                encoding="utf-8",
            ) as output:
                output.write("\n".join(header).rstrip() + "\n")

        return 0

    dlog(
        "DREG_SKALA_4_0",
        "PLAYLIST_VALIDATION_START",
        path=target_path,
        candidates=len(donor_entries),
    )

    valid = []

    with ThreadPoolExecutor(
        max_workers=MAX_STREAM_WORKERS
    ) as executor:
        for result in executor.map(
            process_single_stream_check_simple,
            donor_entries,
        ):
            if result:
                valid.append(result)

    to_append = []

    for entry in valid:
        extinf, _group = smart_format_entry(
            entry.get("name", ""),
            entry.get("extinf", ""),
        )
        to_append.append((extinf, entry))

    existing_body = ""

    if os.path.exists(target_path):
        backup_file(target_path)

        try:
            with open(
                target_path,
                "r",
                encoding="utf-8",
                errors="replace",
            ) as source:
                existing_body = remove_existing_metadata_header(
                    source.read()
                )

        except OSError as exc:
            dlog(
                "DREG_SKALA_4_0",
                "PLAYLIST_READ_FAILED",
                "ERROR",
                path=target_path,
                error=str(exc),
            )

            print(f"Не удалось прочитать {target_path}: {exc}")
            return 0

    os.makedirs(
        os.path.dirname(target_path) or ".",
        exist_ok=True,
    )

    output_lines = build_playlist_header(
        target_path,
        len(donor_entries),
        len(to_append),
    )

    if existing_body.strip():
        output_lines.append(existing_body.rstrip())

    if to_append:
        output_lines.append(
            "# ===== AUTO-UPDATED STABLE STREAMS "
            + time.strftime("%Y-%m-%d %H:%M:%S")
            + " ====="
        )

        for extinf, entry in to_append:
            write_entry_lines(output_lines, extinf, entry)

    with open(
        target_path,
        "w",
        encoding="utf-8",
    ) as output:
        output.write(
            "\n".join(output_lines).rstrip() + "\n"
        )

    dlog(
        "DREG_SKALA_4_0",
        "PLAYLIST_WRITE_COMPLETE",
        path=target_path,
        candidates=len(donor_entries),
        valid=len(to_append),
    )

    print(
        f"В {target_path} добавлено рабочих записей: "
        f"{len(to_append)}"
    )

    return len(to_append)


# =============================================================================
# RTRS PLAYLIST BUILDER — SD/HD + THEMATIC GROUPS
# =============================================================================

def build_rtrs_stable(
    donor_entries,
    target_path=TARGET_RTRS_STABLE,
):
    rtrs_candidates = []
    other_candidates = []

    for entry in donor_entries:
        candidate = dict(entry)
        matched = match_rtrs(entry.get("name", ""))

        if matched:
            candidate["_rtrs"] = matched
            rtrs_candidates.append(candidate)
        else:
            other_candidates.append(candidate)

    dlog(
        "DREG_SKALA_4_0",
        "RTRS_CLASSIFICATION",
        total=len(donor_entries),
        rtrs=len(rtrs_candidates),
        other=len(other_candidates),
    )

    valid_rtrs = []
    valid_other = []

    with ThreadPoolExecutor(
        max_workers=MAX_RTRS_WORKERS
    ) as executor:
        for result in executor.map(
            process_single_stream_check_ffprobe,
            rtrs_candidates,
        ):
            if result:
                valid_rtrs.append(result)

        for result in executor.map(
            process_single_stream_check_ffprobe,
            other_candidates,
        ):
            if result:
                valid_other.append(result)

    slots = defaultdict(list)

    for entry in valid_rtrs:
        group, base_name, order = entry["_rtrs"]
        slots[(order, base_name, group)].append(entry)

    lines = []
    total = 0

    if valid_rtrs:
        lines.append("# ===== РТРС SD / РТРС HD =====")

        for (
            order,
            base_name,
            group,
        ), variants in sorted(
            slots.items(),
            key=lambda item: (
                item[0][0],
                item[0][1],
            ),
        ):
            prefix, _, rest = base_name.partition(" ")

            for index, entry in enumerate(variants):
                display_name = (
                    base_name
                    if index == 0
                    else f"{prefix}.{index} {rest}"
                )

                extinf = (
                    f'#EXTINF:-1 group-title="{group}",'
                    f"{display_name}"
                )

                write_entry_lines(lines, extinf, entry)
                total += 1

    by_category = defaultdict(list)

    for entry in valid_other:
        category = detect_category(entry.get("name", ""))
        by_category[category].append(entry)

    if valid_other:
        lines.append("")
        lines.append("# ===== Тематические группы =====")

        for category in CATEGORY_ORDER:
            if category not in by_category:
                continue

            lines.append(f"# --- {category} ---")

            for entry in by_category[category]:
                name = rename_special_channel(
                    entry.get("name", "") or "Без названия"
                )

                old_extinf = entry.get("extinf", "")

                if old_extinf and "," in old_extinf:
                    meta, old_name = old_extinf.split(",", 1)
                    old_name = rename_special_channel(old_name.strip())

                    if re.search(
                        r'\bgroup-title="[^"]*"',
                        meta,
                        re.I,
                    ):
                        meta = re.sub(
                            r'\bgroup-title="[^"]*"',
                            f'group-title="{category}"',
                            meta,
                            flags=re.I,
                        )
                    else:
                        meta += f' group-title="{category}"'

                    extinf = f"{meta},{old_name or name}"

                else:
                    extinf = (
                        f'#EXTINF:-1 group-title="{category}",{name}'
                    )

                write_entry_lines(lines, extinf, entry)
                total += 1

            lines.append("")

    if total == 0 and os.path.exists(target_path):
        try:
            with open(
                target_path,
                "r",
                encoding="utf-8",
                errors="replace",
            ) as source:
                old_body = remove_existing_metadata_header(
                    source.read()
                )

            if old_body.strip():
                print(
                    "Нет новых проверенных RTRS-кандидатов; "
                    "старый файл сохраняется."
                )

                dlog(
                    "DREG_SKALA_4_0",
                    "RTRS_EXISTING_PRESERVED",
                    path=target_path,
                )

                return 0

        except OSError as exc:
            dlog(
                "DREG_SKALA_4_0",
                "RTRS_READ_FAILED",
                "ERROR",
                error=str(exc),
            )
            return 0

    if os.path.exists(target_path):
        backup_file(target_path)

    os.makedirs(
        os.path.dirname(target_path) or ".",
        exist_ok=True,
    )

    header = build_playlist_header(
        target_path,
        len(donor_entries),
        total,
    )

    with open(
        target_path,
        "w",
        encoding="utf-8",
    ) as output:
        output.write(
            "\n".join(header + lines).rstrip() + "\n"
        )

    dlog(
        "DREG_SKALA_4_0",
        "RTRS_WRITE_COMPLETE",
        path=target_path,
        rtrs_valid=len(valid_rtrs),
        other_valid=len(valid_other),
        written=total,
    )

    print(f"В {target_path} записано потоков: {total}")

    return total


# =============================================================================
# SOURCE WORKERS
# =============================================================================

def fetch_source_worker(source_tuple):
    name, url, _legacy_filter_flag = source_tuple

    if not url or not url.strip():
        dlog(
            "DREG_SKALA_4_0",
            "DONOR_SKIPPED",
            "WARNING",
            source=name,
            reason="empty_url",
        )
        return name, []

    dlog(
        "DREG_SKALA_4_0",
        "DONOR_START",
        source=name,
        url=url,
    )

    print(f"Загрузка {name}: {url}")

    raw = fetch_url(url)

    if not raw:
        return name, []

    try:
        if name == "DISNEY_SOURCE_URL":
            entries = parse_disney_source(raw, url)
        else:
            entries = parse_m3u_content(raw, name)

    except Exception as exc:
        dlog(
            "DREG_SKALA_4_0",
            "DONOR_PARSE_FAILED",
            "ERROR",
            source=name,
            error=str(exc),
        )
        return name, []

    filtered = []
    excluded_18 = 0

    for entry in entries:
        if is_blacklisted(entry.get("name", "")):
            excluded_18 += 1
            continue

        entry["source"] = name
        filtered.append(entry)

    dlog(
        "DREG_SKALA_4_0",
        "DONOR_PARSE_COMPLETE",
        source=name,
        parsed=len(entries),
        excluded_18=excluded_18,
        accepted=len(filtered),
    )

    print(
        f"{name}: распознано {len(entries)}, "
        f"исключено 18+ {excluded_18}, "
        f"принято {len(filtered)}"
    )

    return name, filtered


# =============================================================================
# HVK CONTROL — UNIQUE URL FALLBACK
# =============================================================================

def normalize_url_for_compare(url):
    """
    Normalize only URL scheme and hostname.

    Preserve path and query exactly, including case-sensitive paths,
    catch-up parameters and archive offsets. Remove fragments.
    """

    value = (url or "").strip()

    try:
        parsed = urlsplit(value)

        if not parsed.scheme or not parsed.netloc:
            return value

        scheme = parsed.scheme.lower()
        hostname = (parsed.hostname or "").lower()

        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"

        port = parsed.port

        if port and not (
            (scheme == "http" and port == 80)
            or (scheme == "https" and port == 443)
        ):
            hostname = f"{hostname}:{port}"

        if parsed.username is not None:
            auth = parsed.username

            if parsed.password is not None:
                auth += ":" + parsed.password

            hostname = auth + "@" + hostname

        return urlunsplit((
            scheme,
            hostname,
            parsed.path,
            parsed.query,
            "",
        ))

    except Exception:
        return value


def process_hvk_control(existing_entries):
    """
    HVK is not an ordinary donor.

    Compare normalized URLs against all previous donors.
    Accept only unique fallback URLs. Do not deduplicate ordinary donors.
    """

    name = "HVK_CONTROL_SOURCE_URL"

    raw = fetch_url(HVK_CONTROL_SOURCE_URL)

    if not raw:
        dlog(
            "DREG_SKALA_4_0",
            "HVK_CONTROL_UNAVAILABLE",
            "WARNING",
            url=HVK_CONTROL_SOURCE_URL,
        )
        return []

    try:
        entries = parse_m3u_content(raw, name)

    except Exception as exc:
        dlog(
            "DREG_SKALA_4_0",
            "HVK_CONTROL_PARSE_FAILED",
            "ERROR",
            error=str(exc),
        )
        return []

    known = {
        normalize_url_for_compare(item.get("url", ""))
        for item in existing_entries
    }

    accepted = []
    duplicates = 0
    excluded_18 = 0
    empty_urls = 0

    for entry in entries:
        if is_blacklisted(entry.get("name", "")):
            excluded_18 += 1
            continue

        normalized = normalize_url_for_compare(
            entry.get("url", "")
        )

        if not normalized:
            empty_urls += 1
            continue

        if normalized in known:
            duplicates += 1
            continue

        entry["source"] = name
        accepted.append(entry)
        known.add(normalized)

    dlog(
        "DREG_SKALA_4_0",
        "HVK_CONTROL_COMPLETE",
        parsed=len(entries),
        duplicates_skipped=duplicates,
        unique_fallback=len(accepted),
        excluded_18=excluded_18,
        empty_urls=empty_urls,
    )

    print(
        f"HVK control: записей {len(entries)}; "
        f"пересечений пропущено {duplicates}; "
        f"уникальных добавлено {len(accepted)}; "
        f"18+ исключено {excluded_18}"
    )

    return accepted


# =============================================================================
# DONOR CONFIGURATION — 15 NORMAL + HVK CONTROL
# =============================================================================

def build_source_list():
    return [
        ("SOURCE_URL", SOURCE_URL, False),
        ("SECOND_SOURCE_URL", SECOND_SOURCE_URL, True),
        ("THIRD_SOURCE_URL", THIRD_SOURCE_URL, True),
        ("TEST_CHANNELS_URL", TEST_CHANNELS_URL, False),
        ("FIFTH_SOURCE_URL", FIFTH_SOURCE_URL, False),
        ("SIXTH_SOURCE_URL", SIXTH_SOURCE_URL, False),
        ("SEVENTH_SOURCE_URL", SEVENTH_SOURCE_URL, False),
        ("EIGHTH_SOURCE_URL", EIGHTH_SOURCE_URL, False),
        ("NINTH_SOURCE_URL", NINTH_SOURCE_URL, False),
        ("CHEBUR_SOURCE_URL", CHEBUR_SOURCE_URL, False),
        ("ELEVENTH_SOURCE_URL", ELEVENTH_SOURCE_URL, False),
        ("TWELFTH_SOURCE_URL", TWELFTH_SOURCE_URL, False),
        ("DISNEY_SOURCE_URL", DISNEY_SOURCE_URL, False),
        (
            "CHANNELS_INVENTORY_SOURCE_URL",
            CHANNELS_INVENTORY_SOURCE_URL,
            False,
        ),
        ("ALISKA_SOURCE_URL", ALISKA_SOURCE_URL, False),
    ]


# =============================================================================
# GIT PUBLICATION
# =============================================================================

def publish_changes(modified_files, added_total):
    """
    Publish playlists and optionally DREG logs.

    DREG is closed before staging so the committed journal snapshot is
    stable. The final Git result is printed to the original console.
    """

    include_logs = bool(
        DREG_INCLUDE_IN_GIT and not SKIP_PUSH
    )

    dlog(
        "DREG_SKALA_4_0",
        "GIT_PUBLICATION_PREPARE",
        branch=TARGET_PUSH_BRANCH,
        playlist_files=modified_files,
        include_dreg_logs=include_logs,
    )

    stop_dreg_for_git()

    git_files = list(modified_files)

    if include_logs:
        for path in (LOG_COMMON, LOG_SKALA, LOG_TITAN):
            if os.path.isfile(path):
                git_files.append(path)

    # Preserve ordering and remove repeated path arguments only.
    git_files = list(dict.fromkeys(git_files))

    if not git_files:
        print("Нет файлов для публикации.")
        return 0

    run(["git", "add", "--"] + git_files, check=True)

    staged = run(
        ["git", "diff", "--cached", "--quiet"],
        check=False,
    )

    if staged.returncode == 0:
        print("Нет staged-изменений; коммит не создаётся.")
        return 0

    if staged.returncode not in (0, 1):
        raise RuntimeError(
            f"git diff --cached завершился с кодом {staged.returncode}"
        )

    commit_msg = (
        f"chore: Phoenix Baryshnikov {VERSION} "
        f"({REVISION}) — 16-source IPTV + DREG dual logs"
    )

    run(["git", "commit", "-m", commit_msg], check=True)

    run(
        ["git", "push", "origin", TARGET_PUSH_BRANCH],
        check=True,
    )

    print(
        f"Успешно опубликовано в ветку {TARGET_PUSH_BRANCH}: "
        f"{VERSION}"
    )

    print(f"Build ID: {BUILD_ID}")
    print(f"Revision: {REVISION}")
    print(f"Добавлено записей: {added_total}")

    return 0


# =============================================================================
# MAIN
# =============================================================================

def main():
    """
    Full build sequence:
      1. Git preflight and optional branch preparation.
      2. DREG initialization.
      3. ffprobe bootstrap.
      4. Parallel download of 15 ordinary donors.
      5. HVK unique-URL fallback.
      6. Four playlist targets.
      7. Local completion or Git publication.
    """

    # -------------------------------------------------------------------------
    # Git preflight before opening DREG files.
    # This prevents branch checkout from moving or conflicting with open logs.
    # -------------------------------------------------------------------------

    if not shutil.which("git"):
        print("КРИТИЧЕСКАЯ ОШИБКА: Git не найден в PATH.")
        return 1

    try:
        if not git_is_clean():
            print(
                "Рабочее дерево Git не чистое. "
                "Очистите или закоммитьте изменения."
            )
            return 1

        if not SKIP_PUSH:
            print(
                f"Подготовка ветки публикации: {TARGET_PUSH_BRANCH}"
            )
            ensure_on_branch(TARGET_PUSH_BRANCH)

    except Exception as exc:
        print(f"Ошибка предварительной подготовки Git: {exc}")
        return 1

    # -------------------------------------------------------------------------
    # DREG initialization after branch preparation.
    # -------------------------------------------------------------------------

    start_dreg()

    dlog(
        "DREG_SKALA_4_0",
        "BUILD_CONFIGURATION",
        version=VERSION,
        version_code=VERSION_CODE,
        build_id=BUILD_ID,
        channel=RELEASE_CHANNEL,
        revision=REVISION,
        skip_push=SKIP_PUSH,
        target_branch=TARGET_PUSH_BRANCH,
        dreg_include_in_git=DREG_INCLUDE_IN_GIT,
        source_count=16,
    )

    try:
        if SKIP_PUSH:
            print(
                "SKIP_PUSH=true — публикация отключена; "
                "DREG-журналы остаются локальными."
            )

        # ---------------------------------------------------------------------
        # ffprobe preparation.
        # ---------------------------------------------------------------------

        if not ensure_ffprobe():
            raise RuntimeError(
                "ffprobe недоступен: невозможно выполнить проверку"
            )

        # ---------------------------------------------------------------------
        # Parallel ordinary donors.
        # 15 ordinary donors + 1 HVK control donor.
        # ---------------------------------------------------------------------

        sources = build_source_list()

        donor_entries = []
        source_counts = {}

        dlog(
            "DREG_SKALA_4_0",
            "DONOR_BATCH_START",
            normal_sources=len(sources),
            hvk_mode="unique_fallback_after_other_donors",
        )

        print(
            f"Запуск параллельной загрузки {len(sources)} "
            "основных доноров; HVK будет обработан отдельно."
        )

        with ThreadPoolExecutor(
            max_workers=min(MAX_SOURCE_WORKERS, len(sources))
        ) as executor:

            future_map = {
                executor.submit(
                    fetch_source_worker,
                    source,
                ): source[0]
                for source in sources
            }

            for future in as_completed(future_map):
                source_name = future_map[future]

                try:
                    name, entries = future.result()

                    source_counts[name] = len(entries)
                    donor_entries.extend(entries)

                    dlog(
                        "DREG_SKALA_4_0",
                        "DONOR_MERGED",
                        source=name,
                        records=len(entries),
                        pool_size=len(donor_entries),
                    )

                except Exception as exc:
                    source_counts[source_name] = 0

                    dlog(
                        "DREG_SKALA_4_0",
                        "DONOR_WORKER_EXCEPTION",
                        "ERROR",
                        source=source_name,
                        error=str(exc),
                    )

                    print(
                        f"Ошибка донора {source_name}: {exc}"
                    )

        # ---------------------------------------------------------------------
        # HVK is processed only after all ordinary donor records are known.
        # ---------------------------------------------------------------------

        hvk_entries = process_hvk_control(donor_entries)

        donor_entries.extend(hvk_entries)

        source_counts["HVK_CONTROL_SOURCE_URL"] = len(hvk_entries)

        # ---------------------------------------------------------------------
        # Global pool summary.
        # ---------------------------------------------------------------------

        if not donor_entries:
            dlog(
                "DREG_SKALA_4_0",
                "EMPTY_GLOBAL_POOL",
                "ERROR",
                source_count=len(sources) + 1,
            )

            raise RuntimeError(
                "Ни один донор не предоставил пригодных записей"
            )

        print("\nСводка по источникам:")

        for source in sources:
            name = source[0]
            print(f"  {name}: {source_counts.get(name, 0)}")

        print(
            "  HVK_CONTROL_SOURCE_URL: "
            f"{source_counts.get('HVK_CONTROL_SOURCE_URL', 0)}"
        )

        print(f"\nВсего кандидатов: {len(donor_entries)}")
        print("Глобальная дедупликация отключена.")
        print("HVK: контроль уникальных URL включён.")

        dlog(
            "DREG_SKALA_4_0",
            "GLOBAL_POOL_READY",
            records=len(donor_entries),
            sources_with_records=sum(
                1
                for count in source_counts.values()
                if count
            ),
            global_deduplication=False,
            hvk_unique_fallback=True,
        )

        # ---------------------------------------------------------------------
        # Ordinary playlist targets.
        # ---------------------------------------------------------------------

        modified = []
        added_total = 0

        ordinary_targets = (
            TARGET_MIR,
            TARGET_STABLE_LEGACY,
            TARGET_STABLE,
        )

        for target in ordinary_targets:
            before_exists = os.path.exists(target)

            added = append_all_valid(
                target,
                donor_entries,
            )

            if added:
                modified.append(target)
                added_total += added

            elif (
                not SKIP_PUSH
                and os.path.exists(target)
                and os.path.getsize(target) > 0
            ):
                # Preserve original publication behavior:
                # existing target files remain eligible for publication.
                modified.append(target)

            elif not before_exists and os.path.exists(target):
                # An empty first-run playlist may still be a new output.
                modified.append(target)

        # ---------------------------------------------------------------------
        # RTRS SD/HD and thematic output.
        # ---------------------------------------------------------------------

        rtrs_added = build_rtrs_stable(
            donor_entries,
            TARGET_RTRS_STABLE,
        )

        if os.path.exists(TARGET_RTRS_STABLE):
            if TARGET_RTRS_STABLE not in modified:
                modified.append(TARGET_RTRS_STABLE)

        added_total += rtrs_added

        # ---------------------------------------------------------------------
        # Build summary.
        # ---------------------------------------------------------------------

        dlog(
            "DREG_SKALA_4_0",
            "BUILD_SUMMARY",
            candidate_count=len(donor_entries),
            added_count=added_total,
            modified_files=modified,
            source_counts=source_counts,
            stream_cache_entries=len(_STREAM_CHECK_CACHE),
        )

        print("\nИТОГ СБОРКИ")
        print(f"Кандидатов: {len(donor_entries)}")
        print(f"Добавлено проверенных записей: {added_total}")
        print(f"Файлы: {modified}")
        print(
            "Проверок URL в кэше: "
            f"{len(_STREAM_CHECK_CACHE)}"
        )

        # ---------------------------------------------------------------------
        # Local-only mode.
        # ---------------------------------------------------------------------

        if SKIP_PUSH:
            print("\nЛокальная сборка завершена.")
            print("SKIP_PUSH=true — commit/push не выполнялись.")
            print("DREG-журналы сохранены локально.")

            dlog(
                "DREG_SKALA_4_0",
                "LOCAL_BUILD_COMPLETE",
                added=added_total,
                modified=modified,
                logs_published=False,
            )

            return 0

        # ---------------------------------------------------------------------
        # Publish mode.
        # Do not return early when playlist files are unchanged:
        # DREG logs may still have new events worth publishing.
        # ---------------------------------------------------------------------

        if not modified:
            print(
                "\nПлейлисты не изменились. "
                "Проверяется публикация DREG-журналов."
            )

            dlog(
                "DREG_SKALA_4_0",
                "NO_PLAYLIST_CHANGES",
            )

        return publish_changes(
            modified,
            added_total,
        )

    except Exception as exc:
        dlog(
            "DREG_SKALA_4_0",
            "BUILD_FATAL",
            "ERROR",
            error=str(exc),
        )

        print(f"КРИТИЧЕСКАЯ ОШИБКА СБОРКИ: {exc}")

        return 1

    finally:
        if DREG is not None:
            try:
                stop_dreg_for_git()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())