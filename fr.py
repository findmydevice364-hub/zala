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
Revision:        17-SOURCE-DREG-DUAL

File:            git_bot_for_stable.py

17-source IPTV collector, validation pipeline, RTRS normalizer,
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
REVISION = "17-SOURCE-DREG-DUAL"

PROJECT_NAME = 'Phoenix Hub "Baryshnikov"'
SCRIPT_NAME = "git_bot_for_stable.py"


# =============================================================================
# DONOR URLS — 17 SOURCES
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

# 16th ordinary donor.
SIXTEENTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "vasil133767-afk/my-iptv/main/playlist.m3u"
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
    "SIXTEENTH_SOURCE_URL",
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
        source_count=17,
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
                except 