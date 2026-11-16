
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
===============================================================================
Project:       findmydevice364-hub_lab
Owner:         #Phoenix89S_Lab©
Public Name:   Phoenix Hub "Baryshnikov"
File:          git_bot_for_baryshnikov.py

Version:       3.8.0-alpha.1
VersionCode:   31.8.0.1
Build ID:      P3100-A1-20261116
Release:       ALPHA
Revision:      16-SOURCE-DREG-DUAL

PURPOSE:
    Shirt.uz IPTV collector.
    Recursive M3U/M3U8 parser.
    Automatic FFprobe installer.
    IPTV stream validator.
    Stable playlist builder.

PARALLEL ARCHITECTURE:
    DOWNLOAD:       36 workers
    PARSING:        57 workers
    VALIDATION:     60 workers

OUTPUTS:
    Main/donors_uz.m3u
    Main/stable_uz.m3u
    Shirt_uz/donors_uz.m3u
    Shirt_uz/stable_uz.m3u

DREG JOURNALS IN BOTH OUTPUT DIRECTORIES:
    logs/DREG_COMMON_uz.txt
    logs/DREG_SKALA_4_0_uz.txt
    logs/DREG_TITAN_2_8_uz.txt

OTHER REPORTS:
    errors.json

ARCHITECTURE:
    SKALA 4.0 — operational journal.
    TITAN 2.8 — extended technical journal.
    DREG COMMON — duplicated journal of all events.

IMPORTANT:
    - Donor records are not globally deduplicated.
    - Duplicate source URLs are suppressed during discovery.
    - Inconclusive checks are configurable.
    - FFprobe is installed automatically when necessary.
    - FFprobe archives are verified using SHA-256.
    - Downloaded FFprobe is cached in .tools/ffmpeg.
    - Playlist writes use temporary files and atomic replacement.
===============================================================================
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import uuid
import zipfile

from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import (
    urljoin,
    urlsplit,
    urlunsplit,
    unquote,
)

import requests


# =============================================================================
# 01. BUILD METADATA
# =============================================================================

PROJECT = "findmydevice364-hub_lab"
OWNER = "#Phoenix89S_Lab©"
PUBLIC_NAME = 'Phoenix Hub "Baryshnikov"'
SCRIPT_NAME = "git_bot_for_baryshnikov.py"

VERSION = "3.8.0-alpha.1"
VERSION_CODE = "31.8.0.1"
BUILD_ID = "P3100-A1-20261116"
RELEASE_CHANNEL = "ALPHA"
REVISION = "16-SOURCE-DREG-DUAL"

BUILD_METADATA = {
    "project": PROJECT,
    "owner": OWNER,
    "public_name": PUBLIC_NAME,
    "script": SCRIPT_NAME,
    "version": VERSION,
    "version_code": VERSION_CODE,
    "build_id": BUILD_ID,
    "release_channel": RELEASE_CHANNEL,
    "revision": REVISION,
}


# =============================================================================
# 02. DIRECTORY STRUCTURE
# =============================================================================

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

MAIN_DONORS = MAIN_DIR / "donors_uz.m3u"
MAIN_STABLE = MAIN_DIR / "stable_uz.m3u"

SHIRT_DONORS = SHIRT_DIR / "donors_uz.m3u"
SHIRT_STABLE = SHIRT_DIR / "stable_uz.m3u"

INDEX_URL = os.getenv(
    "SHIRT_UZ_INDEX_URL",
    "https://shirtv.uz/playlists.html",
)

FFPROBE_DIR = ROOT_DIR / ".tools" / "ffmpeg"


# =============================================================================
# 03. PARALLEL CONFIGURATION
# =============================================================================

MAX_DOWNLOAD_WORKERS = max(
    1,
    int(os.getenv("MAX_DOWNLOAD_WORKERS", "36")),
)

MAX_PARSE_WORKERS = max(
    1,
    int(os.getenv("MAX_PARSE_WORKERS", "57")),
)

MAX_VALIDATION_WORKERS = max(
    1,
    int(os.getenv("MAX_VALIDATION_WORKERS", "60")),
)

# Compatibility with previous diagnostics.
MAX_WORKERS = MAX_VALIDATION_WORKERS

DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(
    MAX_DOWNLOAD_WORKERS
)

VALIDATION_SEMAPHORE = threading.BoundedSemaphore(
    MAX_VALIDATION_WORKERS
)


# =============================================================================
# 04. NETWORK CONFIGURATION
# =============================================================================

REQUEST_TIMEOUT = int(
    os.getenv("REQUEST_TIMEOUT", "20")
)

MAX_RETRIES = max(
    0,
    int(os.getenv("MAX_RETRIES", "3")),
)

RETRY_DELAY = float(
    os.getenv("RETRY_DELAY", "2")
)

MAX_PAGE_SIZE = 8 * 1024 * 1024
MAX_PLAYLIST_SIZE = 64 * 1024 * 1024

MAX_REDIRECTS = 10

MAX_PLAYLIST_DEPTH = max(
    0,
    int(os.getenv("MAX_PLAYLIST_DEPTH", "2")),
)


# =============================================================================
# 05. FFPROBE CONFIGURATION
# =============================================================================

ENABLE_FFPROBE = os.getenv(
    "ENABLE_FFPROBE",
    "true",
).lower() in {
    "1",
    "true",
    "yes",
    "on",
}

AUTO_INSTALL_FFPROBE = os.getenv(
    "AUTO_INSTALL_FFPROBE",
    "true",
).lower() in {
    "1",
    "true",
    "yes",
    "on",
}

FFPROBE_CONFIGURED = os.getenv(
    "FFPROBE_BIN",
    "",
).strip()

FFPROBE_BIN = (
    FFPROBE_CONFIGURED
    or "ffprobe"
)

FFPROBE_TIMEOUT = int(
    os.getenv("FFPROBE_TIMEOUT", "20")
)

FFPROBE_READ_SECONDS = int(
    os.getenv("FFPROBE_READ_SECONDS", "8")
)

FFPROBE_DOWNLOAD_TIMEOUT = int(
    os.getenv("FFPROBE_DOWNLOAD_TIMEOUT", "180")
)

MAX_FFPROBE_ARCHIVE_SIZE = (
    250 * 1024 * 1024
)

KEEP_INCONCLUSIVE = os.getenv(
    "KEEP_INCONCLUSIVE",
    "true",
).lower() in {
    "1",
    "true",
    "yes",
    "on",
}

REQUIRE_MEDIA_PROBE = os.getenv(
    "REQUIRE_MEDIA_PROBE",
    "false",
).lower() in {
    "1",
    "true",
    "yes",
    "on",
}


# =============================================================================
# 06. PLAYLIST CONFIGURATION
# =============================================================================

DEDUPLICATE_DONOR_RECORDS = False

PLAYLIST_EXTENSIONS = (
    ".m3u",
    ".m3u8",
    ".m3u.txt",
)

DIRECTIVE_PREFIXES = (
    "#EXTVLCOPT:",
    "#EXTGRP:",
    "#KODIPROP:",
    "#EXTHTTP:",
    "#EXTALB:",
    "#EXTART:",
    "#EXTGENRE:",
    "#EXTLANG:",
    "#EXTLOGO:",
    "#EXTOPT:",
    "#EXTM3U:",
)

ADULT_PATTERNS = (
    r"\b18\+\b",
    r"\bxxx\b",
    r"\badult\b",
    r"\bporn\b",
    r"\berotic\b",
    r"для\s+взрослых",
    r"эрот",
)

PLAYLIST_LINK_PATTERN = re.compile(
    r"""(?i)(?:https?://|/|\.{1,2}/)[^\s"'<>]+
        \.(?:m3u8?|m3u\.txt)(?:\?[^\s"'<>]*)?""",
    re.VERBOSE,
)


# =============================================================================
# 07. GLOBAL STATE
# =============================================================================

RUN_ID = uuid.uuid4().hex[:16].upper()

DREG = None

STOP_EVENT = threading.Event()

HTTP_LOCAL = threading.local()

COUNTER_LOCK = threading.RLock()

SOURCE_ERRORS_LOCK = threading.RLock()

URL_CACHE_LOCK = threading.RLock()

FFPROBE_INSTALL_LOCK = threading.RLock()

URL_CACHE: dict[str, Any] = {}

SOURCE_ERRORS = []

COUNTERS = {
    "sources_discovered": 0,
    "sources_downloaded": 0,
    "source_errors": 0,
    "nested_playlists": 0,
    "donor_entries": 0,
    "adult_filtered": 0,
    "checked": 0,
    "accepted": 0,
    "rejected": 0,
    "inconclusive": 0,
    "cache_hits": 0,
    "http_errors": 0,
    "ffprobe_errors": 0,
    "ffprobe_downloads": 0,
    "ffprobe_cache_hits": 0,
    "write_errors": 0,
}


def counter_add(name: str, value: int = 1):
    with COUNTER_LOCK:
        COUNTERS[name] = (
            COUNTERS.get(name, 0) + value
        )


def counter_snapshot():
    with COUNTER_LOCK:
        return dict(COUNTERS)


def remember_error(source, error):
    with SOURCE_ERRORS_LOCK:
        SOURCE_ERRORS.append({
            "source": str(source),
            "error": str(error),
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
        })


# =============================================================================
# 08. DREG JOURNAL SYSTEM
# =============================================================================

class DREGManager:

    SYSTEM_SKALA = "DREG_SKALA_4_0"
    SYSTEM_TITAN = "DREG_TITAN_2_8"

    FILENAMES = {
        "common": "DREG_COMMON_uz.txt",
        "skala": "DREG_SKALA_4_0_uz.txt",
        "titan": "DREG_TITAN_2_8_uz.txt",
    }

    def __init__(self):
        self.lock = threading.RLock()
        self.seq = 0
        self.closed = False

        self.files = {}
        self.failed_destinations = set()

        self._open_all()

    def _open_all(self):

        for directory in (
            MAIN_LOG_DIR,
            SHIRT_LOG_DIR,
        ):

            try:
                directory.mkdir(
                    parents=True,
                    exist_ok=True,
                )

            except Exception:
                self.failed_destinations.add(
                    str(directory)
                )
                continue

            for filename in self.FILENAMES.values():

                path = directory / filename

                try:
                    handle = path.open(
                        "a",
                        encoding="utf-8",
                        newline="\n",
                    )

                    self.files[
                        (directory, filename)
                    ] = handle

                except Exception:
                    self.failed_destinations.add(
                        str(path)
                    )

    def _serialize(
        self,
        system,
        level,
        message,
        details,
    ):

        self.seq += 1

        record = {
            "timestamp": datetime.now(
                timezone.utc
            ).astimezone().isoformat(
                timespec="milliseconds"
            ),
            "run_id": RUN_ID,
            "seq": self.seq,
            "level": str(level).upper(),
            "system": system,
            "message": str(message),
        }

        if details:
            record["details"] = details

        return json.dumps(
            record,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        ) + "\n"

    def emit(
        self,
        system,
        level,
        message,
        **details,
    ):

        with self.lock:

            if self.closed:
                return

            if system == self.SYSTEM_TITAN:
                system_file = self.FILENAMES["titan"]
            else:
                system = self.SYSTEM_SKALA
                system_file = self.FILENAMES["skala"]

            try:
                line = self._serialize(
                    system,
                    level,
                    message,
                    details,
                )

            except Exception:
                return

            common_file = self.FILENAMES["common"]

            for directory in (
                MAIN_LOG_DIR,
                SHIRT_LOG_DIR,
            ):

                for filename in (
                    system_file,
                    common_file,
                ):

                    key = (directory, filename)

                    handle = self.files.get(key)

                    if handle is None:
                        continue

                    try:
                        handle.write(line)
                        handle.flush()

                    except Exception:

                        self.failed_destinations.add(
                            str(directory / filename)
                        )

                        try:
                            handle.close()
                        except Exception:
                            pass

                        self.files.pop(key, None)

    def skala(
        self,
        message,
        level="INFO",
        **details,
    ):

        self.emit(
            self.SYSTEM_SKALA,
            level,
            message,
            **details,
        )

    def titan(
        self,
        message,
        level="INFO",
        **details,
    ):

        self.emit(
            self.SYSTEM_TITAN,
            level,
            message,
            **details,
        )

    def close(self):

        with self.lock:

            if self.closed:
                return

            for handle in list(
                self.files.values()
            ):

                try:
                    handle.flush()
                    handle.close()
                except Exception:
                    pass

            self.files.clear()
            self.closed = True


def log_skala(
    message,
    level="INFO",
    **details,
):

    if DREG is not None:
        DREG.skala(
            message,
            level,
            **details,
        )


def log_titan(
    message,
    level="INFO",
    **details,
):

    if DREG is not None:
        DREG.titan(
            message,
            level,
            **details,
        )


# =============================================================================
# 09. HTTP SESSION MANAGEMENT
# =============================================================================

def get_http_session():

    session = getattr(
        HTTP_LOCAL,
        "session",
        None,
    )

    if session is None:

        session = requests.Session()

        session.max_redirects = MAX_REDIRECTS

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 "
                "(compatible; PhoenixHub-Baryshnikov/3.8)"
            ),
            "Accept": (
                "application/vnd.apple.mpegurl,"
                "application/x-mpegURL,"
                "audio/mpegurl,"
                "audio/x-mpegurl,"
                "application/octet-stream,"
                "*/*"
            ),
            "Accept-Language": "ru,en;q=0.8",
            "Connection": "keep-alive",
        })

        HTTP_LOCAL.session = session

    return session


# =============================================================================
# 10. CONTROLLED HTTP DOWNLOAD
# =============================================================================

def fetch_url(
    url,
    max_bytes=MAX_PLAYLIST_SIZE,
    referer=None,
):

    last_error = None

    headers = {}

    if referer:
        headers["Referer"] = referer

    for attempt in range(
        MAX_RETRIES + 1
    ):

        if STOP_EVENT.is_set():
            raise RuntimeError(
                "Operation interrupted"
            )

        response = None

        DOWNLOAD_SEMAPHORE.acquire()

        try:

            response = get_http_session().get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                stream=True,
            )

            response.raise_for_status()

            chunks = []
            total = 0

            for chunk in response.iter_content(
                131072
            ):

                if not chunk:
                    continue

                total += len(chunk)

                if total > max_bytes:
                    raise ValueError(
                        f"Response exceeds {max_bytes} bytes"
                    )

                chunks.append(chunk)

            result = b"".join(chunks)

            log_titan(
                "HTTP download successful",
                requested_url=url,
                final_url=response.url,
                status=response.status_code,
                bytes=len(result),
                attempt=attempt + 1,
                content_type=response.headers.get(
                    "Content-Type"
                ),
            )

            return result

        except Exception as exc:

            last_error = exc

            counter_add("http_errors")

            log_titan(
                "HTTP download failed",
                level="WARNING",
                url=url,
                attempt=attempt + 1,
                error=repr(exc),
            )

            if attempt < MAX_RETRIES:
                time.sleep(
                    RETRY_DELAY * (attempt + 1)
                )

        finally:

            if response is not None:

                try:
                    response.close()
                except Exception:
                    pass

            DOWNLOAD_SEMAPHORE.release()

    raise RuntimeError(
        f"Unable to download {url}: {last_error}"
    )


# =============================================================================
# 11. URL NORMALIZATION
# =============================================================================

def normalize_url_for_compare(url):

    try:

        parts = urlsplit(
            str(url).strip()
        )

        scheme = parts.scheme.lower()

        hostname = (
            parts.hostname or ""
        ).lower()

        if not scheme or not hostname:

            return str(url).strip().split(
                "#",
                1,
            )[0]

        username = parts.username
        password = parts.password

        credentials = ""

        if username is not None:

            credentials = username

            if password is not None:
                credentials += ":" + password

            credentials += "@"

        try:
            port = parts.port
        except ValueError:
            port = None

        default_port = (
            (scheme == "http" and port == 80)
            or (
                scheme == "https"
                and port == 443
            )
        )

        host = hostname

        if ":" in hostname and not hostname.startswith("["):
            host = f"[{hostname}]"

        if port is not None and not default_port:
            host += f":{port}"

        return urlunsplit((
            scheme,
            credentials + host,
            parts.path,
            parts.query,
            "",
        ))

    except Exception:

        return str(url).strip().split(
            "#",
            1,
        )[0]


def is_http_url(url):

    try:

        parts = urlsplit(
            str(url)
        )

        return (
            parts.scheme.lower() in (
                "http",
                "https",
            )
            and bool(parts.netloc)
        )

    except Exception:
        return False


def is_playlist_url(url):

    path = urlsplit(
        str(url)
    ).path.lower()

    return any(
        path.endswith(extension)
        for extension in PLAYLIST_EXTENSIONS
    )


# =============================================================================
# 12. HTML SOURCE DISCOVERY
# =============================================================================

class ShirtPlaylistHTMLParser(HTMLParser):

    def __init__(self):

        super().__init__(
            convert_charrefs=True
        )

        self.links = []
        self.text_fragments = []

    def _add_candidate(self, value):

        if not value:
            return

        value = str(value).strip().strip(
            "\"'"
        )

        if value:
            self.links.append(value)

    def handle_starttag(
        self,
        tag,
        attrs,
    ):

        attributes = dict(attrs)

        for key in (
            "href",
            "data-href",
            "data-url",
            "data-src",
            "src",
        ):

            self._add_candidate(
                attributes.get(key)
            )

        onclick = attributes.get(
            "onclick",
            "",
        )

        if onclick:

            matches = re.findall(
                r"""['"]([^'"]+\.(?:m3u8?|m3u\.txt)(?:\?[^'"]*)?)['"]""",
                onclick,
                re.I,
            )

            for match in matches:
                self._add_candidate(match)

    def handle_data(self, data):

        if data:
            self.text_fragments.append(data)


def discover_playlist_urls():

    log_skala(
        "Loading Shirt.uz playlist catalogue",
        url=INDEX_URL,
    )

    page = fetch_url(
        INDEX_URL,
        max_bytes=MAX_PAGE_SIZE,
    )

    html = page.decode(
        "utf-8",
        errors="replace",
    )

    parser = ShirtPlaylistHTMLParser()

    parser.feed(html)
    parser.close()

    candidates = list(
        parser.links
    )

    embedded_matches = (
        PLAYLIST_LINK_PATTERN.findall(html)
    )

    candidates.extend(
        embedded_matches
    )

    result = []
    seen = set()

    for href in candidates:

        href = unquote(
            str(href).strip()
        )

        absolute = urljoin(
            INDEX_URL,
            href,
        )

        if not is_http_url(absolute):
            continue

        if not is_playlist_url(absolute):
            continue

        normalized = normalize_url_for_compare(
            absolute
        )

        if normalized in seen:
            continue

        seen.add(normalized)

        result.append(absolute)

    counter_add(
        "sources_discovered",
        len(result),
    )

    log_skala(
        "Playlist catalogue parsed",
        raw_candidates=len(candidates),
        unique_sources=len(result),
    )

    log_titan(
        "Catalogue parser diagnostics",
        html_size=len(page),
        raw_links=len(parser.links),
        embedded_matches=len(embedded_matches),
        unique_sources=len(result),
        sources=result,
    )

    if not result:

        raise RuntimeError(
            "No M3U/M3U8 links discovered on Shirt.uz"
        )

    return result


# =============================================================================
# 13. PLAYLIST DECODING
# =============================================================================

def decode_playlist(data):

    for encoding in (
        "utf-8-sig",
        "utf-8",
        "cp1251",
        "latin-1",
    ):

        try:
            return data.decode(encoding)

        except UnicodeDecodeError:
            continue

    return data.decode(
        "utf-8",
        errors="replace",
    )


def is_hls_media_playlist(content):

    head = content[:16000].upper()

    return any(
        marker in head
        for marker in (
            "#EXT-X-TARGETDURATION",
            "#EXT-X-MEDIA-SEQUENCE",
            "#EXT-X-PLAYLIST-TYPE",
            "#EXT-X-ENDLIST",
            "#EXT-X-PART",
        )
    )


def is_hls_master_playlist(content):

    head = content[:16000].upper()

    return (
        "#EXT-X-STREAM-INF" in head
        or "#EXT-X-I-FRAME-STREAM-INF" in head
    )


def extract_extinf_name(extinf):

    try:

        return extinf.split(
            ",",
            1,
        )[1].strip()

    except Exception:
        return "Без названия"


def make_default_entry(
    url,
    name,
    source=None,
):

    safe_name = (
        name.strip()
        if name
        else "Без названия"
    )

    return {
        "extinf": (
            '#EXTINF:-1 group-title="Прочее",'
            + safe_name
        ),
        "url": url,
        "directives": [],
        "source": source or url,
        "source_line": 0,
        "status": "unverified",
    }


# =============================================================================
# 14. M3U PARSER
# =============================================================================

def parse_m3u_content(
    content,
    source_url,
):

    if not content:
        return []

    if not content.lstrip().startswith("#EXTM3U"):

        log_titan(
            "Source does not start with EXT M3U",
            level="WARNING",
            source=source_url,
            sample=content[:160],
        )

    # HLS media playlist:
    # do not turn media segments into IPTV channels.

    if is_hls_media_playlist(content):

        log_titan(
            "HLS media playlist detected",
            source=source_url,
            action="retain_playlist_as_stream",
        )

        return [
            make_default_entry(
                source_url,
                Path(
                    urlsplit(source_url).path
                ).stem or "HLS Stream",
                source_url,
            )
        ]

    entries = []

    current_extinf = None
    current_directives = []

    pending_stream_inf = None

    malformed = 0
    line_count = 0

    for line_number, raw in enumerate(
        content.splitlines(),
        1,
    ):

        line_count += 1

        line = raw.strip().lstrip(
            "\ufeff"
        )

        if not line:
            continue

        if line.startswith("#EXTM3U"):
            continue

        if line.startswith(
            "#EXT-X-STREAM-INF:"
        ):

            pending_stream_inf = line
            continue

        if line.startswith(
            "#EXT-X-I-FRAME-STREAM-INF:"
        ):

            uri_match = re.search(
                r'URI="([^"]+)"',
                line,
                re.I,
            )

            if uri_match:

                variant_url = urljoin(
                    source_url,
                    uri_match.group(1),
                )

                entries.append(
                    make_default_entry(
                        variant_url,
                        "HLS I-Frame",
                        source_url,
                    )
                )

            continue

        if line.startswith("#EXTINF:"):

            if "," not in line:

                malformed += 1

                current_extinf = None
                current_directives = []

                continue

            current_extinf = line
            current_directives = []

            continue

        if line.startswith("#"):

            if current_extinf is not None:

                if line.startswith(
                    DIRECTIVE_PREFIXES
                ):

                    current_directives.append(line)

            continue

        absolute_url = urljoin(
            source_url,
            line,
        )

        if not is_http_url(absolute_url):
            continue

        if pending_stream_inf:

            variant_name = (
                "HLS Variant "
                + str(len(entries) + 1)
            )

            entries.append(
                make_default_entry(
                    absolute_url,
                    variant_name,
                    source_url,
                )
            )

            pending_stream_inf = None

            continue

        if current_extinf is None:

            extinf = (
                '#EXTINF:-1 group-title="Прочее",'
                "Без названия"
            )

            directives = []

        else:

            extinf = current_extinf

            directives = list(
                current_directives
            )

        entries.append({
            "extinf": extinf,
            "url": absolute_url,
            "directives": directives,
            "source": source_url,
            "source_line": line_number,
            "status": "unverified",
        })

        current_extinf = None
        current_directives = []

    log_titan(
        "M3U parser completed",
        source=source_url,
        lines=line_count,
        records=len(entries),
        malformed_extinf=malformed,
        hls_master=is_hls_master_playlist(content),
    )

    return entries


# =============================================================================
# 15. ADULT CONTENT FILTER
# =============================================================================

def is_blacklisted(entry):

    searchable = (
        entry.get("extinf", "")
        + " "
        + entry.get("url", "")
    ).lower()

    return any(
        re.search(
            pattern,
            searchable,
            re.I,
        )
        for pattern in ADULT_PATTERNS
    )


# =============================================================================
# 16. RECURSIVE PLAYLIST PARSER
# =============================================================================

def parse_and_expand_content(
    content,
    source_url,
    depth=0,
    visited=None,
):

    if visited is None:
        visited = set()

    normalized = normalize_url_for_compare(
        source_url
    )

    if normalized in visited:

        log_titan(
            "Recursive playlist cycle detected",
            url=source_url,
            depth=depth,
        )

        return []

    if depth > MAX_PLAYLIST_DEPTH:

        log_titan(
            "Playlist recursion depth limit reached",
            level="WARNING",
            url=source_url,
            depth=depth,
            maximum=MAX_PLAYLIST_DEPTH,
        )

        return []

    visited.add(normalized)

    parsed = parse_m3u_content(
        content,
        source_url,
    )

    output = []

    for entry in parsed:

        entry_url = entry["url"]

        if (
            is_playlist_url(entry_url)
            and depth < MAX_PLAYLIST_DEPTH
        ):

            counter_add(
                "nested_playlists"
            )

            log_skala(
                "Nested playlist discovered",
                parent=source_url,
                child=entry_url,
                depth=depth + 1,
            )

            try:

                nested_data = fetch_url(
                    entry_url,
                    max_bytes=MAX_PLAYLIST_SIZE,
                    referer=source_url,
                )

                nested_content = decode_playlist(
                    nested_data
                )

                nested_entries = parse_and_expand_content(
                    nested_content,
                    entry_url,
                    depth + 1,
                    visited,
                )

                output.extend(
                    nested_entries
                )

            except Exception as exc:

                log_titan(
                    "Nested playlist processing failed",
                    level="WARNING",
                    url=entry_url,
                    error=repr(exc),
                )

                # Keep original record if expansion fails.

                output.append(entry)

        else:

            output.append(entry)

    return output


# =============================================================================
# 17. DONOR DOWNLOAD AND PARSING
# =============================================================================

def download_and_parse_source(
    url,
    parse_executor,
):

    log_skala(
        "Donor download started",
        source=url,
    )

    data = fetch_url(
        url,
        max_bytes=MAX_PLAYLIST_SIZE,
        referer=INDEX_URL,
    )

    counter_add(
        "sources_downloaded"
    )

    parse_future = parse_executor.submit(
        parse_and_expand_content,
        decode_playlist(data),
        url,
        0,
        set(),
    )

    entries = parse_future.result()

    accepted = []
    filtered = 0

    for entry in entries:

        if is_blacklisted(entry):

            filtered += 1

            continue

        accepted.append(entry)

    counter_add(
        "adult_filtered",
        filtered,
    )

    log_skala(
        "Donor download and parsing completed",
        source=url,
        parsed=len(entries),
        adult_filtered=filtered,
        accepted=len(accepted),
    )

    return accepted


# =============================================================================
# 18. DONOR AGGREGATION
# =============================================================================

def collect_donors(source_urls):

    all_entries = []

    log_skala(
        "Donor collection started",
        sources=len(source_urls),
        download_workers=MAX_DOWNLOAD_WORKERS,
        parse_workers=MAX_PARSE_WORKERS,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_PARSE_WORKERS,
        thread_name_prefix="Phoenix-Parser",
    ) as parse_executor:

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_DOWNLOAD_WORKERS,
            thread_name_prefix="Phoenix-Download",
        ) as download_executor:

            future_map = {
                download_executor.submit(
                    download_and_parse_source,
                    url,
                    parse_executor,
                ): url
                for url in source_urls
            }

            for future in concurrent.futures.as_completed(
                future_map
            ):

                source = future_map[future]

                try:

                    entries = future.result()

                    # No global donor record deduplication.

                    all_entries.extend(entries)

                    log_skala(
                        "Donor records appended",
                        source=source,
                        count=len(entries),
                        aggregate_count=len(all_entries),
                    )

                except Exception as exc:

                    counter_add(
                        "source_errors"
                    )

                    remember_error(
                        source,
                        exc,
                    )

                    log_skala(
                        "Donor processing failed",
                        level="ERROR",
                        source=source,
                        error=repr(exc),
                    )

                    log_titan(
                        "Donor worker exception",
                        level="ERROR",
                        source=source,
                        error=repr(exc),
                    )

    counter_add(
        "donor_entries",
        len(all_entries),
    )

    log_skala(
        "Donor aggregation completed",
        records=len(all_entries),
    )

    return all_entries


# =============================================================================
# 19. HTTP STREAM PROBE
# =============================================================================

def http_probe(
    url,
    method,
):

    response = None

    try:

        headers = {}

        if method.upper() == "GET":
            headers["Range"] = "bytes=0-4095"

        response = get_http_session().request(
            method.upper(),
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            stream=True,
        )

        status = response.status_code

        content_type = response.headers.get(
            "Content-Type",
            "",
        ).lower()

        content_length = response.headers.get(
            "Content-Length"
        )

        if method.upper() == "GET":

            try:

                sample = next(
                    response.iter_content(4096),
                    b"",
                )

            except Exception:

                sample = b""

        else:

            sample = b""

        if 200 <= status < 400:

            return True, {
                "status": status,
                "content_type": content_type,
                "content_length": content_length,
                "sample_size": len(sample),
                "final_url": response.url,
            }

        if status in (404, 410):

            return False, {
                "status": status,
                "error": "definitive_not_found",
            }

        return None, {
            "status": status,
            "error": f"http_status_{status}",
        }

    except requests.RequestException as exc:

        return None, {
            "error": repr(exc),
        }

    finally:

        if response is not None:

            try:
                response.close()
            except Exception:
                pass


def check_stream_alive_simple(url):

    head_result, head_details = http_probe(
        url,
        "HEAD",
    )

    if head_result is True:

        return True, {
            "head": head_details,
            "get": None,
            "definitive_404_410": False,
        }

    get_result, get_details = http_probe(
        url,
        "GET",
    )

    if get_result is True:

        return True, {
            "head": head_details,
            "get": get_details,
            "definitive_404_410": False,
        }

    definitive = (
        head_details.get("status") in (404, 410)
        or get_details.get("status") in (404, 410)
    )

    return None, {
        "head": head_details,
        "get": get_details,
        "definitive_404_410": definitive,
    }


# =============================================================================
# 20. FFPROBE PLATFORM DETECTION
# =============================================================================

def ffprobe_asset_for_platform():

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "linux":

        if machine in (
            "x86_64",
            "amd64",
        ):

            return (
                "ffmpeg-master-latest-linux64-gpl.tar.xz",
                "tar.xz",
            )

        if machine in (
            "aarch64",
            "arm64",
        ):

            return (
                "ffmpeg-master-latest-linuxarm64-gpl.tar.xz",
                "tar.xz",
            )

    if system == "windows":

        if machine in (
            "amd64",
            "x86_64",
        ):

            return (
                "ffmpeg-master-latest-win64-gpl.zip",
                "zip",
            )

    raise RuntimeError(
        f"Unsupported FFprobe platform: {system}/{machine}"
    )


# =============================================================================
# 21. FFPROBE BINARY VALIDATION
# =============================================================================

def verify_ffprobe_binary(path):

    path = Path(path)

    if not path.is_file():
        return False

    try:

        result = subprocess.run(
            [str(path), "-version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        output = (
            result.stdout
            + result.stderr
        ).lower()

        return (
            result.returncode == 0
            and "ffprobe version" in output
        )

    except Exception:
        return False


# =============================================================================
# 22. FFPROBE ARCHIVE DOWNLOAD
# =============================================================================

def download_binary_file(
    url,
    destination,
    max_size,
):

    destination = Path(destination)

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    total = 0

    with requests.get(
        url,
        stream=True,
        timeout=FFPROBE_DOWNLOAD_TIMEOUT,
        headers={
            "User-Agent": "PhoenixHub-Baryshnikov"
        },
    ) as response:

        response.raise_for_status()

        declared_size = response.headers.get(
            "Content-Length"
        )

        if declared_size:

            if int(declared_size) > max_size:

                raise ValueError(
                    "FFprobe archive exceeds size limit"
                )

        with destination.open("wb") as output:

            for chunk in response.iter_content(
                1024 * 1024
            ):

                if not chunk:
                    continue

                total += len(chunk)

                if total > max_size:

                    raise ValueError(
                        "FFprobe download exceeded size limit"
                    )

                output.write(chunk)

    return total


# =============================================================================
# 23. FFPROBE SHA-256 VERIFICATION
# =============================================================================

def download_and_verify_ffprobe_archive(
    asset_name,
    archive_path,
):

    base_url = (
        "https://github.com/BtbN/FFmpeg-Builds/"
        "releases/latest/download/"
    )

    checksum_url = (
        base_url + "checksums.sha256"
    )

    archive_url = (
        base_url + asset_name
    )

    log_skala(
        "FFprobe automatic download started",
        asset=asset_name,
    )

    checksum_response = requests.get(
        checksum_url,
        timeout=30,
        headers={
            "User-Agent": "PhoenixHub-Baryshnikov"
        },
    )

    checksum_response.raise_for_status()

    expected_hash = None

    for line in checksum_response.text.splitlines():

        parts = line.strip().split()

        if len(parts) < 2:
            continue

        filename = parts[-1].lstrip("*")

        if filename == asset_name:

            expected_hash = parts[0].lower()

            break

    if not expected_hash or not re.fullmatch(
        r"[a-f0-9]{64}",
        expected_hash,
    ):

        raise RuntimeError(
            f"SHA-256 not found for {asset_name}"
        )

    downloaded = download_binary_file(
        archive_url,
        archive_path,
        MAX_FFPROBE_ARCHIVE_SIZE,
    )

    digest = hashlib.sha256()

    with Path(archive_path).open("rb") as source:

        for chunk in iter(
            lambda: source.read(1024 * 1024),
            b"",
        ):

            digest.update(chunk)

    actual_hash = digest.hexdigest()

    if actual_hash != expected_hash:

        raise RuntimeError(
            "FFprobe archive SHA-256 mismatch: "
            f"expected={expected_hash}, "
            f"actual={actual_hash}"
        )

    log_titan(
        "FFprobe archive SHA-256 verified",
        asset=asset_name,
        sha256=actual_hash,
        bytes=downloaded,
    )

    return archive_path


# =============================================================================
# 24. FFPROBE ARCHIVE EXTRACTION
# =============================================================================

def extract_ffprobe(
    archive_path,
    archive_type,
):

    FFPROBE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    executable_name = (
        "ffprobe.exe"
        if platform.system().lower() == "windows"
        else "ffprobe"
    )

    temporary_binary = FFPROBE_DIR / (
        executable_name + ".download"
    )

    found = False

    if archive_type == "tar.xz":

        with tarfile.open(
            archive_path,
            mode="r:xz",
        ) as archive:

            for member in archive.getmembers():

                if not member.isfile():
                    continue

                if Path(member.name).name != executable_name:
                    continue

                normalized = member.name.replace(
                    "\\",
                    "/",
                )

                if not normalized.endswith(
                    "/bin/" + executable_name
                ):
                    continue

                source = archive.extractfile(
                    member
                )

                if source is None:
                    continue

                with source:

                    with temporary_binary.open("wb") as output:

                        shutil.copyfileobj(
                            source,
                            output,
                        )

                found = True

                break

    elif archive_type == "zip":

        with zipfile.ZipFile(
            archive_path
        ) as archive:

            for member in archive.infolist():

                normalized = member.filename.replace(
                    "\\",
                    "/",
                )

                if not normalized.endswith(
                    "/bin/" + executable_name
                ):
                    continue

                if member.is_dir():
                    continue

                with archive.open(member) as source:

                    with temporary_binary.open("wb") as output:

                        shutil.copyfileobj(
                            source,
                            output,
                        )

                found = True

                break

    if not found:

        temporary_binary.unlink(
            missing_ok=True
        )

        raise RuntimeError(
            "ffprobe executable not found in archive"
        )

    if platform.system().lower() != "windows":

        temporary_binary.chmod(0o755)

    final_path = (
        FFPROBE_DIR / executable_name
    )

    temporary_binary.replace(
        final_path
    )

    if not verify_ffprobe_binary(final_path):

        final_path.unlink(
            missing_ok=True
        )

        raise RuntimeError(
            "Downloaded FFprobe failed version check"
        )

    return final_path


# =============================================================================
# 25. AUTOMATIC FFPROBE INSTALLER
# =============================================================================

def ensure_ffprobe():

    global FFPROBE_BIN

    if not ENABLE_FFPROBE:

        log_skala(
            "FFprobe disabled by configuration",
            level="WARNING",
        )

        return False

    with FFPROBE_INSTALL_LOCK:

        # -------------------------------------------------------------
        # 1. Explicitly configured binary.
        # -------------------------------------------------------------

        if FFPROBE_CONFIGURED:

            configured = (
                shutil.which(FFPROBE_CONFIGURED)
                or FFPROBE_CONFIGURED
            )

            if verify_ffprobe_binary(configured):

                FFPROBE_BIN = str(configured)

                log_skala(
                    "Using configured FFprobe",
                    path=FFPROBE_BIN,
                )

                return True

            log_titan(
                "Configured FFprobe unavailable",
                level="WARNING",
                path=FFPROBE_CONFIGURED,
            )

        # -------------------------------------------------------------
        # 2. System FFprobe.
        # -------------------------------------------------------------

        system_binary = shutil.which(
            "ffprobe"
        )

        if (
            system_binary
            and verify_ffprobe_binary(system_binary)
        ):

            FFPROBE_BIN = system_binary

            log_skala(
                "System FFprobe detected",
                path=FFPROBE_BIN,
            )

            return True

        # -------------------------------------------------------------
        # 3. Previously downloaded FFprobe.
        # -------------------------------------------------------------

        executable_name = (
            "ffprobe.exe"
            if platform.system().lower() == "windows"
            else "ffprobe"
        )

        cached_binary = (
            FFPROBE_DIR / executable_name
        )

        if verify_ffprobe_binary(cached_binary):

            FFPROBE_BIN = str(cached_binary)

            counter_add(
                "ffprobe_cache_hits"
            )

            log_skala(
                "Cached FFprobe reused",
                path=FFPROBE_BIN,
            )

            return True

        # -------------------------------------------------------------
        # 4. Automatic installation.
        # -------------------------------------------------------------

        if not AUTO_INSTALL_FFPROBE:

            log_skala(
                "FFprobe missing and auto-install disabled",
                level="WARNING",
            )

            return False

        archive_path = None

        try:

            asset_name, archive_type = (
                ffprobe_asset_for_platform()
            )

            FFPROBE_DIR.mkdir(
                parents=True,
                exist_ok=True,
            )

            archive_path = (
                FFPROBE_DIR / asset_name
            )

            download_and_verify_ffprobe_archive(
                asset_name,
                archive_path,
            )

            installed = extract_ffprobe(
                archive_path,
                archive_type,
            )

            FFPROBE_BIN = str(installed)

            counter_add(
                "ffprobe_downloads"
            )

            version_result = subprocess.run(
                [FFPROBE_BIN, "-version"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )

            log_skala(
                "FFprobe installed successfully",
                path=FFPROBE_BIN,
            )

            log_titan(
                "FFprobe installation completed",
                path=FFPROBE_BIN,
                version=version_result.stdout.splitlines()[:2],
            )

            return True

        except Exception as exc:

            log_skala(
                "FFprobe automatic installation failed",
                level="ERROR",
                error=repr(exc),
            )

            log_titan(
                "FFprobe installer exception",
                level="ERROR",
                error=repr(exc),
            )

            return False

        finally:

            if archive_path is not None:

                try:
                    archive_path.unlink(
                        missing_ok=True
                    )
                except Exception:
                    pass


# =============================================================================
# 26. FFPROBE STREAM VALIDATION
# =============================================================================

def check_stream_quality_ffprobe(url):

    if not ENABLE_FFPROBE:

        return None, {
            "ffprobe": "disabled",
        }

    if not verify_ffprobe_binary(FFPROBE_BIN):

        counter_add(
            "ffprobe_errors"
        )

        return None, {
            "ffprobe": "binary_unavailable",
        }

    command = [
        FFPROBE_BIN,
        "-hide_banner",
        "-v",
        "error",
        "-rw_timeout",
        "10000000",
        "-read_intervals",
        f"%+{FFPROBE_READ_SECONDS}",
        "-show_entries",
        "stream=codec_type,codec_name,width,height",
        "-of",
        "json",
        url,
    ]

    try:

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT,
            check=False,
        )

        if result.returncode != 0:

            counter_add(
                "ffprobe_errors"
            )

            return None, {
                "ffprobe": "inconclusive",
                "returncode": result.returncode,
                "stderr": result.stderr[-1000:],
            }

        payload = json.loads(
            result.stdout or "{}"
        )

        streams = payload.get(
            "streams",
            [],
        )

        media_streams = [
            stream
            for stream in streams
            if stream.get("codec_type") in (
                "audio",
                "video",
            )
        ]

        has_media = bool(
            media_streams
        )

        return (
            True if has_media else False,
            {
                "ffprobe": (
                    "media_found"
                    if has_media
                    else "no_media"
                ),
                "stream_count": len(streams),
                "media_streams": media_streams,
            },
        )

    except FileNotFoundError:

        counter_add(
            "ffprobe_errors"
        )

        return None, {
            "ffprobe": "not_installed",
        }

    except subprocess.TimeoutExpired:

        counter_add(
            "ffprobe_errors"
        )

        return None, {
            "ffprobe": "timeout",
        }

    except Exception as exc:

        counter_add(
            "ffprobe_errors"
        )

        return None, {
            "ffprobe": "inconclusive",
            "error": repr(exc),
        }


# =============================================================================
# 27. STREAM VALIDATION DECISION
# =============================================================================

def check_stream_all(url):

    http_result, http_details = (
        check_stream_alive_simple(url)
    )

    quality_result = None

    quality_details = {
        "ffprobe": "not_run",
    }

    if ENABLE_FFPROBE:

        quality_result, quality_details = (
            check_stream_quality_ffprobe(url)
        )

    definitive_404 = http_details.get(
        "definitive_404_410",
        False,
    )

    # FFprobe confirms actual media.

    if quality_result is True:

        return True, {
            "decision": "media_confirmed",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # HTTP works, but FFprobe explicitly detects no media.

    if (
        http_result is True
        and quality_result is False
    ):

        return False, {
            "decision": "http_ok_but_no_media",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # Definitive missing endpoint:
    # only after no successful HTTP method.

    if (
        definitive_404
        and http_result is not True
        and quality_result is not True
    ):

        return False, {
            "decision": "definitive_not_found",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # HTTP-only operation.

    if (
        http_result is True
        and not ENABLE_FFPROBE
    ):

        return True, {
            "decision": "http_only",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # Explicitly no media.

    if quality_result is False:

        return False, {
            "decision": "no_media",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # Unknown state: retain according to configuration.

    if KEEP_INCONCLUSIVE:

        return True, {
            "decision": "retain_inconclusive",
            "http": http_details,
            "ffprobe": quality_details,
        }

    return None, {
        "decision": "inconclusive",
        "http": http_details,
        "ffprobe": quality_details,
    }


# =============================================================================
# 28. VALIDATION CACHE
# =============================================================================

class CacheSlot:

    def __init__(self):

        self.event = threading.Event()

        self.result = None
        self.error = None


def check_stream_cached(url):

    key = normalize_url_for_compare(
        url
    )

    with URL_CACHE_LOCK:

        slot = URL_CACHE.get(key)

        if slot is None:

            slot = CacheSlot()

            URL_CACHE[key] = slot

            is_owner = True

        else:

            is_owner = False

            counter_add(
                "cache_hits"
            )

    if not is_owner:

        log_titan(
            "Stream validation cache wait",
            url=url,
        )

        slot.event.wait()

        if slot.result is not None:
            return slot.result

        if slot.error is not None:

            return None, {
                "decision": "cache_owner_error",
                "error": slot.error,
            }

        return None, {
            "decision": "cache_empty",
        }

    try:

        result = check_stream_all(
            url
        )

        slot.result = result

        return result

    except Exception as exc:

        slot.error = repr(exc)

        log_titan(
            "Stream cache owner failed",
            level="ERROR",
            url=url,
            error=repr(exc),
        )

        return None, {
            "decision": "validation_exception",
            "error": repr(exc),
        }

    finally:

        slot.event.set()


# =============================================================================
# 29. VALIDATION WORKER
# =============================================================================

def validate_one_entry(entry):

    url = entry["url"]

    with VALIDATION_SEMAPHORE:

        valid, details = check_stream_cached(
            url
        )

    log_titan(
        "Stream validation result",
        url=url,
        valid=valid,
        details=details,
    )

    return entry, valid, details


# =============================================================================
# 30. STABLE PLAYLIST BUILDER
# =============================================================================

def build_stable(donor_entries):

    stable_entries = []

    log_skala(
        "Stable validation started",
        candidates=len(donor_entries),
        workers=MAX_VALIDATION_WORKERS,
        ffprobe_enabled=ENABLE_FFPROBE,
        ffprobe_bin=FFPROBE_BIN,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_VALIDATION_WORKERS,
        thread_name_prefix="Phoenix-Validator",
    ) as executor:

        futures = [
            executor.submit(
                validate_one_entry,
                entry,
            )
            for entry in donor_entries
        ]

        for future in concurrent.futures.as_completed(
            futures
        ):

            try:

                entry, valid, details = future.result()

                counter_add(
                    "checked"
                )

                decision = details.get(
                    "decision",
                    "",
                )

                is_inconclusive = decision in (
                    "retain_inconclusive",
                    "inconclusive",
                    "cache_owner_error",
                    "cache_empty",
                    "validation_exception",
                )

                if is_inconclusive:

                    counter_add(
                        "inconclusive"
                    )

                if valid is True:

                    entry["status"] = (
                        "inconclusive"
                        if is_inconclusive
                        else "verified"
                    )

                    entry["validation"] = details

                    stable_entries.append(
                        entry
                    )

                    counter_add(
                        "accepted"
                    )

                elif valid is False:

                    counter_add(
                        "rejected"
                    )

                else:

                    if not is_inconclusive:

                        counter_add(
                            "inconclusive"
                        )

                    log_skala(
                        "Stream not accepted: inconclusive",
                        level="WARNING",
                        url=entry.get("url"),
                    )

            except Exception as exc:

                counter_add(
                    "rejected"
                )

                log_titan(
                    "Validation worker failed",
                    level="ERROR",
                    error=repr(exc),
                )

    log_skala(
        "Stable validation completed",
        **counter_snapshot(),
    )

    return stable_entries


# =============================================================================
# 31. PLAYLIST SERIALIZATION
# =============================================================================

def playlist_header():

    return "#EXTM3U\n"


def serialize_entry(entry):

    lines = []

    extinf = entry.get(
        "extinf",
        '#EXTINF:-1 group-title="Прочее",Без названия',
    )

    lines.append(
        extinf.rstrip()
    )

    for directive in entry.get(
        "directives",
        [],
    ):

        if directive:
            lines.append(
                directive.rstrip()
            )

    url = entry.get(
        "url",
        "",
    ).strip()

    if url:
        lines.append(url)

    return "\n".join(lines) + "\n"


# =============================================================================
# 32. ATOMIC PLAYLIST WRITER
# =============================================================================

def write_playlist(
    path,
    entries,
):

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        path.name + ".tmp"
    )

    try:

        with temporary.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:

            handle.write(
                playlist_header()
            )

            for entry in entries:

                handle.write(
                    serialize_entry(entry)
                )

            handle.flush()

            os.fsync(
                handle.fileno()
            )

        temporary.replace(path)

        log_titan(
            "Playlist file written",
            path=str(path),
            records=len(entries),
            bytes=path.stat().st_size,
        )

        return True

    except Exception as exc:

        counter_add(
            "write_errors"
        )

        try:

            temporary.unlink(
                missing_ok=True
            )

        except Exception:
            pass

        log_titan(
            "Playlist write failed",
            level="ERROR",
            path=str(path),
            error=repr(exc),
        )

        raise


# =============================================================================
# 33. OUTPUT MIRRORING
# =============================================================================

def write_all_outputs(
    donors,
    stable,
):

    destinations = (
        (
            MAIN_DONORS,
            MAIN_STABLE,
        ),
        (
            SHIRT_DONORS,
            SHIRT_STABLE,
        ),
    )

    for donor_path, stable_path in destinations:

        write_playlist(
            donor_path,
            donors,
        )

        write_playlist(
            stable_path,
            stable,
        )

        log_skala(
            "Both result playlists saved",
            donor_file=str(donor_path),
            stable_file=str(stable_path),
            donor_records=len(donors),
            stable_records=len(stable),
        )


# =============================================================================
# 34. ERROR REPORT
# =============================================================================

def write_error_report():

    with SOURCE_ERRORS_LOCK:

        errors = list(
            SOURCE_ERRORS
        )

    if not errors:
        return

    report = {
        "run_id": RUN_ID,
        "timestamp": datetime.now(
            timezone.utc
        ).astimezone().isoformat(),
        "errors": errors,
    }

    # JSON report name remains unchanged.
    # The _uz suffix is applied only to TXT reports.

    for directory in (
        MAIN_DIR,
        SHIRT_DIR,
    ):

        try:

            directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            path = (
                directory / "errors.json"
            )

            temporary = path.with_suffix(
                ".json.tmp"
            )

            temporary.write_text(
                json.dumps(
                    report,
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            temporary.replace(path)

        except Exception as exc:

            log_titan(
                "Error report write failed",
                level="WARNING",
                directory=str(directory),
                error=repr(exc),
            )


# =============================================================================
# 35. FINAL SUMMARY
# =============================================================================

def print_summary(
    donor_count,
    stable_count,
    elapsed,
):

    summary = {
        **BUILD_METADATA,
        "run_id": RUN_ID,
        **counter_snapshot(),
        "stable_entries": stable_count,
        "elapsed_seconds": round(
            elapsed,
            2,
        ),
        "parallelism": {
            "download_workers": MAX_DOWNLOAD_WORKERS,
            "parse_workers": MAX_PARSE_WORKERS,
            "validation_workers": MAX_VALIDATION_WORKERS,
        },
        "ffprobe": {
            "enabled": ENABLE_FFPROBE,
            "automatic_install": AUTO_INSTALL_FFPROBE,
            "binary": FFPROBE_BIN,
            "directory": str(FFPROBE_DIR),
        },
        "main_outputs": {
            "donors": str(MAIN_DONORS),
            "stable": str(MAIN_STABLE),
        },
        "shirt_outputs": {
            "donors": str(SHIRT_DONORS),
            "stable": str(SHIRT_STABLE),
        },
    }

    log_skala(
        "FINAL BUILD SUMMARY",
        **summary,
    )

    log_titan(
        "FINAL TECHNICAL SUMMARY",
        **summary,
    )

    print(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
        )
    )


# =============================================================================
# 36. RUNTIME DIAGNOSTICS
# =============================================================================

def runtime_diagnostics():

    diagnostics = {
        "python": sys.version,
        "platform": sys.platform,
        "machine": platform.machine(),
        "hostname": socket.gethostname(),
        "root_dir": str(ROOT_DIR),
        "main_dir": str(MAIN_DIR),
        "shirt_dir": str(SHIRT_DIR),
        "index_url": INDEX_URL,
        "download_workers": MAX_DOWNLOAD_WORKERS,
        "parse_workers": MAX_PARSE_WORKERS,
        "validation_workers": MAX_VALIDATION_WORKERS,
        "request_timeout": REQUEST_TIMEOUT,
        "max_retries": MAX_RETRIES,
        "max_playlist_depth": MAX_PLAYLIST_DEPTH,
        "ffprobe_enabled": ENABLE_FFPROBE,
        "auto_install_ffprobe": AUTO_INSTALL_FFPROBE,
        "ffprobe_bin": FFPROBE_BIN,
        "ffprobe_directory": str(FFPROBE_DIR),
        "keep_inconclusive": KEEP_INCONCLUSIVE,
        "require_media_probe": REQUIRE_MEDIA_PROBE,
    }

    log_titan(
        "Runtime diagnostics",
        **diagnostics,
    )

    return diagnostics


# =============================================================================
# 37. HTTP SESSION CLEANUP
# =============================================================================

def close_http_session():

    session = getattr(
        HTTP_LOCAL,
        "session",
        None,
    )

    if session is not None:

        try:
            session.close()
        except Exception:
            pass

        HTTP_LOCAL.session = None


# =============================================================================
# 38. MAIN EXECUTION
# =============================================================================

def main():

    global DREG

    started = time.monotonic()

    STOP_EVENT.clear()

    MAIN_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    SHIRT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    MAIN_LOG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    SHIRT_LOG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    DREG = DREGManager()

    try:

        log_skala(
            "Phoenix Hub Shirt.uz collector started",
            **BUILD_METADATA,
            run_id=RUN_ID,
        )

        runtime_diagnostics()

        # -----------------------------------------------------------------
        # 00. FFPROBE AUTOMATIC INSTALLATION.
        # -----------------------------------------------------------------

        ffprobe_ready = ensure_ffprobe()

        if ENABLE_FFPROBE and not ffprobe_ready:

            log_skala(
                "Continuing without FFprobe",
                level="WARNING",
                fallback="HTTP validation",
            )

            log_titan(
                "FFprobe unavailable; HTTP-only fallback activated",
                level="WARNING",
            )

        # -----------------------------------------------------------------
        # 01. SOURCE DISCOVERY.
        # -----------------------------------------------------------------

        source_urls = discover_playlist_urls()

        # -----------------------------------------------------------------
        # 02. DONOR DOWNLOAD AND PARSING.
        # -----------------------------------------------------------------

        donor_entries = collect_donors(
            source_urls
        )

        if not donor_entries:

            raise RuntimeError(
                "No donor records were collected"
            )

        # -----------------------------------------------------------------
        # 03. STREAM VALIDATION.
        # -----------------------------------------------------------------

        stable_entries = build_stable(
            donor_entries
        )

        # -----------------------------------------------------------------
        # 04. WRITE FOUR PLAYLIST FILES.
        # -----------------------------------------------------------------

        write_all_outputs(
            donor_entries,
            stable_entries,
        )

        # -----------------------------------------------------------------
        # 05. WRITE ERROR REPORT.
        # -----------------------------------------------------------------

        write_error_report()

        # -----------------------------------------------------------------
        # 06. FINAL SUMMARY.
        # -----------------------------------------------------------------

        elapsed = (
            time.monotonic() - started
        )

        print_summary(
            len(donor_entries),
            len(stable_entries),
            elapsed,
        )

        log_skala(
            "Phoenix Hub Shirt.uz collector completed",
            level="SUCCESS",
            donor_entries=len(donor_entries),
            stable_entries=len(stable_entries),
            elapsed_seconds=round(
                elapsed,
                2,
            ),
        )

        return 0

    except KeyboardInterrupt:

        STOP_EVENT.set()

        log_skala(
            "Collector interrupted by operator",
            level="WARNING",
        )

        log_titan(
            "KeyboardInterrupt",
            level="WARNING",
        )

        return 130

    except Exception as exc:

        log_skala(
            "Main build failed",
            level="CRITICAL",
            error=repr(exc),
        )

        log_titan(
            "Unhandled main exception",
            level="CRITICAL",
            error=repr(exc),
        )

        remember_error(
            "MAIN",
            exc,
        )

        write_error_report()

        print(
            f"[FATAL] {exc}",
            file=sys.stderr,
        )

        return 1

    finally:

        close_http_session()

        if DREG is not None:

            DREG.close()


# =============================================================================
# 39. ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    raise SystemExit(
        main()
    )
