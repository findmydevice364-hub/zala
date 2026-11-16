
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

Purpose:
    Shirt.uz IPTV collector, recursive playlist parser,
    stream validator and stable playlist builder.

Outputs:
    Main/donors_uz.m3u
    Main/stable_uz.m3u
    Shirt_uz/donors_uz.m3u
    Shirt_uz/stable_uz.m3u

Journals in BOTH output directories:
    logs/DREG_COMMON.txt
    logs/DREG_SKALA_4_0.txt
    logs/DREG_TITAN_2_8.txt

Source:
    https://shirtv.uz/playlists.html

Architecture:
    SKALA 4.0 — operational journal.
    TITAN 2.8 — extended technical journal.
    DREG COMMON — duplicated journal of all events.

Notes:
    - Donor records are not globally deduplicated.
    - Duplicate source URLs are suppressed during discovery.
    - Inconclusive stream checks are configurable.
    - Playlist writes use temporary files and atomic replacement.
===============================================================================
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid

from collections import Counter
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Optional
from urllib.parse import (
    urljoin,
    urlsplit,
    urlunsplit,
    unquote,
)

import requests


# =============================================================================
# BUILD METADATA
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
# DIRECTORY STRUCTURE
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


# =============================================================================
# CONFIGURATION
# =============================================================================

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

MAX_WORKERS = max(
    1,
    int(os.getenv("MAX_WORKERS", "12")),
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

MAX_PLAYLIST_DEPTH = int(
    os.getenv("MAX_PLAYLIST_DEPTH", "2")
)

FFPROBE_BIN = os.getenv(
    "FFPROBE_BIN",
    "ffprobe",
)

FFPROBE_TIMEOUT = int(
    os.getenv("FFPROBE_TIMEOUT", "15")
)

ENABLE_FFPROBE = os.getenv(
    "ENABLE_FFPROBE",
    "true",
).lower() in {"1", "true", "yes", "on"}

# If both HTTP and FFprobe cannot determine stream status,
# retain the record as inconclusive instead of deleting it.
KEEP_INCONCLUSIVE = os.getenv(
    "KEEP_INCONCLUSIVE",
    "true",
).lower() in {"1", "true", "yes", "on"}

# A stream must produce actual media when FFprobe is enabled.
REQUIRE_MEDIA_PROBE = os.getenv(
    "REQUIRE_MEDIA_PROBE",
    "false",
).lower() in {"1", "true", "yes", "on"}

# Preserve the same source record more than once.
# No global deduplication of donor entries.
DEDUPLICATE_DONOR_RECORDS = False

# Maximum number of redirects handled by requests.
MAX_REDIRECTS = 10

# Number of seconds read by FFprobe before giving up.
FFPROBE_READ_SECONDS = 8

ADULT_PATTERNS = (
    r"\b18\+\b",
    r"\bxxx\b",
    r"\badult\b",
    r"\bporn\b",
    r"\berotic\b",
    r"для\s+взрослых",
    r"эрот",
)

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
)

PLAYLIST_LINK_PATTERN = re.compile(
    r"""(?i)(?:https?://|/|\.{1,2}/)[^\s"'<>]+
        \.(?:m3u8?|m3u\.txt)(?:\?[^\s"'<>]*)?""",
    re.VERBOSE,
)


# =============================================================================
# GLOBAL STATE
# =============================================================================

RUN_ID = uuid.uuid4().hex[:16].upper()

DREG = None

STOP_EVENT = threading.Event()

HTTP_LOCAL = threading.local()

URL_CACHE: dict[str, Any] = {}
URL_CACHE_LOCK = threading.RLock()

COUNTER_LOCK = threading.RLock()

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
    "write_errors": 0,
}

SOURCE_ERRORS = []
SOURCE_ERRORS_LOCK = threading.RLock()


def counter_add(name: str, value: int = 1):
    with COUNTER_LOCK:
        COUNTERS[name] = COUNTERS.get(name, 0) + value


def counter_snapshot():
    with COUNTER_LOCK:
        return dict(COUNTERS)


def remember_error(source, error):
    with SOURCE_ERRORS_LOCK:
        SOURCE_ERRORS.append({
            "source": str(source),
            "error": str(error),
        })


# =============================================================================
# DREG JOURNAL SYSTEM
# =============================================================================

class DREGManager:
    """
    Two-system DREG journal.

    SKALA:
        Operational events.

    TITAN:
        Extended technical events.

    COMMON:
        Receives every event from both systems.

    Each event is duplicated to both directory trees.
    A failure in one journal destination does not disable all logging.
    """

    SYSTEM_SKALA = "DREG_SKALA_4_0"
    SYSTEM_TITAN = "DREG_TITAN_2_8"

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

            for filename in (
                "DREG_COMMON.txt",
                "DREG_SKALA_4_0.txt",
                "DREG_TITAN_2_8.txt",
            ):
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
                system_file = "DREG_TITAN_2_8.txt"
            else:
                system = self.SYSTEM_SKALA
                system_file = "DREG_SKALA_4_0.txt"

            try:
                line = self._serialize(
                    system,
                    level,
                    message,
                    details,
                )
            except Exception:
                return

            for directory in (
                MAIN_LOG_DIR,
                SHIRT_LOG_DIR,
            ):
                for filename in (
                    system_file,
                    "DREG_COMMON.txt",
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

            for handle in list(self.files.values()):
                try:
                    handle.flush()
                    handle.close()
                except Exception:
                    pass

            self.files.clear()
            self.closed = True


def log_skala(message, level="INFO", **details):
    if DREG is not None:
        DREG.skala(
            message,
            level,
            **details,
        )


def log_titan(message, level="INFO", **details):
    if DREG is not None:
        DREG.titan(
            message,
            level,
            **details,
        )


# =============================================================================
# HTTP SESSION
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


def fetch_url(
    url,
    max_bytes=MAX_PLAYLIST_SIZE,
    referer=None,
):
    last_error = None

    headers = {}

    if referer:
        headers["Referer"] = referer

    for attempt in range(MAX_RETRIES + 1):
        if STOP_EVENT.is_set():
            raise RuntimeError(
                "Operation interrupted"
            )

        response = None

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

    raise RuntimeError(
        f"Unable to download {url}: {last_error}"
    )


# =============================================================================
# URL NORMALIZATION
# =============================================================================

def normalize_url_for_compare(url):
    """
    Normalize scheme and hostname.

    Preserve:
        - path case
        - query parameters
        - non-default ports
        - authentication data

    Remove:
        - fragment
        - default HTTP/HTTPS port
    """

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
        parts = urlsplit(str(url))

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
# HTML PLAYLIST DISCOVERY
# =============================================================================

class ShirtPlaylistHTMLParser(HTMLParser):
    """
    Collect playlist links from:
        href
        data-href
        data-url
        data-src
        onclick
        embedded text URLs
    """

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

        if not value:
            return

        self.links.append(value)

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)

        for key in (
            "href",
            "data-href",
            "data-url",
            "data-src",
        ):
            self._add_candidate(
                attributes.get(key)
            )

        onclick = attributes.get(
            "onclick",
            "",
        )

        if onclick:
            for match in re.findall(
                r"""['"]([^'"]+\.(?:m3u8?|m3u\.txt)(?:\?[^'"]*)?)['"]""",
                onclick,
                re.I,
            ):
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

    candidates = list(parser.links)

    # Search embedded URLs in page scripts and text.
    for match in PLAYLIST_LINK_PATTERN.findall(
        html
    ):
        candidates.append(match.strip())

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
        embedded_matches=len(
            PLAYLIST_LINK_PATTERN.findall(html)
        ),
        unique_sources=len(result),
        sources=result,
    )

    if not result:
        raise RuntimeError(
            "No M3U/M3U8 links discovered on Shirt.uz"
        )

    return result


# =============================================================================
# M3U DECODING
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
    head = content[:12000].upper()

    return (
        "#EXT-X-TARGETDURATION" in head
        or "#EXT-X-MEDIA-SEQUENCE" in head
        or "#EXT-X-PLAYLIST-TYPE" in head
        or "#EXT-X-ENDLIST" in head
        or "#EXT-X-PART" in head
    )


def is_hls_master_playlist(content):
    head = content[:12000].upper()

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


def make_default_entry(url, name):
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
        "source": url,
        "source_line": 0,
        "status": "unverified",
    }


# =============================================================================
# M3U PARSER
# =============================================================================

def parse_m3u_content(
    content,
    source_url,
):
    """
    Parse IPTV M3U.

    HLS media playlists are not expanded into individual segments.
    HLS master playlists are represented by their variant streams.
    """

    if not content:
        return []

    if not content.lstrip().startswith("#EXTM3U"):
        log_titan(
            "Source does not start with EXT M3U",
            level="WARNING",
            source=source_url,
            sample=content[:160],
        )

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

        if not is_http_url(
            urljoin(source_url, line)
        ):
            continue

        absolute_url = urljoin(
            source_url,
            line,
        )

        if pending_stream_inf:
            variant_name = (
                "HLS Variant "
                + str(len(entries) + 1)
            )

            entries.append(
                make_default_entry(
                    absolute_url,
                    variant_name,
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
# ADULT CONTENT FILTER
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
# RECURSIVE PLAYLIST EXPANSION
# =============================================================================

def download_playlist_entries(
    url,
    depth=0,
    visited=None,
):
    if visited is None:
        visited = set()

    normalized = normalize_url_for_compare(
        url
    )

    if normalized in visited:
        log_titan(
            "Recursive playlist cycle detected",
            url=url,
            depth=depth,
        )
        return []

    if depth > MAX_PLAYLIST_DEPTH:
        log_titan(
            "Playlist recursion depth limit reached",
            level="WARNING",
            url=url,
            depth=depth,
            maximum=MAX_PLAYLIST_DEPTH,
        )
        return []

    visited.add(normalized)

    data = fetch_url(
        url,
        max_bytes=MAX_PLAYLIST_SIZE,
        referer=INDEX_URL,
    )

    content = decode_playlist(data)

    if is_hls_media_playlist(content):
        entries = parse_m3u_content(
            content,
            url,
        )

        return entries

    parsed = parse_m3u_content(
        content,
        url,
    )

    output = []

    for entry in parsed:
        entry_url = entry["url"]

        if (
            is_playlist_url(entry_url)
            and depth < MAX_PLAYLIST_DEPTH
        ):
            counter_add("nested_playlists")

            log_skala(
                "Nested playlist discovered",
                parent=url,
                child=entry_url,
                depth=depth + 1,
            )

            try:
                nested = download_playlist_entries(
                    entry_url,
                    depth=depth + 1,
                    visited=visited,
                )

                output.extend(nested)

            except Exception as exc:
                log_titan(
                    "Nested playlist download failed",
                    level="WARNING",
                    url=entry_url,
                    error=repr(exc),
                )

                # Keep the original URL as a possible stream.
                output.append(entry)

        else:
            output.append(entry)

    return output


# =============================================================================
# DONOR DOWNLOAD WORKER
# =============================================================================

def download_and_parse_source(url):
    log_skala(
        "Donor download started",
        source=url,
    )

    entries = download_playlist_entries(
        url,
        depth=0,
        visited=set(),
    )

    accepted = []
    filtered = 0

    for entry in entries:
        if is_blacklisted(entry):
            filtered += 1
            continue

        accepted.append(entry)

    counter_add(
        "sources_downloaded"
    )

    counter_add(
        "adult_filtered",
        filtered,
    )

    log_skala(
        "Donor download completed",
        source=url,
        parsed=len(entries),
        adult_filtered=filtered,
        accepted=len(accepted),
    )

    return accepted


# =============================================================================
# DONOR AGGREGATION
# =============================================================================

def collect_donors(source_urls):
    all_entries = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        future_map = {
            executor.submit(
                download_and_parse_source,
                url,
            ): url
            for url in source_urls
        }

        for future in concurrent.futures.as_completed(
            future_map
        ):
            source = future_map[future]

            try:
                entries = future.result()

                # No global record deduplication.
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

    return all_entries


# =============================================================================
# HTTP STREAM PROBE
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
            "Content-Length",
        )

        if method.upper() == "GET":
            try:
                sample = next(
                    response.iter_content(
                        4096
                    ),
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

        if status in (
            404,
            410,
        ):
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
        head_details.get("status") in (
            404,
            410,
        )
        or get_details.get("status") in (
            404,
            410,
        )
    )

    return None, {
        "head": head_details,
        "get": get_details,
        "definitive_404_410": definitive,
    }


# =============================================================================
# FFPROBE
# =============================================================================

def check_stream_quality_ffprobe(url):
    if not ENABLE_FFPROBE:
        return None, {
            "ffprobe": "disabled",
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
# STREAM VALIDATION DECISION
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

    # A successful HTTP response is not sufficient if FFprobe
    # explicitly confirms that the endpoint contains no media.
    if (
        http_result is True
        and quality_result is False
    ):
        return False, {
            "decision": "http_ok_but_no_media",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # Definitive missing endpoint.
    if (
        definitive_404
        and quality_result is not True
    ):
        return False, {
            "decision": "definitive_not_found",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # HTTP success without FFprobe.
    if (
        http_result is True
        and not ENABLE_FFPROBE
    ):
        return True, {
            "decision": "http_only",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # FFprobe explicitly found no media.
    if quality_result is False:
        return False, {
            "decision": "no_media",
            "http": http_details,
            "ffprobe": quality_details,
        }

    # Unknown state: configurable retention.
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
# URL CHECK CACHE
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
            counter_add("cache_hits")

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
# STABLE STREAM WORKERS
# =============================================================================

def validate_one_entry(entry):
    url = entry["url"]

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


def build_stable(donor_entries):
    stable_entries = []

    log_skala(
        "Stable validation started",
        candidates=len(donor_entries),
        workers=MAX_WORKERS,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS
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

                counter_add("checked")

                decision = details.get(
                    "decision",
                    "",
                )

                if decision in (
                    "retain_inconclusive",
                    "inconclusive",
                    "cache_owner_error",
                    "cache_empty",
                    "validation_exception",
                ):
                    counter_add(
                        "inconclusive"
                    )

                if valid is True:
                    entry["status"] = (
                        "inconclusive"
                        if "inconclusive" in decision
                        or decision in (
                            "cache_owner_error",
                            "cache_empty",
                            "validation_exception",
                        )
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
# PLAYLIST SERIALIZATION
# =============================================================================

def playlist_header():
    return (
        "#EXTM3U\n"
    )


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
# ATOMIC PLAYLIST WRITER
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
# OUTPUT MIRRORING
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
# ERROR REPORT
# =============================================================================

def write_error_report():
    if not SOURCE_ERRORS:
        return

    report = {
        "run_id": RUN_ID,
        "timestamp": datetime.now(
            timezone.utc
        ).astimezone().isoformat(),
        "errors": list(SOURCE_ERRORS),
    }

    for directory in (
        MAIN_DIR,
        SHIRT_DIR,
    ):
        try:
            directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            path = directory / "errors.json"

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
# SUMMARY
# =============================================================================

def print_summary(
    donor_count,
    stable_count,
    elapsed,
):
    summary = {
        "run_id": RUN_ID,
        "version": VERSION,
        "version_code": VERSION_CODE,
        "build_id": BUILD_ID,
        "revision": REVISION,
        **counter_snapshot(),
        "stable_entries": stable_count,
        "elapsed_seconds": round(
            elapsed,
            2,
        ),
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
# ENVIRONMENT DIAGNOSTICS
# =============================================================================

def runtime_diagnostics():
    diagnostics = {
        "python": sys.version,
        "platform": sys.platform,
        "hostname": socket.gethostname(),
        "root_dir": str(ROOT_DIR),
        "main_dir": str(MAIN_DIR),
        "shirt_dir": str(SHIRT_DIR),
        "index_url": INDEX_URL,
        "workers": MAX_WORKERS,
        "request_timeout": REQUEST_TIMEOUT,
        "max_retries": MAX_RETRIES,
        "max_playlist_depth": MAX_PLAYLIST_DEPTH,
        "ffprobe_enabled": ENABLE_FFPROBE,
        "ffprobe_bin": FFPROBE_BIN,
        "keep_inconclusive": KEEP_INCONCLUSIVE,
    }

    log_titan(
        "Runtime diagnostics",
        **diagnostics,
    )

    return diagnostics


# =============================================================================
# MAIN
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

        # ---------------------------------------------------------------------
        # 1. Discover donor sources.
        # ---------------------------------------------------------------------

        source_urls = discover_playlist_urls()

        # ---------------------------------------------------------------------
        # 2. Download and parse donor playlists.
        # ---------------------------------------------------------------------

        donor_entries = collect_donors(
            source_urls
        )

        if not donor_entries:
            raise RuntimeError(
                "No donor records were collected"
            )

        # ---------------------------------------------------------------------
        # 3. Build stable playlist.
        # ---------------------------------------------------------------------

        stable_entries = build_stable(
            donor_entries
        )

        # ---------------------------------------------------------------------
        # 4. Save both result playlists in both directories.
        # ---------------------------------------------------------------------

        write_all_outputs(
            donor_entries,
            stable_entries,
        )

        # ---------------------------------------------------------------------
        # 5. Write diagnostic error report.
        # ---------------------------------------------------------------------

        write_error_report()

        # ---------------------------------------------------------------------
        # 6. Final summary.
        # ---------------------------------------------------------------------

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
        if DREG is not None:
            DREG.close()

        for session in (
            getattr(
                HTTP_LOCAL,
                "session",
                None,
            ),
        ):
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    raise SystemExit(
        main()
    )
