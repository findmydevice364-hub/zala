
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
CHEBUR DONOR BUILDER v4.1
============================================================

SOURCE:
Чебур.NET_2.m3u

OUTPUT:
Chebur_donor.m3u

PIPELINE:
1. DOWNLOAD                 8 WORKERS
2. M3U STRUCTURE PARSER    16 WORKERS
3. ORBIT DETECTOR          28 WORKERS
4. ORBIT BUILDER           32 WORKERS
5. PLAYLIST BUILDER        36 WORKERS

FEATURES:
- Programmed orbit range: -36 ... +36
- Base channel records included in orbit groups
- Per-channel URL template detection
- Per-channel stream URL generation
- Forced catchup metadata on programmed orbits
- Global 7-day catchup header
- Preserve original real orbit records
- Preserve original duplicates and order
- Remove donation, advertising and file-link entries
- Preserve original M3U directives
- Preserve VLC and audio directives
- Preserve logos and group names
- Insert generated orbits beside their source channel
- JSON audit report
- Source backup
- Parallel HTTP Range download when supported

IMPORTANT:
Generated URLs are based on observed channel templates.
A generated URL is not proof of provider-side availability.
Archive metadata does not create server-side archive access.
============================================================
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import sys
import time

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlsplit,
    urlunsplit,
)
from urllib.request import Request, urlopen


# ============================================================
# 01. GLOBAL CONFIGURATION
# ============================================================

SOURCE_URL = (
    "https://raw.githubusercontent.com/spartvan/iptvrus/dh-pages/"
    "%D0%A7%D0%B5%D0%B1%D1%83%D1%80.NET_2.m3u"
)

OUTPUT_FILE = "Chebur_donor.m3u"
BACKUP_FILE = "playlist_original.m3u"
REPORT_FILE = "chebur_orbit_report.json"

HTTP_USER_AGENT = "Cheburashka"
REQUEST_TIMEOUT = 30
MAX_PLAYLIST_BYTES = 64 * 1024 * 1024

DOWNLOAD_WORKERS = 8
PARSER_WORKERS = 16
ORBIT_DETECT_WORKERS = 28
ORBIT_BUILD_WORKERS = 32
PLAYLIST_BUILD_WORKERS = 36

MIN_ORBIT_HOURS = -36
MAX_ORBIT_HOURS = 36

# Forced archive metadata for programmed orbits.
ORBIT_CATCHUP = "append"
ORBIT_CATCHUP_DAYS = 3
ORBIT_CATCHUP_SOURCE = (
    "?offset=-${offset}&utcstart=${timestamp}"
)

# Global playlist header.
GLOBAL_CATCHUP = "default"
GLOBAL_CATCHUP_DAYS = 7
GLOBAL_CATCHUP_CORRECTION = 0
GLOBAL_EPG_URL = "https://gabbarit.page.link/epg"

# Only Russian geoblock groups are processed for orbit generation.
# Original non-target channels remain in the playlist.
TARGET_RUSSIAN_GEOBLOCK = True


# ============================================================
# 02. REGULAR EXPRESSIONS
# ============================================================

ORBIT_PATTERNS = [
    re.compile(
        r"\(\s*(?P<sign>[+-])\s*"
        r"(?P<hours>\d{1,2})\s*\)"
    ),
    re.compile(
        r"(?<!\w)(?P<sign>[+-])\s*"
        r"(?P<hours>\d{1,2})\s*"
        r"(?:ч|час(?:а|ов)?)(?!\w)",
        re.IGNORECASE
    )
]

PATH_SUFFIX_RE = re.compile(
    r"_(?P<hours>\d{1,2})(?=/|$)"
)

ATTR_RE = re.compile(
    r'([\w-]+)="([^"]*)"'
)

OFFSET_RE = re.compile(r"^-?\d+$")

# Explicit non-channel groups and names.
JUNK_TEXT_PATTERNS = [
    "сказать спасибо",
    "поблагодарить",
    "стабильный лист",
    "поддержать автора",
    "поддержка автора",
    "донат",
    "donate",
    "donation",
    "boosty",
    "юмани",
    "yoomoney",
    "сберtips",
    "сбер tips",
    "оплатить",
    "пожертвовать",
    "telegram.me/",
    "t.me/",
]

# Known non-stream file/payment endpoints.
JUNK_URL_MARKERS = [
    "getfile.dokpub.com",
    "disk.yandex.",
    "yadi.sk",
    "boosty.to/",
    "pay.mysbertips.ru",
    "yoomoney.ru",
    "money.yandex.",
    "t.me/",
    "telegram.me/",
]


# ============================================================
# 03. DATA STRUCTURES
# ============================================================

@dataclass
class Entry:
    index: int
    lines: list[str]
    extinf: Optional[str]

    title: str = ""
    group: str = ""
    tvg_name: str = ""
    tvg_id: str = ""
    logo: str = ""
    url: str = ""

    orbit_hours: Optional[int] = None
    orbit_evidence: str = ""

    generated: bool = False
    channel_key: str = ""


@dataclass
class ChannelGroup:
    key: str

    entries: list[Entry] = field(default_factory=list)

    present_hours: set[int] = field(default_factory=set)
    generated_hours: set[int] = field(default_factory=set)

    offset_template: Optional[Entry] = None
    positive_template: Optional[Entry] = None

    strategy: str = "none"
    strategy_note: str = ""

    archive_status: str = "PRESERVED_UNVERIFIED"


STATS = {
    "download": {},
    "parser": {},
    "cleanup": {},
    "orbit_detection": {},
    "orbit_generation": {},
    "playlist_build": {}
}


def log(stage: str, message: str):
    print(f"[{stage}] {message}", flush=True)


# ============================================================
# 04. HTTP DOWNLOAD
# ============================================================

def download_single(url: str) -> bytes:
    request = Request(
        url,
        headers={"User-Agent": HTTP_USER_AGENT}
    )

    with urlopen(
        request,
        timeout=REQUEST_TIMEOUT
    ) as response:
        data = response.read(MAX_PLAYLIST_BYTES + 1)

    if not data:
        raise ValueError("Downloaded playlist is empty")

    if len(data) > MAX_PLAYLIST_BYTES:
        raise ValueError("Playlist exceeds maximum size")

    return data


def download_range(url: str, start: int, end: int) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": HTTP_USER_AGENT,
            "Range": f"bytes={start}-{end}"
        }
    )

    with urlopen(
        request,
        timeout=REQUEST_TIMEOUT
    ) as response:

        if response.status != 206:
            raise RuntimeError("HTTP Range not supported")

        content_range = response.headers.get(
            "Content-Range", ""
        )

        expected = f"bytes {start}-{end}/"

        if not content_range.startswith(expected):
            raise RuntimeError("Invalid Content-Range")

        data = response.read(end - start + 2)

    if len(data) != end - start + 1:
        raise RuntimeError("Incomplete HTTP range")

    return data


def download_parallel(url: str, workers: int) -> bytes:
    request = Request(
        url,
        headers={"User-Agent": HTTP_USER_AGENT},
        method="HEAD"
    )

    try:
        with urlopen(
            request,
            timeout=REQUEST_TIMEOUT
        ) as response:

            size = int(
                response.headers.get("Content-Length", "0")
            )

            accepts_ranges = (
                response.headers.get(
                    "Accept-Ranges", ""
                ).lower() == "bytes"
            )

    except Exception:
        size = 0
        accepts_ranges = False

    if (
        not accepts_ranges
        or size <= 0
        or size > MAX_PLAYLIST_BYTES
        or size < workers
    ):
        log(
            "DOWNLOAD",
            "Range unavailable; using one standard download"
        )
        return download_single(url)

    chunk_size = (size + workers - 1) // workers

    ranges = []

    for i in range(workers):
        start = i * chunk_size
        end = min(size - 1, start + chunk_size - 1)

        if start <= end:
            ranges.append((start, end))

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers
        ) as executor:

            chunks = list(
                executor.map(
                    lambda r: download_range(
                        url, r[0], r[1]
                    ),
                    ranges
                )
            )

        data = b"".join(chunks)

        if len(data) != size:
            raise RuntimeError("Downloaded size mismatch")

        return data

    except Exception as exc:
        log("DOWNLOAD", f"Range fallback: {exc}")
        return download_single(url)


def stage_download(url: str) -> bytes:
    started = time.time()

    log(
        "DOWNLOAD",
        f"Starting with {DOWNLOAD_WORKERS} workers"
    )

    data = download_parallel(
        url,
        DOWNLOAD_WORKERS
    )

    STATS["download"] = {
        "bytes": len(data),
        "workers": DOWNLOAD_WORKERS,
        "elapsed_seconds": round(
            time.time() - started, 3
        )
    }

    log("DOWNLOAD", f"Completed: {len(data)} bytes")

    return data


# ============================================================
# 05. DECODING
# ============================================================

def decode_playlist(data: bytes):
    had_bom = data.startswith(b"\xef\xbb\xbf")

    text = data.decode("utf-8-sig")

    newline = "\r\n" if "\r\n" in text else "\n"

    return text, newline, had_bom


# ============================================================
# 06. M3U STRUCTURE PARSER
# ============================================================

def split_structure(text: str):
    lines = text.splitlines()

    preamble = []
    raw_entries = []

    current = []
    current_extinf = None

    def flush():
        nonlocal current, current_extinf

        if current:
            raw_entries.append(
                (
                    len(raw_entries),
                    current,
                    current_extinf
                )
            )

        current = []
        current_extinf = None

    for line in lines:
        if line.startswith("#EXTINF:"):
            flush()
            current = [line]
            current_extinf = line

        elif current:
            current.append(line)

        else:
            preamble.append(line)

    flush()

    return preamble, raw_entries


def parse_entry(raw):
    index, lines, extinf = raw

    return Entry(
        index=index,
        lines=lines,
        extinf=extinf
    )


def stage_parser(text: str):
    started = time.time()

    log(
        "PARSER",
        f"Parsing with {PARSER_WORKERS} workers"
    )

    preamble, raw_entries = split_structure(text)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=PARSER_WORKERS
    ) as executor:

        entries = list(
            executor.map(parse_entry, raw_entries)
        )

    STATS["parser"] = {
        "workers": PARSER_WORKERS,
        "entries": len(entries),
        "preamble_lines": len(preamble),
        "elapsed_seconds": round(
            time.time() - started, 3
        )
    }

    log("PARSER", f"Entries parsed: {len(entries)}")

    return preamble, entries


# ============================================================
# 07. EXTINF PARSING
# ============================================================

def split_extinf(extinf: str):
    """
    Split EXTINF at the first comma outside quoted attributes.
    """

    quoted = False

    for i, char in enumerate(extinf):
        if char == '"':
            quoted = not quoted

        elif char == "," and not quoted:
            return extinf[:i], extinf[i + 1:].strip()

    return extinf, ""


def parse_extinf(extinf: str):
    if not extinf:
        return {}, ""

    head, title = split_extinf(extinf)

    attrs = {
        key: value
        for key, value in ATTR_RE.findall(head)
    }

    return attrs, title


# ============================================================
# 08. ENTRY INSPECTION
# ============================================================

def inspect_entry(entry: Entry):
    if not entry.extinf:
        return entry

    attrs, title = parse_extinf(entry.extinf)

    entry.title = title
    entry.group = attrs.get("group-title", "")
    entry.tvg_name = attrs.get("tvg-name", "")
    entry.tvg_id = attrs.get("tvg-id", "")
    entry.logo = attrs.get("tvg-logo", "")

    for line in entry.lines[1:]:
        candidate = line.strip()

        if candidate and not candidate.startswith("#"):
            entry.url = candidate
            break

    return entry


# ============================================================
# 09. NON-CHANNEL CLEANUP
# ============================================================

def is_junk_entry(entry: Entry) -> bool:
    """
    Remove explicit advertising, donation and file-link records.
    Do not remove a record merely because its stream URL is unusual.
    """

    attrs, title = parse_extinf(entry.extinf or "")

    group = attrs.get("group-title", "")
    logo = attrs.get("tvg-logo", "")

    text = " ".join([
        title,
        group,
        entry.url,
        logo
    ]).casefold()

    if any(
        marker in text
        for marker in JUNK_TEXT_PATTERNS
    ):
        return True

    if any(
        marker in entry.url.casefold()
        for marker in JUNK_URL_MARKERS
    ):
        return True

    # Empty URL is not a playable channel record.
    if not entry.url:
        return True

    return False


def stage_cleanup(entries: list[Entry]):
    started = time.time()

    kept = []
    removed = []

    for entry in entries:
        inspected = inspect_entry(entry)

        if is_junk_entry(inspected):
            removed.append({
                "index": entry.index,
                "title": entry.title,
                "group": entry.group,
                "url": entry.url
            })
        else:
            kept.append(inspected)

    STATS["cleanup"] = {
        "input_entries": len(entries),
        "kept_entries": len(kept),
        "removed_entries": len(removed),
        "removed_records": removed,
        "elapsed_seconds": round(
            time.time() - started, 3
        )
    }

    log(
        "CLEANUP",
        f"Kept {len(kept)}; removed {len(removed)} non-channel records"
    )

    return kept


# ============================================================
# 10. GEO-BLOCK DETECTION
# ============================================================

def is_russian_geoblock(group: str):
    value = (
        group.casefold()
        .replace("ё", "е")
    )

    return (
        "геоблок" in value
        and (
            "россия" in value
            or "российск" in value
        )
    )


# ============================================================
# 11. ORBIT NAME DETECTION
# ============================================================

def parse_name_orbit(value: str):
    for pattern in ORBIT_PATTERNS:
        match = pattern.search(value or "")

        if match:
            hours = int(match.group("hours"))

            if match.group("sign") == "-":
                hours = -hours

            return hours, "name"

    return None, ""


# ============================================================
# 12. URL ORBIT DETECTION
# ============================================================

def parse_url_orbit(url: str, name_hours=None):
    if not url:
        return None, ""

    parts = urlsplit(url)

    query = parse_qsl(
        parts.query,
        keep_blank_values=True
    )

    for key, value in query:
        if (
            key.casefold() == "offset"
            and OFFSET_RE.match(value)
        ):
            seconds = int(value)

            if seconds % 3600 == 0:
                return (
                    seconds // 3600,
                    "url-offset-seconds"
                )

    match = PATH_SUFFIX_RE.search(parts.path)

    if match:
        suffix_hours = int(match.group("hours"))

        # Numeric path suffix is accepted as an orbit marker
        # only when the channel name confirms the same value.
        if (
            name_hours is not None
            and suffix_hours == name_hours
            and name_hours > 0
        ):
            return suffix_hours, "url-path-suffix"

    return None, ""


# ============================================================
# 13. ORBIT DETECTION STAGE
# ============================================================

def detect_entry_orbit(entry: Entry):
    entry = inspect_entry(entry)

    hours, evidence = parse_name_orbit(
        entry.tvg_name
    )

    if hours is None:
        hours, evidence = parse_name_orbit(
            entry.title
        )

    if hours is None:
        hours, evidence = parse_url_orbit(
            entry.url,
            None
        )

    # A path suffix alone is not sufficient evidence.
    if evidence == "url-offset-seconds":
        entry.orbit_hours = hours
        entry.orbit_evidence = evidence
        return entry

    if evidence == "url-path-suffix":
        entry.orbit_hours = hours
        entry.orbit_evidence = evidence
        return entry

    # If a name marker exists, prefer it over URL marker.
    if hours is not None:
        entry.orbit_hours = hours
        entry.orbit_evidence = evidence
        return entry

    # Check URL path suffix against the name marker.
    name_hours, name_evidence = parse_name_orbit(
        entry.tvg_name or entry.title
    )

    url_hours, url_evidence = parse_url_orbit(
        entry.url,
        name_hours
    )

    if url_hours is not None:
        entry.orbit_hours = url_hours
        entry.orbit_evidence = url_evidence
    else:
        entry.orbit_hours = None
        entry.orbit_evidence = ""

    return entry


def stage_orbit_detection(entries):
    started = time.time()

    log(
        "ORBIT DETECTOR",
        f"Searching with {ORBIT_DETECT_WORKERS} workers"
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=ORBIT_DETECT_WORKERS
    ) as executor:

        detected = list(
            executor.map(
                detect_entry_orbit,
                entries
            )
        )

    geoblock_entries = [
        e for e in detected
        if is_russian_geoblock(e.group)
    ]

    orbit_entries = [
        e for e in geoblock_entries
        if e.orbit_hours is not None
    ]

    STATS["orbit_detection"] = {
        "workers": ORBIT_DETECT_WORKERS,
        "total_entries": len(detected),
        "russian_geoblock_entries": len(geoblock_entries),
        "recognized_orbits": len(orbit_entries),
        "elapsed_seconds": round(
            time.time() - started, 3
        )
    }

    log(
        "ORBIT DETECTOR",
        f"Recognized orbit records: {len(orbit_entries)}"
    )

    return detected


# ============================================================
# 14. CHANNEL IDENTITY
# ============================================================

def strip_orbit_suffix(value: str):
    result = value or ""

    for pattern in ORBIT_PATTERNS:
        result = pattern.sub("", result)

    result = re.sub(r"\s+", " ", result)

    return result.strip(" -+()")


def normalized_path(url: str):
    parts = urlsplit(url)

    path = PATH_SUFFIX_RE.sub("", parts.path)

    return path.casefold()


def normalized_host(url: str):
    return urlsplit(url).netloc.casefold()


def channel_identity(entry: Entry):
    """
    Name is used as the primary identity so that different
    tvg-id values do not split one channel's orbit family.
    """

    base = strip_orbit_suffix(
        entry.tvg_name or entry.title
    )

    return base.casefold()


def channel_key(entry: Entry):
    return "|".join([
        entry.group.casefold(),
        channel_identity(entry),
        normalized_host(entry.url),
        normalized_path(entry.url)
    ])


def channel_base_name(group: ChannelGroup):
    for entry in group.entries:
        value = strip_orbit_suffix(
            entry.tvg_name or entry.title
        )

        if value:
            return value

    return group.key


# ============================================================
# 15. CHANNEL GROUP COLLECTION
# ============================================================

def collect_channel_groups(entries):
    """
    Important:
    First collect ALL eligible channel entries, including
    unmarked base entries. Then select groups containing
    at least one recognized programmed orbit.
    """

    candidates = {}

    for entry in entries:
        if not entry.extinf or not entry.url:
            continue

        if (
            TARGET_RUSSIAN_GEOBLOCK
            and not is_russian_geoblock(entry.group)
        ):
            continue

        key = channel_key(entry)

        group = candidates.setdefault(
            key,
            ChannelGroup(key)
        )

        group.entries.append(entry)

    # Only groups with at least one orbit marker are programmed.
    programmed = {}

    for key, group in candidates.items():
        marked = [
            e for e in group.entries
            if e.orbit_hours is not None
        ]

        if not marked:
            continue

        for entry in group.entries:
            entry.channel_key = key

        for entry in marked:
            if (
                MIN_ORBIT_HOURS
                <= entry.orbit_hours
                <= MAX_ORBIT_HOURS
            ):
                group.present_hours.add(
                    entry.orbit_hours
                )

        programmed[key] = group

    return programmed


# ============================================================
# 16. URL TEMPLATE DETECTION
# ============================================================

def get_query_offset(url: str):
    parts = urlsplit(url)

    for key, value in parse_qsl(
        parts.query,
        keep_blank_values=True
    ):
        if (
            key.casefold() == "offset"
            and OFFSET_RE.match(value)
        ):
            seconds = int(value)

            if seconds % 3600 == 0:
                return seconds // 3600

    return None


def detect_templates(group: ChannelGroup):
    offset_template = None
    positive_template = None

    for entry in group.entries:
        if not entry.url:
            continue

        query_hours = get_query_offset(entry.url)

        if (
            query_hours is not None
            and entry.orbit_hours is not None
            and query_hours == entry.orbit_hours
            and offset_template is None
        ):
            offset_template = entry

        parts = urlsplit(entry.url)
        path_match = PATH_SUFFIX_RE.search(parts.path)

        if (
            path_match
            and entry.orbit_hours is not None
            and entry.orbit_hours > 0
            and int(path_match.group("hours"))
                == entry.orbit_hours
            and positive_template is None
        ):
            positive_template = entry

    group.offset_template = offset_template
    group.positive_template = positive_template

    if offset_template and positive_template:
        group.strategy = "hybrid"
        group.strategy_note = (
            "Positive hours use observed path suffix; "
            "zero and negative hours use observed query offset"
        )

    elif offset_template:
        group.strategy = "offset"
        group.strategy_note = (
            "Observed query offset template used for all hours"
        )

    elif positive_template:
        group.strategy = "positive-path"
        group.strategy_note = (
            "Observed positive path template; "
            "zero may use base path; negative hours unavailable"
        )

    else:
        group.strategy = "none"
        group.strategy_note = (
            "No matching LIVE URL template detected"
        )


# ============================================================
# 17. URL TRANSFORMATION
# ============================================================

def set_query_parameter(url: str, key: str, value: str):
    parts = urlsplit(url)

    query = parse_qsl(
        parts.query,
        keep_blank_values=True
    )

    new_query = []
    replaced = False

    for old_key, old_value in query:
        if old_key.casefold() == key.casefold():
            if not replaced:
                new_query.append((old_key, value))
                replaced = True
        else:
            new_query.append((old_key, old_value))

    if not replaced:
        new_query.append((key, value))

    return urlunsplit((
        parts.scheme,
        parts.netloc,
        parts.path,
        urlencode(new_query),
        parts.fragment
    ))


def build_offset_url(template: Entry, hours: int):
    return set_query_parameter(
        template.url,
        "offset",
        str(hours * 3600)
    )


def build_path_url(template: Entry, hours: int):
    parts = urlsplit(template.url)

    match = PATH_SUFFIX_RE.search(parts.path)

    if not match:
        return None

    if hours == 0:
        new_path = (
            parts.path[:match.start()]
            + parts.path[match.end():]
        )

    elif hours > 0:
        new_path = (
            parts.path[:match.start("hours")]
            + str(hours)
            + parts.path[match.end("hours"):]
        )

    else:
        return None

    return urlunsplit((
        parts.scheme,
        parts.netloc,
        new_path,
        parts.query,
        parts.fragment
    ))


# ============================================================
# 18. ORBIT LABEL STYLE
# ============================================================

def detect_label_style(group: ChannelGroup):
    for entry in group.entries:
        value = entry.tvg_name or entry.title

        if re.search(
            r"\(\s*[+-]\s*\d{1,2}\s*\)",
            value
        ):
            return "parentheses"

        if re.search(
            r"[+-]\s*\d{1,2}\s*ч",
            value,
            re.IGNORECASE
        ):
            return "hours"

    return "hours"


def make_orbit_label(base: str, hours: int, style: str):
    if hours == 0:
        return base

    if style == "parentheses":
        return f"{base} ({hours:+d})"

    sign = "+" if hours > 0 else ""
    return f"{base} {sign}{hours}ч"


# ============================================================
# 19. EXTINF ATTRIBUTE MODIFICATION
# ============================================================

def set_extinf_attribute(
    extinf: str,
    name: str,
    value: str
):
    head, title = split_extinf(extinf)

    pattern = re.compile(
        rf'(?<![\w-]){re.escape(name)}="[^"]*"'
    )

    replacement = f'{name}="{value}"'

    if pattern.search(head):
        head = pattern.sub(
            replacement,
            head,
            count=1
        )
    else:
        head = head.rstrip() + " " + replacement

    return head + "," + title


def set_extinf_title(extinf: str, title: str):
    head, _ = split_extinf(extinf)
    return head + "," + title


def force_orbit_archive(extinf: str):
    """
    Explicitly set the requested archive metadata.
    This changes playlist metadata, not server capabilities.
    """

    extinf = set_extinf_attribute(
        extinf,
        "catchup",
        ORBIT_CATCHUP
    )

    extinf = set_extinf_attribute(
        extinf,
        "catchup-days",
        str(ORBIT_CATCHUP_DAYS)
    )

    extinf = set_extinf_attribute(
        extinf,
        "catchup-source",
        ORBIT_CATCHUP_SOURCE
    )

    return extinf


# ============================================================
# 20. CLONE CHANNEL RECORD
# ============================================================

def clone_entry(
    template: Entry,
    new_index: int,
    hours: int,
    new_url: str,
    base_name: str,
    style: str,
    group_key: str
):
    lines = list(template.lines)

    if not lines or not lines[0].startswith("#EXTINF:"):
        return None

    extinf = lines[0]

    label = make_orbit_label(
        base_name,
        hours,
        style
    )

    attrs, _ = parse_extinf(extinf)

    if "tvg-name" in attrs:
        extinf = set_extinf_attribute(
            extinf,
            "tvg-name",
            label
        )

    extinf = set_extinf_title(
        extinf,
        label
    )

    # Every generated programmed orbit gets archive metadata.
    extinf = force_orbit_archive(extinf)

    lines[0] = extinf

    url_replaced = False

    for i in range(1, len(lines)):
        candidate = lines[i].strip()

        if candidate and not candidate.startswith("#"):
            leading = lines[i][
                :len(lines[i]) - len(lines[i].lstrip())
            ]

            lines[i] = leading + new_url
            url_replaced = True
            break

    if not url_replaced:
        return None

    result = Entry(
        index=new_index,
        lines=lines,
        extinf=extinf,
        generated=True,
        channel_key=group_key
    )

    result = inspect_entry(result)

    result.orbit_hours = hours
    result.orbit_evidence = "generated"

    return result


# ============================================================
# 21. ORBIT GENERATION TASK
# ============================================================

def generate_one_orbit(task):
    (
        group,
        hours,
        new_index,
        base_name
    ) = task

    template = None
    new_url = None

    # Hybrid: positive versions follow the observed path pattern.
    if (
        hours > 0
        and group.positive_template is not None
    ):
        template = group.positive_template

        new_url = build_path_url(
            template,
            hours
        )

    # Query-offset template supports all hours.
    elif group.offset_template is not None:
        template = group.offset_template

        new_url = build_offset_url(
            template,
            hours
        )

    # Positive path template can safely produce the zero path
    # only if the suffix is the observed orbit suffix.
    elif (
        hours == 0
        and group.positive_template is not None
    ):
        template = group.positive_template

        new_url = build_path_url(
            template,
            0
        )

    if not template or not new_url:
        return None

    style = detect_label_style(group)

    return clone_entry(
        template=template,
        new_index=new_index,
        hours=hours,
        new_url=new_url,
        base_name=base_name,
        style=style,
        group_key=group.key
    )


# ============================================================
# 22. FORCE ARCHIVE ON EXISTING ORBITS
# ============================================================

def force_archive_on_existing_orbits(entries):
    changed = 0

    for entry in entries:
        if entry.orbit_hours is None:
            continue

        if not entry.extinf:
            continue

        new_extinf = force_orbit_archive(
            entry.extinf
        )

        if new_extinf != entry.extinf:
            entry.extinf = new_extinf
            entry.lines[0] = new_extinf
            changed += 1

    return changed


# ============================================================
# 23. PROGRAM ORBIT BUILDER
# ============================================================

def stage_orbit_generation(entries):
    started = time.time()

    log(
        "ORBIT BUILDER",
        f"Preparing with {ORBIT_BUILD_WORKERS} workers"
    )

    groups = collect_channel_groups(entries)

    for group in groups.values():
        detect_templates(group)

    tasks = []
    group_info = {}

    next_index = len(entries)

    for key, group in groups.items():
        base_name = channel_base_name(group)

        missing_hours = [
            h
            for h in range(
                MIN_ORBIT_HOURS,
                MAX_ORBIT_HOURS + 1
            )
            if h not in group.present_hours
        ]

        group_info[key] = {
            "group": group,
            "base_name": base_name,
            "requested": missing_hours,
            "created": []
        }

        for hours in missing_hours:
            if group.strategy == "none":
                continue

            if (
                hours > 0
                and group.positive_template is None
                and group.offset_template is None
            ):
                continue

            if (
                hours < 0
                and group.offset_template is None
            ):
                continue

            if (
                hours == 0
                and group.offset_template is None
                and group.positive_template is None
            ):
                continue

            tasks.append((
                group,
                hours,
                next_index,
                base_name
            ))

            next_index += 1

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=ORBIT_BUILD_WORKERS
    ) as executor:

        results = list(
            executor.map(
                generate_one_orbit,
                tasks
            )
        )

    generated = []

    for task, result in zip(tasks, results):
        group, hours, _, _ = task

        if result is None:
            continue

        generated.append(result)

        group.generated_hours.add(hours)

        group_info[group.key]["created"].append(
            hours
        )

    # Force archive metadata on all original orbit records.
    archive_changed = force_archive_on_existing_orbits(
        entries
    )

    channel_report = {}

    for key, info in group_info.items():
        group = info["group"]

        missing = [
            h
            for h in range(
                MIN_ORBIT_HOURS,
                MAX_ORBIT_HOURS + 1
            )
            if (
                h not in group.present_hours
                and h not in group.generated_hours
            )
        ]

        if group.offset_template:
            archive_status = "PRESERVED_UNVERIFIED"
        else:
            archive_status = "PRESERVED_UNVERIFIED"

        channel_report[key] = {
            "channel": info["base_name"],
            "group": (
                group.entries[0].group
                if group.entries else ""
            ),
            "source_records": len(group.entries),
            "existing_hours": sorted(group.present_hours),
            "generated_hours": sorted(group.generated_hours),
            "missing_hours": missing,
            "url_strategy": group.strategy,
            "strategy_note": group.strategy_note,
            "offset_template": (
                group.offset_template.url
                if group.offset_template else None
            ),
            "positive_template": (
                group.positive_template.url
                if group.positive_template else None
            ),
            "archive": {
                "catchup": ORBIT_CATCHUP,
                "catchup_days": ORBIT_CATCHUP_DAYS,
                "catchup_source": ORBIT_CATCHUP_SOURCE,
                "status": archive_status,
                "live_offset_changes_archive": (
                    "Not verified; catchup append uses server URL behavior"
                )
            }
        }

    STATS["orbit_generation"] = {
        "workers": ORBIT_BUILD_WORKERS,
        "programmed_channels": len(groups),
        "generation_tasks": len(tasks),
        "generated_entries": len(generated),
        "existing_orbits_archive_updated": archive_changed,
        "elapsed_seconds": round(
            time.time() - started, 3
        )
    }

    log(
        "ORBIT BUILDER",
        f"Programmed channels: {len(groups)}"
    )

    log(
        "ORBIT BUILDER",
        f"Generation tasks: {len(tasks)}"
    )

    log(
        "ORBIT BUILDER",
        f"Generated entries: {len(generated)}"
    )

    log(
        "ORBIT BUILDER",
        f"Existing orbit archive records updated: {archive_changed}"
    )

    # Preserve source order and insert generated entries after
    # the last original entry of their corresponding channel.
    return (
        entries,
        generated,
        channel_report
    )


# ============================================================
# 24. GLOBAL M3U HEADER
# ============================================================

def build_global_header(preamble: list[str]):
    """
    Replace the existing EXTM3U line with the requested
    global catchup and EPG header.
    """

    header = (
        '#EXTM3U '
        f'catchup="{GLOBAL_CATCHUP}" '
        f'catchup-days="{GLOBAL_CATCHUP_DAYS}" '
        f'catchup-correction="{GLOBAL_CATCHUP_CORRECTION}" '
        f'url-tvg="{GLOBAL_EPG_URL}"'
    )

    result = []
    header_written = False

    for line in preamble:
        if line.startswith("#EXTM3U"):
            if not header_written:
                result.append(header)
                header_written = True
        else:
            result.append(line)

    if not header_written:
        result.insert(0, header)

    return result


# ============================================================
# 25. PLAYLIST BUILD WORKER
# ============================================================

def render_entry(entry: Entry):
    return list(entry.lines)


# ============================================================
# 26. ORDERED INSERTION
# ============================================================

def insert_generated_entries(
    original_entries,
    generated_entries
):
    """
    Original entries are never reordered.
    Generated entries are placed after the last original
    entry belonging to their channel group.
    """

    generated_by_key = {}

    for entry in generated_entries:
        generated_by_key.setdefault(
            entry.channel_key,
            []
        ).append(entry)

    last_index_by_key = {}

    for position, entry in enumerate(original_entries):
        if entry.channel_key:
            last_index_by_key[entry.channel_key] = position

    insert_after = {}

    for key, generated in generated_by_key.items():
        position = last_index_by_key.get(key)

        if position is None:
            continue

        generated.sort(
            key=lambda e: (
                e.orbit_hours
                if e.orbit_hours is not None
                else 0
            )
        )

        insert_after[position] = generated

    result = []

    for position, entry in enumerate(original_entries):
        result.append(entry)

        if position in insert_after:
            result.extend(insert_after[position])

    # Defensive fallback: do not silently discard generated records.
    inserted_ids = {
        id(e)
        for e in result
        if e.generated
    }

    for entry in generated_entries:
        if id(entry) not in inserted_ids:
            result.append(entry)

    return result


# ============================================================
# 27. FINAL PLAYLIST BUILDER
# ============================================================

def stage_playlist_build(
    preamble,
    original_entries,
    generated_entries,
    newline,
    had_bom,
    output_path
):
    started = time.time()

    log(
        "PLAYLIST BUILDER",
        f"Building with {PLAYLIST_BUILD_WORKERS} workers"
    )

    ordered_entries = insert_generated_entries(
        original_entries,
        generated_entries
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=PLAYLIST_BUILD_WORKERS
    ) as executor:

        rendered_entries = list(
            executor.map(
                render_entry,
                ordered_entries
            )
        )

    lines = list(preamble)

    for entry_lines in rendered_entries:
        lines.extend(entry_lines)

    output_text = newline.join(lines) + newline

    if had_bom:
        output_text = "\ufeff" + output_text

    Path(output_path).write_text(
        output_text,
        encoding="utf-8",
        newline=""
    )

    output_bytes = len(
        output_text.encode("utf-8")
    )

    STATS["playlist_build"] = {
        "workers": PLAYLIST_BUILD_WORKERS,
        "entries": len(ordered_entries),
        "generated_entries": len(generated_entries),
        "output_bytes": output_bytes,
        "output_lines": len(lines),
        "elapsed_seconds": round(
            time.time() - started, 3
        )
    }

    log(
        "PLAYLIST BUILDER",
        f"Written: {output_path}"
    )

    log(
        "PLAYLIST BUILDER",
        f"Final entries: {len(ordered_entries)}"
    )

    log(
        "PLAYLIST BUILDER",
        f"Final lines: {len(lines)}"
    )

    return output_text, ordered_entries


# ============================================================
# 28. AUDIT REPORT
# ============================================================

def build_report(
    source_entries,
    output_entries,
    channel_report,
    elapsed
):
    source_orbits = [
        e for e in source_entries
        if e.orbit_hours is not None
    ]

    generated = [
        e for e in output_entries
        if e.generated
    ]

    return {
        "engine": "CHEBUR DONOR BUILDER",
        "version": "4.1",

        "source_url": SOURCE_URL,
        "output": OUTPUT_FILE,

        "orbit_range": {
            "minimum": MIN_ORBIT_HOURS,
            "maximum": MAX_ORBIT_HOURS,
            "total_positions": (
                MAX_ORBIT_HOURS
                - MIN_ORBIT_HOURS
                + 1
            )
        },

        "source": {
            "total_entries_after_cleanup": len(source_entries),
            "recognized_orbits": len(source_orbits)
        },

        "output_statistics": {
            "total_entries": len(output_entries),
            "generated_entries": len(generated),
            "total_lines": STATS.get(
                "playlist_build", {}
            ).get("output_lines", 0),
            "increase_entries": (
                len(output_entries) - len(source_entries)
            )
        },

        "programmed_channels": channel_report,

        "global_settings": {
            "catchup": GLOBAL_CATCHUP,
            "catchup_days": GLOBAL_CATCHUP_DAYS,
            "catchup_correction": GLOBAL_CATCHUP_CORRECTION,
            "url_tvg": GLOBAL_EPG_URL,

            "orbit_catchup": ORBIT_CATCHUP,
            "orbit_catchup_days": ORBIT_CATCHUP_DAYS,
            "orbit_catchup_source": ORBIT_CATCHUP_SOURCE,

            "original_duplicates_preserved": True,
            "original_order_preserved": True,
            "original_directives_preserved": True,
            "real_orbits_preserved": True
        },

        "worker_configuration": {
            "download": DOWNLOAD_WORKERS,
            "parser": PARSER_WORKERS,
            "orbit_detection": ORBIT_DETECT_WORKERS,
            "orbit_generation": ORBIT_BUILD_WORKERS,
            "playlist_build": PLAYLIST_BUILD_WORKERS
        },

        "pipeline_statistics": STATS,

        "elapsed_seconds": round(elapsed, 3),

        "warnings": [
            "Generated URLs require provider validation.",
            "A generated URL is based on an observed channel template.",
            "Archive metadata does not create server-side archive access.",
            "With catchup=append, archive behavior depends on provider URL handling.",
            "The global seven-day setting does not override server retention limits.",
            "Only recognized Russian geoblock groups are used for orbit generation."
        ]
    }


# ============================================================
# 29. MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "CHEBUR DONOR BUILDER v4.1 - "
            "programmed orbit and archive generator"
        )
    )

    parser.add_argument(
        "--input",
        help="Local input M3U file"
    )

    parser.add_argument(
        "--url",
        default=SOURCE_URL,
        help="Source M3U URL"
    )

    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        help="Output M3U filename"
    )

    parser.add_argument(
        "--no-backup",
        action="store_true"
    )

    args = parser.parse_args()

    started = time.time()

    try:
        # ----------------------------------------------------
        # STAGE 1: DOWNLOAD
        # ----------------------------------------------------

        if args.input:
            log("DOWNLOAD", "Using local input file")

            raw_data = Path(args.input).read_bytes()

        else:
            raw_data = stage_download(args.url)

        if not raw_data:
            raise ValueError("Input playlist is empty")

        if len(raw_data) > MAX_PLAYLIST_BYTES:
            raise ValueError("Input playlist exceeds size limit")

        # ----------------------------------------------------
        # SOURCE BACKUP
        # ----------------------------------------------------

        if not args.no_backup:
            Path(BACKUP_FILE).write_bytes(raw_data)
            log("BACKUP", BACKUP_FILE)

        # ----------------------------------------------------
        # DECODE
        # ----------------------------------------------------

        text, newline, had_bom = decode_playlist(raw_data)

        # ----------------------------------------------------
        # STAGE 2: STRUCTURE PARSER
        # ----------------------------------------------------

        preamble, entries = stage_parser(text)

        # ----------------------------------------------------
        # CLEANUP NON-CHANNEL RECORDS
        # ----------------------------------------------------

        entries = stage_cleanup(entries)

        # ----------------------------------------------------
        # STAGE 3: ORBIT DETECTION
        # ----------------------------------------------------

        inspected = stage_orbit_detection(entries)

        # ----------------------------------------------------
        # STAGE 4: ORBIT GENERATION
        # ----------------------------------------------------

        (
            original_entries,
            generated_entries,
            channel_report
        ) = stage_orbit_generation(inspected)

        # ----------------------------------------------------
        # GLOBAL HEADER
        # ----------------------------------------------------

        preamble = build_global_header(preamble)

        # ----------------------------------------------------
        # STAGE 5: FINAL PLAYLIST BUILD
        # ----------------------------------------------------

        output_text, output_entries = stage_playlist_build(
            preamble=preamble,
            original_entries=original_entries,
            generated_entries=generated_entries,
            newline=newline,
            had_bom=had_bom,
            output_path=args.output
        )

        # ----------------------------------------------------
        # REPORT
        # ----------------------------------------------------

        elapsed = time.time() - started

        report = build_report(
            source_entries=original_entries,
            output_entries=output_entries,
            channel_report=channel_report,
            elapsed=elapsed
        )

        report["output"] = args.output

        Path(REPORT_FILE).write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2
            ),
            encoding="utf-8"
        )

        # ----------------------------------------------------
        # FINAL SUMMARY
        # ----------------------------------------------------

        generated_count = sum(
            1 for e in output_entries if e.generated
        )

        print()
        print("=" * 64)
        print("CHEBUR DONOR BUILDER v4.1 COMPLETED")
        print("=" * 64)

        print("Original entries:", len(original_entries))
        print("Final entries:", len(output_entries))
        print("Generated orbit entries:", generated_count)
        print("Programmed channels:", len(channel_report))

        print(
            "Removed non-channel records:",
            STATS["cleanup"].get("removed_entries", 0)
        )

        print("Output:", args.output)

        print(
            "Backup:",
            "disabled" if args.no_backup else BACKUP_FILE
        )

        print("Report:", REPORT_FILE)

        print(
            "Total elapsed:",
            round(elapsed, 3),
            "seconds"
        )

        print("=" * 64)

        return 0

    except Exception as exc:
        print(
            "FATAL ERROR:",
            str(exc),
            file=sys.stderr
        )

        return 1


# ============================================================
# 30. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    raise SystemExit(main())
