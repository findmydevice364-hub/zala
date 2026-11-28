
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
CHEBUR DONOR BUILDER
============================================================

SOURCE:
Чебур.NET_2.m3u

OUTPUT:
Chebur_donor.m3u

PIPELINE:

1. DOWNLOAD                 8 WORKERS
2. M3U STRUCTURE PARSER    16 WORKERS
3. PROGRAM ORBIT DETECTOR  28 WORKERS
4. PROGRAM ORBIT BUILDER   32 WORKERS
5. PLAYLIST BUILDER        36 WORKERS

FEATURES:

- Programmed orbit range: -36 ... 0 ... +36
- Preserve real orbit streams
- Preserve original duplicates
- Preserve original channel order
- Preserve original M3U directives
- Preserve catchup metadata
- Preserve audio metadata
- Preserve VLC options
- Preserve logos and group names
- Generate JSON audit report
- Source backup
- Parallel HTTP Range download when supported

IMPORTANT:

A URL pattern is used only when an appropriate source
template has been detected.

Generated URLs are not guaranteed to be operational
until validated by the provider.
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
from urllib.request import (
    Request,
    urlopen,
)


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

# Five independent worker pools.

DOWNLOAD_WORKERS = 8

PARSER_WORKERS = 16

ORBIT_DETECT_WORKERS = 28

ORBIT_BUILD_WORKERS = 32

PLAYLIST_BUILD_WORKERS = 36

# Programmed orbit range.

MIN_ORBIT_HOURS = -36

MAX_ORBIT_HOURS = 36

# None = preserve source archive settings.
# Set a value only when provider support is confirmed.

GLOBAL_CATCHUP_DAYS = None

# Original M3U metadata is preserved.
# No universal M3U pause directive exists.


# ============================================================
# 02. REGULAR EXPRESSIONS
# ============================================================

EXTINF_RE = re.compile(
    r"^(#EXTINF:)(?P<duration>[^ ]+)"
    r"(?P<attrs>.*?),(?P<title>.*)$"
)

ATTR_RE = re.compile(
    r'([\w-]+)="([^"]*)"'
)

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

OFFSET_VALUE_RE = re.compile(
    r"^-?\d+$"
)


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

    url: str = ""

    orbit_hours: Optional[int] = None

    orbit_evidence: str = ""

    generated: bool = False


@dataclass
class ChannelGroup:

    key: str

    entries: list[Entry] = field(
        default_factory=list
    )

    present_hours: set[int] = field(
        default_factory=set
    )

    generated_hours: set[int] = field(
        default_factory=set
    )

    offset_template: Optional[Entry] = None

    positive_template: Optional[Entry] = None

    strategy: str = "none"

    strategy_note: str = ""


# ============================================================
# 04. PIPELINE STATISTICS
# ============================================================

STATS = {

    "download": {},

    "parser": {},

    "orbit_detection": {},

    "orbit_generation": {},

    "playlist_build": {}

}


def print_stage(name: str, message: str):

    print(
        f"[{name}] {message}",
        flush=True
    )


# ============================================================
# 05. HTTP DOWNLOAD
# ============================================================

def download_single(url: str) -> bytes:

    request = Request(
        url,
        headers={
            "User-Agent": HTTP_USER_AGENT
        }
    )

    with urlopen(
        request,
        timeout=REQUEST_TIMEOUT
    ) as response:

        data = response.read(
            MAX_PLAYLIST_BYTES + 1
        )

    if len(data) > MAX_PLAYLIST_BYTES:

        raise ValueError(
            "Playlist exceeds maximum size"
        )

    if not data:

        raise ValueError(
            "Downloaded playlist is empty"
        )

    return data


def download_range(
    url: str,
    start: int,
    end: int
) -> bytes:

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

            raise RuntimeError(
                "Server does not support HTTP Range"
            )

        content_range = response.headers.get(
            "Content-Range", ""
        )

        expected_prefix = (
            f"bytes {start}-{end}/"
        )

        if not content_range.startswith(
            expected_prefix
        ):

            raise RuntimeError(
                "Unexpected Content-Range"
            )

        data = response.read(
            end - start + 2
        )

    if len(data) != end - start + 1:

        raise RuntimeError(
            "Incomplete HTTP range"
        )

    return data


def download_parallel(
    url: str,
    workers: int
) -> bytes:

    """
    Try parallel HTTP Range downloads.

    If the server does not support byte ranges,
    fall back to a normal complete download.
    """

    request = Request(
        url,
        headers={
            "User-Agent": HTTP_USER_AGENT
        },
        method="HEAD"
    )

    try:

        with urlopen(
            request,
            timeout=REQUEST_TIMEOUT
        ) as response:

            size = int(
                response.headers.get(
                    "Content-Length", "0"
                )
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

        print_stage(
            "DOWNLOAD",
            "Range unavailable; using standard download"
        )

        return download_single(url)

    chunk_size = (
        size + workers - 1
    ) // workers

    ranges = []

    for i in range(workers):

        start = i * chunk_size

        end = min(
            size - 1,
            start + chunk_size - 1
        )

        if start > end:
            break

        ranges.append(
            (start, end)
        )

    try:

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers
        ) as executor:

            chunks = list(
                executor.map(
                    lambda r: download_range(
                        url,
                        r[0],
                        r[1]
                    ),
                    ranges
                )
            )

        data = b"".join(chunks)

        if len(data) != size:

            raise RuntimeError(
                "Downloaded size mismatch"
            )

        return data

    except Exception as exc:

        print_stage(
            "DOWNLOAD",
            f"Range fallback: {exc}"
        )

        return download_single(url)


def stage_download(url: str):

    started = time.time()

    print_stage(
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
            time.time() - started,
            3
        )

    }

    print_stage(
        "DOWNLOAD",
        f"Completed: {len(data)} bytes"
    )

    return data


# ============================================================
# 06. DECODING
# ============================================================

def decode_playlist(data: bytes):

    had_bom = data.startswith(
        b"\xef\xbb\xbf"
    )

    text = data.decode(
        "utf-8-sig"
    )

    newline = (
        "\r\n"
        if "\r\n" in text
        else "\n"
    )

    return text, newline, had_bom


# ============================================================
# 07. STRUCTURE PARSER
# ============================================================

def split_structure(text: str):

    lines = text.splitlines()

    preamble = []

    raw_entries = []

    current = []

    current_extinf = None

    def flush():

        nonlocal current
        nonlocal current_extinf

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

    print_stage(
        "PARSER",
        f"Parsing with {PARSER_WORKERS} workers"
    )

    preamble, raw_entries = split_structure(
        text
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=PARSER_WORKERS
    ) as executor:

        entries = list(
            executor.map(
                parse_entry,
                raw_entries
            )
        )

    STATS["parser"] = {

        "workers": PARSER_WORKERS,

        "entries": len(entries),

        "preamble_lines": len(preamble),

        "elapsed_seconds": round(
            time.time() - started,
            3
        )

    }

    print_stage(
        "PARSER",
        f"Entries parsed: {len(entries)}"
    )

    return preamble, entries


# ============================================================
# 08. EXTINF PARSING
# ============================================================

def parse_extinf(extinf: str):

    match = EXTINF_RE.match(
        extinf
    )

    if not match:

        return {}, ""

    attrs = {

        key: value

        for key, value in ATTR_RE.findall(
            match.group("attrs")
        )

    }

    return (
        attrs,
        match.group("title").strip()
    )


# ============================================================
# 09. GEO-BLOCK DETECTION
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
# 10. ORBIT NAME DETECTION
# ============================================================

def parse_name_orbit(value: str):

    for pattern in ORBIT_PATTERNS:

        match = pattern.search(
            value or ""
        )

        if match:

            hours = int(
                match.group("hours")
            )

            if match.group("sign") == "-":

                hours = -hours

            return hours, "name"

    return None, ""


# ============================================================
# 11. URL ORBIT DETECTION
# ============================================================

def parse_url_orbit(url: str):

    if not url:
        return None, ""

    parts = urlsplit(url)

    for key, value in parse_qsl(
        parts.query,
        keep_blank_values=True
    ):

        if (
            key.casefold() == "offset"
            and OFFSET_VALUE_RE.match(value)
        ):

            seconds = int(value)

            if seconds % 3600 == 0:

                return (
                    seconds // 3600,
                    "url-offset-seconds"
                )

    match = PATH_SUFFIX_RE.search(
        parts.path
    )

    if match:

        return (
            int(match.group("hours")),
            "url-path-suffix"
        )

    return None, ""


# ============================================================
# 12. ENTRY INSPECTION
# ============================================================

def inspect_entry(entry: Entry):

    if not entry.extinf:
        return entry

    attrs, title = parse_extinf(
        entry.extinf
    )

    entry.title = title

    entry.group = attrs.get(
        "group-title", ""
    )

    entry.tvg_name = attrs.get(
        "tvg-name", ""
    )

    entry.tvg_id = attrs.get(
        "tvg-id", ""
    )

    for line in entry.lines[1:]:

        candidate = line.strip()

        if (
            candidate
            and not candidate.startswith("#")
        ):

            entry.url = candidate

            break

    return entry


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
            entry.url
        )

    entry.orbit_hours = hours

    entry.orbit_evidence = evidence

    return entry


def stage_orbit_detection(entries):

    started = time.time()

    print_stage(
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

        "russian_geoblock_entries": len(
            geoblock_entries
        ),

        "recognized_orbits": len(
            orbit_entries
        ),

        "elapsed_seconds": round(
            time.time() - started,
            3
        )

    }

    print_stage(
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

        result = pattern.sub(
            "",
            result
        )

    result = re.sub(
        r"\s+",
        " ",
        result
    )

    return result.strip(
        " -+()"
    )


def channel_key(entry: Entry):

    if entry.tvg_id.strip():

        return (
            "id:"
            + entry.tvg_id.strip().casefold()
        )

    base = strip_orbit_suffix(
        entry.tvg_name or entry.title
    )

    return "name:" + base.casefold()


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

    groups = {}

    for entry in entries:

        if not entry.extinf:
            continue

        if not is_russian_geoblock(
            entry.group
        ):
            continue

        # A channel must have at least one recognized
        # programmed orbit marker.

        if entry.orbit_hours is None:
            continue

        key = channel_key(entry)

        group = groups.setdefault(
            key,
            ChannelGroup(key)
        )

        group.entries.append(entry)

        if (
            MIN_ORBIT_HOURS
            <= entry.orbit_hours
            <= MAX_ORBIT_HOURS
        ):

            group.present_hours.add(
                entry.orbit_hours
            )

    return groups


# ============================================================
# 16. URL TEMPLATE DETECTION
# ============================================================

def detect_templates(group: ChannelGroup):

    offset_template = None

    positive_template = None

    for entry in group.entries:

        if not entry.url:
            continue

        parts = urlsplit(
            entry.url
        )

        query = parse_qsl(
            parts.query,
            keep_blank_values=True
        )

        has_offset = any(

            key.casefold() == "offset"

            for key, value in query

        )

        if (
            has_offset
            and offset_template is None
        ):

            offset_template = entry

        path_match = PATH_SUFFIX_RE.search(
            parts.path
        )

        if (
            path_match
            and entry.orbit_hours is not None
            and entry.orbit_hours > 0
            and positive_template is None
        ):

            # The suffix must agree with the recognized
            # positive orbit in the channel record.

            suffix_hours = int(
                path_match.group("hours")
            )

            if suffix_hours == entry.orbit_hours:

                positive_template = entry

    group.offset_template = offset_template

    group.positive_template = positive_template

    if (
        offset_template
        and positive_template
    ):

        group.strategy = "hybrid"

        group.strategy_note = (
            "Positive versions use observed path suffix; "
            "zero and negative versions use observed offset"
        )

    elif offset_template:

        group.strategy = "offset-only"

        group.strategy_note = (
            "Observed offset URL available; "
            "positive path versions not inferred"
        )

    elif positive_template:

        group.strategy = "positive-path-only"

        group.strategy_note = (
            "Observed positive path URL available; "
            "negative and zero versions not inferred"
        )

    else:

        group.strategy = "none"

        group.strategy_note = (
            "No supported URL template detected"
        )


# ============================================================
# 17. URL TRANSFORMATION
# ============================================================

def set_query_parameter(
    url: str,
    key: str,
    value: str
):

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

                new_query.append(
                    (old_key, value)
                )

                replaced = True

        else:

            new_query.append(
                (old_key, old_value)
            )

    if not replaced:

        new_query.append(
            (key, value)
        )

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(new_query),
            parts.fragment
        )
    )


def build_offset_url(
    template: Entry,
    hours: int
):

    return set_query_parameter(
        template.url,
        "offset",
        str(hours * 3600)
    )


def build_path_url(
    template: Entry,
    hours: int
):

    if hours <= 0:
        return None

    parts = urlsplit(
        template.url
    )

    match = PATH_SUFFIX_RE.search(
        parts.path
    )

    if not match:
        return None

    new_path = (

        parts.path[:match.start("hours")]

        + str(hours)

        + parts.path[match.end("hours"):]

    )

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            new_path,
            parts.query,
            parts.fragment
        )
    )


# ============================================================
# 18. ORBIT LABEL STYLE
# ============================================================

def detect_label_style(
    group: ChannelGroup,
    hours: int
):

    for entry in group.entries:

        if entry.orbit_hours is None:
            continue

        if hours > 0 and entry.orbit_hours <= 0:
            continue

        if hours < 0 and entry.orbit_hours >= 0:
            continue

        value = (
            entry.tvg_name
            or entry.title
        )

        if re.search(
            r"\(\s*[+-]\s*\d{1,2}\s*\)",
            value
        ):

            return "parentheses"

        if re.search(
            r"[+-]\s*\d{1,2}\s*ч",
            value,
            re.I
        ):

            return "hours"

    return "hours"


def make_orbit_label(
    base: str,
    hours: int,
    style: str
):

    if hours == 0:
        return base

    if style == "parentheses":

        return f"{base} ({hours:+d})"

    return f"{base} {hours:+d}ч"


# ============================================================
# 19. EXTINF MODIFICATION
# ============================================================

def replace_extinf_attribute(
    extinf: str,
    name: str,
    value: str
):

    pattern = re.compile(
        rf'(?<![\w-]){re.escape(name)}="[^"]*"'
    )

    replacement = (
        f'{name}="{value}"'
    )

    if pattern.search(extinf):

        return pattern.sub(
            replacement,
            extinf,
            count=1
        )

    match = EXTINF_RE.match(extinf)

    if not match:
        return extinf

    prefix = extinf[:match.start("title")]

    title = extinf[match.start("title"):]

    return (
        prefix.rstrip()
        + " "
        + replacement
        + ","
        + title
    )


def replace_extinf_title(
    extinf: str,
    title: str
):

    match = EXTINF_RE.match(extinf)

    if not match:
        return extinf

    return (
        extinf[:match.start("title")]
        + title
    )


# ============================================================
# 20. CLONE CHANNEL RECORD
# ============================================================

def clone_entry(
    template: Entry,
    new_index: int,
    hours: int,
    new_url: str,
    base_name: str,
    style: str
):

    lines = list(
        template.lines
    )

    extinf = lines[0]

    label = make_orbit_label(
        base_name,
        hours,
        style
    )

    attrs, old_title = parse_extinf(
        extinf
    )

    if "tvg-name" in attrs:

        extinf = replace_extinf_attribute(
            extinf,
            "tvg-name",
            label
        )

    extinf = replace_extinf_title(
        extinf,
        label
    )

    lines[0] = extinf

    # Keep every original directive.
    # Replace only the stream URL.

    url_replaced = False

    for i in range(1, len(lines)):

        line = lines[i]

        candidate = line.strip()

        if (
            candidate
            and not candidate.startswith("#")
        ):

            leading = line[
                :len(line) - len(line.lstrip())
            ]

            lines[i] = (
                leading + new_url
            )

            url_replaced = True

            break

    if not url_replaced:

        return None

    result = Entry(

        index=new_index,

        lines=lines,

        extinf=extinf,

        generated=True

    )

    return detect_entry_orbit(result)


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

    if (
        hours > 0
        and group.positive_template is not None
    ):

        template = group.positive_template

        new_url = build_path_url(
            template,
            hours
        )

    elif (
        hours <= 0
        and group.offset_template is not None
    ):

        template = group.offset_template

        new_url = build_offset_url(
            template,
            hours
        )

    if not template or not new_url:

        return None

    style = detect_label_style(
        group,
        hours
    )

    return clone_entry(
        template,
        new_index,
        hours,
        new_url,
        base_name,
        style
    )


# ============================================================
# 22. PROGRAM ORBIT BUILDER
# ============================================================

def stage_orbit_generation(entries):

    started = time.time()

    print_stage(
        "ORBIT BUILDER",
        f"Preparing with {ORBIT_BUILD_WORKERS} workers"
    )

    groups = collect_channel_groups(
        entries
    )

    for group in groups.values():

        detect_templates(group)

    tasks = []

    next_index = len(entries)

    group_tasks = {}

    for key, group in groups.items():

        base_name = channel_base_name(
            group
        )

        missing_hours = [

            hours

            for hours in range(
                MIN_ORBIT_HOURS,
                MAX_ORBIT_HOURS + 1
            )

            if hours not in group.present_hours

        ]

        group_tasks[key] = {

            "group": group,

            "base_name": base_name,

            "requested": missing_hours,

            "created": []

        }

        for hours in missing_hours:

            # No template means no fabricated URL.

            if group.strategy == "none":
                continue

            if (
                hours > 0
                and group.positive_template is None
            ):
                continue

            if (
                hours <= 0
                and group.offset_template is None
            ):
                continue

            tasks.append(
                (
                    group,
                    hours,
                    next_index,
                    base_name
                )
            )

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

    for task, result in zip(
        tasks,
        results
    ):

        group, hours, index, base = task

        if result is None:
            continue

        generated.append(result)

        group.generated_hours.add(
            hours
        )

        group_tasks[group.key][
            "created"
        ].append(hours)

    channel_report = {}

    for key, info in group_tasks.items():

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

        channel_report[key] = {

            "channel": info["base_name"],

            "group": (
                group.entries[0].group
                if group.entries else ""
            ),

            "source_records": len(
                group.entries
            ),

            "existing_hours": sorted(
                group.present_hours
            ),

            "generated_hours": sorted(
                group.generated_hours
            ),

            "missing_hours": missing,

            "url_strategy": group.strategy,

            "strategy_note": group.strategy_note,

            "offset_template": (
                group.offset_template.url
                if group.offset_template
                else None
            ),

            "positive_template": (
                group.positive_template.url
                if group.positive_template
                else None
            )

        }

    STATS["orbit_generation"] = {

        "workers": ORBIT_BUILD_WORKERS,

        "programmed_channels": len(groups),

        "generation_tasks": len(tasks),

        "generated_entries": len(generated),

        "elapsed_seconds": round(
            time.time() - started,
            3
        )

    }

    print_stage(
        "ORBIT BUILDER",
        f"Channels: {len(groups)}"
    )

    print_stage(
        "ORBIT BUILDER",
        f"Generated entries: {len(generated)}"
    )

    # Original entries remain untouched and in original order.
    # Generated entries are appended after original records.

    return (
        entries + generated,
        channel_report
    )


# ============================================================
# 23. GLOBAL CATCHUP POLICY
# ============================================================

def apply_global_catchup(
    preamble: list[str],
    days: Optional[int]
):

    """
    Preserve source settings by default.

    Optional override modifies only the #EXTM3U header.
    Does not create a server-side archive.
    """

    if days is None:
        return preamble

    updated = list(preamble)

    for i, line in enumerate(updated):

        if line.startswith("#EXTM3U"):

            if re.search(
                r'\bcatchup-days="[^"]*"',
                line
            ):

                line = re.sub(
                    r'\bcatchup-days="[^"]*"',
                    f'catchup-days="{days}"',
                    line,
                    count=1
                )

            else:

                line += (
                    f' catchup-days="{days}"'
                )

            if not re.search(
                r'\bcatchup="[^"]*"',
                line
            ):

                line += ' catchup="append"'

            updated[i] = line

            return updated

    updated.insert(
        0,
        (
            '#EXTM3U '
            f'catchup="append" '
            f'catchup-days="{days}"'
        )
    )

    return updated


# ============================================================
# 24. PLAYLIST BUILD WORKER
# ============================================================

def render_entry(entry: Entry):

    return list(
        entry.lines
    )


# ============================================================
# 25. FINAL PLAYLIST BUILDER
# ============================================================

def stage_playlist_build(
    preamble,
    entries,
    newline,
    had_bom,
    output_path
):

    started = time.time()

    print_stage(
        "PLAYLIST BUILDER",
        f"Building with {PLAYLIST_BUILD_WORKERS} workers"
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=PLAYLIST_BUILD_WORKERS
    ) as executor:

        rendered_entries = list(
            executor.map(
                render_entry,
                entries
            )
        )

    lines = list(
        preamble
    )

    for entry_lines in rendered_entries:

        lines.extend(
            entry_lines
        )

    output_text = (
        newline.join(lines)
        + newline
    )

    if had_bom:

        output_text = (
            "\ufeff" + output_text
        )

    Path(output_path).write_text(
        output_text,
        encoding="utf-8",
        newline=""
    )

    STATS["playlist_build"] = {

        "workers": PLAYLIST_BUILD_WORKERS,

        "entries": len(entries),

        "output_bytes": len(
            output_text.encode("utf-8")
        ),

        "elapsed_seconds": round(
            time.time() - started,
            3
        )

    }

    print_stage(
        "PLAYLIST BUILDER",
        f"Written: {output_path}"
    )

    return output_text


# ============================================================
# 26. AUDIT REPORT
# ============================================================

def build_report(
    source_entries,
    output_entries,
    channel_report,
    elapsed
):

    source_geoblock = [

        e for e in source_entries

        if is_russian_geoblock(e.group)

    ]

    source_orbits = [

        e for e in source_geoblock

        if e.orbit_hours is not None

    ]

    generated = [

        e for e in output_entries

        if e.generated

    ]

    outside_range = [

        e for e in source_orbits

        if not (
            MIN_ORBIT_HOURS
            <= e.orbit_hours
            <= MAX_ORBIT_HOURS
        )

    ]

    return {

        "engine": "CHEBUR DONOR BUILDER",

        "version": "3.0",

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

            "total_entries": len(
                source_entries
            ),

            "russian_geoblock_entries": len(
                source_geoblock
            ),

            "recognized_orbits": len(
                source_orbits
            ),

            "outside_requested_range": [

                {
                    "title": e.title,

                    "hours": e.orbit_hours,

                    "url": e.url

                }

                for e in outside_range

            ]

        },

        "output_statistics": {

            "total_entries": len(
                output_entries
            ),

            "generated_entries": len(
                generated
            )

        },

        "programmed_channels": channel_report,

        "global_settings": {

            "catchup_header_override": (
                GLOBAL_CATCHUP_DAYS
            ),

            "original_catchup_preserved": True,

            "original_pause_metadata_preserved": True,

            "original_audio_metadata_preserved": True,

            "original_vlc_options_preserved": True,

            "real_orbits_preserved": True,

            "duplicates_preserved": True,

            "original_order_preserved": True

        },

        "worker_configuration": {

            "download": DOWNLOAD_WORKERS,

            "parser": PARSER_WORKERS,

            "orbit_detection": ORBIT_DETECT_WORKERS,

            "orbit_generation": ORBIT_BUILD_WORKERS,

            "playlist_build": PLAYLIST_BUILD_WORKERS

        },

        "pipeline_statistics": STATS,

        "elapsed_seconds": round(
            elapsed,
            3
        ),

        "warnings": [

            "Generated URLs require provider validation.",

            "Missing URL templates are not fabricated.",

            "M3U metadata cannot create server-side archive.",

            "Pause support depends on player and stream.",

            "Worker pools process playlist records; "
            "they do not create parallel audio playback."

        ]

    }


# ============================================================
# 27. MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(

        description=(
            "CHEBUR DONOR: "
            "multi-stage programmed orbit generator"
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
        "--global-catchup-days",
        type=int,
        default=GLOBAL_CATCHUP_DAYS,
        help="Optional global catchup-days header override"
    )

    parser.add_argument(
        "--no-backup",
        action="store_true"
    )

    args = parser.parse_args()

    if (
        args.global_catchup_days is not None
        and args.global_catchup_days < 1
    ):

        parser.error(
            "--global-catchup-days must be >= 1"
        )

    started = time.time()

    try:

        # ----------------------------------------------------
        # STAGE 1: DOWNLOAD
        # ----------------------------------------------------

        if args.input:

            print_stage(
                "DOWNLOAD",
                "Using local input file"
            )

            raw_data = Path(
                args.input
            ).read_bytes()

        else:

            raw_data = stage_download(
                args.url
            )

        if not raw_data:

            raise ValueError(
                "Input playlist is empty"
            )

        if len(raw_data) > MAX_PLAYLIST_BYTES:

            raise ValueError(
                "Input playlist exceeds size limit"
            )

        # ----------------------------------------------------
        # SOURCE BACKUP
        # ----------------------------------------------------

        if not args.no_backup:

            Path(BACKUP_FILE).write_bytes(
                raw_data
            )

            print_stage(
                "BACKUP",
                BACKUP_FILE
            )

        # ----------------------------------------------------
        # DECODE
        # ----------------------------------------------------

        (
            text,
            newline,
            had_bom
        ) = decode_playlist(
            raw_data
        )

        # ----------------------------------------------------
        # STAGE 2: STRUCTURE PARSER
        # ----------------------------------------------------

        preamble, entries = stage_parser(
            text
        )

        # ----------------------------------------------------
        # STAGE 3: ORBIT DETECTION
        # ----------------------------------------------------

        inspected = stage_orbit_detection(
            entries
        )

        # ----------------------------------------------------
        # STAGE 4: ORBIT GENERATION
        # ----------------------------------------------------

        (
            output_entries,
            channel_report
        ) = stage_orbit_generation(
            inspected
        )

        # ----------------------------------------------------
        # GLOBAL HEADER POLICY
        # ----------------------------------------------------

        preamble = apply_global_catchup(
            preamble,
            args.global_catchup_days
        )

        # ----------------------------------------------------
        # STAGE 5: FINAL PLAYLIST BUILD
        # ----------------------------------------------------

        stage_playlist_build(
            preamble,
            output_entries,
            newline,
            had_bom,
            args.output
        )

        # ----------------------------------------------------
        # REPORT
        # ----------------------------------------------------

        elapsed = (
            time.time() - started
        )

        report = build_report(
            inspected,
            output_entries,
            channel_report,
            elapsed
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

        print()
        print("=" * 60)
        print("CHEBUR DONOR BUILD COMPLETED")
        print("=" * 60)

        print(
            "Source entries:",
            len(inspected)
        )

        print(
            "Output entries:",
            len(output_entries)
        )

        print(
            "Generated orbit entries:",
            sum(
                1
                for e in output_entries
                if e.generated
            )
        )

        print(
            "Programmed channels:",
            len(channel_report)
        )

        print(
            "Output:",
            args.output
        )

        print(
            "Backup:",
            (
                "disabled"
                if args.no_backup
                else BACKUP_FILE
            )
        )

        print(
            "Report:",
            REPORT_FILE
        )

        print(
            "Total elapsed:",
            round(elapsed, 3),
            "seconds"
        )

        print("=" * 60)

        return 0

    except Exception as exc:

        print(
            "FATAL ERROR:",
            str(exc),
            file=sys.stderr
        )

        return 1


# ============================================================
# 28. ENTRY POINT
# ============================================================

if __name__ == "__main__":

    raise SystemExit(
        main()
    )
