
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
====================================================================
 PROJECT: IPTV NODE MASTER AGGREGATOR
 ENGINE:  RECURSIVE DISCOVERY ENGINE
 VERSION: 3.0.0
 OWNER:   Phoenix89S Lab

 PURPOSE:
   Recursive M3U/M3U8 catalog discovery and aggregation.

 INPUT:
   hvk.m3u (downloaded from SOURCE_URL)

 OUTPUT:
   find_output/
     01_nodes.json
     02_masters.json
     03_channels/
       channels_extinf.txt
       channels_urls.txt
       channels_inventory.json
       channels_inventory.m3u
     04_final/
       AGGREGATED_MASTER.m3u
     05_reports/
       report.txt
       errors.json
       statistics.json
     engine.log

 IMPORTANT:
   NO CHANNEL DEDUPLICATION.

   Every accepted channel occurrence is preserved:
   - Identical URLs
   - Identical names
   - Identical EXTINF records
   - Repeated entries from different catalogs

   Each channel is stored as one complete record:
       EXTINF + additional tags + stream URL

 DISCOVERY:
   - Known master URLs
   - Explicitly linked M3U/M3U8 catalogs
   - Catalog links matching known filename hints
   - Recursive discovery from downloaded catalogs

 LIMITATIONS:
   - No arbitrary port scanning
   - No hidden-path guessing
   - No random URL generation
   - No individual stream probing
   - Only catalogs explicitly referenced or configured
   - Adult content name filter enabled

 TECHNICAL:
   - No artificial master count limit
   - No artificial recursion depth limit
   - Iterative queue instead of recursive function calls
   - Per-response size protection
====================================================================
"""

from __future__ import annotations

import json
import re
import time
import hashlib
import logging
import sys

from pathlib import Path
from urllib.parse import (
    urlparse,
    urljoin,
    unquote,
    urldefrag,
)
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from collections import defaultdict, deque
from datetime import datetime, timezone


# ================================================================
# 01. CONFIGURATION
# ================================================================

ENGINE_NAME = "IPTV NODE MASTER AGGREGATOR"
ENGINE_VERSION = "3.0.0"

SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/hvk.m3u"
)

ROOT_DIR = Path.cwd()

OUTPUT_DIR = ROOT_DIR / "find_output"

CHANNELS_DIR = OUTPUT_DIR / "03_channels"
FINAL_DIR = OUTPUT_DIR / "04_final"
REPORTS_DIR = OUTPUT_DIR / "05_reports"

# None means no artificial limit.
MAX_DEPTH = None
MAX_MASTERS = None

# Maximum response size per catalog.
MAX_RESPONSE_SIZE = 128 * 1024 * 1024

REQUEST_TIMEOUT = 25
REQUEST_DELAY = 0.5
MAX_RETRIES = 3

USER_AGENT = (
    "IPTV-Node-Master-Aggregator/3.0"
)

KNOWN_MASTERS = [
    "http://91.205.196.7:8000/playlist.m3u8",
]

MASTER_HINTS = (
    "playlist",
    "master",
    "channels",
    "channel_list",
    "all_channels",
    "all_channels_list",
)

EXCLUDE_ADULT = True

BLOCKED_TERMS = (
    "adult",
    "porn",
    "porno",
    "xxx",
    "erotic",
    "18+",
    "18plus",
    "для взрослых",
    "порно",
    "эрот",
)


# ================================================================
# 02. GLOBAL REGISTRY
# ================================================================

visited_masters = set()

master_queue = deque()

master_results = []

channel_records = []

node_registry = defaultdict(lambda: {
    "host": "",
    "ports": set(),
    "master_urls": [],
    "channel_records": [],
})

errors = []

statistics = {
    "source_downloads": 0,
    "masters_discovered": 0,
    "masters_attempted": 0,
    "masters_successful": 0,
    "masters_failed": 0,
    "channels_parsed": 0,
    "channels_exported": 0,
    "channels_filtered": 0,
    "duplicate_urls_preserved": 0,
    "nested_masters_discovered": 0,
    "bytes_downloaded": 0,
    "nodes_registered": 0,
}


# ================================================================
# 03. TIME AND LOGGING
# ================================================================

def now():

    return datetime.now(
        timezone.utc
    ).isoformat()


def initialize_logging():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                OUTPUT_DIR / "engine.log",
                encoding="utf-8",
            ),
        ],
        force=True,
    )


def log(message):

    logging.info(message)


def log_error(message):

    logging.error(message)


# ================================================================
# 04. FILE SYSTEM
# ================================================================

def safe_mkdirs():

    for directory in (
        OUTPUT_DIR,
        CHANNELS_DIR,
        FINAL_DIR,
        REPORTS_DIR,
    ):

        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def atomic_write(path, content):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    temporary.write_text(
        content,
        encoding="utf-8",
        newline="\n",
    )

    temporary.replace(path)


def atomic_json(path, data):

    atomic_write(
        path,
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ) + "\n",
    )


# ================================================================
# 05. URL UTILITIES
# ================================================================

def normalize_url(url):

    return (
        str(url)
        .strip()
        .strip('"')
        .strip("'")
    )


def valid_http_url(url):

    try:

        parsed = urlparse(
            normalize_url(url)
        )

        return (
            parsed.scheme.lower() in (
                "http",
                "https",
            )
            and bool(parsed.hostname)
        )

    except Exception:

        return False


def canonical_url(url):

    parsed = urlparse(
        normalize_url(url)
    )

    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
    ).geturl()

    return urldefrag(normalized)[0]


def stable_id(value):

    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()[:20]


def absolute_url(base_url, value):

    return normalize_url(
        urljoin(
            base_url,
            normalize_url(value),
        )
    )


# ================================================================
# 06. ADULT CONTENT FILTER
# ================================================================

def is_blocked(name, attributes=None):

    if not EXCLUDE_ADULT:
        return False

    values = [str(name or "")]

    if attributes:

        for key in (
            "group-title",
            "tvg-name",
            "tvg-id",
        ):

            values.append(
                str(attributes.get(key, ""))
            )

    value = " ".join(values).casefold()

    for term in BLOCKED_TERMS:

        if term in (
            "для взрослых",
            "порно",
            "эрот",
            "18+",
        ):

            if term in value:
                return True

            continue

        pattern = (
            r"(?<![\w])"
            + re.escape(term)
            + r"(?![\w])"
        )

        if re.search(pattern, value):
            return True

    return False


# ================================================================
# 07. CATALOG IDENTIFICATION
# ================================================================

def is_catalog_url(url):

    parsed = urlparse(url)

    filename = Path(
        unquote(parsed.path)
    ).name.casefold()

    if not filename.endswith(
        (".m3u", ".m3u8")
    ):
        return False

    return any(
        hint in filename
        for hint in MASTER_HINTS
    )


def is_m3u_file(url):

    parsed = urlparse(url)

    path = unquote(
        parsed.path
    ).casefold()

    return path.endswith(
        (".m3u", ".m3u8")
    )


def discover_masters(content, base_url):

    discovered = set()

    current_extinf = False

    for raw_line in content.splitlines():

        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF:"):

            current_extinf = True

            continue

        if line.startswith("#"):
            continue

        absolute = absolute_url(
            base_url,
            line,
        )

        if not valid_http_url(absolute):
            current_extinf = False
            continue

        # A URL directly following EXTINF is normally
        # a channel stream, not a standalone catalog.
        #
        # Only known catalog filename hints are accepted
        # in this position.
        if current_extinf:

            if is_catalog_url(absolute):

                discovered.add(
                    canonical_url(absolute)
                )

            current_extinf = False

            continue

        # An unpaired explicit M3U/M3U8 URL is a
        # catalog candidate regardless of filename.
        if is_m3u_file(absolute):

            discovered.add(
                canonical_url(absolute)
            )

    return discovered


# ================================================================
# 08. HTTP FETCH ENGINE
# ================================================================

def fetch_text(url):

    if not valid_http_url(url):

        raise ValueError(
            f"Invalid HTTP/HTTPS URL: {url}"
        )

    last_error = None

    for attempt in range(
        1,
        MAX_RETRIES + 2,
    ):

        request = Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "application/vnd.apple.mpegurl,"
                    "application/x-mpegURL,"
                    "text/plain,*/*"
                ),
                "Accept-Encoding": "identity",
            },
        )

        try:

            with urlopen(
                request,
                timeout=REQUEST_TIMEOUT,
            ) as response:

                final_url = response.geturl()

                if not valid_http_url(final_url):

                    raise ValueError(
                        "Invalid redirected URL"
                    )

                content_type = response.headers.get(
                    "Content-Type",
                    "",
                )

                data = response.read(
                    MAX_RESPONSE_SIZE + 1
                )

                if len(data) > MAX_RESPONSE_SIZE:

                    raise ValueError(
                        "Response exceeds size limit"
                    )

                if not data.strip():

                    raise ValueError(
                        "Empty response"
                    )

                statistics[
                    "bytes_downloaded"
                ] += len(data)

                text = data.decode(
                    "utf-8-sig",
                    errors="replace",
                )

                return (
                    text,
                    final_url,
                    content_type,
                )

        except (
            HTTPError,
            URLError,
            TimeoutError,
            OSError,
        ) as exc:

            last_error = exc

            if attempt <= MAX_RETRIES:

                delay = REQUEST_DELAY * attempt

                log(
                    f"[RETRY {attempt}] {url}"
                )

                time.sleep(delay)

    raise RuntimeError(
        f"HTTP request failed: {last_error}"
    )


# ================================================================
# 09. NODE REGISTRY
# ================================================================

def register_node(url):

    parsed = urlparse(url)

    host = parsed.hostname

    if not host:
        return

    try:

        port = parsed.port

    except ValueError:

        return

    if port is None:

        port = (
            443
            if parsed.scheme.lower() == "https"
            else 80
        )

    node = node_registry[host]

    node["host"] = host

    node["ports"].add(port)


def register_master_node(url):

    register_node(url)

    host = urlparse(url).hostname

    if not host:
        return

    records = node_registry[
        host
    ]["master_urls"]

    if url not in records:

        records.append(url)


# ================================================================
# 10. EXTINF PARSER
# ================================================================

ATTRIBUTE_PATTERN = re.compile(
    r'([\w-]+)="((?:\\.|[^"])*)"'
)


def parse_extinf(line):

    result = {
        "duration": None,
        "attributes": {},
        "name": "",
    }

    if not line.startswith("#EXTINF:"):
        return result

    payload = line[len("#EXTINF:"):]

    in_quotes = False
    comma_position = -1

    for index, char in enumerate(payload):

        if char == '"':

            in_quotes = not in_quotes

        elif char == "," and not in_quotes:

            comma_position = index

    if comma_position >= 0:

        metadata = payload[:comma_position]

        result["name"] = (
            payload[comma_position + 1:].strip()
        )

    else:

        metadata = payload

    duration_match = re.match(
        r"\s*(-?\d+(?:\.\d+)?)",
        metadata,
    )

    if duration_match:

        result["duration"] = (
            duration_match.group(1)
        )

    result["attributes"] = dict(
        ATTRIBUTE_PATTERN.findall(metadata)
    )

    return result


# ================================================================
# 11. M3U PARSER
# ================================================================

def parse_m3u(
    content,
    base_url,
    source_type="master",
):

    entries = []

    current_info = None

    extra_tags = []

    for raw_line in content.splitlines():

        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF:"):

            # A second EXTINF before a URL means
            # the previous record was incomplete.
            if current_info is not None:

                log(
                    "[PARSER] Incomplete EXTINF replaced"
                )

            current_info = line

            extra_tags = []

            continue

        if line.startswith("#"):

            if current_info is not None:

                extra_tags.append(line)

            continue

        stream_url = absolute_url(
            base_url,
            line,
        )

        if not valid_http_url(stream_url):

            current_info = None

            extra_tags = []

            continue

        # No EXTINF means this is not a channel record.
        if current_info is None:
            continue

        metadata = parse_extinf(
            current_info
        )

        name = metadata["name"]

        attributes = metadata["attributes"]

        if is_blocked(
            name,
            attributes,
        ):

            statistics[
                "channels_filtered"
            ] += 1

            current_info = None

            extra_tags = []

            continue

        record = {

            "record_number": 0,

            "id": stable_id(
                canonical_url(stream_url)
            ),

            "name": name,

            "url": stream_url,

            "canonical_url": canonical_url(
                stream_url
            ),

            "extinf": current_info,

            "duration": metadata["duration"],

            "attributes": attributes,

            "group_title": attributes.get(
                "group-title",
                "",
            ),

            "tvg_id": attributes.get(
                "tvg-id",
                "",
            ),

            "tvg_name": attributes.get(
                "tvg-name",
                "",
            ),

            "tvg_logo": attributes.get(
                "tvg-logo",
                "",
            ),

            "extra_tags": list(extra_tags),

            "source_master": base_url,

            "source_type": source_type,

        }

        entries.append(record)

        current_info = None

        extra_tags = []

    return entries


# ================================================================
# 12. CHANNEL REGISTRY — NO DEDUPLICATION
# ================================================================

def register_channels(entries, source_url):

    for entry in entries:

        statistics[
            "channels_parsed"
        ] += 1

        stream_url = entry["url"]

        register_node(stream_url)

        host = urlparse(
            stream_url
        ).hostname

        record = dict(entry)

        # Each occurrence gets a unique sequential number.
        record["record_number"] = (
            len(channel_records) + 1
        )

        record["sources"] = [
            source_url
        ]

        record["first_seen"] = now()

        # Deliberately no channel deduplication.
        channel_records.append(record)

        statistics[
            "channels_exported"
        ] += 1

        if host:

            node_registry[
                host
            ]["channel_records"].append(
                record["record_number"]
            )

    # Diagnostics only. Records remain untouched.
    counts = defaultdict(int)

    for record in channel_records:

        counts[
            record["canonical_url"]
        ] += 1

    statistics[
        "duplicate_urls_preserved"
    ] = sum(
        count - 1
        for count in counts.values()
        if count > 1
    )


# ================================================================
# 13. MASTER QUEUE
# ================================================================

def enqueue_master(url, depth=0, discovered_from=""):

    url = normalize_url(url)

    if not valid_http_url(url):
        return False

    key = canonical_url(url)

    if key in visited_masters:
        return False

    if MAX_DEPTH is not None:

        if depth > MAX_DEPTH:
            return False

    if MAX_MASTERS is not None:

        if (
            len(visited_masters)
            + len(master_queue)
            >= MAX_MASTERS
        ):
            return False

    queued_keys = {
        item["key"]
        for item in master_queue
    }

    if key in queued_keys:
        return False

    master_queue.append({

        "url": url,

        "key": key,

        "depth": depth,

        "discovered_from": discovered_from,

    })

    statistics[
        "masters_discovered"
    ] += 1

    return True


# ================================================================
# 14. MASTER PROCESSOR
# ================================================================

def process_master(item):

    url = item["url"]

    key = item["key"]

    depth = item["depth"]

    if key in visited_masters:
        return

    visited_masters.add(key)

    register_master_node(url)

    statistics[
        "masters_attempted"
    ] += 1

    result = {

        "id": stable_id(key),

        "url": url,

        "depth": depth,

        "discovered_from": item[
            "discovered_from"
        ],

        "status": "pending",

        "channels": 0,

        "error": None,

        "started_at": now(),

    }

    master_results.append(result)

    log(
        f"[MASTER] depth={depth} | {url}"
    )

    try:

        content, final_url, content_type = fetch_text(
            url
        )

        if "#EXTM3U" not in content[:4096]:

            raise ValueError(
                "Response is not a recognizable M3U catalog"
            )

        final_key = canonical_url(final_url)

        visited_masters.add(final_key)

        register_master_node(final_url)

        entries = parse_m3u(
            content,
            final_url,
            source_type="master",
        )

        register_channels(
            entries,
            final_url,
        )

        result["status"] = "success"

        result["channels"] = len(entries)

        result["final_url"] = final_url

        result["content_type"] = content_type

        result["finished_at"] = now()

        statistics[
            "masters_successful"
        ] += 1

        log(
            f"[OK] {len(entries)} channel records | {final_url}"
        )

        nested = discover_masters(
            content,
            final_url,
        )

        statistics[
            "nested_masters_discovered"
        ] += len(nested)

        for nested_url in sorted(nested):

            enqueue_master(
                nested_url,
                depth + 1,
                discovered_from=final_url,
            )

    except Exception as exc:

        result["status"] = "error"

        result["error"] = str(exc)

        result["finished_at"] = now()

        statistics[
            "masters_failed"
        ] += 1

        errors.append({

            "url": url,

            "depth": depth,

            "error": str(exc),

            "timestamp": now(),

        })

        log_error(
            f"[MASTER ERROR] {url} | {exc}"
        )


# ================================================================
# 15. SOURCE PROCESSOR
# ================================================================

def process_source():

    log(
        "[SOURCE] Downloading source playlist"
    )

    content, final_url, _ = fetch_text(
        SOURCE_URL
    )

    if "#EXTM3U" not in content[:4096]:

        raise ValueError(
            "Source does not contain #EXTM3U"
        )

    statistics[
        "source_downloads"
    ] += 1

    log(
        "[SOURCE] Parsing source channels"
    )

    entries = parse_m3u(
        content,
        final_url,
        source_type="source",
    )

    register_channels(
        entries,
        final_url,
    )

    log(
        f"[SOURCE] Accepted records: {len(entries)}"
    )

    discovered = discover_masters(
        content,
        final_url,
    )

    for known_url in KNOWN_MASTERS:

        if valid_http_url(known_url):

            discovered.add(
                canonical_url(known_url)
            )

    log(
        f"[DISCOVERY] Initial catalog candidates: {len(discovered)}"
    )

    for url in sorted(discovered):

        enqueue_master(
            url,
            depth=1,
            discovered_from=final_url,
        )


# ================================================================
# 16. DISCOVERY ENGINE
# ================================================================

def run_discovery():

    while master_queue:

        item = master_queue.popleft()

        key = item["key"]

        if key in visited_masters:
            continue

        if MAX_MASTERS is not None:

            if len(visited_masters) >= MAX_MASTERS:

                log(
                    "[LIMIT] Master count limit reached"
                )

                break

        process_master(item)

        if master_queue:

            time.sleep(
                REQUEST_DELAY
            )

        log(
            "[PROGRESS] "
            f"masters={statistics['masters_attempted']} "
            f"successful={statistics['masters_successful']} "
            f"channels={statistics['channels_exported']} "
            f"queue={len(master_queue)}"
        )


# ================================================================
# 17. NODE EXPORT
# ================================================================

def export_nodes():

    result = []

    for host, data in sorted(
        node_registry.items()
    ):

        result.append({

            "host": host,

            "ports": sorted(
                data["ports"]
            ),

            "master_urls": list(
                data["master_urls"]
            ),

            "channel_record_count": len(
                data["channel_records"]
            ),

            "channel_records": list(
                data["channel_records"]
            ),

        })

    statistics[
        "nodes_registered"
    ] = len(result)

    atomic_json(
        OUTPUT_DIR / "01_nodes.json",
        result,
    )

    log(
        f"[EXPORT] Nodes: {len(result)}"
    )


# ================================================================
# 18. MASTER EXPORT
# ================================================================

def export_masters():

    atomic_json(
        OUTPUT_DIR / "02_masters.json",
        master_results,
    )

    log(
        f"[EXPORT] Masters: {len(master_results)}"
    )


# ================================================================
# 19. CHANNEL EXPORT
# ================================================================

def render_record(record):

    lines = [
        record["extinf"]
    ]

    lines.extend(
        record.get("extra_tags", [])
    )

    lines.append(
        record["url"]
    )

    return "\n".join(lines)


def export_channels():

    extinf_lines = []

    url_lines = []

    for record in channel_records:

        extinf_lines.append(
            record["extinf"]
        )

        url_lines.append(
            record["url"]
        )

    atomic_write(
        CHANNELS_DIR / "channels_extinf.txt",
        "\n".join(extinf_lines) + "\n",
    )

    atomic_write(
        CHANNELS_DIR / "channels_urls.txt",
        "\n".join(url_lines) + "\n",
    )

    atomic_json(
        CHANNELS_DIR / "channels_inventory.json",
        channel_records,
    )

    m3u_lines = [
        "#EXTM3U"
    ]

    for record in channel_records:

        m3u_lines.append(
            render_record(record)
        )

    atomic_write(
        CHANNELS_DIR / "channels_inventory.m3u",
        "\n".join(m3u_lines) + "\n",
    )

    log(
        f"[EXPORT] Channel occurrences: {len(channel_records)}"
    )


# ================================================================
# 20. FINAL MASTER PLAYLIST
# ================================================================

def export_final_m3u():

    lines = [
        "#EXTM3U"
    ]

    for record in channel_records:

        # Name and URL remain inside the same record.
        lines.append(
            render_record(record)
        )

    atomic_write(
        FINAL_DIR / "AGGREGATED_MASTER.m3u",
        "\n".join(lines) + "\n",
    )

    log(
        "[EXPORT] Final master playlist written"
    )


# ================================================================
# 21. STATISTICS
# ================================================================

def export_statistics():

    statistics["unique_master_urls"] = len(
        visited_masters
    )

    statistics["total_channel_occurrences"] = len(
        channel_records
    )

    statistics["total_nodes"] = len(
        node_registry
    )

    statistics["total_errors"] = len(
        errors
    )

    statistics["adult_filter_enabled"] = (
        EXCLUDE_ADULT
    )

    statistics["completed_at"] = now()

    atomic_json(
        REPORTS_DIR / "statistics.json",
        statistics,
    )


# ================================================================
# 22. ERROR EXPORT
# ================================================================

def export_errors():

    atomic_json(
        REPORTS_DIR / "errors.json",
        errors,
    )


# ================================================================
# 23. TEXT REPORT
# ================================================================

def export_report():

    lines = [

        "=" * 68,

        ENGINE_NAME,

        f"VERSION: {ENGINE_VERSION}",

        "=" * 68,

        "",

        f"Started/completed: {now()}",

        "",

        "DISCOVERY",

        "-" * 68,

        f"Masters discovered: {statistics['masters_discovered']}",

        f"Masters attempted: {statistics['masters_attempted']}",

        f"Masters successful: {statistics['masters_successful']}",

        f"Masters failed: {statistics['masters_failed']}",

        f"Nested catalog links: {statistics['nested_masters_discovered']}",

        "",

        "CHANNELS",

        "-" * 68,

        f"Parsed: {statistics['channels_parsed']}",

        f"Exported: {statistics['channels_exported']}",

        f"Filtered by 18+ rule: {statistics['channels_filtered']}",

        f"Duplicate URLs preserved: {statistics['duplicate_urls_preserved']}",

        "",

        "NODES",

        "-" * 68,

        f"Registered nodes: {statistics['nodes_registered']}",

        "",

        "NETWORK",

        "-" * 68,

        f"Bytes downloaded: {statistics['bytes_downloaded']}",

        f"Errors: {len(errors)}",

        "",

        "FILTERS",

        "-" * 68,

        f"Adult filter enabled: {EXCLUDE_ADULT}",

        "",

        "IMPORTANT",

        "-" * 68,

        "Channel deduplication: DISABLED",

        "Master URL cycle protection: ENABLED",

        "Channel name/URL pairing: ENABLED",

        "Random port scanning: DISABLED",

        "Hidden path guessing: DISABLED",

        "Individual stream probing: DISABLED",

        "",

        "=" * 68,

    ]

    atomic_write(
        REPORTS_DIR / "report.txt",
        "\n".join(lines) + "\n",
    )


# ================================================================
# 24. FINAL VALIDATION
# ================================================================

def validate_outputs():

    required_files = [

        OUTPUT_DIR / "01_nodes.json",

        OUTPUT_DIR / "02_masters.json",

        CHANNELS_DIR / "channels_extinf.txt",

        CHANNELS_DIR / "channels_urls.txt",

        CHANNELS_DIR / "channels_inventory.json",

        CHANNELS_DIR / "channels_inventory.m3u",

        FINAL_DIR / "AGGREGATED_MASTER.m3u",

        REPORTS_DIR / "report.txt",

        REPORTS_DIR / "errors.json",

        REPORTS_DIR / "statistics.json",

    ]

    missing = []

    for path in required_files:

        if not path.exists():

            missing.append(str(path))

            continue

        if path.stat().st_size == 0:

            # Empty channel exports are allowed only when
            # there were no accepted channels.
            if (
                "channels_" in path.name
                and len(channel_records) == 0
            ):
                continue

            missing.append(str(path))

    if missing:

        for path in missing:

            log_error(
                f"[VALIDATION] Missing or empty: {path}"
            )

        raise RuntimeError(
            "Output validation failed"
        )

    if len(channel_records) > 0:

        if (
            len(channel_records)
            != statistics["channels_exported"]
        ):

            raise RuntimeError(
                "Channel record count mismatch"
            )

    log(
        "[VALIDATION] All output files checked"
    )


# ================================================================
# 25. MAIN
# ================================================================

def main():

    started = time.time()

    initialize_logging()

    safe_mkdirs()

    log("=" * 68)

    log(
        f"{ENGINE_NAME} v{ENGINE_VERSION}"
    )

    log(
        f"Started: {now()}"
    )

    log(
        "Channel deduplication: DISABLED"
    )

    log(
        "18+ filter: ENABLED"
    )

    log(
        "Master count limit: DISABLED"
    )

    log(
        "Depth limit: DISABLED"
    )

    log("=" * 68)

    try:

        process_source()

        run_discovery()

        export_nodes()

        export_masters()

        export_channels()

        export_final_m3u()

        export_statistics()

        export_errors()

        export_report()

        validate_outputs()

        elapsed = round(
            time.time() - started,
            2,
        )

        log("=" * 68)

        log(
            "[SUCCESS] Aggregation completed"
        )

        log(
            f"Masters: {statistics['masters_successful']}"
        )

        log(
            f"Channels: {statistics['channels_exported']}"
        )

        log(
            f"Filtered 18+: {statistics['channels_filtered']}"
        )

        log(
            f"Elapsed seconds: {elapsed}"
        )

        log("=" * 68)

        return 0

    except KeyboardInterrupt:

        log_error(
            "[INTERRUPTED] Execution cancelled"
        )

        return 130

    except Exception as exc:

        log_error(
            f"[FATAL] {exc}"
        )

        errors.append({

            "url": SOURCE_URL,

            "depth": 0,

            "error": str(exc),

            "timestamp": now(),

            "fatal": True,

        })

        try:

            export_errors()

            export_statistics()

        except Exception:

            pass

        return 1


# ================================================================
# 26. ENTRY POINT
# ================================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )
