
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
====================================================================
 PROJECT: IPTV NODE MASTER AGGREGATOR
 ENGINE:  RECURSIVE DISCOVERY ENGINE
 VERSION: 2.1.0
 OWNER:   Phoenix89S Lab

 PURPOSE:
   Recursive M3U catalog discovery and aggregation.

 INPUT:
   hvk.m3u

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
   NO DEDUPLICATION.

   Every channel occurrence is preserved, including:
   - Identical URLs
   - Identical names
   - Identical EXTINF records
   - Channels discovered through different masters

 NETWORK:
   Only explicitly identified catalog URLs are fetched.
   No arbitrary port scanning.
   No hidden-path guessing.
   No individual live stream probing.
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
from urllib.request import (
    Request,
    urlopen,
)
from urllib.error import (
    HTTPError,
    URLError,
)
from collections import defaultdict
from datetime import datetime, timezone


# ================================================================
# 01. CONFIGURATION
# ================================================================

ENGINE_NAME = "IPTV NODE MASTER AGGREGATOR"
ENGINE_VERSION = "2.1.0"

SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/hvk.m3u"
)

ROOT_DIR = Path.cwd()

OUTPUT_DIR = ROOT_DIR / "find_output"

CHANNELS_DIR = OUTPUT_DIR / "03_channels"
FINAL_DIR = OUTPUT_DIR / "04_final"
REPORTS_DIR = OUTPUT_DIR / "05_reports"

MAX_DEPTH = 5
MAX_MASTERS = 100

MAX_RESPONSE_SIZE = 12 * 1024 * 1024

REQUEST_TIMEOUT = 15
REQUEST_DELAY = 1.0
MAX_RETRIES = 2

USER_AGENT = (
    "IPTV-Node-Master-Aggregator/2.1"
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
    "vixen",
    "mb_sexy",
)


# ================================================================
# 02. GLOBAL REGISTRY
# ================================================================

visited_masters = set()

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
    "masters_attempted": 0,
    "masters_successful": 0,
    "masters_failed": 0,
    "channels_parsed": 0,
    "channels_exported": 0,
    "channels_filtered": 0,
    "nested_masters_discovered": 0,
    "bytes_downloaded": 0,
    "duplicate_urls_preserved": 0,
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

    content = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
    )

    atomic_write(
        path,
        content + "\n",
    )


# ================================================================
# 05. URL UTILITIES
# ================================================================

def normalize_url(url):

    return url.strip().strip('"').strip("'")


def valid_http_url(url):

    try:

        parsed = urlparse(url)

        return (
            parsed.scheme.lower() in ("http", "https")
            and bool(parsed.hostname)
        )

    except Exception:

        return False


def canonical_url(url):

    parsed = urlparse(
        normalize_url(url)
    )

    return parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
    ).geturl()


def stable_id(value):

    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()[:20]


# ================================================================
# 06. CONTENT FILTER
# ================================================================

def is_blocked(name):

    if not EXCLUDE_ADULT:
        return False

    value = name.casefold()

    for term in BLOCKED_TERMS:

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


def discover_masters(content, base_url):

    discovered = set()

    for raw_line in content.splitlines():

        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#"):
            continue

        absolute = normalize_url(
            urljoin(base_url, line)
        )

        if not valid_http_url(absolute):
            continue

        if is_catalog_url(absolute):

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
            "Invalid HTTP/HTTPS URL"
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
                    f"Retry {attempt}: {url}"
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
            if parsed.scheme == "https"
            else 80
        )

    node = node_registry[host]

    node["host"] = host

    node["ports"].add(port)


def register_master_node(url):

    register_node(url)

    host = urlparse(url).hostname

    if host:

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

            current_info = line

            extra_tags = []

            continue

        if line.startswith("#"):

            if current_info is not None:

                extra_tags.append(line)

            continue

        stream_url = normalize_url(
            urljoin(base_url, line)
        )

        if not valid_http_url(stream_url):

            current_info = None

            extra_tags = []

            continue

        if current_info is None:
            continue

        metadata = parse_extinf(
            current_info
        )

        name = metadata["name"]

        if is_blocked(name):

            statistics[
                "channels_filtered"
            ] += 1

            current_info = None

            extra_tags = []

            continue

        attributes = metadata["attributes"]

        record_number = (
            len(channel_records) + len(entries) + 1
        )

        entries.append({

            "record_number": record_number,

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

        })

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

        # Every occurrence gets its own record number.
        record["record_number"] = (
            len(channel_records) + 1
        )

        record["sources"] = [
            source_url
        ]

        record["first_seen"] = now()

        # No URL comparison.
        # No name comparison.
        # No channel_index.
        # No record rejection due to duplicates.

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

    # Duplicate URLs are counted for diagnostics only.
    # They are never removed.
    counts = defaultdict(int)

    for record in channel_records:
        counts[record["canonical_url"]] += 1

    statistics[
        "duplicate_urls_preserved"
    ] = sum(
        count - 1
        for count in counts.values()
        if count > 1
    )


# ================================================================
# 13. MASTER PROCESSOR
# ================================================================

def process_master(url, depth=0):

    url = normalize_url(url)

    key = canonical_url(url)

    if key in visited_masters:
        return

    if depth > MAX_DEPTH:

        log(
            f"Depth limit reached: {url}"
        )

        return

    if len(visited_masters) >= MAX_MASTERS:

        log(
            "Maximum master catalog limit reached"
        )

        return

    if not valid_http_url(url):
        return

    # Master URL deduplication prevents repeated fetching.
    # Channel records are NEVER deduplicated.
    visited_masters.add(key)

    register_master_node(url)

    statistics[
        "masters_attempted"
    ] += 1

    result = {

        "id": stable_id(key),

        "url": url,

        "depth": depth,

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
                "Not a recognizable M3U catalog"
            )

        entries = parse_m3u(
            content,
            final_url,
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
            f"[OK] {len(entries)} records | {final_url}"
        )

        nested = discover_masters(
            content,
            final_url,
        )

        statistics[
            "nested_masters_discovered"
        ] += len(nested)

        for nested_url in sorted(nested):

            if len(visited_masters) >= MAX_MASTERS:
                break

            time.sleep(REQUEST_DELAY)

            process_master(
                nested_url,
                depth + 1,
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
# 14. SOURCE PROCESSOR
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
        "[SOURCE] Parsing initial channels"
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
        f"[SOURCE] Initial records: {len(entries)}"
    )

    discovered = discover_masters(
        content,
        final_url,
    )

    discovered.update(
        canonical_url(url)
        for url in KNOWN_MASTERS
        if valid_http_url(url)
    )

    log(
        f"[DISCOVERY] Explicit masters: {len(discovered)}"
    )

    for url in sorted(discovered):

        time.sleep(REQUEST_DELAY)

        process_master(
            url,
            depth=1,
        )


# ================================================================
# 15. NODE EXPORT
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

            "channel_record_numbers": list(
                data["channel_records"]
            ),

        })

    atomic_json(
        OUTPUT_DIR / "01_nodes.json",
        result,
    )


# ================================================================
# 16. CHANNEL INVENTORY EXPORT
# ================================================================

def export_channel_inventory():

    log(
        "[EXPORT] Creating complete channel inventory"
    )

    extinf_lines = []

    url_lines = []

    m3u_lines = [
        "#EXTM3U"
    ]

    for channel in channel_records:

        extinf_lines.append(
            channel["extinf"]
        )

        url_lines.append(
            channel["url"]
        )

        m3u_lines.append(
            channel["extinf"]
        )

        m3u_lines.extend(
            channel["extra_tags"]
        )

        m3u_lines.append(
            channel["url"]
        )

    atomic_write(
        CHANNELS_DIR / "channels_extinf.txt",
        "\n".join(extinf_lines) + "\n"
        if extinf_lines else "",
    )

    atomic_write(
        CHANNELS_DIR / "channels_urls.txt",
        "\n".join(url_lines) + "\n"
        if url_lines else "",
    )

    atomic_json(
        CHANNELS_DIR / "channels_inventory.json",
        channel_records,
    )

    atomic_write(
        CHANNELS_DIR / "channels_inventory.m3u",
        "\n".join(m3u_lines) + "\n",
    )

    log(
        f"[EXPORT] Records: {len(channel_records)}"
    )


# ================================================================
# 17. FINAL PLAYLIST
# ================================================================

def export_final_m3u():

    log(
        "[EXPORT] Building final master playlist"
    )

    lines = [
        "#EXTM3U"
    ]

    for channel in channel_records:

        lines.append(
            channel["extinf"]
        )

        lines.extend(
            channel["extra_tags"]
        )

        lines.append(
            channel["url"]
        )

    path = (
        FINAL_DIR /
        "AGGREGATED_MASTER.m3u"
    )

    atomic_write(
        path,
        "\n".join(lines) + "\n",
    )

    log(
        f"[EXPORT] Final playlist: {path}"
    )


# ================================================================
# 18. MASTER EXPORT
# ================================================================

def export_masters():

    atomic_json(
        OUTPUT_DIR / "02_masters.json",
        master_results,
    )


# ================================================================
# 19. REPORT
# ================================================================

def export_report():

    lines = [

        "=" * 72,

        ENGINE_NAME,

        f"VERSION: {ENGINE_VERSION}",

        "MODE: NO DEDUPLICATION",

        "=" * 72,

        f"Generated UTC: {now()}",

        "",

        "STATISTICS",

        "-" * 72,

    ]

    for key, value in statistics.items():

        lines.append(
            f"{key}: {value}"
        )

    lines.extend([

        "",

        f"Nodes registered: {len(node_registry)}",

        f"Masters visited: {len(visited_masters)}",

        f"Channel records: {len(channel_records)}",

        "",

        "NODE INVENTORY",

        "-" * 72,

    ])

    for host, data in sorted(
        node_registry.items()
    ):

        ports = ",".join(
            map(str, sorted(data["ports"]))
        )

        lines.append(

            f"{host} | ports={ports} | "

            f"records={len(data['channel_records'])}"

        )

    lines.extend([

        "",

        "MASTER RESULTS",

        "-" * 72,

    ])

    for master in master_results:

        lines.append(

            f"{master['status'].upper()} | "

            f"depth={master['depth']} | "

            f"channels={master['channels']} | "

            f"{master['url']}"

        )

    if errors:

        lines.extend([

            "",

            "ERRORS",

            "-" * 72,

        ])

        for error in errors:

            lines.append(

                f"{error['url']} | {error['error']}"

            )

    atomic_write(

        REPORTS_DIR / "report.txt",

        "\n".join(lines) + "\n",

    )

    atomic_json(

        REPORTS_DIR / "errors.json",

        errors,

    )

    atomic_json(

        REPORTS_DIR / "statistics.json",

        {

            "engine": ENGINE_NAME,

            "version": ENGINE_VERSION,

            "generated": now(),

            "deduplication": False,

            "statistics": statistics,

        },

    )


# ================================================================
# 20. OUTPUT VALIDATION
# ================================================================

def validate_outputs():

    required = [

        CHANNELS_DIR / "channels_extinf.txt",

        CHANNELS_DIR / "channels_urls.txt",

        CHANNELS_DIR / "channels_inventory.json",

        CHANNELS_DIR / "channels_inventory.m3u",

        FINAL_DIR / "AGGREGATED_MASTER.m3u",

        OUTPUT_DIR / "01_nodes.json",

        OUTPUT_DIR / "02_masters.json",

        REPORTS_DIR / "report.txt",

    ]

    missing = [

        str(path)

        for path in required

        if not path.is_file()

    ]

    if missing:

        raise RuntimeError(

            "Missing output files: "

            + ", ".join(missing)

        )

    with (
        CHANNELS_DIR / "channels_inventory.json"
    ).open(
        "r",
        encoding="utf-8",
    ) as file:

        inventory = json.load(file)

    if len(inventory) != len(channel_records):

        raise RuntimeError(
            "Inventory record count mismatch"
        )

    extinf_count = sum(
        1
        for line in (
            CHANNELS_DIR / "channels_extinf.txt"
        ).read_text(
            encoding="utf-8"
        ).splitlines()
    )

    url_count = sum(
        1
        for line in (
            CHANNELS_DIR / "channels_urls.txt"
        ).read_text(
            encoding="utf-8"
        ).splitlines()
    )

    if extinf_count != len(channel_records):

        raise RuntimeError(
            "EXTINF record count mismatch"
        )

    if url_count != len(channel_records):

        raise RuntimeError(
            "URL record count mismatch"
        )

    log(
        "[VALIDATION] All output files verified"
    )


# ================================================================
# 21. MAIN
# ================================================================

def main():

    started = time.time()

    safe_mkdirs()

    initialize_logging()

    log("=" * 72)

    log(
        f"{ENGINE_NAME} v{ENGINE_VERSION}"
    )

    log(
        "DEDUPLICATION: DISABLED"
    )

    log("=" * 72)

    success = False

    try:

        process_source()

        # Inventory is generated before final M3U.
        export_channel_inventory()

        export_nodes()

        export_masters()

        export_final_m3u()

        export_report()

        validate_outputs()

        success = True

    except Exception as exc:

        log_error(
            f"FATAL: {exc}"
        )

        errors.append({

            "stage": "main",

            "error": str(exc),

            "timestamp": now(),

        })

        try:

            export_nodes()

            export_masters()

            export_report()

        except Exception as export_exc:

            log_error(
                f"Diagnostic export failed: {export_exc}"
            )

    elapsed = time.time() - started

    log("=" * 72)

    log(
        f"STATUS: {'SUCCESS' if success else 'FAILED'}"
    )

    log(
        f"NODES: {len(node_registry)}"
    )

    log(
        f"MASTERS: {len(visited_masters)}"
    )

    log(
        f"CHANNEL RECORDS: {len(channel_records)}"
    )

    log(
        f"DUPLICATE URL OCCURRENCES PRESERVED: "
        f"{statistics['duplicate_urls_preserved']}"
    )

    log(
        f"ELAPSED: {elapsed:.2f} seconds"
    )

    log("=" * 72)

    if not success:

        raise SystemExit(1)


if __name__ == "__main__":

    main()
