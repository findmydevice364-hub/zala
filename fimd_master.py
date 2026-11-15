
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
===============================================================
 PROJECT: IPTV NODE MASTER AGGREGATOR
 VERSION: 1.0.0
 PURPOSE: Recursive M3U catalog aggregation
===============================================================

INPUT:
  hvk.m3u

OUTPUT:
  output/AGGREGATED_MASTER.m3u
  output/nodes.json
  output/masters.json
  output/channels.json
  output/report.txt

FEATURES:
  - Source M3U parsing
  - Node extraction
  - Explicit master playlist discovery
  - Recursive catalog discovery
  - Channel metadata preservation
  - URL deduplication with source history
  - Adult-content exclusion
  - JSON and M3U export

Only explicitly identified catalog URLs are fetched.
Individual live stream URLs are not probed.
"""

from __future__ import annotations

import json
import re
import time
import hashlib

from pathlib import Path
from urllib.parse import (
    urlparse,
    urljoin,
    unquote
)
from urllib.request import (
    Request,
    urlopen
)
from urllib.error import (
    URLError,
    HTTPError
)
from collections import defaultdict
from datetime import datetime, timezone


# ===============================================================
# CONFIGURATION
# ===============================================================

SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/hvk.m3u"
)

OUTPUT_DIR = Path("output")

MAX_DEPTH = 3
MAX_RESPONSE_SIZE = 8 * 1024 * 1024
REQUEST_TIMEOUT = 12
REQUEST_DELAY = 1.0

USER_AGENT = "IPTV-Node-Master-Aggregator/1.0"

# Explicit catalog URL present in the supplied source.
# Other catalog URLs can be added when their addresses
# are documented and known.

KNOWN_MASTERS = [
    "http://91.205.196.7:8000/playlist.m3u8",
]

# Catalog-like names only.
# Do not interpret ordinary channel endpoints as catalogs.

MASTER_HINTS = (
    "playlist",
    "master",
    "channels",
    "channel_list",
    "all_channels",
    "all_channels_list",
)

# Exclude adult-oriented channel labels.
BLOCKED_TERMS = (
    "adult",
    "porn",
    "porno",
    "xxx",
    "sex",
    "sexy",
    "erotic",
    "vixen",
    "mb_sexy",
)

# ===============================================================
# GLOBAL DATA
# ===============================================================

visited_masters = set()
master_results = []
channel_records = []

nodes = defaultdict(lambda: {
    "host": "",
    "ports": set(),
    "channels": 0,
    "master_urls": set(),
})

errors = []


# ===============================================================
# UTILITIES
# ===============================================================

def now():
    return datetime.now(timezone.utc).isoformat()


def normalize_url(url):
    return url.strip().strip('"').strip("'")


def valid_http_url(url):
    try:
        parsed = urlparse(url)

        return (
            parsed.scheme in ("http", "https")
            and bool(parsed.hostname)
        )

    except Exception:
        return False


def is_blocked(name):
    value = name.casefold()

    return any(
        term in value
        for term in BLOCKED_TERMS
    )


def is_catalog_url(url):
    parsed = urlparse(url)

    filename = Path(
        unquote(parsed.path)
    ).name.casefold()

    if not filename.endswith((".m3u", ".m3u8")):
        return False

    return any(
        hint in filename
        for hint in MASTER_HINTS
    )


def stable_id(url):
    return hashlib.sha256(
        url.encode("utf-8")
    ).hexdigest()[:16]


# ===============================================================
# HTTP FETCH
# ===============================================================

def fetch_text(url):

    if not valid_http_url(url):
        raise ValueError("Invalid HTTP URL")

    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": (
                "application/vnd.apple.mpegurl,"
                "application/x-mpegURL,"
                "text/plain,*/*"
            ),
        }
    )

    with urlopen(
        request,
        timeout=REQUEST_TIMEOUT
    ) as response:

        content_type = response.headers.get(
            "Content-Type", ""
        )

        data = response.read(
            MAX_RESPONSE_SIZE + 1
        )

        if len(data) > MAX_RESPONSE_SIZE:
            raise ValueError(
                "Response exceeds size limit"
            )

        text = data.decode(
            "utf-8-sig",
            errors="replace"
        )

        if not text.strip():
            raise ValueError("Empty response")

        return text, response.geturl(), content_type


# ===============================================================
# NODE EXTRACTION
# ===============================================================

def register_node(url):

    parsed = urlparse(url)

    host = parsed.hostname

    if not host:
        return

    port = parsed.port or (
        443 if parsed.scheme == "https" else 80
    )

    node = nodes[host]

    node["host"] = host
    node["ports"].add(port)


# ===============================================================
# M3U PARSER
# ===============================================================

def parse_m3u(content, base_url):

    entries = []

    lines = content.splitlines()

    current_info = None
    extra_tags = []

    for raw_line in lines:

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

        if not valid_http_url(urljoin(base_url, line)):
            continue

        stream_url = urljoin(base_url, line)

        if current_info is None:
            continue

        name = current_info.rsplit(",", 1)[-1].strip()

        attributes = {}

        for key, value in re.findall(
            r'([\w-]+)="([^"]*)"',
            current_info
        ):
            attributes[key] = value

        if is_blocked(name):
            current_info = None
            extra_tags = []
            continue

        entries.append({
            "id": stable_id(stream_url),
            "name": name,
            "url": stream_url,
            "extinf": current_info,
            "extra_tags": list(extra_tags),
            "attributes": attributes,
            "source_master": base_url,
        })

        current_info = None
        extra_tags = []

    return entries


# ===============================================================
# CHANNEL REGISTRY
# ===============================================================

def register_channels(entries, source_url):

    for entry in entries:

        stream_url = entry["url"]

        register_node(stream_url)

        existing = next(
            (
                item
                for item in channel_records
                if item["url"] == stream_url
            ),
            None
        )

        if existing:

            if source_url not in existing["sources"]:
                existing["sources"].append(source_url)

            continue

        record = dict(entry)

        record["sources"] = [source_url]

        channel_records.append(record)

        host = urlparse(stream_url).hostname

        if host:
            nodes[host]["channels"] += 1


# ===============================================================
# MASTER DISCOVERY
# ===============================================================

def discover_masters(content, base_url):

    discovered = set()

    for line in content.splitlines():

        line = line.strip()

        if not line or line.startswith("#"):
            continue

        absolute = urljoin(base_url, line)

        if not valid_http_url(absolute):
            continue

        if is_catalog_url(absolute):
            discovered.add(absolute)

    return discovered


# ===============================================================
# MASTER PROCESSOR
# ===============================================================

def process_master(url, depth=0):

    url = normalize_url(url)

    if url in visited_masters:
        return

    if depth > MAX_DEPTH:
        return

    if not valid_http_url(url):
        return

    visited_masters.add(url)

    parsed = urlparse(url)

    if parsed.hostname:
        nodes[parsed.hostname]["master_urls"].add(url)
        register_node(url)

    print(
        f"[MASTER] depth={depth} | {url}"
    )

    result = {
        "url": url,
        "depth": depth,
        "status": "pending",
        "channels": 0,
        "error": None,
    }

    master_results.append(result)

    try:

        content, final_url, content_type = fetch_text(url)

        if "#EXTM3U" not in content[:4096]:
            raise ValueError(
                "Response is not a recognizable M3U catalog"
            )

        entries = parse_m3u(
            content,
            final_url
        )

        register_channels(
            entries,
            final_url
        )

        result["status"] = "success"
        result["channels"] = len(entries)
        result["content_type"] = content_type

        print(
            f"  [+] Channels: {len(entries)}"
        )

        nested = discover_masters(
            content,
            final_url
        )

        for nested_url in sorted(nested):

            time.sleep(REQUEST_DELAY)

            process_master(
                nested_url,
                depth + 1
            )

    except (
        HTTPError,
        URLError,
        TimeoutError,
        ValueError,
        OSError
    ) as exc:

        result["status"] = "error"
        result["error"] = str(exc)

        errors.append({
            "url": url,
            "error": str(exc),
        })

        print(
            f"  [ERROR] {exc}"
        )


# ===============================================================
# SOURCE PROCESSOR
# ===============================================================

def process_source():

    print("\n[1] Downloading source playlist")

    content, final_url, _ = fetch_text(
        SOURCE_URL
    )

    if "#EXTM3U" not in content[:4096]:
        raise ValueError(
            "Source is not an M3U playlist"
        )

    print("[2] Parsing source channels")

    entries = parse_m3u(
        content,
        final_url
    )

    register_channels(
        entries,
        final_url
    )

    print(
        f"  [+] Initial channels: {len(entries)}"
    )

    print("\n[3] Discovering explicit master URLs")

    discovered = discover_masters(
        content,
        final_url
    )

    discovered.update(KNOWN_MASTERS)

    for url in sorted(discovered):

        time.sleep(REQUEST_DELAY)

        process_master(url, depth=1)


# ===============================================================
# EXPORT: M3U
# ===============================================================

def export_m3u():

    path = OUTPUT_DIR / "AGGREGATED_MASTER.m3u"

    with path.open(
        "w",
        encoding="utf-8",
        newline="\n"
    ) as file:

        file.write("#EXTM3U\n")

        for channel in channel_records:

            file.write(
                channel["extinf"] + "\n"
            )

            for tag in channel["extra_tags"]:
                file.write(tag + "\n")

            file.write(
                channel["url"] + "\n"
            )

    print(f"[EXPORT] {path}")


# ===============================================================
# EXPORT: JSON
# ===============================================================

def export_json():

    node_output = []

    for host, data in nodes.items():

        node_output.append({
            "host": data["host"],
            "ports": sorted(data["ports"]),
            "channels": data["channels"],
            "master_urls": sorted(data["master_urls"]),
        })

    with (
        OUTPUT_DIR / "nodes.json"
    ).open("w", encoding="utf-8") as file:

        json.dump(
            node_output,
            file,
            ensure_ascii=False,
            indent=2
        )

    with (
        OUTPUT_DIR / "masters.json"
    ).open("w", encoding="utf-8") as file:

        json.dump(
            master_results,
            file,
            ensure_ascii=False,
            indent=2
        )

    with (
        OUTPUT_DIR / "channels.json"
    ).open("w", encoding="utf-8") as file:

        json.dump(
            channel_records,
            file,
            ensure_ascii=False,
            indent=2
        )

    print("[EXPORT] JSON files saved")


# ===============================================================
# REPORT
# ===============================================================

def export_report():

    successful = sum(
        1 for item in master_results
        if item["status"] == "success"
    )

    failed = sum(
        1 for item in master_results
        if item["status"] == "error"
    )

    lines = [
        "=" * 65,
        "IPTV NODE MASTER AGGREGATOR",
        "=" * 65,
        f"Generated: {now()}",
        "",
        f"Nodes discovered: {len(nodes)}",
        f"Masters visited: {len(visited_masters)}",
        f"Masters successful: {successful}",
        f"Masters failed: {failed}",
        f"Unique channels: {len(channel_records)}",
        "",
        "NODE INVENTORY",
        "-" * 65,
    ]

    for host, data in sorted(nodes.items()):

        ports = ", ".join(
            map(str, sorted(data["ports"]))
        )

        lines.append(
            f"{host} | ports: {ports} | "
            f"channels: {data['channels']}"
        )

    lines.extend([
        "",
        "MASTER PLAYLIST RESULTS",
        "-" * 65,
    ])

    for master in master_results:

        lines.append(
            f"{master['status'].upper()} | "
            f"{master['channels']} | "
            f"{master['url']}"
        )

    if errors:

        lines.extend([
            "",
            "ERRORS",
            "-" * 65,
        ])

        for error in errors:

            lines.append(
                f"{error['url']} | {error['error']}"
            )

    path = OUTPUT_DIR / "report.txt"

    path.write_text(
        "\n".join(lines),
        encoding="utf-8"
    )

    print(f"[REPORT] {path}")


# ===============================================================
# MAIN
# ===============================================================

def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    print("=" * 65)
    print("IPTV NODE MASTER AGGREGATOR v1.0")
    print("=" * 65)

    started = time.time()

    try:

        process_source()

    except Exception as exc:

        print(
            f"\n[FATAL] {exc}"
        )

    finally:

        export_m3u()
        export_json()
        export_report()

    elapsed = time.time() - started

    print("\n" + "=" * 65)
    print("SCAN COMPLETED")
    print("=" * 65)

    print(
        f"Nodes: {len(nodes)}"
    )

    print(
        f"Masters: {len(visited_masters)}"
    )

    print(
        f"Channels: {len(channel_records)}"
    )

    print(
        f"Elapsed: {elapsed:.2f} sec"
    )


if __name__ == "__main__":
    main()
