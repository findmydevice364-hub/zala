
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VK IPTV EXTINF HUNTER
Phoenix 89S lab.

Source:
https://vk.ru/club228871429

Functions:
1. Download community wall publications.
2. Extract #EXTINF + URL pairs.
3. Preserve original metadata and channel names.
4. Detect duplicate stream URLs.
5. Generate M3U, CSV and JSON reports.
6. Maintain a structured execution log.

Requires:
    pip install requests

Environment:
    VK_ACCESS_TOKEN=your_vk_access_token
"""

import os
import re
import csv
import json
import time
import html
import logging
import hashlib
import requests

from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlsplit


# ============================================================
# CONFIGURATION
# ============================================================

APP_NAME = "VK IPTV EXTINF HUNTER"
VERSION = "1.0.0"

GROUP_ID = 228871429
OWNER_ID = -GROUP_ID

VK_API = "https://api.vk.com/method/wall.get"
VK_API_VERSION = "5.199"

ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN", "")

PAGE_SIZE = 100
REQUEST_DELAY = 0.35
MAX_RETRIES = 4

OUTPUT_DIR = Path("VK_IPTV_OUTPUT")

M3U_FILE = OUTPUT_DIR / "All_Channels.m3u"
CSV_FILE = OUTPUT_DIR / "Playlist_Report.csv"
JSON_FILE = OUTPUT_DIR / "channels.json"
LOG_FILE = OUTPUT_DIR / "hunter.log"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(APP_NAME)


# ============================================================
# HTTP SESSION
# ============================================================

SESSION = requests.Session()

SESSION.headers.update({
    "User-Agent": "Phoenix89S-VK-IPTV-Hunter/1.0",
    "Accept": "application/json"
})


# ============================================================
# VK API
# ============================================================

def vk_request(method, params):

    request_params = {
        "access_token": ACCESS_TOKEN,
        "v": VK_API_VERSION,
        **params
    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = SESSION.get(
                f"https://api.vk.com/method/{method}",
                params=request_params,
                timeout=30
            )

            response.raise_for_status()

            data = response.json()

            if "error" in data:

                error = data["error"]

                raise RuntimeError(
                    f"VK API error {error.get('error_code')}: "
                    f"{error.get('error_msg')}"
                )

            return data["response"]

        except Exception as exc:

            logger.warning(
                "VK request attempt %s/%s failed: %s",
                attempt,
                MAX_RETRIES,
                exc
            )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(attempt * 2)


def get_wall_posts():

    logger.info("Starting VK wall collection")

    offset = 0
    collected = []

    while True:

        result = vk_request(
            "wall.get",
            {
                "owner_id": OWNER_ID,
                "count": PAGE_SIZE,
                "offset": offset,
                "filter": "all"
            }
        )

        items = result.get("items", [])

        if not items:
            break

        collected.extend(items)

        logger.info(
            "Downloaded %s / %s posts",
            len(collected),
            result.get("count", "?")
        )

        offset += len(items)

        if offset >= result.get("count", 0):
            break

        time.sleep(REQUEST_DELAY)

    logger.info("Total posts collected: %s", len(collected))

    return collected


# ============================================================
# TEXT EXTRACTION
# ============================================================

def extract_post_text(post):

    parts = []

    text = post.get("text", "")

    if text:
        parts.append(text)

    # Extract textual content from reposts.
    for repost in post.get("copy_history", []):

        repost_text = extract_post_text(repost)

        if repost_text:
            parts.append(repost_text)

    # Preserve external URLs found in attachment metadata.
    for attachment in post.get("attachments", []):

        attachment_type = attachment.get("type", "")

        obj = attachment.get(attachment_type, {})

        if isinstance(obj, dict):

            link = obj.get("url")

            if link:
                parts.append(link)

    return "\n".join(parts)


# ============================================================
# EXTINF PARSER
# ============================================================

URL_PATTERN = re.compile(
    r"^(?:https?|rtmp|rtsp)://\S+$",
    re.IGNORECASE
)

EXTINF_PATTERN = re.compile(
    r"^\s*#EXTINF\s*:",
    re.IGNORECASE
)


def normalize_text(value):

    return html.unescape(value).strip()


def extract_pairs(post):

    text = extract_post_text(post)

    if not text:
        return []

    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    pairs = []

    for index, line in enumerate(lines):

        extinf = normalize_text(line)

        if not EXTINF_PATTERN.match(extinf):
            continue

        url = None

        # Find the next non-empty line.
        for next_index in range(index + 1, len(lines)):

            candidate = normalize_text(lines[next_index])

            if not candidate:
                continue

            if candidate.startswith("#"):
                break

            if URL_PATTERN.match(candidate):
                url = candidate

            break

        if not url:

            logger.warning(
                "Incomplete EXTINF in post %s, line %s",
                post.get("id"),
                index + 1
            )

            continue

        pairs.append({
            "extinf": extinf,
            "url": url,
            "post_id": post.get("id"),
            "owner_id": post.get("owner_id"),
            "date": post.get("date"),
            "source": (
                f"https://vk.ru/wall"
                f"{post.get('owner_id')}_{post.get('id')}"
            )
        })

    return pairs


# ============================================================
# COLLECTION ENGINE
# ============================================================

def collect_all_pairs(posts):

    all_pairs = []

    for post in posts:

        pairs = extract_pairs(post)

        if pairs:

            logger.info(
                "Post %s: extracted %s pairs",
                post.get("id"),
                len(pairs)
            )

            all_pairs.extend(pairs)

    return all_pairs


# ============================================================
# DUPLICATE ANALYSIS
# ============================================================

def analyze_duplicates(pairs):

    url_counter = {}

    for pair in pairs:

        url = pair["url"]

        url_counter[url] = url_counter.get(url, 0) + 1

    duplicates = {
        url: count
        for url, count in url_counter.items()
        if count > 1
    }

    logger.info("Total pairs: %s", len(pairs))
    logger.info("Unique URLs: %s", len(url_counter))
    logger.info("Duplicate URLs: %s", len(duplicates))

    return duplicates


# ============================================================
# M3U GENERATOR
# ============================================================

def generate_m3u(pairs):

    with open(M3U_FILE, "w", encoding="utf-8", newline="\n") as file:

        file.write("#EXTM3U\n")

        for pair in pairs:

            file.write(pair["extinf"] + "\n")
            file.write(pair["url"] + "\n")

    logger.info("M3U generated: %s", M3U_FILE)


# ============================================================
# CSV REPORT
# ============================================================

def generate_csv(pairs, duplicates):

    fields = [
        "extinf",
        "url",
        "post_id",
        "owner_id",
        "date",
        "source",
        "duplicate"
    ]

    with open(
        CSV_FILE,
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        writer = csv.DictWriter(file, fieldnames=fields)

        writer.writeheader()

        for pair in pairs:

            record = dict(pair)

            record["duplicate"] = (
                pair["url"] in duplicates
            )

            writer.writerow(record)

    logger.info("CSV report generated")


# ============================================================
# JSON DATABASE
# ============================================================

def generate_json(pairs, duplicates):

    data = {
        "application": APP_NAME,
        "version": VERSION,
        "community_id": GROUP_ID,
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "statistics": {
            "total_pairs": len(pairs),
            "unique_urls": len(
                set(pair["url"] for pair in pairs)
            ),
            "duplicate_urls": len(duplicates)
        },
        "duplicates": duplicates,
        "channels": pairs
    }

    with open(
        JSON_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4
        )

    logger.info("JSON database generated")


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info("=" * 60)
    logger.info("%s v%s", APP_NAME, VERSION)
    logger.info("Phoenix 89S lab.")
    logger.info("=" * 60)

    if not ACCESS_TOKEN:

        raise RuntimeError(
            "VK_ACCESS_TOKEN is not configured. "
            "Set an authorized VK API access token."
        )

    try:

        posts = get_wall_posts()

        if not posts:

            logger.warning("No posts received")
            return

        pairs = collect_all_pairs(posts)

        if not pairs:

            logger.warning("No EXTINF + URL pairs found")
            return

        duplicates = analyze_duplicates(pairs)

        generate_m3u(pairs, duplicates)

        generate_csv(pairs, duplicates)

        generate_json(pairs, duplicates)

        logger.info("=" * 60)
        logger.info("COLLECTION COMPLETED")
        logger.info("Posts: %s", len(posts))
        logger.info("Pairs: %s", len(pairs))
        logger.info(
            "Unique URLs: %s",
            len(set(p["url"] for p in pairs))
        )
        logger.info("Duplicates: %s", len(duplicates))
        logger.info("=" * 60)

    except Exception:

        logger.exception("COLLECTION FAILED")
        raise


if __name__ == "__main__":
    main()
