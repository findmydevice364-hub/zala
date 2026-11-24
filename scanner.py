
import os
import re
import json
import time
import threading
import requests

from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from urllib.parse import urljoin, urlparse, unquote

# =====================================================
# CONFIGURATION
# =====================================================

HOST = "88.218.242.237"
PORT = 8001
BASE = f"http://{HOST}:{PORT}/"

WORKERS = 30
TIMEOUT = 12

MAX_BODY = 1024 * 1024
MAX_DOCUMENTS = 300
MAX_DEPTH = 5
MAX_SEGMENT_SIZE = 4096

OUTPUT_FILE = "FOUND_CHANNELS.m3u"
STATE_FILE = "scan_state.json"
DREG_FILE = "DREG_log.txt"
SKALA_FILE = "SKALA_log.txt"

USER_AGENT = "Authorized-IPTV-Scanner/3.0"

# =====================================================
# DATA
# =====================================================

lock = threading.RLock()

found = {}
checked = set()
in_progress = set()

events = []

started = time.time()
last_dreg = started
last_skala = started

# =====================================================
# HTTP SESSION PER THREAD
# =====================================================

local = threading.local()


def session():
    if not hasattr(local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": USER_AGENT
        })
        local.session = s

    return local.session


# =====================================================
# LOGGING
# =====================================================

def log(message):
    global last_dreg, last_skala

    line = (
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"{message}"
    )

    with lock:
        print(line, flush=True)
        events.append(line)

        now = time.time()

        if now - last_dreg >= 1800:
            with open(DREG_FILE, "a", encoding="utf-8") as f:
                f.write(
                    f"\n===== 30 MIN REPORT {datetime.now()} =====\n"
                    f"Documents: {len(checked)}\n"
                    f"Channels: {len(found)}\n"
                )

            last_dreg = now

        if now - last_skala >= 3600:
            with open(SKALA_FILE, "a", encoding="utf-8") as f:
                f.write(
                    f"\n===== HOURLY REPORT {datetime.now()} =====\n"
                )
                f.write("\n".join(events))
                f.write(
                    f"\nDocuments: {len(checked)}\n"
                    f"Channels: {len(found)}\n"
                )

            events.clear()
            last_skala = now


# =====================================================
# PUBLIC DISCOVERY ENDPOINTS
# =====================================================

DISCOVERY_PATHS = [
    "/",
    "/playlist.m3u",
    "/playlist.m3u8",
    "/master.m3u",
    "/master.m3u8",
    "/master_playlist.m3u",
    "/master_playlist.m3u8",
    "/channels.m3u",
    "/channels.m3u8",
    "/index.m3u",
    "/index.m3u8",
    "/live.m3u",
    "/live.m3u8",
    "/tv.m3u",
    "/tv.m3u8",
    "/all.m3u",
    "/all.m3u8",
    "/iptv.m3u",
    "/iptv.m3u8",
    "/api/",
    "/api/channels",
    "/api/playlist",
    "/channels",
    "/status",
    "/robots.txt",
    "/sitemap.xml"
]

# =====================================================
# URL VALIDATION
# =====================================================

def valid_target(url):
    try:
        p = urlparse(url)

        return (
            p.scheme in ("http", "https")
            and p.hostname == HOST
            and p.port in (None, PORT)
            and not p.username
            and not p.password
        )

    except Exception:
        return False


def normalize_url(url, base=BASE):
    if not url:
        return None

    url = unquote(url.strip())

    result = urljoin(base, url)

    if not valid_target(result):
        return None

    p = urlparse(result)

    return p._replace(fragment="").geturl()


# =====================================================
# HTTP REQUEST
# =====================================================

def get_url(url, byte_limit=MAX_BODY):
    url = normalize_url(url)

    if not url:
        return None

    try:
        with session().get(
            url,
            timeout=TIMEOUT,
            allow_redirects=True,
            stream=True,
            headers={
                "Range": f"bytes=0-{byte_limit - 1}"
            }
        ) as response:

            if not valid_target(response.url):
                return None

            body = response.raw.read(
                byte_limit,
                decode_content=True
            )

            return {
                "url": url,
                "final_url": response.url,
                "status": response.status_code,
                "type": response.headers.get(
                    "Content-Type", ""
                ),
                "body": body
            }

    except requests.RequestException as e:
        log(f"HTTP ERROR {url}: {e}")
        return None


# =====================================================
# M3U PARSER
# Preserves EXTINF and metadata
# =====================================================

def parse_m3u(text, playlist_url):
    channels = []
    metadata = []

    for raw in text.splitlines():
        line = raw.strip()

        if not line:
            continue

        if line.startswith("#EXTM3U"):
            continue

        if line.startswith("#EXTINF:"):
            metadata = [line]
            continue

        if line.startswith("#"):
            if metadata:
                metadata.append(line)
            continue

        url = normalize_url(line, playlist_url)

        if url and metadata:
            extinf = metadata[0]

            name = extinf.split(",", 1)[-1].strip()

            channels.append({
                "name": name or "Unknown",
                "url": url,
                "metadata": list(metadata)
            })

        metadata = []

    return channels


# =====================================================
# EXTRACT PUBLISHED LINKS
# =====================================================

def extract_links(text, page_url):
    patterns = [
        r'''href=["']([^"'#]+)["']''',
        r'''src=["']([^"'#]+)["']''',
        r'''url=["']([^"']+)["']'''
    ]

    links = set()

    for pattern in patterns:
        for item in re.findall(pattern, text, re.I):
            url = normalize_url(item, page_url)

            if not url:
                continue

            path = urlparse(url).path.lower()

            if (
                path.endswith((
                    ".m3u",
                    ".m3u8",
                    ".xml",
                    ".json"
                ))
                or path in (
                    "/",
                    "/api/",
                    "/channels"
                )
            ):
                links.add(url)

    return links


# =====================================================
# VALIDATE MEDIA SEGMENT
# =====================================================

def validate_segment(url):
    url = normalize_url(url)

    if not url:
        return False

    result = get_url(url, MAX_SEGMENT_SIZE)

    if not result:
        return False

    return result["status"] in (200, 206) and bool(result["body"])


# =====================================================
# HLS VALIDATION
# =====================================================

def validate_hls(url, depth=0):
    if depth > 3:
        return False

    result = get_url(url)

    if not result or result["status"] not in (200, 206):
        return False

    body = result["body"]

    if not body.startswith(b"#EXTM3U"):
        return False

    text = body.decode("utf-8", errors="ignore")
    lines = [x.strip() for x in text.splitlines() if x.strip()]

    # Master playlist
    for i, line in enumerate(lines):

        if line.startswith("#EXT-X-STREAM-INF:"):
            for child_line in lines[i + 1:]:
                if child_line.startswith("#"):
                    continue

                child = normalize_url(
                    child_line,
                    result["final_url"]
                )

                if child and validate_hls(child, depth + 1):
                    return True

                break

    # Media playlist
    segments = []

    for line in lines:
        if line.startswith("#"):
            continue

        segment = normalize_url(line, result["final_url"])

        if segment:
            segments.append(segment)

    if (
        "#EXTINF:" in text
        or "#EXT-X-TARGETDURATION:" in text
    ):
        # Verify an actual media segment
        for segment in segments[:2]:
            if validate_segment(segment):
                return True

        return False

    return False


# =====================================================
# SAVE M3U
# =====================================================

def save_playlist():
    with lock:
        items = list(found.values())

    if not items:
        log("NO VERIFIED CHANNELS: keeping previous playlist")
        return False

    temp = OUTPUT_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8", newline="\n") as f:
        f.write("#EXTM3U\n")

        for item in sorted(
            items,
            key=lambda x: x["name"].lower()
        ):
            url = item["url"]

            if not valid_target(url):
                continue

            metadata = item.get("metadata") or [
                f"#EXTINF:-1,{item['name']}"
            ]

            for line in metadata:
                f.write(line.replace("\r", "") + "\n")

            f.write(url + "\n")

    os.replace(temp, OUTPUT_FILE)

    log(f"PLAYLIST SAVED: {len(items)} channels")

    return True


# =====================================================
# SAVE STATE
# =====================================================

def save_state():
    with lock:
        state = {
            "host": HOST,
            "port": PORT,
            "checked": list(checked),
            "found": found,
            "updated": datetime.now().isoformat()
        }

    temp = STATE_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8") as f:
        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2
        )

    os.replace(temp, STATE_FILE)


# =====================================================
# RESTORE STATE
# =====================================================

def restore_state():
    if not os.path.exists(STATE_FILE):
        return

    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)

        if (
            state.get("host") != HOST
            or state.get("port") != PORT
        ):
            return

        old_found = state.get("found", {})

        with lock:
            for url, item in old_found.items():

                if isinstance(item, str):
                    item = {
                        "name": item,
                        "url": url,
                        "metadata": [
                            f"#EXTINF:-1,{item}"
                        ]
                    }

                if (
                    isinstance(item, dict)
                    and valid_target(url)
                ):
                    item.setdefault("url", url)
                    item.setdefault("name", "Unknown")
                    item.setdefault(
                        "metadata",
                        [f"#EXTINF:-1,{item['name']}"]
                    )

                    found[url] = item

        log(f"RESTORED CHANNELS: {len(found)}")

    except Exception as e:
        log(f"STATE ERROR: {e}")


# =====================================================
# VERIFY CHANNEL
# ALL CHANNELS ARE INCLUDED
# =====================================================

def verify_channel(channel):
    url = channel["url"]

    with lock:
        if url in checked or url in in_progress:
            return

        in_progress.add(url)

    try:
        if validate_hls(url):
            with lock:
                found[url] = channel

            log(
                f"VERIFIED CHANNEL: "
                f"{channel['name']} | {url}"
            )

            save_playlist()
            save_state()

        else:
            log(f"UNVERIFIED STREAM: {url}")

    finally:
        with lock:
            in_progress.discard(url)
            checked.add(url)


# =====================================================
# DISCOVERY WORKER
# =====================================================

def discover(url, depth=0):
    result = get_url(url)

    if not result:
        return []

    if result["status"] not in (200, 206):
        log(f"HTTP {result['status']}: {url}")
        return []

    text = result["body"].decode(
        "utf-8",
        errors="ignore"
    )

    log(
        f"DOCUMENT: {url} | "
        f"HTTP {result['status']}"
    )

    discovered = []

    # Master playlist / regular M3U
    if text.lstrip().startswith("#EXTM3U"):
        channels = parse_m3u(
            text,
            result["final_url"]
        )

        log(
            f"PLAYLIST DETECTED: {url} | "
            f"ENTRIES: {len(channels)}"
        )

        for channel in channels:
            verify_channel(channel)

    # Additional published playlist links
    if depth < MAX_DEPTH:
        for link in extract_links(
            text,
            result["final_url"]
        ):
            discovered.append((link, depth + 1))

    return discovered


# =====================================================
# MAIN DISCOVERY ENGINE
# =====================================================

def main():
    log("=" * 60)
    log(f"START SCAN: {BASE}")
    log(f"WORKERS: {WORKERS}")
    log("ALL CHANNELS ENABLED")
    log("ROOT MASTER PLAYLIST DISCOVERY ENABLED")
    log("=" * 60)

    restore_state()

    # Keep previous verified results
    if found:
        save_playlist()

    queue = deque()

    for path in DISCOVERY_PATHS:
        url = normalize_url(path, BASE)

        if url:
            queue.append((url, 0))

    submitted = set()
    completed = set()

    with ThreadPoolExecutor(
        max_workers=WORKERS
    ) as executor:

        pending = {}

        while (
            queue or pending
        ) and len(completed) < MAX_DOCUMENTS:

            while (
                queue
                and len(pending) < WORKERS
                and len(completed) + len(pending) < MAX_DOCUMENTS
            ):
                url, depth = queue.popleft()

                if url in submitted:
                    continue

                submitted.add(url)

                future = executor.submit(
                    discover,
                    url,
                    depth
                )

                pending[future] = (url, depth)

            if not pending:
                break

            done, _ = wait(
                pending,
                return_when=FIRST_COMPLETED
            )

            for future in done:
                url, depth = pending.pop(future)

                completed.add(url)

                with lock:
                    checked.add(url)

                try:
                    new_links = future.result()

                    for link, new_depth in new_links:
                        if (
                            link not in submitted
                            and new_depth <= MAX_DEPTH
                        ):
                            queue.append((link, new_depth))

                except Exception as e:
                    log(f"DISCOVERY ERROR: {url} | {e}")

                save_state()

    save_playlist()
    save_state()

    elapsed = round(time.time() - started, 2)

    log("=" * 60)
    log("SCAN FINISHED")
    log(f"DOCUMENTS: {len(completed)}")
    log(f"CHANNELS CHECKED: {len(checked)}")
    log(f"VERIFIED CHANNELS: {len(found)}")
    log(f"TIME: {elapsed} seconds")
    log("=" * 60)

    with open(DREG_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"\n===== FINAL REPORT {datetime.now()} =====\n"
            f"Documents: {len(completed)}\n"
            f"Checked: {len(checked)}\n"
            f"Found: {len(found)}\n"
            f"Time: {elapsed}\n"
        )

    with open(SKALA_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(events) + "\n")

    print("\nFINAL RESULT")
    print(f"Verified channels: {len(found)}")
    print(f"Playlist: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
