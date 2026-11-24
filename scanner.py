
import requests
import time
import json
import re
import os
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from urllib.parse import urljoin, urlparse, unquote
from collections import deque

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

OUTPUT_FILE = "FOUND_CHANNELS.m3u"
STATE_FILE = "scan_state.json"
DREG_FILE = "DREG_log.txt"
SKALA_FILE = "SKALA_log.txt"

KNOWN_CHANNELS = {
    "cartoon_classics",
    "detectivejam_hd",
    "the_explorers_hd",
    "magnat_hd",
    "scream_hd",
    "eda",
    "romance_hd",
    "start_triumf_hd",
    "suspense_hd",
    "kinoman",
    "pes-i-ko"
}

# =====================================================
# THREAD SAFE DATA
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
# LOGGING
# =====================================================

def log(message):
    global last_dreg, last_skala

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"

    with lock:
        events.append(line)
        print(line, flush=True)

        now = time.time()

        if now - last_dreg >= 1800:
            with open(DREG_FILE, "a", encoding="utf-8") as f:
                f.write(
                    f"\n===== 30 MIN REPORT {datetime.now()} =====\n"
                    f"Checked: {len(checked)}\n"
                    f"Found: {len(found)}\n"
                    f"Documents: {len(checked)}\n"
                )

            last_dreg = now

        if now - last_skala >= 3600:
            with open(SKALA_FILE, "a", encoding="utf-8") as f:
                f.write(
                    f"\n===== HOURLY REPORT {datetime.now()} =====\n"
                )
                f.write("\n".join(events))
                f.write(
                    f"\nChecked: {len(checked)}\n"
                    f"Found: {len(found)}\n"
                )

            last_skala = now
            events.clear()

# =====================================================
# ROOT MASTER PLAYLIST DISCOVERY
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
    "/zala.m3u",
    "/zala.m3u8",
    "/api/",
    "/api/channels",
    "/api/playlist",
    "/channels",
    "/status",
    "/robots.txt",
    "/sitemap.xml",
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
    url = unquote(url.strip())

    if not url:
        return None

    result = urljoin(base, url)

    if not valid_target(result):
        return None

    p = urlparse(result)

    return p._replace(fragment="").geturl()


# =====================================================
# HTTP REQUEST
# =====================================================

def get_url(url):
    url = normalize_url(url)

    if not url:
        return None

    try:
        with requests.get(
            url,
            timeout=TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": "Authorized-IPTV-Inventory/2.0",
                "Range": f"bytes=0-{MAX_BODY - 1}"
            },
            stream=True
        ) as response:

            if not valid_target(response.url):
                return None

            body = response.raw.read(
                MAX_BODY,
                decode_content=True
            )

            return {
                "url": url,
                "final_url": response.url,
                "status": response.status_code,
                "type": response.headers.get("Content-Type", ""),
                "body": body
            }

    except requests.RequestException as e:
        log(f"HTTP ERROR {url}: {e}")
        return None


# =====================================================
# M3U PARSER
# Preserves EXTINF and additional metadata
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

        if line.startswith("#EXTINF"):
            metadata = [line]
            continue

        if line.startswith("#"):
            if metadata and not line.startswith("#EXTVLCOPT:"):
                metadata.append(line)
            elif metadata:
                metadata.append(line)
            continue

        url = normalize_url(line, playlist_url)

        if url and metadata:
            extinf = metadata[0]

            # Extract channel name without losing EXTINF attributes
            name = extinf.split(",", 1)[-1].strip()

            channels.append({
                "name": name or "Unknown",
                "url": url,
                "metadata": list(metadata)
            })

        metadata = []

    return channels


# =====================================================
# EXTRACT LINKS FROM HTML / XML
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

            if url:
                path = urlparse(url).path.lower()

                if (
                    path.endswith((
                        ".m3u",
                        ".m3u8",
                        ".xml",
                        ".json"
                    ))
                    or path in ("/", "/api/", "/channels")
                ):
                    links.add(url)

    return links


# =====================================================
# HLS VALIDATION
# =====================================================

def validate_hls(url, depth=0):
    url = normalize_url(url)

    if not url or depth > 3:
        return False

    result = get_url(url)

    if not result or result["status"] not in (200, 206):
        return False

    body = result["body"]

    if b"#EXTM3U" not in body[:1024]:
        return False

    text = body.decode("utf-8", errors="ignore")

    # Media playlist
    if (
        "#EXTINF:" in text
        or "#EXT-X-TARGETDURATION:" in text
        or "#EXT-X-MEDIA-SEQUENCE:" in text
    ):
        return True

    # Master playlist: locate variants
    lines = text.splitlines()

    for i, line in enumerate(lines):
        line = line.strip()

        if line.startswith("#EXT-X-STREAM-INF"):
            for candidate in lines[i + 1:]:
                candidate = candidate.strip()

                if candidate and not candidate.startswith("#"):
                    child = normalize_url(candidate, url)

                    if child and validate_hls(child, depth + 1):
                        return True

                    break

        # I-frame variant
        if line.startswith("#EXT-X-I-FRAME-STREAM-INF"):
            match = re.search(r'URI="([^"]+)"', line)

            if match:
                child = normalize_url(match.group(1), url)

                if child and validate_hls(child, depth + 1):
                    return True

    return False


# =====================================================
# KNOWN CHANNEL FILTER
# =====================================================

def is_known_channel(url):
    path = urlparse(url).path.rstrip("/")
    name = path.split("/")[-1].lower()

    return name in KNOWN_CHANNELS


# =====================================================
# SAVE PLAYLIST
# =====================================================

def save_playlist():
    with lock:
        items = list(found.values())

    temp = OUTPUT_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8", newline="\n") as f:
        f.write("#EXTM3U\n")

        for item in sorted(
            items,
            key=lambda x: x["name"].lower()
        ):
            for metadata in item["metadata"]:
                f.write(metadata + "\n")

            f.write(item["url"] + "\n")

    os.replace(temp, OUTPUT_FILE)


# =====================================================
# CHANNEL VERIFICATION
# =====================================================

def verify_channel(channel):
    url = channel["url"]

    if is_known_channel(url):
        log(f"KNOWN CHANNEL SKIPPED: {channel['name']}")
        return

    with lock:
        if url in checked or url in in_progress:
            return

        in_progress.add(url)

    try:
        if validate_hls(url):
            with lock:
                found[url] = channel

            log(f"FOUND HLS: {channel['name']} | {url}")
            save_playlist()

        else:
            log(f"INVALID HLS: {url}")

    finally:
        with lock:
            in_progress.discard(url)
            checked.add(url)


# =====================================================
# DOCUMENT DISCOVERY
# =====================================================

def discover(url, depth=0):
    result = get_url(url)

    if not result:
        return []

    status = result["status"]

    log(
        f"DOCUMENT: {url} | HTTP {status} | "
        f"{result['type']}"
    )

    if status not in (200, 206):
        return []

    text = result["body"].decode("utf-8", errors="ignore")

    discovered = []

    # Check whether root or another endpoint is itself M3U
    if "#EXTM3U" in text[:2048]:
        channels = parse_m3u(text, result["final_url"])

        log(
            f"PLAYLIST DETECTED: {url} | "
            f"Entries: {len(channels)}"
        )

        for channel in channels:
            verify_channel(channel)

    # Discover nested playlist and metadata links
    if depth < MAX_DEPTH:
        for link in extract_links(text, result["final_url"]):
            discovered.append((link, depth + 1))

    return discovered


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
        json.dump(state, f, ensure_ascii=False, indent=2)

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
            state.get("host") == HOST
            and state.get("port") == PORT
        ):
            old_found = state.get("found", {})

            with lock:
                for url, item in old_found.items():

                    # Support previous state format
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
                        and item.get("metadata")
                    ):
                        found[url] = item

            log(f"RESTORED CHANNELS: {len(found)}")

    except Exception as e:
        log(f"STATE RESTORE ERROR: {e}")


# =====================================================
# MAIN DISCOVERY ENGINE
# Dynamically processes newly discovered playlists
# =====================================================

def main():
    log("=" * 60)
    log(f"START SCAN: {BASE}")
    log(f"WORKERS: {WORKERS}")
    log("ROOT MASTER PLAYLIST DISCOVERY ENABLED")
    log("=" * 60)

    restore_state()
    save_playlist()

    queue = deque()

    for path in DISCOVERY_PATHS:
        url = normalize_url(path, BASE)

        if url:
            queue.append((url, 0))

    submitted = set()
    completed = set()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:

        pending = {}

        while (queue or pending) and len(completed) < MAX_DOCUMENTS:

            # Submit newly discovered documents
            while (
                queue
                and len(pending) < WORKERS
                and len(completed) + len(pending) < MAX_DOCUMENTS
            ):
                url, depth = queue.popleft()

                if url in submitted or url in completed:
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

                try:
                    new_links = future.result()

                    for link, new_depth in new_links:
                        if (
                            link not in submitted
                            and link not in completed
                            and new_depth <= MAX_DEPTH
                        ):
                            queue.append((link, new_depth))

                except Exception as e:
                    log(f"DISCOVERY ERROR {url}: {e}")

                save_state()
                save_playlist()

    # Final save
    save_playlist()
    save_state()

    elapsed = round(time.time() - started, 2)

    log("=" * 60)
    log("SCAN FINISHED")
    log(f"DOCUMENTS: {len(completed)}")
    log(f"CHECKED CHANNELS: {len(checked)}")
    log(f"FOUND CHANNELS: {len(found)}")
    log(f"TIME: {elapsed} seconds")
    log(f"OUTPUT: {OUTPUT_FILE}")
    log("=" * 60)

    with open(DREG_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"\n===== FINAL REPORT {datetime.now()} =====\n"
            f"Documents: {len(completed)}\n"
            f"Checked: {len(checked)}\n"
            f"Found: {len(found)}\n"
            f"Time: {elapsed} seconds\n"
        )

    with open(SKALA_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(events) + "\n")

    print("\nFINAL RESULT")
    print(f"Found: {len(found)}")
    print(f"Playlist: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
