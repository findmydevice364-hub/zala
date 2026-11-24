
import requests
import time
import json
import re
import logging
import threading
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse, unquote

# =====================================================
# CONFIGURATION
# =====================================================

HOST = "88.218.242.237"
PORT = 8001
BASE = f"http://{HOST}:{PORT}"

WORKERS = 30
TIMEOUT = 12

OUTPUT_FILE = "FOUND_CHANNELS.m3u"
STATE_FILE = "scan_state.json"
DREG_FILE = "DREG_log.txt"
SKALA_FILE = "SKALA_log.txt"

KNOWN_CHANNELS = {
    "Cartoon_Classics",
    "DetectiveJam_HD",
    "The_Explorers_HD",
    "Magnat_HD",
    "Scream_HD",
    "eda",
    "Romance_HD",
    "Start_Triumf_HD",
    "Suspense_HD",
    "kinoman",
    "pes-i-ko"
}

# =====================================================
# THREAD SAFE DATA
# =====================================================

lock = threading.Lock()

found = {}
checked = set()
events = []

started = time.time()
last_dreg = started
last_skala = started

# =====================================================
# LOGGING
# =====================================================

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"

    with lock:
        events.append(line)
        print(line)

def save_logs():
    global last_dreg, last_skala

    now = time.time()

    with lock:
        snapshot = list(events)
        count = len(found)
        checks = len(checked)

    if now - last_dreg >= 1800:
        with open(DREG_FILE, "a", encoding="utf-8") as f:
            f.write(
                f"\n===== 30 MIN REPORT "
                f"{datetime.now()} =====\n"
                f"Checked: {checks}\n"
                f"Found: {count}\n"
            )
        last_dreg = now

    if now - last_skala >= 3600:
        with open(SKALA_FILE, "a", encoding="utf-8") as f:
            f.write(
                f"\n===== HOURLY REPORT "
                f"{datetime.now()} =====\n"
            )
            f.write("\n".join(snapshot))
            f.write(
                f"\nChecked: {checks}\nFound: {count}\n"
            )
        last_skala = now

# =====================================================
# INITIAL DISCOVERY
# Only conventional public index and metadata paths
# =====================================================

DISCOVERY_PATHS = [
    "/",
    "/robots.txt",
    "/sitemap.xml",
    "/playlist.m3u",
    "/playlist.m3u8",
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
    "/api/",
    "/api/channels",
    "/api/playlist",
    "/channels",
    "/status",
    "/server-status",
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
        )
    except Exception:
        return False

# =====================================================
# HTTP REQUEST
# =====================================================

def get_url(path):
    url = urljoin(BASE + "/", path.lstrip("/"))

    if not valid_target(url):
        return None

    try:
        response = requests.get(
            url,
            timeout=TIMEOUT,
            allow_redirects=True,
            headers={
                "User-Agent": "Authorized-IPTV-Inventory/1.0",
                "Range": "bytes=0-65535"
            },
            stream=True
        )

        # Do not follow redirects to another host
        if not valid_target(response.url):
            response.close()
            return None

        body = response.raw.read(65536, decode_content=True)
        status = response.status_code
        content_type = response.headers.get(
            "Content-Type", ""
        )

        result = {
            "url": url,
            "status": status,
            "type": content_type,
            "body": body
        }

        response.close()
        return result

    except requests.RequestException as e:
        log(f"HTTP ERROR {url}: {e}")
        return None

# =====================================================
# EXTRACT CHANNELS FROM M3U
# =====================================================

def parse_m3u(text):
    lines = text.splitlines()
    channels = []
    name = None

    for line in lines:
        line = line.strip()

        if line.startswith("#EXTINF"):
            name = line.split(",")[-1].strip()

        elif line and not line.startswith("#") and name:
            url = urljoin(BASE + "/", line)

            if valid_target(url):
                channels.append({
                    "name": name,
                    "url": url
                })

            name = None

    return channels

# =====================================================
# EXTRACT LINKS FROM HTML / XML
# =====================================================

def extract_links(text):
    patterns = [
        r'''href=["']([^"'#]+)["']''',
        r'''src=["']([^"'#]+)["']'''
    ]

    links = set()

    for pattern in patterns:
        for item in re.findall(pattern, text, re.I):
            url = urljoin(BASE + "/", unquote(item))

            if valid_target(url):
                links.add(url)

    return links

# =====================================================
# HLS VALIDATION
# =====================================================

def validate_hls(url):
    if not valid_target(url):
        return False

    result = get_url(url)

    if not result or result["status"] != 200:
        return False

    body = result["body"]

    if b"#EXTM3U" not in body[:1024]:
        return False

    text = body.decode("utf-8", errors="ignore")

    # Media playlist is immediately usable
    if "#EXTINF" in text or "#EXT-X-TARGETDURATION" in text:
        return True

    # Master playlist: validate a referenced child playlist
    for line in text.splitlines():
        line = line.strip()

        if line and not line.startswith("#"):
            child = urljoin(url, line)

            if valid_target(child):
                child_result = get_url(child)

                if (
                    child_result
                    and child_result["status"] == 200
                    and b"#EXTM3U" in child_result["body"][:1024]
                ):
                    return True

    return False

# =====================================================
# CHANNEL VERIFICATION
# =====================================================

def verify_channel(channel):
    url = channel["url"]

    with lock:
        if url in checked:
            return

        checked.add(url)

    if validate_hls(url):
        with lock:
            found[url] = channel["name"]

        log(f"FOUND HLS: {channel['name']} | {url}")
        save_playlist()

# =====================================================
# SAVE M3U
# =====================================================

def save_playlist():
    with lock:
        items = list(found.items())

    temp = OUTPUT_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")

        for url, name in sorted(items, key=lambda x: x[1].lower()):
            f.write(f"#EXTINF:-1,{name}\n")
            f.write(url + "\n")

    os.replace(temp, OUTPUT_FILE)

# =====================================================
# SAVE RESUME STATE
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

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# =====================================================
# RESTORE STATE
# =====================================================

if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)

        if state.get("host") == HOST and state.get("port") == PORT:
            checked.update(state.get("checked", []))
            found.update(state.get("found", {}))

        log("Previous scan state restored")

    except Exception as e:
        log(f"State restore error: {e}")

# =====================================================
# DISCOVERY WORKER
# =====================================================

def discover(path):
    result = get_url(path)

    if not result:
        return set()

    status = result["status"]
    body = result["body"]

    log(
        f"CHECK {path} | HTTP {status} | "
        f"{result['type']}"
    )

    if status != 200:
        return set()

    text = body.decode("utf-8", errors="ignore")
    discovered = set()

    if "#EXTM3U" in text[:2048]:
        for channel in parse_m3u(text):
            discovered.add(channel["url"])

            log(
                f"PLAYLIST ENTRY: "
                f"{channel['name']} | {channel['url']}"
            )

            verify_channel(channel)

    for url in extract_links(text):
        path_part = urlparse(url).path.lower()

        if (
            path_part.endswith((".m3u", ".m3u8"))
            or path_part in ("/", "/api/", "/channels")
        ):
            discovered.add(url)

    return discovered

# =====================================================
# MAIN
# =====================================================

def main():
    log("=" * 55)
    log(f"START SCAN: {BASE}")
    log(f"WORKERS: {WORKERS}")
    log("=" * 55)

    paths = list(DISCOVERY_PATHS)

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(discover, path): path
            for path in paths
        }

        for future in as_completed(futures):
            try:
                discovered = future.result()

                for url in discovered:
                    path = urlparse(url).path

                    if path not in paths:
                        paths.append(path)

            except Exception as e:
                log(f"DISCOVERY ERROR: {e}")

    save_playlist()
    save_state()
    save_logs()

    log("=" * 55)
    log("DISCOVERY FINISHED")
    log(f"CHECKED: {len(checked)}")
    log(f"FOUND: {len(found)}")
    log(f"OUTPUT: {OUTPUT_FILE}")
    log("=" * 55)

    print("\nSCAN COMPLETE")
    print(f"Found channels: {len(found)}")
    print(f"Playlist: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
