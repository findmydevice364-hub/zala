
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

# ============================================================
# CONFIGURATION
# ============================================================

HOST = "88.218.242.237"
PORT = 8001
BASE = f"http://{HOST}:{PORT}/"

WORKERS = 30
TIMEOUT = 12

MAX_BODY = 1024 * 1024
MAX_SEGMENT_SIZE = 4096
MAX_DOCUMENTS = 300
MAX_DEPTH = 5

OUTPUT_FILE = "FOUND_CHANNELS.m3u"
STATE_FILE = "scan_state.json"
DREG_FILE = "DREG_log.txt"
SKALA_FILE = "SKALA_log.txt"

USER_AGENT = "IPTV-Node-Scanner/4.0"

# ============================================================
# ALL KNOWN CHANNELS — NONE ARE EXCLUDED
# ============================================================

KNOWN_CHANNELS = [
    ("cartoon_classic", "Cartoon_Classics"),
    ("detective_lam_hd HD", "DetectiveJam_HD"),
    ("explorers_hd HD", "The_Explorers_HD"),
    ("magnat HD", "Magnat_HD"),
    ("Scream HD", "Scream_HD"),
    ("Еда", "eda"),
    ("romance_hd HD", "Romance_HD"),
    ("Старт Триумф", "Start_Triumf_HD"),
    ("suspense_hd HD", "Suspense_HD"),
    ("kinoman", "kinoman"),
    ("pes-i-ko", "pes-i-ko"),
]

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
    "/sitemap.xml",
]

# ============================================================
# GLOBAL DATA
# ============================================================

lock = threading.RLock()

found = {}
checked_channels = set()
checked_documents = set()
in_progress = set()

events = []

started = time.time()
last_dreg = started
last_skala = started

local = threading.local()


# ============================================================
# HTTP SESSION
# ============================================================

def session():
    if not hasattr(local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "keep-alive",
        })
        local.session = s

    return local.session


# ============================================================
# LOGGING
# ============================================================

def log(message):
    global last_dreg, last_skala

    line = (
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
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
                    f"Documents: {len(checked_documents)}\n"
                    f"Checked channels: {len(checked_channels)}\n"
                    f"Found channels: {len(found)}\n"
                )

            last_dreg = now

        if now - last_skala >= 3600:
            with open(SKALA_FILE, "a", encoding="utf-8") as f:
                f.write(
                    f"\n===== HOURLY REPORT {datetime.now()} =====\n"
                )
                f.write("\n".join(events))
                f.write(
                    f"\nDocuments: {len(checked_documents)}\n"
                    f"Checked channels: {len(checked_channels)}\n"
                    f"Found channels: {len(found)}\n"
                )

            events.clear()
            last_skala = now


# ============================================================
# URL HANDLING
# ============================================================

def valid_target(url):
    try:
        p = urlparse(url)

        return (
            p.scheme in ("http", "https")
            and p.hostname == HOST
            and p.port == PORT
            and not p.username
            and not p.password
        )

    except Exception:
        return False


def normalize_url(url, base=BASE):
    if not url:
        return None

    url = unquote(str(url).strip())

    if not url:
        return None

    result = urljoin(base, url)

    if not valid_target(result):
        return None

    p = urlparse(result)

    return p._replace(fragment="").geturl()


# ============================================================
# HTTP POLLING
# ============================================================

def get_url(url, byte_limit=MAX_BODY):
    url = normalize_url(url)

    if not url:
        return None

    try:
        with session().get(
            url,
            timeout=(5, TIMEOUT),
            allow_redirects=True,
            stream=True,
            headers={
                "Range": f"bytes=0-{byte_limit - 1}"
            }
        ) as response:

            # Do not follow a redirect outside this node.
            if not valid_target(response.url):
                log(f"REDIRECT OUTSIDE NODE: {url}")
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
                ).lower(),
                "body": body,
            }

    except requests.RequestException as e:
        log(f"REQUEST ERROR: {url} | {e}")
        return None


# ============================================================
# M3U PARSER
# ============================================================

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
            name = metadata[0].split(",", 1)[-1].strip()

            channels.append({
                "name": name or "Unknown",
                "url": url,
                "metadata": list(metadata),
            })

        metadata = []

    return channels


# ============================================================
# MPEG-TS SIGNATURE
# ============================================================

def looks_like_mpeg_ts(data):
    if len(data) < 189:
        return False

    # MPEG-TS packets are normally 188 bytes.
    for offset in range(min(188, len(data))):
        if (
            data[offset] == 0x47
            and offset + 188 < len(data)
            and data[offset + 188] == 0x47
        ):
            return True

    return False


# ============================================================
# DIRECT STREAM VALIDATION
# Supports MPEG-TS and direct video responses.
# ============================================================

def validate_direct_stream(url):
    result = get_url(url, MAX_SEGMENT_SIZE)

    if not result:
        return False

    if result["status"] not in (200, 206):
        return False

    body = result["body"]
    content_type = result["type"]

    if not body:
        return False

    if "text/html" in content_type:
        return False

    if "application/json" in content_type:
        return False

    if "text/plain" in content_type:
        return False

    if looks_like_mpeg_ts(body):
        return True

    if content_type.startswith("video/"):
        return True

    return False


# ============================================================
# HLS SEGMENT VALIDATION
# ============================================================

def validate_segment(url):
    result = get_url(url, MAX_SEGMENT_SIZE)

    if not result:
        return False

    if result["status"] not in (200, 206):
        return False

    return bool(result["body"])


# ============================================================
# HLS VALIDATION
# ============================================================

def validate_hls(url, depth=0):
    if depth > 3:
        return False

    result = get_url(url)

    if not result:
        return False

    if result["status"] not in (200, 206):
        return False

    body = result["body"]

    if not body.lstrip().startswith(b"#EXTM3U"):
        return False

    text = body.decode("utf-8", errors="ignore")

    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]

    # Master playlist: check its child playlist.
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

    # Media playlist: check actual segment.
    segments = []

    for line in lines:
        if line.startswith("#"):
            continue

        segment = normalize_url(
            line,
            result["final_url"]
        )

        if segment:
            segments.append(segment)

    is_media_playlist = (
        "#EXTINF:" in text
        or "#EXT-X-TARGETDURATION:" in text
    )

    if is_media_playlist:
        for segment in segments[:3]:
            if validate_segment(segment):
                return True

    return False


# ============================================================
# VALIDATE ANY CHANNEL TYPE
# HLS FIRST, THEN DIRECT STREAM
# ============================================================

def validate_channel(url):
    if validate_hls(url):
        return True

    return validate_direct_stream(url)


# ============================================================
# SAVE PLAYLIST
# ============================================================

def save_playlist():
    with lock:
        items = list(found.values())

    if not items:
        log("NO VERIFIED CHANNELS — previous playlist preserved")
        return False

    temp = OUTPUT_FILE + ".tmp"
    written = 0

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
            written += 1

    if written == 0:
        os.remove(temp)
        return False

    os.replace(temp, OUTPUT_FILE)

    log(f"PLAYLIST UPDATED: {written} CHANNELS")

    return True


# ============================================================
# SAVE STATE
# ============================================================

def save_state():
    with lock:
        state = {
            "host": HOST,
            "port": PORT,
            "found": found,
            "updated": datetime.now().isoformat(),
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


# ============================================================
# RESTORE PREVIOUS RESULTS
# ============================================================

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

        for url, item in state.get("found", {}).items():
            if not valid_target(url):
                continue

            if isinstance(item, dict):
                item.setdefault("url", url)
                item.setdefault("name", "Unknown")
                item.setdefault(
                    "metadata",
                    [f"#EXTINF:-1,{item['name']}"]
                )
                found[url] = item

        log(f"RESTORED PREVIOUS CHANNELS: {len(found)}")

    except Exception as e:
        log(f"STATE RESTORE ERROR: {e}")


# ============================================================
# VERIFY AND ADD CHANNEL
# ============================================================

def verify_channel(channel):
    url = normalize_url(channel["url"])

    if not url:
        return

    with lock:
        if url in checked_channels or url in in_progress:
            return

        in_progress.add(url)

    try:
        if validate_channel(url):
            channel["url"] = url

            with lock:
                found[url] = channel

            log(
                f"VERIFIED: {channel['name']} | {url}"
            )

            save_playlist()
            save_state()

        else:
            log(f"NOT A VALID STREAM: {url}")

    except Exception as e:
        log(f"CHANNEL ERROR: {url} | {e}")

    finally:
        with lock:
            in_progress.discard(url)
            checked_channels.add(url)


# ============================================================
# EXTRACT LINKS DISCLOSED BY THE NODE
# ============================================================

def extract_links(text, page_url):
    links = set()

    patterns = [
        r'''href=["']([^"'#]+)["']''',
        r'''src=["']([^"'#]+)["']''',
        r'''url=["']([^"']+)["']''',
    ]

    for pattern in patterns:
        for value in re.findall(pattern, text, re.I):
            url = normalize_url(value, page_url)

            if url:
                links.add(url)

    # Also recognize absolute URLs published in JSON/text.
    absolute_pattern = (
        r'''https?://88\.218\.242\.237:8001/[^\s"'<>]+'''
    )

    for value in re.findall(absolute_pattern, text, re.I):
        url = normalize_url(value, page_url)

        if url:
            links.add(url)

    return links


# ============================================================
# DOCUMENT DISCOVERY
# ============================================================

def is_document_url(url):
    path = urlparse(url).path.lower()

    return (
        path.endswith((
            ".m3u",
            ".m3u8",
            ".json",
            ".xml",
        ))
        or path in (
            "/",
            "/api",
            "/api/",
            "/channels",
            "/api/channels",
            "/api/playlist",
            "/sitemap.xml",
            "/robots.txt",
        )
    )


def discover_document(url, depth=0):
    result = get_url(url)

    if not result:
        return []

    if result["status"] not in (200, 206):
        log(f"DOCUMENT HTTP {result['status']}: {url}")
        return []

    text = result["body"].decode(
        "utf-8",
        errors="ignore"
    )

    log(f"NODE RESPONSE: {url} | HTTP {result['status']}")

    # If this is an M3U playlist, process every EXTINF entry.
    if text.lstrip().startswith("#EXTM3U"):
        channels = parse_m3u(
            text,
            result["final_url"]
        )

        log(
            f"M3U FOUND: {url} | "
            f"CHANNEL ENTRIES: {len(channels)}"
        )

        for channel in channels:
            verify_channel(channel)

    if depth >= MAX_DEPTH:
        return []

    discovered = []

    for link in extract_links(text, result["final_url"]):
        if is_document_url(link):
            discovered.append((link, depth + 1))
        else:
            # A plain path published by the node may itself
            # be a channel endpoint.
            name = os.path.basename(
                urlparse(link).path
            ) or "Discovered channel"

            verify_channel({
                "name": name,
                "url": link,
                "metadata": [
                    f"#EXTINF:-1,{name}"
                ],
            })

    return discovered


# ============================================================
# INITIAL CHANNEL CANDIDATES
# ============================================================

def initial_channels():
    result = []

    for name, path in KNOWN_CHANNELS:
        result.append({
            "name": name,
            "url": normalize_url(path, BASE),
            "metadata": [
                f"#EXTINF:-1,{name}"
            ],
        })

    return result


# ============================================================
# MAIN
# ============================================================

def main():
    log("=" * 65)
    log("IPTV NODE SCANNER STARTED")
    log(f"NODE: {BASE}")
    log(f"WORKERS: {WORKERS}")
    log("KNOWN CHANNELS: ENABLED")
    log("HLS VALIDATION: ENABLED")
    log("DIRECT MPEG-TS VALIDATION: ENABLED")
    log("=" * 65)

    restore_state()

    # First poll all 11 known endpoints.
    log("CHECKING ALL KNOWN CHANNEL ENDPOINTS")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [
            executor.submit(verify_channel, channel)
            for channel in initial_channels()
        ]

        wait(futures)

    # Poll the node's public discovery endpoints.
    queue = deque()

    for path in DISCOVERY_PATHS:
        url = normalize_url(path, BASE)

        if url:
            queue.append((url, 0))

    submitted = set()
    completed = set()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
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
                    discover_document,
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
                    checked_documents.add(url)

                try:
                    new_links = future.result()

                    for link, new_depth in new_links:
                        if link not in submitted:
                            queue.append((link, new_depth))

                except Exception as e:
                    log(f"DISCOVERY ERROR: {url} | {e}")

                save_state()

    # Final output
    save_playlist()
    save_state()

    elapsed = round(time.time() - started, 2)

    log("=" * 65)
    log("SCAN FINISHED")
    log(f"DOCUMENTS POLLED: {len(completed)}")
    log(f"CHANNEL ENDPOINTS CHECKED: {len(checked_channels)}")
    log(f"VERIFIED CHANNELS: {len(found)}")
    log(f"ELAPSED: {elapsed} seconds")
    log("=" * 65)

    with open(DREG_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"\n===== FINAL REPORT {datetime.now()} =====\n"
            f"Documents: {len(completed)}\n"
            f"Checked channels: {len(checked_channels)}\n"
            f"Found channels: {len(found)}\n"
            f"Elapsed: {elapsed} seconds\n"
        )

    with open(SKALA_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(events) + "\n")

    print("\nFINAL RESULT")
    print(f"Verified channels: {len(found)}")
    print(f"Playlist: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
