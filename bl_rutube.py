#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rutube TV channels -> signed HLS M3U.

The input M3U is the seed catalogue. Existing signed HLS entries are retained
as fallback, Rutube /live/video/<id>/ entries are resolved through the official
player options endpoint, and additional candidates are discovered through TV
feeds and TV-specific searches (never the general live-stream feed).

Only HLS URLs returned by Rutube are emitted. No signature or expiry token is
fabricated. Unresolved source entries are listed in the scan report.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin

import requests

BASE = "https://rutube.ru"
TV_FEEDS = ("tv", "телеканалы", "ТВ")
TV_SEARCHES = (
    "телеканал прямой эфир",
    "телевизионный канал прямой эфир",
    "тв канал онлайн",
    "телеканал HD",
    "региональный телеканал",
    "новостной телеканал",
    "музыкальный телеканал",
    "детский телеканал",
    "познавательный телеканал",
    "спортивный телеканал",
    "TV channel live",
    "music TV channel live",
    "news TV channel live",
)
PLAY_API = BASE + "/api/play/options/{video_id}/"
VIDEO_API = BASE + "/api/video/{video_id}/"
SEARCH_API = BASE + "/api/search/video/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": BASE + "/live/",
    "Origin": BASE,
}
ID_RE = re.compile(r"(?i)(?:/live/video/|/video/|/play/embed/|/livestream/)([0-9a-f]{32})")
ANY_ID_RE = re.compile(r"(?i)\b([0-9a-f]{32})\b")
ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')

# Reject obvious personal/gameplay streams. This is deliberately conservative:
# unknown candidates go to the report rather than being auto-added.
DENY_RE = re.compile(
    r"(?i)\b(gameplay|геймплей|стример|стримера|стрим\s+игр|игровой\s+стрим|"
    r"летсплей|minecraft|fortnite|pubg|dota\s*2|counter.?strike|cs2|twitch|"
    r"личный\s+эфир|мой\s+стрим|игрок|киберспортсмен)\b"
)
TV_HINT_RE = re.compile(
    r"(?i)(телеканал|телеканалы|тв\s*канал|телевизионн|\bканал\b|\btv\b|"
    r"\bhd\b|\bfhd\b|\b24\b|\bnews\b|\bchannel\b|\bтелевидени)"
)

@dataclass
class Entry:
    extinf: str
    url: str
    title: str
    video_id: str | None


def clean_title(value: Any, fallback: str = "RUTUBE TV") -> str:
    if not isinstance(value, str):
        return fallback
    return " ".join(value.replace("\r", " ").replace("\n", " ").split()) or fallback


def parse_extinf(line: str) -> tuple[dict[str, str], str]:
    if "," not in line:
        return {}, "RUTUBE TV"
    head, title = line.split(",", 1)
    attrs = dict(ATTR_RE.findall(head))
    return attrs, clean_title(title)


def build_extinf(attrs: dict[str, str], title: str) -> str:
    extra = "".join(f' {key}="{value}"' for key, value in attrs.items())
    return f"#EXTINF:-1{extra},{clean_title(title)}"


def parse_m3u(path: Path) -> list[Entry]:
    if not path.is_file():
        raise FileNotFoundError(f"Входной плейлист не найден: {path}")
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    entries: list[Entry] = []
    pending = "#EXTINF:-1,RUTUBE TV"
    for line in lines:
        line = line.strip()
        if not line or line == "#EXTM3U":
            continue
        if line.startswith("#EXTINF:"):
            pending = line
            continue
        if line.startswith("#"):
            continue
        attrs, title = parse_extinf(pending)
        match = ID_RE.search(line)
        video_id = match.group(1).lower() if match else None
        # Ignore explanatory text accidentally pasted after the playlist.
        if not video_id and not (line.startswith("https://bl.rutube.ru/") and ".m3u8" in line):
            continue
        entries.append(Entry(pending, line, title, video_id))
        pending = "#EXTINF:-1,RUTUBE TV"
    return entries


def merge_entries(entries: list[Entry]) -> list[Entry]:
    merged: dict[str, Entry] = {}
    order: list[str] = []
    no_id = 0
    for entry in entries:
        key = entry.video_id or f"url:{entry.url}"
        if key not in merged:
            merged[key] = entry
            order.append(key)
            continue
        old = merged[key]
        old_attrs, _ = parse_extinf(old.extinf)
        new_attrs, _ = parse_extinf(entry.extinf)
        attrs = dict(old_attrs)
        attrs.update({k: v for k, v in new_attrs.items() if v})
        # Keep the first/base title and enrich it with tvg-id/group-title metadata.
        merged[key] = Entry(build_extinf(attrs, old.title), old.url, old.title, old.video_id)
    return [merged[k] for k in order]


def request_json(session: requests.Session, url: str, params: dict[str, Any] | None = None) -> Any:
    response = session.get(url, params=params, headers=HEADERS, timeout=25)
    response.raise_for_status()
    return response.json()


def extract_hls(data: Any) -> str | None:
    if not isinstance(data, dict):
        return None
    candidates: list[Any] = []
    live = data.get("live_streams") or {}
    hls = live.get("hls") if isinstance(live, dict) else None
    if isinstance(hls, str):
        candidates.append(hls)
    elif isinstance(hls, dict):
        candidates.extend((hls.get("url"), hls.get("src")))
    elif isinstance(hls, list):
        for item in hls:
            if isinstance(item, str):
                candidates.append(item)
            elif isinstance(item, dict):
                candidates.extend((item.get("url"), item.get("src")))
    balancer = data.get("video_balancer") or {}
    if isinstance(balancer, dict):
        candidates.append(balancer.get("m3u8"))
    for value in candidates:
        if isinstance(value, str) and value.startswith("https://") and ".m3u8" in value:
            return value
    return None


def get_live_hls(session: requests.Session, video_id: str) -> str | None:
    data = request_json(session, PLAY_API.format(video_id=video_id), {
        "format": "json", "no_404": "true", "referer": BASE + "/live/", "platform": "player"
    })
    if not isinstance(data, dict) or not data.get("live_streams"):
        return None
    return extract_hls(data)


def walk_dicts(data: Any):
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from walk_dicts(value)
    elif isinstance(data, list):
        for value in data:
            yield from walk_dicts(value)


def get_video_id(obj: dict[str, Any]) -> str | None:
    raw = obj.get("id") or obj.get("video_id") or obj.get("videoId") or obj.get("code")
    if raw is not None and re.fullmatch(r"(?i)[0-9a-f]{32}", str(raw)):
        return str(raw).lower()
    for field in ("video_url", "url", "embed_url"):
        value = obj.get(field)
        if isinstance(value, str):
            match = ID_RE.search(value)
            if match:
                return match.group(1).lower()
    return None


def get_title(obj: dict[str, Any], video_id: str) -> str:
    for key in ("title", "name", "video_title", "feed_name"):
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            return clean_title(value, f"Rutube TV {video_id[:8]}")
    return f"Rutube TV {video_id[:8]}"


def get_owner_text(obj: dict[str, Any]) -> str:
    owner = obj.get("author") or obj.get("owner") or obj.get("channel") or {}
    if isinstance(owner, dict):
        return " ".join(str(owner.get(k, "")) for k in ("name", "title", "username", "description"))
    return str(owner) if owner else ""


def discover_from_feed(session: requests.Session, slug: str, pages: int, delay: float) -> dict[str, tuple[str, bool]]:
    found: dict[str, tuple[str, bool]] = {}
    next_url = f"{BASE}/api/feeds/{quote(slug, safe='')}/?page=1"
    visited: set[str] = set()
    for page in range(1, pages + 1):
        if not next_url or next_url in visited:
            break
        visited.add(next_url)
        try:
            data = request_json(session, next_url)
        except Exception as exc:
            print(f"[TV FEED SKIP] {slug} page {page}: {exc}")
            break
        for obj in walk_dicts(data):
            video_id = get_video_id(obj)
            if not video_id:
                continue
            title = get_title(obj, video_id)
            owner = get_owner_text(obj)
            description = obj.get("description", "")
            haystack = f"{title} {owner} {description}"
            # Feed endpoints may contain TV programmes as well as channels;
            # keep only channel-like cards and reject obvious personal streams.
            if DENY_RE.search(haystack) or not TV_HINT_RE.search(haystack):
                continue
            found.setdefault(video_id, (title, True))
        nxt = data.get("next") if isinstance(data, dict) else None
        if isinstance(nxt, str) and nxt:
            next_url = urljoin(BASE, nxt)
        elif isinstance(data, dict) and data.get("has_next"):
            next_url = f"{BASE}/api/feeds/{quote(slug, safe='')}/?page={page + 1}"
        else:
            break
        if delay:
            time.sleep(delay)
    return found


def discover_from_search(session: requests.Session, query: str, pages: int, delay: float) -> dict[str, tuple[str, bool]]:
    found: dict[str, tuple[str, bool]] = {}
    for page in range(1, pages + 1):
        try:
            data = request_json(session, SEARCH_API, {"query": query, "page": page})
        except Exception as exc:
            print(f"[SEARCH SKIP] {query!r} page {page}: {exc}")
            break
        results = data.get("results", []) if isinstance(data, dict) else []
        if not isinstance(results, list) or not results:
            break
        for obj in results:
            if not isinstance(obj, dict):
                continue
            video_id = get_video_id(obj)
            if not video_id:
                continue
            title = get_title(obj, video_id)
            owner = get_owner_text(obj)
            text = f"{title} {owner} {obj.get('description', '')}"
            if DENY_RE.search(text) or not TV_HINT_RE.search(text):
                continue
            found.setdefault(video_id, (title, False))
        if delay:
            time.sleep(delay)
    return found


def make_entry(old: Entry, hls_url: str) -> Entry:
    attrs, _ = parse_extinf(old.extinf)
    return Entry(build_extinf(attrs, old.title), hls_url, old.title, old.video_id)


def main() -> int:
    ap = argparse.ArgumentParser(description="Сборщик телеканалов Rutube в M3U (без общей ленты игровых/личных эфиров)")
    ap.add_argument("input", nargs="?", default="Rutubefile.txt", help="Исходный M3U-список телеканалов")
    ap.add_argument("-o", "--output", default="Rutube_TV.m3u")
    ap.add_argument("--report", default="Rutube_TV_scan_report.txt")
    ap.add_argument("--search-pages", type=int, default=2, help="Страниц на каждый TV-поисковый запрос")
    ap.add_argument("--feed-pages", type=int, default=3, help="Страниц TV-каталога на каждый feed slug")
    ap.add_argument("--delay", type=float, default=0.35)
    ap.add_argument("--no-discovery", action="store_true", help="Только преобразовать каналы из исходного M3U")
    args = ap.parse_args()

    try:
        seed = merge_entries(parse_m3u(Path(args.input)))
    except (OSError, UnicodeError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    session = requests.Session()
    candidates: dict[str, tuple[str, bool]] = {}
    if not args.no_discovery:
        print("[1/4] Ищу каналы в TV-витринах Rutube...")
        for slug in TV_FEEDS:
            for video_id, item in discover_from_feed(session, slug, args.feed_pages, args.delay).items():
                candidates.setdefault(video_id, item)
        print("[2/4] Ищу дополнительные телеканалы по тематическим запросам...")
        for query in TV_SEARCHES:
            for video_id, item in discover_from_search(session, query, args.search_pages, args.delay).items():
                candidates.setdefault(video_id, item)

    # Seed entries always take precedence and retain their original metadata.
    by_id = {e.video_id: e for e in seed if e.video_id}
    for video_id, (title, from_tv_feed) in candidates.items():
        if video_id not in by_id:
            extinf = build_extinf({"tvg-id": video_id, "group-title": "Телеканалы Rutube"}, title)
            by_id[video_id] = Entry(extinf, f"https://rutube.ru/live/video/{video_id}/", title, video_id)

    print(f"[3/4] Обновляю HLS для {len(by_id)} уникальных ID...")
    output: list[Entry] = []
    failed: list[str] = []
    refreshed = 0
    kept_fallback = 0
    for index, (video_id, entry) in enumerate(by_id.items(), 1):
        try:
            hls = get_live_hls(session, video_id)
            if hls:
                output.append(make_entry(entry, hls))
                refreshed += 1
                print(f"[HLS OK] {index}/{len(by_id)} {entry.title}")
            elif entry.url.startswith("https://bl.rutube.ru/") and ".m3u8" in entry.url:
                # Preserve a previously supplied signed URL if Rutube API is
                # temporarily unavailable; never fabricate a replacement.
                output.append(entry)
                kept_fallback += 1
                failed.append(f"{entry.title} [{video_id}]: API не вернул live HLS; оставлена исходная HLS-ссылка")
            else:
                failed.append(f"{entry.title} [{video_id}]: действующий HLS не получен; запись пропущена")
        except Exception as exc:
            if entry.url.startswith("https://bl.rutube.ru/") and ".m3u8" in entry.url:
                output.append(entry)
                kept_fallback += 1
            failed.append(f"{entry.title} [{video_id}]: {type(exc).__name__}: {exc}")
            print(f"[WARN] {entry.title}: {exc}")
        if args.delay:
            time.sleep(args.delay)

    # Retain any source lines without a parseable Rutube ID only if already HLS.
    for entry in seed:
        if not entry.video_id and entry.url.startswith("https://bl.rutube.ru/") and ".m3u8" in entry.url:
            output.append(entry)

    # Stable de-duplication by ID and signed URL.
    final: list[Entry] = []
    seen: set[str] = set()
    for entry in output:
        key = entry.video_id or entry.url
        if key not in seen:
            seen.add(key)
            final.append(entry)

    print("[4/4] Записываю M3U и отчёт...")
    out_path = Path(args.output)
    out_path.write_text("#EXTM3U\n" + "\n".join(x for e in final for x in (e.extinf, e.url)) + "\n", encoding="utf-8")
    report = [
        "RUTUBE TV CHANNEL DISCOVERY REPORT",
        f"Input: {args.input}",
        f"Output: {args.output}",
        f"Seed entries: {len(seed)}",
        f"Discovered candidates: {len(candidates)}",
        f"Unique IDs processed: {len(by_id)}",
        f"Fresh HLS from Rutube API: {refreshed}",
        f"Kept previous signed HLS fallback: {kept_fallback}",
        f"Final M3U entries: {len(final)}",
        f"Unresolved / warnings: {len(failed)}",
        "",
        "Unresolved entries:",
        *(failed or ["None"]),
        "",
        "Discovery note: TV feeds and TV-focused search are best-effort public sources;",
        "this is not a guarantee that every Rutube TV channel is publicly indexed.",
    ]
    Path(args.report).write_text("\n".join(report) + "\n", encoding="utf-8")
    print(f"[DONE] {args.output}: {len(final)} channels; {args.report}")
    return 0 if final else 3

if __name__ == "__main__":
    raise SystemExit(main())
