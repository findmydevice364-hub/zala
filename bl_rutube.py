#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Получает актуальные HLS URL для Rutube Live и обновляет M3U.

Использование:
  python rutube_refresh_m3u.py Rutubefile_formatted.m3u
  python rutube_refresh_m3u.py input.m3u -o output.m3u --delay 0.5

Требуется: python -m pip install requests
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests

API_URL = "https://rutube.ru/api/play/options/{video_id}/"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://rutube.ru/feeds/live/",
    "Origin": "https://rutube.ru",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# Поддерживаются как исходные страницы live/video/ID, так и строки-заглушки,
# созданные в Rutubefile_formatted.m3u.
LIVE_PAGE_RE = re.compile(r"https?://rutube\.ru/live/video/([0-9a-fA-F]{32})/?")
PLACEHOLDER_RE = re.compile(
    r"https?://bl\.rutube\.ru/livestream/([0-9a-fA-F]{32})/index\.m3u8\?[^\s]*REQUIRES_"
)


def extract_hls(data: dict) -> str | None:
    """Достаёт выданную самим Rutube ссылку HLS из вариантов ответа API."""
    live = data.get("live_streams") or {}
    hls = live.get("hls") or []

    # В актуальных/старых вариантах API hls встречается как список объектов,
    # словарь или сразу строка.
    candidates = []
    if isinstance(hls, str):
        candidates.append(hls)
    elif isinstance(hls, dict):
        candidates.extend([hls.get("url"), hls.get("src")])
    elif isinstance(hls, list):
        for item in hls:
            if isinstance(item, str):
                candidates.append(item)
            elif isinstance(item, dict):
                candidates.extend([item.get("url"), item.get("src")])

    balancer = data.get("video_balancer") or {}
    if isinstance(balancer, dict):
        candidates.append(balancer.get("m3u8"))

    # Некоторые ответы помещают адрес глубже в структуру.
    for key in ("m3u8", "url"):
        value = data.get(key)
        if isinstance(value, str):
            candidates.append(value)

    for url in candidates:
        if isinstance(url, str) and url.startswith(("https://", "http://")) and ".m3u8" in url:
            return url
    return None


def request_stream(session: requests.Session, video_id: str) -> str:
    url = API_URL.format(video_id=video_id)
    params = {
        "no_404": "true",
        "referer": "https://rutube.ru/feeds/live/",
        "platform": "player-carousel",
        "format": "json",
    }
    response = session.get(url, params=params, headers=HEADERS, timeout=25)
    response.raise_for_status()
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError("Rutube API вернул не JSON") from exc

    stream_url = extract_hls(data)
    if not stream_url:
        # Не подставляем выдуманные s/e: если API не выдал URL, канал
        # остаётся без обновления и попадает в список ошибок.
        status = data.get("status") or data.get("detail") or data.get("message") or "HLS URL отсутствует"
        raise RuntimeError(str(status))
    return stream_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Обновление HLS-ссылок Rutube в M3U")
    parser.add_argument("input", help="Исходный M3U-файл")
    parser.add_argument("-o", "--output", help="Выходной файл (по умолчанию *_refreshed.m3u)")
    parser.add_argument("--delay", type=float, default=0.35, help="Пауза между запросами в секундах")
    args = parser.parse_args()

    src = Path(args.input)
    if not src.is_file():
        print(f"[ОШИБКА] Файл не найден: {src}", file=sys.stderr)
        return 2

    dst = Path(args.output) if args.output else src.with_name(src.stem + "_refreshed" + src.suffix)
    # Сохраняем исходную кодировку с BOM, если он присутствовал.
    raw = src.read_bytes()
    has_bom = raw.startswith(b"\xef\xbb\xbf")
    text = raw.decode("utf-8-sig")
    lines = text.splitlines(keepends=True)

    session = requests.Session()
    cache: dict[str, str | Exception] = {}
    updated = 0
    failed: list[tuple[str, str]] = []

    for index, line in enumerate(lines):
        stripped = line.strip()
        match = PLACEHOLDER_RE.search(stripped) or LIVE_PAGE_RE.search(stripped)
        if not match:
            continue
        video_id = match.group(1).lower()

        if video_id not in cache:
            try:
                print(f"[Rutube] Запрашиваю {video_id} ...", flush=True)
                cache[video_id] = request_stream(session, video_id)
            except Exception as exc:
                cache[video_id] = exc
            if args.delay > 0:
                time.sleep(args.delay)

        result = cache[video_id]
        if isinstance(result, Exception):
            failed.append((video_id, str(result)))
            print(f"[НЕТ ПОТОКА] {video_id}: {result}")
            continue

        ending = "\r\n" if line.endswith("\r\n") else "\n" if line.endswith("\n") else ""
        lines[index] = result + ending
        updated += 1
        print(f"[OK] {video_id} -> {result}")

    output_text = "".join(lines)
    dst.write_bytes(("\ufeff" if has_bom else "").encode("utf-8") + output_text.encode("utf-8"))

    print("\n========== ИТОГ ==========")
    print(f"Обновлено URL: {updated}")
    print(f"Не получено:   {len(failed)}")
    print(f"Файл:          {dst}")
    if failed:
        report = dst.with_name(dst.stem + "_failed.txt")
        report.write_text("\n".join(f"{vid}\t{reason}" for vid, reason in failed), encoding="utf-8")
        print(f"Список ошибок: {report}")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
