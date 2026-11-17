#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rutube Live Discovery + HLS M3U Builder

Ищет публичные трансляции в общей ленте RUTUBE, проходит страницы ленты,
проверяет каждый найденный ID через play/options и получает настоящий HLS URL.

Зависимость:
    python -m pip install requests

Запуск:
    python rutube_live_discovery.py
    python rutube_live_discovery.py Rutubefile_formatted.m3u
    python rutube_live_discovery.py Rutubefile_formatted.m3u --max-pages 50

Важно:
- Ищутся публичные эфиры, которые RUTUBE показывает в общей ленте.
- «Все трансляции» не означает все когда-либо созданные эфиры: приватные,
  скрытые, завершённые и недоступные аккаунту эфиры в публичной ленте не видны.
- Скрипт не генерирует подписи s/e. Он использует только HLS URL, который
  реально вернул сервер RUTUBE.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import requests

BASE = "https://rutube.ru"
FEED_PAGE = BASE + "/feeds/stream/"
FEED_API = BASE + "/api/feeds/stream/"
PLAY_API = BASE + "/api/play/options/{video_id}/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": FEED_PAGE,
    "Origin": BASE,
}

ID_RE = re.compile(r"(?i)(?:/video/|/live/video/|/play/embed/)([0-9a-f]{32})")
ANY_ID_RE = re.compile(r"(?i)\b([0-9a-f]{32})\b")
PLACEHOLDER_RE = re.compile(r"REQUIRES_(?:SIGNATURE|TIMESTAMP)")


class FeedHTMLParser(HTMLParser):
    """Извлекает ссылки на видео и подписи карточек из HTML ленты."""
    def __init__(self):
        super().__init__()
        self.items = []
        self._href = None
        self._text = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            attrs = dict(attrs)
            href = attrs.get("href", "")
            if href:
                self._href = href
                self._text = []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._href is not None:
            label = " ".join(" ".join(self._text).split())
            self.items.append((self._href, label))
            self._href = None
            self._text = []


def get_json(session: requests.Session, url: str):
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def recursive_json_items(data):
    """Обходит JSON и выдаёт словари-карточки с ID/названием."""
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from recursive_json_items(value)
    elif isinstance(data, list):
        for value in data:
            yield from recursive_json_items(value)


def extract_next_url(data, current_url):
    """Ищет URL следующей страницы в распространённых структурах пагинации."""
    if not isinstance(data, dict):
        return None

    candidates = [
        data.get("next"),
        data.get("next_url"),
        data.get("nextUrl"),
    ]
    pagination = data.get("pagination")
    if isinstance(pagination, dict):
        candidates += [
            pagination.get("next"),
            pagination.get("next_url"),
            pagination.get("nextUrl"),
        ]
    meta = data.get("meta")
    if isinstance(meta, dict):
        candidates += [meta.get("next"), meta.get("next_url")]

    for value in candidates:
        if isinstance(value, str) and value.strip():
            return urljoin(current_url, value)
    return None


def collect_from_json(data, found):
    for obj in recursive_json_items(data):
        raw_id = obj.get("id") or obj.get("video_id") or obj.get("videoId")
        if raw_id is None:
            continue
        video_id = str(raw_id).lower()
        if not re.fullmatch(r"[0-9a-f]{32}", video_id):
            continue

        title = (
            obj.get("title") or obj.get("name") or obj.get("video_title")
            or obj.get("description") or f"RUTUBE Live {video_id[:8]}"
        )
        if isinstance(title, (dict, list)):
            title = f"RUTUBE Live {video_id[:8]}"
        found.setdefault(video_id, str(title).strip())


def collect_from_html(html, found):
    parser = FeedHTMLParser()
    parser.feed(html)
    for href, label in parser.items:
        match = ID_RE.search(href)
        if match:
            video_id = match.group(1).lower()
            found.setdefault(video_id, label or f"RUTUBE Live {video_id[:8]}")

    # Иногда сервер встраивает JSON с карточками прямо в HTML.
    for match in ANY_ID_RE.finditer(html):
        video_id = match.group(1).lower()
        if video_id not in found:
            found[video_id] = f"RUTUBE Live {video_id[:8]}"


def discover_live_candidates(session, max_pages, delay):
    """
    Сначала пробует JSON-ленту /api/feeds/stream/.
    Если endpoint не отдаёт JSON, использует HTML-ленту с ?page=N.
    Все кандидаты дополнительно проверяются через play/options.
    """
    found = {}
    visited = set()
    page_url = FEED_API + "?format=json&limit=50&page=1"
    json_mode = True

    for page_no in range(1, max_pages + 1):
        if not page_url or page_url in visited:
            break
        visited.add(page_url)

        try:
            response = session.get(page_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").lower()

            if json_mode and ("json" in content_type or response.text.lstrip().startswith(("{", "["))):
                data = response.json()
                before = len(found)
                collect_from_json(data, found)
                next_url = extract_next_url(data, page_url)

                # Если API не вернул явный next, пробуем стандартную пагинацию.
                if not next_url and len(found) > before:
                    parsed = urlparse(page_url)
                    query = parse_qs(parsed.query)
                    query["page"] = [str(page_no + 1)]
                    from urllib.parse import urlencode
                    next_url = parsed._replace(query=urlencode(query, doseq=True)).geturl()

                if not next_url or next_url in visited:
                    break
                page_url = next_url
            else:
                json_mode = False
                break

        except (requests.RequestException, ValueError):
            json_mode = False
            break

        if delay:
            time.sleep(delay)

    # HTML fallback. Запрашиваем страницы с ?page=N; останавливаемся,
    # когда страница повторилась или новых ID больше нет.
    if not json_mode or not found:
        previous_page_ids = None
        for page_no in range(1, max_pages + 1):
            url = FEED_PAGE if page_no == 1 else f"{FEED_PAGE}?page={page_no}"
            if url in visited:
                continue
            try:
                response = session.get(url, headers=HEADERS, timeout=30)
                response.raise_for_status()
            except requests.RequestException as exc:
                print(f"[WARN] Не удалось загрузить страницу ленты {page_no}: {exc}")
                break

            before = len(found)
            collect_from_html(response.text, found)
            page_ids = frozenset(found)

            if page_no > 1 and (page_ids == previous_page_ids or len(found) == before):
                break
            previous_page_ids = page_ids

            print(f"[ЛЕНТА] Страница {page_no}: найдено кандидатов всего {len(found)}")
            if delay:
                time.sleep(delay)

    return found


def extract_hls(data):
    """Извлекает HLS только из полей, выданных самим Rutube."""
    candidates = []
    live = data.get("live_streams") or {}
    hls = live.get("hls") if isinstance(live, dict) else None

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

    for value in candidates:
        if isinstance(value, str) and value.startswith("https://") and ".m3u8" in value:
            return value
    return None


def get_live_stream(session, video_id):
    url = PLAY_API.format(video_id=video_id)
    params = {
        "format": "json",
        "no_404": "true",
        "referer": FEED_PAGE,
        "platform": "player",
    }
    response = session.get(url, params=params, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Не допускаем случайного добавления обычного VOD.
    live_streams = data.get("live_streams")
    if not live_streams:
        return None

    return extract_hls(data)


def read_existing_m3u(path):
    if not path:
        return [], set(), False
    src = Path(path)
    if not src.is_file():
        raise FileNotFoundError(f"Входной M3U не найден: {src}")

    raw = src.read_bytes()
    bom = raw.startswith(b"\xef\xbb\xbf")
    text = raw.decode("utf-8-sig")
    lines = text.splitlines()
    existing_ids = set()

    for line in lines:
        if "rutube.ru/" in line or "bl.rutube.ru/" in line:
            match = ANY_ID_RE.search(line)
            if match:
                existing_ids.add(match.group(1).lower())
    return lines, existing_ids, bom


def main():
    parser = argparse.ArgumentParser(
        description="Поиск публичных прямых эфиров RUTUBE и создание M3U"
    )
    parser.add_argument(
        "input", nargs="?",
        help="Необязательный исходный M3U: его записи сохраняются, новые эфиры добавляются"
    )
    parser.add_argument("-o", "--output", default="Rutube_ALL_LIVE.m3u")
    parser.add_argument("--max-pages", type=int, default=30,
                        help="Максимум страниц общей ленты (по умолчанию 30)")
    parser.add_argument("--delay", type=float, default=0.4,
                        help="Пауза между запросами (по умолчанию 0.4 сек)")
    parser.add_argument("--verify-existing", action="store_true",
                        help="Повторно запросить HLS для уже имеющихся ID из входного M3U")
    args = parser.parse_args()

    session = requests.Session()
    try:
        lines, existing_ids, has_bom = read_existing_m3u(args.input)
    except (OSError, UnicodeError) as exc:
        print(f"[ОШИБКА] {exc}", file=sys.stderr)
        return 2

    print("[1/3] Ищу публичные эфиры в общей ленте RUTUBE...")
    candidates = discover_live_candidates(session, args.max_pages, args.delay)
    print(f"[ЛЕНТА] Всего найдено ID-кандидатов: {len(candidates)}")

    if args.verify_existing:
        for video_id in existing_ids:
            candidates.setdefault(video_id, f"RUTUBE Live {video_id[:8]}")

    print("[2/3] Проверяю кандидатов и запрашиваю настоящие HLS-ссылки...")
    live_entries = []
    failed = []
    checked = 0

    for video_id, title in candidates.items():
        if video_id in existing_ids and not args.verify_existing:
            continue
        checked += 1
        try:
            hls_url = get_live_stream(session, video_id)
            if not hls_url:
                continue
            safe_title = title.replace("\r", " ").replace("\n", " ").strip()
            live_entries.append((video_id, safe_title, hls_url))
            print(f"[LIVE OK] {safe_title} | {video_id}")
        except Exception as exc:
            failed.append((video_id, str(exc)))
            print(f"[SKIP] {video_id}: {exc}")

        if args.delay:
            time.sleep(args.delay)

    print("[3/3] Собираю плейлист...")
    output_lines = list(lines)

    if not output_lines:
        output_lines = ["#EXTM3U"]
    elif not any(line.strip().startswith("#EXTM3U") for line in output_lines):
        output_lines.insert(0, "#EXTM3U")

    if output_lines and output_lines[-1].strip():
        output_lines.append("")

    # Не дублируем ID, уже присутствующие во входном M3U.
    added = 0
    for video_id, title, hls_url in live_entries:
        if video_id in existing_ids:
            continue
        output_lines.append(f'#EXTINF:-1 group-title="RUTUBE LIVE",{title}')
        output_lines.append(hls_url)
        existing_ids.add(video_id)
        added += 1

    dst = Path(args.output)
    dst.write_text("\n".join(output_lines).rstrip() + "\n", encoding="utf-8-sig")

    report = dst.with_name(dst.stem + "_scan_report.txt")
    report_lines = [
        "RUTUBE LIVE SCAN REPORT",
        f"Кандидатов из ленты: {len(candidates)}",
        f"Проверено play/options: {checked}",
        f"Получено HLS: {len(live_entries)}",
        f"Добавлено новых каналов: {added}",
        f"Ошибок запросов: {len(failed)}",
        "",
    ]
    if failed:
        report_lines.append("ОШИБКИ:")
        report_lines.extend(f"{vid}\t{error}" for vid, error in failed)
    report.write_text("\n".join(report_lines), encoding="utf-8")

    print("\n========== РЕЗУЛЬТАТ ==========")
    print(f"Кандидатов из ленты: {len(candidates)}")
    print(f"Рабочих HLS:         {len(live_entries)}")
    print(f"Добавлено новых:     {added}")
    print(f"Ошибок:              {len(failed)}")
    print(f"Плейлист:            {dst.resolve()}")
    print(f"Отчёт:               {report.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
