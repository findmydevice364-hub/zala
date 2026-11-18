#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phoenix Hub "Baryshnikov" — Shirt.uz Collector
Рабочая версия с надёжным парсером страницы
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit, unquote

import requests


# ====================== НАСТРОЙКИ ======================

INDEX_URL = "https://shirtv.uz/playlists.html"

SCRIPT_DIR = Path(__file__).resolve().parent
if SCRIPT_DIR.name.lower() == "main":
    ROOT_DIR = SCRIPT_DIR.parent
    MAIN_DIR = SCRIPT_DIR
else:
    ROOT_DIR = SCRIPT_DIR
    MAIN_DIR = ROOT_DIR / "Main"

SHIRT_DIR = ROOT_DIR / "Shirt_uz"
MAIN_LOG_DIR = MAIN_DIR / "logs"
SHIRT_LOG_DIR = SHIRT_DIR / "logs"

MAIN_DONORS = MAIN_DIR / "donors_uz.m3u"
MAIN_STABLE = MAIN_DIR / "stable_uz.m3u"
SHIRT_DONORS = SHIRT_DIR / "donors_uz.m3u"
SHIRT_STABLE = SHIRT_DIR / "stable_uz.m3u"

FFPROBE_DIR = ROOT_DIR / ".tools" / "ffmpeg"

MAX_DOWNLOAD_WORKERS = 24
MAX_PARSE_WORKERS = 32
MAX_VALIDATION_WORKERS = 40

REQUEST_TIMEOUT = 25
MAX_RETRIES = 3
MAX_PLAYLIST_DEPTH = 1
ENABLE_FFPROBE = True
KEEP_INCONCLUSIVE = True

# ========================================================

RUN_ID = uuid.uuid4().hex[:12].upper()
STOP_EVENT = threading.Event()
HTTP_LOCAL = threading.local()

DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(MAX_DOWNLOAD_WORKERS)
VALIDATION_SEMAPHORE = threading.BoundedSemaphore(MAX_VALIDATION_WORKERS)

COUNTERS = {
    "sources_discovered": 0,
    "sources_downloaded": 0,
    "source_errors": 0,
    "donor_entries": 0,
    "adult_filtered": 0,
    "checked": 0,
    "accepted": 0,
    "rejected": 0,
    "inconclusive": 0,
}

COUNTER_LOCK = threading.Lock()
SOURCE_ERRORS = []
SOURCE_ERRORS_LOCK = threading.Lock()


def counter_add(name, value=1):
    with COUNTER_LOCK:
        COUNTERS[name] = COUNTERS.get(name, 0) + value


def log(msg, **kwargs):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)
    if kwargs:
        for k, v in kwargs.items():
            print(f"         {k}: {v}", flush=True)


# ====================== HTTP ======================

def get_session():
    session = getattr(HTTP_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        })
        HTTP_LOCAL.session = session
    return session


def fetch(url, is_html=False, referer=None):
    headers = {}
    if is_html:
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    else:
        headers["Accept"] = "*/*"
    if referer:
        headers["Referer"] = referer

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        if STOP_EVENT.is_set():
            raise RuntimeError("Interrupted")

        DOWNLOAD_SEMAPHORE.acquire()
        try:
            resp = get_session().get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1))
        finally:
            DOWNLOAD_SEMAPHORE.release()

    raise RuntimeError(f"Failed to download {url}: {last_error}")


# ====================== ПОИСК ПЛЕЙЛИСТОВ (РАБОЧИЙ) ======================

def discover_playlist_urls():
    log("Загружаю страницу каталога...", url=INDEX_URL)

    html_bytes = fetch(INDEX_URL, is_html=True)
    html = html_bytes.decode("utf-8", errors="replace")

    # Самый надёжный способ — несколько простых регулярных выражений
    patterns = [
        r'href=["\'](playlists/[^"\']+\.m3u8?)["\']',
        r'data-url=["\'](https?://shirtv\.uz/playlists/[^"\']+\.m3u8?)["\']',
        r'(https?://shirtv\.uz/playlists/[^"\'\s<>]+\.m3u8?)',
        r'(playlists/[^"\'\s<>]+\.m3u8?)',
    ]

    candidates = []
    for pat in patterns:
        found = re.findall(pat, html, flags=re.IGNORECASE)
        candidates.extend(found)

    result = []
    seen = set()

    for href in candidates:
        href = href.strip().strip("'\"")
        if not href:
            continue

        if href.startswith("http"):
            absolute = href
        else:
            absolute = "https://shirtv.uz/" + href.lstrip("/")

        # Принудительно https
        if absolute.startswith("http://"):
            absolute = "https://" + absolute[7:]

        # Нормализация
        key = absolute.lower().split("?")[0]
        if key in seen:
            continue
        if not key.endswith((".m3u", ".m3u8")):
            continue

        seen.add(key)
        result.append(absolute)

    counter_add("sources_discovered", len(result))

    log(f"Найдено плейлистов: {len(result)}")
    for i, u in enumerate(result[:30], 1):
        print(f"  {i:2d}. {u}")
    if len(result) > 30:
        print(f"  ... и ещё {len(result) - 30}")

    if not result:
        # Сохраняем страницу для отладки
        debug_file = MAIN_DIR / "debug_page.html"
        try:
            MAIN_DIR.mkdir(parents=True, exist_ok=True)
            debug_file.write_text(html, encoding="utf-8")
            log("Страница сохранена для отладки", path=str(debug_file))
        except Exception:
            pass
        raise RuntimeError("Не удалось найти ни одной ссылки на .m3u / .m3u8")

    return result


# ====================== ПАРСИНГ M3U ======================

def parse_m3u(content: str, source: str):
    entries = []
    lines = content.splitlines()
    current_extinf = None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            current_extinf = line
            continue
        if line.startswith("#"):
            continue

        url = urljoin(source, line)
        if not url.startswith("http"):
            continue

        name = "Без названия"
        if current_extinf and "," in current_extinf:
            name = current_extinf.split(",", 1)[1].strip() or name

        entries.append({
            "extinf": current_extinf or f'#EXTINF:-1,{name}',
            "url": url,
            "source": source,
        })
        current_extinf = None

    return entries


def is_adult(entry):
    text = (entry.get("extinf", "") + " " + entry.get("url", "")).lower()
    return any(x in text for x in ["18+", "xxx", "porn", "adult", "эрот", "для взрослых"])


# ====================== СКАЧИВАНИЕ И СБОР ======================

def process_one_source(url):
    try:
        data = fetch(url, referer=INDEX_URL)
        content = data.decode("utf-8", errors="replace")
        entries = parse_m3u(content, url)

        clean = []
        for e in entries:
            if is_adult(e):
                counter_add("adult_filtered")
                continue
            clean.append(e)

        counter_add("sources_downloaded")
        counter_add("donor_entries", len(clean))
        log(f"OK  {url} → {len(clean)} каналов")
        return clean
    except Exception as e:
        counter_add("source_errors")
        log(f"FAIL {url} → {e}")
        with SOURCE_ERRORS_LOCK:
            SOURCE_ERRORS.append({"url": url, "error": str(e)})
        return []


def collect_all(sources):
    all_entries = []
    log(f"Начинаю скачивание {len(sources)} плейлистов...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as pool:
        futures = [pool.submit(process_one_source, url) for url in sources]
        for fut in concurrent.futures.as_completed(futures):
            try:
                all_entries.extend(fut.result())
            except Exception as e:
                log(f"Worker error: {e}")

    log(f"Всего собрано записей: {len(all_entries)}")
    return all_entries


# ====================== ПРОВЕРКА ПОТОКОВ ======================

def check_stream(url):
    try:
        r = get_session().head(url, timeout=12, allow_redirects=True)
        if 200 <= r.status_code < 400:
            return True
        if r.status_code in (404, 410):
            return False
    except Exception:
        pass

    try:
        r = get_session().get(url, headers={"Range": "bytes=0-1023"}, timeout=12, stream=True)
        chunk = next(r.iter_content(1024), b"")
        r.close()
        if chunk and 200 <= r.status_code < 400:
            return True
    except Exception:
        pass

    return None  # inconclusive


def validate_entries(entries):
    stable = []
    log(f"Проверяю {len(entries)} потоков...")

    def worker(entry):
        result = check_stream(entry["url"])
        return entry, result

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as pool:
        futures = [pool.submit(worker, e) for e in entries]
        for fut in concurrent.futures.as_completed(futures):
            entry, result = fut.result()
            counter_add("checked")
            if result is True:
                stable.append(entry)
                counter_add("accepted")
            elif result is False:
                counter_add("rejected")
            else:
                if KEEP_INCONCLUSIVE:
                    stable.append(entry)
                    counter_add("accepted")
                    counter_add("inconclusive")
                else:
                    counter_add("rejected")

    log(f"После проверки осталось: {len(stable)}")
    return stable


# ====================== ЗАПИСЬ ======================

def write_m3u(path: Path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for e in entries:
            f.write(e["extinf"].rstrip() + "\n")
            f.write(e["url"].rstrip() + "\n")
    tmp.replace(path)
    log(f"Записан файл: {path} ({len(entries)} записей)")


# ====================== MAIN ======================

def main():
    start = time.time()
    print("=" * 60)
    print("Phoenix Hub Baryshnikov — Shirt.uz Collector")
    print("=" * 60)

    MAIN_DIR.mkdir(parents=True, exist_ok=True)
    SHIRT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Находим все плейлисты
        sources = discover_playlist_urls()

        # 2. Скачиваем и парсим
        donors = collect_all(sources)
        if not donors:
            raise RuntimeError("Не удалось собрать ни одной записи")

        # 3. Проверяем потоки
        stable = validate_entries(donors)

        # 4. Сохраняем
        write_m3u(MAIN_DONORS, donors)
        write_m3u(MAIN_STABLE, stable)
        write_m3u(SHIRT_DONORS, donors)
        write_m3u(SHIRT_STABLE, stable)

        elapsed = time.time() - start
        print("\n" + "=" * 60)
        print("ГОТОВО")
        print(f"Найдено источников : {COUNTERS['sources_discovered']}")
        print(f"Скачано            : {COUNTERS['sources_downloaded']}")
        print(f"Ошибок скачивания  : {COUNTERS['source_errors']}")
        print(f"Всего записей      : {COUNTERS['donor_entries']}")
        print(f"После проверки     : {len(stable)}")
        print(f"Время работы       : {elapsed:.1f} сек")
        print("=" * 60)

        return 0

    except KeyboardInterrupt:
        print("\nПрервано пользователем")
        return 130
    except Exception as e:
        print(f"\n[FATAL] {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())