#!/usr/bin/env python3
# coding: utf-8
"""
git_bot_for_stable.py (Full Extended Version)
- читает локальные целевые файлы (gh-pages checkout) или создаёт их с нуля (#EXTM3U)
- загружает все 4 донора, проверяет их на "живость" (health-check)
- раскладывает базовые каналы по строгой сетке РТРС SD и HD с нумерацией 1-20
- фильтрует мусор и категории 18+
- добавляет новые рабочие потоки в конец целевых файлов с бэкапами
- поддерживает SKIP_PUSH для интеграции с GitHub Actions Workflow
"""

import os
import re
import subprocess
import urllib.request
import time
import shutil
import sys

# ---------- Конфигурация источников ----------
SOURCE_URL = "https://raw.githubusercontent.com/Phoenix89S/IpTV_playlist_2026Ru/main/ngenix_found_1.m3u"
SECOND_SOURCE_URL = "https://raw.githubusercontent.com/Free-TV/IPTV/refs/heads/master/playlists/playlist_russia.m3u8"
THIRD_SOURCE_URL = "https://raw.githubusercontent.com/Phoenix89S/Mir-IpTV-Ru-/main/Super_RTRS_2026.m3u"
TEST_CHANNELS_URL = "https://raw.githubusercontent.com/Phoenix89S/IpTV_playlist_2026Ru/gh-pages/test_channels.m3u"

# Переопределение через переменные окружения (для GitHub Actions)
SECOND_SOURCE_URL = os.environ.get("SECOND_SOURCE_URL", SECOND_SOURCE_URL)
SKIP_PUSH = os.environ.get("SKIP_PUSH", "true").lower() in ("1", "true", "yes")
TARGET_PUSH_BRANCH = os.environ.get("TARGET_PUSH_BRANCH", "gh-pages-2")

# Локальные целевые файлы
TARGET_MIR = "IPTVmir.m3u8"
TARGET_STABLE = "legacy/IPTVstable.m3u8"

# Резервные raw-фоллбеки
TARGET_MIR_RAW = "https://raw.githubusercontent.com/Phoenix89S/IPTVru/gh-pages/IPTVmir.m3u8"
TARGET_STABLE_RAW = "https://raw.githubusercontent.com/Phoenix89S/IPTVru/gh-pages/legacy/IPTVstable.m3u8"

# 🚫 ЧЁРНЫЙ СПИСОК (Только 18+ и нежелательный спам)
BLACKLIST = {
    "playboy", "erox", "blue hustler", "barely legal", 
    "adult", "private", "penthouse", "senstv", "o-la-la"
}

# Шаблоны для строгой привязки федеральных каналов РТРС (1-20) в две общие папки
RTRS_CHANNELS_MAP = {
    # РТРС SD (1-20)
    "1.sd первый канал": ("РТРС SD", "1.SD Первый канал"),
    "2.sd россия 1": ("РТРС SD", "2.SD Россия 1"),
    "3.sd матч тв": ("РТРС SD", "3.SD Матч ТВ"),
    "4.sd нтв": ("РТРС SD", "4.SD НТВ"),
    "5.sd пятый канал": ("РТРС SD", "5.SD Пятый канал"),
    "6.sd россия к": ("РТРС SD", "6.SD Россия К"),
    "7.sd россия 24": ("РТРС SD", "7.SD Россия 24"),
    "8.sd карусель": ("РТРС SD", "8.SD Карусель"),
    "9.sd отр": ("РТРС SD", "9.SD ОТР"),
    "10.sd тв центр": ("РТРС SD", "10.SD ТВ Центр"),
    "11.sd рен тв": ("РТРС SD", "11.SD РЕН ТВ"),
    "12.sd спас": ("РТРС SD", "12.SD Спас"),
    "13.sd стс": ("РТРС SD", "13.SD СТС"),
    "14.sd домашний": ("РТРС SD", "14.SD Домашний"),
    "15.sd тв-3": ("РТРС SD", "15.SD ТВ-3"),
    "16.sd пятница!": ("РТРС SD", "16.SD Пятница!"),
    "17.sd звезда": ("РТРС SD", "17.SD Звезда"),
    "18.sd мир": ("РТРС SD", "18.SD Мир"),
    "19.sd тнт": ("РТРС SD", "19.SD ТНТ"),
    "20.sd муз-тв": ("РТРС SD", "20.SD Муз-ТВ"),
    
    # РТРС HD (1-20)
    "1.hd первый канал hd": ("РТРС HD", "1.HD Первый канал HD"),
    "2.hd россия 1 hd": ("РТРС HD", "2.HD Россия 1 HD"),
    "3.hd матч тв hd": ("РТРС HD", "3.HD Матч ТВ HD"),
    "4.hd нтв hd": ("РТРС HD", "4.HD НТВ HD"),
    "5.hd пятый канал hd": ("РТРС HD", "5.HD Пятый канал HD"),
    "6.hd россия к hd": ("РТРС HD", "6.HD Россия К HD"),
    "7.hd россия 24 hd": ("РТРС HD", "7.HD Россия 24 HD"),
    "8.hd карусель hd": ("РТРС HD", "8.HD Карусель HD"),
    "9.hd отр hd": ("РТРС HD", "9.HD ОТР HD"),
    "10.hd тв центр hd": ("РТРС HD", "10.HD ТВ Центр HD"),
    "11.hd рен тв hd": ("РТРС HD", "11.HD РЕН ТВ HD"),
    "12.hd спас hd": ("РТРС HD", "12.HD Спас HD"),
    "13.hd стс hd": ("РТРС HD", "13.HD СТС HD"),
    "14.hd домашний hd": ("РТРС HD", "14.HD Домашний HD"),
    "15.hd тв-3 hd": ("РТРС HD", "15.HD ТВ-3 HD"),
    "16.hd пятница! hd": ("РТРС HD", "16.HD Пятница! HD"),
    "17.hd звезда hd": ("РТРС HD", "17.HD Звезда HD"),
    "18.hd мир hd": ("РТРС HD", "18.HD Мир HD"),
    "19.hd тнт hd": ("РТРС HD", "19.HD ТНТ HD"),
    "20.hd муз-тв hd": ("РТРС HD", "20.HD Муз-ТВ HD")
}

# ---------- Утилиты сети и Git ----------
def run(cmd, cwd=None, check=True):
    try:
        res = subprocess.run(cmd, cwd=cwd, check=check, capture_output=True, text=True)
        return res
    except subprocess.CalledProcessError as e:
        print(f"Команда {' '.join(cmd)} завершилась с ошибкой: {e}\nstderr:\n{e.stderr}")
        raise

def git_is_clean():
    res = run(["git", "status", "--porcelain"], check=True)
    return res.stdout.strip() == ""

def ensure_on_branch(branch="gh-pages"):
    run(["git", "fetch", "origin"], check=True)
    run(["git", "checkout", branch], check=True)
    run(["git", "pull", "origin", branch], check=True)

def fetch_url(url, timeout=12):
    try:
        req = urllib.request.Request(
            url, 
            headers={"User-Agent": "Mozilla/5.0 (compatible; IPTVBot/4.0)"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw_data = r.read()
            for enc in ('utf-8', 'cp1251', 'latin-1'):
                try:
                    return raw_data.decode(enc)
                except UnicodeDecodeError:
                    continue
            return raw_data.decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"Ошибка загрузки {url}: {e}")
        return ""

def check_stream_alive(url, timeout=5):
    """Быстрая проверка доступности потока (живой/мертвый)"""
    if not url or not url.startswith("http"):
        return False
    try:
        req = urllib.request.Request(
            url, 
            headers={"User-Agent": "VLC/3.0.16 LibVLC/3.0.16"},
            method="HEAD"
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status < 400
    except Exception:
        try:
            req = urllib.request.Request(
                url, 
                headers={"User-Agent": "VLC/3.0.16 LibVLC/3.0.16", "Range": "bytes=0-512"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return response.status < 400
        except Exception:
            return False

# ---------- Парсинг M3U ----------
def parse_m3u_content(content):
    lines = content.splitlines()
    entries = []
    current_meta = None
    tvg_re = re.compile(r'tvg-id="([^"]+)"', re.IGNORECASE)
    
    for line in lines:
        s = line.strip()
        if not s:
            continue
        if s.startswith("#EXTINF:"):
            current_meta = s
        elif not s.startswith("#"):
            url = s
            if current_meta:
                parts = current_meta.split(",", 1)
                name = parts[1].strip() if len(parts) > 1 else ""
                m = tvg_re.search(current_meta)
                key = m.group(1).strip().lower() if m else url.strip().lower()
                entries.append({'extinf': current_meta, 'url': url, 'key': key, 'name': name})
                current_meta = None
            else:
                entries.append({'extinf': '', 'url': url, 'key': url.strip().lower(), 'name': ''})
    return entries

def read_local_or_raw(path, raw_fallback=None):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    if raw_fallback:
        print(f"Локальный файл {path} не найден — пробуем raw {raw_fallback}")
        return fetch_url(raw_fallback)
    return ""

def is_blacklisted(title):
    t = (title or "").strip().lower()
    return any(bad in t for bad in BLACKLIST)

def has_digit_index_in_path(url):
    return bool(re.search(r'/\d+/', url))

def collect_existing_keys(path, raw_fallback=None):
    content = read_local_or_raw(path, raw_fallback=raw_fallback)
    if not content:
        return set()
    entries = parse_m3u_content(content)
    keys = set()
    for e in entries:
        if e.get('key'):
            keys.add(e['key'])
        if e.get('url'):
            keys.add(e['url'].strip().lower())
    return keys

def smart_format_entry(name, extinf, default_group="Стабильные ТВ"):
    clean_name_lower = name.strip().lower()
    
    for pattern, (group_title, formatted_name) in RTRS_CHANNELS_MAP.items():
        if pattern in clean_name_lower:
            if 'group-title=' in extinf:
                new_extinf = re.sub(r'group-title="[^"]*"', f'group-title="{group_title}"', extinf)
            else:
                meta = extinf.split(',')[0]
                new_extinf = f'{meta} group-title="{group_title}"'
            
            if ',' in new_extinf:
                new_extinf = new_extinf.split(',')[0] + f',{formatted_name}'
            else:
                new_extinf = f'{new_extinf},{formatted_name}'
            return new_extinf, group_title

    if not extinf:
        return f'#EXTINF:-1 group-title="{default_group}",{name}', default_group
    
    if 'group-title=' in extinf:
        return extinf, default_group
    else:
        meta, ch_name = extinf.split(',', 1) if ',' in extinf else (extinf, name)
        return f'{meta} group-title="{default_group}",{ch_name}', default_group

def backup_file(path):
    if os.path.exists(path):
        ts = time.strftime('%Y%m%d_%H%M%S')
        bak = f"{path}.bak_{ts}"
        shutil.copy2(path, bak)
        print(f"Создан backup: {bak}")

def append_group_if_new(target_path, donor_entries, raw_fallback=None):
    if not donor_entries:
        return 0

    file_exists = os.path.exists(target_path)
    existing_keys = collect_existing_keys(target_path, raw_fallback=raw_fallback)
    to_append = []
    
    print(f"Проверка работоспособности и уникальности потоков для {target_path}...")
    for e in donor_entries:
        key = e.get('key') or e.get('url') or e.get('extinf')
        url = e.get('url') or ""
        norm_url = url.strip().lower()
        name = e.get('name', '')
        
        if key in existing_keys or norm_url in existing_keys:
            continue
            
        if not check_stream_alive(url):
            continue  

        formatted_extinf, _ = smart_format_entry(name, e.get('extinf'))
        
        to_append.append((formatted_extinf, url))
        existing_keys.add(key)
        existing_keys.add(norm_url)

    if not to_append:
        return 0

    if file_exists:
        backup_file(target_path)

    os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)
    
    is_brand_new = not os.path.exists(target_path)

    with open(target_path, "a", encoding="utf-8") as f:
        if is_brand_new:
            f.write("#EXTM3U\n")
        f.write("\n")
        f.write(f"# ===== AUTO-UPDATED STABLE STREAMS ({time.strftime('%Y-%m-%d %H:%M:%S')}) =====\n")
        for extinf, url in to_append:
            f.write(extinf.rstrip() + "\n")
            f.write(url.rstrip() + "\n")

    return len(to_append)

# ---------- Основной поток ----------
def main():
    if not shutil.which("git"):
        print("Git не найден в PATH. Установите git и повторите.")
        sys.exit(1)

    if not git_is_clean():
        print("Рабочее дерево git не чистое. Очистите или закоммитьте изменения перед запуском.")
        sys.exit(1)

    branch_name = f"fix/restore-playlists-{time.strftime('%Y%m%d_%H%M%S')}"

    if not SKIP_PUSH:
        print(f"Создаём ветку: {branch_name}")
        try:
            ensure_on_branch("gh-pages")
            run(["git", "checkout", "-b", branch_name], check=True)
        except Exception as e:
            print(f"Не удалось создать ветку: {e}")
            sys.exit(1)
    else:
        print("SKIP_PUSH=true — работаем в текущем рабочем дереве для workflow.")

    donor_entries = []
    sources = [
        ("SOURCE_URL", SOURCE_URL, False),
        ("SECOND_SOURCE_URL", SECOND_SOURCE_URL, True),
        ("THIRD_SOURCE_URL", THIRD_SOURCE_URL, True),
        ("TEST_CHANNELS_URL", TEST_CHANNELS_URL, False)
    ]

    for name, url, apply_filter in sources:
        if not url:
            continue
        print(f"Загрузка {name}: {url}")
        raw_data = fetch_url(url)
        if raw_data:
            entries = parse_m3u_content(raw_data)
            valid_count = 0
            for e in entries:
                ch_name = e.get('name', '')
                ch_url = e.get('url', '')
                
                if apply_filter:
                    if is_blacklisted(ch_name) or has_digit_index_in_path(ch_url):
                        continue
                
                e['source'] = name
                valid_count += 1
                donor_entries.append(e)
            print(f"Из {name} добавлено в пул: {valid_count} записей")
        else:
            print(f"Источник {name} недоступен или пуст.")

    if not donor_entries:
        print("Нет донорских записей для обработки.")
        sys.exit(0)

    modified = []
    added_total = 0

    for target, raw_fb in [(TARGET_MIR, TARGET_MIR_RAW), (TARGET_STABLE, TARGET_STABLE_RAW)]:
        added = append_group_if_new(target, donor_entries, raw_fallback=raw_fb)
        if added:
            print(f"В {target} добавлено новых рабочих потоков: {added}")
            modified.append(target)
            added_total += added
        else:
            print(f"В {target} новых рабочих потоков не обнаружено.")

    if not modified:
        print("Изменений нет. Новые рабочие потоки не прошли проверку или уже существуют.")
        if not SKIP_PUSH:
            try:
                run(["git", "checkout", "gh-pages"], check=True)
                run(["git", "branch", "-D", branch_name], check=True)
            except Exception:
                pass
        sys.exit(0)

    if SKIP_PUSH:
        print(f"Готово! Автоматически добавлено рабочих потоков: {added_total}. Файлы готовы для отправки через workflow.")
        sys.exit(0)

    try:
        run(["git", "add"] + modified, check=True)
        commit_msg = f"chore: auto-update IPTV streams & RTRS grid ({time.strftime('%Y-%m-%d %H:%M:%S')})"
        run(["git", "commit", "-m", commit_msg], check=True)
        run(["git", "push", "-u", "origin", branch_name], check=True)
        print(f"Успешно запушено в ветку: {branch_name}")
    except Exception as e:
        print(f"Ошибка при коммите/пуше: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
