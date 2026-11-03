#!/usr/bin/env python3
# coding: utf-8
"""
git_bot_for_stable.py (Advanced Multi-Source & RTRS Smart Grid)
- читает локальные целевые файлы (gh-pages checkout)
- загружает несколько доноров, проверяет их на "живость" (health-check)
- раскладывает базовые каналы по строгой сетке РТРС 1/2 (SD и HD) с нумерацией 1-20
- фильтрует мусор и 18+
- добавляет новые рабочие потоки в конец целевых файлов
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

# Шаблоны для строгой привязки федеральных каналов РТРС (1-20)
RTRS_CHANNELS_MAP = {
    # РТРС 1 (SD)
    "1.sd первый канал": ("РТРС 1", "1.SD Первый канал"),
    "2.sd россия 1": ("РТРС 1", "2.SD Россия 1"),
    "3.sd матч тв": ("РТРС 1", "3.SD Матч ТВ"),
    "4.sd нтв": ("РТРС 1", "4.SD НТВ"),
    "5.sd пятый канал": ("РТРС 1", "5.SD Пятый канал"),
    "6.sd россия к": ("РТРС 1", "6.SD Россия К"),
    "7.sd россия 24": ("РТРС 1", "7.SD Россия 24"),
    "8.sd карусель": ("РТРС 1", "8.SD Карусель"),
    "9.sd отр": ("РТРС 1", "9.SD ОТР"),
    "10.sd тв центр": ("РТРС 1", "10.SD ТВ Центр"),
    # РТРС 2 (SD)
    "11.sd рен тв": ("РТРС 2", "11.SD РЕН ТВ"),
    "12.sd спас": ("РТРС 2", "12.SD Спас"),
    "13.sd стс": ("РТРС 2", "13.SD СТС"),
    "14.sd домашний": ("РТРС 2", "14.SD Домашний"),
    "15.sd тв-3": ("РТРС 2", "15.SD ТВ-3"),
    "16.sd пятница!": ("РТРС 2", "16.SD Пятница!"),
    "17.sd звезда": ("РТРС 2", "17.SD Звезда"),
    "18.sd мир": ("РТРС 2", "18.SD Мир"),
    "19.sd тнт": ("РТРС 2", "19.SD ТНТ"),
    "20.sd муз-тв": ("РТРС 2", "20.SD Муз-ТВ"),
    # РТРС 1 HD
    "1.hd первый канал hd": ("РТРС 1 HD", "1.HD Первый канал HD"),
    "2.hd россия 1 hd": ("РТРС 1 HD", "2.HD Россия 1 HD"),
    "3.hd матч тв hd": ("РТРС 1 HD", "3.HD Матч ТВ HD"),
    "4.hd нтв hd": ("РТРС 1 HD", "4.HD НТВ HD"),
    "5.hd пятый канал hd": ("РТРС 1 HD", "5.HD Пятый канал HD"),
    "6.hd россия к hd": ("РТРС 1 HD", "6.HD Россия К HD"),
    "7.hd россия 24 hd": ("РТРС 1 HD", "7.HD Россия 24 HD"),
    "8.hd карусель hd": ("РТРС 1 HD", "8.HD Карусель HD"),
    "9.hd отр hd": ("РТРС 1 HD", "9.HD ОТР HD"),
    "10.hd тв центр hd": ("РТРС 1 HD", "10.HD ТВ Центр HD"),
    # РТРС 2 HD
    "11.hd рен тв hd": ("РТРС 2 HD", "11.HD РЕН ТВ HD"),
    "12.hd спас hd": ("РТРС 2 HD", "12.HD Спас HD"),
    "13.hd стс hd": ("РТРС 2 HD", "13.HD СТС HD"),
    "14.hd домашний hd": ("РТРС 2 HD", "14.HD Домашний HD"),
    "15.hd тв-3 hd": ("РТРС 2 HD", "15.HD ТВ-3 HD"),
    "16.hd пятница! hd": ("РТРС 2 HD", "16.HD Пятница! HD"),
    "17.hd звезда hd": ("РТРС 2 HD", "17.HD Звезда HD"),
    "18.hd мир hd": ("РТРС 2 HD", "18.HD Мир HD"),
    "19.hd тнт hd": ("РТРС 2 HD", "19.HD ТНТ HD"),
    "20.hd муз-тв hd": ("РТРС 2 HD", "20.HD Муз-ТВ HD")
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
        # Если HEAD не поддержан сервером, пробуем открыть кусочек через GET
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
    """Определяет, попадает ли канал под жесткую сетку РТРС (1-20), либо оставляет свою группу"""
    clean_name_lower = name.strip().lower()
    
    # Проверяем, есть ли канал в нашей фиксированной карте РТРС
    for pattern, (group_title, formatted_name) in RTRS_CHANNELS_MAP.items():
        if pattern in clean_name_lower:
            # Пересобираем extinf с нужной группой и красивым именем
            if 'group-title=' in extinf:
                new_extinf = re.sub(r'group-title="[^"]*"', f'group-title="{group_title}"', extinf)
            else:
                meta = extinf.split(',')[0]
                new_extinf = f'{meta} group-title="{group_title}"'
            
            # Меняем название на правильное отформатированное
            if ',' in new_extinf:
                new_extinf = new_extinf.split(',')[0] + f',{formatted_name}'
            else:
                new_extinf = f'{new_extinf},{formatted_name}'
            return new_extinf, group_title

    # Если это обычный канал (не входит в топ-20 эфирных), оставляем его в стандартной группе
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
            
        # Проверка на "живость" потока
        if not check_stream_alive(url):
            continue  # Мертвые потоки не добавляем!

        # Форматируем имя и группу (РТРС 1/2 SD/HD или Стабильные ТВ)
        formatted_extinf, _ = smart_format_entry(name, e.get('extinf'))
        
        to_append.append((formatted_extinf, url))
        existing_keys.add(key)
        existing_keys.add(norm_url)

    if not to_append:
        return 0

    backup_file(target_path)

    os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)
    with open(target_path, "a", encoding="utf-8") as f:
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

    # Сбор со всех источников
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
                
                # Фильтрация (blacklist + цифровые мусорные индексы в путях)
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

    # Обновление целевых файлов с проверкой работоспособности
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

    # Коммит и пуш (если SKIP_PUSH = false)
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
