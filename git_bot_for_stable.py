
#!/usr/bin/env python3
# coding: utf-8
"""
git_bot_for_stable.py
Universal Multi-threaded Full Logic Version
===========================================

Полная логика обоих исходных скриптов + все доработки:

- Многопоточная загрузка всех донорских источников (ThreadPoolExecutor)
- Многопоточная проверка живости потоков (HEAD + Range fallback)
- Фильтр только 18+ (минимальный blacklist)
- Без дедупликации — добавляем все прошедшие проверку записи
- РТРС SD / РТРС HD (1–20) с индексацией вариантов:
    1.SD Первый канал
    1.SD.1 Первый канал
    1.HD Первый канал HD
    1.HD.1 Первый канал HD
- Тематические группы для остальных каналов:
    Новости, Музыка, Детские, Кино, Познавательные,
    Развлекательные, Научные, Прочее
- Целевые файлы:
    IPTVmir.m3u8
    legacy/IPTVstable.m3u8
    stable.m3u
    RTRS_stable.m3u8
- Поддержка SKIP_PUSH для GitHub Actions
- Создание #EXTM3U если файла нет
- Бэкапы перед изменением
- Полная git-логика (ветка / коммит / пуш)
"""
#Phoenix89S_Lab© findmydevice364-hub_lab
#Ver3.0

import os
import re
import subprocess
import urllib.request
import time
import shutil
import sys
import concurrent.futures
from collections import defaultdict

# ==============================================================================
# КОНФИГУРАЦИЯ ИСТОЧНИКОВ
# ==============================================================================

SOURCE_URL = "https://raw.githubusercontent.com/Phoenix89S/IpTV_playlist_2026Ru/main/ngenix_found_1.m3u"
SECOND_SOURCE_URL = "https://raw.githubusercontent.com/Free-TV/IPTV/refs/heads/master/playlists/playlist_russia.m3u8"
THIRD_SOURCE_URL = "https://raw.githubusercontent.com/Phoenix89S/Mir-IpTV-Ru-/main/Super_RTRS_2026.m3u"
TEST_CHANNELS_URL = "https://raw.githubusercontent.com/Phoenix89S/IpTV_playlist_2026Ru/gh-pages/test_channels.m3u"

# Переопределение через переменные окружения (для GitHub Actions)
SECOND_SOURCE_URL = os.environ.get("SECOND_SOURCE_URL", SECOND_SOURCE_URL)
SKIP_PUSH = os.environ.get("SKIP_PUSH", "true").lower() in ("1", "true", "yes")
TARGET_PUSH_BRANCH = os.environ.get("TARGET_PUSH_BRANCH", "gh-pages-2")

# ==============================================================================
# ЦЕЛЕВЫЕ ФАЙЛЫ
# ==============================================================================

TARGET_MIR = "IPTVmir.m3u8"
TARGET_STABLE_LEGACY = "legacy/IPTVstable.m3u8"
TARGET_STABLE = "stable.m3u"
TARGET_RTRS_STABLE = "RTRS_stable.m3u8"

# Резервные raw-фоллбеки (оставлены на будущее)
TARGET_MIR_RAW = "https://raw.githubusercontent.com/Phoenix89S/IPTVru/gh-pages/IPTVmir.m3u8"
TARGET_STABLE_LEGACY_RAW = "https://raw.githubusercontent.com/Phoenix89S/IPTVru/gh-pages/legacy/IPTVstable.m3u8"

# ==============================================================================
# ЧЁРНЫЙ СПИСОК (только 18+)
# ==============================================================================

BLACKLIST = {
    "playboy", "erox", "blue hustler", "barely legal",
    "adult", "private", "penthouse", "senstv", "o-la-la",
    "xxx", "18+", "porn", "hustler"
}

# ==============================================================================
# СТРОГАЯ СЕТКА РТРС (первые два мультиплекса SD + HD)
# ==============================================================================

RTRS_CHANNELS_MAP = {
    # ----- РТРС SD (1-й и 2-й мультиплекс) -----
    "1.sd первый канал":       ("РТРС SD", "1.SD Первый канал", 1),
    "2.sd россия 1":           ("РТРС SD", "2.SD Россия 1", 2),
    "3.sd матч тв":            ("РТРС SD", "3.SD Матч ТВ", 3),
    "4.sd нтв":                ("РТРС SD", "4.SD НТВ", 4),
    "5.sd пятый канал":        ("РТРС SD", "5.SD Пятый канал", 5),
    "6.sd россия к":           ("РТРС SD", "6.SD Россия К", 6),
    "7.sd россия 24":          ("РТРС SD", "7.SD Россия 24", 7),
    "8.sd карусель":           ("РТРС SD", "8.SD Карусель", 8),
    "9.sd отр":                ("РТРС SD", "9.SD ОТР", 9),
    "10.sd тв центр":          ("РТРС SD", "10.SD ТВ Центр", 10),
    "11.sd рен тв":            ("РТРС SD", "11.SD РЕН ТВ", 11),
    "12.sd спас":              ("РТРС SD", "12.SD Спас", 12),
    "13.sd стс":               ("РТРС SD", "13.SD СТС", 13),
    "14.sd домашний":          ("РТРС SD", "14.SD Домашний", 14),
    "15.sd тв-3":              ("РТРС SD", "15.SD ТВ-3", 15),
    "16.sd пятница!":          ("РТРС SD", "16.SD Пятница!", 16),
    "17.sd звезда":            ("РТРС SD", "17.SD Звезда", 17),
    "18.sd мир":               ("РТРС SD", "18.SD Мир", 18),
    "19.sd тнт":               ("РТРС SD", "19.SD ТНТ", 19),
    "20.sd муз-тв":            ("РТРС SD", "20.SD Муз-ТВ", 20),

    # ----- РТРС HD -----
    "1.hd первый канал hd":    ("РТРС HD", "1.HD Первый канал HD", 101),
    "2.hd россия 1 hd":        ("РТРС HD", "2.HD Россия 1 HD", 102),
    "3.hd матч тв hd":         ("РТРС HD", "3.HD Матч ТВ HD", 103),
    "4.hd нтв hd":             ("РТРС HD", "4.HD НТВ HD", 104),
    "5.hd пятый канал hd":     ("РТРС HD", "5.HD Пятый канал HD", 105),
    "6.hd россия к hd":        ("РТРС HD", "6.HD Россия К HD", 106),
    "7.hd россия 24 hd":       ("РТРС HD", "7.HD Россия 24 HD", 107),
    "8.hd карусель hd":        ("РТРС HD", "8.HD Карусель HD", 108),
    "9.hd отр hd":             ("РТРС HD", "9.HD ОТР HD", 109),
    "10.hd тв центр hd":       ("РТРС HD", "10.HD ТВ Центр HD", 110),
    "11.hd рен тв hd":         ("РТРС HD", "11.HD РЕН ТВ HD", 111),
    "12.hd спас hd":           ("РТРС HD", "12.HD Спас HD", 112),
    "13.hd стс hd":            ("РТРС HD", "13.HD СТС HD", 113),
    "14.hd домашний hd":       ("РТРС HD", "14.HD Домашний HD", 114),
    "15.hd тв-3 hd":           ("РТРС HD", "15.HD ТВ-3 HD", 115),
    "16.hd пятница! hd":       ("РТРС HD", "16.HD Пятница! HD", 116),
    "17.hd звезда hd":         ("РТРС HD", "17.HD Звезда HD", 117),
    "18.hd мир hd":            ("РТРС HD", "18.HD Мир HD", 118),
    "19.hd тнт hd":            ("РТРС HD", "19.HD ТНТ HD", 119),
    "20.hd муз-тв hd":         ("РТРС HD", "20.HD Муз-ТВ HD", 120),
}

# ==============================================================================
# ТЕМАТИЧЕСКИЕ ГРУППЫ (порядок важен)
# ==============================================================================

CATEGORY_RULES = [
    ("Новости", [
        "новост", "news", "россия 24", "рбк", "rbc", "известия", "жизнь",
        "москва 24", "миротворец", "rt", "russia today", "bbc", "cnn",
        "euronews", "tv rain", "дождь", "current time", "наст. время",
        "360", "москва 24", "life", "life news"
    ]),
    ("Музыка", [
        "муз", "music", "радио", "radio", "hit", "mtv", "vh1", "jazz", "rock",
        "dance", "hip-hop", "hiphop", "shanson", "шансон", "europa plus",
        "love radio", "bridge", "bridge tv", "ru.tv", "music box"
    ]),
    ("Детские", [
        "дет", "kids", "cartoon", "мульт", "карусель", "disney", "nickelodeon",
        "baby", "детский", "мультик", "tiaradio", "cartoon network",
        "nick jr", "tiara", "мультиландия"
    ]),
    ("Кино", [
        "кино", "movie", "film", "cinema", "фильм", "кинохит", "кинопремьера",
        "premiere", "киносемья", "kino", "fox", "hbo", "amc", "sony",
        "кинопоказ", "кинокомедия", "индийское кино", "наше кино"
    ]),
    ("Познавательные", [
        "познават", "discovery", "national geographic", "nat geo", "history",
        "animal", "наука", "doc", "докум", "viasat", "viasat history",
        "viasat nature", "animal planet", "discovery science"
    ]),
    ("Развлекательные", [
        "развлек", "comedy", "юмор", "тв3", "пятница", "тнт", "ctc", "стс",
        "развлека", "юморист", "квн", "comedy club", "тнт4", "2x2"
    ]),
    ("Научные", [
        "науч", "science", "cosmos", "space", "tech", "техно",
        "наука 2.0", "science 2.0", "galaxy", "cosmos tv"
    ]),
]

# ==============================================================================
# УТИЛИТЫ GIT И СЕТИ
# ==============================================================================

def run(cmd, cwd=None, check=True):
    try:
        res = subprocess.run(cmd, cwd=cwd, check=check, capture_output=True, text=True)
        return res
    except subprocess.CalledProcessError as e:
        print(f"Команда {' '.join(cmd)} завершилась с ошибкой: {e}")
        print(f"stderr:\n{e.stderr}")
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
            for enc in ("utf-8", "cp1251", "latin-1"):
                try:
                    return raw_data.decode(enc)
                except UnicodeDecodeError:
                    continue
            return raw_data.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"Ошибка загрузки {url}: {e}")
        return ""

def check_stream_alive(url, timeout=5):
    """
    Быстрая проверка живости потока.
    Сначала HEAD, при неудаче — GET с Range bytes=0-512.
    """
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
                headers={
                    "User-Agent": "VLC/3.0.16 LibVLC/3.0.16",
                    "Range": "bytes=0-512"
                }
            )
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return response.status < 400
        except Exception:
            return False

# ==============================================================================
# ПАРСИНГ M3U
# ==============================================================================

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
                entries.append({
                    "extinf": current_meta,
                    "url": url,
                    "key": key,
                    "name": name
                })
                current_meta = None
            else:
                entries.append({
                    "extinf": "",
                    "url": url,
                    "key": url.strip().lower(),
                    "name": ""
                })
    return entries

def is_blacklisted(title):
    t = (title or "").strip().lower()
    return any(bad in t for bad in BLACKLIST)

def has_digit_index_in_path(url):
    return bool(re.search(r"/\d+/", url))

def match_rtrs(name):
    """Возвращает (group, base_name, order) или None"""
    clean = (name or "").strip().lower()
    for pattern, (group, base_name, order) in RTRS_CHANNELS_MAP.items():
        if pattern in clean:
            return group, base_name, order
    return None

def detect_category(name):
    """Определяет тематическую группу по названию канала"""
    clean = (name or "").strip().lower()
    for category, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in clean:
                return category
    return "Прочее"

def smart_format_entry(name, extinf, default_group="Стабильные ТВ"):
    """
    Применяет РТРС-сетку если канал совпал,
    иначе ставит group-title по умолчанию.
    """
    matched = match_rtrs(name)
    if matched:
        group_title, formatted_name, _ = matched
        if "group-title=" in (extinf or ""):
            new_extinf = re.sub(r'group-title="[^"]*"', f'group-title="{group_title}"', extinf)
        else:
            meta = (extinf or "#EXTINF:-1").split(",")[0]
            new_extinf = f'{meta} group-title="{group_title}"'
        if "," in new_extinf:
            new_extinf = new_extinf.split(",")[0] + f",{formatted_name}"
        else:
            new_extinf = f"{new_extinf},{formatted_name}"
        return new_extinf, group_title

    if not extinf:
        return f'#EXTINF:-1 group-title="{default_group}",{name}', default_group

    if "group-title=" in extinf:
        return extinf, default_group
    else:
        meta, ch_name = extinf.split(",", 1) if "," in extinf else (extinf, name)
        return f'{meta} group-title="{default_group}",{ch_name}', default_group

def backup_file(path):
    if os.path.exists(path):
        ts = time.strftime("%Y%m%d_%H%M%S")
        bak = f"{path}.bak_{ts}"
        shutil.copy2(path, bak)
        print(f"Создан backup: {bak}")

def process_single_stream_check(e):
    """Воркер для многопоточной проверки живости потока"""
    url = e.get("url", "")
    if check_stream_alive(url):
        return e
    return None

# ==============================================================================
# ОБЫЧНОЕ ДОБАВЛЕНИЕ (без дедупликации)
# ==============================================================================

def append_all_valid(target_path, donor_entries):
    """
    Без дедупликации.
    Добавляем ВСЕ прошедшие фильтр + health-check записи.
    Если файла нет — создаём с #EXTM3U.
    """
    if not donor_entries:
        return 0

    print(f"Многопоточная проверка работоспособности {len(donor_entries)} потоков для {target_path}...")
    valid_candidates = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
        results = executor.map(process_single_stream_check, donor_entries)
        for res in results:
            if res:
                valid_candidates.append(res)

    if not valid_candidates:
        return 0

    to_append = []
    for e in valid_candidates:
        name = e.get("name", "")
        formatted_extinf, _ = smart_format_entry(name, e.get("extinf", ""))
        to_append.append((formatted_extinf, e.get("url", "")))

    file_exists = os.path.exists(target_path)
    if file_exists:
        backup_file(target_path)

    os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)

    with open(target_path, "a", encoding="utf-8") as f:
        if not file_exists:
            f.write("#EXTM3U\n")
        f.write("\n")
        f.write(f"# ===== AUTO-UPDATED STABLE STREAMS ({time.strftime('%Y-%m-%d %H:%M:%S')}) =====\n")
        for extinf, url in to_append:
            f.write(extinf.rstrip() + "\n")
            f.write(url.rstrip() + "\n")

    return len(to_append)

# ==============================================================================
# СПЕЦИАЛЬНАЯ ЛОГИКА RTRS_stable.m3u8
# ==============================================================================

def build_rtrs_stable(donor_entries, target_path=TARGET_RTRS_STABLE):
    """
    Формирует RTRS_stable.m3u8:

    1. Блок РТРС SD / РТРС HD (первые два мультиплекса 1–20)
       - Варианты одного канала получают индекс:
         1.SD Первый канал
         1.SD.1 Первый канал
         1.HD Первый канал HD
         1.HD.1 Первый канал HD

    2. Блок тематических групп для всех остальных живых каналов:
       Новости → Музыка → Детские → Кино →
       Познавательные → Развлекательные → Научные → Прочее
    """
    print(f"Формирование {target_path} (РТРС мультиплексы + тематика)...")

    rtrs_candidates = []
    other_candidates = []

    for e in donor_entries:
        matched = match_rtrs(e.get("name", ""))
        if matched:
            group, base_name, order = matched
            e["_rtrs"] = (group, base_name, order)
            rtrs_candidates.append(e)
        else:
            other_candidates.append(e)

    print(f"РТРС-кандидатов: {len(rtrs_candidates)}")
    print(f"Остальных кандидатов: {len(other_candidates)}")
    print("Многопоточная проверка живости...")

    valid_rtrs = []
    valid_other = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
        for res in executor.map(process_single_stream_check, rtrs_candidates):
            if res:
                valid_rtrs.append(res)
        for res in executor.map(process_single_stream_check, other_candidates):
            if res:
                valid_other.append(res)

    print(f"Живых РТРС-потоков: {len(valid_rtrs)}")
    print(f"Живых остальных потоков: {len(valid_other)}")

    lines = ["#EXTM3U"]
    lines.append(f"# Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    total = 0

    # --------------------------------------------------------------------------
    # Блок 1: РТРС SD + HD (с вариантами .1 .2 ...)
    # --------------------------------------------------------------------------
    if valid_rtrs:
        slots = defaultdict(list)
        for e in valid_rtrs:
            group, base_name, order = e["_rtrs"]
            slots[base_name].append({
                "url": e["url"],
                "order": order,
                "group": group
            })

        sorted_slots = sorted(slots.items(), key=lambda x: x[1][0]["order"])

        lines.append("# ===== РТРС SD / РТРС HD (первые два мультиплекса) =====")
        for base_name, variants in sorted_slots:
            for idx, v in enumerate(variants):
                if idx == 0:
                    display_name = base_name
                else:
                    # Индексация работает одинаково для SD и HD:
                    # 1.SD → 1.SD.1, 1.HD → 1.HD.1 и т.д.
                    prefix = base_name.split(" ", 1)[0]
                    rest = base_name[len(prefix):].lstrip()
                    display_name = f"{prefix}.{idx} {rest}"

                lines.append(f'#EXTINF:-1 group-title="{v["group"]}",{display_name}')
                lines.append(v["url"])
                total += 1
        lines.append("")

    # --------------------------------------------------------------------------
    # Блок 2: Остальные каналы по тематике
    # --------------------------------------------------------------------------
    if valid_other:
        by_category = defaultdict(list)
        for e in valid_other:
            cat = detect_category(e.get("name", ""))
            by_category[cat].append(e)

        category_order = [
            "Новости",
            "Музыка",
            "Детские",
            "Кино",
            "Познавательные",
            "Развлекательные",
            "Научные",
            "Прочее"
        ]

        lines.append("# ===== Тематические группы =====")
        for cat in category_order:
            if cat not in by_category:
                continue
            lines.append(f"# --- {cat} ---")
            for e in by_category[cat]:
                name = e.get("name", "Unknown")
                extinf = e.get("extinf", f"#EXTINF:-1,{name}")
                if "group-title=" in extinf:
                    new_extinf = re.sub(r'group-title="[^"]*"', f'group-title="{cat}"', extinf)
                else:
                    meta = extinf.split(",")[0]
                    new_extinf = f'{meta} group-title="{cat}",{name}'
                lines.append(new_extinf)
                lines.append(e["url"])
                total += 1
            lines.append("")

    # Запись файла целиком (чистая структура)
    if os.path.exists(target_path):
        backup_file(target_path)

    with open(target_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"В {target_path} записано {total} потоков.")
    return total

# ==============================================================================
# ЗАГРУЗКА ИСТОЧНИКОВ
# ==============================================================================

def fetch_source_worker(source_tuple):
    """
    Воркер для параллельной загрузки и первичного парсинга источников.
    apply_filter=True → применяется blacklist 18+ и фильтр digit-index.
    """
    name, url, apply_filter = source_tuple
    if not url:
        return name, []
    print(f"Параллельная загрузка {name}: {url}")
    raw_data = fetch_url(url)
    if not raw_data:
        print(f"Источник {name} недоступен или пуст.")
        return name, []

    entries = parse_m3u_content(raw_data)
    filtered_entries = []
    for e in entries:
        ch_name = e.get("name", "")
        ch_url = e.get("url", "")

        if apply_filter:
            if is_blacklisted(ch_name) or has_digit_index_in_path(ch_url):
                continue

        e["source"] = name
        filtered_entries.append(e)

    print(f"Из {name} успешно распарсено в пул: {len(filtered_entries)} записей")
    return name, filtered_entries

# ==============================================================================
# ОСНОВНОЙ ПОТОК
# ==============================================================================

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

    sources = [
        ("SOURCE_URL", SOURCE_URL, False),
        ("SECOND_SOURCE_URL", SECOND_SOURCE_URL, True),
        ("THIRD_SOURCE_URL", THIRD_SOURCE_URL, True),
        ("TEST_CHANNELS_URL", TEST_CHANNELS_URL, False),
    ]

    donor_entries = []

    print("Запуск многопоточного скачивания всех донорских источников...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_source = {executor.submit(fetch_source_worker, src): src[0] for src in sources}
        for future in concurrent.futures.as_completed(future_to_source):
            src_name = future_to_source[future]
            try:
                name, entries = future.result()
                if entries:
                    donor_entries.extend(entries)
            except Exception as exc:
                print(f"Источник {src_name} вызвал исключение при загрузке: {exc}")

    if not donor_entries:
        print("Нет донорских записей для обработки.")
        sys.exit(0)

    print(f"Всего кандидатов после первичной фильтрации: {len(donor_entries)}")

    modified = []
    added_total = 0

    # --------------------------------------------------------------------------
    # 1. Обычные плейлисты (полная логика без дедупликации)
    # --------------------------------------------------------------------------
    for target in (TARGET_MIR, TARGET_STABLE_LEGACY, TARGET_STABLE):
        added = append_all_valid(target, donor_entries)
        if added:
            print(f"В {target} добавлено рабочих потоков: {added}")
            modified.append(target)
            added_total += added
        else:
            print(f"В {target} новых рабочих потоков не обнаружено.")

    # --------------------------------------------------------------------------
    # 2. Специальный RTRS_stable.m3u8
    # --------------------------------------------------------------------------
    rtrs_added = build_rtrs_stable(donor_entries, TARGET_RTRS_STABLE)
    if rtrs_added:
        modified.append(TARGET_RTRS_STABLE)
        added_total += rtrs_added

    if not modified:
        print("Изменений нет. Новые рабочие потоки не прошли проверку.")
        if not SKIP_PUSH:
            try:
                run(["git", "checkout", "gh-pages"], check=True)
                run(["git", "branch", "-D", branch_name], check=True)
            except Exception:
                pass
        sys.exit(0)

    if SKIP_PUSH:
        print(f"Готово! Автоматически добавлено/обновлено рабочих потоков: {added_total}.")
        print("Modified files:", modified)
        sys.exit(0)

    # --------------------------------------------------------------------------
    # Коммит + пуш
    # --------------------------------------------------------------------------
    try:
        run(["git", "add"] + modified, check=True)
        commit_msg = (
            f"chore: multi-threaded auto-update IPTV streams "
            f"& RTRS grid + themes ({time.strftime('%Y-%m-%d %H:%M:%S')})"
        )
        run(["git", "commit", "-m", commit_msg], check=True)
        run(["git", "push", "-u", "origin", branch_name], check=True)
        print(f"Успешно запушено в ветку: {branch_name}")
    except Exception as e:
        print(f"Ошибка при коммите/пуше: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()