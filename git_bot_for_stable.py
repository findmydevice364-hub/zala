#!/usr/bin/env python3
# coding: utf-8
"""
# ==============================================================================
# Project:      findmydevice364-hub_lab
# Owner:        #Phoenix89S_Lab©
# Public Name:  Phoenix Hub "Baryshnikov"
# Version:      3.0.0-beta.2.1
# versionCode:  30.2.0.1
# Build ID:     P3021-B1-20261105
# Target State: Beta 2.1 (Auto-bootstrap ffprobe + Full Playlist Validation)
# ==============================================================================

git_bot_for_stable.py
Universal Multi-threaded Full Logic Version
===========================================

Полная логика исходной версии + дополнительные исправления:

- Многопоточная загрузка всех донорских источников (ThreadPoolExecutor)
- Полный сбор кандидатов: без фильтрации по индексам URL, группам или числу записей
- Фильтр только 18+ для всех источников
- Многопоточная проверка потоков HEAD -> GET fallback -> ffprobe для всех целей
- Автоматическая загрузка ffprobe для Linux x86_64, если он не найден
- Кэш результата проверки URL на один запуск (не удаляет повторные записи)
- Без дедупликации — добавляем все прошедшие проверку записи
- РТРС SD / РТРС HD (1–20) с индексацией повторных вариантов
- Строгая нормализация названий только в RTRS_stable.m3u8
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
- Бэкапы вне рабочего дерева Git, чтобы они не блокировали следующий запуск
- Полная git-логика с использованием TARGET_PUSH_BRANCH

Важно: проверка доступности и ffprobe подтверждают состояние потока на момент проверки,
но не гарантируют его постоянную работоспособность.
"""

import os
import re
import subprocess
import urllib.request
import urllib.error
import time
import shutil
import sys
import tempfile
import threading
import concurrent.futures
from collections import defaultdict
import json
import tarfile
import platform

# ==============================================================================
# КОНФИГУРАЦИЯ ИСТОЧНИКОВ
# ==============================================================================

SOURCE_URL = "https://raw.githubusercontent.com/Phoenix89S/IpTV_playlist_2026Ru/main/ngenix_found_1.m3u"
SECOND_SOURCE_URL = "https://raw.githubusercontent.com/Free-TV/IPTV/refs/heads/master/playlists/playlist_russia.m3u8"
THIRD_SOURCE_URL = "https://raw.githubusercontent.com/Phoenix89S/Mir-IpTV-Ru-/main/Super_RTRS_2026.m3u"
TEST_CHANNELS_URL = "https://raw.githubusercontent.com/Phoenix89S/IpTV_playlist_2026Ru/gh-pages/test_channels.m3u"

# Переопределение через переменные окружения (для GitHub Actions)
SECOND_SOURCE_URL = os.environ.get("SECOND_SOURCE_URL", SECOND_SOURCE_URL)
SOURCE_URL = os.environ.get("SOURCE_URL", SOURCE_URL)
THIRD_SOURCE_URL = os.environ.get("THIRD_SOURCE_URL", THIRD_SOURCE_URL)
TEST_CHANNELS_URL = os.environ.get("TEST_CHANNELS_URL", TEST_CHANNELS_URL)
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
# ГИБКАЯ СЕТКА РТРС
# ==============================================================================

# Значение — (группа, нормализованное имя, порядок сортировки).
# SD и HD имеют отдельные записи; повторные кандидаты не удаляются.
RTRS_CHANNELS_MAP = {
    # ----- РТРС SD -----
    "первый канал":          ("РТРС SD", "1.SD Первый канал", 1),
    "россия 1":              ("РТРС SD", "2.SD Россия 1", 2),
    "матч тв":               ("РТРС SD", "3.SD Матч ТВ", 3),
    "нтв":                   ("РТРС SD", "4.SD НТВ", 4),
    "пятый канал":           ("РТРС SD", "5.SD Пятый канал", 5),
    "россия к":              ("РТРС SD", "6.SD Россия К", 6),
    "россия 24":             ("РТРС SD", "7.SD Россия 24", 7),
    "карусель":              ("РТРС SD", "8.SD Карусель", 8),
    "отр":                   ("РТРС SD", "9.SD ОТР", 9),
    "тв центр":              ("РТРС SD", "10.SD ТВ Центр", 10),
    "твц":                   ("РТРС SD", "10.SD ТВ Центр", 10),
    "рен тв":                ("РТРС SD", "11.SD РЕН ТВ", 11),
    "спас":                  ("РТРС SD", "12.SD Спас", 12),
    "стс":                   ("РТРС SD", "13.SD СТС", 13),
    "домашний":              ("РТРС SD", "14.SD Домашний", 14),
    "тв-3":                  ("РТРС SD", "15.SD ТВ-3", 15),
    "тв3":                   ("РТРС SD", "15.SD ТВ-3", 15),
    "тв 3":                  ("РТРС SD", "15.SD ТВ-3", 15),
    "пятница":               ("РТРС SD", "16.SD Пятница!", 16),
    "звезда":                ("РТРС SD", "17.SD Звезда", 17),
    "мир":                   ("РТРС SD", "18.SD Мир", 18),
    "тнт":                   ("РТРС SD", "19.SD ТНТ", 19),
    "муз тв":                ("РТРС SD", "20.SD Муз-ТВ", 20),

    # ----- РТРС HD -----
    "первый канал hd":       ("РТРС HD", "1.HD Первый канал HD", 101),
    "россия 1 hd":           ("РТРС HD", "2.HD Россия 1 HD", 102),
    "матч тв hd":            ("РТРС HD", "3.HD Матч ТВ HD", 103),
    "нтв hd":                ("РТРС HD", "4.HD НТВ HD", 104),
    "пятый канал hd":        ("РТРС HD", "5.HD Пятый канал HD", 105),
    "россия к hd":           ("РТРС HD", "6.HD Россия К HD", 106),
    "россия 24 hd":          ("РТРС HD", "7.HD Россия 24 HD", 107),
    "карусель hd":           ("РТРС HD", "8.HD Карусель HD", 108),
    "отр hd":                ("РТРС HD", "9.HD ОТР HD", 109),
    "тв центр hd":           ("РТРС HD", "10.HD ТВ Центр HD", 110),
    "твц hd":                ("РТРС HD", "10.HD ТВ Центр HD", 110),
    "рен тв hd":             ("РТРС HD", "11.HD РЕН ТВ HD", 111),
    "спас hd":               ("РТРС HD", "12.HD Спас HD", 112),
    "стс hd":                ("РТРС HD", "13.HD СТС HD", 113),
    "домашний hd":           ("РТРС HD", "14.HD Домашний HD", 114),
    "тв-3 hd":               ("РТРС HD", "15.HD ТВ-3 HD", 115),
    "тв3 hd":                ("РТРС HD", "15.HD ТВ-3 HD", 115),
    "тв 3 hd":               ("РТРС HD", "15.HD ТВ-3 HD", 115),
    "пятница hd":            ("РТРС HD", "16.HD Пятница! HD", 116),
    "звезда hd":             ("РТРС HD", "17.HD Звезда HD", 117),
    "мир hd":                ("РТРС HD", "18.HD Мир HD", 118),
    "тнт hd":                ("РТРС HD", "19.HD ТНТ HD", 119),
    "муз тв hd":             ("РТРС HD", "20.HD Муз-ТВ HD", 120),
}

# ==============================================================================
# ТЕМАТИЧЕСКИЕ ГРУППЫ
# ==============================================================================

CATEGORY_RULES = [
    ("Новости", [
        "новост", "news", "россия 24", "рбк", "rbc", "известия", "жизнь",
        "москва 24", "миротворец", "rt", "russia today", "bbc", "cnn",
        "euronews", "tv rain", "дождь", "current time", "наст. время",
        "360", "life", "life news"
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

CATEGORY_ORDER = [
    "Новости", "Музыка", "Детские", "Кино",
    "Познавательные", "Развлекательные", "Научные", "Прочее"
]

# ==============================================================================
# КЭШИ И СИНХРОНИЗАЦИЯ
# ==============================================================================

# Кэш используется только для повторной проверки одинакового URL в рамках одного запуска.
# Записи плейлиста при этом не удаляются и не объединяются.
_STREAM_CHECK_CACHE = {}
_STREAM_CHECK_IN_PROGRESS = {}
_STREAM_CHECK_LOCK = threading.Lock()

# ==============================================================================
# УТИЛИТЫ GIT И СЕТИ
# ==============================================================================

def run(cmd, cwd=None, check=True):
    try:
        res = subprocess.run(
            cmd, cwd=cwd, check=check, capture_output=True, text=True
        )
        return res
    except subprocess.CalledProcessError as e:
        print(f"Команда {' '.join(cmd)} завершилась с ошибкой: {e}")
        print(f"stderr:\n{e.stderr}")
        raise


def git_is_clean():
    res = run(["git", "status", "--porcelain"], check=True)
    return res.stdout.strip() == ""


def ensure_on_branch(branch=TARGET_PUSH_BRANCH):
    """Переключается на целевую ветку и синхронизирует её с origin."""
    if not branch or branch.startswith("-"):
        raise ValueError(f"Некорректное имя целевой ветки: {branch!r}")

    run(["git", "fetch", "origin", branch], check=True)

    local_exists = subprocess.run(
        ["git", "show-ref", "--verify", f"refs/heads/{branch}"],
        capture_output=True,
        text=True
    ).returncode == 0

    if local_exists:
        run(["git", "checkout", branch], check=True)
    else:
        run(["git", "checkout", "-b", branch, f"origin/{branch}"], check=True)

    run(["git", "pull", "--ff-only", "origin", branch], check=True)


def fetch_url(url, timeout=12):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; IPTVBot/4.0)"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw_data = response.read()
            for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
                try:
                    return raw_data.decode(encoding)
                except UnicodeDecodeError:
                    continue
            return raw_data.decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"Ошибка загрузки {url}: {exc}")
        return ""


def check_stream_alive_simple(url, timeout=5):
    """Проверяет URL через HEAD, а при неудаче — GET с Range."""
    if not url or not url.lower().startswith(("http://", "https://")):
        return False

    headers = {"User-Agent": "VLC/3.0.16 LibVLC/3.0.16"}

    try:
        request = urllib.request.Request(
            url, headers=headers, method="HEAD"
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if 200 <= response.status < 400:
                return True
    except Exception:
        pass

    try:
        get_headers = dict(headers)
        get_headers["Range"] = "bytes=0-512"
        request = urllib.request.Request(
            url, headers=get_headers, method="GET"
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read(513)
            return 200 <= response.status < 400
    except Exception:
        return False


def ensure_ffprobe():
    """Находит ffprobe или скачивает Linux x86_64 static build в локальный кэш.

    Архив John Van Sickle предназначен для Linux amd64. На других ОС/архитектурах
    функция не пытается установить неподходящий бинарник и возвращает False.
    Бинарник извлекается напрямую в файл, без распаковки остальных членов архива.
    """
    existing = shutil.which("ffprobe")
    if existing:
        print(f"Найден системный ffprobe: {existing}")
        return True

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system != "linux" or machine not in ("x86_64", "amd64"):
        print(
            "Автозагрузка ffprobe поддерживается этим скриптом только на Linux x86_64. "
            f"Текущая платформа: {system}/{machine}. Установите ffmpeg вручную."
        )
        return False

    cache_dir = os.path.join(
        os.path.expanduser("~"), ".cache", "iptv_bot_bin"
    )
    os.makedirs(cache_dir, exist_ok=True)
    target_ffprobe = os.path.join(cache_dir, "ffprobe")

    def activate_binary(path):
        try:
            os.chmod(path, 0o755)
            result = subprocess.run(
                [path, "-version"],
                capture_output=True,
                text=True,
                timeout=8
            )
            if result.returncode != 0:
                return False

            os.environ["PATH"] = (
                cache_dir + os.pathsep + os.environ.get("PATH", "")
            )
            print(f"ffprobe готов: {path}")
            return True
        except (OSError, subprocess.SubprocessError) as exc:
            print(f"Кэшированный ffprobe не удалось запустить: {exc}")
            return False

    if os.path.isfile(target_ffprobe) and activate_binary(target_ffprobe):
        return True

    if os.path.exists(target_ffprobe):
        try:
            os.remove(target_ffprobe)
        except OSError:
            pass

    download_url = (
        "https://johnvansickle.com/ffmpeg/releases/"
        "ffmpeg-release-amd64-static.tar.xz"
    )

    print("ffprobe не найден. Скачиваю статическую сборку для Linux x86_64...")

    temp_archive = None
    temp_binary = None

    try:
        fd, temp_archive = tempfile.mkstemp(
            prefix="iptv-ffmpeg-", suffix=".tar.xz"
        )
        os.close(fd)

        request = urllib.request.Request(
            download_url,
            headers={
                "User-Agent": "Mozilla/5.0 "
                              "(compatible; IPTVBotBootstrapper/1.0)"
            }
        )

        with urllib.request.urlopen(request, timeout=45) as response:
            with open(temp_archive, "wb") as out_file:
                shutil.copyfileobj(response, out_file)

        with tarfile.open(temp_archive, mode="r:xz") as archive:
            member = next(
                (
                    item for item in archive.getmembers()
                    if item.isfile()
                    and item.name.rstrip("/").endswith("/ffprobe")
                ),
                None
            )

            if member is None:
                raise RuntimeError(
                    "В загруженном архиве не найден бинарник ffprobe"
                )

            source = archive.extractfile(member)
            if source is None:
                raise RuntimeError(
                    "Не удалось прочитать ffprobe из архива"
                )

            fd, temp_binary = tempfile.mkstemp(
                prefix="ffprobe-", dir=cache_dir
            )

            with os.fdopen(fd, "wb") as out_file, source:
                shutil.copyfileobj(source, out_file)

        os.chmod(temp_binary, 0o755)

        # Проверяем бинарник до замены кэшированного файла.
        check = subprocess.run(
            [temp_binary, "-version"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if check.returncode != 0:
            raise RuntimeError(
                "Скачанный ffprobe не запускается на этой системе"
            )

        os.replace(temp_binary, target_ffprobe)
        temp_binary = None

        os.environ["PATH"] = (
            cache_dir + os.pathsep + os.environ.get("PATH", "")
        )

        print(f"Автоматически загружен ffprobe: {target_ffprobe}")
        return True

    except Exception as exc:
        print(f"Не удалось автоматически скачать/подготовить ffprobe: {exc}")
        return False

    finally:
        for temporary_path in (temp_archive, temp_binary):
            if temporary_path:
                try:
                    os.remove(temporary_path)
                except OSError:
                    pass


def check_stream_quality_ffprobe(url, timeout=8):
    """Возвращает True, если ffprobe обнаружил аудио- или видеопоток."""
    if not url or not url.lower().startswith(("http://", "https://")):
        return False

    if not shutil.which("ffprobe"):
        print(
            "ОШИБКА: ffprobe недоступен. "
            "Автозагрузка должна быть выполнена в main()."
        )
        return False

    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-print_format", "json",
            "-show_streams", "-show_format",
            "-rw_timeout", str(int(timeout * 1000000)),
            url
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 2
        )

        if result.returncode != 0:
            return False

        data = json.loads(result.stdout or "{}")
        streams = data.get("streams", [])

        has_video = any(
            stream.get("codec_type") == "video" for stream in streams
        )
        has_audio = any(
            stream.get("codec_type") == "audio" for stream in streams
        )

        return has_video or has_audio

    except (
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
        OSError,
        ValueError
    ):
        return False
    except Exception:
        return False


def check_stream_all(url):
    """Общий конвейер HEAD -> GET fallback -> ffprobe для всех целей.

    Для каждого URL выполняется один конвейер за запуск. Остальные потоки,
    встретившие тот же URL параллельно, ожидают результат, но их записи сохраняются.
    """
    if not url:
        return False

    with _STREAM_CHECK_LOCK:
        if url in _STREAM_CHECK_CACHE:
            return _STREAM_CHECK_CACHE[url]

        event = _STREAM_CHECK_IN_PROGRESS.get(url)

        if event is None:
            event = threading.Event()
            _STREAM_CHECK_IN_PROGRESS[url] = event
            is_owner = True
        else:
            is_owner = False

    if not is_owner:
        event.wait()
        with _STREAM_CHECK_LOCK:
            return _STREAM_CHECK_CACHE.get(url, False)

    try:
        # HEAD/GET — предварительная проверка доступности. ffprobe запускается и после
        # неудачи HTTP-проверки: некоторые IPTV-серверы ограничивают HEAD/Range,
        # но могут отдавать поток протоколу ffmpeg.
        http_alive = check_stream_alive_simple(url)
        ffprobe_alive = check_stream_quality_ffprobe(url)

        valid = bool(ffprobe_alive and (http_alive or ffprobe_alive))

        with _STREAM_CHECK_LOCK:
            _STREAM_CHECK_CACHE[url] = valid

        return valid

    finally:
        with _STREAM_CHECK_LOCK:
            completed_event = _STREAM_CHECK_IN_PROGRESS.pop(url, None)
            if completed_event is not None:
                completed_event.set()


# ==============================================================================
# ПАРСИНГ M3U
# ==============================================================================

def parse_m3u_content(content):
    lines = content.splitlines()
    entries = []
    current_meta = None
    tvg_re = re.compile(r'tvg-id="([^"]+)"', re.IGNORECASE)

    for line in lines:
        stripped = line.strip()

        if not stripped:
            continue

        if stripped.startswith("#EXTINF:"):
            current_meta = stripped

        elif not stripped.startswith("#"):
            url = stripped

            if current_meta:
                parts = current_meta.split(",", 1)
                name = parts[1].strip() if len(parts) > 1 else ""

                match = tvg_re.search(current_meta)
                key = (
                    match.group(1).strip().lower()
                    if match
                    else url.strip().lower()
                )

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
    title_lower = (title or "").strip().lower()
    return any(bad in title_lower for bad in BLACKLIST)


def has_digit_index_in_path(url):
    """Оставлена для совместимости; намеренно не используется как фильтр."""
    return bool(re.search(r"/\d+/", url or ""))


def normalize_channel_text(value):
    """Нормализует пунктуацию/пробелы, сохраняя границы отдельных слов."""
    clean = (value or "").casefold().replace("ё", "е")

    # Разные написания дефиса, подчёркивания и точки приводим к разделителям.
    clean = re.sub(r"[‐‑‒–—−_./\\:]+", " ", clean)
    clean = re.sub(r"[^\w\s]", " ", clean, flags=re.UNICODE)
    clean = re.sub(r"\s+", " ", clean).strip()

    return clean


def _contains_phrase(text, phrase):
    """Ищет фразу по границам слов, а не произвольной подстрокой."""
    if not text or not phrase:
        return False

    return (
        re.search(
            r"(?<!\w)" + re.escape(phrase) + r"(?!\w)",
            text,
            flags=re.UNICODE
        )
        is not None
    )


# Предварительно нормализованные ключи сохраняют исходный словарь и порядок его объявления.
_NORMALIZED_RTRS_KEYS = []

for _key, _value in RTRS_CHANNELS_MAP.items():
    _normalized_key = normalize_channel_text(_key)

    if _normalized_key:
        _NORMALIZED_RTRS_KEYS.append(
            (_normalized_key, _value, _key)
        )


def match_rtrs(name):
    """Строго сопоставляет название с сеткой РТРС; HD проверяется первым."""
    clean = normalize_channel_text(name)

    if not clean:
        return None

    # Сначала наиболее длинные HD-ключи, затем наиболее длинные SD-ключи.
    hd_keys = [
        item for item in _NORMALIZED_RTRS_KEYS
        if "hd" in item[0].split()
    ]

    sd_keys = [
        item for item in _NORMALIZED_RTRS_KEYS
        if "hd" not in item[0].split()
    ]

    for key, value, original_key in sorted(
        hd_keys, key=lambda item: len(item[0]), reverse=True
    ):
        if _contains_phrase(clean, key):
            return value

    for key, value, original_key in sorted(
        sd_keys, key=lambda item: len(item[0]), reverse=True
    ):
        if _contains_phrase(clean, key):
            return value

    return None


def _keyword_match(clean, keyword):
    """Сопоставляет тематические ключи; короткие латинские аббревиатуры — как токены."""
    keyword_normalized = normalize_channel_text(keyword)

    if not keyword_normalized:
        return False

    # Эти короткие обозначения слишком рискованны для поиска внутри слов.
    if keyword_normalized in {
        "rt", "rbc", "bbc", "cnn", "mtv", "vh1",
        "fox", "hbo", "amc", "ctc"
    }:
        return _contains_phrase(clean, keyword_normalized)

    # Для остальных фраз сохраняем ожидаемый поиск по фрагментам названия.
    return keyword_normalized in clean


def detect_category(name):
    """Определяет тематическую группу по названию канала."""
    clean = normalize_channel_text(name)

    for category, keywords in CATEGORY_RULES:
        for keyword in keywords:
            if _keyword_match(clean, keyword):
                return category

    return "Прочее"


def smart_format_entry(name, extinf, default_group="Стабильные ТВ"):
    """Применяет РТРС-сетку если канал совпал, иначе сохраняет/назначает группу."""
    matched = match_rtrs(name)

    if matched:
        group_title, formatted_name, _ = matched
        source_meta = (extinf or "#EXTINF:-1").split(",", 1)[0]

        if re.search(
            r'\bgroup-title="[^"]*"',
            source_meta,
            flags=re.IGNORECASE
        ):
            source_meta = re.sub(
                r'\bgroup-title="[^"]*"',
                f'group-title="{group_title}"',
                source_meta,
                flags=re.IGNORECASE
            )
        else:
            source_meta = f'{source_meta} group-title="{group_title}"'

        return f"{source_meta},{formatted_name}", group_title

    if not extinf:
        return (
            f'#EXTINF:-1 group-title="{default_group}",{name}',
            default_group
        )

    if re.search(
        r'\bgroup-title="[^"]*"',
        extinf,
        flags=re.IGNORECASE
    ):
        return extinf, default_group

    if "," in extinf:
        meta, channel_name = extinf.split(",", 1)
    else:
        meta, channel_name = extinf, name

    return f'{meta} group-title="{default_group}",{channel_name}', default_group


def backup_file(path):
    """Создаёт бэкап во временном каталоге, вне рабочего дерева Git."""
    if not os.path.exists(path):
        return None

    backup_root = os.path.join(
        tempfile.gettempdir(), "iptv_bot_backups"
    )
    os.makedirs(backup_root, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    safe_name = (
        os.path.abspath(path)
        .replace(":", "")
        .replace(os.sep, "_")
    )

    backup_path = os.path.join(
        backup_root, f"{safe_name}.bak_{timestamp}"
    )

    shutil.copy2(path, backup_path)
    print(f"Создан backup вне рабочего дерева Git: {backup_path}")

    return backup_path


def process_single_stream_check_simple(entry):
    """Воркер общей проверки HEAD -> GET fallback -> ffprobe."""
    if check_stream_all(entry.get("url", "")):
        return entry

    return None


def process_single_stream_check_ffprobe(entry):
    """Совместимое имя воркера; использует тот же общий конвейер для всех целей."""
    if check_stream_all(entry.get("url", "")):
        return entry

    return None


# ==============================================================================
# ОБЫЧНОЕ ДОБАВЛЕНИЕ (без дедупликации)
# ==============================================================================

def append_all_valid(target_path, donor_entries):
    """Добавляет все прошедшие проверку записи без дедупликации."""
    if not donor_entries:
        return 0

    print(
        f"Многопоточная проверка HEAD -> GET -> ffprobe: "
        f"{len(donor_entries)} кандидатов → {target_path}"
    )

    valid_candidates = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
        results = executor.map(
            process_single_stream_check_simple,
            donor_entries
        )

        for result in results:
            if result:
                valid_candidates.append(result)

    if not valid_candidates:
        return 0

    to_append = []

    for entry in valid_candidates:
        name = entry.get("name", "")
        formatted_extinf, _ = smart_format_entry(
            name, entry.get("extinf", "")
        )
        to_append.append((formatted_extinf, entry.get("url", "")))

    file_exists = os.path.exists(target_path)

    if file_exists:
        backup_file(target_path)

    os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)

    with open(target_path, "a", encoding="utf-8") as output_file:
        if not file_exists:
            output_file.write("#EXTM3U\n")

        output_file.write("\n")
        output_file.write(
            f"# ===== AUTO-UPDATED STABLE STREAMS "
            f"({time.strftime('%Y-%m-%d %H:%M:%S')}) =====\n"
        )

        for extinf, url in to_append:
            output_file.write(extinf.rstrip() + "\n")
            output_file.write(url.rstrip() + "\n")

    return len(to_append)


# ==============================================================================
# СПЕЦИАЛЬНАЯ ЛОГИКА RTRS_stable.m3u8
# ==============================================================================

def build_rtrs_stable(donor_entries, target_path=TARGET_RTRS_STABLE):
    """Формирует стабильный РТРС-блок и тематические группы остальных каналов."""
    print(
        f"Формирование {target_path} "
        "(РТРС + тематика + HEAD/GET/ffprobe)..."
    )

    rtrs_candidates = []
    other_candidates = []

    for entry in donor_entries:
        matched = match_rtrs(entry.get("name", ""))

        if matched:
            group, base_name, order = matched

            # Не изменяем исходное имя записи — метаданные РТРС храним отдельно.
            candidate = dict(entry)
            candidate["_rtrs"] = (group, base_name, order)
            rtrs_candidates.append(candidate)
        else:
            other_candidates.append(entry)

    print(f"РТРС-кандидатов: {len(rtrs_candidates)}")
    print(f"Остальных кандидатов: {len(other_candidates)}")
    print(
        "Многопоточная проверка всех кандидатов "
        "через HEAD -> GET -> ffprobe..."
    )

    valid_rtrs = []
    valid_other = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        for result in executor.map(
            process_single_stream_check_ffprobe,
            rtrs_candidates
        ):
            if result:
                valid_rtrs.append(result)

        for result in executor.map(
            process_single_stream_check_ffprobe,
            other_candidates
        ):
            if result:
                valid_other.append(result)

    print(f"Рабочих РТРС (полная проверка): {len(valid_rtrs)}")
    print(f"Рабочих остальных (полная проверка): {len(valid_other)}")

    lines = ["#EXTM3U"]
    lines.append(
        f"# Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    lines.append(
        "# Project: findmydevice364-hub_lab | Phoenix Hub Baryshnikov"
    )
    lines.append(
        "# Version: 3.0.0-beta.2.1 | versionCode: 30.2.0.1 "
        "| Build ID: P3021-B1-20261105"
    )
    lines.append(
        "# Validation: HEAD -> GET fallback -> ffprobe "
        "| Deduplication: disabled"
    )
    lines.append("")

    total = 0

    # --------------------------------------------------------------------------
    # Блок 1: РТРС SD + HD (нормализованные названия + все варианты)
    # --------------------------------------------------------------------------
    if valid_rtrs:
        slots = defaultdict(list)

        for entry in valid_rtrs:
            group, base_name, order = entry["_rtrs"]

            slots[(order, base_name, group)].append({
                "url": entry["url"],
                "order": order,
                "group": group
            })

        sorted_slots = sorted(
            slots.items(),
            key=lambda item: (item[0][0], item[0][1])
        )

        lines.append(
            "# ===== РТРС SD / РТРС HD (первые два мультиплекса) ====="
        )

        for (order, base_name, group), variants in sorted_slots:
            prefix, _, rest = base_name.partition(" ")

            for index, variant in enumerate(variants):
                if index == 0:
                    display_name = base_name
                else:
                    display_name = f"{prefix}.{index} {rest}"

                lines.append(
                    f'#EXTINF:-1 group-title="{group}",{display_name}'
                )
                lines.append(variant["url"])
                total += 1

        lines.append("")

    # --------------------------------------------------------------------------
    # Блок 2: Остальные каналы по тематике
    # --------------------------------------------------------------------------
    if valid_other:
        by_category = defaultdict(list)

        for entry in valid_other:
            category = detect_category(entry.get("name", ""))
            by_category[category].append(entry)

        lines.append("# ===== Тематические группы =====")

        for category in CATEGORY_ORDER:
            if category not in by_category:
                continue

            lines.append(f"# --- {category} ---")

            for entry in by_category[category]:
                name = entry.get("name", "Unknown")
                extinf = entry.get(
                    "extinf", f"#EXTINF:-1,{name}"
                )

                if re.search(
                    r'\bgroup-title="[^"]*"',
                    extinf,
                    flags=re.IGNORECASE
                ):
                    new_extinf = re.sub(
                        r'\bgroup-title="[^"]*"',
                        f'group-title="{category}"',
                        extinf,
                        flags=re.IGNORECASE
                    )
                else:
                    meta = extinf.split(",", 1)[0]
                    new_extinf = (
                        f'{meta} group-title="{category}",{name}'
                    )

                lines.append(new_extinf)
                lines.append(entry["url"])
                total += 1

            lines.append("")

    # Запись файла целиком: резервная копия не загрязняет рабочее дерево Git.
    if os.path.exists(target_path):
        backup_file(target_path)

    os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)

    with open(target_path, "w", encoding="utf-8") as output_file:
        output_file.write("\n".join(lines) + "\n")

    print(f"В {target_path} записано {total} потоков.")
    return total


# ==============================================================================
# ЗАГРУЗКА ИСТОЧНИКОВ
# ==============================================================================

def fetch_source_worker(source_tuple):
    """Параллельно загружает и парсит источник; сохраняет все кандидаты, кроме 18+."""
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

    for entry in entries:
        channel_name = entry.get("name", "")

        # Никакой фильтрации по индексу URL, номеру канала или группе.
        # Параметр apply_filter сохранён для совместимости с исходной сигнатурой.
        if is_blacklisted(channel_name):
            continue

        entry["source"] = name
        filtered_entries.append(entry)

    print(
        f"Из {name} добавлено в полный пул кандидатов: "
        f"{len(filtered_entries)} записей"
    )

    return name, filtered_entries


# ==============================================================================
# ОСНОВНОЙ ПОТОК
# ==============================================================================

def main():
    if not shutil.which("git"):
        print("Git не найден в PATH. Установите git и повторите.")
        sys.exit(1)

    # Автоматически находим или скачиваем ffprobe.
    if not ensure_ffprobe():
        print(
            "ОШИБКА: не удалось найти или автоматически загрузить ffprobe. "
            "Проверка потоков невозможна."
        )
        sys.exit(1)

    if not git_is_clean():
        print(
            "Рабочее дерево git не чистое. "
            "Очистите или закоммитьте изменения перед запуском."
        )
        sys.exit(1)

    branch_name = TARGET_PUSH_BRANCH

    if not SKIP_PUSH:
        print(f"Целевая ветка публикации: {branch_name}")

        try:
            ensure_on_branch(branch_name)
        except Exception as exc:
            print(
                f"Не удалось перейти на целевую ветку "
                f"{branch_name}: {exc}"
            )
            sys.exit(1)
    else:
        print(
            "SKIP_PUSH=true — файлы будут созданы в текущем "
            "рабочем дереве без коммита/пуша."
        )

    sources = [
        ("SOURCE_URL", SOURCE_URL, False),
        ("SECOND_SOURCE_URL", SECOND_SOURCE_URL, True),
        ("THIRD_SOURCE_URL", THIRD_SOURCE_URL, True),
        ("TEST_CHANNELS_URL", TEST_CHANNELS_URL, False),
    ]

    donor_entries = []

    print("Запуск многопоточного скачивания всех донорских источников...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_source = {
            executor.submit(fetch_source_worker, source): source[0]
            for source in sources
        }

        for future in concurrent.futures.as_completed(future_to_source):
            source_name = future_to_source[future]

            try:
                name, entries = future.result()

                if entries:
                    donor_entries.extend(entries)

            except Exception as exc:
                print(
                    f"Источник {source_name} вызвал исключение "
                    f"при загрузке: {exc}"
                )

    if not donor_entries:
        print("Нет донорских записей для обработки.")
        sys.exit(0)

    print(
        "Всего кандидатов после исключения только blacklist 18+: "
        f"{len(donor_entries)}"
    )
    print(
        "Дедупликация отключена: все повторные записи "
        "и альтернативные URL сохраняются."
    )

    modified = []
    added_total = 0

    # --------------------------------------------------------------------------
    # 1. Обычные плейлисты — HEAD -> GET -> ffprobe, без дедупликации
    # --------------------------------------------------------------------------
    for target in (
        TARGET_MIR,
        TARGET_STABLE_LEGACY,
        TARGET_STABLE
    ):
        added = append_all_valid(target, donor_entries)

        if added:
            print(
                f"В {target} добавлено рабочих записей: {added}"
            )
            modified.append(target)
            added_total += added
        else:
            print(
                f"В {target} новых прошедших проверку записей "
                "не обнаружено."
            )

    # --------------------------------------------------------------------------
    # 2. Специальный RTRS_stable.m3u8
    # --------------------------------------------------------------------------
    rtrs_added = build_rtrs_stable(
        donor_entries, TARGET_RTRS_STABLE
    )

    # build_rtrs_stable перезаписывает файл даже при нуле прошедших каналов.
    # Поэтому файл включается в git add независимо от количества потоков.
    if TARGET_RTRS_STABLE not in modified:
        modified.append(TARGET_RTRS_STABLE)

    added_total += rtrs_added

    if not modified:
        print(
            "Изменений нет. Новые рабочие потоки не прошли проверку."
        )
        sys.exit(0)

    if SKIP_PUSH:
        print(
            f"Готово! Обработано рабочих записей: {added_total}."
        )
        print("Modified files:", modified)
        print("SKIP_PUSH=true — коммит и пуш не выполнялись.")
        sys.exit(0)

    # --------------------------------------------------------------------------
    # Коммит + пуш в TARGET_PUSH_BRANCH
    # --------------------------------------------------------------------------
    try:
        run(["git", "add", "--"] + modified, check=True)

        staged = run(
            ["git", "diff", "--cached", "--quiet"],
            check=False
        )

        if staged.returncode == 0:
            print("Нет staged-изменений; коммит не создаётся.")
            sys.exit(0)

        commit_msg = (
            "chore: full IPTV validation + RTRS normalization "
            f"(v30.2.0.1 / 3.0.0-beta.2.1) "
            f"({time.strftime('%Y-%m-%d %H:%M:%S')})"
        )

        run(["git", "commit", "-m", commit_msg], check=True)
        run(
            ["git", "push", "origin", TARGET_PUSH_BRANCH],
            check=True
        )

        print(
            f"Успешно опубликовано в ветку: {TARGET_PUSH_BRANCH}"
        )

    except Exception as exc:
        print(f"Ошибка при коммите/пуше: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()