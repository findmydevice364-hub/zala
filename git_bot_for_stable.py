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

Полная логика версии с исправлениями устойчивости:

- Многопоточная загрузка восьми донорских источников.
- Каждый доступный донор загружается целиком.
- Повреждённая запись M3U не останавливает разбор всего донора.
- Недоступность одного донора не отменяет обработку остальных.
- Фильтр только 18+ для всех источников.
- Без фильтрации по индексам URL, группам или количеству записей.
- Многопоточная проверка потоков HEAD -> GET fallback -> ffprobe.
- Кэш результата проверки URL на один запуск.
- Без дедупликации: повторные записи и альтернативные URL сохраняются.
- РТРС SD / РТРС HD (1–20) с нормализацией названий и строгой сортировкой.
- Нераспознанные названия не выбрасываются из пула из-за нормализации.
- Тематические группы для остальных каналов:
    Новости, Музыка, Детские, Кино, Познавательные,
    Развлекательные, Научные, Прочее.
- Целевые файлы:
    IPTVmir.m3u8
    legacy/IPTVstable.m3u8
    stable.m3u
    RTRS_stable.m3u8
- Поддержка SKIP_PUSH для GitHub Actions.
- Создание #EXTM3U, если файла нет.
- Бэкапы вне рабочего дерева Git.
- Полная git-логика с использованием TARGET_PUSH_BRANCH.

Правила устойчивости:
1. Отсутствующий URL у отдельного EXTINF не вызывает аварийного завершения.
2. Повреждённая запись пропускается; разбор продолжается со следующей.
3. Ошибка загрузки одного донора не отменяет обработку других доноров.
4. Пустой донор не обнуляет общий пул.
5. Нормализация РТРС меняет имена и порядок, а не отбрасывает
   кандидатов только из-за несовпадения с шаблоном.
6. Дедупликация отключена.
7. Явный фильтр 18+ остаётся активным.
8. Проверка доступности может исключить подтверждённо нерабочий поток;
   это отдельная операция, не связанная с нормализацией названий.

Важно: проверка доступности и ffprobe отражают состояние потока
на момент проверки и не гарантируют его постоянную работоспособность.
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

SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/IpTV_playlist_2026Ru/main/ngenix_found_1.m3u"
)

SECOND_SOURCE_URL = (
    "https://raw.githubusercontent.com/Free-TV/IPTV/"
    "refs/heads/master/playlists/playlist_russia.m3u8"
)

THIRD_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/Mir-IpTV-Ru-/main/Super_RTRS_2026.m3u"
)

TEST_CHANNELS_URL = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/IpTV_playlist_2026Ru/gh-pages/test_channels.m3u"
)

FIFTH_SOURCE_URL = "https://iptv.org.ua/iptv/avto-full.m3u"

SIXTH_SOURCE_URL = "https://smart-iptv.ru/russia.m3u"

SEVENTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "Dmitryi55/ruIPTV/main/IPTVru.m3u"
)

# Восьмой донор: Zala.
# Ссылка преобразована из GitHub /blob/ в raw.githubusercontent.com.
EIGHTH_SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "findmydevice364-hub/zala/main/zala_N.m3u"
)


# ==============================================================================
# ПЕРЕОПРЕДЕЛЕНИЕ ИСТОЧНИКОВ ЧЕРЕЗ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ==============================================================================

SOURCE_URL = os.environ.get("SOURCE_URL", SOURCE_URL)
SECOND_SOURCE_URL = os.environ.get(
    "SECOND_SOURCE_URL", SECOND_SOURCE_URL
)
THIRD_SOURCE_URL = os.environ.get(
    "THIRD_SOURCE_URL", THIRD_SOURCE_URL
)
TEST_CHANNELS_URL = os.environ.get(
    "TEST_CHANNELS_URL", TEST_CHANNELS_URL
)
FIFTH_SOURCE_URL = os.environ.get(
    "FIFTH_SOURCE_URL", FIFTH_SOURCE_URL
)
SIXTH_SOURCE_URL = os.environ.get(
    "SIXTH_SOURCE_URL", SIXTH_SOURCE_URL
)
SEVENTH_SOURCE_URL = os.environ.get(
    "SEVENTH_SOURCE_URL", SEVENTH_SOURCE_URL
)
EIGHTH_SOURCE_URL = os.environ.get(
    "EIGHTH_SOURCE_URL", EIGHTH_SOURCE_URL
)

SKIP_PUSH = (
    os.environ.get("SKIP_PUSH", "true").lower()
    in ("1", "true", "yes")
)

TARGET_PUSH_BRANCH = os.environ.get(
    "TARGET_PUSH_BRANCH", "gh-pages-2"
)


# ==============================================================================
# ЦЕЛЕВЫЕ ФАЙЛЫ
# ==============================================================================

TARGET_MIR = "IPTVmir.m3u8"
TARGET_STABLE_LEGACY = "legacy/IPTVstable.m3u8"
TARGET_STABLE = "stable.m3u"
TARGET_RTRS_STABLE = "RTRS_stable.m3u8"

# Резервные raw-фоллбеки, оставлены для совместимости.
TARGET_MIR_RAW = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/IPTVru/gh-pages/IPTVmir.m3u8"
)

TARGET_STABLE_LEGACY_RAW = (
    "https://raw.githubusercontent.com/"
    "Phoenix89S/IPTVru/gh-pages/legacy/IPTVstable.m3u8"
)


# ==============================================================================
# ЧЁРНЫЙ СПИСОК — ТОЛЬКО 18+
# ==============================================================================

BLACKLIST = {
    "playboy",
    "erox",
    "blue hustler",
    "barely legal",
    "adult",
    "private",
    "penthouse",
    "senstv",
    "o-la-la",
    "xxx",
    "18+",
    "porn",
    "hustler",
}


# ==============================================================================
# ГИБКАЯ СЕТКА РТРС
# ==============================================================================

# Значение — (группа, нормализованное имя, порядок сортировки).
# SD и HD имеют отдельные записи.
# Повторные кандидаты не удаляются.

RTRS_CHANNELS_MAP = {
    # ----- РТРС SD -----
    "первый канал": (
        "РТРС SD", "1.SD Первый канал", 1
    ),
    "россия 1": (
        "РТРС SD", "2.SD Россия 1", 2
    ),
    "матч тв": (
        "РТРС SD", "3.SD Матч ТВ", 3
    ),
    "нтв": (
        "РТРС SD", "4.SD НТВ", 4
    ),
    "пятый канал": (
        "РТРС SD", "5.SD Пятый канал", 5
    ),
    "россия к": (
        "РТРС SD", "6.SD Россия К", 6
    ),
    "россия 24": (
        "РТРС SD", "7.SD Россия 24", 7
    ),
    "карусель": (
        "РТРС SD", "8.SD Карусель", 8
    ),
    "отр": (
        "РТРС SD", "9.SD ОТР", 9
    ),
    "тв центр": (
        "РТРС SD", "10.SD ТВ Центр", 10
    ),
    "твц": (
        "РТРС SD", "10.SD ТВ Центр", 10
    ),
    "рен тв": (
        "РТРС SD", "11.SD РЕН ТВ", 11
    ),
    "спас": (
        "РТРС SD", "12.SD Спас", 12
    ),
    "стс": (
        "РТРС SD", "13.SD СТС", 13
    ),
    "домашний": (
        "РТРС SD", "14.SD Домашний", 14
    ),
    "тв-3": (
        "РТРС SD", "15.SD ТВ-3", 15
    ),
    "тв3": (
        "РТРС SD", "15.SD ТВ-3", 15
    ),
    "тв 3": (
        "РТРС SD", "15.SD ТВ-3", 15
    ),
    "пятница": (
        "РТРС SD", "16.SD Пятница!", 16
    ),
    "звезда": (
        "РТРС SD", "17.SD Звезда", 17
    ),
    "мир": (
        "РТРС SD", "18.SD Мир", 18
    ),
    "тнт": (
        "РТРС SD", "19.SD ТНТ", 19
    ),
    "муз тв": (
        "РТРС SD", "20.SD Муз-ТВ", 20
    ),

    # ----- РТРС HD -----
    "первый канал hd": (
        "РТРС HD", "1.HD Первый канал HD", 101
    ),
    "россия 1 hd": (
        "РТРС HD", "2.HD Россия 1 HD", 102
    ),
    "матч тв hd": (
        "РТРС HD", "3.HD Матч ТВ HD", 103
    ),
    "нтв hd": (
        "РТРС HD", "4.HD НТВ HD", 104
    ),
    "пятый канал hd": (
        "РТРС HD", "5.HD Пятый канал HD", 105
    ),
    "россия к hd": (
        "РТРС HD", "6.HD Россия К HD", 106
    ),
    "россия 24 hd": (
        "РТРС HD", "7.HD Россия 24 HD", 107
    ),
    "карусель hd": (
        "РТРС HD", "8.HD Карусель HD", 108
    ),
    "отр hd": (
        "РТРС HD", "9.HD ОТР HD", 109
    ),
    "тв центр hd": (
        "РТРС HD", "10.HD ТВ Центр HD", 110
    ),
    "твц hd": (
        "РТРС HD", "10.HD ТВ Центр HD", 110
    ),
    "рен тв hd": (
        "РТРС HD", "11.HD РЕН ТВ HD", 111
    ),
    "спас hd": (
        "РТРС HD", "12.HD Спас HD", 112
    ),
    "стс hd": (
        "РТРС HD", "13.HD СТС HD", 113
    ),
    "домашний hd": (
        "РТРС HD", "14.HD Домашний HD", 114
    ),
    "тв-3 hd": (
        "РТРС HD", "15.HD ТВ-3 HD", 115
    ),
    "тв3 hd": (
        "РТРС HD", "15.HD ТВ-3 HD", 115
    ),
    "тв 3 hd": (
        "РТРС HD", "15.HD ТВ-3 HD", 115
    ),
    "пятница hd": (
        "РТРС HD", "16.HD Пятница! HD", 116
    ),
    "звезда hd": (
        "РТРС HD", "17.HD Звезда HD", 117
    ),
    "мир hd": (
        "РТРС HD", "18.HD Мир HD", 118
    ),
    "тнт hd": (
        "РТРС HD", "19.HD ТНТ HD", 119
    ),
    "муз тв hd": (
        "РТРС HD", "20.HD Муз-ТВ HD", 120
    ),
}


# ==============================================================================
# ТЕМАТИЧЕСКИЕ ГРУППЫ
# ==============================================================================

CATEGORY_RULES = [
    ("Новости", [
        "новост", "news", "россия 24", "рбк", "rbc",
        "известия", "жизнь", "москва 24", "миротворец",
        "rt", "russia today", "bbc", "cnn", "euronews",
        "tv rain", "дождь", "current time", "наст. время",
        "360", "life", "life news"
    ]),
    ("Музыка", [
        "муз", "music", "радио", "radio", "hit", "mtv",
        "vh1", "jazz", "rock", "dance", "hip-hop",
        "hiphop", "shanson", "шансон", "europa plus",
        "love radio", "bridge", "bridge tv", "ru.tv",
        "music box"
    ]),
    ("Детские", [
        "дет", "kids", "cartoon", "мульт", "карусель",
        "disney", "nickelodeon", "baby", "детский",
        "мультик", "tiaradio", "cartoon network",
        "nick jr", "tiara", "мультиландия"
    ]),
    ("Кино", [
        "кино", "movie", "film", "cinema", "фильм",
        "кинохит", "кинопремьера", "premiere",
        "киносемья", "kino", "fox", "hbo", "amc",
        "sony", "кинопоказ", "кинокомедия",
        "индийское кино", "наше кино"
    ]),
    ("Познавательные", [
        "познават", "discovery", "national geographic",
        "nat geo", "history", "animal", "наука", "doc",
        "докум", "viasat", "viasat history",
        "viasat nature", "animal planet",
        "discovery science"
    ]),
    ("Развлекательные", [
        "развлек", "comedy", "юмор", "тв3", "пятница",
        "тнт", "ctc", "стс", "развлека", "юморист",
        "квн", "comedy club", "тнт4", "2x2"
    ]),
    ("Научные", [
        "науч", "science", "cosmos", "space", "tech",
        "техно", "наука 2.0", "science 2.0",
        "galaxy", "cosmos tv"
    ]),
]

CATEGORY_ORDER = [
    "Новости",
    "Музыка",
    "Детские",
    "Кино",
    "Познавательные",
    "Развлекательные",
    "Научные",
    "Прочее",
]


# ==============================================================================
# КЭШИ И СИНХРОНИЗАЦИЯ
# ==============================================================================

_STREAM_CHECK_CACHE = {}
_STREAM_CHECK_IN_PROGRESS = {}
_STREAM_CHECK_LOCK = threading.Lock()


# ==============================================================================
# УТИЛИТЫ GIT И СЕТИ
# ==============================================================================

def run(cmd, cwd=None, check=True):
    try:
        res = subprocess.run(
            cmd,
            cwd=cwd,
            check=check,
            capture_output=True,
            text=True
        )
        return res
    except subprocess.CalledProcessError as exc:
        print(
            f"Команда {' '.join(cmd)} завершилась с ошибкой: {exc}"
        )
        print(f"stderr:\n{exc.stderr}")
        raise


def git_is_clean():
    res = run(["git", "status", "--porcelain"], check=True)
    return res.stdout.strip() == ""


def ensure_on_branch(branch=TARGET_PUSH_BRANCH):
    """Переключается на целевую ветку и синхронизирует её с origin."""
    if not branch or branch.startswith("-"):
        raise ValueError(
            f"Некорректное имя целевой ветки: {branch!r}"
        )

    run(["git", "fetch", "origin", branch], check=True)

    local_exists = subprocess.run(
        [
            "git", "show-ref", "--verify",
            f"refs/heads/{branch}"
        ],
        capture_output=True,
        text=True
    ).returncode == 0

    if local_exists:
        run(["git", "checkout", branch], check=True)
    else:
        run([
            "git", "checkout", "-b",
            branch, f"origin/{branch}"
        ], check=True)

    run(["git", "pull", "--ff-only", "origin", branch], check=True)


def fetch_url(url, timeout=45, attempts=5):
    """
    Загружает донор целиком и повторяет временные сетевые ошибки.

    При окончательной ошибке возвращает пустую строку.
    Вызывающая функция решает, как продолжить работу с остальными
    источниками.
    """
    last_error = None

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; IPTVBot/4.0)",
        "Accept": "*/*",
        "Cache-Control": "no-cache",
    }

    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(
                req, timeout=timeout
            ) as response:
                raw_data = response.read()
                content_length = response.headers.get(
                    "Content-Length"
                )

            if not raw_data:
                raise ValueError(
                    "источник вернул пустое тело ответа"
                )

            if content_length:
                expected_size = int(content_length)

                if len(raw_data) != expected_size:
                    raise ValueError(
                        f"неполная загрузка: получено "
                        f"{len(raw_data)} из {expected_size} байт"
                    )

            for encoding in (
                "utf-8-sig", "utf-8", "cp1251", "latin-1"
            ):
                try:
                    return raw_data.decode(encoding)
                except UnicodeDecodeError:
                    continue

            return raw_data.decode(
                "utf-8", errors="replace"
            )

        except Exception as exc:
            last_error = exc

            print(
                f"Ошибка загрузки донора "
                f"(попытка {attempt}/{attempts}): "
                f"{url}: {exc}"
            )

            if attempt < attempts:
                time.sleep(min(3 * attempt, 15))

    print(
        f"ПРЕДУПРЕЖДЕНИЕ: донор не загружен после "
        f"{attempts} попыток: {url}; "
        f"последняя ошибка: {last_error}"
    )

    return ""


def check_stream_alive_simple(url, timeout=12):
    """Проверяет HTTP-доступность потока."""
    if not url or not url.lower().startswith(
        ("http://", "https://")
    ):
        return False

    headers = {
        "User-Agent": "VLC/3.0.16 LibVLC/3.0.16",
        "Accept": "*/*",
    }

    definitive_dead = False

    try:
        req = urllib.request.Request(
            url, headers=headers, method="HEAD"
        )

        with urllib.request.urlopen(
            req, timeout=timeout
        ) as response:
            if 200 <= response.status < 400:
                pass

    except urllib.error.HTTPError as exc:
        if exc.code in (404, 410):
            definitive_dead = True

    except Exception:
        pass

    try:
        get_headers = dict(headers)
        get_headers["Range"] = "bytes=0-2047"

        req = urllib.request.Request(
            url, headers=get_headers, method="GET"
        )

        with urllib.request.urlopen(
            req, timeout=timeout
        ) as response:
            chunk = response.read(2048)

            if 200 <= response.status < 400 and chunk:
                return True

    except urllib.error.HTTPError as exc:
        if exc.code in (404, 410):
            definitive_dead = True

    except Exception:
        pass

    return False if definitive_dead else None


def ensure_ffprobe():
    """Находит ffprobe или скачивает Linux x86_64 static build."""
    existing = shutil.which("ffprobe")

    if existing:
        print(f"Найден системный ffprobe: {existing}")
        return True

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system != "linux" or machine not in (
        "x86_64", "amd64"
    ):
        print(
            "Автозагрузка ffprobe поддерживается только "
            "на Linux x86_64. "
            f"Текущая платформа: {system}/{machine}. "
            "Установите ffmpeg вручную."
        )
        return False

    cache_dir = os.path.join(
        os.path.expanduser("~"),
        ".cache",
        "iptv_bot_bin"
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
                cache_dir
                + os.pathsep
                + os.environ.get("PATH", "")
            )

            print(f"ffprobe готов: {path}")
            return True

        except (
            OSError,
            subprocess.SubprocessError
        ) as exc:
            print(
                f"Кэшированный ffprobe не удалось запустить: "
                f"{exc}"
            )
            return False

    if (
        os.path.isfile(target_ffprobe)
        and activate_binary(target_ffprobe)
    ):
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

    print(
        "ffprobe не найден. Скачиваю статическую сборку "
        "для Linux x86_64..."
    )

    temp_archive = None
    temp_binary = None

    try:
        fd, temp_archive = tempfile.mkstemp(
            prefix="iptv-ffmpeg-",
            suffix=".tar.xz"
        )
        os.close(fd)

        request = urllib.request.Request(
            download_url,
            headers={
                "User-Agent":
                    "Mozilla/5.0 "
                    "(compatible; IPTVBotBootstrapper/1.0)"
            }
        )

        with urllib.request.urlopen(
            request, timeout=45
        ) as response, open(
            temp_archive, "wb"
        ) as out_file:
            shutil.copyfileobj(response, out_file)

        with tarfile.open(
            temp_archive, mode="r:xz"
        ) as archive:
            member = next(
                (
                    item
                    for item in archive.getmembers()
                    if (
                        item.isfile()
                        and item.name.rstrip("/").endswith(
                            "/ffprobe"
                        )
                    )
                ),
                None
            )

            if member is None:
                raise RuntimeError(
                    "В архиве не найден бинарник ffprobe"
                )

            source = archive.extractfile(member)

            if source is None:
                raise RuntimeError(
                    "Не удалось прочитать ffprobe из архива"
                )

            fd, temp_binary = tempfile.mkstemp(
                prefix="ffprobe-",
                dir=cache_dir
            )

            with os.fdopen(
                fd, "wb"
            ) as out_file, source:
                shutil.copyfileobj(source, out_file)

        os.chmod(temp_binary, 0o755)

        check = subprocess.run(
            [temp_binary, "-version"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if check.returncode != 0:
            raise RuntimeError(
                "Скачанный ffprobe не запускается"
            )

        os.replace(temp_binary, target_ffprobe)
        temp_binary = None

        os.environ["PATH"] = (
            cache_dir
            + os.pathsep
            + os.environ.get("PATH", "")
        )

        print(
            f"Автоматически загружен ffprobe: "
            f"{target_ffprobe}"
        )

        return True

    except Exception as exc:
        print(
            "Не удалось автоматически скачать/подготовить "
            f"ffprobe: {exc}"
        )
        return False

    finally:
        for temporary_path in (
            temp_archive, temp_binary
        ):
            if temporary_path:
                try:
                    os.remove(temporary_path)
                except OSError:
                    pass


def check_stream_quality_ffprobe(url, timeout=20):
    """True, если ffprobe обнаружил аудио- или видеопоток."""
    if not url or not url.lower().startswith(
        ("http://", "https://")
    ):
        return False

    if not shutil.which("ffprobe"):
        print("ОШИБКА: ffprobe недоступен.")
        return False

    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
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
            stream.get("codec_type") == "video"
            for stream in streams
        )

        has_audio = any(
            stream.get("codec_type") == "audio"
            for stream in streams
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
    """
    Проверяет поток с кэшированием результата.

    Сетевые сбои и неопределённые проверки не удаляют запись.
    Подтверждённые 404/410 при отсутствии медиапотока отклоняются.
    """
    if not url or not url.lower().startswith(
        ("http://", "https://")
    ):
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
            return _STREAM_CHECK_CACHE.get(url, True)

    try:
        http_result = check_stream_alive_simple(url)

        if http_result is True:
            valid = True
            print(
                "HTTP GET подтвердил получение данных: "
                f"{url}"
            )

        else:
            ffprobe_alive = check_stream_quality_ffprobe(url)

            if ffprobe_alive:
                valid = True

            elif http_result is False:
                valid = False
                print(
                    "Поток отклонён: подтверждённый HTTP "
                    "404/410 и ffprobe без медиапотока: "
                    f"{url}"
                )

            else:
                valid = True
                print(
                    "Проверка неопределённа; запись сохранена: "
                    f"{url}"
                )

        with _STREAM_CHECK_LOCK:
            _STREAM_CHECK_CACHE[url] = valid

        return valid

    finally:
        with _STREAM_CHECK_LOCK:
            completed_event = (
                _STREAM_CHECK_IN_PROGRESS.pop(url, None)
            )

            if completed_event is not None:
                completed_event.set()


# ==============================================================================
# УСТОЙЧИВЫЙ ПАРСИНГ M3U
# ==============================================================================

def parse_m3u_content(content):
    """
    Устойчиво разбирает M3U/M3U8.

    Правила:
    - повреждённая EXTINF без URL не останавливает парсер;
    - если следующий EXTINF пришёл раньше URL, пропускается только
      незакрытая предыдущая запись;
    - комментарии между EXTINF и URL разрешены;
    - любая непустая строка, не начинающаяся с #, рассматривается
      как строка URL-кандидата;
    - записи без EXTINF сохраняются как URL-кандидаты;
    - порядок записей сохраняется;
    - дедупликации нет;
    - количество записей не ограничивается;
    - после ошибки разбора продолжается чтение следующих строк.
    """
    if not isinstance(content, str) or not content.strip():
        raise ValueError(
            "Источник пуст или не содержит текста для разбора"
        )

    lines = content.splitlines()
    entries = []

    current_meta = None
    current_meta_line = None

    malformed_count = 0
    parsed_count = 0
    standalone_count = 0

    tvg_re = re.compile(
        r'\btvg-id\s*=\s*"([^"]*)"',
        re.IGNORECASE
    )

    def save_entry(meta, url, line_number):
        nonlocal parsed_count, standalone_count

        url = (url or "").strip()

        if not url:
            return

        if meta:
            parts = meta.split(",", 1)

            name = (
                parts[1].strip()
                if len(parts) > 1
                else ""
            )

            match = tvg_re.search(meta)

            if match and match.group(1).strip():
                key = match.group(1).strip().casefold()
            else:
                key = url.casefold()

            entries.append({
                "extinf": meta,
                "url": url,
                "key": key,
                "name": name,
                "source_line": line_number,
            })

            parsed_count += 1

        else:
            # URL без EXTINF не выбрасывается из общего пула.
            # Для записи будет сформирован стандартный EXTINF.
            entries.append({
                "extinf": "",
                "url": url,
                "key": url.casefold(),
                "name": "",
                "source_line": line_number,
            })

            standalone_count += 1
            parsed_count += 1

    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()

        if not stripped:
            continue

        if stripped.startswith("#EXTINF:"):
            if current_meta is not None:
                malformed_count += 1

                print(
                    "[M3U WARNING] EXTINF без URL: "
                    f"запись началась в строке "
                    f"{current_meta_line}; "
                    f"новая EXTINF найдена в строке "
                    f"{line_number}. "
                    "Пропускается только незакрытая запись."
                )

            current_meta = stripped
            current_meta_line = line_number
            continue

        # Комментарии, параметры VLC и прочие служебные строки
        # не считаются URL и не закрывают текущую EXTINF.
        if stripped.startswith("#"):
            continue

        # Любая непустая строка вне комментария считается URL-кандидатом.
        # Протокол и доступность проверяются на следующем этапе.
        if current_meta is not None:
            save_entry(
                current_meta,
                stripped,
                current_meta_line
            )

            current_meta = None
            current_meta_line = None

        else:
            save_entry(
                "",
                stripped,
                line_number
            )

    if current_meta is not None:
        malformed_count += 1

        print(
            "[M3U WARNING] Последняя запись EXTINF не содержит URL: "
            f"строка {current_meta_line}. "
            "Запись пропущена; разбор завершён без аварии."
        )

    print(
        "[M3U PARSER] "
        f"Строк: {len(lines)}; "
        f"разобрано записей: {parsed_count}; "
        f"URL без EXTINF: {standalone_count}; "
        f"повреждённых EXTINF: {malformed_count}."
    )

    # Пустой результат не является аварией для всего процесса.
    # Вызывающая функция продолжит работу с другими донорами.
    return entries


def is_blacklisted(title):
    """Исключает записи, название которых соответствует фильтру 18+."""
    text_value = (title or "").strip().casefold()
    return any(
        bad.casefold() in text_value
        for bad in BLACKLIST
    )


def has_digit_index_in_path(url):
    """Оставлена для совместимости; намеренно не используется как фильтр."""
    return bool(re.search(r"/\d+/", url or ""))


# ==============================================================================
# НОРМАЛИЗАЦИЯ ИМЁН
# ==============================================================================

def normalize_channel_text(value):
    """Нормализует пунктуацию и пробелы, сохраняя границы слов."""
    clean = (
        (value or "")
        .casefold()
        .replace("ё", "е")
    )

    clean = re.sub(
        r"[‐-‒–—−_./\\:]+",
        " ",
        clean
    )

    clean = re.sub(
        r"[^\w\s]",
        " ",
        clean,
        flags=re.UNICODE
    )

    clean = re.sub(r"\s+", " ", clean).strip()

    return clean


def _contains_phrase(text, phrase):
    """Ищет фразу по границам слов."""
    if not text or not phrase:
        return False

    return re.search(
        r"(?<!\w)"
        + re.escape(phrase)
        + r"(?!\w)",
        text,
        flags=re.UNICODE
    ) is not None


_NORMALIZED_RTRS_KEYS = []

for _key, _value in RTRS_CHANNELS_MAP.items():
    _normalized_key = normalize_channel_text(_key)

    if _normalized_key:
        _NORMALIZED_RTRS_KEYS.append(
            (_normalized_key, _value, _key)
        )


def match_rtrs(name):
    """
    Сопоставляет название с сеткой РТРС.

    Важно: отсутствие совпадения возвращает None, но не является
    основанием удалить запись из общего пула.
    """
    clean = normalize_channel_text(name)

    if not clean:
        return None

    hd_keys = [
        item
        for item in _NORMALIZED_RTRS_KEYS
        if "hd" in item[0].split()
    ]

    sd_keys = [
        item
        for item in _NORMALIZED_RTRS_KEYS
        if "hd" not in item[0].split()
    ]

    for key, value, original_key in sorted(
        hd_keys,
        key=lambda item: len(item[0]),
        reverse=True
    ):
        if _contains_phrase(clean, key):
            return value

    for key, value, original_key in sorted(
        sd_keys,
        key=lambda item: len(item[0]),
        reverse=True
    ):
        if _contains_phrase(clean, key):
            return value

    return None


def _keyword_match(clean, keyword):
    """Сопоставляет тематические ключи."""
    kw = normalize_channel_text(keyword)

    if not kw:
        return False

    if kw in {
        "rt", "rbc", "bbc", "cnn",
        "mtv", "vh1", "fox", "hbo",
        "amc", "ctc"
    }:
        return _contains_phrase(clean, kw)

    return kw in clean


def detect_category(name):
    """Определяет тематическую группу по названию канала."""
    clean = normalize_channel_text(name)

    for category, keywords in CATEGORY_RULES:
        for keyword in keywords:
            if _keyword_match(clean, keyword):
                return category

    return "Прочее"


def smart_format_entry(
    name,
    extinf,
    default_group="Стабильные ТВ"
):
    """
    Форматирует запись.

    Сопоставление с РТРС меняет название и группу, но само по себе
    не удаляет и не отклоняет запись.
    """
    matched = match_rtrs(name)

    if matched:
        group_title, formatted_name, _ = matched

        source_meta = (
            (extinf or "#EXTINF:-1").split(",", 1)[0]
        )

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
            source_meta = (
                f'{source_meta} group-title="{group_title}"'
            )

        return (
            f"{source_meta},{formatted_name}",
            group_title
        )

    if not extinf:
        safe_name = name or "Без названия"

        return (
            f'#EXTINF:-1 group-title="{default_group}",'
            f'{safe_name}',
            default_group
        )

    if re.search(
        r'\bgroup-title="[^"]*"',
        extinf,
        flags=re.IGNORECASE
    ):
        meta, channel_name = (
            extinf.split(",", 1)
            if "," in extinf
            else (extinf, name or "Без названия")
        )

        if re.search(
            r'\bgroup-title="[^"]*"',
            meta,
            flags=re.IGNORECASE
        ):
            meta = re.sub(
                r'\bgroup-title="[^"]*"',
                f'group-title="{default_group}"',
                meta,
                flags=re.IGNORECASE
            )

        return f"{meta},{channel_name}", default_group

    meta, channel_name = (
        extinf.split(",", 1)
        if "," in extinf
        else (extinf, name or "Без названия")
    )

    return (
        f'{meta} group-title="{default_group}",{channel_name}',
        default_group
    )


# ==============================================================================
# МЕТАДАННЫЕ ПЛЕЙЛИСТОВ
# ==============================================================================

def get_build_metadata(
    playlist_schema="3.0",
    total_candidates=0,
    valid_added=0
):
    """Формирует метаданные сборщика и плейлиста."""
    builder_version = "3.0.0-beta.2.1"
    builder_code = "30.2.0.1"

    run_number = os.environ.get("GITHUB_RUN_NUMBER")
    commit_sha = os.environ.get("GITHUB_SHA")

    build_phase = (
        f"B{run_number}"
        if run_number
        else "B-local"
    )

    commit_tag = (
        commit_sha[:7]
        if commit_sha
        else time.strftime("%Y%m%d")
    )

    build_id = os.environ.get(
        "BUILD_ID",
        f"P3021-{build_phase}-{commit_tag}"
    )

    return {
        "playlist_schema": playlist_schema,
        "builder_name": "git_bot_for_stable.py",
        "builder_version": builder_version,
        "builder_code": builder_code,
        "build_id": build_id,
        "total_candidates": total_candidates,
        "valid_added": valid_added,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def build_playlist_header(
    target_path,
    total_candidates=0,
    valid_added=0,
    playlist_schema="3.0"
):
    """Возвращает единый паспорт сборки для любого плейлиста."""
    meta = get_build_metadata(
        playlist_schema=playlist_schema,
        total_candidates=total_candidates,
        valid_added=valid_added
    )

    return [
        "#EXTM3U",
        "# ===== BUILD METADATA BEGIN =====",
        "# ==============================================================================",
        f"# Target File:      {os.path.basename(target_path)}",
        (
            f"# Playlist Schema:  v{meta['playlist_schema']} "
            "(Strict RTRS + Eight-source pools)"
        ),
        f"# Generated At:     {meta['timestamp']}",
        "# ------------------------------------------------------------------------------",
        f"# Builder Engine:   {meta['builder_name']}",
        (
            f"# Builder Version:  {meta['builder_version']} "
            f"(versionCode: {meta['builder_code']})"
        ),
        f"# Build ID:         {meta['build_id']}",
        (
            f"# Pipeline Stats:   Candidates: "
            f"{meta['total_candidates']} | "
            f"Written Valid: {meta['valid_added']}"
        ),
        (
            "# Validation:       GET/ffprobe; inconclusive checks retained "
            "| Deduplication: disabled"
        ),
        "# Donor policy:      Partial donor failures are non-fatal",
        "# RTRS policy:       Normalize/order without name-based deletion",
        "# ==============================================================================",
        "# ===== BUILD METADATA END =====",
        "",
    ]


def remove_existing_metadata_header(content):
    """Удаляет прежнюю шапку сборки, сохраняя записи и их порядок."""
    lines = content.splitlines()
    result = []
    in_metadata = False
    at_file_start = True

    legacy_header_prefixes = (
        "# Generated:",
        "# Project:",
        "# Version:",
        "# Validation:",
    )

    for line in lines:
        stripped = line.strip()

        if stripped == "# ===== BUILD METADATA BEGIN =====":
            in_metadata = True
            continue

        if in_metadata:
            if stripped == "# ===== BUILD METADATA END =====":
                in_metadata = False
            continue

        if at_file_start:
            if not stripped or stripped == "#EXTM3U":
                continue

            if stripped.startswith(legacy_header_prefixes):
                continue

            at_file_start = False

        result.append(line)

    return "\n".join(result).lstrip("\n")


def backup_file(path):
    """Создаёт бэкап во временном каталоге вне рабочего дерева Git."""
    if not os.path.exists(path):
        return None

    backup_root = os.path.join(
        tempfile.gettempdir(),
        "iptv_bot_backups"
    )

    os.makedirs(backup_root, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")

    safe_name = (
        os.path.abspath(path)
        .replace(":", "")
        .replace(os.sep, "_")
    )

    backup_path = os.path.join(
        backup_root,
        f"{safe_name}.bak_{timestamp}"
    )

    shutil.copy2(path, backup_path)

    print(
        "Создан backup вне рабочего дерева Git: "
        f"{backup_path}"
    )

    return backup_path


# ==============================================================================
# ВОРКЕРЫ ПРОВЕРКИ ПОТОКОВ
# ==============================================================================

def process_single_stream_check_simple(entry):
    """Воркер общей проверки HEAD -> GET fallback -> ffprobe."""
    if check_stream_all(entry.get("url", "")):
        return entry

    return None


def process_single_stream_check_ffprobe(entry):
    """Совместимое имя воркера; использует общий конвейер."""
    if check_stream_all(entry.get("url", "")):
        return entry

    return None


# ==============================================================================
# ОБЫЧНОЕ ДОБАВЛЕНИЕ — БЕЗ ДЕДУПЛИКАЦИИ
# ==============================================================================

def append_all_valid(target_path, donor_entries):
    """
    Добавляет прошедшие проверку записи без дедупликации.

    Существующие записи сохраняются.
    Повторные URL и одинаковые названия не удаляются.
    """
    if not donor_entries:
        print(
            f"Для {target_path} нет новых донорских кандидатов. "
            "Существующий файл будет сохранён."
        )

        if not os.path.exists(target_path):
            os.makedirs(
                os.path.dirname(target_path) or ".",
                exist_ok=True
            )

            header = build_playlist_header(
                target_path,
                total_candidates=0,
                valid_added=0
            )

            with open(
                target_path, "w", encoding="utf-8"
            ) as file:
                file.write("\n".join(header).rstrip() + "\n")

        return 0

    print(
        "Многопоточная проверка HEAD -> GET -> ffprobe: "
        f"{len(donor_entries)} кандидатов → {target_path}"
    )

    valid_candidates = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=12
    ) as executor:
        results = executor.map(
            process_single_stream_check_simple,
            donor_entries
        )

        for result in results:
            if result:
                valid_candidates.append(result)

    to_append = []

    for entry in valid_candidates:
        name = entry.get("name", "")
        extinf, _ = smart_format_entry(
            name,
            entry.get("extinf", "")
        )

        to_append.append((
            extinf,
            entry.get("url", "")
        ))

    file_exists = os.path.exists(target_path)
    existing_body = ""

    if file_exists:
        backup_file(target_path)

        try:
            with open(
                target_path,
                "r",
                encoding="utf-8",
                errors="replace"
            ) as file:
                existing_body = remove_existing_metadata_header(
                    file.read()
                )

        except OSError as exc:
            print(
                f"Не удалось прочитать прежний плейлист "
                f"{target_path}: {exc}"
            )

            # Не перезаписываем существующий файл пустым содержимым
            # при ошибке чтения.
            print(
                "Операция для этого файла пропущена, "
                "чтобы не потерять прежние записи."
            )
            return 0

    os.makedirs(
        os.path.dirname(target_path) or ".",
        exist_ok=True
    )

    header = build_playlist_header(
        target_path,
        total_candidates=len(donor_entries),
        valid_added=len(to_append)
    )

    output_lines = list(header)

    if existing_body.strip():
        output_lines.append(existing_body.rstrip())

    if to_append:
        output_lines.append(
            "# ===== AUTO-UPDATED STABLE STREAMS "
            f"({time.strftime('%Y-%m-%d %H:%M:%S')}) ====="
        )

        for extinf, url in to_append:
            output_lines.append(extinf.rstrip())
            output_lines.append(url.rstrip())

    with open(
        target_path, "w", encoding="utf-8"
    ) as file:
        file.write(
            "\n".join(output_lines).rstrip() + "\n"
        )

    print(
        f"В {target_path} добавлено новых рабочих записей: "
        f"{len(to_append)}"
    )

    return len(to_append)


# ==============================================================================
# СПЕЦИАЛЬНАЯ ЛОГИКА RTRS_stable.m3u8
# ==============================================================================

def build_rtrs_stable(
    donor_entries,
    target_path=TARGET_RTRS_STABLE
):
    """
    Формирует RTRS_stable.m3u8.

    РТРС:
    - распознанные названия нормализуются;
    - порядок определяется сеткой SD/HD;
    - повторные кандидаты сохраняются;
    - повторные URL сохраняются.

    Остальные:
    - сохраняются в тематических группах;
    - неизвестная тематика относится в «Прочее»;
    - отсутствие совпадения с RTRS_CHANNELS_MAP не удаляет канал.

    Проверка доступности является отдельным этапом и не зависит
    от результата нормализации названия.
    """
    print(
        f"Формирование {target_path} "
        "(РТРС + тематика + HEAD/GET/ffprobe)..."
    )

    rtrs_candidates = []
    other_candidates = []

    # Сначала классифицируем ВСЕ записи.
    # Само отсутствие совпадения с сеткой РТРС не отбрасывает запись.
    for entry in donor_entries:
        matched = match_rtrs(entry.get("name", ""))

        candidate = dict(entry)

        if matched:
            group, base_name, order = matched

            candidate["_rtrs"] = (
                group,
                base_name,
                order
            )

            rtrs_candidates.append(candidate)

        else:
            other_candidates.append(candidate)

    print(f"РТРС-кандидатов до проверки: {len(rtrs_candidates)}")
    print(
        "Кандидатов вне сетки РТРС до проверки: "
        f"{len(other_candidates)}"
    )
    print(
        "Нормализация не удаляет записи: "
        "проверяются все кандидаты обеих категорий."
    )

    valid_rtrs = []
    valid_other = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=8
    ) as executor:
        rtrs_results = executor.map(
            process_single_stream_check_ffprobe,
            rtrs_candidates
        )

        for result in rtrs_results:
            if result:
                valid_rtrs.append(result)

        other_results = executor.map(
            process_single_stream_check_ffprobe,
            other_candidates
        )

        for result in other_results:
            if result:
                valid_other.append(result)

    print(
        f"Рабочих РТРС после проверки потоков: "
        f"{len(valid_rtrs)}"
    )

    print(
        "Рабочих остальных после проверки потоков: "
        f"{len(valid_other)}"
    )

    lines = []
    total = 0

    # --------------------------------------------------------------------------
    # БЛОК 1: РТРС SD + HD
    # --------------------------------------------------------------------------

    if valid_rtrs:
        slots = defaultdict(list)

        for entry in valid_rtrs:
            group, base_name, order = entry["_rtrs"]

            slots[
                (order, base_name, group)
            ].append({
                "url": entry.get("url", ""),
                "order": order,
                "group": group,
                "source": entry.get("source", ""),
                "source_line": entry.get("source_line"),
            })

        sorted_slots = sorted(
            slots.items(),
            key=lambda item: (
                item[0][0],
                item[0][1]
            )
        )

        lines.append(
            "# ===== РТРС SD / РТРС HD "
            "(первые два мультиплекса) ====="
        )

        for (order, base_name, group), variants in sorted_slots:
            prefix, separator, rest = base_name.partition(" ")

            for index, variant in enumerate(variants):
                if index == 0:
                    display_name = base_name
                else:
                    # Номер варианта нужен для различения записей.
                    # Записи при этом не объединяются и не удаляются.
                    display_name = (
                        f"{prefix}.{index} {rest}"
                    )

                lines.append(
                    f'#EXTINF:-1 group-title="{group}",'
                    f'{display_name}'
                )

                lines.append(variant["url"])
                total += 1

        lines.append("")

    # --------------------------------------------------------------------------
    # БЛОК 2: ОСТАЛЬНЫЕ КАНАЛЫ ПО ТЕМАТИКЕ
    # --------------------------------------------------------------------------

    if valid_other:
        by_category = defaultdict(list)

        for entry in valid_other:
            category = detect_category(
                entry.get("name", "")
            )

            by_category[category].append(entry)

        lines.append("# ===== Тематические группы =====")

        for category in CATEGORY_ORDER:
            if category not in by_category:
                continue

            lines.append(f"# --- {category} ---")

            for entry in by_category[category]:
                name = (
                    entry.get("name", "").strip()
                    or "Без названия"
                )

                extinf = entry.get("extinf", "")

                if not extinf:
                    new_extinf = (
                        f'#EXTINF:-1 group-title="{category}",'
                        f'{name}'
                    )

                else:
                    if "," in extinf:
                        meta, original_name = extinf.split(",", 1)
                    else:
                        meta = extinf
                        original_name = name

                    if re.search(
                        r'\bgroup-title="[^"]*"',
                        meta,
                        flags=re.IGNORECASE
                    ):
                        new_meta = re.sub(
                            r'\bgroup-title="[^"]*"',
                            f'group-title="{category}"',
                            meta,
                            flags=re.IGNORECASE
                        )
                    else:
                        new_meta = (
                            f'{meta} group-title="{category}"'
                        )

                    new_extinf = (
                        f"{new_meta},{original_name}"
                    )

                lines.append(new_extinf)
                lines.append(entry.get("url", ""))
                total += 1

            lines.append("")

    # Если пул пуст, файл всё равно получает корректную шапку.
    # Но существующий файл с записями нельзя обнулять только потому,
    # что на текущем запуске нет пригодных кандидатов.
    if total == 0 and os.path.exists(target_path):
        try:
            with open(
                target_path,
                "r",
                encoding="utf-8",
                errors="replace"
            ) as file:
                existing_content = file.read()

            existing_body = remove_existing_metadata_header(
                existing_content
            )

            if existing_body.strip():
                print(
                    "ВНИМАНИЕ: на текущем запуске не найдено "
                    "прошедших проверку кандидатов для RTRS. "
                    "Существующее содержимое сохраняется, "
                    "чтобы не обнулить плейлист."
                )
                return 0

        except OSError as exc:
            print(
                f"Не удалось прочитать существующий "
                f"{target_path}: {exc}. "
                "Существующий файл не перезаписывается."
            )
            return 0

    header = build_playlist_header(
        target_path,
        total_candidates=len(donor_entries),
        valid_added=total
    )

    output_lines = header + lines

    if os.path.exists(target_path):
        backup_file(target_path)

    os.makedirs(
        os.path.dirname(target_path) or ".",
        exist_ok=True
    )

    with open(
        target_path, "w", encoding="utf-8"
    ) as file:
        file.write(
            "\n".join(output_lines).rstrip() + "\n"
        )

    print(
        f"В {target_path} записано рабочих потоков: {total}"
    )

    print(
        "Проверка нормализации завершена: "
        "неизвестные названия не отбрасывались из-за отсутствия "
        "совпадения с сеткой РТРС."
    )

    return total


# ==============================================================================
# ЗАГРУЗКА ИСТОЧНИКОВ
# ==============================================================================

def fetch_source_worker(source_tuple):
    """
    Загружает и разбирает одного донора.

    Ошибка отдельной записи не прерывает разбор.
    Ошибка загрузки донора возвращает пустой список и не останавливает
    общий сбор остальных источников.
    """
    name, url, apply_filter = source_tuple

    if not url or not url.strip():
        print(
            f"[DONOR WARNING] Для {name} не задан URL. "
            "Донор пропущен; остальные источники продолжают работу."
        )

        return name, []

    print(
        f"Параллельная загрузка {name}: {url}"
    )

    raw_data = fetch_url(
        url,
        timeout=45,
        attempts=5
    )

    if not raw_data:
        print(
            f"[DONOR WARNING] {name} не удалось загрузить. "
            "Продолжаем обработку остальных доноров."
        )

        return name, []

    try:
        entries = parse_m3u_content(raw_data)

    except Exception as exc:
        print(
            f"[DONOR WARNING] Ошибка разбора {name}: {exc}. "
            "Донор не остановит сборку остальных источников."
        )

        return name, []

    if not entries:
        print(
            f"[DONOR WARNING] {name} не содержит распознаваемых "
            "записей. Остальные доноры продолжают обработку."
        )

        return name, []

    parsed_count = len(entries)
    filtered_entries = []

    for entry in entries:
        channel_name = entry.get("name", "")

        # Не фильтруем по индексу URL, номеру канала или группе.
        # apply_filter сохранён для совместимости с прежней сигнатурой.
        # Единственный контентный фильтр — явный BLACKLIST 18+.
        if is_blacklisted(channel_name):
            continue

        entry["source"] = name
        filtered_entries.append(entry)

    excluded_18 = parsed_count - len(filtered_entries)

    print(
        f"Из {name}: распознано {parsed_count}; "
        f"исключено только 18+ {excluded_18}; "
        f"добавлено в общий пул {len(filtered_entries)}"
    )

    return name, filtered_entries


# ==============================================================================
# ОСНОВНОЙ ПОТОК
# ==============================================================================

def main():
    if not shutil.which("git"):
        print(
            "Git не найден в PATH. Установите git и повторите."
        )
        sys.exit(1)

    if not ensure_ffprobe():
        print(
            "ОШИБКА: не удалось найти или автоматически загрузить "
            "ffprobe. Проверка потоков невозможна."
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
        print(
            f"Целевая ветка публикации: {branch_name}"
        )

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

    # Восемь доноров.
    # Ни один донор не является критическим условием для запуска
    # обработки остальных источников.
    sources = [
        ("SOURCE_URL", SOURCE_URL, False),
        ("SECOND_SOURCE_URL", SECOND_SOURCE_URL, True),
        ("THIRD_SOURCE_URL", THIRD_SOURCE_URL, True),
        ("TEST_CHANNELS_URL", TEST_CHANNELS_URL, False),
        ("FIFTH_SOURCE_URL", FIFTH_SOURCE_URL, False),
        ("SIXTH_SOURCE_URL", SIXTH_SOURCE_URL, False),
        ("SEVENTH_SOURCE_URL", SEVENTH_SOURCE_URL, False),
        ("EIGHTH_SOURCE_URL", EIGHTH_SOURCE_URL, False),
    ]

    donor_entries = []
    source_counts = {}

    print(
        "Запуск многопоточной загрузки доноров: "
        f"{len(sources)}. "
        "Каждый доступный источник загружается целиком; "
        "повреждённые отдельные записи не останавливают разбор."
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=len(sources)
    ) as executor:
        future_to_source = {
            executor.submit(
                fetch_source_worker,
                source
            ): source[0]
            for source in sources
        }

        for future in concurrent.futures.as_completed(
            future_to_source
        ):
            source_name = future_to_source[future]

            try:
                name, entries = future.result()

                source_counts[name] = len(entries)
                donor_entries.extend(entries)

                print(
                    f"ИТОГО ПО ДОНOРУ {name}: "
                    f"{len(entries)} записей добавлено "
                    "в общий пул"
                )

            except Exception as exc:
                # Страховка от неожиданной ошибки в воркере.
                # Остальные доноры и общий пул сохраняются.
                source_counts[source_name] = 0

                print(
                    f"[DONOR WARNING] Необработанная ошибка "
                    f"{source_name}: {exc}. "
                    "Продолжаем работу с другими донорами."
                )

    # --------------------------------------------------------------------------
    # ПРОВЕРКА ОБЩЕГО ПУЛА
    # --------------------------------------------------------------------------

    if not donor_entries:
        print(
            "Ни один донор не предоставил пригодных для разбора "
            "записей. Выходные плейлисты не изменены."
        )
        sys.exit(1)

    print()
    print("Сводка по донорам:")

    for source_name in (
        "SOURCE_URL",
        "SECOND_SOURCE_URL",
        "THIRD_SOURCE_URL",
        "TEST_CHANNELS_URL",
        "FIFTH_SOURCE_URL",
        "SIXTH_SOURCE_URL",
        "SEVENTH_SOURCE_URL",
        "EIGHTH_SOURCE_URL",
    ):
        print(
            f"  {source_name}: "
            f"{source_counts.get(source_name, 0)}"
        )

    print()
    print(
        "Всего кандидатов после исключения только blacklist 18+: "
        f"{len(donor_entries)}"
    )

    print(
        "Дедупликация отключена: все повторные записи "
        "и альтернативные URL сохраняются."
    )

    print(
        "Нормализация РТРС не удаляет кандидатов из-за "
        "несовпадения названий с шаблонами."
    )

    print(
        "Политика проверки: подтверждённые HTTP 404/410 "
        "вместе с отсутствием медиапотока могут быть отклонены."
    )

    print(
        "Тайм-ауты и неопределённые сетевые сбои сохраняются "
        "как кандидаты."
    )

    print(
        "Ошибки отдельных доноров не отменяют общую сборку."
    )

    modified = []
    added_total = 0

    # --------------------------------------------------------------------------
    # 1. ОБЫЧНЫЕ ПЛЕЙЛИСТЫ
    # --------------------------------------------------------------------------

    for target in (
        TARGET_MIR,
        TARGET_STABLE_LEGACY,
        TARGET_STABLE
    ):
        added = append_all_valid(
            target,
            donor_entries
        )

        if added:
            print(
                f"В {target} добавлено рабочих записей: "
                f"{added}"
            )

            modified.append(target)
            added_total += added

        else:
            print(
                f"В {target} новых прошедших проверку "
                "записей не обнаружено."
            )

            # Если функция создала пустой плейлист с шапкой,
            # файл также следует учитывать для публикации.
            if os.path.exists(target) and not SKIP_PUSH:
                modified.append(target)

    # --------------------------------------------------------------------------
    # 2. СПЕЦИАЛЬНЫЙ RTRS_stable.m3u8
    # --------------------------------------------------------------------------

    rtrs_added = build_rtrs_stable(
        donor_entries,
        TARGET_RTRS_STABLE
    )

    if (
        os.path.exists(TARGET_RTRS_STABLE)
        and TARGET_RTRS_STABLE not in modified
    ):
        modified.append(TARGET_RTRS_STABLE)

    added_total += rtrs_added

    # --------------------------------------------------------------------------
    # ИТОГИ И ПУБЛИКАЦИЯ
    # --------------------------------------------------------------------------

    if not modified:
        print(
            "Изменений для публикации нет. "
            "Новые рабочие потоки не добавлены."
        )
        sys.exit(0)

    if SKIP_PUSH:
        print()
        print(
            f"Готово! Обработано рабочих записей: "
            f"{added_total}."
        )
        print("Modified files:", modified)
        print(
            "SKIP_PUSH=true — коммит и пуш не выполнялись."
        )
        sys.exit(0)

    try:
        run(
            ["git", "add", "--"] + modified,
            check=True
        )

        staged = run(
            ["git", "diff", "--cached", "--quiet"],
            check=False
        )

        if staged.returncode == 0:
            print(
                "Нет staged-изменений; коммит не создаётся."
            )
            sys.exit(0)

        commit_msg = (
            "chore: full IPTV validation + RTRS normalization "
            "(v30.2.0.1 / 3.0.0-beta.2.1) "
            f"({time.strftime('%Y-%m-%d %H:%M:%S')})"
        )

        run(
            ["git", "commit", "-m", commit_msg],
            check=True
        )

        run(
            ["git", "push", "origin", TARGET_PUSH_BRANCH],
            check=True
        )

        print(
            f"Успешно опубликовано в ветку: "
            f"{TARGET_PUSH_BRANCH}"
        )

    except Exception as exc:
        print(
            f"Ошибка при коммите/пуше: {exc}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()