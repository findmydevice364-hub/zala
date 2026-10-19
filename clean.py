
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import hashlib
from urllib.request import Request, urlopen
from urllib.parse import urlsplit

SOURCE_URL = "https://jekalich.github.io/smtk/tv.smtk.m3u"
OUTPUT_FILE = "playlist_clean.m3u"
REPORT_FILE = "playlist_report.txt"

TIMEOUT = 30


def download_playlist(url):
    request = Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 IPTV-Playlist-Cleaner/1.0"}
    )
    with urlopen(request, timeout=TIMEOUT) as response:
        data = response.read()

    # Поддержка UTF-8 с BOM и распространённых кодировок
    for encoding in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue

    raise ValueError("Не удалось определить кодировку плейлиста")


def valid_stream_url(value):
    """Проверяет формат URL, но не подключается к каналу."""
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme.lower() in {
                "http", "https", "rtsp", "rtmp",
                "udp", "rtp", "acestream"
            }
            and bool(parsed.netloc or parsed.scheme.lower() in {
                "udp", "rtp", "acestream"
            })
        )
    except ValueError:
        return False


def clean_playlist(text):
    lines = [
        line.strip().lstrip("\ufeff")
        for line in text.splitlines()
        if line.strip()
    ]

    if not lines or not any(
        line.upper().startswith("#EXTM3U") for line in lines[:5]
    ):
        raise ValueError("Источник не похож на M3U-плейлист")

    result = ["#EXTM3U"]
    report = []
    seen_urls = set()

    total = 0
    kept = 0
    duplicates = 0
    invalid = 0
    missing_url = 0

    # Обрабатываем отдельные записи, сохраняя директивы
    # #EXTVLCOPT, #KODIPROP и другие строки внутри записи.
    entry = []
    has_header = False

    def flush_entry(entry):
        nonlocal total, kept, duplicates, invalid, missing_url

        if not entry:
            return []

        extinf_index = next(
            (i for i, line in enumerate(entry)
             if line.startswith("#EXTINF:")),
            None
        )

        if extinf_index is None:
            # Глобальные директивы и прочие комментарии
            return []

        total += 1
        extinf = entry[extinf_index]

        if "," not in extinf:
            invalid += 1
            report.append(f"Некорректная EXTINF: {extinf[:150]}")
            return []

        # Ищем первую строку, не являющуюся директивой.
        url_index = next(
            (i for i, line in enumerate(entry)
             if not line.startswith("#")),
            None
        )

        if url_index is None:
            missing_url += 1
            report.append(f"Нет URL: {extinf.split(',', 1)[-1]}")
            return []

        url = entry[url_index].strip()

        if not valid_stream_url(url):
            invalid += 1
            report.append(f"Некорректный URL: {url[:200]}")
            return []

        if url in seen_urls:
            duplicates += 1
            return []

        seen_urls.add(url)

        # Сохраняем EXTINF и все дополнительные параметры.
        cleaned_entry = [
            line for i, line in enumerate(entry)
            if i != url_index
        ]
        cleaned_entry.append(url)

        kept += 1
        return cleaned_entry

    for line in lines:
        if line.upper().startswith("#EXTM3U"):
            if has_header:
                report.append("Повторный заголовок #EXTM3U пропущен")
            else:
                has_header = True
            continue

        if line.startswith("#EXTINF:"):
            if entry:
                result.extend(flush_entry(entry))
            entry = [line]
            continue

        if entry:
            entry.append(line)
        elif not line.startswith("#"):
            report.append(f"Строка вне записи пропущена: {line[:150]}")
        else:
            # Сохраняем глобальные директивы, если они встречаются.
            result.append(line)

    if entry:
        result.extend(flush_entry(entry))

    stats = (
        f"Всего записей: {total}\n"
        f"Сохранено: {kept}\n"
        f"Дубликатов удалено: {duplicates}\n"
        f"Некорректных записей: {invalid}\n"
        f"Без URL: {missing_url}\n"
    )

    return "\n".join(result) + "\n", stats, report


def main():
    source = sys.argv[1] if len(sys.argv) > 1 else SOURCE_URL

    print(f"Загружаю: {source}")

    try:
        original = download_playlist(source)
        cleaned, stats, report = clean_playlist(original)

        with open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
            f.write(cleaned)

        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(stats + "\n")
            f.write("\n".join(report) if report else "Ошибок формата не найдено")

        print("\nОбработка завершена.")
        print(stats)
        print(f"Плейлист: {OUTPUT_FILE}")
        print(f"Отчёт:    {REPORT_FILE}")
        print("Доступность и работоспособность каналов не проверялись.")

    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
