
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV COMPACT AUDITOR
====================

Обрабатывает только:

1. stable.m3u
2. IPTVmir.m3u8
3. legacy/IPTVstable.m3u8

Правила:
- Полностью одинаковые EXTINF + URL = одна запись.
- Одинаковое название, разные URL = сохранить все.
- Разные EXTINF = сохранить обе записи.
- Исходные файлы не изменяются.
- Компактные копии создаются в COMPACT/.
- Формируется JSON-отчет.
"""

import json
import hashlib

from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime, timezone


# =====================================================
# CONFIGURATION
# =====================================================

ROOT = Path(__file__).resolve().parent

COMPACT_DIR = ROOT / "COMPACT"

REPORT_DIR = ROOT / "IPTV_AUDIT"

REPORT_FILE = REPORT_DIR / "report.json"


# Только три целевых плейлиста

TARGET_PLAYLISTS = [
    Path("stable.m3u"),
    Path("IPTVmir.m3u8"),
    Path("legacy/IPTVstable.m3u8"),
]


# =====================================================
# FILE READING
# =====================================================

def read_text(path):

    raw = path.read_bytes()

    for encoding in (
        "utf-8-sig",
        "utf-8",
        "cp1251",
    ):

        try:
            return raw.decode(encoding)

        except UnicodeDecodeError:
            continue

    return raw.decode(
        "utf-8",
        errors="replace"
    )


# =====================================================
# M3U PARSER
# =====================================================

def parse_playlist(content):

    lines = content.splitlines()

    header = []
    entries = []
    current = []

    for line in lines:

        stripped = line.strip()

        if stripped.startswith("#EXTINF:"):

            if current:
                entries.append(current)

            current = [line]

        elif current:

            current.append(line)

        else:

            header.append(line)

    if current:
        entries.append(current)

    return header, entries


# =====================================================
# EXACT COMPARISON
# =====================================================

def normalize_entry(entry):

    return tuple(
        line.strip()
        for line in entry
        if line.strip()
    )


def entry_hash(entry):

    normalized = normalize_entry(entry)

    content = "\n".join(normalized)

    return hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()


# =====================================================
# CHANNEL INFORMATION
# =====================================================

def channel_name(entry):

    for line in entry:

        if line.startswith("#EXTINF:"):

            if "," in line:
                return line.split(",", 1)[1].strip()

            return line.strip()

    return "UNKNOWN"


def stream_urls(entry):

    return [
        line.strip()
        for line in entry
        if line.strip()
        and not line.strip().startswith("#")
    ]


# =====================================================
# COMPACT
# =====================================================

def compact_entries(entries):

    unique_entries = []

    seen = set()

    duplicate_count = 0

    for entry in entries:

        key = entry_hash(entry)

        if key in seen:

            duplicate_count += 1

            continue

        seen.add(key)

        unique_entries.append(entry)

    return unique_entries, duplicate_count


# =====================================================
# WRITE PLAYLIST
# =====================================================

def write_playlist(path, header, entries):

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    output = []

    if header:

        output.extend(header)

    else:

        output.append("#EXTM3U")

    for entry in entries:

        output.extend(entry)

    path.write_text(
        "\n".join(output) + "\n",
        encoding="utf-8"
    )


# =====================================================
# ANALYSIS
# =====================================================

def analyze_playlist(path):

    content = read_text(path)

    header, entries = parse_playlist(content)

    unique_entries, duplicates = compact_entries(
        entries
    )

    names = Counter(
        channel_name(entry)
        for entry in entries
    )

    urls_by_name = defaultdict(set)

    for entry in entries:

        name = channel_name(entry)

        for url in stream_urls(entry):

            urls_by_name[name].add(url)

    repeated_names = {}

    for name, count in names.items():

        if count > 1:

            repeated_names[name] = {
                "occurrences": count,
                "different_urls": len(
                    urls_by_name[name]
                )
            }

    return {
        "header": header,
        "entries": entries,
        "unique_entries": unique_entries,
        "total": len(entries),
        "unique": len(unique_entries),
        "duplicates": duplicates,
        "repeated_names": repeated_names
    }


# =====================================================
# MAIN
# =====================================================

def main():

    COMPACT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    report = {

        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),

        "target_playlists": [],

        "results": [],

        "summary": {}
    }

    total_records = 0
    total_unique = 0
    total_duplicates = 0

    total_original_bytes = 0
    total_compact_bytes = 0

    print("\n" + "=" * 65)
    print("IPTV COMPACT AUDITOR")
    print("=" * 65)

    for relative in TARGET_PLAYLISTS:

        source = ROOT / relative

        print("\nFILE:", relative.as_posix())

        # Проверка существования

        if not source.is_file():

            print("ERROR: SOURCE FILE NOT FOUND")

            report["results"].append({

                "file": relative.as_posix(),

                "error": "SOURCE FILE NOT FOUND"

            })

            continue

        try:

            original_size = source.stat().st_size

            result = analyze_playlist(source)

            # Сохраняем оригинальную структуру каталогов

            compact_path = COMPACT_DIR / relative

            write_playlist(
                compact_path,
                result["header"],
                result["unique_entries"]
            )

            compact_size = compact_path.stat().st_size

            saved_bytes = (
                original_size - compact_size
            )

            reduction = (

                100 * saved_bytes / original_size

                if original_size else 0

            )

            total_records += result["total"]

            total_unique += result["unique"]

            total_duplicates += result["duplicates"]

            total_original_bytes += original_size

            total_compact_bytes += compact_size

            item = {

                "file": relative.as_posix(),

                "compact_file": str(
                    compact_path.relative_to(ROOT)
                ),

                "original_size_bytes": original_size,

                "compact_size_bytes": compact_size,

                "saved_bytes": saved_bytes,

                "reduction_percent": round(
                    reduction,
                    2
                ),

                "total_records": result["total"],

                "unique_records": result["unique"],

                "duplicates_removed": result["duplicates"],

                "repeated_channel_names": (
                    result["repeated_names"]
                ),

                "status": "SUCCESS"

            }

            report["results"].append(item)

            report["target_playlists"].append(
                relative.as_posix()
            )

            print(
                "TOTAL RECORDS:",
                result["total"]
            )

            print(
                "UNIQUE RECORDS:",
                result["unique"]
            )

            print(
                "DUPLICATES REMOVED:",
                result["duplicates"]
            )

            print(
                "ORIGINAL SIZE:",
                round(
                    original_size / 1024 / 1024,
                    2
                ),
                "MB"
            )

            print(
                "COMPACT SIZE:",
                round(
                    compact_size / 1024 / 1024,
                    2
                ),
                "MB"
            )

            print(
                "SAVED:",
                round(
                    saved_bytes / 1024 / 1024,
                    2
                ),
                "MB"
            )

            print(
                "REDUCTION:",
                round(reduction, 2),
                "%"
            )

            print(
                "OUTPUT:",
                compact_path.relative_to(ROOT)
            )

        except Exception as error:

            print("ERROR:", str(error))

            report["results"].append({

                "file": relative.as_posix(),

                "status": "ERROR",

                "error": str(error)

            })

    # =================================================
    # SUMMARY
    # =================================================

    total_saved = (
        total_original_bytes - total_compact_bytes
    )

    total_reduction = (

        100 * total_saved / total_original_bytes

        if total_original_bytes else 0

    )

    report["summary"] = {

        "playlists_processed": len(
            report["target_playlists"]
        ),

        "total_records": total_records,

        "unique_records": total_unique,

        "duplicates_removed": total_duplicates,

        "original_size_mb": round(
            total_original_bytes / 1024 / 1024,
            2
        ),

        "compact_size_mb": round(
            total_compact_bytes / 1024 / 1024,
            2
        ),

        "saved_mb": round(
            total_saved / 1024 / 1024,
            2
        ),

        "reduction_percent": round(
            total_reduction,
            2
        )

    }

    REPORT_FILE.write_text(

        json.dumps(
            report,
            ensure_ascii=False,
            indent=2
        ),

        encoding="utf-8"

    )

    print("\n" + "=" * 65)
    print("AUDIT FINISHED")
    print("=" * 65)

    print("PROCESSED:", len(report["target_playlists"]))

    print("TOTAL:", total_records)

    print("UNIQUE:", total_unique)

    print("DUPLICATES:", total_duplicates)

    print("SAVED MB:", round(total_saved / 1024 / 1024, 2))

    print("REDUCTION:", round(total_reduction, 2), "%")

    print("\nCOMPACT DIRECTORY:", COMPACT_DIR)

    print("REPORT:", REPORT_FILE)


if __name__ == "__main__":
    main()
