import requests
from pathlib import Path

URL = "https://raw.githubusercontent.com/findmydevice364-hub/zala/refs/heads/main/Main/donors_uz.m3u"

# Корневая папка репозитория
ROOT_DIR = Path.cwd()

# Файл в корне
OUTPUT_FILE = ROOT_DIR / "donors_uz.m3u"

try:
    response = requests.get(URL, timeout=60)
    response.raise_for_status()

    OUTPUT_FILE.write_bytes(response.content)

    print(f"[OK] Плейлист скачан: {OUTPUT_FILE}")
    print(f"[OK] Размер: {OUTPUT_FILE.stat().st_size} байт")

except requests.RequestException as e:
    print(f"[ERROR] Ошибка скачивания: {e}")

except OSError as e:
    print(f"[ERROR] Ошибка сохранения: {e}")