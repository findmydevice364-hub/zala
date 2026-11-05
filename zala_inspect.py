import urllib.request
import urllib.error
import urllib.parse
import socket
import ssl
import sys
import time
import datetime
import uuid
import re
import json
import os
import hashlib
from collections import Counter

# ============================================================
# Конфигурация пайплайна Phoenix Hub 'Baryshnikov'
# ============================================================

PIPELINE_NAME = "Phoenix Hub 'Baryshnikov'"
VERSION = "3.0.0-beta.2.1"
VERSION_CODE = 30021

BUILD_ID = (
    f"BUILD-{datetime.datetime.now().strftime('%Y%m%d')}-"
    f"{uuid.uuid4().hex[:6].upper()}"
)

TEST_URL = (
    "https://mfe.svc.ott.zala.by/"
    "hls/M52YG9EVYD_HLS/variant.m3u8"
)

REPORT_FILENAME = "zala_diagnostic_report.txt"
JSON_REPORT_FILENAME = "zala_diagnostic_report.json"

REQUEST_TIMEOUT = 6
READ_TIMEOUT = 6
RETRY_COUNT = 1
RETRY_DELAY = 0.5
REQUEST_DELAY = 0.2

MAX_RESPONSE_BYTES = 1024 * 1024
BODY_PREVIEW_LENGTH = 240
MAX_REDIRECTS = 5

# Фирменная стратегия обхода региональных ограничений:
# Шлюз OTT может проверять сигнатуру проксирования и подсеть BY.
BY_GEO_SIMULATION_IP = "37.214.42.10" # Пул Белтелекома (Минск)

REFERER_URL = "https://tv.zala.by/"
ORIGIN_URL = "https://tv.zala.by"

# ============================================================
# База кандидатов User-Agent для проверки Zala
# ============================================================

ALL_ZALA_USER_AGENTS = [
    ("Smartlabs Minimal", "smartlabs"),
    ("Smartlabs Lowercase", "smartlabs/1.0"),
    (
        "SmartLabs Official",
        "SmartLabs/1.0 (SmartLabs OTT Engine)"
    ),
    ("SmartLabs SML-482", "SmartLabs SML-482/1.0"),
    (
        "SmartLabs SML-5050",
        "SmartLabs SML-5050/2.0 (Android)"
    ),
    ("Zala App Short", "Zala/1.0"),
    ("Zala Android App", "ZalaAndroid/3.5.2"),
    (
        "Zala SmartTV Tizen",
        "Mozilla/5.0 (SMART-TV; Linux; Tizen 5.5) "
        "AppleWebkit/605.1.15 ZalaSmartTV/2.0"
    ),
    (
        "Zala WebOS",
        "Mozilla/5.0 (Web0S; Linux/SmartTV) "
        "AppleWebKit/537.36 Zala/1.0.4"
    ),
    (
        "Beltelecom OTT Engine",
        "BeltelecomOTT/1.0 (Linux; Android 9)"
    ),
    ("ZTE STB", "ZTE_STB_B700V2A/1.0"),
    ("Huawei STB", "Huawei_STB_EC2108/1.0"),
    (
        "Dalvik SML-5050",
        "Dalvik/2.1.0 (Linux; U; Android 9; "
        "SML-5050 Build/PPR1.180610.011)"
    ),
    ("Dalvik Pure", "Dalvik/2.1.0"),
    (
        "ExoPlayer Zala",
        "ExoPlayerLib/2.18.1 (Linux; Android 10) ZalaOTT"
    ),
    ("VLC LibVLC", "VLC/3.0.18 LibVLC/3.0.18"),
    ("TiviMate", "TiviMate/4.7.0")
]

STANDARD_DIAGNOSTIC_USER_AGENTS = []
ENABLE_STANDARD_DIAGNOSTIC_PROFILES = False
RESTRICT_REDIRECT_HOST = True

# ============================================================
# Класс одновременного логирования в консоль и TXT
# ============================================================

class DualLogger:
    """Дублирует сообщения в терминал и текстовый отчет."""

    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")
        self.closed = False

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        if not self.closed:
            self.flush()
            self.log.close()
            self.closed = True


# ============================================================
# Безопасная обработка HTTP-перенаправлений
# ============================================================

class RestrictedRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self, original_hostname):
        super().__init__()
        self.original_hostname = original_hostname.lower()

    def redirect_request(
        self, req, fp, code, msg, headers, newurl
    ):
        parsed = urllib.parse.urlparse(newurl)

        if parsed.scheme.lower() != "https":
            raise urllib.error.URLError("Redirect rejected: HTTPS required")

        if (
            RESTRICT_REDIRECT_HOST
            and parsed.hostname
            and parsed.hostname.lower() != self.original_hostname
        ):
            raise urllib.error.URLError("Redirect rejected: hostname changed")

        if len(getattr(req, "_redirect_count", [])) > MAX_REDIRECTS:
            raise urllib.error.URLError("Redirect rejected: limit exceeded")

        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None:
            redirected._redirect_count = getattr(req, "_redirect_count", []) + [newurl]
        return redirected


# ============================================================
# Вспомогательные функции валидации и анализа
# ============================================================

def safe_url_for_report(url):
    try:
        parsed = urllib.parse.urlsplit(url)
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    except Exception:
        return "[URL unavailable]"

def safe_exception_message(error):
    message = str(error)
    message = re.sub(r"https?://[^\s]+", "[URL redacted]", message)
    return message.replace("\r", " ").replace("\n", " ")[:500]

def get_response_hostname(url):
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() != "https":
        raise ValueError("TEST_URL должен использовать HTTPS")
    if not parsed.hostname:
        raise ValueError("В TEST_URL отсутствует hostname")
    return parsed.hostname.lower()

def build_opener(url):
    hostname = get_response_hostname(url)
    return urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ssl.create_default_context()),
        RestrictedRedirectHandler(hostname)
    )

def validate_test_url(url):
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() != "https":
        raise ValueError("Разрешены только HTTPS-адреса")
    if not parsed.hostname:
        raise ValueError("Не указан hostname")
    return parsed

def decode_body(body):
    if body.startswith(b"\xef\xbb\xbf"):
        body = body[3:]
    return body.decode("utf-8", errors="replace")

def classify_content_type(content_type):
    value = (content_type or "").lower().strip()
    if "mpegurl" in value or "vnd.apple.mpegurl" in value or "x-mpegurl" in value:
        return "M3U8_CONTENT_TYPE"
    if "text/html" in value:
        return "HTML_CONTENT_TYPE"
    if "application/json" in value:
        return "JSON_CONTENT_TYPE"
    return "OTHER_CONTENT_TYPE"

def inspect_hls_playlist(body):
    result = {
        "is_hls": False, "is_master_playlist": False,
        "is_media_playlist": False, "has_extinf": False,
        "diagnostic": "UNKNOWN"
    }
    if not body:
        result["diagnostic"] = "EMPTY_BODY"
        return result

    text_body = decode_body(body)
    lines = [line.strip() for line in text_body.splitlines() if line.strip()]
    if not lines:
        result["diagnostic"] = "EMPTY_TEXT"
        return result

    if lines[0] != "#EXTM3U":
        result["diagnostic"] = "MISSING_EXTM3U"
        return result

    result["is_hls"] = True
    result["has_extinf"] = any(line.startswith("#EXTINF:") for line in lines)
    result["is_master_playlist"] = any(line.startswith("#EXT-X-STREAM-INF:") for line in lines)
    result["is_media_playlist"] = result["has_extinf"] or any(line.startswith("#EXT-X-TARGETDURATION:") for line in lines)

    if result["is_master_playlist"]:
        result["diagnostic"] = "HLS_MASTER_PLAYLIST"
    elif result["is_media_playlist"]:
        result["diagnostic"] = "HLS_MEDIA_PLAYLIST"
    else:
        result["diagnostic"] = "HLS_HEADER_ONLY"
    return result

def classify_http_response(status, content_type, body):
    if status == 403:
        return "HTTP_FORBIDDEN_GEO_OR_GATEWAY"
    if status != 200:
        return f"HTTP_UNEXPECTED_{status}"
    
    hls_info = inspect_hls_playlist(body)
    if hls_info["is_hls"]:
        return "HTTP_OK_PLAYLIST"
    return "HTTP_OK_NON_PLAYLIST"

def safe_body_preview(body, limit=BODY_PREVIEW_LENGTH):
    if not body:
        return ""
    preview = decode_body(body).replace("\r", " ").replace("\n", " ")
    preview = re.sub(r"(?i)(token|key|sig)=([^&\s]+)", r"\1=[REDACTED]", preview)
    return preview[:limit]


# ============================================================
# Выполнение запроса с гео-эмуляцией
# ============================================================

def perform_request(profile_name, user_agent):
    result = {
        "profile": profile_name, "user_agent": user_agent,
        "status_code": None, "category": "UNKNOWN",
        "latency_ms": None, "bytes_read": 0, "preview": "",
        "error_type": "", "error_message": ""
    }

    # Возвращаем защитный шлюз X-Forwarded-For для корректного пробития регионального слоя
    headers = {
        "User-Agent": user_agent,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Referer": REFERER_URL,
        "Origin": ORIGIN_URL,
        "X-Forwarded-For": BY_GEO_SIMULATION_IP,
        "Connection": "close"
    }

    opener = build_opener(TEST_URL)

    for attempt in range(1, RETRY_COUNT + 2):
        start_time = time.monotonic()
        request = urllib.request.Request(TEST_URL, headers=headers, method="GET")

        try:
            with opener.open(request, timeout=REQUEST_TIMEOUT) as response:
                result["latency_ms"] = round((time.monotonic() - start_time) * 1000, 2)
                result["status_code"] = response.status
                content_type = response.headers.get("Content-Type", "")
                
                body = response.read(MAX_RESPONSE_BYTES)
                result["bytes_read"] = len(body)
                result["body_sha256"] = hashlib.sha256(body).hexdigest()
                result["preview"] = safe_body_preview(body)
                result["category"] = classify_http_response(response.status, content_type, body)
                return result

        except urllib.error.HTTPError as error:
            result["latency_ms"] = round((time.monotonic() - start_time) * 1000, 2)
            result["status_code"] = error.code
            result["error_type"] = "HTTPError"
            result["error_message"] = safe_exception_message(error.reason)
            
            try:
                body = error.read(1024)
                result["preview"] = safe_body_preview(body)
            except Exception:
                body = b0

            result["category"] = classify_http_response(error.code, "", body)
            return result

        except Exception as error:
            result["category"] = "NETWORK_ERROR"
            result["error_type"] = type(error).__name__
            result["error_message"] = safe_exception_message(error)

        if attempt <= RETRY_COUNT:
            time.sleep(RETRY_DELAY)

    return result


def print_profile_result(result, index, total):
    category = result["category"]
    marker = "[SUCCESS]" if category == "HTTP_OK_PLAYLIST" else "[FAIL]"
    
    print(f"{marker} [{index:02d}/{total:02d}] Профиль: {result['profile']}")
    print(f"    Категория       : {category}")
    print(f"    HTTP status     : {result['status_code']}")
    print(f"    Задержка        : {result['latency_ms']} ms")
    print(f"    UA              : {result['user_agent']}")
    if result.get("preview"):
        print(f"    Preview         : {result['preview']}")
    if result.get("error_message"):
        print(f"    Ошибка          : {result['error_message']}")
    print()


# ============================================================
# Главный пайплайн
# ============================================================

def run_zala_diagnosis_pipeline():
    original_stdout = sys.stdout
    logger = DualLogger(REPORT_FILENAME)
    sys.stdout = logger

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = []
    success_found = False
    winning_profile = None
    winning_ua = None

    try:
        print("=" * 72)
        print(f" PIPELINE: {PIPELINE_NAME}")
        print(f" BUILD ID: {BUILD_ID} | TIMESTAMP: {timestamp}")
        print(f" GEO-BYPASS SIMULATION IP: {BY_GEO_SIMULATION_IP}")
        print("=" * 72 + "\n")

        profiles = list(ALL_ZALA_USER_AGENTS)
        total_profiles = len(profiles)

        for index, (name, ua) in enumerate(profiles, start=1):
            result = perform_request(name, ua)
            results.append(result)
            print_profile_result(result, index, total_profiles)

            if result["category"] == "HTTP_OK_PLAYLIST" and not success_found:
                success_found = True
                winning_profile = name
                winning_ua = ua

            time.sleep(REQUEST_DELAY)

        print("=" * 72)
        if success_found:
            print(f" [✓] СТАТУС: ПОБЕДА! Найден рабочий UA: {winning_ua} ({winning_profile})")
        else:
            print(" [!] СТАТУС: Сетевой барьер. Все профили отклонены (возможна жесткая ASN-фильтрация хостинга).")
        print("=" * 72)

        return {
            "build_id": BUILD_ID,
            "success": success_found,
            "winning_profile": winning_profile,
            "winning_ua": winning_ua,
            "report_file": REPORT_FILENAME
        }
    finally:
        sys.stdout = original_stdout
        logger.close()

if __name__ == "__main__":
    run_zala_diagnosis_pipeline()
