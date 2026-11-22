
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix URL Dissector 2.3.1.0-beta
HTTP / proxy / HLS / LiveM3U / M3U source diagnostics.

Python 3.9+, standard library only.

Многопоточные пулы:
  50 — обнаружение источников;
  50 — HTTP-загрузка;
  50 — разбор плейлистов;
  150 — независимые URL и вложенные ресурсы.

Сохраняет основные возможности версии 2.2.0.
Не обходит DRM, авторизацию или ограничения доступа.
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import datetime as dt
import ipaddress
import json
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from pathlib import Path


VERSION = "2.3.1.0-beta"

LIVE_M3U_BASE = "https://livem3u.tatnet.app"
LIVE_M3U_INDEX = LIVE_M3U_BASE + "/data/playlists/INDEX.m3u"
LIVE_M3U_MAIN = LIVE_M3U_BASE + "/data/playlist.m3u"

DISCOVERY_WORKERS = 50
DOWNLOAD_WORKERS = 50
PARSER_WORKERS = 50
URL_WORKERS = 150

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/131.0 Safari/537.36"
)

REDIRECTS = {301, 302, 303, 307, 308}

HLS_TYPES = (
    "mpegurl",
    "vnd.apple.mpegurl",
    "x-mpegurl",
    "application/m3u8",
)

SECRET_KEYS = {
    "token", "key", "auth", "authorization",
    "password", "passwd", "secret", "signature",
    "sig", "jwt", "session", "access_token",
    "api_key", "apikey",
}

CLAIM_HEADERS = {
    "x-final-url", "x-request-url",
    "x-cors-redirect-1", "x-original-url",
    "x-rewrite-url", "x-upstream-url",
}


# ============================================================
# Общие утилиты
# ============================================================

def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(
        timespec="seconds"
    )


def redact(url):
    """Маскирует типовые секреты в URL отчёта."""
    try:
        p = urllib.parse.urlsplit(url)
        net = p.hostname or ""

        if p.port:
            net += ":" + str(p.port)

        if p.username:
            net = "[redacted]@" + net

        query = [
            (
                k,
                "[REDACTED]" if k.lower() in SECRET_KEYS else v,
            )
            for k, v in urllib.parse.parse_qsl(
                p.query, keep_blank_values=True
            )
        ]

        return urllib.parse.urlunsplit((
            p.scheme, net, p.path,
            urllib.parse.urlencode(query), "",
        ))
    except Exception:
        return url


def normalize(raw):
    raw = raw.strip().strip("\"'")

    if not raw or raw.startswith("#"):
        return ""

    if raw.startswith("//"):
        raw = "https:" + raw

    if not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", raw):
        raw = "https://" + raw

    p = urllib.parse.urlsplit(raw)

    if (
        p.scheme.lower() not in ("http", "https")
        or not p.hostname
    ):
        raise ValueError("Требуется HTTP(S) URL")

    return urllib.parse.urlunsplit((
        p.scheme.lower(), p.netloc,
        p.path or "/", p.query, "",
    ))


def public_ip(value):
    try:
        return ipaddress.ip_address(value).is_global
    except ValueError:
        return False


def host_port(url):
    p = urllib.parse.urlsplit(url)
    return p.hostname or "", p.port or (
        443 if p.scheme.lower() == "https" else 80
    )


def safe_headers(headers):
    wanted = {
        "content-type", "content-length", "content-range",
        "accept-ranges", "location", "server", "via",
        "www-authenticate", "retry-after",
        "access-control-allow-origin",
        "x-final-url", "x-request-url", "x-cors-redirect-1",
        "x-cache", "x-proxy-cache", "x-tatnet-cache",
        "x-github-edge-region", "date", "etag",
        "last-modified", "cache-control",
    }
    return {
        k: str(v)[:1200]
        for k, v in headers.items()
        if k.lower() in wanted
    }


# ============================================================
# DNS, TCP, TLS
# ============================================================

def dns_lookup(host, port, allow_private=False):
    started = time.monotonic()

    try:
        addresses = sorted({
            item[4][0]
            for item in socket.getaddrinfo(
                host, port, type=socket.SOCK_STREAM
            )
        })

        if not addresses:
            return {
                "host": host, "port": port,
                "addresses": [],
                "error": "DNS не вернул адресов",
            }

        blocked = [ip for ip in addresses if not public_ip(ip)]

        if blocked and not allow_private:
            return {
                "host": host, "port": port,
                "addresses": addresses,
                "error": (
                    "DNS вернул непубличные адреса; "
                    "соединение заблокировано"
                ),
            }

        return {
            "host": host,
            "port": port,
            "addresses": addresses,
            "elapsed_ms": round(
                (time.monotonic() - started) * 1000, 2
            ),
        }

    except Exception as exc:
        return {
            "host": host,
            "port": port,
            "error": f"{type(exc).__name__}: {exc}",
        }


def tcp_diagnostics(host, port, addresses, timeout=5):
    started = time.monotonic()
    attempts = []

    for address in addresses:
        try:
            with socket.create_connection(
                (address, port), timeout=timeout
            ):
                attempts.append({
                    "address": address,
                    "connected": True,
                })
                return {
                    "host": host, "port": port,
                    "connected": True,
                    "attempts": attempts,
                    "elapsed_ms": round(
                        (time.monotonic() - started) * 1000, 2
                    ),
                }
        except Exception as exc:
            attempts.append({
                "address": address,
                "connected": False,
                "error": f"{type(exc).__name__}: {exc}",
            })

    return {
        "host": host, "port": port,
        "connected": False,
        "attempts": attempts,
        "elapsed_ms": round(
            (time.monotonic() - started) * 1000, 2
        ),
    }


def tls_diagnostics(host, port, addresses, timeout=8):
    result = {"host": host, "port": port}
    started = time.monotonic()

    if not addresses:
        result.update({
            "verified": False,
            "error": "Нет разрешённых IP для TLS-проверки",
        })
        return result

    context = ssl.create_default_context()

    for address in addresses:
        try:
            with socket.create_connection(
                (address, port), timeout=timeout
            ) as raw:
                with context.wrap_socket(
                    raw, server_hostname=host
                ) as tls:
                    cert = tls.getpeercert()
                    cipher = tls.cipher()

                    result.update({
                        "verified": True,
                        "connected_address": address,
                        "tls_version": tls.version(),
                        "cipher": cipher[0] if cipher else None,
                        "certificate_subject": cert.get(
                            "subject", []
                        ),
                        "certificate_issuer": cert.get(
                            "issuer", []
                        ),
                        "certificate_not_before": cert.get(
                            "notBefore"
                        ),
                        "certificate_not_after": cert.get(
                            "notAfter"
                        ),
                        "certificate_san": [
                            value
                            for kind, value
                            in cert.get("subjectAltName", [])
                            if kind == "DNS"
                        ][:40],
                    })
                    break
        except Exception as exc:
            result["verified"] = False
            result["error"] = f"{type(exc).__name__}: {exc}"

    result["elapsed_ms"] = round(
        (time.monotonic() - started) * 1000, 2
    )
    return result


# ============================================================
# HTTP-клиент с общими потокобезопасными бюджетами
# ============================================================

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Client:
    def __init__(self, args):
        self.args = args
        self.dns = {}
        self.started = time.monotonic()

        self.request_count = 0
        self.bytes_read = 0
        self.bytes_reserved = 0

        self._budget_lock = threading.RLock()
        self._dns_lock = threading.RLock()
        self._local = threading.local()

    def _get_opener(self):
        """Отдельный urllib opener для каждого рабочего потока."""
        opener = getattr(self._local, "opener", None)

        if opener is None:
            context = (
                ssl._create_unverified_context()
                if self.args.insecure_tls
                else ssl.create_default_context()
            )

            opener = urllib.request.build_opener(
                NoRedirect(),
                urllib.request.HTTPSHandler(context=context),
            )
            self._local.opener = opener

        return opener

    def resolve(self, host, port):
        key = (host, port)

        with self._dns_lock:
            if key not in self.dns:
                self.dns[key] = dns_lookup(
                    host, port, self.args.allow_private
                )
            return self.dns[key]

    def budget_error(self):
        with self._budget_lock:
            if self.request_count >= self.args.max_requests:
                return "Достигнут общий лимит HTTP-запросов"

            if (
                self.bytes_read + self.bytes_reserved
                >= self.args.max_total_bytes
            ):
                return "Достигнут общий лимит загруженных байтов"

            if (
                time.monotonic() - self.started
                >= self.args.max_runtime
            ):
                return "Достигнут общий лимит времени"

        return None

    def _reserve_request(self):
        """Атомарно резервирует один HTTP-запрос."""
        with self._budget_lock:
            if self.request_count >= self.args.max_requests:
                return "Достигнут общий лимит HTTP-запросов"

            if (
                self.bytes_read + self.bytes_reserved
                >= self.args.max_total_bytes
            ):
                return "Достигнут общий лимит загруженных байтов"

            if (
                time.monotonic() - self.started
                >= self.args.max_runtime
            ):
                return "Достигнут общий лимит времени"

            self.request_count += 1
            return None

    def _reserve_bytes(self, wanted):
        with self._budget_lock:
            remaining = max(
                0,
                self.args.max_total_bytes
                - self.bytes_read
                - self.bytes_reserved,
            )
            allowed = min(max(0, wanted), remaining)
            self.bytes_reserved += allowed
            return allowed

    def _commit_bytes(self, reserved, actual):
        with self._budget_lock:
            self.bytes_reserved = max(
                0, self.bytes_reserved - reserved
            )
            self.bytes_read += max(0, actual)

    def _read_limited(self, response, limit):
        """
        Читает не более limit байтов и учитывает общий
        бюджет даже при одновременных загрузках.
        """
        allowed = self._reserve_bytes(limit)

        if allowed <= 0:
            return b"", False

        body = b""
        truncated = False

        try:
            raw = response.read(allowed + 1)
            truncated = len(raw) > allowed
            body = raw[:allowed]
            return body, truncated
        finally:
            self._commit_bytes(allowed, len(body))

    def request(self, url, method="GET", headers=None, limit=None):
        started = time.monotonic()
        current = url
        chain = []

        limit = min(
            self.args.max_bytes,
            max(
                0,
                limit if limit is not None
                else self.args.max_bytes,
            ),
        )

        request_headers = {
            "User-Agent": self.args.user_agent,
            "Accept": "*/*",
            "Connection": "close",
        }
        request_headers.update(headers or {})

        for hop in range(self.args.max_redirects + 1):
            budget = self._reserve_request()

            if budget:
                return self._result(
                    url, current, method, None, {}, b"",
                    budget, chain, started, None,
                )

            try:
                current = normalize(current)
            except Exception as exc:
                return self._result(
                    url, current, method, None, {}, b"",
                    f"Некорректный URL: {exc}",
                    chain, started, None,
                )

            host, port = host_port(current)
            dns = self.resolve(host, port)

            if dns.get("error"):
                return self._result(
                    url, current, method, None, {}, b"",
                    dns["error"], chain, started, dns,
                )

            request = urllib.request.Request(
                current,
                headers=request_headers,
                method=method,
            )

            try:
                response = self._get_opener().open(
                    request, timeout=self.args.timeout
                )
                status = getattr(response, "status", 200)
                response_headers = dict(response.headers.items())

                if method == "HEAD" or limit == 0:
                    body = b""
                    truncated = False
                else:
                    body, truncated = self._read_limited(
                        response, limit
                    )

                response.close()

                return self._result(
                    url, current, method, status,
                    response_headers, body, None,
                    chain, started, dns, truncated,
                )

            except urllib.error.HTTPError as exc:
                status = exc.code
                response_headers = (
                    dict(exc.headers.items()) if exc.headers else {}
                )
                location = response_headers.get("Location")

                if (
                    status in REDIRECTS
                    and location
                    and hop < self.args.max_redirects
                ):
                    next_url = urllib.parse.urljoin(
                        current, location
                    )

                    try:
                        next_url = normalize(next_url)
                    except Exception as redirect_error:
                        exc.close()
                        return self._result(
                            url, current, method, status,
                            response_headers, b"",
                            f"Некорректный URL редиректа: "
                            f"{redirect_error}",
                            chain, started, dns,
                        )

                    chain.append({
                        "status": status,
                        "from": redact(current),
                        "location": redact(next_url),
                    })

                    old = urllib.parse.urlsplit(current)
                    new = urllib.parse.urlsplit(next_url)

                    if (
                        old.scheme != new.scheme
                        or old.netloc.lower() != new.netloc.lower()
                    ):
                        for header in (
                            "Authorization", "Cookie", "Origin",
                            "Referer", "X-Requested-With",
                        ):
                            request_headers.pop(header, None)

                    current = next_url

                    if status == 303:
                        method = "GET"

                    exc.close()
                    continue

                body = b""
                truncated = False

                try:
                    if method != "HEAD" and limit > 0:
                        body, truncated = self._read_limited(
                            exc, min(limit, 65536)
                        )
                except Exception:
                    body = b""
                finally:
                    exc.close()

                return self._result(
                    url, current, method, status,
                    response_headers, body,
                    f"HTTP {status}: {exc.reason}",
                    chain, started, dns, truncated,
                )

            except Exception as exc:
                return self._result(
                    url, current, method, None, {}, b"",
                    f"{type(exc).__name__}: {exc}",
                    chain, started, dns,
                )

        return self._result(
            url, current, method, None, {}, b"",
            "Превышен лимит редиректов",
            chain, started, None,
        )

    @staticmethod
    def _result(
        url, current, method, status, headers, body,
        error, chain, started, dns, truncated=False,
    ):
        return {
            "url": redact(url),
            "final_url": redact(current),
            "method": method,
            "status": status,
            "headers": headers,
            "body": body,
            "error": error,
            "redirects": chain,
            "dns": dns,
            "truncated": truncated,
            "elapsed_ms": round(
                (time.monotonic() - started) * 1000, 2
            ),
        }


# ============================================================
# Origin / Referer
# ============================================================

def origins_for(url, supplied=None):
    result = []

    def add(value):
        try:
            p = urllib.parse.urlsplit(value)
            if p.scheme in ("http", "https") and p.hostname:
                origin = f"{p.scheme}://{p.netloc}"
                if origin not in result:
                    result.append(origin)
        except Exception:
            pass

    if supplied:
        add(supplied)

    p = urllib.parse.urlsplit(url)
    current_host = (p.hostname or "").lower()

    for part in urllib.parse.unquote(p.path).split("/")[:8]:
        part = part.strip()

        if not re.fullmatch(
            r"[A-Za-z0-9.-]+(?::\d+)?", part
        ):
            continue

        host = part.split(":", 1)[0].lower()

        if host == current_host or "." not in host:
            continue

        if re.search(
            r"\.(com|net|org|ru|by|io|app|dev|tv|uz|"
            r"site|online|live|me|info|xyz)$",
            host,
        ):
            add("https://" + part)

    return result[:5]


# ============================================================
# HLS-парсер
# ============================================================

def attrs(raw):
    result = {}
    pattern = r'([^,=]+)=((?:"[^"]*")|[^,]*)'

    for match in re.finditer(pattern, raw):
        result[match.group(1).strip()] = (
            match.group(2).strip().strip('"')
        )

    return result


def looks_hls(body, content_type, url):
    sample = body.lstrip(b"\xef\xbb\xbf\x00\t\r\n ")
    path = urllib.parse.urlsplit(url).path.lower()

    return (
        sample.startswith(b"#EXTM3U")
        or any(t in content_type.lower() for t in HLS_TYPES)
        or path.endswith((".m3u8", ".m3u"))
    )


def parse_hls(text, base_url):
    lines = [
        line.strip()
        for line in text.replace("\r", "").split("\n")
        if line.strip()
    ]

    tags = collections.Counter(
        line.split(":", 1)[0]
        for line in lines if line.startswith("#")
    )

    result = {
        "valid_header": bool(
            lines and lines[0].lstrip("\ufeff") == "#EXTM3U"
        ),
        "line_count": len(lines),
        "tag_counts": dict(tags),
        "is_master": False,
        "is_media": False,
        "variants": [],
        "renditions": [],
        "segments": [],
        "keys": [],
        "maps": [],
        "warnings": [],
        "duplicate_uris": [],
        "duplicate_variant_uris": [],
        "target_duration": None,
        "media_sequence": None,
        "endlist": "#EXT-X-ENDLIST" in lines,
        "discontinuities": 0,
        "program_date_times": [],
        "byte_ranges": [],
        "declared_duration_seconds": 0.0,
        "encryption_methods": [],
        "playlist_type": None,
    }

    pending = None
    seen = collections.Counter()

    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            result["is_master"] = True
            pending = attrs(line.split(":", 1)[1])
            continue

        if line.startswith("#EXT-X-MEDIA:"):
            item = attrs(line.split(":", 1)[1])

            if item.get("URI"):
                item["resolved_uri"] = urllib.parse.urljoin(
                    base_url, item["URI"]
                )

            result["renditions"].append(item)
            continue

        recognized = False

        for tag, field in (
            ("#EXT-X-TARGETDURATION:", "target_duration"),
            ("#EXT-X-MEDIA-SEQUENCE:", "media_sequence"),
            ("#EXT-X-PLAYLIST-TYPE:", "playlist_type"),
        ):
            if line.startswith(tag):
                result[field] = line.split(":", 1)[1]
                recognized = True
                break

        if recognized:
            continue

        if line == "#EXT-X-DISCONTINUITY":
            result["discontinuities"] += 1
            continue

        if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            result["program_date_times"].append(
                line.split(":", 1)[1]
            )
            continue

        if line.startswith("#EXT-X-BYTERANGE:"):
            result["byte_ranges"].append(
                line.split(":", 1)[1]
            )
            continue

        if line.startswith((
            "#EXT-X-KEY:", "#EXT-X-SESSION-KEY:"
        )):
            item = attrs(line.split(":", 1)[1])
            method = item.get("METHOD")

            if (
                method
                and method not in result["encryption_methods"]
            ):
                result["encryption_methods"].append(method)

            if item.get("URI"):
                item["resolved_uri"] = urllib.parse.urljoin(
                    base_url, item["URI"]
                )

            result["keys"].append(item)
            continue

        if line.startswith("#EXT-X-MAP:"):
            item = attrs(line.split(":", 1)[1])

            if item.get("URI"):
                item["resolved_uri"] = urllib.parse.urljoin(
                    base_url, item["URI"]
                )

            result["maps"].append(item)
            continue

        if line.startswith("#EXTINF:"):
            result["is_media"] = True

            try:
                value = float(
                    line.split(":", 1)[1].split(",", 1)[0]
                )
                result["declared_duration_seconds"] += value
            except (ValueError, IndexError):
                result["warnings"].append(
                    "Некорректная длительность EXTINF"
                )

            continue

        if line.startswith("#"):
            continue

        uri = urllib.parse.urljoin(base_url, line)
        seen[uri] += 1

        if pending is not None:
            result["variants"].append({
                "uri": uri,
                "bandwidth": pending.get("BANDWIDTH"),
                "average_bandwidth": pending.get(
                    "AVERAGE-BANDWIDTH"
                ),
                "resolution": pending.get("RESOLUTION"),
                "codecs": pending.get("CODECS"),
                "frame_rate": pending.get("FRAME-RATE"),
                "attributes": pending,
            })
            pending = None
        else:
            result["segments"].append(uri)

    result["duplicate_uris"] = [
        {"uri": uri, "count": count}
        for uri, count in seen.items() if count > 1
    ]

    if not result["valid_header"]:
        result["warnings"].append(
            "Нет #EXTM3U в начале плейлиста"
        )

    if pending is not None:
        result["warnings"].append(
            "#EXT-X-STREAM-INF без URI"
        )

    if result["is_master"] and result["segments"]:
        result["warnings"].append(
            "Смешаны master-варианты и обычные URI"
        )

    if not result["is_master"] and not result["is_media"]:
        result["warnings"].append(
            "Не обнаружены теги master/media HLS"
        )

    if result["is_media"] and not result["segments"]:
        result["warnings"].append(
            "Есть EXTINF, но нет URI сегментов"
        )

    by_uri = collections.defaultdict(list)

    for variant in result["variants"]:
        by_uri[variant["uri"]].append(variant)

    result["duplicate_variant_uris"] = [
        {
            "uri": uri,
            "count": len(items),
            "declared_resolutions": [
                item.get("resolution") for item in items
            ],
            "declared_bandwidths": [
                item.get("bandwidth") for item in items
            ],
        }
        for uri, items in by_uri.items() if len(items) > 1
    ]

    return result


# ============================================================
# M3U: обнаружение и классификация плейлистов
# ============================================================

def is_playlist_url(url):
    try:
        path = urllib.parse.urlsplit(url).path.lower()
        return path.endswith((".m3u", ".m3u8", ".m3u.txt"))
    except Exception:
        return False


def extract_playlist_urls(body, base_url):
    """
    Извлекает URL плейлистов, не принимая обычные TS-потоки
    за отдельные M3U-источники.
    """
    text = body.decode("utf-8-sig", errors="replace")
    found = []
    seen = set()

    for raw in text.splitlines():
        line = raw.strip().strip("\"'")

        if not line or line.startswith("#"):
            continue

        candidates = re.findall(
            r'https?://[^\s<>"\']+|'
            r'//[^\s<>"\']+|'
            r'(?<!\S)[^\s<>"\']+\.(?:m3u8?|m3u\.txt)'
            r'(?:\?[^\s<>"\']*)?',
            line,
            flags=re.IGNORECASE,
        )

        if not candidates and line.lower().endswith(
            (".m3u", ".m3u8", ".m3u.txt")
        ):
            candidates = [line]

        for candidate in candidates:
            candidate = candidate.rstrip("),;]")

            try:
                absolute = normalize(
                    urllib.parse.urljoin(base_url, candidate)
                )
            except ValueError:
                continue

            if is_playlist_url(absolute) and absolute not in seen:
                seen.add(absolute)
                found.append(absolute)

    return found


def discover_livem3u(dissector):
    """
    Параллельно загружает INDEX.m3u и основной playlist.m3u.
    Ошибка одного источника не блокирует обработку другого.
    """
    result = {
        "source": LIVE_M3U_BASE,
        "index_url": LIVE_M3U_INDEX,
        "main_url": LIVE_M3U_MAIN,
        "index_status": None,
        "main_status": None,
        "discovered": [],
        "errors": [],
    }

    candidates = [LIVE_M3U_INDEX, LIVE_M3U_MAIN]

    def fetch(url):
        response = dissector.request(
            url, "GET", limit=dissector.args.max_bytes
        )
        return url, response

    futures = [
        dissector.discovery_pool.submit(fetch, url)
        for url in candidates
    ]

    responses = {}

    for future in concurrent.futures.as_completed(futures):
        try:
            url, response = future.result()
            responses[url] = response
        except Exception as exc:
            result["errors"].append({
                "url": "LiveM3U discovery",
                "error": f"{type(exc).__name__}: {exc}",
            })

    # Обрабатываем результаты в стабильном порядке.
    for url in candidates:
        response = responses.get(url)

        if response is None:
            continue

        field = (
            "index_status"
            if url == LIVE_M3U_INDEX
            else "main_status"
        )

        result[field] = {
            "status": response["status"],
            "error": response["error"],
            "bytes": len(response["body"]),
            "final_url": response["final_url"],
            "truncated": response["truncated"],
        }

        if response["error"]:
            result["errors"].append({
                "url": redact(url),
                "error": response["error"],
            })

        if response["body"]:
            result["discovered"].extend(
                extract_playlist_urls(
                    response["body"],
                    response["final_url"] or url,
                )
            )

    unique = list(dict.fromkeys(result["discovered"]))
    result["discovered"] = unique[:dissector.args.max_sources]
    return result


def download_m3u_source(dissector, url):
    """
    Загружает отдельный плейлист-источник и возвращает
    диагностическую информацию о его содержимом.
    """
    response = dissector.request(
        url, "GET", limit=dissector.args.max_bytes
    )

    body = response["body"]
    text = body.decode("utf-8-sig", errors="replace")

    playlist_lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]

    channel_entries = sum(
        1 for line in playlist_lines
        if line.startswith("#EXTINF:")
    )

    nested = extract_playlist_urls(
        body, response["final_url"] or url
    )

    return {
        "url": redact(url),
        "final_url": response["final_url"],
        "status": response["status"],
        "error": response["error"],
        "bytes": len(body),
        "truncated": response["truncated"],
        "looks_like_playlist": (
            bool(body.lstrip(b"\xef\xbb\xbf").startswith(b"#EXTM3U"))
            or is_playlist_url(url)
        ),
        "line_count": len(playlist_lines),
        "channel_entries": channel_entries,
        "nested_playlists": nested,
        "content_type": next(
            (
                value for key, value in response["headers"].items()
                if key.lower() == "content-type"
            ),
            "",
        ),
    }


# ============================================================
# Основной анализатор
# ============================================================

class Dissector:
    def __init__(self, args):
        self.args = args
        self.client = Client(args)

        self.visited = set()
        self._visited_lock = threading.RLock()
        self._shutdown = False

        self.discovery_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=DISCOVERY_WORKERS,
            thread_name_prefix="phoenix-discovery",
        )
        self.download_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=DOWNLOAD_WORKERS,
            thread_name_prefix="phoenix-download",
        )
        self.parser_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=PARSER_WORKERS,
            thread_name_prefix="phoenix-parser",
        )
        self.url_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=URL_WORKERS,
            thread_name_prefix="phoenix-url",
        )

    def shutdown(self):
        if self._shutdown:
            return

        self._shutdown = True

        # Сначала прекращаем подачу новых заданий.
        self.discovery_pool.shutdown(
            wait=True, cancel_futures=True
        )
        self.url_pool.shutdown(
            wait=True, cancel_futures=True
        )
        self.parser_pool.shutdown(
            wait=True, cancel_futures=True
        )
        self.download_pool.shutdown(
            wait=True, cancel_futures=True
        )

    def request(self, url, method="GET", headers=None, limit=None):
        """
        Все HTTP-операции проходят через отдельный пул загрузки.
        Вызовы из URL/discovery-пулов не блокируют сам пул загрузки.
        """
        future = self.download_pool.submit(
            self.client.request, url, method, headers, limit
        )
        return future.result()

    def parse_playlist(self, text, base_url):
        future = self.parser_pool.submit(
            parse_hls, text, base_url
        )
        return future.result()

    def check_network(self, url):
        host, port = host_port(url)
        dns = self.client.resolve(host, port)
        network = {"dns": dns}

        if dns.get("error"):
            network["tcp"] = {
                "connected": False,
                "error": "Пропущено из-за ошибки DNS/политики IP",
            }

            if urllib.parse.urlsplit(url).scheme == "https":
                network["tls"] = {
                    "verified": False,
                    "error": "Пропущено из-за ошибки DNS/политики IP",
                }

            return network

        addresses = dns.get("addresses", [])

        network["tcp"] = tcp_diagnostics(
            host, port, addresses, self.args.timeout
        )

        if urllib.parse.urlsplit(url).scheme == "https":
            network["tls"] = tls_diagnostics(
                host, port, addresses, self.args.timeout
            )

        return network

    def _claim_url(self, url):
        with self._visited_lock:
            if url in self.visited:
                return "SKIPPED_DUPLICATE"

            if len(self.visited) >= self.args.max_sources:
                return "SOURCE_LIMIT"

            self.visited.add(url)
            return None

    def check(self, url, depth=0, parent=None, kind="source"):
        if depth > self.args.max_depth:
            return {
                "url": redact(url),
                "resource_type": kind,
                "status_label": "DEPTH_LIMIT",
            }

        claim_error = self._claim_url(url)

        if claim_error:
            return {
                "url": redact(url),
                "resource_type": kind,
                "status_label": claim_error,
            }

        try:
            parsed = urllib.parse.urlsplit(normalize(url))
        except Exception as exc:
            return {
                "url": redact(url),
                "resource_type": kind,
                "status_label": "BROKEN",
                "errors": [f"Некорректный URL: {exc}"],
            }

        record = {
            "url": redact(url),
            "parent": redact(parent) if parent else None,
            "resource_type": kind,
            "depth": depth,
            "checked_at": now(),
            "host": parsed.hostname,
            "path": parsed.path,
            "attempts": [],
            "children": [],
            "errors": [],
            "status_label": "UNKNOWN",
        }

        record["network"] = self.check_network(url)

        head = self.request(url, "HEAD", limit=0)

        record["head"] = {
            k: v for k, v in head.items()
            if k not in ("body", "headers")
        }
        record["head"]["headers"] = safe_headers(
            head.get("headers", {})
        )

        origins = origins_for(url, self.args.origin)
        strategies = [("plain", {})]

        strategies += [
            (
                "origin:" + origin,
                {"Origin": origin, "Referer": origin + "/"},
            )
            for origin in origins
        ]

        strategies.append((
            "x-requested-with",
            {"X-Requested-With": "XMLHttpRequest"},
        ))

        strategies += [
            (
                "origin+x-requested-with:" + origin,
                {
                    "Origin": origin,
                    "Referer": origin + "/",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            for origin in origins
        ]

        best = None
        tried = set()

        for label, headers in strategies:
            key = tuple(sorted(headers.items()))

            if key in tried:
                continue

            tried.add(key)

            response = self.request(
                url, "GET", headers,
                limit=self.args.max_bytes,
            )

            record["attempts"].append({
                "strategy": label,
                "status": response["status"],
                "final_url": response["final_url"],
                "error": response["error"],
                "headers": safe_headers(response["headers"]),
                "elapsed_ms": response["elapsed_ms"],
                "body_bytes": len(response["body"]),
                "redirects": response["redirects"],
                "truncated": response["truncated"],
            })

            if best is None:
                best = response
            elif (
                response["status"] is not None
                and (
                    best["status"] is None
                    or (
                        best["status"] >= 400
                        and response["status"] < 400
                    )
                )
            ):
                best = response

            if (
                response["status"] is not None
                and 200 <= response["status"] < 300
                and response["body"]
            ):
                best = response
                break

            if self.client.budget_error():
                break

        if best is None:
            record["errors"].append("Нет результата HTTP")
            return record

        headers = best["headers"]
        body = best["body"]
        status = best["status"]

        content_type = next(
            (
                value for key, value in headers.items()
                if key.lower() == "content-type"
            ),
            "",
        )

        record.update({
            "http_status": status,
            "final_url": redact(best["final_url"]),
            "response_headers": safe_headers(headers),
            "content_type": content_type,
            "body_bytes": len(body),
            "body_truncated": best["truncated"],
            "redirects": best["redirects"],
            "elapsed_ms": best["elapsed_ms"],
            "server_claimed_urls": {
                key: value for key, value in headers.items()
                if key.lower() in CLAIM_HEADERS
            },
        })

        if body and looks_hls(
            body, content_type, best["final_url"] or url
        ):
            hls = self.parse_playlist(
                body.decode("utf-8-sig", errors="replace"),
                best["final_url"] or url,
            )
            record["hls"] = hls

            children = []

            if (
                self.args.mode != "quick"
                and depth < self.args.max_depth
            ):
                children += [
                    (item["uri"], "variant")
                    for item in hls["variants"]
                ]

                children += [
                    (item["resolved_uri"], "rendition")
                    for item in hls["renditions"]
                    if item.get("resolved_uri")
                ]

                if not hls["is_master"]:
                    children += [
                        (item["resolved_uri"], "key_or_map")
                        for item in hls["keys"] + hls["maps"]
                        if item.get("resolved_uri")
                        and not item["resolved_uri"].startswith("data:")
                    ]

                    children += [
                        (segment, "segment")
                        for segment in hls["segments"][
                            :self.args.max_segments
                        ]
                    ]

                local_seen = set()
                unique = []

                for child_url, child_kind in children:
                    if child_url not in local_seen:
                        local_seen.add(child_url)
                        unique.append((child_url, child_kind))

                # Рекурсия выполняется в текущем URL-потоке:
                # вложенный вызов не ждёт задачу из того же пула.
                for child_url, child_kind in unique[
                    :self.args.max_children
                ]:
                    record["children"].append(
                        self.check(
                            child_url, depth + 1,
                            url, child_kind,
                        )
                    )

            bad = [
                child for child in record["children"]
                if child.get("status_label") in (
                    "BROKEN", "ACCESS_RESTRICTED",
                    "UNKNOWN", "PARTIAL",
                )
            ]

            record["status_label"] = (
                "PARTIAL"
                if (
                    hls["warnings"]
                    or hls["duplicate_variant_uris"]
                    or bad
                    or best["truncated"]
                )
                else "WORKING"
            )

        elif status is not None and 200 <= status < 300 and body:
            record["status_label"] = "ACCESSIBLE_NON_HLS"
            record["body_preview"] = body[:200].decode(
                "utf-8", errors="replace"
            ).replace("\x00", "?")

        elif status in (401, 403):
            record["status_label"] = "ACCESS_RESTRICTED"
            record["errors"].append(
                f"HTTP {status}: доступ ограничен сервером"
            )

        elif status is not None and status >= 400:
            record["status_label"] = "BROKEN"
            record["errors"].append(f"HTTP {status}")

        elif best["error"]:
            record["status_label"] = "UNKNOWN"
            record["errors"].append(best["error"])

        if not body and status is not None and 200 <= status < 300:
            record["errors"].append(
                "Успешный HTTP-ответ с пустым телом"
            )

        return record


# ============================================================
# Загрузка пользовательских источников
# ============================================================

def load_urls(args):
    items = list(args.url or [])

    if args.input_file:
        path = Path(args.input_file)

        if not path.exists():
            raise FileNotFoundError(f"Не найден файл: {path}")

        items += path.read_text(
            encoding="utf-8-sig", errors="replace"
        ).splitlines()

    if args.stdin:
        items += list(sys.stdin)

    result = []

    for line in items:
        line = line.strip()

        if not line or line.startswith("#"):
            continue

        for token in line.split():
            if token.startswith(("http://", "https://", "//")):
                try:
                    url = normalize(token)
                    if url and url not in result:
                        result.append(url)
                except ValueError:
                    pass

    return result


# ============================================================
# Параллельное обнаружение и загрузка M3U-источников
# ============================================================

def process_m3u_sources(dissector, urls):
    """
    Параллельно загружает M3U/M3U8-источники, извлекает
    вложенные ссылки на плейлисты и формирует диагностические
    сведения. Исходные URL сохраняются в стабильном порядке.
    """
    unique_urls = list(dict.fromkeys(
        url for url in urls if is_playlist_url(url)
    ))

    unique_urls = unique_urls[:dissector.args.max_sources]

    futures = {
        url: dissector.discovery_pool.submit(
            download_m3u_source, dissector, url
        )
        for url in unique_urls
    }

    results = []
    discovered_nested = []

    for url in unique_urls:
        try:
            result = futures[url].result()
        except Exception as exc:
            result = {
                "url": redact(url),
                "status": None,
                "error": f"{type(exc).__name__}: {exc}",
                "bytes": 0,
                "nested_playlists": [],
            }

        results.append(result)
        discovered_nested.extend(
            result.get("nested_playlists", [])
        )

    nested_unique = list(dict.fromkeys(discovered_nested))
    return results, nested_unique


# ============================================================
# Отчёты
# ============================================================

def flatten(record):
    yield record

    for child in record.get("children", []):
        yield from flatten(child)


def text_report(report):
    lines = [
        f"Phoenix URL Dissector {VERSION}",
        f"UTC: {report['generated_at']}",
        f"Mode: {report['mode']}",
        "=" * 78,
    ]

    counts = collections.Counter()

    if report.get("livem3u"):
        live = report["livem3u"]

        lines += [
            "",
            "LIVEM3U DISCOVERY",
            f"Index: {live['index_url']}",
            f"Main: {live['main_url']}",
            f"Discovered playlist URLs: {len(live['discovered'])}",
        ]

        for error in live.get("errors", []):
            lines.append(
                f"  DISCOVERY ERROR {error['url']}: {error['error']}"
            )

    if report.get("m3u_sources"):
        lines += [
            "",
            "M3U SOURCE DOWNLOADS",
            f"Sources processed: {len(report['m3u_sources'])}",
        ]

        for item in report["m3u_sources"]:
            lines.append(
                f"  [{item.get('status')}] "
                f"{item.get('url')} "
                f"bytes={item.get('bytes', 0)} "
                f"entries={item.get('channel_entries', 0)}"
            )

            if item.get("error"):
                lines.append("    ERROR: " + item["error"])

            lines.append(
                f"    nested playlists="
                f"{len(item.get('nested_playlists', []))}"
            )

    for source in report["sources"]:
        lines += [
            "",
            f"[{source.get('status_label', 'UNKNOWN')}] "
            f"{source.get('url')}",
            (
                f"HTTP={source.get('http_status')} "
                f"type={source.get('content_type')} "
                f"bytes={source.get('body_bytes')}"
            ),
        ]

        for node in flatten(source):
            counts[node.get("status_label", "UNKNOWN")] += 1

        for error in source.get("errors", []):
            lines.append("  ERROR: " + error)

        network = source.get("network") or {}
        tcp = network.get("tcp") or {}

        if tcp:
            lines.append(
                f"  TCP connected={tcp.get('connected')} "
                f"latency_ms={tcp.get('elapsed_ms')}"
            )

        tls = network.get("tls") or {}

        if tls:
            lines.append(
                f"  TLS version={tls.get('tls_version')} "
                f"verified={tls.get('verified')} "
                f"cipher={tls.get('cipher')}"
            )

        hls = source.get("hls") or {}

        if hls:
            lines.append(
                f"  HLS master={hls.get('is_master')} "
                f"media={hls.get('is_media')} "
                f"variants={len(hls.get('variants', []))} "
                f"segments={len(hls.get('segments', []))} "
                f"duration={hls.get('declared_duration_seconds')}s "
                f"encryption={hls.get('encryption_methods')}"
            )

            for warning in hls.get("warnings", []):
                lines.append("  HLS WARNING: " + warning)

            for duplicate in hls.get("duplicate_variant_uris", []):
                lines.append(
                    f"  DUPLICATE VARIANT x{duplicate['count']}: "
                    f"{duplicate['uri']} "
                    f"resolutions={duplicate['declared_resolutions']}"
                )

            for variant in hls.get("variants", []):
                lines.append(
                    f"  VARIANT {variant.get('resolution')} "
                    f"bw={variant.get('bandwidth')} "
                    f"-> {variant['uri']}"
                )

        for child in source.get("children", []):
            lines.append(
                f"  CHILD {child.get('resource_type')} "
                f"[{child.get('status_label')}] "
                f"HTTP={child.get('http_status')} "
                f"{child.get('url')}"
            )

    lines += [
        "",
        "=" * 78,
        "Statuses: " + ", ".join(
            f"{key}={value}" for key, value in sorted(counts.items())
        ),
        "Note: ограниченная диагностика не гарантирует "
        "непрерывное воспроизведение.",
    ]

    return "\n".join(lines) + "\n"


# ============================================================
# Самотестирование
# ============================================================

def self_test():
    sample = (
        "#EXTM3U\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=4716800,"
        'RESOLUTION=1920x1080,CODECS="avc1,mp4a"\n'
        "https://stream.example/a.m3u8\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=640x360\n"
        "https://stream.example/a.m3u8\n"
    )

    parsed = parse_hls(
        sample, "https://example.org/master.m3u8"
    )

    assert parsed["valid_header"]
    assert len(parsed["variants"]) == 2
    assert len(parsed["duplicate_variant_uris"]) == 1

    assert normalize("example.org/a.m3u8") == (
        "https://example.org/a.m3u8"
    )

    assert "REDACTED" in redact(
        "https://example.org/x?token=abc"
    )

    assert "https://xykt-fix.github.io" in origins_for(
        "https://proxy.example/xykt-fix.github.io/a.m3u8"
    )

    extracted = extract_playlist_urls(
        b"#EXTM3U\n"
        b"https://example.org/data/playlists/NEWS.m3u\n"
        b"https://example.org/live/channel/12345.ts\n",
        "https://example.org/index.m3u",
    )

    assert len(extracted) == 1
    assert extracted[0].endswith("NEWS.m3u")

    print(f"Phoenix URL Dissector {VERSION}: self-test OK")


# ============================================================
# GitHub Actions workflow
# ============================================================

def write_workflow():
    path = Path(".github/workflows/phoenix.yml")
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        """name: Phoenix URL Dissector
on:
  workflow_dispatch:
    inputs:
      mode:
        description: quick, deep or hls
        required: true
        default: deep
  push:
    paths:
      - sources.txt
      - phoenix_dissector.py
jobs:
  analyze:
    runs-on: ubuntu-latest
    timeout-minutes: 25
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Analyze
        run: >-
          python phoenix_dissector.py
          --input-file sources.txt
          --mode "${{ github.event.inputs.mode || 'deep' }}"
          --output-dir reports
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: phoenix-reports
          path: reports/
          if-no-files-found: warn
""",
        encoding="utf-8",
    )

    print(f"Создан файл: {path}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description=f"Phoenix URL Dissector {VERSION}"
    )

    parser.add_argument("--url", action="append")
    parser.add_argument("--input-file")
    parser.add_argument("--stdin", action="store_true")

    parser.add_argument(
        "--mode", choices=("quick", "deep", "hls"),
        default="deep",
    )

    parser.add_argument("--origin")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--max-bytes", type=int, default=1_000_000)
    parser.add_argument(
        "--max-total-bytes", type=int, default=30_000_000
    )
    parser.add_argument("--max-runtime", type=float, default=120)
    parser.add_argument("--max-requests", type=int, default=250)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--max-segments", type=int, default=3)
    parser.add_argument("--max-children", type=int, default=12)
    parser.add_argument("--max-redirects", type=int, default=5)
    parser.add_argument("--max-sources", type=int, default=100)
    parser.add_argument("--user-agent", default=UA)
    parser.add_argument("--output-dir", default="phoenix_reports")
    parser.add_argument("--name", default="phoenix_report")
    parser.add_argument("--allow-private", action="store_true")
    parser.add_argument("--insecure-tls", action="store_true")
    parser.add_argument("--discover-livem3u", action="store_true")
    parser.add_argument(
        "--download-m3u-sources",
        action="store_true",
        help="Параллельно скачать и проанализировать M3U-источники",
    )
    parser.add_argument("--init-actions", action="store_true")
    parser.add_argument("--self-test", action="store_true")

    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0

    if args.init_actions:
        write_workflow()
        return 0

    if (
        args.timeout <= 0
        or args.max_bytes < 1024
        or args.max_total_bytes < args.max_bytes
        or args.max_runtime <= 0
        or args.max_requests < 1
        or args.max_sources < 1
        or min(
            args.max_depth,
            args.max_segments,
            args.max_children,
            args.max_redirects,
        ) < 0
    ):
        parser.error("Некорректные лимиты")

    try:
        urls = load_urls(args)
    except Exception as exc:
        print(
            f"Ошибка входных данных: {exc}",
            file=sys.stderr,
        )
        return 2

    if not urls and not args.discover_livem3u:
        parser.error(
            "Укажи --url, --input-file или --discover-livem3u"
        )

    Path(args.output_dir).mkdir(
        parents=True, exist_ok=True
    )

    dissector = Dissector(args)

    report = {
        "tool": "Phoenix URL Dissector",
        "version": VERSION,
        "generated_at": now(),
        "mode": args.mode,
        "settings": {
            "timeout": args.timeout,
            "max_bytes": args.max_bytes,
            "max_total_bytes": args.max_total_bytes,
            "max_runtime": args.max_runtime,
            "max_requests": args.max_requests,
            "max_depth": args.max_depth,
            "max_segments": args.max_segments,
            "max_children": args.max_children,
            "max_sources": args.max_sources,
            "allow_private": args.allow_private,
            "insecure_tls": args.insecure_tls,
            "threading": {
                "discovery_workers": DISCOVERY_WORKERS,
                "download_workers": DOWNLOAD_WORKERS,
                "parser_workers": PARSER_WORKERS,
                "url_workers": URL_WORKERS,
                "maximum_workers": (
                    DISCOVERY_WORKERS
                    + DOWNLOAD_WORKERS
                    + PARSER_WORKERS
                    + URL_WORKERS
                ),
            },
        },
        "livem3u": None,
        "m3u_sources": [],
        "sources": [],
    }

    try:
        if args.discover_livem3u:
            print(
                "[LiveM3U] Параллельно загружаю "
                "индекс и основной плейлист...",
                flush=True,
            )

            live = discover_livem3u(dissector)
            report["livem3u"] = live

            for url in live["discovered"]:
                if url not in urls:
                    urls.append(url)

            print(
                "[LiveM3U] Обнаружено плейлистов: "
                f"{len(live['discovered'])}",
                flush=True,
            )

        urls = list(dict.fromkeys(urls))[:args.max_sources]

        if not urls:
            parser.error(
                "Не удалось обнаружить источники. "
                "Проверь сеть или укажи --url/--input-file."
            )

        if args.download_m3u_sources:
            print(
                "[M3U] Параллельное скачивание источников...",
                flush=True,
            )

            m3u_results, nested_urls = process_m3u_sources(
                dissector, urls
            )

            report["m3u_sources"] = m3u_results

            for url in nested_urls:
                if url not in urls and len(urls) < args.max_sources:
                    urls.append(url)

            print(
                f"[M3U] Обработано источников: "
                f"{len(m3u_results)}",
                flush=True,
            )

        urls = list(dict.fromkeys(urls))[:args.max_sources]

        # Основные URL запускаются одновременно.
        # Результаты добавляются в отчёт в исходном порядке.
        futures = []

        for index, url in enumerate(urls, 1):
            print(
                f"[{index}/{len(urls)}] {redact(url)}",
                flush=True,
            )

            futures.append(
                dissector.url_pool.submit(
                    dissector.check, url
                )
            )

        for index, (url, future) in enumerate(
            zip(urls, futures), 1
        ):
            try:
                record = future.result()
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                record = {
                    "url": redact(url),
                    "status_label": "UNKNOWN",
                    "errors": [
                        f"{type(exc).__name__}: {exc}"
                    ],
                }

            report["sources"].append(record)

            print(
                f"  [{index}/{len(urls)}] "
                f"=> {record.get('status_label')} "
                f"HTTP={record.get('http_status', '?')}",
                flush=True,
            )

            budget_error = dissector.client.budget_error()

            if budget_error:
                print(
                    "[LIMIT] " + budget_error,
                    flush=True,
                )
                # Уже отправленные задания завершаются безопасно.
                # Новые URL больше не добавляются.
                break

    finally:
        dissector.shutdown()

    report["runtime"] = round(
        time.monotonic() - dissector.client.started, 3
    )
    report["http_requests"] = dissector.client.request_count
    report["downloaded_bytes"] = dissector.client.bytes_read
    report["visited_urls"] = len(dissector.visited)

    stem = re.sub(
        r"[^A-Za-z0-9_.-]+", "_", args.name
    ).strip("._") or "phoenix_report"

    json_path = Path(args.output_dir) / f"{stem}.json"
    txt_path = Path(args.output_dir) / f"{stem}.txt"

    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    txt_path.write_text(
        text_report(report),
        encoding="utf-8",
    )

    print("\n" + text_report(report))
    print(f"JSON: {json_path}")
    print(f"TXT:  {txt_path}")
    print(
        "HTTP requests: "
        f"{report['http_requests']}"
    )
    print(
        "Downloaded bytes: "
        f"{report['downloaded_bytes']}"
    )
    print(
        "Runtime: "
        f"{report['runtime']} s"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print(
            "Прервано пользователем",
            file=sys.stderr,
        )
        raise SystemExit(130)
