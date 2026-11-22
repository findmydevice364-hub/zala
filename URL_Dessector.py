
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix URL Dissector 2.2.0
HTTP / proxy / HLS / LiveM3U diagnostics.
Python 3.9+, standard library only.

Возможности:
- автоматическое обнаружение плейлистов LiveM3U;
- INDEX.m3u, основной playlist.m3u, вложенные M3U;
- DNS, TCP, TLS, HTTP, редиректы;
- Origin / Referer / X-Requested-With;
- master/media HLS, варианты качества, сегменты, ключи и карты;
- защита от циклов, ограничение времени и объёма запросов;
- JSON/TXT отчёты;
- Termux и GitHub Actions.

Не обходит DRM, авторизацию или ограничения доступа.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import ipaddress
import json
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from pathlib import Path


VERSION = "2.2.0"

LIVE_M3U_BASE = "https://livem3u.tatnet.app"
LIVE_M3U_INDEX = (
    LIVE_M3U_BASE + "/data/playlists/INDEX.m3u"
)
LIVE_M3U_MAIN = LIVE_M3U_BASE + "/data/playlist.m3u"

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
        ip = ipaddress.ip_address(value)
        return ip.is_global
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
# Защищённая сетевая диагностика
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
                "host": host,
                "port": port,
                "addresses": [],
                "error": "DNS не вернул адресов",
            }

        blocked = [
            ip for ip in addresses if not public_ip(ip)
        ]

        if blocked and not allow_private:
            return {
                "host": host,
                "port": port,
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
    """Проверяет только адреса, прошедшие DNS-политику."""
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
                    "host": host,
                    "port": port,
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
        "host": host,
        "port": port,
        "connected": False,
        "attempts": attempts,
        "elapsed_ms": round(
            (time.monotonic() - started) * 1000, 2
        ),
    }


def tls_diagnostics(host, port, addresses, timeout=8):
    """TLS с проверкой сертификата и ограничением DNS."""
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
# HTTP-клиент: ограничение запросов, байтов и редиректов
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

        context = (
            ssl._create_unverified_context()
            if args.insecure_tls
            else ssl.create_default_context()
        )

        self.opener = urllib.request.build_opener(
            NoRedirect(),
            urllib.request.HTTPSHandler(context=context),
        )

    def budget_error(self):
        if self.request_count >= self.args.max_requests:
            return "Достигнут общий лимит HTTP-запросов"

        if self.bytes_read >= self.args.max_total_bytes:
            return "Достигнут общий лимит загруженных байтов"

        if (
            time.monotonic() - self.started
            >= self.args.max_runtime
        ):
            return "Достигнут общий лимит времени"

        return None

    def request(self, url, method="GET", headers=None, limit=None):
        started = time.monotonic()
        current = url
        chain = []

        limit = min(
            self.args.max_bytes,
            max(0, limit if limit is not None
                else self.args.max_bytes),
        )

        request_headers = {
            "User-Agent": self.args.user_agent,
            "Accept": "*/*",
            "Connection": "close",
        }
        request_headers.update(headers or {})

        for hop in range(self.args.max_redirects + 1):
            budget = self.budget_error()
            if budget:
                return self._result(
                    url, current, method, None, {}, b"",
                    budget, chain, started, None
                )

            host, port = host_port(current)
            key = (host, port)

            if key not in self.dns:
                self.dns[key] = dns_lookup(
                    host, port, self.args.allow_private
                )

            dns = self.dns[key]
            if dns.get("error"):
                return self._result(
                    url, current, method, None, {}, b"",
                    dns["error"], chain, started, dns
                )

            self.request_count += 1
            request = urllib.request.Request(
                current,
                headers=request_headers,
                method=method,
            )

            try:
                response = self.opener.open(
                    request, timeout=self.args.timeout
                )
                status = getattr(response, "status", 200)
                response_headers = dict(response.headers.items())

                if method == "HEAD" or limit == 0:
                    body = b""
                    truncated = False
                else:
                    remaining = max(
                        0,
                        self.args.max_total_bytes - self.bytes_read,
                    )
                    allowed = min(limit, remaining)
                    raw = response.read(allowed + 1)
                    truncated = len(raw) > allowed
                    body = raw[:allowed]
                    self.bytes_read += len(body)

                response.close()

                return self._result(
                    url, current, method, status,
                    response_headers, body, None,
                    chain, started, dns, truncated
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
                    continue

                try:
                    raw = b"" if method == "HEAD" else exc.read(
                        min(limit, 65536) + 1
                    )
                    body = raw[:limit]
                    self.bytes_read += len(body)
                except Exception:
                    body = b""

                exc.close()
                return self._result(
                    url, current, method, status,
                    response_headers, body,
                    f"HTTP {status}: {exc.reason}",
                    chain, started, dns,
                    len(body) >= limit > 0,
                )

            except Exception as exc:
                return self._result(
                    url, current, method, None, {}, b"",
                    f"{type(exc).__name__}: {exc}",
                    chain, started, dns
                )

        return self._result(
            url, current, method, None, {}, b"",
            "Превышен лимит редиректов",
            chain, started, None
        )

    @staticmethod
    def _result(
        url, current, method, status, headers, body,
        error, chain, started, dns, truncated=False
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
# Автоматический поиск Origin
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
        if not re.fullmatch(r"[A-Za-z0-9.-]+(?::\d+)?", part):
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

        for tag, field in (
            ("#EXT-X-TARGETDURATION:", "target_duration"),
            ("#EXT-X-MEDIA-SEQUENCE:", "media_sequence"),
            ("#EXT-X-PLAYLIST-TYPE:", "playlist_type"),
        ):
            if line.startswith(tag):
                result[field] = line.split(":", 1)[1]
                break
        else:
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
        result["warnings"].append("Нет #EXTM3U в начале плейлиста")
    if pending is not None:
        result["warnings"].append("#EXT-X-STREAM-INF без URI")
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
# LiveM3U: обнаружение ссылок на плейлисты
# ============================================================

def is_playlist_url(url):
    try:
        path = urllib.parse.urlsplit(url).path.lower()
        return path.endswith((
            ".m3u", ".m3u8", ".m3u.txt",
        ))
    except Exception:
        return False


def extract_playlist_urls(body, base_url):
    """
    Извлекает только ссылки, похожие на файлы плейлистов.
    Обычные канальные потоки не превращаются в источники каталога.
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

        if not candidates and (
            line.lower().endswith((".m3u", ".m3u8"))
        ):
            candidates = [line]

        for candidate in candidates:
            candidate = candidate.rstrip("),;]")
            absolute = urllib.parse.urljoin(base_url, candidate)

            try:
                absolute = normalize(absolute)
            except ValueError:
                continue

            if is_playlist_url(absolute) and absolute not in seen:
                seen.add(absolute)
                found.append(absolute)

    return found


def discover_livem3u(dissector):
    """
    Получает индекс и основной плейлист LiveM3U.
    Каждый обнаруженный URL сохраняется в результате, даже
    если отдельная загрузка не удалась.
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

    for url in candidates:
        response = dissector.client.request(
            url, "GET", limit=dissector.args.max_bytes
        )

        field = "index_status" if url == LIVE_M3U_INDEX else "main_status"
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

    unique = []
    seen = set()

    for url in result["discovered"]:
        if url not in seen:
            seen.add(url)
            unique.append(url)

    result["discovered"] = unique[:dissector.args.max_sources]
    return result


# ============================================================
# Основной анализатор
# ============================================================

class Dissector:
    def __init__(self, args):
        self.args = args
        self.client = Client(args)
        self.visited = set()

    def check_network(self, url):
        host, port = host_port(url)
        dns = self.client.dns.get((host, port))

        if dns is None:
            dns = dns_lookup(
                host, port, self.args.allow_private
            )

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

    def check(self, url, depth=0, parent=None, kind="source"):
        if url in self.visited:
            return {
                "url": redact(url),
                "resource_type": kind,
                "status_label": "SKIPPED_DUPLICATE",
            }

        if depth > self.args.max_depth:
            return {
                "url": redact(url),
                "resource_type": kind,
                "status_label": "DEPTH_LIMIT",
            }

        if len(self.visited) >= self.args.max_sources:
            return {
                "url": redact(url),
                "resource_type": kind,
                "status_label": "SOURCE_LIMIT",
            }

        self.visited.add(url)
        parsed = urllib.parse.urlsplit(url)

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

        head = self.client.request(url, "HEAD", limit=0)
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

            response = self.client.request(
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
            hls = parse_hls(
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

                for child_url, child_kind in unique[
                    :self.args.max_children
                ]:
                    # Не переносим Origin от обёртки на чужой хост.
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
        "Note: ограниченная диагностика не гарантирует непрерывное воспроизведение.",
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

    parsed = parse_hls(sample, "https://example.org/master.m3u8")
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
        description="Phoenix URL Dissector 2.2.0"
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
    parser.add_argument("--max-total-bytes", type=int, default=30_000_000)
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
        print(f"Ошибка входных данных: {exc}", file=sys.stderr)
        return 2

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

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
        },
        "livem3u": None,
        "sources": [],
    }

    if args.discover_livem3u:
        print("[LiveM3U] Загружаю индекс и основной плейлист...")
        live = discover_livem3u(dissector)
        report["livem3u"] = live

        for url in live["discovered"]:
            if url not in urls:
                urls.append(url)

        print(
            f"[LiveM3U] Обнаружено плейлистов: "
            f"{len(live['discovered'])}"
        )

    # Сохраняем порядок, убираем повторы исходных URL.
    urls = list(dict.fromkeys(urls))[:args.max_sources]

    if not urls:
        parser.error(
            "Укажи --url, --input-file или --discover-livem3u"
        )

    for index, url in enumerate(urls, 1):
        print(
            f"[{index}/{len(urls)}] {redact(url)}",
            flush=True,
        )

        try:
            record = dissector.check(url)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            record = {
                "url": redact(url),
                "status_label": "UNKNOWN",
                "errors": [f"{type(exc).__name__}: {exc}"],
            }

        report["sources"].append(record)
        print(
            f"  => {record.get('status_label')} "
            f"HTTP={record.get('http_status', '?')}",
            flush=True,
        )

        if dissector.client.budget_error():
            print(
                "[LIMIT] " + dissector.client.budget_error(),
                flush=True,
            )
            break

    stem = re.sub(
        r"[^A-Za-z0-9_.-]+", "_", args.name
    ).strip("._") or "phoenix_report"

    json_path = Path(args.output_dir) / f"{stem}.json"
    txt_path = Path(args.output_dir) / f"{stem}.txt"

    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    txt_path.write_text(text_report(report), encoding="utf-8")

    print("\n" + text_report(report))
    print(f"JSON: {json_path}")
    print(f"TXT:  {txt_path}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Прервано пользователем", file=sys.stderr)
        raise SystemExit(130)
