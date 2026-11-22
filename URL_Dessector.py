
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phoenix URL Dissector 2.3.1.0-beta
HTTP / proxy / M3U / HLS recursive stream diagnostics.

Python 3.9+
Standard library only.

Основные возможности:
- многопоточная обработка исходных URL;
- автоматическое обнаружение LiveM3U INDEX.m3u и playlist.m3u;
- рекурсивный обход дочерних M3U и HLS master-playlist;
- отдельная проверка конечных потоков;
- проверка структуры HLS media-playlist и тест первого сегмента;
- единый ограничитель HTTP-запросов и загруженных байтов;
- ограничение глубины обхода, времени работы и количества URL;
- защита от циклов и повторного обхода URL;
- сохранение метаданных EXTINF и связанных директив;
- итоговый M3U только из подтверждённых конечных адресов;
- TXT и JSON отчёты;
- диагностика DNS/TCP/TLS;
- поддержка GitHub Actions и Termux.

Важно:
HTTP-ответ 200 не доказывает, что поток воспроизводится в IPTV-плеере.
Отчёт разделяет сетевую доступность, структуру HLS и результат проверки
конечного URL. Полное декодирование аудио/видео требует внешнего декодера.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import queue
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


VERSION = "2.3.1.0-beta"

LIVE_M3U_BASE = "https://livem3u.tatnet.app"
LIVE_M3U_INDEX = (
    LIVE_M3U_BASE + "/data/playlists/INDEX.m3u"
)
LIVE_M3U_MAIN = (
    LIVE_M3U_BASE + "/data/playlist.m3u"
)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/131.0 Safari/537.36 "
    "PhoenixURLDissector/2.3.1.0-beta"
)

PLAYLIST_TYPES = (
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/mpegurl",
    "audio/x-mpegurl",
)

MEDIA_TYPES = (
    "video/",
    "audio/",
    "application/octet-stream",
    "application/ogg",
    "application/mp2t",
)

URL_RE = re.compile(
    r"""https?://[^\s"'<>]+""",
    re.IGNORECASE,
)

ATTR_RE = re.compile(
    r"""([A-Za-z0-9_-]+)\s*=\s*"""
    r"""(?:"((?:[^"\\]|\\.)*)"|([^,]*))"""
)

HLS_TAG_RE = re.compile(r"^#EXT-X-[A-Z0-9-]+", re.I)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def normalize_url(url: str) -> str:
    """Normalize URL without changing its query parameters."""
    url = (url or "").strip().strip('"').strip("'")
    if not url:
        return ""

    parts = urllib.parse.urlsplit(url)

    if parts.scheme.lower() not in ("http", "https"):
        return ""

    if not parts.hostname:
        return ""

    scheme = parts.scheme.lower()
    host = parts.hostname.lower()

    try:
        port = parts.port
    except ValueError:
        return ""

    if ":" in host and not host.startswith("["):
        host = "[" + host + "]"

    if port and not (
        (scheme == "http" and port == 80)
        or (scheme == "https" and port == 443)
    ):
        netloc = "{}:{}".format(host, port)
    else:
        netloc = host

    if parts.username:
        # Do not put credentials into the reconstructed netloc.
        # URLs containing credentials are still accepted by urllib
        # only if the caller supplied them; reports redact them.
        userinfo = urllib.parse.quote(parts.username, safe="")
        if parts.password:
            userinfo += ":" + urllib.parse.quote(
                parts.password, safe=""
            )
        netloc = userinfo + "@" + netloc

    path = parts.path or "/"

    return urllib.parse.urlunsplit(
        (scheme, netloc, path, parts.query, parts.fragment)
    )


def redact_url(url: str) -> str:
    """Hide URL credentials and common sensitive query values."""
    try:
        parts = urllib.parse.urlsplit(url)
        host = parts.hostname or ""

        try:
            port = parts.port
        except ValueError:
            port = None

        if ":" in host and not host.startswith("["):
            host = "[" + host + "]"

        netloc = host
        if port:
            netloc += ":" + str(port)

        sensitive = {
            "token", "access_token", "auth", "password",
            "passwd", "key", "apikey", "api_key", "secret",
            "signature", "sig", "session", "sessionid",
        }

        query = []
        for key, value in urllib.parse.parse_qsl(
            parts.query, keep_blank_values=True
        ):
            if key.lower() in sensitive:
                value = "***"
            query.append((key, value))

        return urllib.parse.urlunsplit(
            (
                parts.scheme,
                netloc,
                parts.path,
                urllib.parse.urlencode(query),
                parts.fragment,
            )
        )
    except Exception:
        return "<redacted-url>"


def host_port(url: str) -> Tuple[Optional[str], Optional[int]]:
    try:
        parts = urllib.parse.urlsplit(url)
        if not parts.hostname:
            return None, None
        port = parts.port
        if port is None:
            port = 443 if parts.scheme.lower() == "https" else 80
        return parts.hostname, port
    except Exception:
        return None, None


def is_playlist_url(url: str) -> bool:
    path = urllib.parse.urlsplit(url).path.lower()
    return path.endswith((".m3u", ".m3u8", ".m3u8.gz"))


def parse_attributes(line: str) -> Dict[str, str]:
    result: Dict[str, str] = {}

    for match in ATTR_RE.finditer(line):
        key = match.group(1)
        value = match.group(2)

        if value is None:
            value = (match.group(3) or "").strip()

        result[key] = value

    return result


def resolve_url(base_url: str, child_url: str) -> str:
    child_url = (child_url or "").strip()

    if not child_url:
        return ""

    if child_url.startswith("#"):
        return ""

    return normalize_url(
        urllib.parse.urljoin(base_url, child_url)
    )


@dataclass
class HttpResult:
    requested_url: str
    final_url: str = ""
    status: Optional[int] = None
    reason: str = ""
    content_type: str = ""
    content_length: Optional[int] = None
    body: bytes = b""
    bytes_read: int = 0
    elapsed_seconds: float = 0.0
    headers: Dict[str, str] = field(default_factory=dict)
    error: str = ""
    truncated: bool = False
    request_method: str = "GET"

    @property
    def ok(self) -> bool:
        return (
            self.status is not None
            and 200 <= self.status < 300
            and not self.error
        )

    @property
    def text(self) -> str:
        charset = "utf-8"

        content_type = self.content_type or ""
        match = re.search(
            r"charset\s*=\s*['\"]?([^;'\"\s]+)",
            content_type,
            re.IGNORECASE,
        )

        if match:
            charset = match.group(1)

        try:
            return self.body.decode(charset, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")

    def as_dict(self, include_body: bool = False) -> Dict[str, Any]:
        data = asdict(self)
        data["requested_url"] = redact_url(self.requested_url)
        data["final_url"] = redact_url(self.final_url)
        data.pop("body", None)

        if include_body:
            data["body_preview"] = self.text[:2000]

        return data


class RequestBudgetExceeded(RuntimeError):
    pass


class Client:
    """
    Shared HTTP client.

    Every HTTP request is reserved under one lock before opening a
    connection. The aggregate body-byte budget is reserved as well,
    so concurrent workers cannot independently exceed the configured
    aggregate byte allowance.

    DNS/TCP/TLS diagnostic sockets are reported separately and are
    not HTTP requests. They are only used when --network-diagnostics
    is enabled.
    """

    def __init__(
        self,
        timeout: float = 12.0,
        max_bytes: int = 2_000_000,
        max_total_bytes: int = 40_000_000,
        max_runtime: float = 300.0,
        max_requests: int = 500,
        user_agent: str = DEFAULT_USER_AGENT,
        insecure_tls: bool = False,
    ) -> None:
        self.timeout = max(1.0, float(timeout))
        self.max_bytes = max(1, int(max_bytes))
        self.max_total_bytes = max(1, int(max_total_bytes))
        self.max_runtime = max(1.0, float(max_runtime))
        self.max_requests = max(1, int(max_requests))
        self.user_agent = user_agent
        self.insecure_tls = insecure_tls

        self.started = time.monotonic()
        self.lock = threading.RLock()

        self.request_count = 0
        self.bytes_read = 0
        self.bytes_reserved = 0
        self.budget_events: List[str] = []

        self._thread_local = threading.local()

    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def remaining_runtime(self) -> float:
        return max(0.0, self.max_runtime - self.elapsed())

    def _reserve_request(
        self,
        requested_limit: int,
    ) -> int:
        with self.lock:
            if self.elapsed() >= self.max_runtime:
                self.budget_events.append("runtime limit reached")
                raise RequestBudgetExceeded(
                    "Global runtime limit reached"
                )

            if self.request_count >= self.max_requests:
                self.budget_events.append("HTTP request limit reached")
                raise RequestBudgetExceeded(
                    "Global HTTP request limit reached"
                )

            remaining = (
                self.max_total_bytes
                - self.bytes_read
                - self.bytes_reserved
            )

            if remaining <= 0:
                self.budget_events.append(
                    "aggregate byte limit reached"
                )
                raise RequestBudgetExceeded(
                    "Global byte limit reached"
                )

            allowed = min(
                max(1, requested_limit),
                self.max_bytes,
                remaining,
            )

            self.request_count += 1
            self.bytes_reserved += allowed

            return allowed

    def _finish_request(
        self,
        reserved: int,
        actual: int,
    ) -> None:
        with self.lock:
            self.bytes_reserved -= reserved
            if self.bytes_reserved < 0:
                self.bytes_reserved = 0

            self.bytes_read += actual

    def _opener(self):
        opener = getattr(self._thread_local, "opener", None)

        if opener is not None:
            return opener

        handlers: List[Any] = [
            urllib.request.ProxyHandler(),
        ]

        if self.insecure_tls:
            context = ssl._create_unverified_context()
            handlers.append(
                urllib.request.HTTPSHandler(context=context)
            )

        opener = urllib.request.build_opener(*handlers)
        self._thread_local.opener = opener
        return opener

    def request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        read_limit: Optional[int] = None,
    ) -> HttpResult:
        started = time.monotonic()
        url = normalize_url(url)

        if not url:
            return HttpResult(
                requested_url=url,
                error="Invalid HTTP(S) URL",
                request_method=method,
            )

        requested_limit = (
            self.max_bytes
            if read_limit is None
            else max(1, int(read_limit))
        )

        try:
            reserved = self._reserve_request(requested_limit)
        except RequestBudgetExceeded as exc:
            return HttpResult(
                requested_url=url,
                error="BUDGET: " + str(exc),
                request_method=method,
            )

        actual = 0
        result = HttpResult(
            requested_url=url,
            request_method=method,
        )

        request_headers = {
            "User-Agent": self.user_agent,
            "Accept": (
                "application/vnd.apple.mpegurl,"
                "application/x-mpegURL,"
                "audio/mpegurl,"
                "video/*,"
                "audio/*,"
                "*/*"
            ),
            "Connection": "close",
        }

        if headers:
            request_headers.update(headers)

        req = urllib.request.Request(
            url,
            headers=request_headers,
            method=method,
        )

        try:
            with self._opener().open(
                req,
                timeout=min(
                    self.timeout,
                    max(1.0, self.remaining_runtime()),
                ),
            ) as response:
                result.status = getattr(response, "status", None)
                result.reason = str(
                    getattr(response, "reason", "") or ""
                )
                result.final_url = response.geturl()

                raw_headers = response.headers
                result.headers = {
                    str(k).lower(): str(v)
                    for k, v in raw_headers.items()
                }

                result.content_type = (
                    raw_headers.get("Content-Type", "") or ""
                ).lower()

                try:
                    raw_length = raw_headers.get("Content-Length")
                    result.content_length = (
                        int(raw_length) if raw_length else None
                    )
                except (TypeError, ValueError):
                    result.content_length = None

                if method.upper() != "HEAD":
                    chunks = []
                    remaining = reserved

                    while remaining > 0:
                        if self.remaining_runtime() <= 0:
                            result.truncated = True
                            break

                        chunk = response.read(
                            min(16_384, remaining)
                        )

                        if not chunk:
                            break

                        chunks.append(chunk)
                        actual += len(chunk)
                        remaining -= len(chunk)

                    result.body = b"".join(chunks)

                    if actual >= reserved:
                        result.truncated = True

        except urllib.error.HTTPError as exc:
            result.status = exc.code
            result.reason = str(exc.reason or "")
            result.final_url = exc.geturl()

            try:
                result.headers = {
                    str(k).lower(): str(v)
                    for k, v in exc.headers.items()
                }
                result.content_type = (
                    exc.headers.get("Content-Type", "") or ""
                ).lower()
            except Exception:
                pass

            result.error = "HTTPError: {}".format(exc.code)

            if method.upper() != "HEAD":
                try:
                    remaining = reserved

                    while remaining > 0:
                        if self.remaining_runtime() <= 0:
                            result.truncated = True
                            break

                        chunk = exc.read(
                            min(16_384, remaining)
                        )

                        if not chunk:
                            break

                        actual += len(chunk)
                        result.body += chunk
                        remaining -= len(chunk)

                    if actual >= reserved:
                        result.truncated = True

                except Exception:
                    pass

        except Exception as exc:
            result.error = "{}: {}".format(
                type(exc).__name__, str(exc)
            )

        finally:
            result.bytes_read = actual
            result.elapsed_seconds = time.monotonic() - started
            self._finish_request(reserved, actual)

        return result

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "http_requests": self.request_count,
                "bytes_read": self.bytes_read,
                "bytes_reserved": self.bytes_reserved,
                "max_requests": self.max_requests,
                "max_bytes_per_request": self.max_bytes,
                "max_total_bytes": self.max_total_bytes,
                "max_runtime_seconds": self.max_runtime,
                "elapsed_seconds": round(self.elapsed(), 3),
                "budget_events": list(self.budget_events),
            }


def network_diagnostics(
    url: str,
    timeout: float,
    insecure_tls: bool = False,
) -> Dict[str, Any]:
    """
    Optional low-level diagnostics.

    These DNS/TCP/TLS socket operations are not HTTP requests.
    They have their own timeout and should be used sparingly.
    """
    host, port = host_port(url)

    report: Dict[str, Any] = {
        "url": redact_url(url),
        "host": host,
        "port": port,
        "dns": {},
        "tcp": {},
        "tls": {},
    }

    if not host or not port:
        report["error"] = "Unable to parse host and port"
        return report

    started = time.monotonic()

    try:
        records = socket.getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
        )
        addresses = sorted(
            {
                item[4][0]
                for item in records
                if item and item[4]
            }
        )

        report["dns"] = {
            "ok": bool(addresses),
            "addresses": addresses,
            "elapsed_seconds": round(
                time.monotonic() - started, 3
            ),
        }
    except Exception as exc:
        report["dns"] = {
            "ok": False,
            "error": "{}: {}".format(type(exc).__name__, exc),
        }
        return report

    tcp_started = time.monotonic()
    sock = None

    try:
        sock = socket.create_connection(
            (host, port),
            timeout=timeout,
        )
        report["tcp"] = {
            "ok": True,
            "elapsed_seconds": round(
                time.monotonic() - tcp_started, 3
            ),
        }
    except Exception as exc:
        report["tcp"] = {
            "ok": False,
            "error": "{}: {}".format(type(exc).__name__, exc),
        }
        return report

    if urllib.parse.urlsplit(url).scheme.lower() == "https":
        tls_started = time.monotonic()

        try:
            if insecure_tls:
                context = ssl._create_unverified_context()
            else:
                context = ssl.create_default_context()

            with context.wrap_socket(
                sock,
                server_hostname=host,
            ) as tls_sock:
                cert = tls_sock.getpeercert()

                report["tls"] = {
                    "ok": True,
                    "version": tls_sock.version(),
                    "cipher": tls_sock.cipher(),
                    "certificate_subject": cert.get("subject", []),
                    "elapsed_seconds": round(
                        time.monotonic() - tls_started, 3
                    ),
                }

                sock = None

        except Exception as exc:
            report["tls"] = {
                "ok": False,
                "error": "{}: {}".format(
                    type(exc).__name__, exc
                ),
            }

    else:
        report["tls"] = {
            "ok": None,
            "reason": "URL uses HTTP, not HTTPS",
        }

    if sock is not None:
        try:
            sock.close()
        except Exception:
            pass

    return report


@dataclass
class M3UEntry:
    metadata_lines: List[str]
    info_line: str
    title: str
    url: str


def parse_m3u_entries(
    text: str,
    base_url: str,
) -> List[M3UEntry]:
    """
    Parse standard M3U records while preserving EXTINF and other
    channel directives. Each record ends at its first non-comment URL.
    """
    entries: List[M3UEntry] = []
    pending: List[str] = []
    current_info = ""
    current_metadata: List[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF"):
            if current_info:
                pending = []

            current_info = line
            current_metadata = [
                item for item in pending
                if item.startswith("#")
            ]
            pending = []
            continue

        if line.startswith("#"):
            if current_info:
                current_metadata.append(line)
            else:
                pending.append(line)
            continue

        resolved = resolve_url(base_url, line)

        if not resolved:
            current_info = ""
            current_metadata = []
            pending = []
            continue

        if not current_info:
            current_info = (
                '#EXTINF:-1,{}'.format(
                    urllib.parse.urlsplit(resolved).path.rsplit(
                        "/", 1
                    )[-1] or resolved
                )
            )
            current_metadata = list(pending)

        title = ""
        if "," in current_info:
            title = current_info.split(",", 1)[1].strip()

        entries.append(
            M3UEntry(
                metadata_lines=list(current_metadata),
                info_line=current_info,
                title=title,
                url=resolved,
            )
        )

        current_info = ""
        current_metadata = []
        pending = []

    return entries


def parse_hls(
    text: str,
    base_url: str,
) -> Dict[str, Any]:
    """
    Extract HLS master variants, alternate renditions, and media
    segments. Segment URLs are never emitted as IPTV channels.
    """
    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]

    variants: List[Dict[str, Any]] = []
    renditions: List[Dict[str, Any]] = []
    segments: List[str] = []
    warnings: List[str] = []

    is_master = False
    has_extinf = False
    has_target_duration = False
    has_endlist = False
    has_media_sequence = False

    pending_stream_info = ""
    pending_extinf = False

    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            is_master = True
            pending_stream_info = line.partition(":")[2]
            continue

        if line.startswith("#EXT-X-MEDIA:"):
            is_master = True
            attrs = parse_attributes(line.partition(":")[2])
            uri = attrs.get("URI", "")

            renditions.append({
                "attributes": attrs,
                "url": resolve_url(base_url, uri) if uri else "",
            })
            continue

        if line.startswith("#EXTINF:"):
            has_extinf = True
            pending_extinf = True
            continue

        if line.startswith("#EXT-X-TARGETDURATION:"):
            has_target_duration = True
            continue

        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            has_media_sequence = True
            continue

        if line.startswith("#EXT-X-ENDLIST"):
            has_endlist = True
            continue

        if line.startswith("#"):
            if HLS_TAG_RE.match(line):
                pass
            continue

        resolved = resolve_url(base_url, line)

        if not resolved:
            continue

        if pending_stream_info:
            attrs = parse_attributes(pending_stream_info)

            try:
                bandwidth = int(
                    attrs.get("BANDWIDTH", "0")
                )
            except ValueError:
                bandwidth = 0

            variants.append({
                "url": resolved,
                "bandwidth": bandwidth,
                "attributes": attrs,
            })

            pending_stream_info = ""
            continue

        if pending_extinf:
            segments.append(resolved)
            pending_extinf = False
            continue

        # In a media playlist, a URI after EXT-X-MAP or a URI line
        # without a preceding EXTINF may be an initialization segment.
        # It is kept for structural diagnostics only.
        if has_extinf:
            segments.append(resolved)

    if is_master and not variants and not renditions:
        warnings.append(
            "HLS master tags were found, but no variant URLs were parsed"
        )

    if has_extinf and not segments:
        warnings.append(
            "EXTINF tags were found, but no segment URLs were parsed"
        )

    is_media = (
        has_extinf
        and bool(segments)
        and not variants
    )

    return {
        "is_master": is_master,
        "is_media": is_media,
        "has_extinf": has_extinf,
        "has_target_duration": has_target_duration,
        "has_endlist": has_endlist,
        "has_media_sequence": has_media_sequence,
        "variants": variants,
        "renditions": renditions,
        "segments": segments,
        "warnings": warnings,
        "line_count": len(lines),
    }


def looks_like_playlist(
    url: str,
    result: HttpResult,
) -> bool:
    content_type = (result.content_type or "").lower()

    if any(item in content_type for item in PLAYLIST_TYPES):
        return True

    body_start = result.body.lstrip()[:128].upper()

    if body_start.startswith(b"#EXTM3U"):
        return True

    if is_playlist_url(url):
        return True

    return False


def is_html_error_page(result: HttpResult) -> bool:
    content_type = (result.content_type or "").lower()
    prefix = result.body.lstrip()[:256].lower()

    if "text/html" in content_type:
        return True

    if (
        prefix.startswith(b"<!doctype html")
        or prefix.startswith(b"<html")
        or b"<html" in prefix[:128]
    ):
        return True

    return False


def direct_stream_acceptable(result: HttpResult) -> Tuple[bool, str]:
    """
    Conservative HTTP-level check for a direct media endpoint.
    This is not a full codec/decode test.
    """
    if result.status is None:
        return False, "No HTTP status"

    if not (200 <= result.status < 300):
        return False, "HTTP status {}".format(result.status)

    if not result.body:
        return False, "Empty response body"

    if is_html_error_page(result):
        return False, "HTML response instead of a media stream"

    content_type = (result.content_type or "").lower()

    if any(content_type.startswith(t) for t in MEDIA_TYPES):
        return True, "HTTP success, non-empty media-like response"

    if any(t in content_type for t in PLAYLIST_TYPES):
        return False, "Playlist response, not a terminal stream"

    # Some IPTV servers return application/unknown or no Content-Type.
    # Reject obvious text/error responses, but accept a non-empty body.
    if content_type.startswith("text/"):
        prefix = result.body.lstrip()[:256].lower()

        if (
            prefix.startswith(b"error")
            or prefix.startswith(b"access denied")
            or prefix.startswith(b"forbidden")
            or prefix.startswith(b"not found")
        ):
            return False, "Text error response"

        return False, "Text response is not confirmed as media"

    return True, "HTTP success, non-empty response; media type inconclusive"


def extract_playlist_urls(text: str, base_url: str) -> List[str]:
    """
    Extract playlist URLs from an index-like text document.
    Only explicit HTTP(S) URLs are accepted here.
    """
    found: List[str] = []

    for match in URL_RE.findall(text or ""):
        candidate = match.rstrip("),.;]}")

        if not candidate:
            continue

        normalized = normalize_url(candidate)

        if normalized and is_playlist_url(normalized):
            found.append(normalized)

    # Also support relative playlist paths in a simple index.
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith(("http://", "https://")):
            continue

        if ".m3u" not in line.lower():
            continue

        resolved = resolve_url(base_url, line)

        if resolved:
            found.append(resolved)

    unique: List[str] = []
    seen: Set[str] = set()

    for item in found:
        if item not in seen:
            seen.add(item)
            unique.append(item)

    return unique


def merge_metadata(
    outer: Optional[M3UEntry],
    inner: Optional[M3UEntry],
) -> Tuple[List[str], str, str]:
    """
    Prefer a child entry's explicit channel metadata. If the child has
    only a bare URL, inherit the parent channel metadata.
    """
    if inner is not None and inner.info_line:
        return (
            list(inner.metadata_lines),
            inner.info_line,
            inner.title,
        )

    if outer is not None:
        return (
            list(outer.metadata_lines),
            outer.info_line,
            outer.title,
        )

    return [], "#EXTINF:-1,Unknown stream", "Unknown stream"


@dataclass
class WorkItem:
    url: str
    depth: int = 0
    inherited_entry: Optional[M3UEntry] = None
    parent_url: str = ""
    role: str = "candidate"


class Dissector:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

        self.client = Client(
            timeout=args.timeout,
            max_bytes=args.max_bytes,
            max_total_bytes=args.max_total_bytes,
            max_runtime=args.max_runtime,
            max_requests=args.max_requests,
            user_agent=args.user_agent,
            insecure_tls=args.insecure_tls,
        )

        self.work_queue: queue.Queue = queue.Queue()

        self.lock = threading.RLock()

        self.visited: Set[str] = set()
        self.queued_count = 0

        self.records: List[Dict[str, Any]] = []
        self.errors: List[Dict[str, Any]] = []
        self.events: List[Dict[str, Any]] = []
        self.validated: List[Dict[str, Any]] = []
        self.network_reports: List[Dict[str, Any]] = []

        self.network_diagnostics_done: Set[str] = set()

        self.stop_event = threading.Event()
        self.finished_workers = 0

    def log_event(
        self,
        kind: str,
        url: str,
        message: str,
        **extra: Any,
    ) -> None:
        with self.lock:
            self.events.append({
                "time": now_iso(),
                "kind": kind,
                "url": redact_url(url),
                "message": message,
                **extra,
            })

    def add_error(
        self,
        url: str,
        stage: str,
        message: str,
    ) -> None:
        with self.lock:
            self.errors.append({
                "time": now_iso(),
                "url": redact_url(url),
                "stage": stage,
                "message": message,
            })

    def enqueue(
        self,
        item: WorkItem,
    ) -> bool:
        url = normalize_url(item.url)

        if not url:
            return False

        item.url = url

        with self.lock:
            if url in self.visited:
                self.events.append({
                    "time": now_iso(),
                    "kind": "duplicate_or_cycle",
                    "url": redact_url(url),
                    "message": "URL already visited; skipped",
                    "depth": item.depth,
                })
                return False

            if self.queued_count >= self.args.max_sources:
                self.events.append({
                    "time": now_iso(),
                    "kind": "source_limit",
                    "url": redact_url(url),
                    "message": "Maximum URL count reached",
                    "depth": item.depth,
                })
                return False

            if item.depth > self.args.max_depth:
                self.events.append({
                    "time": now_iso(),
                    "kind": "depth_limit",
                    "url": redact_url(url),
                    "message": "Maximum traversal depth reached",
                    "depth": item.depth,
                })
                return False

            self.visited.add(url)
            self.queued_count += 1

        self.work_queue.put(item)
        return True

    def maybe_network_diagnostics(self, url: str) -> None:
        if not self.args.network_diagnostics:
            return

        with self.lock:
            if url in self.network_diagnostics_done:
                return
            self.network_diagnostics_done.add(url)

        try:
            result = network_diagnostics(
                url,
                timeout=self.args.timeout,
                insecure_tls=self.args.insecure_tls,
            )
        except Exception as exc:
            result = {
                "url": redact_url(url),
                "error": "{}: {}".format(type(exc).__name__, exc),
            }

        with self.lock:
            self.network_reports.append(result)

    def request_candidate(
        self,
        url: str,
        likely_playlist: bool,
    ) -> HttpResult:
        headers: Dict[str, str] = {}

        if not likely_playlist:
            # Many IPTV servers support Range requests. Servers that
            # ignore Range may still return 200; Client reads only the
            # configured small probe limit.
            headers["Range"] = "bytes=0-4095"

        first = self.client.request(
            url,
            method="GET",
            headers=headers,
            read_limit=(
                self.args.max_bytes
                if likely_playlist
                else self.args.probe_bytes
            ),
        )

        if (
            first.ok
            and looks_like_playlist(url, first)
            and first.truncated
            and self.args.refetch_truncated_playlists
        ):
            self.log_event(
                "playlist_refetch",
                url,
                "Playlist probe was truncated; attempting full fetch",
            )

            second = self.client.request(
                first.final_url or url,
                method="GET",
                headers={},
                read_limit=self.args.max_bytes,
            )

            if second.status is not None:
                return second

        return first

    def process_item(self, item: WorkItem) -> None:
        url = item.url

        if self.stop_event.is_set():
            return

        if self.client.remaining_runtime() <= 0:
            self.add_error(url, "budget", "Runtime limit reached")
            return

        self.maybe_network_diagnostics(url)

        likely_playlist = is_playlist_url(url)

        result = self.request_candidate(
            url,
            likely_playlist=likely_playlist,
        )

        record: Dict[str, Any] = {
            "url": redact_url(url),
            "parent_url": redact_url(item.parent_url),
            "depth": item.depth,
            "role": item.role,
            "status": result.status,
            "final_url": redact_url(
                result.final_url or url
            ),
            "content_type": result.content_type,
            "bytes_read": result.bytes_read,
            "truncated": result.truncated,
            "elapsed_seconds": round(
                result.elapsed_seconds, 3
            ),
            "http_ok": result.ok,
            "error": result.error,
            "classification": "unclassified",
            "decision": "not_emitted",
        }

        with self.lock:
            self.records.append(record)

        if not result.ok:
            self.add_error(
                url,
                "http",
                result.error
                or "HTTP status {}".format(result.status),
            )
            record["classification"] = "http_failure"
            return

        final_url = normalize_url(result.final_url or url)

        if looks_like_playlist(final_url, result):
            self.process_playlist(
                item=item,
                result=result,
                final_url=final_url,
                record=record,
            )
            return

        accepted, reason = direct_stream_acceptable(result)

        record["classification"] = (
            "direct_stream" if accepted else "not_confirmed_stream"
        )
        record["stream_check"] = reason

        if accepted:
            self.emit_validated(
                url=final_url,
                entry=item.inherited_entry,
                source=item.parent_url or url,
                validation={
                    "type": "direct_http_probe",
                    "status": result.status,
                    "content_type": result.content_type,
                    "bytes_read": result.bytes_read,
                    "reason": reason,
                },
            )
            record["decision"] = "emitted"
        else:
            self.add_error(
                url,
                "stream_validation",
                reason,
            )

    def process_playlist(
        self,
        item: WorkItem,
        result: HttpResult,
        final_url: str,
        record: Dict[str, Any],
    ) -> None:
        text = result.text
        upper = text.lstrip().upper()

        if not upper.startswith("#EXTM3U"):
            # Index pages can be plain URL lists rather than M3U files.
            if (
                "INDEX.M3U" in final_url.upper()
                or "PLAYLISTS" in final_url.upper()
            ):
                children = extract_playlist_urls(
                    text, final_url
                )

                record["classification"] = "playlist_index"
                record["child_count"] = len(children)

                for child in children:
                    self.enqueue(
                        WorkItem(
                            url=child,
                            depth=item.depth + 1,
                            inherited_entry=item.inherited_entry,
                            parent_url=final_url,
                            role="index_child",
                        )
                    )

                if not children:
                    self.add_error(
                        final_url,
                        "index_parse",
                        "No child playlist URLs found in index",
                    )

                return

            record["classification"] = "not_m3u"
            self.add_error(
                final_url,
                "playlist_parse",
                "URL looks like a playlist, but body is not #EXTM3U",
            )
            return

        hls = parse_hls(text, final_url)

        record["hls"] = {
            "is_master": hls["is_master"],
            "is_media": hls["is_media"],
            "has_extinf": hls["has_extinf"],
            "has_target_duration": hls["has_target_duration"],
            "has_endlist": hls["has_endlist"],
            "has_media_sequence": hls["has_media_sequence"],
            "variant_count": len(hls["variants"]),
            "rendition_count": len(hls["renditions"]),
            "segment_count": len(hls["segments"]),
            "warnings": hls["warnings"],
        }

        # HLS master playlist: variants are child playlists.
        if hls["is_master"] and hls["variants"]:
            record["classification"] = "hls_master"
            record["child_count"] = len(hls["variants"])

            variants = list(hls["variants"])

            if self.args.hls_variant_policy == "highest":
                variants.sort(
                    key=lambda v: v.get("bandwidth", 0),
                    reverse=True,
                )
                variants = variants[:1]
            elif self.args.hls_variant_policy == "lowest":
                variants.sort(
                    key=lambda v: v.get("bandwidth", 0)
                )
                variants = variants[:1]
            # "all" traverses all variants.

            for variant in variants:
                self.enqueue(
                    WorkItem(
                        url=variant["url"],
                        depth=item.depth + 1,
                        inherited_entry=item.inherited_entry,
                        parent_url=final_url,
                        role="hls_variant",
                    )
                )

            # Alternate renditions may be audio-only media playlists.
            if self.args.include_hls_renditions:
                for rendition in hls["renditions"]:
                    rendition_url = rendition.get("url", "")

                    if rendition_url:
                        self.enqueue(
                            WorkItem(
                                url=rendition_url,
                                depth=item.depth + 1,
                                inherited_entry=item.inherited_entry,
                                parent_url=final_url,
                                role="hls_rendition",
                            )
                        )

            return

        # HLS media playlist: validate its structure and test one
        # referenced segment. The segment itself is never emitted.
        if hls["is_media"]:
            record["classification"] = "hls_media_playlist"

            if not hls["segments"]:
                self.add_error(
                    final_url,
                    "hls_validation",
                    "No segment URLs found",
                )
                return

            segment_url = hls["segments"][0]
            segment_result: Optional[HttpResult] = None

            if self.args.check_hls_segment:
                segment_result = self.client.request(
                    segment_url,
                    method="GET",
                    headers={"Range": "bytes=0-1023"},
                    read_limit=self.args.segment_probe_bytes,
                )

                segment_ok = (
                    segment_result.ok
                    and bool(segment_result.body)
                    and not is_html_error_page(segment_result)
                )

                record["hls"]["first_segment_probe"] = {
                    "url": redact_url(segment_url),
                    "status": segment_result.status,
                    "content_type": segment_result.content_type,
                    "bytes_read": segment_result.bytes_read,
                    "ok": segment_ok,
                    "error": segment_result.error,
                }

                if not segment_ok:
                    self.add_error(
                        segment_url,
                        "hls_segment_probe",
                        segment_result.error
                        or "First HLS segment probe failed",
                    )
                    return

            self.emit_validated(
                url=final_url,
                entry=item.inherited_entry,
                source=item.parent_url or final_url,
                validation={
                    "type": "hls_media_playlist",
                    "status": result.status,
                    "segment_count": len(hls["segments"]),
                    "first_segment_checked": bool(
                        self.args.check_hls_segment
                    ),
                    "first_segment_ok": (
                        True
                        if not self.args.check_hls_segment
                        else bool(
                            segment_result
                            and segment_result.ok
                            and segment_result.body
                            and not is_html_error_page(
                                segment_result
                            )
                        )
                    ),
                    "warning": (
                        "HTTP/HLS structure check; no full decode performed"
                    ),
                },
            )

            record["decision"] = "emitted"
            return

        # Generic M3U playlist: walk channel entries recursively.
        entries = parse_m3u_entries(text, final_url)

        if entries:
            record["classification"] = "nested_m3u"
            record["child_count"] = len(entries)

            for entry in entries:
                inherited = entry

                if (
                    item.inherited_entry is not None
                    and not entry.title
                ):
                    metadata, info, title = merge_metadata(
                        item.inherited_entry,
                        entry,
                    )
                    inherited = M3UEntry(
                        metadata_lines=metadata,
                        info_line=info,
                        title=title,
                        url=entry.url,
                    )

                self.enqueue(
                    WorkItem(
                        url=entry.url,
                        depth=item.depth + 1,
                        inherited_entry=inherited,
                        parent_url=final_url,
                        role="m3u_entry",
                    )
                )

            return

        # Some indexes contain URLs but still start with #EXTM3U.
        # If no EXTINF records exist, extract playlist references.
        children = extract_playlist_urls(text, final_url)

        if children:
            record["classification"] = "m3u_index"
            record["child_count"] = len(children)

            for child in children:
                self.enqueue(
                    WorkItem(
                        url=child,
                        depth=item.depth + 1,
                        inherited_entry=item.inherited_entry,
                        parent_url=final_url,
                        role="m3u_index_child",
                    )
                )
            return

        record["classification"] = "empty_or_unsupported_playlist"
        self.add_error(
            final_url,
            "playlist_parse",
            "No M3U entries, HLS variants, or media segments found",
        )

    def emit_validated(
        self,
        url: str,
        entry: Optional[M3UEntry],
        source: str,
        validation: Dict[str, Any],
    ) -> None:
        url = normalize_url(url)

        if not url:
            return

        if entry is None:
            metadata_lines: List[str] = []
            info_line = "#EXTINF:-1,{}".format(
                urllib.parse.urlsplit(url).hostname or "Validated stream"
            )
            title = info_line.split(",", 1)[1]
        else:
            metadata_lines = list(entry.metadata_lines)
            info_line = entry.info_line or "#EXTINF:-1,Validated stream"
            title = entry.title

        with self.lock:
            # Preserve one playlist entry per terminal URL. This avoids
            # duplicate variants from overlapping parent playlists.
            for existing in self.validated:
                if existing["url"] == url:
                    existing.setdefault("sources", []).append(
                        redact_url(source)
                    )
                    return

            self.validated.append({
                "url": url,
                "metadata_lines": metadata_lines,
                "info_line": info_line,
                "title": title,
                "source": redact_url(source),
                "sources": [redact_url(source)],
                "validation": validation,
                "validated_at": now_iso(),
            })

    def worker(self) -> None:
        while True:
            try:
                item = self.work_queue.get(timeout=0.25)
            except queue.Empty:
                if self.stop_event.is_set():
                    return

                if self.client.remaining_runtime() <= 0:
                    self.stop_event.set()
                    return

                continue

            try:
                if self.client.remaining_runtime() <= 0:
                    self.add_error(
                        item.url,
                        "budget",
                        "Runtime limit reached before processing URL",
                    )
                    self.stop_event.set()
                else:
                    self.process_item(item)
            except Exception as exc:
                self.add_error(
                    item.url,
                    "internal",
                    "{}: {}".format(type(exc).__name__, exc),
                )
            finally:
                self.work_queue.task_done()

    def run(self, roots: List[str]) -> None:
        for root in roots:
            self.enqueue(
                WorkItem(
                    url=root,
                    depth=0,
                    inherited_entry=None,
                    parent_url="",
                    role="root",
                )
            )

        if self.args.discover_livem3u:
            self.enqueue(
                WorkItem(
                    url=LIVE_M3U_INDEX,
                    depth=0,
                    role="livem3u_index",
                )
            )
            self.enqueue(
                WorkItem(
                    url=LIVE_M3U_MAIN,
                    depth=0,
                    role="livem3u_main",
                )
            )

        worker_count = max(1, min(self.args.workers, 64))

        threads: List[threading.Thread] = []

        for index in range(worker_count):
            thread = threading.Thread(
                target=self.worker,
                name="PhoenixWorker-{}".format(index + 1),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        try:
            # queue.join() waits for every item, including child URLs
            # enqueued by active workers.
            while True:
                if self.client.remaining_runtime() <= 0:
                    self.stop_event.set()
                    break

                if self.work_queue.unfinished_tasks == 0:
                    break

                time.sleep(0.1)

            self.work_queue.join()

        except KeyboardInterrupt:
            self.stop_event.set()
            self.add_error(
                "",
                "runtime",
                "Interrupted by user",
            )

        finally:
            self.stop_event.set()

            for thread in threads:
                thread.join(timeout=1.0)

    def report(self) -> Dict[str, Any]:
        with self.lock:
            classification_counts = Counter(
                item.get("classification", "unknown")
                for item in self.records
            )

            return {
                "program": "Phoenix URL Dissector",
                "version": VERSION,
                "created_at": now_iso(),
                "settings": {
                    "timeout": self.args.timeout,
                    "max_bytes": self.args.max_bytes,
                    "max_total_bytes": self.args.max_total_bytes,
                    "max_runtime": self.args.max_runtime,
                    "max_requests": self.args.max_requests,
                    "max_depth": self.args.max_depth,
                    "max_sources": self.args.max_sources,
                    "workers": self.args.workers,
                    "probe_bytes": self.args.probe_bytes,
                    "check_hls_segment": self.args.check_hls_segment,
                    "hls_variant_policy": self.args.hls_variant_policy,
                },
                "budget": self.client.snapshot(),
                "summary": {
                    "urls_seen": len(self.visited),
                    "http_records": len(self.records),
                    "validated_terminal_streams": len(self.validated),
                    "errors": len(self.errors),
                    "classifications": dict(classification_counts),
                },
                "validated_streams": [
                    {
                        **item,
                        "url": redact_url(item["url"]),
                    }
                    for item in self.validated
                ],
                "records": self.records,
                "errors": self.errors,
                "events": self.events,
                "network_diagnostics": self.network_reports,
            }


def load_input_urls(args: argparse.Namespace) -> List[str]:
    urls: List[str] = []

    if args.url:
        urls.extend(args.url)

    if args.input_file:
        path = Path(args.input_file)

        if not path.exists():
            raise FileNotFoundError(
                "Input file does not exist: {}".format(path)
            )

        for raw_line in path.read_text(
            encoding="utf-8-sig",
            errors="replace",
        ).splitlines():
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            urls.append(line)

    if args.stdin:
        for raw_line in sys.stdin:
            line = raw_line.strip()

            if line and not line.startswith("#"):
                urls.append(line)

    unique: List[str] = []
    seen: Set[str] = set()

    for url in urls:
        normalized = normalize_url(url)

        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)

    return unique


def build_m3u(
    validated: List[Dict[str, Any]],
    include_source_comments: bool = False,
) -> str:
    lines = [
        "#EXTM3U",
        "# Phoenix URL Dissector {}".format(VERSION),
        "# Only validated terminal endpoints are included",
    ]

    for item in validated:
        if include_source_comments:
            lines.append(
                "# Phoenix-source: {}".format(
                    redact_url(item.get("source", ""))
                )
            )

        for metadata in item.get("metadata_lines", []):
            if metadata and metadata.startswith("#"):
                # Do not reinsert an accidental M3U header.
                if metadata.upper() != "#EXTM3U":
                    lines.append(metadata)

        info_line = (
            item.get("info_line")
            or "#EXTINF:-1,Validated stream"
        )

        lines.append(info_line)
        lines.append(item["url"])

    return "\n".join(lines) + "\n"


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)


def write_outputs(
    args: argparse.Namespace,
    dissector: Dissector,
) -> Dict[str, str]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    report = dissector.report()

    safe_name = re.sub(
        r"[^A-Za-z0-9_.-]+",
        "_",
        args.name.strip() or "phoenix_report",
    )

    json_path = output_dir / (safe_name + ".json")
    txt_path = output_dir / (safe_name + ".txt")
    m3u_path = output_dir / "validated_streams.m3u"

    write_text(
        json_path,
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
    )

    summary = report["summary"]
    budget = report["budget"]

    lines = [
        "Phoenix URL Dissector {}".format(VERSION),
        "=" * 72,
        "Created: {}".format(report["created_at"]),
        "",
        "SUMMARY",
        "-" * 72,
        "URLs seen: {}".format(summary["urls_seen"]),
        "HTTP records: {}".format(summary["http_records"]),
        "Validated terminal streams: {}".format(
            summary["validated_terminal_streams"]
        ),
        "Errors: {}".format(summary["errors"]),
        "",
        "CLASSIFICATIONS",
        "-" * 72,
    ]

    for key, value in sorted(
        summary["classifications"].items()
    ):
        lines.append("{}: {}".format(key, value))

    lines.extend([
        "",
        "HTTP BUDGET",
        "-" * 72,
        "HTTP requests: {}/{}".format(
            budget["http_requests"],
            budget["max_requests"],
        ),
        "Bytes read: {}/{}".format(
            budget["bytes_read"],
            budget["max_total_bytes"],
        ),
        "Elapsed seconds: {}".format(
            budget["elapsed_seconds"]
        ),
        "Runtime limit: {}".format(
            budget["max_runtime_seconds"]
        ),
        "",
        "VALIDATED TERMINAL STREAMS",
        "-" * 72,
    ])

    if dissector.validated:
        for index, item in enumerate(
            dissector.validated, 1
        ):
            lines.append(
                "{}. {}".format(
                    index,
                    redact_url(item["url"]),
                )
            )
            lines.append(
                "   Name: {}".format(
                    item.get("title", "")
                )
            )
            lines.append(
                "   Validation: {}".format(
                    item.get("validation", {}).get("type", "")
                )
            )
    else:
        lines.append(
            "No terminal stream passed the configured validation."
        )

    lines.extend([
        "",
        "ERRORS",
        "-" * 72,
    ])

    if dissector.errors:
        for error in dissector.errors:
            lines.append(
                "[{}] {} | {} | {}".format(
                    error.get("stage", ""),
                    error.get("url", ""),
                    error.get("message", ""),
                    error.get("time", ""),
                )
            )
    else:
        lines.append("No errors recorded.")

    lines.extend([
        "",
        "BUDGET EVENTS",
        "-" * 72,
    ])

    if budget["budget_events"]:
        lines.extend(budget["budget_events"])
    else:
        lines.append("No budget limit reached.")

    lines.extend([
        "",
        "LIMITATIONS",
        "-" * 72,
        "HTTP success is not equivalent to successful video decoding.",
        "HLS media playlists are structurally checked; optionally the",
        "first referenced segment is probed. No full media decode is",
        "performed by this standard-library-only script.",
        "DNS/TCP/TLS checks, when enabled, are separate socket operations",
        "and are not counted as HTTP requests.",
    ])

    write_text(txt_path, "\n".join(lines) + "\n")

    write_text(
        m3u_path,
        build_m3u(
            dissector.validated,
            include_source_comments=args.m3u_source_comments,
        ),
    )

    return {
        "json": str(json_path),
        "txt": str(txt_path),
        "m3u": str(m3u_path),
    }


def write_github_actions_workflow(
    path: str = ".github/workflows/phoenix-dissector.yml",
) -> str:
    """
    Generate a minimal scheduled/manual GitHub Actions workflow.
    The workflow assumes the Python script is named
    URL_Dessector.py in the repository root.
    """
    content = r"""name: Phoenix URL Dissector

on:
  workflow_dispatch:
  schedule:
    - cron: "17 */6 * * *"

permissions:
  contents: write

jobs:
  diagnose:
    runs-on: ubuntu-latest
    timeout-minutes: 20

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          persist-credentials: true

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Run Phoenix URL Dissector
        env:
          PYTHONUNBUFFERED: "1"
        run: |
          python -u URL_Dessector.py \
            --input-file sources.txt \
            --discover-livem3u \
            --workers 16 \
            --max-requests 500 \
            --max-total-bytes 40000000 \
            --max-runtime 900 \
            --output-dir reports \
            --name phoenix_report

      - name: Commit generated reports
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add reports/
          if ! git diff --cached --quiet; then
            git commit -m "Update Phoenix URL Dissector reports"
            git push
          fi
"""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_text(output, content)
    return str(output)


def self_test() -> int:
    sample = """#EXTM3U
#EXTINF:-1 tvg-id="demo" tvg-name="Demo" group-title="Test",Demo channel
#EXTVLCOPT:http-user-agent=TestAgent
https://example.com/live.m3u8
"""
    entries = parse_m3u_entries(
        sample,
        "https://example.com/root.m3u",
    )

    assert len(entries) == 1
    assert entries[0].title == "Demo channel"
    assert 'tvg-id="demo"' in entries[0].info_line
    assert entries[0].metadata_lines == [
        "#EXTVLCOPT:http-user-agent=TestAgent"
    ]

    hls_sample = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXTINF:10.0,
segment001.ts
#EXTINF:10.0,
segment002.ts
#EXT-X-ENDLIST
"""
    hls = parse_hls(
        hls_sample,
        "https://example.com/stream/index.m3u8",
    )

    assert hls["is_media"]
    assert len(hls["segments"]) == 2
    assert (
        hls["segments"][0]
        == "https://example.com/stream/segment001.ts"
    )

    master_sample = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=1200000,RESOLUTION=1280x720
720p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=640x360
360p/index.m3u8
"""
    master = parse_hls(
        master_sample,
        "https://example.com/master.m3u8",
    )

    assert master["is_master"]
    assert len(master["variants"]) == 2

    assert normalize_url(
        "HTTPS://Example.COM:443/test.m3u8"
    ) == "https://example.com/test.m3u8"

    print("Self-test passed.")
    return 0


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Phoenix URL Dissector {}: recursive M3U/HLS "
            "diagnostics and validated terminal playlist builder."
        ).format(VERSION)
    )

    parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="Root URL; may be supplied more than once",
    )

    parser.add_argument(
        "--input-file",
        default="",
        help="Text file containing one URL per line",
    )

    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read root URLs from stdin",
    )

    parser.add_argument(
        "--mode",
        choices=("quick", "deep", "hls"),
        default="deep",
        help=(
            "quick: shallow; deep: recursive M3U/HLS; "
            "hls: deep with HLS segment probe"
        ),
    )

    parser.add_argument(
        "--discover-livem3u",
        action="store_true",
        help="Add LiveM3U INDEX.m3u and playlist.m3u as roots",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=12.0,
        help="Per-request timeout in seconds",
    )

    parser.add_argument(
        "--max-bytes",
        type=int,
        default=2_000_000,
        help="Maximum body bytes reserved for one HTTP request",
    )

    parser.add_argument(
        "--max-total-bytes",
        type=int,
        default=40_000_000,
        help="Global aggregate HTTP response-body byte budget",
    )

    parser.add_argument(
        "--max-runtime",
        type=float,
        default=300.0,
        help="Maximum total runtime in seconds",
    )

    parser.add_argument(
        "--max-requests",
        type=int,
        default=500,
        help="Global HTTP request budget",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=8,
        help="Maximum child traversal depth",
    )

    parser.add_argument(
        "--max-sources",
        type=int,
        default=1500,
        help="Maximum distinct URLs enqueued",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Concurrent workers (1-64)",
    )

    parser.add_argument(
        "--probe-bytes",
        type=int,
        default=4096,
        help="Response bytes used for a direct-stream probe",
    )

    parser.add_argument(
        "--segment-probe-bytes",
        type=int,
        default=1024,
        help="Maximum body bytes read from the first HLS segment",
    )

    parser.add_argument(
        "--max-redirects",
        type=int,
        default=10,
        help="Compatibility option; urllib handles redirects",
    )

    parser.add_argument(
        "--max-children",
        type=int,
        default=1000,
        help="Compatibility option; --max-sources is the global URL cap",
    )

    parser.add_argument(
        "--max-segments",
        type=int,
        default=200,
        help="Compatibility option for report consumers",
    )

    parser.add_argument(
        "--max-total-requests",
        type=int,
        default=0,
        help=argparse.SUPPRESS,
    )

    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent",
    )

    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Directory for report files and generated playlist",
    )

    parser.add_argument(
        "--name",
        default="phoenix_report",
        help="Report file base name",
    )

    parser.add_argument(
        "--allow-private",
        action="store_true",
        help=(
            "Compatibility flag; private HTTP(S) targets are not "
            "blocked by default in this version"
        ),
    )

    parser.add_argument(
        "--insecure-tls",
        action="store_true",
        help="Disable TLS certificate verification",
    )

    parser.add_argument(
        "--network-diagnostics",
        action="store_true",
        help="Run separate DNS/TCP/TLS diagnostics",
    )

    parser.add_argument(
        "--check-hls-segment",
        action="store_true",
        help="Probe the first HLS segment before accepting media playlist",
    )

    parser.add_argument(
        "--refetch-truncated-playlists",
        action="store_true",
        help="Retry a truncated playlist with the full per-request cap",
    )

    parser.add_argument(
        "--hls-variant-policy",
        choices=("all", "highest", "lowest"),
        default="all",
        help="HLS master variant policy",
    )

    parser.add_argument(
        "--include-hls-renditions",
        action="store_true",
        help="Also traverse alternate HLS audio/subtitle renditions",
    )

    parser.add_argument(
        "--m3u-source-comments",
        action="store_true",
        help="Write source comments into the generated M3U",
    )

    parser.add_argument(
        "--init-actions",
        action="store_true",
        help="Generate a GitHub Actions workflow",
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run offline parser tests and exit",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    if args.mode == "quick":
        args.max_depth = min(args.max_depth, 1)
        args.check_hls_segment = False

    if args.mode == "hls":
        args.check_hls_segment = True

    if args.max_total_requests > 0:
        args.max_requests = min(
            args.max_requests,
            args.max_total_requests,
        )

    if args.max_bytes <= 0:
        parser.error("--max-bytes must be greater than zero")

    if args.max_total_bytes <= 0:
        parser.error("--max-total-bytes must be greater than zero")

    if args.max_requests <= 0:
        parser.error("--max-requests must be greater than zero")

    if args.max_runtime <= 0:
        parser.error("--max-runtime must be greater than zero")

    if args.max_depth < 0:
        parser.error("--max-depth cannot be negative")

    if args.max_sources <= 0:
        parser.error("--max-sources must be greater than zero")

    if args.init_actions:
        path = write_github_actions_workflow()
        print("GitHub Actions workflow generated:", path)

        if not (
            args.url
            or args.input_file
            or args.stdin
            or args.discover_livem3u
        ):
            return 0

    try:
        roots = load_input_urls(args)
    except Exception as exc:
        print(
            "Input error: {}: {}".format(type(exc).__name__, exc),
            file=sys.stderr,
        )
        return 2

    if not roots and not args.discover_livem3u:
        parser.error(
            "Provide --url, --input-file, --stdin, "
            "or --discover-livem3u"
        )

    print("=" * 72)
    print("Phoenix URL Dissector", VERSION)
    print("=" * 72)
    print("Root URLs:", len(roots))
    print("Workers:", max(1, min(args.workers, 64)))
    print("HTTP request budget:", args.max_requests)
    print("Aggregate byte budget:", args.max_total_bytes)
    print("Runtime limit:", args.max_runtime, "seconds")
    print("Maximum depth:", args.max_depth)
    print("Output directory:", args.output_dir)
    print()

    dissector = Dissector(args)
    started = time.monotonic()

    try:
        dissector.run(roots)
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
    finally:
        try:
            outputs = write_outputs(args, dissector)
        except Exception as exc:
            print(
                "Output error: {}: {}".format(type(exc).__name__, exc),
                file=sys.stderr,
            )
            return 3

    elapsed = time.monotonic() - started
    report = dissector.report()

    print()
    print("=" * 72)
    print("FINISHED")
    print("=" * 72)
    print("Elapsed:", round(elapsed, 2), "seconds")
    print("URLs seen:", report["summary"]["urls_seen"])
    print("HTTP requests:", report["budget"]["http_requests"])
    print("Bytes read:", report["budget"]["bytes_read"])
    print(
        "Validated terminal streams:",
        report["summary"]["validated_terminal_streams"],
    )
    print("Errors:", report["summary"]["errors"])
    print()
    print("JSON report:", outputs["json"])
    print("TXT report:", outputs["txt"])
    print("Validated playlist:", outputs["m3u"])

    if report["summary"]["validated_terminal_streams"] == 0:
        print()
        print(
            "WARNING: no terminal streams passed the configured "
            "checks. The output playlist contains only its header."
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
