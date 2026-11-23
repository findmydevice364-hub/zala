#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phoenix URL Dissector
Version: 3.2.3-pre.3.3.5-beta.0

Назначение:
  1. Загрузить исходные LiveM3U-плейлисты.
  2. Найти обычные M3U, HLS master/media и вложенные ресурсы.
  3. Выявить скрытые Tatnet HLS по диагностическому ответу HTTP 400.
  4. Повторить запрос с Origin / X-Requested-With при подтверждённой
     сигнатуре обязательного заголовка.
  5. Сохранить каждый запрос и ответ как отдельное событие.
  6. Построить граф parent -> child.
  7. После этапа обнаружения независимо проверить найденные ресурсы.
  8. При наличии ffprobe проверить ограниченное число медиасегментов.
  9. Создать reports/Verifiziert_tv.m3u.
 10. Создать JSONL-журнал, JSON-отчёт и текстовую сводку.

Python 3.9+
Зависимости: стандартная библиотека Python.
ffprobe необязателен.

Важно:
  HTTP 200 не доказывает, что ресурс является рабочим видеопотоком.
  HTTP 400 с сигнатурой Tatnet не означает, что источник не работает.
  Отказ ECONNREFUSED фиксируется отдельно от обычного HTTP 404.
  Код не обходит DRM, авторизацию или другие средства защиты.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import hashlib
import ipaddress
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


VERSION = "3.2.3-pre.3.3.5-beta.0"

ROOT_SOURCES = [
    "https://livem3u.tatnet.app/data/playlists/INDEX.m3u",
    "https://livem3u.tatnet.app/data/playlist.m3u",
]

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 12) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Mobile Safari/537.36"
)

HEADER_REQUIRED_RE = re.compile(
    r"Missing required request header.*"
    r"(origin\s*,?\s*x-requested-with|x-requested-with\s*,?\s*origin)",
    re.IGNORECASE | re.DOTALL,
)

HLS_TAG_RE = re.compile(r"^\s*#EXT-X-", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)

PLAYLIST_EXTENSIONS = (
    ".m3u",
    ".m3u8",
    ".m3u? ",
    ".mpd",
)

SEGMENT_EXTENSIONS = (
    ".ts",
    ".m4s",
    ".mp4",
    ".m4v",
    ".aac",
    ".ac3",
    ".ec3",
    ".mp3",
    ".webvtt",
    ".vtt",
    ".cmfv",
    ".cmfa",
)

ERROR_STATUSES = {
    "HEADER_REQUIRED",
    "PROXY_UPSTREAM_REFUSED",
    "HTTP_ERROR",
    "INVALID_PLAYLIST",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()[:20]


def safe_url(url: str) -> str:
    """Маскирует логин/пароль в URL, если они присутствуют."""
    try:
        p = urllib.parse.urlsplit(url)
        host = p.hostname or ""
        if p.port:
            host = f"{host}:{p.port}"
        if p.username or p.password:
            host = "***:***@" + host
        return urllib.parse.urlunsplit(
            (p.scheme, host, p.path, p.query, p.fragment)
        )
    except Exception:
        return url


def normalized_url(url: str) -> str:
    """
    Нормализует только безопасные структурные элементы URL.
    Query-параметры и path не удаляются: они могут быть частью токена
    или маршрута источника.
    """
    url = url.strip()
    p = urllib.parse.urlsplit(url)
    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
    return urllib.parse.urlunsplit(
        (scheme, netloc, p.path, p.query, "")
    )


def is_http_url(value: str) -> bool:
    try:
        p = urllib.parse.urlsplit(value)
        return p.scheme.lower() in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def resolve_url(parent: str, child: str) -> str:
    return urllib.parse.urljoin(parent, child.strip())


def content_type_is_playlist(content_type: str) -> bool:
    value = (content_type or "").lower()
    return any(
        x in value
        for x in (
            "mpegurl",
            "vnd.apple.mpegurl",
            "application/x-mpegurl",
            "dash+xml",
        )
    )


def looks_like_playlist_url(url: str) -> bool:
    path = urllib.parse.urlsplit(url).path.lower()
    return path.endswith((".m3u", ".m3u8", ".mpd"))


def looks_like_segment_url(url: str) -> bool:
    path = urllib.parse.urlsplit(url).path.lower()
    return any(path.endswith(ext) for ext in SEGMENT_EXTENSIONS)


def is_hls_text(text: str) -> bool:
    sample = text.lstrip("\ufeff \r\n\t")[:8192]
    return sample.startswith("#EXTM3U")


def is_dash_text(text: str) -> bool:
    return "<MPD" in text[:8192] or "<mpd" in text[:8192]


def header_required_signature(body: bytes) -> bool:
    text = body[:8192].decode("utf-8", "replace")
    return bool(HEADER_REQUIRED_RE.search(text))


def proxy_upstream_refused(body: bytes) -> bool:
    text = body[:8192].decode("utf-8", "replace")
    return (
        "ECONNREFUSED" in text
        or "connect ECONNREFUSED" in text
        or "Not found because of proxy error" in text
    )


def extract_origin_candidates(url: str, headers: Dict[str, str]) -> List[str]:
    """
    Собирает возможные Origin на основе наблюдаемых метаданных.
    Это только кандидаты. Их наличие не доказывает исходный Origin.
    """
    candidates: List[str] = []

    for key, value in headers.items():
        low_key = key.lower()
        if low_key in ("x-request-url", "x-cors-redirect-1", "x-final-url"):
            match = re.search(r"https?://[^\s]+", value, re.IGNORECASE)
            if match:
                parsed = urllib.parse.urlsplit(match.group(0))
                if parsed.scheme and parsed.netloc:
                    origin = f"{parsed.scheme}://{parsed.netloc}"
                    if origin not in candidates:
                        candidates.append(origin)

    try:
        p = urllib.parse.urlsplit(url)
        path = p.path.lstrip("/")
        first = path.split("/", 1)[0]
        if "." in first and not first.startswith("."):
            if "/" not in first and ":" not in first:
                origin = "https://" + first
                if origin not in candidates:
                    candidates.append(origin)
    except Exception:
        pass

    return candidates


def resolve_ip(hostname: str) -> List[str]:
    if not hostname:
        return []
    try:
        answers = socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)
        result = []
        for answer in answers:
            ip = answer[4][0]
            if ip not in result:
                result.append(ip)
        return result
    except Exception:
        return []


@dataclasses.dataclass
class HttpResponse:
    requested_url: str
    final_url: str
    status: Optional[int]
    reason: str
    headers: Dict[str, str]
    body: bytes
    elapsed_seconds: float
    redirect_chain: List[str]
    error: Optional[str] = None
    method: str = "GET"

    def to_record(self) -> Dict[str, Any]:
        parsed = urllib.parse.urlsplit(self.final_url)
        content_type = self.headers.get("content-type", "")
        return {
            "timestamp": utc_now(),
            "method": self.method,
            "requested_url": safe_url(self.requested_url),
            "final_url": safe_url(self.final_url),
            "status": self.status,
            "reason": self.reason,
            "headers": dict(self.headers),
            "content_type": content_type,
            "body_bytes": len(self.body),
            "elapsed_seconds": round(self.elapsed_seconds, 4),
            "redirect_chain": [safe_url(x) for x in self.redirect_chain],
            "hostname": parsed.hostname,
            "resolved_ips": resolve_ip(parsed.hostname or ""),
            "error": self.error,
        }


class RedirectRecorder(urllib.request.HTTPRedirectHandler):
    def __init__(self, max_redirects: int):
        super().__init__()
        self.max_redirects = max_redirects
        self.local = threading.local()

    def begin(self) -> None:
        self.local.chain = []
        self.local.count = 0

    def get_chain(self) -> List[str]:
        return list(getattr(self.local, "chain", []))

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        count = getattr(self.local, "count", 0) + 1
        self.local.count = count
        if count > self.max_redirects:
            raise urllib.error.HTTPError(
                req.full_url,
                code,
                "Maximum redirect count exceeded",
                headers,
                fp,
            )
        self.local.chain.append(newurl)
        return super().redirect_request(
            req, fp, code, msg, headers, newurl
        )


class HttpClient:
    def __init__(
        self,
        timeout: float,
        max_bytes: int,
        max_redirects: int,
        user_agent: str,
        insecure_tls: bool = False,
    ):
        self.timeout = timeout
        self.max_bytes = max_bytes
        self.max_redirects = max_redirects
        self.user_agent = user_agent
        self.insecure_tls = insecure_tls
        self.local = threading.local()

    def _opener(self):
        if not hasattr(self.local, "opener"):
            redirect = RedirectRecorder(self.max_redirects)
            handlers: List[Any] = [redirect]
            if self.insecure_tls:
                import ssl
                context = ssl._create_unverified_context()
                handlers.append(urllib.request.HTTPSHandler(context=context))
            self.local.redirect = redirect
            self.local.opener = urllib.request.build_opener(*handlers)
        return self.local.opener, self.local.redirect

    def request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
    ) -> HttpResponse:
        request_headers = {
            "User-Agent": self.user_agent,
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "close",
        }
        if headers:
            request_headers.update(headers)

        req = urllib.request.Request(
            url,
            headers=request_headers,
            method=method,
        )

        opener, redirect = self._opener()
        redirect.begin()
        started = time.monotonic()
        body = b""
        status: Optional[int] = None
        reason = ""
        response_headers: Dict[str, str] = {}
        final_url = url
        error: Optional[str] = None

        try:
            with opener.open(req, timeout=self.timeout) as response:
                status = getattr(response, "status", None)
                reason = getattr(response, "reason", "") or ""
                final_url = response.geturl()
                response_headers = {
                    k.lower(): v for k, v in response.headers.items()
                }
                if method.upper() != "HEAD":
                    if self.max_bytes > 0:
                        body = response.read(self.max_bytes + 1)
                        if len(body) > self.max_bytes:
                            body = body[:self.max_bytes]
                            error = "RESPONSE_TRUNCATED_MAX_BYTES"
                    else:
                        body = response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            reason = str(exc.reason or "")
            final_url = exc.geturl() or url
            response_headers = {
                k.lower(): v for k, v in (exc.headers.items() if exc.headers else [])
            }
            if method.upper() != "HEAD":
                try:
                    if self.max_bytes > 0:
                        body = exc.read(self.max_bytes + 1)
                        if len(body) > self.max_bytes:
                            body = body[:self.max_bytes]
                            error = "RESPONSE_TRUNCATED_MAX_BYTES"
                    else:
                        body = exc.read()
                except Exception:
                    body = b""
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            reason = str(exc)

        elapsed = time.monotonic() - started
        return HttpResponse(
            requested_url=url,
            final_url=final_url,
            status=status,
            reason=reason,
            headers=response_headers,
            body=body,
            elapsed_seconds=elapsed,
            redirect_chain=redirect.get_chain(),
            error=error,
            method=method.upper(),
        )


class PhoenixDissector:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.started_monotonic = time.monotonic()
        self.started_iso = utc_now()

        self.output_dir = Path(args.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.client = HttpClient(
            timeout=args.timeout,
            max_bytes=args.max_bytes,
            max_redirects=args.max_redirects,
            user_agent=args.user_agent,
            insecure_tls=args.insecure_tls,
        )

        self.lock = threading.RLock()
        self.queue = deque()
        self.queued: Set[str] = set()
        self.processed: Set[str] = set()
        self.records: Dict[str, Dict[str, Any]] = {}
        self.events: List[Dict[str, Any]] = []
        self.edges: List[Dict[str, Any]] = []
        self.source_entries: List[Dict[str, Any]] = []
        self.playlist_pool: List[Dict[str, Any]] = []
        self.validation_queue: List[str] = []
        self.visited_playlist_text: Dict[str, str] = {}

        self.request_count = 0
        self.total_bytes = 0
        self.segment_checks = 0
        self.started_sources: List[str] = []
        self.stop_reason: Optional[str] = None

    def event(self, event_type: str, **fields: Any) -> None:
        with self.lock:
            self.events.append({
                "timestamp": utc_now(),
                "event": event_type,
                **fields,
            })

    def limits_exceeded(self) -> bool:
        if self.args.max_runtime > 0:
            if time.monotonic() - self.started_monotonic >= self.args.max_runtime:
                self.stop_reason = "MAX_RUNTIME"
                return True
        if self.args.max_requests > 0 and self.request_count >= self.args.max_requests:
            self.stop_reason = "MAX_REQUESTS"
            return True
        if self.args.max_total_bytes > 0 and self.total_bytes >= self.args.max_total_bytes:
            self.stop_reason = "MAX_TOTAL_BYTES"
            return True
        return False

    def http_request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
    ) -> HttpResponse:
        if self.limits_exceeded():
            return HttpResponse(
                requested_url=url,
                final_url=url,
                status=None,
                reason="LIMIT_REACHED",
                headers={},
                body=b"",
                elapsed_seconds=0.0,
                redirect_chain=[],
                error=self.stop_reason,
                method=method,
            )

        with self.lock:
            self.request_count += 1

        response = self.client.request(url, method, headers)

        with self.lock:
            self.total_bytes += len(response.body)

        self.event(
            "HTTP_ATTEMPT",
            response=response.to_record(),
            request_headers=headers or {},
        )
        return response

    def add_edge(
        self,
        parent: Optional[str],
        child: str,
        relation: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        edge = {
            "timestamp": utc_now(),
            "parent": safe_url(parent) if parent else None,
            "child": safe_url(child),
            "relation": relation,
            "metadata": metadata or {},
        }
        self.edges.append(edge)

    def enqueue(
        self,
        url: str,
        parent: Optional[str] = None,
        relation: str = "DISCOVERED",
        depth: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        url = url.strip()
        if not is_http_url(url):
            return

        if self.args.max_depth > 0 and depth > self.args.max_depth:
            self.event(
                "DEPTH_LIMIT",
                url=safe_url(url),
                depth=depth,
                max_depth=self.args.max_depth,
            )
            return

        if self.args.max_sources > 0:
            known = len(self.queued) + len(self.processed)
            if known >= self.args.max_sources and normalized_url(url) not in self.queued:
                self.event(
                    "SOURCE_LIMIT",
                    url=safe_url(url),
                    max_sources=self.args.max_sources,
                )
                return

        key = normalized_url(url)
        self.add_edge(parent, url, relation, metadata)

        if key in self.queued or key in self.processed:
            self.event(
                "DUPLICATE_REFERENCE",
                url=safe_url(url),
                parent=safe_url(parent) if parent else None,
                relation=relation,
            )
            return

        self.queued.add(key)
        self.queue.append({
            "url": url,
            "depth": depth,
            "parent": parent,
            "relation": relation,
        })

    def _origin_header_attempts(
        self,
        url: str,
        first_response: HttpResponse,
    ) -> List[Tuple[str, Dict[str, str]]]:
        candidates = extract_origin_candidates(url, first_response.headers)
        attempts: List[Tuple[str, Dict[str, str]]] = []

        for origin in candidates:
            attempts.append(("ORIGIN_CANDIDATE", {"Origin": origin}))

        attempts.append((
            "X_REQUESTED_WITH",
            {"X-Requested-With": "XMLHttpRequest"},
        ))

        # Не повторяем идентичные заголовки.
        unique: List[Tuple[str, Dict[str, str]]] = []
        seen: Set[Tuple[Tuple[str, str], ...]] = set()
        for label, headers in attempts:
            signature = tuple(sorted(
                (k.lower(), v) for k, v in headers.items()
            ))
            if signature not in seen:
                seen.add(signature)
                unique.append((label, headers))
        return unique

    def diagnostic_get(self, url: str) -> Tuple[HttpResponse, List[Dict[str, Any]]]:
        """
        Этап 1: обычный GET.
        Этап 2: только если тело содержит известную сигнатуру Tatnet,
        повторные запросы с допустимыми заголовками.
        """
        attempts: List[Dict[str, Any]] = []
        response = self.http_request(url, "GET")
        attempts.append({
            "kind": "BASELINE_GET",
            "response": response.to_record(),
            "request_headers": {},
        })

        if header_required_signature(response.body):
            self.event(
                "TATNET_HEADER_REQUIRED_DETECTED",
                url=safe_url(url),
                status=response.status,
                body=response.body.decode("utf-8", "replace"),
            )

            for label, headers in self._origin_header_attempts(url, response):
                if self.limits_exceeded():
                    break

                retry = self.http_request(url, "GET", headers)
                attempts.append({
                    "kind": label,
                    "request_headers": headers,
                    "response": retry.to_record(),
                })

                if retry.status is not None and 200 <= retry.status < 300:
                    response = retry
                    self.event(
                        "TATNET_RETRY_SUCCEEDED",
                        url=safe_url(url),
                        method=label,
                        status=retry.status,
                        request_headers=headers,
                    )
                    break

                if proxy_upstream_refused(retry.body):
                    self.event(
                        "PROXY_UPSTREAM_REFUSED",
                        url=safe_url(url),
                        status=retry.status,
                        body=retry.body.decode("utf-8", "replace"),
                        request_headers=headers,
                    )

        return response, attempts

    def classify_response(
        self,
        response: HttpResponse,
        body_text: str = "",
    ) -> str:
        if header_required_signature(response.body):
            return "HEADER_REQUIRED"
        if proxy_upstream_refused(response.body):
            return "PROXY_UPSTREAM_REFUSED"
        if response.status is None:
            return "NETWORK_ERROR"
        if response.status < 200 or response.status >= 300:
            return "HTTP_ERROR"
        if is_hls_text(body_text):
            return "PLAYLIST_CANDIDATE"
        if is_dash_text(body_text):
            return "DASH_PLAYLIST_CANDIDATE"
        content_type = response.headers.get("content-type", "").lower()
        if "video/mp2t" in content_type:
            return "MEDIA_SEGMENT_OK"
        if "video/" in content_type or "audio/" in content_type:
            return "MEDIA_RESOURCE_CANDIDATE"
        if looks_like_segment_url(response.final_url):
            return "MEDIA_RESOURCE_CANDIDATE"
        return "HTTP_OK_UNCLASSIFIED"

    def parse_attribute_list(self, value: str) -> Dict[str, str]:
        """
        Разбирает атрибуты EXT-X-STREAM-INF и похожих HLS-тегов,
        учитывая запятые внутри кавычек.
        """
        result: Dict[str, str] = {}
        parts: List[str] = []
        buf: List[str] = []
        quoted = False

        for char in value:
            if char == '"':
                quoted = not quoted
                buf.append(char)
            elif char == "," and not quoted:
                parts.append("".join(buf).strip())
                buf = []
            else:
                buf.append(char)

        if buf:
            parts.append("".join(buf).strip())

        for part in parts:
            if "=" not in part:
                continue
            key, val = part.split("=", 1)
            result[key.strip().upper()] = val.strip().strip('"')

        return result

    def parse_m3u(
        self,
        url: str,
        text: str,
        depth: int,
    ) -> Dict[str, Any]:
        lines = text.lstrip("\ufeff").splitlines()
        if not lines or not lines[0].strip().startswith("#EXTM3U"):
            return {
                "valid": False,
                "kind": "INVALID",
                "children": [],
                "entries": [],
                "error": "Missing #EXTM3U header",
            }

        children: List[Dict[str, Any]] = []
        entries: List[Dict[str, Any]] = []
        pending_inf: Optional[str] = None
        pending_stream: Optional[Dict[str, str]] = None
        pending_duration: Optional[float] = None
        media_sequence: Optional[int] = None
        target_duration: Optional[float] = None
        playlist_version: Optional[int] = None
        endlist = False
        is_master = False
        is_media = False
        generic_extinf = False

        for raw_line in lines[1:]:
            line = raw_line.strip()
            if not line:
                continue

            if line.startswith("#EXT-X-STREAM-INF:"):
                is_master = True
                pending_stream = self.parse_attribute_list(
                    line.split(":", 1)[1]
                )
                pending_inf = None
                continue

            if line.startswith("#EXT-X-I-FRAME-STREAM-INF:"):
                is_master = True
                attrs = self.parse_attribute_list(line.split(":", 1)[1])
                uri = attrs.get("URI")
                if uri:
                    child_url = resolve_url(url, uri)
                    item = {
                        "url": child_url,
                        "type": "HLS_I_FRAME_VARIANT",
                        "attributes": attrs,
                    }
                    children.append(item)
                    entries.append(item)
                continue

            if line.startswith("#EXT-X-MEDIA:"):
                attrs = self.parse_attribute_list(line.split(":", 1)[1])
                uri = attrs.get("URI")
                if uri:
                    child_url = resolve_url(url, uri)
                    item = {
                        "url": child_url,
                        "type": "HLS_RENDITION",
                        "attributes": attrs,
                    }
                    children.append(item)
                    entries.append(item)
                continue

            if line.startswith("#EXT-X-KEY:") or line.startswith("#EXT-X-SESSION-KEY:"):
                # URI сохраняется как метаданные, но ключи не скачиваются.
                attrs = self.parse_attribute_list(line.split(":", 1)[1])
                if attrs.get("URI"):
                    entries.append({
                        "type": "HLS_KEY_REFERENCE",
                        "method": attrs.get("METHOD"),
                        "keyformat": attrs.get("KEYFORMAT"),
                        "uri": resolve_url(url, attrs["URI"]),
                    })
                continue

            if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                is_media = True
                try:
                    media_sequence = int(line.split(":", 1)[1].strip())
                except ValueError:
                    pass
                continue

            if line.startswith("#EXT-X-TARGETDURATION:"):
                is_media = True
                try:
                    target_duration = float(line.split(":", 1)[1].strip())
                except ValueError:
                    pass
                continue

            if line.startswith("#EXT-X-VERSION:"):
                try:
                    playlist_version = int(line.split(":", 1)[1].strip())
                except ValueError:
                    pass
                continue

            if line.startswith("#EXT-X-ENDLIST"):
                endlist = True
                is_media = True
                continue

            if line.startswith("#EXTINF:"):
                is_media = True
                generic_extinf = True
                value = line.split(":", 1)[1].split(",", 1)[0].strip()
                try:
                    pending_duration = float(value)
                except ValueError:
                    pending_duration = None
                pending_inf = line
                continue

            if line.startswith("#"):
                continue

            # Непустая строка, не являющаяся тегом.
            if not is_http_url(line):
                child_url = resolve_url(url, line)
            else:
                child_url = line

            if pending_stream is not None:
                item = {
                    "url": child_url,
                    "type": "HLS_VARIANT",
                    "attributes": dict(pending_stream),
                }
                children.append(item)
                entries.append(item)
                pending_stream = None
                pending_inf = None
                continue

            if pending_inf is not None or generic_extinf:
                item = {
                    "url": child_url,
                    "type": "HLS_SEGMENT",
                    "duration": pending_duration,
                    "extinf": pending_inf,
                }
                children.append(item)
                entries.append(item)
                pending_inf = None
                pending_duration = None
                generic_extinf = False
                continue

            # Для обычного M3U это может быть вложенный плейлист,
            # ссылка на поток или иной ресурс.
            kind = "M3U_ENTRY"
            if looks_like_playlist_url(child_url):
                kind = "PLAYLIST_REFERENCE"
            elif looks_like_segment_url(child_url):
                kind = "MEDIA_SEGMENT_REFERENCE"

            item = {
                "url": child_url,
                "type": kind,
                "attributes": {},
            }
            children.append(item)
            entries.append(item)

        if is_master:
            kind = "HLS_MASTER"
        elif is_media:
            kind = "HLS_MEDIA"
        else:
            kind = "M3U_GENERIC"

        return {
            "valid": True,
            "kind": kind,
            "children": children,
            "entries": entries,
            "metadata": {
                "version": playlist_version,
                "target_duration": target_duration,
                "media_sequence": media_sequence,
                "endlist": endlist,
                "variant_count": sum(
                    1 for x in entries
                    if x.get("type") in ("HLS_VARIANT", "HLS_I_FRAME_VARIANT")
                ),
                "segment_count": sum(
                    1 for x in entries if x.get("type") == "HLS_SEGMENT"
                ),
            },
        }

    def parse_dash(self, url: str, text: str) -> Dict[str, Any]:
        """
        Извлекает DASH BaseURL и SegmentURL.
        Шаблоны SegmentTemplate сохраняются как метаданные.
        Здесь не производится разворачивание бесконечных или динамических
        DASH-шаблонов в произвольное число запросов.
        """
        import xml.etree.ElementTree as ET

        result = {
            "valid": False,
            "kind": "DASH",
            "children": [],
            "entries": [],
            "metadata": {},
            "error": None,
        }

        try:
            root = ET.fromstring(text)
        except Exception as exc:
            result["error"] = f"DASH_XML_PARSE_ERROR: {exc}"
            return result

        if not root.tag.lower().endswith("mpd"):
            result["error"] = "XML root is not MPD"
            return result

        result["valid"] = True
        result["metadata"] = {
            "type": root.attrib.get("type"),
            "mediaPresentationDuration": root.attrib.get(
                "mediaPresentationDuration"
            ),
            "minBufferTime": root.attrib.get("minBufferTime"),
        }

        base_urls: List[str] = []
        templates: List[Dict[str, Any]] = []

        for element in root.iter():
            tag = element.tag.split("}")[-1]

            if tag == "BaseURL" and element.text:
                value = element.text.strip()
                if value:
                    base_urls.append(resolve_url(url, value))

            elif tag == "SegmentURL":
                media = element.attrib.get("media")
                if media:
                    result["children"].append({
                        "url": resolve_url(url, media),
                        "type": "DASH_SEGMENT",
                        "attributes": dict(element.attrib),
                    })

            elif tag == "SegmentTemplate":
                templates.append(dict(element.attrib))

        for base in base_urls:
            result["children"].append({
                "url": base,
                "type": "DASH_BASE_URL",
                "attributes": {},
            })

        result["metadata"]["segment_templates"] = templates
        result["metadata"]["base_urls"] = base_urls
        result["entries"] = list(result["children"])
        return result

    def record_for(self, url: str) -> Dict[str, Any]:
        key = normalized_url(url)
        if key not in self.records:
            self.records[key] = {
                "id": stable_hash(key),
                "url": url,
                "normalized_url": key,
                "first_seen": utc_now(),
                "parents": [],
                "attempts": [],
                "statuses": [],
                "discovery": {},
                "validation": {},
                "playlist_entries": [],
            }
        return self.records[key]

    def add_parent(self, record: Dict[str, Any], parent: Optional[str]) -> None:
        if not parent:
            return
        value = safe_url(parent)
        if value not in record["parents"]:
            record["parents"].append(value)

    def discover_one(self, item: Dict[str, Any]) -> None:
        url = item["url"]
        depth = item["depth"]
        parent = item["parent"]
        key = normalized_url(url)

        if key in self.processed:
            return

        self.processed.add(key)
        record = self.record_for(url)
        self.add_parent(record, parent)
        record["discovery"]["depth"] = depth
        record["discovery"]["relation"] = item["relation"]

        if self.limits_exceeded():
            record["statuses"].append(self.stop_reason or "LIMIT_REACHED")
            return

        response, attempts = self.diagnostic_get(url)
        record["attempts"].extend(attempts)

        body_text = response.body.decode("utf-8", "replace")
        status = self.classify_response(response, body_text)
        record["discovery"].update({
            "http_status": response.status,
            "final_url": safe_url(response.final_url),
            "content_type": response.headers.get("content-type", ""),
            "bytes": len(response.body),
            "elapsed_seconds": round(response.elapsed_seconds, 4),
            "redirect_chain": [safe_url(x) for x in response.redirect_chain],
            "classification": status,
            "error": response.error,
        })

        if status not in record["statuses"]:
            record["statuses"].append(status)

        if proxy_upstream_refused(response.body):
            record["discovery"]["proxy_error_body"] = body_text[:8192]
            record["statuses"].append("PROXY_UPSTREAM_REFUSED")
            return

        if response.status is None or response.status < 200 or response.status >= 300:
            if header_required_signature(response.body):
                record["statuses"].append("HEADER_REQUIRED")
            elif "HTTP_ERROR" not in record["statuses"]:
                record["statuses"].append("HTTP_ERROR")
            return

        if is_hls_text(body_text):
            parsed = self.parse_m3u(response.final_url, body_text, depth)
            record["discovery"]["playlist"] = {
                "kind": parsed["kind"],
                "metadata": parsed.get("metadata", {}),
                "valid": parsed["valid"],
                "error": parsed.get("error"),
            }

            if not parsed["valid"]:
                record["statuses"].append("INVALID_PLAYLIST")
                return

            record["statuses"].append("PLAYLIST_OK")
            record["playlist_entries"] = parsed["entries"]
            self.visited_playlist_text[key] = body_text

            child_count = 0
            for child in parsed["children"]:
                child_url = child.get("url")
                if not child_url:
                    continue

                if child.get("type") == "HLS_KEY_REFERENCE":
                    self.event(
                        "HLS_KEY_REFERENCE_RECORDED",
                        parent=safe_url(url),
                        reference=safe_url(child_url),
                        method=child.get("method"),
                        keyformat=child.get("keyformat"),
                    )
                    continue

                if (
                    self.args.max_children > 0
                    and child_count >= self.args.max_children
                ):
                    self.event(
                        "CHILD_LIMIT",
                        parent=safe_url(url),
                        max_children=self.args.max_children,
                    )
                    break

                child_count += 1
                child_type = child.get("type", "CHILD")
                self.enqueue(
                    child_url,
                    parent=response.final_url,
                    relation=child_type,
                    depth=depth + 1,
                    metadata={
                        "hls_attributes": child.get("attributes", {}),
                        "duration": child.get("duration"),
                        "source_playlist": safe_url(response.final_url),
                    },
                )

                if child_type == "HLS_SEGMENT":
                    self.validation_queue.append(child_url)
                    self.playlist_pool.append({
                        "url": child_url,
                        "source_url": response.final_url,
                        "type": "HLS_SEGMENT",
                        "duration": child.get("duration"),
                        "metadata": child.get("attributes", {}),
                    })

            self.event(
                "PLAYLIST_PARSED",
                url=safe_url(url),
                kind=parsed["kind"],
                children=len(parsed["children"]),
                metadata=parsed.get("metadata", {}),
            )
            return

        if is_dash_text(body_text):
            parsed = self.parse_dash(response.final_url, body_text)
            record["discovery"]["playlist"] = {
                "kind": "DASH",
                "metadata": parsed.get("metadata", {}),
                "valid": parsed["valid"],
                "error": parsed.get("error"),
            }

            if not parsed["valid"]:
                record["statuses"].append("INVALID_PLAYLIST")
                return

            record["statuses"].append("PLAYLIST_OK")
            record["playlist_entries"] = parsed["entries"]

            for child in parsed["children"]:
                child_url = child.get("url")
                if child_url:
                    self.enqueue(
                        child_url,
                        parent=response.final_url,
                        relation=child.get("type", "DASH_CHILD"),
                        depth=depth + 1,
                        metadata=child.get("attributes", {}),
                    )
            return

        # Если ответ не является плейлистом, классифицируем как медиа
        # по Content-Type и URL. Окончательное подтверждение будет на этапе B.
        if status in ("MEDIA_SEGMENT_OK", "MEDIA_RESOURCE_CANDIDATE"):
            record["statuses"].append("MEDIA_CANDIDATE")
            self.validation_queue.append(url)

        # Не разбираем HTML и текст ошибок как M3U.
        if "text/html" in response.headers.get("content-type", "").lower():
            record["discovery"]["html_response"] = True

    def discover(self) -> None:
        roots = self.args.sources or ROOT_SOURCES
        self.started_sources = list(roots)

        for url in roots:
            self.enqueue(url, parent=None, relation="ROOT_SOURCE", depth=0)

        while self.queue:
            if self.limits_exceeded():
                self.event("DISCOVERY_STOPPED", reason=self.stop_reason)
                break

            item = self.queue.popleft()
            key = normalized_url(item["url"])
            self.queued.discard(key)

            try:
                self.discover_one(item)
            except Exception as exc:
                record = self.record_for(item["url"])
                record["statuses"].append("DISCOVERY_EXCEPTION")
                record["discovery"]["exception"] = (
                    f"{type(exc).__name__}: {exc}"
                )
                self.event(
                    "DISCOVERY_EXCEPTION",
                    url=safe_url(item["url"]),
                    error=f"{type(exc).__name__}: {exc}",
                )

    def head_then_get(self, url: str) -> Tuple[HttpResponse, HttpResponse]:
        """
        Независимая проверка после этапа обнаружения.
        HEAD используется как предварительный запрос; GET обязателен,
        если HEAD не даёт достаточных данных.
        """
        head = self.http_request(url, "HEAD")
        need_get = (
            head.status is None
            or head.status < 200
            or head.status >= 300
            or not head.headers.get("content-type")
            or looks_like_playlist_url(url)
            or looks_like_segment_url(url)
        )

        if need_get:
            get_response = self.http_request(url, "GET")
        else:
            # Даже при успешном HEAD выполняем ограниченный GET для
            # проверки содержимого, если пользователь явно включил
            # сегментную проверку.
            if self.args.check_hls_segment and looks_like_segment_url(url):
                get_response = self.http_request(
                    url,
                    "GET",
                    {"Range": f"bytes=0-{max(0, self.args.segment_probe_bytes - 1)}"},
                )
            else:
                get_response = HttpResponse(
                    requested_url=url,
                    final_url=head.final_url,
                    status=head.status,
                    reason=head.reason,
                    headers=head.headers,
                    body=b"",
                    elapsed_seconds=0.0,
                    redirect_chain=head.redirect_chain,
                    error="GET_SKIPPED_AFTER_SUCCESSFUL_HEAD",
                    method="GET",
                )

        return head, get_response

    def run_ffprobe(self, url: str) -> Dict[str, Any]:
        executable = self.args.ffprobe_path or shutil.which("ffprobe")
        if not executable:
            return {
                "status": "CODEC_PROBE_UNAVAILABLE",
                "error": "ffprobe executable not found",
            }

        timeout = self.args.ffprobe_timeout
        command = [
            executable,
            "-v", "error",
            "-show_entries",
            "format=format_name,duration,size",
            "-show_entries",
            "stream=codec_type,codec_name,width,height,sample_rate,channels",
            "-of", "json",
            url,
        ]

        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                check=False,
            )
            try:
                parsed = json.loads(result.stdout or "{}")
            except json.JSONDecodeError:
                parsed = {}

            return {
                "status": (
                    "FFPROBE_OK" if result.returncode == 0
                    else "FFPROBE_ERROR"
                ),
                "returncode": result.returncode,
                "command": command,
                "stdout": result.stdout[:20000],
                "stderr": result.stderr[:20000],
                "parsed": parsed,
            }
        except subprocess.TimeoutExpired:
            return {
                "status": "FFPROBE_TIMEOUT",
                "command": command,
                "timeout": timeout,
            }
        except Exception as exc:
            return {
                "status": "FFPROBE_ERROR",
                "error": f"{type(exc).__name__}: {exc}",
            }

    def validate_one(self, url: str) -> None:
        record = self.record_for(url)
        if self.limits_exceeded():
            record["validation"] = {
                "status": "MEDIA_VALIDATION_INCOMPLETE",
                "reason": self.stop_reason,
            }
            record["statuses"].append("MEDIA_VALIDATION_INCOMPLETE")
            return

        head, get_response = self.head_then_get(url)

        head_record = head.to_record()
        get_record = get_response.to_record()

        validation: Dict[str, Any] = {
            "timestamp": utc_now(),
            "head": head_record,
            "get": get_record,
        }

        response = get_response if get_response.body else head

        if proxy_upstream_refused(response.body):
            validation["status"] = "PROXY_UPSTREAM_REFUSED"
            validation["error_body"] = response.body.decode(
                "utf-8", "replace"
            )[:8192]
            record["statuses"].append("PROXY_UPSTREAM_REFUSED")

        elif response.status is None:
            validation["status"] = "NETWORK_ERROR"
            record["statuses"].append("MEDIA_VALIDATION_INCOMPLETE")

        elif response.status < 200 or response.status >= 300:
            validation["status"] = "HTTP_ERROR"
            record["statuses"].append("HTTP_ERROR")

        else:
            content_type = response.headers.get("content-type", "").lower()
            body = response.body

            if "video/mp2t" in content_type:
                validation["status"] = "MEDIA_SEGMENT_OK"
                record["statuses"].append("MEDIA_SEGMENT_OK")

            elif looks_like_segment_url(url) and len(body) > 0:
                validation["status"] = "MEDIA_SEGMENT_OK"
                validation["note"] = (
                    "HTTP success and non-empty segment response; "
                    "codec-level verification is a separate result."
                )
                record["statuses"].append("MEDIA_SEGMENT_OK")

            elif is_hls_text(body.decode("utf-8", "replace")):
                validation["status"] = "PLAYLIST_OK"
                record["statuses"].append("PLAYLIST_OK")

            elif len(body) > 0 and (
                content_type.startswith("video/")
                or content_type.startswith("audio/")
                or content_type == "application/octet-stream"
            ):
                validation["status"] = "MEDIA_RESOURCE_OK"
                record["statuses"].append("MEDIA_RESOURCE_OK")

            else:
                validation["status"] = "HTTP_OK_UNCLASSIFIED"
                record["statuses"].append("MEDIA_VALIDATION_INCOMPLETE")

        if self.args.run_ffprobe:
            probe_target = url
            validation["ffprobe"] = self.run_ffprobe(probe_target)
            if validation["ffprobe"]["status"] == "CODEC_PROBE_UNAVAILABLE":
                record["statuses"].append("CODEC_PROBE_UNAVAILABLE")

        record["validation"] = validation
        self.event(
            "RESOURCE_VALIDATED",
            url=safe_url(url),
            status=validation.get("status"),
        )

    def validate(self) -> None:
        # Сохраняем уникальные URL, не уничтожая повторные записи вариантов
        # в playlist_entries и edges.
        unique: List[str] = []
        seen: Set[str] = set()

        for url in self.validation_queue:
            key = normalized_url(url)
            if key not in seen:
                seen.add(key)
                unique.append(url)

        for url in unique:
            if self.limits_exceeded():
                self.event("VALIDATION_STOPPED", reason=self.stop_reason)
                break
            try:
                self.validate_one(url)
            except Exception as exc:
                record = self.record_for(url)
                record["validation"] = {
                    "status": "MEDIA_VALIDATION_INCOMPLETE",
                    "exception": f"{type(exc).__name__}: {exc}",
                }
                record["statuses"].append("MEDIA_VALIDATION_INCOMPLETE")
                self.event(
                    "VALIDATION_EXCEPTION",
                    url=safe_url(url),
                    error=f"{type(exc).__name__}: {exc}",
                )

    def build_verified_playlist(self) -> Dict[str, Any]:
        """
        Формирует Verifiziert_tv.m3u из проверенных медиасегментов,
        которые были объявлены в HLS media playlists.

        Это диагностический пул URI сегментов, а не готовый список
        телеканалов: отдельный .ts сегмент нельзя корректно превратить
        в канал без сохранения его родительского HLS playlist.
        Поэтому в итоговом M3U сохраняются ссылки на проверенные
        родительские media playlists. Сегменты остаются в отчёте.
        """
        output = self.output_dir / "Verifiziert_tv.m3u"
        lines = ["#EXTM3U"]
        seen_playlists: Set[str] = set()
        included: List[str] = []

        for record in self.records.values():
            playlist = record.get("discovery", {}).get("playlist", {})
            if not playlist or playlist.get("kind") != "HLS_MEDIA":
                continue

            statuses = set(record.get("statuses", []))
            validation = record.get("validation", {})
            discovery = record.get("discovery", {})
            url = record.get("url", "")

            # Для живых плейлистов сегменты могут меняться между запросами.
            # Считаем родительский плейлист пригодным для пула, если он
            # действительно вернул валидный HLS и хотя бы один его сегмент
            # прошёл проверку или был получен как HTTP 200.
            segment_urls = [
                entry.get("url")
                for entry in record.get("playlist_entries", [])
                if entry.get("type") == "HLS_SEGMENT"
            ]

            segment_confirmed = False
            for seg_url in segment_urls:
                seg_record = self.record_for(seg_url)
                seg_statuses = set(seg_record.get("statuses", []))
                seg_validation = seg_record.get("validation", {})
                if (
                    "MEDIA_SEGMENT_OK" in seg_statuses
                    or seg_validation.get("status") == "MEDIA_SEGMENT_OK"
                ):
                    segment_confirmed = True
                    break

            if not segment_confirmed:
                continue

            key = normalized_url(url)
            if key in seen_playlists:
                continue
            seen_playlists.add(key)

            lines.append(
                f"#EXTINF:-1,"
                f"Verifiziert HLS | {urllib.parse.urlsplit(url).netloc}"
            )
            lines.append(url)
            included.append(url)

        output.write_text("\n".join(lines) + "\n", encoding="utf-8")

        self.event(
            "VERIFIED_PLAYLIST_WRITTEN",
            path=str(output),
            entries=len(included),
        )

        return {
            "path": str(output),
            "entries": len(included),
            "urls": included,
        }

    def write_reports(self, verified: Dict[str, Any]) -> None:
        finished = utc_now()
        elapsed = round(time.monotonic() - self.started_monotonic, 3)

        report = {
            "program": "Phoenix URL Dissector",
            "version": VERSION,
            "started": self.started_iso,
            "finished": finished,
            "elapsed_seconds": elapsed,
            "roots": self.started_sources,
            "limits": {
                "timeout": self.args.timeout,
                "max_bytes": self.args.max_bytes,
                "max_total_bytes": self.args.max_total_bytes,
                "max_runtime": self.args.max_runtime,
                "max_requests": self.args.max_requests,
                "max_depth": self.args.max_depth,
                "max_children": self.args.max_children,
                "max_sources": self.args.max_sources,
                "max_segments": self.args.max_segments,
            },
            "counters": {
                "requests": self.request_count,
                "total_bytes": self.total_bytes,
                "resources": len(self.records),
                "events": len(self.events),
                "edges": len(self.edges),
                "playlist_pool_entries": len(self.playlist_pool),
            },
            "stop_reason": self.stop_reason,
            "verified_playlist": verified,
            "resources": list(self.records.values()),
            "edges": self.edges,
            "events": self.events,
        }

        json_path = self.output_dir / "phoenix_report.json"
        events_path = self.output_dir / "phoenix_events.jsonl"
        edges_path = self.output_dir / "phoenix_edges.json"
        text_path = self.output_dir / "phoenix_summary.txt"
        pool_path = self.output_dir / "playlist_pool.json"

        json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        with events_path.open("w", encoding="utf-8") as stream:
            for event in self.events:
                stream.write(json.dumps(event, ensure_ascii=False) + "\n")

        edges_path.write_text(
            json.dumps(self.edges, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        pool_path.write_text(
            json.dumps(self.playlist_pool, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        counts: Dict[str, int] = {}
        for record in self.records.values():
            for status in set(record.get("statuses", [])):
                counts[status] = counts.get(status, 0) + 1

        summary_lines = [
            "Phoenix URL Dissector",
            "=" * 72,
            f"Version: {VERSION}",
            f"Started: {self.started_iso}",
            f"Finished: {finished}",
            f"Elapsed seconds: {elapsed}",
            "",
            "ROOT SOURCES",
            *[f"  {safe_url(x)}" for x in self.started_sources],
            "",
            "COUNTERS",
            f"  Requests: {self.request_count}",
            f"  Downloaded body bytes: {self.total_bytes}",
            f"  Resources: {len(self.records)}",
            f"  Events: {len(self.events)}",
            f"  Edges: {len(self.edges)}",
            f"  Playlist pool entries: {len(self.playlist_pool)}",
            f"  Stop reason: {self.stop_reason or 'NONE'}",
            "",
            "STATUS COUNTS",
        ]

        for status, count in sorted(counts.items()):
            summary_lines.append(f"  {status}: {count}")

        summary_lines.extend([
            "",
            "OUTPUTS",
            f"  {json_path}",
            f"  {events_path}",
            f"  {edges_path}",
            f"  {pool_path}",
            f"  {text_path}",
            f"  {verified['path']}",
            "",
            "IMPORTANT",
            "HTTP 200 alone does not prove codec-level playback.",
            "SOURCE_ORIGIN_CONFIRMED is not inferred from proxy headers alone.",
            "A proxy error is not automatically classified as source failure.",
        ])

        text_path.write_text(
            "\n".join(summary_lines) + "\n",
            encoding="utf-8",
        )

    def run(self) -> int:
        print(f"Phoenix URL Dissector {VERSION}")
        print("=" * 72)
        print(f"Started: {self.started_iso}")
        print(f"Output: {self.output_dir}")
        print("Phase A: recursive discovery and hidden-HLS diagnosis")

        self.discover()

        print(f"Discovery finished: {len(self.records)} resources")
        print("Phase B: independent HEAD/GET/media validation")

        self.validate()

        print("Phase C: building Verifiziert_tv.m3u")
        verified = self.build_verified_playlist()

        self.write_reports(verified)

        print()
        print("FINISHED")
        print(f"Resources: {len(self.records)}")
        print(f"Requests: {self.request_count}")
        print(f"Downloaded bytes: {self.total_bytes}")
        print(f"Verified playlist entries: {verified['entries']}")
        print(f"Playlist: {verified['path']}")
        print(f"Report: {self.output_dir / 'phoenix_report.json'}")
        print(f"Events: {self.output_dir / 'phoenix_events.jsonl'}")

        if self.stop_reason:
            print(f"Stopped by limit: {self.stop_reason}")

        return 0


def run_self_tests() -> int:
    parser = object.__new__(PhoenixDissector)

    test_m3u = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=4716800,CODECS="avc1.640028,mp4a.40.2",RESOLUTION=1920x1080,FRAME-RATE=25.000
https://stream.example.test/master/mono.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2657600,RESOLUTION=1280x720
https://stream.example.test/master/mono.m3u8
"""

    result = parser.parse_m3u(
        "https://proxy.example.test/movie.m3u8",
        test_m3u,
        0,
    )
    assert result["valid"] is True
    assert result["kind"] == "HLS_MASTER"
    assert result["metadata"]["variant_count"] == 2
    assert len(result["children"]) == 2
    assert result["children"][0]["url"] == (
        "https://stream.example.test/master/mono.m3u8"
    )
    assert result["children"][0]["attributes"]["RESOLUTION"] == "1920x1080"

    test_media = """#EXTM3U
#EXT-X-TARGETDURATION:6
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:3138009
#EXTINF:6.000,
2026/11/23/09/16/07-06000.ts
#EXTINF:6.000,
2026/11/23/09/16/13-06000.ts
"""
    result = parser.parse_m3u(
        "https://stream.example.test/1444/tracks-v1a1/mono.m3u8",
        test_media,
        1,
    )
    assert result["valid"] is True
    assert result["kind"] == "HLS_MEDIA"
    assert result["metadata"]["media_sequence"] == 3138009
    assert result["metadata"]["target_duration"] == 6
    assert result["metadata"]["segment_count"] == 2
    assert result["children"][0]["url"] == (
        "https://stream.example.test/1444/tracks-v1a1/"
        "2026/11/23/09/16/07-06000.ts"
    )

    assert header_required_signature(
        b"Missing required request header. Must specify one of: origin,x-requested-with"
    )
    assert proxy_upstream_refused(
        b"Not found because of proxy error: Error: connect ECONNREFUSED 45.231.133.30:80"
    )
    assert not proxy_upstream_refused(b"ordinary 404")

    attrs = parser.parse_attribute_list(
        'BANDWIDTH=4716800,CODECS="avc1.640028,mp4a.40.2",RESOLUTION=1920x1080'
    )
    assert attrs["BANDWIDTH"] == "4716800"
    assert attrs["CODECS"] == "avc1.640028,mp4a.40.2"
    assert attrs["RESOLUTION"] == "1920x1080"

    print("SELF-TEST: PASS")
    return 0


def positive_or_zero(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer")
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number")
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="URL_Dessector.py",
        description="Phoenix URL Dissector: recursive HLS/M3U diagnostics",
    )

    parser.add_argument(
        "--discover-livem3u",
        action="store_true",
        help="Use the default LiveM3U root sources.",
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        default=None,
        help="Explicit root URLs. If omitted, default LiveM3U sources are used.",
    )
    parser.add_argument(
        "--origin",
        action="append",
        default=[],
        help="Explicit Origin candidate for Tatnet retry; may be repeated.",
    )
    parser.add_argument("--timeout", type=positive_float, default=30)
    parser.add_argument("--max-bytes", type=positive_or_zero, default=52428800)
    parser.add_argument("--max-total-bytes", type=positive_or_zero, default=5368709120)
    parser.add_argument("--max-runtime", type=positive_or_zero, default=18000)
    parser.add_argument("--max-requests", type=positive_or_zero, default=100000)
    parser.add_argument("--max-depth", type=positive_or_zero, default=50)
    parser.add_argument("--max-children", type=positive_or_zero, default=100000)
    parser.add_argument("--max-sources", type=positive_or_zero, default=100000)
    parser.add_argument("--max-segments", type=positive_or_zero, default=1000)
    parser.add_argument("--segment-probe-bytes", type=positive_or_zero, default=1048576)
    parser.add_argument("--max-redirects", type=positive_or_zero, default=20)
    parser.add_argument(
        "--hls-variant-policy",
        choices=("all", "best", "worst"),
        default="all",
        help="Variant reporting policy. 'all' is the lossless default.",
    )
    parser.add_argument(
        "--check-hls-segment",
        action="store_true",
        help="Enable bounded GET validation for discovered HLS segments.",
    )
    parser.add_argument(
        "--run-ffprobe",
        action="store_true",
        help="Run ffprobe on candidate resources when installed.",
    )
    parser.add_argument("--ffprobe-path", default=None)
    parser.add_argument("--ffprobe-timeout", type=positive_float, default=30)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--name", default="Phoenix URL Dissector")
    parser.add_argument("--allow-private", action="store_true")
    parser.add_argument("--insecure-tls", action="store_true")
    parser.add_argument("--init-actions", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--skip-extended-analysis", action="store_true")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.segment_probe_bytes == 0:
        parser.error("--segment-probe-bytes must be > 0")

    if args.self_test:
        return run_self_tests()

    # Сохраняем совместимость с запуском GitHub Actions, который передаёт
    # --discover-livem3u. Явные --sources имеют приоритет.
    if not args.sources:
        args.sources = list(ROOT_SOURCES)

    if args.origin:
        # Origin-параметры добавляются как кандидаты на уровне конфигурации.
        # Не подменяем ими доказательства, полученные от сервера.
        os.environ["PHOENIX_EXPLICIT_ORIGINS"] = ",".join(args.origin)

    try:
        app = PhoenixDissector(args)
        return app.run()
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())