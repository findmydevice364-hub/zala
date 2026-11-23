#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix URL Dissector 2.9.9..pre.3.2.1-beta

Recursive LiveM3U / M3U / HLS / DASH / HTTP-wrapper discovery and diagnostics.
Python 3.9+, standard library only.

The scanner intentionally has no application-level global caps by default:
no fixed request count, total-byte budget, runtime budget, recursion-depth cap,
source cap, or child cap. GitHub Actions and remote servers still impose their
own external limits. Optional CLI limits can be enabled explicitly.

Safety:
- HTTP(S) only.
- Private/loopback/link-local destinations are blocked unless --allow-private.
- Does not bypass DRM, authentication, paywalls, or access controls.
- Secret-like query values are redacted in reports, not in actual requests.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import ipaddress
import json
import os
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

VERSION = "2.9.9..pre.3.2.1-beta"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 Chrome/131.0 Safari/537.36"
)
ROOTS = (
    "https://livem3u.tatnet.app/data/playlists/INDEX.m3u",
    "https://livem3u.tatnet.app/data/playlist.m3u",
)
REDIRECTS = {301, 302, 303, 307, 308}
HLS_TYPES = ("mpegurl", "vnd.apple.mpegurl", "x-mpegurl", "application/m3u8")
SECRET_KEYS = {
    "token", "key", "auth", "authorization", "password", "passwd",
    "secret", "signature", "sig", "jwt", "session", "access_token",
    "api_key", "apikey", "refresh_token", "client_secret",
}
CLAIM_HEADERS = {
    "x-final-url", "x-request-url", "x-cors-redirect-1", "x-original-url",
    "x-rewrite-url", "x-upstream-url",
}
PLAYLIST_EXTS = (".m3u", ".m3u8", ".m3u_", ".m3u8_", ".txt")
DASH_TYPES = ("dash+xml", "vnd.mpeg.dash.mpd")


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def redact(url: str) -> str:
    """Redact common secrets from report URLs without changing request URLs."""
    try:
        p = urllib.parse.urlsplit(url)
        netloc = p.hostname or ""
        if p.port:
            netloc += ":" + str(p.port)
        if p.username:
            netloc = "[redacted]@" + netloc
        query = []
        for key, value in urllib.parse.parse_qsl(p.query, keep_blank_values=True):
            query.append((key, "[REDACTED]" if key.lower() in SECRET_KEYS else value))
        return urllib.parse.urlunsplit(
            (p.scheme, netloc, p.path, urllib.parse.urlencode(query), "")
        )
    except Exception:
        return url


def normalize(raw: str, base: Optional[str] = None) -> str:
    raw = raw.strip().strip("\"'")
    if not raw or raw.startswith("#"):
        return ""
    if base:
        raw = urllib.parse.urljoin(base, raw)
    elif raw.startswith("//"):
        raw = "https:" + raw
    elif not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", raw):
        raw = "https://" + raw
    p = urllib.parse.urlsplit(raw)
    if p.scheme.lower() not in ("http", "https") or not p.hostname:
        raise ValueError("Требуется HTTP(S) URL")
    # Force port validation now, rather than failing later in a worker.
    _ = p.port
    return urllib.parse.urlunsplit(
        (p.scheme.lower(), p.netloc, p.path or "/", p.query, "")
    )


def public_ip(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
        return not (
            ip.is_private or ip.is_loopback or ip.is_link_local or
            ip.is_multicast or ip.is_reserved or ip.is_unspecified
        )
    except ValueError:
        return False


def dns_lookup(host: str, port: int, allow_private: bool = False) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        addresses = sorted({
            item[4][0] for item in socket.getaddrinfo(
                host, port, type=socket.SOCK_STREAM
            )
        })
        if not addresses:
            return {"host": host, "port": port, "addresses": [],
                    "error": "DNS не вернул адресов"}
        if not allow_private and not any(public_ip(x) for x in addresses):
            return {
                "host": host, "port": port, "addresses": addresses,
                "error": "Все адреса непубличные; блокировка для безопасности CI",
            }
        return {
            "host": host, "port": port, "addresses": addresses,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
        }
    except Exception as exc:
        return {
            "host": host, "port": port,
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
        }


def tcp_diagnostics(host: str, port: int, timeout: float = 8) -> Dict[str, Any]:
    """Probe TCP. A timeout of 0 means no explicit socket timeout."""
    started = time.monotonic()
    effective_timeout = None if timeout == 0 else timeout
    try:
        with socket.create_connection((host, port), timeout=effective_timeout):
            return {
                "host": host, "port": port, "connected": True,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
            }
    except Exception as exc:
        return {
            "host": host, "port": port, "connected": False,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
            "error": f"{type(exc).__name__}: {exc}",
        }


def tls_diagnostics(host: str, port: int = 443, timeout: float = 8) -> Dict[str, Any]:
    """Inspect TLS. A timeout of 0 means no explicit socket timeout."""
    started = time.monotonic()
    result: Dict[str, Any] = {"host": host, "port": port}
    effective_timeout = None if timeout == 0 else timeout
    try:
        context = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=effective_timeout) as sock:
            with context.wrap_socket(sock, server_hostname=host) as tls:
                cert = tls.getpeercert()
                cipher = tls.cipher()
                result.update({
                    "tls_version": tls.version(),
                    "cipher": cipher[0] if cipher else None,
                    "certificate_subject": cert.get("subject", []),
                    "certificate_issuer": cert.get("issuer", []),
                    "certificate_not_before": cert.get("notBefore"),
                    "certificate_not_after": cert.get("notAfter"),
                    "certificate_san": [
                        v for k, v in cert.get("subjectAltName", []) if k == "DNS"
                    ][:100],
                    "verified": True,
                })
    except Exception as exc:
        result.update({
            "verified": False,
            "error": f"{type(exc).__name__}: {exc}",
        })
    result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 2)
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


class Client:
    """HTTP client preserving each redirect hop and tracking total traffic/requests."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.dns: Dict[Tuple[str, int], Dict[str, Any]] = {}
        self.request_count = 0
        self.bytes_read = 0
        self.locked_limit_reason: Optional[str] = None
        context = ssl._create_unverified_context() if args.insecure_tls else ssl.create_default_context()
        self.opener = urllib.request.build_opener(
            NoRedirect(), urllib.request.HTTPSHandler(context=context)
        )

    def _remaining_bytes(self) -> Optional[int]:
        if self.args.max_total_bytes <= 0:
            return None
        return max(0, self.args.max_total_bytes - self.bytes_read)

    def _request_cap_reached(self) -> bool:
        return self.args.max_requests > 0 and self.request_count >= self.args.max_requests

    def request(
        self, url: str, method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        started = time.monotonic()
        original_url = url
        current = url
        chain: List[Dict[str, Any]] = []
        last_dns: Optional[Dict[str, Any]] = None
        # max_bytes=0 means no per-response cap; max_total_bytes=0 means no total cap.
        per_response = self.args.max_bytes if limit is None else limit
        if per_response is not None and per_response <= 0:
            per_response = None

        request_headers = {
            "User-Agent": self.args.user_agent,
            "Accept": "*/*",
            "Connection": "close",
        }
        if method.upper() == "GET":
            request_headers["Range"] = "bytes=0-1048575"
        request_headers.update(headers or {})

        # max_redirects=0 means unlimited redirects. Track redirect URLs separately
        # so a redirect cycle is detected without imposing an arbitrary hop cap.
        hop = 0
        seen_redirect_urls = {current}
        while True:
            if self._request_cap_reached():
                self.locked_limit_reason = "MAX_REQUESTS"
                return self._result(
                    original_url, current, method, None, {}, b"",
                    "Достигнут явно заданный лимит HTTP-запросов",
                    chain, started, last_dns,
                )
            parsed = urllib.parse.urlsplit(current)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                return self._result(
                    original_url, current, method, None, {}, b"",
                    "Недопустимый URL после редиректа", chain, started, last_dns,
                )
            host = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            key = (host, port)
            if key not in self.dns:
                self.dns[key] = dns_lookup(host, port, self.args.allow_private)
            last_dns = self.dns[key]
            if last_dns.get("error"):
                return self._result(
                    original_url, current, method, None, {}, b"",
                    last_dns["error"], chain, started, last_dns,
                )

            req = urllib.request.Request(current, headers=request_headers, method=method)
            self.request_count += 1
            try:
                # urllib accepts None for the socket default; timeout=0 is not a
                # valid blocking timeout in Python's socket layer.
                request_timeout = None if self.args.timeout == 0 else self.args.timeout
                response = self.opener.open(req, timeout=request_timeout)
                status = getattr(response, "status", 200)
                response_headers = dict(response.headers.items())
                body = b""
                truncated = False
                if method.upper() != "HEAD":
                    read_limit = per_response
                    remaining = self._remaining_bytes()
                    if remaining is not None:
                        if remaining <= 0:
                            response.close()
                            self.locked_limit_reason = "MAX_TOTAL_BYTES"
                            return self._result(
                                original_url, current, method, status, response_headers,
                                b"", "Достигнут явно заданный общий лимит байтов",
                                chain, started, last_dns,
                            )
                        read_limit = remaining if read_limit is None else min(read_limit, remaining)
                    chunk_size = 65536
                    chunks = []
                    got = 0
                    while True:
                        want = chunk_size if read_limit is None else min(chunk_size, read_limit - got + 1)
                        if want <= 0:
                            truncated = True
                            break
                        chunk = response.read(want)
                        if not chunk:
                            break
                        chunks.append(chunk)
                        got += len(chunk)
                        if read_limit is not None and got > read_limit:
                            truncated = True
                            break
                    body = b"".join(chunks)
                    if truncated and read_limit is not None:
                        body = body[:read_limit]
                    self.bytes_read += len(body)
                response.close()
                return self._result(
                    original_url, current, method, status, response_headers,
                    body, None, chain, started, last_dns, truncated,
                )

            except urllib.error.HTTPError as exc:
                status = exc.code
                response_headers = dict(exc.headers.items()) if exc.headers else {}
                location = response_headers.get("Location") or response_headers.get("location")
                can_follow_redirect = (
                    self.args.max_redirects == 0 or hop < self.args.max_redirects
                )
                if status in REDIRECTS and location and can_follow_redirect:
                    next_url = urllib.parse.urljoin(current, location)
                    chain.append({
                        "status": status, "from": redact(current),
                        "location": redact(next_url),
                    })
                    if next_url in seen_redirect_urls:
                        return self._result(
                            original_url, current, method, status, response_headers, b"",
                            "Обнаружен цикл HTTP-редиректов", chain, started, last_dns,
                        )
                    seen_redirect_urls.add(next_url)
                    hop += 1
                    old = urllib.parse.urlsplit(current)
                    new = urllib.parse.urlsplit(next_url)
                    if (old.hostname or "").lower() != (new.hostname or "").lower():
                        request_headers.pop("Authorization", None)
                        request_headers.pop("Cookie", None)
                    current = next_url
                    if status == 303 or (status in (301, 302) and method == "POST"):
                        method = "GET"
                        request_headers.pop("Content-Length", None)
                        request_headers.pop("Content-Type", None)
                    continue
                try:
                    if method.upper() == "HEAD":
                        body = b""
                    else:
                        remaining = self._remaining_bytes()
                        read_limit = per_response
                        if remaining is not None:
                            read_limit = remaining if read_limit is None else min(read_limit, remaining)
                        if read_limit is None:
                            body = exc.read()
                        elif read_limit <= 0:
                            body = b""
                            self.locked_limit_reason = "MAX_TOTAL_BYTES"
                        else:
                            body = exc.read(read_limit + 1)[:read_limit]
                        self.bytes_read += len(body)
                except Exception:
                    body = b""
                exc.close()
                return self._result(
                    original_url, current, method, status, response_headers, body,
                    f"HTTP {status}: {exc.reason}", chain, started, last_dns,
                    bool(per_response and len(body) >= per_response),
                )
            except Exception as exc:
                return self._result(
                    original_url, current, method, None, {}, b"",
                    f"{type(exc).__name__}: {exc}", chain, started, last_dns,
                )
        return self._result(
            original_url, current, method, None, {}, b"",
            "Обход редиректов завершён", chain, started, last_dns,
        )

    @staticmethod
    def _result(
        url: str, current: str, method: str, status: Optional[int],
        headers: Dict[str, str], body: bytes, error: Optional[str],
        chain: List[Dict[str, Any]], started: float,
        dns: Optional[Dict[str, Any]], truncated: bool = False,
    ) -> Dict[str, Any]:
        return {
            "url": redact(url), "final_url": redact(current), "method": method,
            "status": status, "headers": headers, "body": body, "error": error,
            "redirects": chain, "dns": dns, "truncated": truncated,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
        }


def origins_for(url: str, supplied: Optional[str] = None) -> List[str]:
    result: List[str] = []

    def add(value: str) -> None:
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
    decoded = urllib.parse.unquote(p.path)
    for component in decoded.split("/")[:12]:
        component = component.strip()
        if not re.fullmatch(r"[A-Za-z0-9.-]+(?::\d+)?", component):
            continue
        if "." not in component:
            continue
        candidate = component.split(":", 1)[0].lower()
        if candidate == current_host or ".." in candidate or candidate.startswith(".") or candidate.endswith("."):
            continue
        if re.search(
            r"\.(com|net|org|ru|by|io|app|dev|tv|uz|site|online|live|me|info|xyz|github\.io)$",
            candidate,
        ):
            add("https://" + component)
    return result[:8]


def parse_attrs(raw: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    # Quoted values may contain commas, e.g. CODECS="avc1,mp4a".
    pattern = r'([A-Za-z0-9-]+)=("(?:[^"\\]|\\.)*"|[^,]*)'
    for match in re.finditer(pattern, raw):
        result[match.group(1).strip()] = match.group(2).strip().strip('"')
    return result


def looks_like_m3u(body: bytes, content_type: str, url: str) -> bool:
    prefix = body.lstrip(b"\xef\xbb\xbf\x00\t\r\n ")
    path = urllib.parse.urlsplit(url).path.lower()
    ctype = content_type.lower()
    return (
        prefix.startswith(b"#EXTM3U")
        or any(x in ctype for x in HLS_TYPES)
        or path.endswith(PLAYLIST_EXTS)
    )


def looks_like_dash(body: bytes, content_type: str, url: str) -> bool:
    ctype = content_type.lower()
    path = urllib.parse.urlsplit(url).path.lower()
    prefix = body.lstrip(b"\xef\xbb\xbf\t\r\n ")
    return (
        any(x in ctype for x in DASH_TYPES)
        or path.endswith(".mpd")
        or b"<MPD" in prefix[:4096]
    )


def parse_m3u(text: str, base_url: str) -> Dict[str, Any]:
    lines = [line.strip() for line in text.replace("\r", "").split("\n") if line.strip()]
    entries: List[Dict[str, Any]] = []
    pending_extinf: Optional[str] = None
    pending_attrs: Dict[str, str] = {}
    pending_tags: List[str] = []
    globals_: List[str] = []
    seen: collections.Counter[str] = collections.Counter()
    for line in lines:
        if line.startswith("#EXTINF:"):
            pending_extinf = line
            raw = line.split(":", 1)[1]
            left, _, display_name = raw.partition(",")
            pending_attrs = parse_attrs(left)
            pending_attrs["display_name"] = display_name.strip()
            pending_tags = []
            continue
        if line.startswith("#"):
            if pending_extinf is not None:
                pending_tags.append(line)
            else:
                globals_.append(line)
            continue
        try:
            resolved = normalize(line, base_url)
        except Exception:
            resolved = urllib.parse.urljoin(base_url, line)
        seen[resolved] += 1
        entries.append({
            "url": resolved,
            "url_redacted": redact(resolved),
            "extinf": pending_extinf,
            "attributes": pending_attrs.copy(),
            "entry_tags": pending_tags.copy(),
            "source_line": line,
            "entry_index": len(entries),
        })
        pending_extinf = None
        pending_attrs = {}
        pending_tags = []
    return {
        "valid_header": bool(lines and lines[0].lstrip("\ufeff") == "#EXTM3U"),
        "line_count": len(lines),
        "global_tags": globals_,
        "entry_count": len(entries),
        "entries": entries,
        "orphan_extinf": pending_extinf,
        "duplicate_urls": [
            {"url": redact(url), "count": count}
            for url, count in seen.items() if count > 1
        ],
    }


def parse_hls(text: str, base_url: str) -> Dict[str, Any]:
    lines = [line.strip() for line in text.replace("\r", "").split("\n") if line.strip()]
    tags = collections.Counter(line.split(":", 1)[0] for line in lines if line.startswith("#"))
    result: Dict[str, Any] = {
        "valid_header": bool(lines and lines[0].lstrip("\ufeff") == "#EXTM3U"),
        "line_count": len(lines), "tag_counts": dict(tags),
        "is_master": False, "is_media": False, "variants": [],
        "renditions": [], "segments": [], "keys": [], "maps": [],
        "warnings": [], "duplicate_uris": [], "duplicate_variant_uris": [],
        "target_duration": None, "media_sequence": None,
        "endlist": "#EXT-X-ENDLIST" in lines, "discontinuities": 0,
        "program_date_times": [], "byte_ranges": [],
        "declared_duration_seconds": 0.0, "encryption_methods": [],
        "playlist_type": None,
        "independent_segments": "#EXT-X-INDEPENDENT-SEGMENTS" in lines,
    }
    pending: Optional[Dict[str, str]] = None
    seen: collections.Counter[str] = collections.Counter()
    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            result["is_master"] = True
            pending = parse_attrs(line.split(":", 1)[1])
            continue
        if line.startswith("#EXT-X-MEDIA:"):
            item = parse_attrs(line.split(":", 1)[1])
            if item.get("URI"):
                item["resolved_uri"] = urllib.parse.urljoin(base_url, item["URI"])
            result["renditions"].append(item)
            continue
        if line.startswith("#EXT-X-TARGETDURATION:"):
            result["target_duration"] = line.split(":", 1)[1]
            continue
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            result["media_sequence"] = line.split(":", 1)[1]
            continue
        if line.startswith("#EXT-X-PLAYLIST-TYPE:"):
            result["playlist_type"] = line.split(":", 1)[1]
            continue
        if line == "#EXT-X-DISCONTINUITY":
            result["discontinuities"] += 1
            continue
        if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            result["program_date_times"].append(line.split(":", 1)[1])
            continue
        if line.startswith("#EXT-X-BYTERANGE:"):
            result["byte_ranges"].append(line.split(":", 1)[1])
            continue
        if line.startswith(("#EXT-X-KEY:", "#EXT-X-SESSION-KEY:")):
            item = parse_attrs(line.split(":", 1)[1])
            method = item.get("METHOD")
            if method and method not in result["encryption_methods"]:
                result["encryption_methods"].append(method)
            if item.get("URI"):
                item["resolved_uri"] = urllib.parse.urljoin(base_url, item["URI"])
            result["keys"].append(item)
            continue
        if line.startswith("#EXT-X-MAP:"):
            item = parse_attrs(line.split(":", 1)[1])
            if item.get("URI"):
                item["resolved_uri"] = urllib.parse.urljoin(base_url, item["URI"])
            result["maps"].append(item)
            continue
        if line.startswith("#EXTINF:"):
            result["is_media"] = True
            try:
                result["declared_duration_seconds"] += float(line.split(":", 1)[1].split(",", 1)[0])
            except (ValueError, IndexError):
                result["warnings"].append("Некорректная длительность EXTINF")
            continue
        if line.startswith("#"):
            continue
        uri = urllib.parse.urljoin(base_url, line)
        seen[uri] += 1
        if pending is not None:
            result["variants"].append({
                "uri": uri, "bandwidth": pending.get("BANDWIDTH"),
                "average_bandwidth": pending.get("AVERAGE-BANDWIDTH"),
                "resolution": pending.get("RESOLUTION"), "codecs": pending.get("CODECS"),
                "frame_rate": pending.get("FRAME-RATE"), "attributes": pending,
            })
            pending = None
        else:
            result["segments"].append(uri)
    result["duplicate_uris"] = [
        {"uri": redact(uri), "count": count} for uri, count in seen.items() if count > 1
    ]
    if not result["valid_header"]:
        result["warnings"].append("Нет #EXTM3U в начале плейлиста")
    if pending is not None:
        result["warnings"].append("#EXT-X-STREAM-INF без URI")
    if result["is_master"] and result["segments"]:
        result["warnings"].append("Смешаны master-варианты и обычные URI")
    if not result["is_master"] and not result["is_media"]:
        result["warnings"].append("Не обнаружены теги master/media HLS")
    if result["is_media"] and not result["segments"]:
        result["warnings"].append("Есть EXTINF, но нет URI сегментов")
    if result["is_master"] and len(result["variants"]) != tags["#EXT-X-STREAM-INF"]:
        result["warnings"].append("Число URI вариантов не совпадает с числом STREAM-INF")
    by_uri: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
    for variant in result["variants"]:
        by_uri[variant["uri"]].append(variant)
    result["duplicate_variant_uris"] = [
        {
            "uri": redact(uri), "count": len(items),
            "declared_resolutions": [x.get("resolution") for x in items],
            "declared_bandwidths": [x.get("bandwidth") for x in items],
        }
        for uri, items in by_uri.items() if len(items) > 1
    ]
    return result


def parse_dash(body: bytes, base_url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "valid_xml": False, "is_dynamic": None, "type": None,
        "duration": None, "base_urls": [], "representations": [],
        "warnings": [],
    }
    try:
        root = ET.fromstring(body)
        result["valid_xml"] = True
        result["type"] = root.attrib.get("type")
        result["is_dynamic"] = root.attrib.get("type") == "dynamic"
        result["duration"] = root.attrib.get("mediaPresentationDuration")
        for elem in root.iter():
            tag = elem.tag.rsplit("}", 1)[-1]
            if tag == "BaseURL" and elem.text and elem.text.strip():
                resolved = urllib.parse.urljoin(base_url, elem.text.strip())
                result["base_urls"].append(resolved)
            elif tag == "Representation":
                result["representations"].append({
                    "id": elem.attrib.get("id"),
                    "bandwidth": elem.attrib.get("bandwidth"),
                    "width": elem.attrib.get("width"),
                    "height": elem.attrib.get("height"),
                    "codecs": elem.attrib.get("codecs"),
                })
    except Exception as exc:
        result["warnings"].append(f"DASH XML parse error: {type(exc).__name__}: {exc}")
    return result


def safe_headers(headers: Dict[str, str]) -> Dict[str, str]:
    wanted = {
        "content-type", "content-length", "content-range", "accept-ranges",
        "location", "server", "via", "www-authenticate", "retry-after",
        "access-control-allow-origin", "x-final-url", "x-request-url",
        "x-cors-redirect-1", "x-cache", "x-proxy-cache", "x-tatnet-cache",
        "x-github-edge-region", "date", "etag", "last-modified", "cache-control",
    }
    result = {}
    for key, value in headers.items():
        if key.lower() in wanted:
            value = str(value)
            if key.lower() == "location":
                value = redact(urllib.parse.urljoin("https://redact.invalid", value))
            result[key] = value[:1200]
    return result


def extract_urls_from_text(text: str, base_url: str) -> List[str]:
    """Find explicit HTTP(S) URLs and URL-like relative paths in wrapper responses."""
    found: List[str] = []
    # JSON and HTML wrappers often expose a target URL in plain text.
    patterns = [
        r'https?://[^\s"\'<>\\]+',
        r'(?:"(?:url|src|stream|playlist|manifest|file|source|target|location|hls|dash)"\s*:\s*")([^"]+)',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            candidate = match.group(1) if match.lastindex else match.group(0)
            candidate = candidate.rstrip("),.;]}\'\"")
            try:
                value = normalize(candidate, base_url)
            except Exception:
                continue
            if value not in found:
                found.append(value)
    return found


def is_probably_text(body: bytes, content_type: str) -> bool:
    ctype = content_type.lower()
    if any(x in ctype for x in ("text/", "json", "xml", "javascript", "mpegurl")):
        return True
    sample = body[:2048]
    if not sample:
        return False
    printable = sum(1 for b in sample if b in (9, 10, 13) or 32 <= b < 127)
    return printable / len(sample) > 0.82


class Dissector:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.client = Client(args)
        self.visited: Set[str] = set()
        self.records: Dict[str, Dict[str, Any]] = {}
        self.queue: collections.deque[Dict[str, Any]] = collections.deque()
        self.started = time.monotonic()
        self.limit_reason: Optional[str] = None
        self.channel_records: List[Dict[str, Any]] = []
        self.playlist_urls: Set[str] = set()
        self.terminal_urls: Set[str] = set()
        self.unresolved: List[Dict[str, Any]] = []
        self.errors: List[Dict[str, Any]] = []
        self.edge_records: List[Dict[str, Any]] = []
        self.processed_count = 0

    def _budget_hit(self) -> bool:
        if self.args.max_runtime > 0 and time.monotonic() - self.started >= self.args.max_runtime:
            self.limit_reason = "MAX_RUNTIME"
            return True
        if self.args.max_requests > 0 and self.client.request_count >= self.args.max_requests:
            self.limit_reason = "MAX_REQUESTS"
            return True
        if self.args.max_total_bytes > 0 and self.client.bytes_read >= self.args.max_total_bytes:
            self.limit_reason = "MAX_TOTAL_BYTES"
            return True
        return False

    def _enqueue(self, url: str, parent: Optional[str], depth: int, kind: str,
                 channel: Optional[Dict[str, Any]] = None) -> None:
        if self.args.max_depth > 0 and depth > self.args.max_depth:
            self.unresolved.append({
                "url": redact(str(url)), "parent": redact(parent) if parent else None,
                "kind": kind, "error": "MAX_DEPTH: заданный предел глубины достигнут",
            })
            return
        if self.args.max_sources > 0 and url not in self.visited and len(self.records) + len(self.queue) >= self.args.max_sources:
            self.unresolved.append({
                "url": redact(str(url)), "parent": redact(parent) if parent else None,
                "kind": kind, "error": "MAX_SOURCES: заданный предел источников достигнут",
            })
            return
        try:
            url = normalize(url)
        except Exception as exc:
            self.unresolved.append({
                "url": str(url), "parent": redact(parent) if parent else None,
                "kind": kind, "error": f"Invalid URL: {exc}",
            })
            return
        # A URL is fetched once, but every channel/provenance edge is preserved separately.
        edge = {
            "url": redact(url), "raw_url": url,
            "parent": redact(parent) if parent else None,
            "depth": depth, "kind": kind,
            "channel": channel,
        }
        self.edge_records.append(edge)
        if channel is not None:
            self.channel_records.append({
                "url": redact(url),
                "raw_url": url,
                "source_playlist": channel.get("source_playlist"),
                "parent_playlist": channel.get("parent_playlist"),
                "original_extinf": channel.get("original_extinf"),
                "channel_attributes": channel.get("channel_attributes", {}),
                "channel_tags": channel.get("channel_tags", []),
                "channel_name": channel.get("channel_name"),
                "discovered_at_depth": depth,
                "kind": kind,
            })
        if url not in self.visited and not any(item["url"] == url for item in self.queue):
            self.queue.append({
                "url": url, "parent": parent, "depth": depth,
                "kind": kind, "channel": channel,
            })

    def _network_diagnostics(self, url: str) -> Dict[str, Any]:
        p = urllib.parse.urlsplit(url)
        host = p.hostname or ""
        port = p.port or (443 if p.scheme == "https" else 80)
        dns = self.client.dns.get((host, port)) or dns_lookup(host, port, self.args.allow_private)
        result: Dict[str, Any] = {"dns": dns}
        # Only open direct sockets after DNS passed the same public-IP gate as HTTP.
        if not dns.get("error"):
            result["tcp"] = tcp_diagnostics(host, port, self.args.timeout)
            if p.scheme == "https":
                result["tls"] = tls_diagnostics(host, port, self.args.timeout)
        return result

    def _fetch_with_strategies(self, url: str, limit: Optional[int] = None) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        origins = origins_for(url, self.args.origin)
        strategies: List[Tuple[str, Dict[str, str]]] = [("plain", {})]
        strategies += [
            (f"origin:{origin}", {"Origin": origin, "Referer": origin + "/"})
            for origin in origins
        ]
        strategies.append(("x-requested-with", {"X-Requested-With": "XMLHttpRequest"}))
        strategies += [
            (
                f"origin+x-requested-with:{origin}",
                {"Origin": origin, "Referer": origin + "/", "X-Requested-With": "XMLHttpRequest"},
            )
            for origin in origins
        ]
        attempts: List[Dict[str, Any]] = []
        best: Optional[Dict[str, Any]] = None
        seen_headers: Set[Tuple[Tuple[str, str], ...]] = set()
        for label, headers in strategies:
            signature = tuple(sorted(headers.items()))
            if signature in seen_headers:
                continue
            seen_headers.add(signature)
            response = self.client.request(url, "GET", headers=headers, limit=limit)
            attempts.append({
                "strategy": label, "status": response["status"],
                "final_url": response["final_url"], "error": response["error"],
                "headers": safe_headers(response["headers"]),
                "elapsed_ms": response["elapsed_ms"],
                "body_bytes": len(response["body"]),
                "truncated": response["truncated"],
                "redirects": response["redirects"], "dns": response.get("dns"),
            })
            if best is None:
                best = response
            elif response["status"] is not None and (
                best["status"] is None or
                (best["status"] >= 400 and response["status"] < 400)
            ):
                best = response
            if response["status"] is not None and 200 <= response["status"] < 300 and response["body"]:
                best = response
                break
            if self._budget_hit():
                break
        return best, attempts

    def check(self, task: Dict[str, Any]) -> Dict[str, Any]:
        url = task["url"]
        parent = task.get("parent")
        depth = task.get("depth", 0)
        kind = task.get("kind", "source")
        if url in self.records:
            return self.records[url]
        record: Dict[str, Any] = {
            "url": redact(url), "raw_url": url,
            "parent": redact(parent) if parent else None,
            "resource_type": kind, "depth": depth, "checked_at": now(),
            "host": urllib.parse.urlsplit(url).hostname,
            "path": urllib.parse.urlsplit(url).path,
            "attempts": [], "children": [], "errors": [],
            "status_label": "UNKNOWN",
            "channel_context": task.get("channel"),
        }
        self.records[url] = record
        self.visited.add(url)
        record["network"] = self._network_diagnostics(url)

        head = self.client.request(url, "HEAD", limit=0)
        record["head"] = {
            key: value for key, value in head.items() if key not in ("body", "headers")
        }
        record["head"]["headers"] = safe_headers(head.get("headers", {}))

        # Playlist/manifests are read fully by default. Media segments and binary
        # terminal resources are only probed, so a .ts/.m4s/.mp4 object is not
        # accidentally downloaded in full merely to establish reachability.
        probe_limit = self.args.segment_probe_bytes if kind in (
            "hls_segment", "dash_segment", "terminal_probe"
        ) else None
        best, attempts = self._fetch_with_strategies(url, limit=probe_limit)
        record["attempts"] = attempts
        if best is None:
            record["errors"].append("Нет результата HTTP")
            record["status_label"] = "UNKNOWN"
            return record

        headers = best["headers"]
        body = best["body"]
        status = best["status"]
        content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "")
        final_url = best["final_url"] or url
        record.update({
            "http_status": status, "final_url": redact(final_url),
            "final_url_raw": final_url, "response_headers": safe_headers(headers),
            "content_type": content_type, "body_bytes": len(body),
            "body_truncated": best["truncated"], "redirects": best["redirects"],
            "elapsed_ms": best["elapsed_ms"],
            "server_claimed_urls": {
                k: v for k, v in headers.items() if k.lower() in CLAIM_HEADERS
            },
        })
        record["redirects_raw"] = best["redirects"]

        children_to_add: List[Tuple[str, str, Optional[Dict[str, Any]]]] = []
        text_body = body.decode("utf-8-sig", errors="replace") if body else ""

        if body and looks_like_m3u(body, content_type, final_url):
            # HLS is also syntactically an M3U file. Detect its EXT-X tags first,
            # otherwise HLS segments would be misclassified as TV-channel entries.
            is_hls_payload = bool(re.search(r"(?m)^#EXT-X-", text_body))
            self.playlist_urls.add(url)
            if is_hls_payload:
                hls = parse_hls(text_body, final_url)
                record["hls"] = hls
                record["resource_type_detected"] = (
                    "hls_master_playlist" if hls["is_master"]
                    else "hls_media_playlist" if hls["is_media"]
                    else "m3u_playlist"
                )
                # Variants are queued once, after applying hls_variant_policy below.
                # Renditions, keys and maps remain independently discoverable.
                for item in hls["renditions"]:
                    if item.get("resolved_uri"):
                        children_to_add.append((item["resolved_uri"], "hls_rendition", task.get("channel")))
                for item in hls["keys"] + hls["maps"]:
                    target = item.get("resolved_uri")
                    if target and not target.startswith("data:"):
                        children_to_add.append((target, "hls_key_or_map", task.get("channel")))
                if not hls["is_master"]:
                    segments = hls["segments"]
                    if self.args.max_segments > 0:
                        segments = segments[:self.args.max_segments]
                    for segment in segments:
                        if self.args.check_hls_segment:
                            children_to_add.append((segment, "hls_segment", task.get("channel")))
                if hls["warnings"]:
                    record["errors"].extend(hls["warnings"])
                if hls["is_master"]:
                    variants = hls["variants"]
                    if self.args.hls_variant_policy == "best" and variants:
                        variants = [max(variants, key=lambda x: int(x.get("bandwidth") or 0))]
                    elif self.args.hls_variant_policy == "worst" and variants:
                        variants = [min(variants, key=lambda x: int(x.get("bandwidth") or 0))]
                    for item in variants:
                        children_to_add.append((item["uri"], "hls_variant", task.get("channel")))
                    record["status_label"] = "HLS_MASTER_PARSED"
                elif hls["is_media"]:
                    record["status_label"] = "HLS_MEDIA_PARSED"
                else:
                    record["status_label"] = "PLAYLIST_PARSED"
            else:
                m3u = parse_m3u(text_body, final_url)
                record["m3u"] = m3u
                record["resource_type_detected"] = "m3u_playlist"
                if not m3u["valid_header"]:
                    record["errors"].append("M3U-подобный ответ без корректного #EXTM3U")
                for entry in m3u["entries"]:
                    channel = {
                        "source_playlist": redact(url),
                        "source_playlist_raw": url,
                        "parent_playlist": redact(parent) if parent else None,
                        "original_extinf": entry.get("extinf"),
                        "channel_attributes": entry.get("attributes", {}),
                        "channel_tags": entry.get("entry_tags", []),
                        "channel_name": entry.get("attributes", {}).get("display_name"),
                        "entry_index": entry.get("entry_index"),
                    }
                    children_to_add.append((entry["url"], "playlist_entry", channel))
                # INDEX.m3u may expose bare playlist links without EXTINF metadata.
                known = {x[0] for x in children_to_add}
                for candidate in extract_urls_from_text(text_body, final_url):
                    if candidate not in known:
                        children_to_add.append((candidate, "embedded_url", None))
                record["status_label"] = "PLAYLIST_PARSED" if m3u["entries"] else "PLAYLIST_EMPTY_OR_UNRECOGNIZED"

        elif body and looks_like_dash(body, content_type, final_url):
            record["resource_type_detected"] = "dash_mpd"
            record["dash"] = parse_dash(body, final_url)
            self.playlist_urls.add(url)
            for base in record["dash"].get("base_urls", []):
                children_to_add.append((base, "dash_base_url", task.get("channel")))
            record["status_label"] = "DASH_MANIFEST_PARSED" if record["dash"]["valid_xml"] else "PARTIAL"

        elif body and looks_like_m3u(body, content_type, final_url):
            # Kept for explicit readability; most M3U responses are handled above.
            record["resource_type_detected"] = "hls_or_m3u"
            record["status_label"] = "PLAYLIST_PARSED"

        elif body and is_probably_text(body, content_type):
            # A wrapper may return JSON/HTML/text containing the next playlist/stream URL.
            discovered = extract_urls_from_text(text_body, final_url)
            if discovered:
                record["resource_type_detected"] = "http_wrapper_or_text"
                record["embedded_urls"] = [redact(x) for x in discovered]
                for candidate in discovered:
                    children_to_add.append((candidate, "wrapper_target", task.get("channel")))
                record["status_label"] = "WRAPPER_DISCOVERED"
                record["body_preview"] = text_body[:500].replace("\x00", "?")
            elif status is not None and 200 <= status < 300:
                record["resource_type_detected"] = "text_or_html"
                record["status_label"] = "ACCESSIBLE_NON_MEDIA"
                record["body_preview"] = text_body[:500].replace("\x00", "?")
        elif body and status is not None and 200 <= status < 300:
            record["resource_type_detected"] = "binary_or_media"
            record["status_label"] = "ACCESSIBLE_TERMINAL_CANDIDATE"
            self.terminal_urls.add(url)

        if status in (401, 403):
            record["status_label"] = "ACCESS_RESTRICTED"
            record["errors"].append(f"HTTP {status}: сервер ограничивает доступ")
        elif status is not None and status >= 400 and status not in (401, 403):
            record["status_label"] = "BROKEN"
            record["errors"].append(f"HTTP {status}")
        elif best["error"] and not body:
            record["status_label"] = "UNKNOWN"
            record["errors"].append(best["error"])
        elif status is not None and 200 <= status < 300 and not body:
            record["status_label"] = "EMPTY_RESPONSE"
            record["errors"].append("Успешный HTTP-ответ с пустым телом")

        # Preserve all candidate links; each unique URL is fetched once, but edges remain per parent/channel.
        local_seen: Set[str] = set()
        if self.args.max_children > 0:
            children_to_add = children_to_add[:self.args.max_children]
        for child_url, child_kind, channel in children_to_add:
            try:
                child_url = normalize(child_url, final_url)
            except Exception as exc:
                self.unresolved.append({
                    "url": str(child_url), "parent": redact(url),
                    "kind": child_kind, "error": f"Invalid child URL: {exc}",
                })
                continue
            if child_url in local_seen:
                continue
            local_seen.add(child_url)
            record["children"].append({
                "url": redact(child_url), "raw_url": child_url,
                "resource_type": child_kind,
                "status_label": "QUEUED_OR_ALREADY_DISCOVERED",
            })
            self._enqueue(child_url, url, depth + 1, child_kind, channel)

        if not body and status is not None and 200 <= status < 300:
            record["errors"].append("Успешный HTTP-ответ с пустым телом")
        if record["status_label"] in ("UNKNOWN", "BROKEN", "ACCESS_RESTRICTED", "PARTIAL"):
            self.unresolved.append({
                "url": redact(url), "raw_url": url,
                "parent": redact(parent) if parent else None,
                "kind": kind, "status_label": record["status_label"],
                "http_status": status, "error": "; ".join(record["errors"]),
            })
        return record

    def run(self, roots: Iterable[str]) -> Dict[str, Any]:
        for root in roots:
            self._enqueue(root, None, 0, "root", None)
        while self.queue:
            if self._budget_hit():
                break
            task = self.queue.popleft()
            url = task["url"]
            if url in self.records:
                continue
            self.processed_count += 1
            print(
                f"[{self.processed_count}] depth={task['depth']} "
                f"type={task['kind']} {redact(url)}",
                flush=True,
            )
            try:
                record = self.check(task)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                record = {
                    "url": redact(url), "raw_url": url,
                    "parent": redact(task["parent"]) if task.get("parent") else None,
                    "resource_type": task["kind"], "depth": task["depth"],
                    "status_label": "UNKNOWN",
                    "errors": [f"{type(exc).__name__}: {exc}"],
                }
                self.records[url] = record
                self.errors.append({"url": redact(url), "error": record["errors"][0]})
                self.unresolved.append(record)
            print(
                f"  => {record.get('status_label')} "
                f"HTTP={record.get('http_status', '?')} "
                f"bytes={record.get('body_bytes', 0)}",
                flush=True,
            )
            if self._budget_hit():
                break

        complete = not self.queue and self.limit_reason is None
        return {
            "tool": "Phoenix URL Dissector",
            "version": VERSION,
            "generated_at": now(),
            "mode": self.args.mode,
            "scan_complete": complete,
            "stop_reason": self.limit_reason,
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "settings": {
                "timeout": self.args.timeout,
                "max_bytes_per_response": self.args.max_bytes,
                "max_total_bytes": self.args.max_total_bytes,
                "max_runtime": self.args.max_runtime,
                "max_requests": self.args.max_requests,
                "max_depth": self.args.max_depth,
                "max_children": self.args.max_children,
                "max_sources": self.args.max_sources,
                "max_redirects": self.args.max_redirects,
                "segment_probe_bytes": self.args.segment_probe_bytes,
                "hls_variant_policy": self.args.hls_variant_policy,
                "check_hls_segment": self.args.check_hls_segment,
                "allow_private": self.args.allow_private,
                "insecure_tls": self.args.insecure_tls,
                "unlimited_mode": all(
                    value <= 0 for value in (
                        self.args.max_bytes, self.args.max_total_bytes,
                        self.args.max_runtime, self.args.max_requests,
                        self.args.max_depth, self.args.max_children,
                        self.args.max_sources,
                    )
                ),
            },
            "stats": {
                "urls_discovered": len(self.edge_records),
                "unique_urls_processed": len(self.records),
                "requests": self.client.request_count,
                "bytes_read": self.client.bytes_read,
                "playlists": len(self.playlist_urls),
                "terminal_candidates": len(self.terminal_urls),
                "unresolved": len(self.unresolved),
                "queue_remaining": len(self.queue),
            },
            "sources": list(self.records.values()),
            "edges": self.edge_records,
            "channel_records": self.channel_records,
            "playlist_urls": sorted(redact(x) for x in self.playlist_urls),
            "terminal_urls": sorted(redact(x) for x in self.terminal_urls),
            "unresolved": self.unresolved,
            "errors": self.errors,
            "pending_queue": [
                {
                    "url": redact(x["url"]), "raw_url": x["url"],
                    "parent": redact(x["parent"]) if x.get("parent") else None,
                    "depth": x["depth"], "kind": x["kind"],
                } for x in self.queue
            ],
        }


def flatten(record: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    yield record
    for child in record.get("children", []):
        if isinstance(child, dict) and "children" in child:
            yield from flatten(child)


def write_outputs(report: Dict[str, Any], output_dir: str, name: str) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "phoenix_report"
    (out / f"{stem}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    lines = [
        f"Phoenix URL Dissector {VERSION}",
        f"Generated: {report['generated_at']}",
        f"Mode: {report['mode']}",
        f"Scan complete: {report['scan_complete']}",
        f"Stop reason: {report['stop_reason']}",
        "=" * 78,
        "STATS",
        json.dumps(report["stats"], ensure_ascii=False, indent=2),
        "",
        "ROOT / RESOURCE RECORDS",
    ]
    counts = collections.Counter()
    for record in report["sources"]:
        counts[record.get("status_label", "UNKNOWN")] += 1
        lines.append(
            f"[{record.get('status_label')}] HTTP={record.get('http_status')} "
            f"type={record.get('resource_type_detected', record.get('resource_type'))} "
            f"bytes={record.get('body_bytes', 0)} {record.get('url')}"
        )
        for err in record.get("errors", []):
            lines.append(f"  ERROR: {err}")
        net = record.get("network", {})
        tcp = net.get("tcp", {})
        if tcp:
            lines.append(f"  TCP connected={tcp.get('connected')} latency_ms={tcp.get('elapsed_ms')}")
        tls = net.get("tls", {})
        if tls:
            lines.append(f"  TLS version={tls.get('tls_version')} verified={tls.get('verified')} cipher={tls.get('cipher')}")
        hls = record.get("hls", {})
        if hls:
            lines.append(
                f"  HLS master={hls.get('is_master')} media={hls.get('is_media')} "
                f"variants={len(hls.get('variants', []))} segments={len(hls.get('segments', []))} "
                f"duration={hls.get('declared_duration_seconds')}s encryption={hls.get('encryption_methods')}"
            )
            for variant in hls.get("variants", []):
                lines.append(
                    f"  VARIANT {variant.get('resolution')} bw={variant.get('bandwidth')} -> {redact(variant['uri'])}"
                )
        for attempt in record.get("attempts", []):
            lines.append(
                f"  ATTEMPT {attempt.get('strategy')} HTTP={attempt.get('status')} "
                f"ms={attempt.get('elapsed_ms')} bytes={attempt.get('body_bytes')} "
                f"URL={attempt.get('final_url')}"
            )
    lines += [
        "", "STATUS COUNTS",
        json.dumps(dict(counts), ensure_ascii=False, indent=2),
        "", "PLAYLIST URLS",
        *report.get("playlist_urls", []),
        "", "TERMINAL CANDIDATES",
        *report.get("terminal_urls", []),
        "", "UNRESOLVED",
    ]
    for item in report.get("unresolved", []):
        lines.append(json.dumps(item, ensure_ascii=False))
    lines += [
        "", "CHANNEL RECORDS (full EXTINF metadata retained)",
    ]
    for item in report.get("channel_records", []):
        lines.append(json.dumps(item, ensure_ascii=False))
    (out / f"{stem}.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Convenient machine-readable exports.
    (out / "source_graph.json").write_text(
        json.dumps({"edges": report.get("edges", []), "sources": report.get("sources", [])},
                   ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out / "playlist_candidates.jsonl").write_text(
        "".join(json.dumps({"url": url}, ensure_ascii=False) + "\n"
                for url in report.get("playlist_urls", [])), encoding="utf-8"
    )
    (out / "unresolved_urls.jsonl").write_text(
        "".join(json.dumps(item, ensure_ascii=False) + "\n"
                for item in report.get("unresolved", [])), encoding="utf-8"
    )
    (out / "channel_records.jsonl").write_text(
        "".join(json.dumps(item, ensure_ascii=False) + "\n"
                for item in report.get("channel_records", [])), encoding="utf-8"
    )
    (out / "terminal_streams.txt").write_text(
        "\n".join(report.get("terminal_urls", [])) + ("\n" if report.get("terminal_urls") else ""),
        encoding="utf-8",
    )
    (out / "pending_queue.json").write_text(
        json.dumps(report.get("pending_queue", []), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_urls(args: argparse.Namespace) -> List[str]:
    items: List[str] = list(args.url or [])
    if args.discover_livem3u:
        items.extend(ROOTS)
    if args.input_file:
        path = Path(args.input_file)
        if not path.exists():
            raise FileNotFoundError(f"Не найден файл: {path}")
        items.extend(path.read_text(encoding="utf-8-sig", errors="replace").splitlines())
    if args.stdin:
        items.extend(sys.stdin)
    result: List[str] = []
    for line in items:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Keep complete URL tokens; ordinary M3U lines are handled by the recursive parser.
        tokens = re.findall(r'https?://[^\s<>"\']+|//[^\s<>"\']+|(?:[A-Za-z0-9.-]+(?::\d+)?/[^\s<>"\']+)', line)
        if not tokens and "://" in line:
            tokens = [line]
        for token in tokens:
            try:
                url = normalize(token)
                if url and url not in result:
                    result.append(url)
            except Exception:
                continue
    return result


def self_test() -> None:
    parsed = parse_hls(
        '#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=4716800,RESOLUTION=1920x1080,CODECS="avc1,mp4a"\n'
        'https://stream.example/a.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=640x360\n'
        'https://stream.example/a.m3u8\n',
        "https://example.org/master.m3u8",
    )
    assert parsed["valid_header"]
    assert len(parsed["variants"]) == 2
    assert len(parsed["duplicate_variant_uris"]) == 1
    assert normalize("example.org/a.m3u8") == "https://example.org/a.m3u8"
    assert "REDACTED" in redact("https://example.org/x?token=abc")
    m3u = parse_m3u(
        '#EXTM3U\n#EXTINF:-1 tvg-id="x" tvg-name="Test" group-title="News",Test Channel\n'
        'https://example.org/test.m3u8\n',
        "https://example.org/list.m3u",
    )
    assert m3u["entry_count"] == 1
    assert m3u["entries"][0]["attributes"]["tvg-name"] == "Test"
    assert m3u["entries"][0]["extinf"].startswith("#EXTINF:")
    media = parse_hls(
        "#EXTM3U\n#EXTINF:6.5,segment\n#EXT-X-BYTERANGE:100@0\nseg.ts\n#EXT-X-ENDLIST",
        "https://example.org/live.m3u8",
    )
    assert media["declared_duration_seconds"] == 6.5
    assert media["byte_ranges"] == ["100@0"]
    print(f"Phoenix URL Dissector {VERSION}: self-test OK")


def write_workflow() -> None:
    path = Path(".github/workflows/phoenix-url-dissector.yml")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """name: Phoenix URL Dissector
on:
  workflow_dispatch:
  push:
    paths:
      - 'URL_Dessector.py'
      - 'sources.txt'
      - '.github/workflows/phoenix-url-dissector.yml'

permissions:
  contents: read

jobs:
  scan:
    runs-on: ubuntu-latest
    # GitHub-hosted runner has its own hard job-duration ceiling.
    timeout-minutes: 360
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Self-test
        run: python -u URL_Dessector.py --self-test
      - name: Recursive discovery and diagnostics
        run: |
          set -euo pipefail
          mkdir -p reports
          python -u URL_Dessector.py \\
            --discover-livem3u \\
            --timeout 30 \\
            --max-bytes 0 \\
            --max-total-bytes 0 \\
            --max-runtime 0 \\
            --max-requests 0 \\
            --max-depth 0 \\
            --max-children 0 \\
            --max-sources 0 \\
            --max-segments 0 \\
            --hls-variant-policy all \\
            --check-hls-segment \\
            --output-dir reports \\
            --name livem3u_full
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: phoenix-url-dissector-reports
          path: reports/
          if-no-files-found: warn
""",
        encoding="utf-8",
    )
    print(f"Создан {path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phoenix URL Dissector — recursive LiveM3U/M3U/HLS/DASH diagnostics"
    )
    parser.add_argument("--url", action="append", help="URL; repeat for multiple sources")
    parser.add_argument("--input-file", help="File containing source URLs")
    parser.add_argument("--stdin", action="store_true", help="Read URLs from stdin")
    parser.add_argument("--discover-livem3u", action="store_true", help="Scan both default LiveM3U roots")
    parser.add_argument("--mode", choices=("quick", "deep", "hls"), default="deep")
    parser.add_argument("--origin", help="Explicit Origin to test")
    parser.add_argument("--timeout", type=float, default=30)
    # Zero means unlimited for all application-level limits.
    parser.add_argument("--max-bytes", type=int, default=0, help="Per-response byte cap; 0 = unlimited")
    parser.add_argument("--max-total-bytes", type=int, default=0, help="Total downloaded bytes; 0 = unlimited")
    parser.add_argument("--max-runtime", type=float, default=0, help="Runtime cap in seconds; 0 = unlimited")
    parser.add_argument("--max-requests", type=int, default=0, help="HTTP request cap; 0 = unlimited")
    parser.add_argument("--max-depth", type=int, default=0, help="Recursive depth cap; 0 = unlimited")
    parser.add_argument("--max-children", type=int, default=0, help="Children cap per resource; 0 = unlimited")
    parser.add_argument("--max-sources", type=int, default=0, help="Unique source cap; 0 = unlimited")
    parser.add_argument("--max-segments", type=int, default=0, help="Segments per HLS media playlist; 0 = all")
    parser.add_argument("--segment-probe-bytes", type=int, default=1048576,
                        help="Maximum bytes read when probing an HLS segment; default 1 MiB")
    parser.add_argument("--max-redirects", type=int, default=20)
    parser.add_argument("--hls-variant-policy", choices=("all", "best", "worst"), default="all")
    parser.add_argument("--check-hls-segment", action="store_true", help="Queue HLS segments for direct diagnostics")
    parser.add_argument("--user-agent", default=UA)
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--name", default="phoenix_report")
    parser.add_argument("--allow-private", action="store_true", help="Allow private IPs; trusted networks only")
    parser.add_argument("--insecure-tls", action="store_true", help="Disable certificate validation for diagnostics only")
    parser.add_argument("--init-actions", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--skip-extended-analysis", action="store_true", help="Skip second-phase validation and expanded exports")
    parser.add_argument("--run-ffprobe", action="store_true", help="Run ffprobe against HTTP-accessible terminal candidates in phase 2")
    parser.add_argument("--ffprobe-path", default="", help="Explicit ffprobe executable path or command name")
    parser.add_argument("--ffprobe-timeout", type=float, default=25.0, help="Timeout per ffprobe inspection, seconds")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        _extension_self_test()
        return 0
    if args.init_actions:
        write_workflow()
        return 0
    if args.timeout < 0 or args.max_redirects < 0 or args.segment_probe_bytes < 1 or args.ffprobe_timeout <= 0:
        parser.error("timeout должен быть >= 0 (0 = без явного тайм-аута), max-redirects >= 0 (0 = без лимита), segment-probe-bytes >= 1")
    for name in ("max_bytes", "max_total_bytes", "max_runtime", "max_requests",
                 "max_depth", "max_children", "max_sources", "max_segments"):
        if getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} должен быть >= 0")

    try:
        urls = load_urls(args)
    except Exception as exc:
        print(f"Ошибка входных данных: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    if not urls:
        parser.error("Укажи --url, --input-file или --discover-livem3u")

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    print(f"Phoenix URL Dissector {VERSION}", flush=True)
    print(f"ROOT SOURCES ({len(urls)}):", flush=True)
    for item in urls:
        print("  " + redact(item), flush=True)
    print(
        "Application limits: "
        f"bytes/response={args.max_bytes or 'UNLIMITED'}, "
        f"bytes/total={args.max_total_bytes or 'UNLIMITED'}, "
        f"runtime={args.max_runtime or 'UNLIMITED'}, "
        f"requests={args.max_requests or 'UNLIMITED'}, "
        f"depth={args.max_depth or 'UNLIMITED'}, "
        f"children={args.max_children or 'UNLIMITED'}, "
        f"sources={args.max_sources or 'UNLIMITED'}",
        flush=True,
    )
    scanner = Dissector(args)
    try:
        report = scanner.run(urls)
    except KeyboardInterrupt:
        print("Прервано пользователем; сохраняю текущий результат.", file=sys.stderr)
        report = {
            "tool": "Phoenix URL Dissector", "version": VERSION,
            "generated_at": now(), "mode": args.mode,
            "scan_complete": False, "stop_reason": "INTERRUPTED",
            "elapsed_seconds": round(time.monotonic() - scanner.started, 3),
            "settings": vars(args), "stats": {
                "urls_discovered": len(scanner.edge_records),
                "unique_urls_processed": len(scanner.records),
                "requests": scanner.client.request_count,
                "bytes_read": scanner.client.bytes_read,
                "queue_remaining": len(scanner.queue),
            },
            "sources": list(scanner.records.values()), "edges": scanner.edge_records,
            "channel_records": scanner.channel_records,
            "playlist_urls": sorted(redact(x) for x in scanner.playlist_urls),
            "terminal_urls": sorted(redact(x) for x in scanner.terminal_urls),
            "unresolved": scanner.unresolved, "errors": scanner.errors,
            "pending_queue": [
                {"url": redact(x["url"]), "raw_url": x["url"],
                 "parent": redact(x["parent"]) if x.get("parent") else None,
                 "depth": x["depth"], "kind": x["kind"]}
                for x in scanner.queue
            ],
        }
        write_outputs(report, args.output_dir, args.name)
        return 130

    write_outputs(report, args.output_dir, args.name)
    if not args.skip_extended_analysis:
        try:
            extended = write_extended_analysis(
                report, args.output_dir, args.name, args.ffprobe_path,
                args.run_ffprobe, args.ffprobe_timeout
            )
            report["extended_analysis"] = {
                "audit_findings": extended["audit"].get("finding_count", 0),
                "terminal_validation_count": extended["terminal_validation"].get("candidate_count", 0),
                "exports": extended.get("exports", []),
            }
            AtomicWriter.json(Path(args.output_dir) / f"{args.name}_final.json", report)
        except Exception as exc:
            print(f"[WARN] Расширенный анализ завершился ошибкой: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            # Keep the primary scan report even if a sidecar analysis fails.
            AtomicWriter.json(Path(args.output_dir) / f"{args.name}_extended_error.json", {
                "generated_at": now(), "error": f"{type(exc).__name__}: {exc}"
            })
    print("\nSCAN SUMMARY", flush=True)
    print(json.dumps(report["stats"], ensure_ascii=False, indent=2), flush=True)
    print(f"scan_complete={report['scan_complete']} stop_reason={report['stop_reason']}", flush=True)
    print(f"Reports written to: {args.output_dir}", flush=True)
    return 0 if report["scan_complete"] else 2


# ---------------------------------------------------------------------------
# Phoenix URL Dissector — extended analysis, validation and export subsystem
# ---------------------------------------------------------------------------
# This subsystem is intentionally part of the same file. It does not replace
# the original HTTP/DNS/TCP/TLS and parsing logic above; it enriches its output.
# ---------------------------------------------------------------------------

import csv as _csv
import hashlib as _hashlib
import shutil as _shutil
import subprocess as _subprocess
import tempfile as _tempfile
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from dataclasses import dataclass as _dataclass
from typing import Callable as _Callable


EXTENSION_SCHEMA_VERSION = "1.0.0"


@_dataclass
class ValidationResult:
    """Normalized second-phase result for a terminal media candidate."""
    url: str
    final_url: str = ""
    status: str = "NOT_CHECKED"
    http_status: Optional[int] = None
    content_type: str = ""
    content_length: Optional[int] = None
    byte_range_supported: Optional[bool] = None
    ffprobe_available: bool = False
    ffprobe_ok: Optional[bool] = None
    media_format: str = ""
    video_streams: int = 0
    audio_streams: int = 0
    subtitle_streams: int = 0
    duration_seconds: Optional[float] = None
    probe_error: str = ""
    network_error: str = ""
    source_record_status: str = ""
    checked_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        data = dict(self.__dict__)
        data["url"] = redact(data["url"])
        data["final_url"] = redact(data["final_url"]) if data["final_url"] else ""
        return data


class AtomicWriter:
    """Atomic UTF-8 writer; a failed write leaves the previous file intact."""

    @staticmethod
    def bytes(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = _tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        except Exception:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise

    @classmethod
    def text(cls, path: Path, payload: str) -> None:
        cls.bytes(path, payload.encode("utf-8", errors="replace"))

    @classmethod
    def json(cls, path: Path, payload: Any) -> None:
        cls.text(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n")

    @classmethod
    def jsonl(cls, path: Path, payload: Iterable[Any]) -> None:
        cls.text(path, "".join(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n" for item in payload))


class URLCanonicalizer:
    """Conservative URL canonicalization for identity and graph indexing.

    It normalizes scheme/host case, removes default ports and fragments, and
    preserves path/query ordering and all query values. Query parameters are
    deliberately not sorted or discarded because signed media URLs may depend
    on their original ordering and repetition.
    """

    @staticmethod
    def canonical(url: str, base: Optional[str] = None) -> str:
        value = normalize(url, base)
        parsed = urllib.parse.urlsplit(value)
        scheme = parsed.scheme.lower()
        host = (parsed.hostname or "").lower()
        if not host:
            raise ValueError("URL has no hostname")
        if ":" in host and not host.startswith("["):
            host = "[" + host + "]"
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("invalid URL port") from exc
        if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
            host = f"{host}:{port}"
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("userinfo in URL is not accepted for canonical identity")
        path = parsed.path or "/"
        return urllib.parse.urlunsplit((scheme, host, path, parsed.query, ""))

    @staticmethod
    def identity_hash(url: str) -> str:
        canonical = URLCanonicalizer.canonical(url)
        return _hashlib.sha256(canonical.encode("utf-8", errors="replace")).hexdigest()


class ProvenanceLedger:
    """Indexes discovered resources without deduplicating provenance edges."""

    def __init__(self) -> None:
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[Dict[str, Any]] = []
        self._edge_keys: Set[Tuple[str, str, str, str]] = set()

    def add_node(self, url: str, resource_type: str = "unknown", **metadata: Any) -> str:
        key = URLCanonicalizer.canonical(url)
        node = self.nodes.setdefault(key, {
            "canonical_url": redact(key),
            "canonical_url_raw": key,
            "resource_types": [],
            "observations": 0,
            "metadata": {},
        })
        if resource_type and resource_type not in node["resource_types"]:
            node["resource_types"].append(resource_type)
        node["observations"] += 1
        for name, value in metadata.items():
            if value is not None and name not in node["metadata"]:
                node["metadata"][name] = value
        return key

    def add_edge(self, source: Optional[str], target: str, relation: str, **metadata: Any) -> None:
        source_key = "" if not source else URLCanonicalizer.canonical(source)
        target_key = URLCanonicalizer.canonical(target)
        key = (source_key, target_key, relation, str(metadata.get("channel_index", "")))
        if key in self._edge_keys:
            return
        self._edge_keys.add(key)
        self.edges.append({
            "source": redact(source_key) if source_key else None,
            "source_raw": source_key or None,
            "target": redact(target_key),
            "target_raw": target_key,
            "relation": relation,
            "metadata": metadata,
        })

    def ingest_report(self, report: Dict[str, Any]) -> Dict[str, Any]:
        for source in report.get("sources", []):
            raw_url = source.get("raw_url") or source.get("url")
            if not raw_url:
                continue
            try:
                key = self.add_node(raw_url, source.get("resource_type_detected") or source.get("resource_type", "unknown"),
                                    status_label=source.get("status_label"), http_status=source.get("http_status"))
            except Exception:
                continue
            parent = source.get("parent")
            if parent:
                try:
                    self.add_edge(parent, key, source.get("resource_type", "discovered"), depth=source.get("depth"))
                except Exception:
                    pass
        for edge in report.get("edges", []):
            target = edge.get("raw_url") or edge.get("url")
            parent = edge.get("parent")
            if not target:
                continue
            try:
                self.add_node(target, edge.get("kind", "discovered"))
                self.add_edge(parent, target, edge.get("kind", "discovered"),
                              depth=edge.get("depth"), channel=edge.get("channel"))
            except Exception:
                continue
        return self.to_dict()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": EXTENSION_SCHEMA_VERSION,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "nodes": list(self.nodes.values()),
            "edges": self.edges,
        }


class GraphAnalyzer:
    """Graph statistics and structural diagnostics over discovered source edges."""

    def __init__(self, report: Dict[str, Any]):
        self.report = report
        self.adjacency: Dict[str, Set[str]] = collections.defaultdict(set)
        self.reverse: Dict[str, Set[str]] = collections.defaultdict(set)
        self.node_types: Dict[str, Set[str]] = collections.defaultdict(set)
        self.statuses: Dict[str, str] = {}
        self.hosts: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        for record in self.report.get("sources", []):
            url = record.get("raw_url") or record.get("url")
            if not url:
                continue
            try:
                key = URLCanonicalizer.canonical(url)
            except Exception:
                continue
            self.node_types[key].add(record.get("resource_type_detected") or record.get("resource_type", "unknown"))
            self.statuses[key] = record.get("status_label", "UNKNOWN")
            host = (urllib.parse.urlsplit(key).hostname or "").lower()
            host_data = self.hosts.setdefault(host, {"host": host, "resources": 0, "ok": 0, "failed": 0, "statuses": collections.Counter()})
            host_data["resources"] += 1
            status = record.get("status_label", "UNKNOWN")
            host_data["statuses"][status] += 1
            if status in ("BROKEN", "UNKNOWN", "ACCESS_RESTRICTED", "PARTIAL"):
                host_data["failed"] += 1
            else:
                host_data["ok"] += 1
        for edge in self.report.get("edges", []):
            source = edge.get("parent")
            target = edge.get("raw_url") or edge.get("url")
            if not source or not target:
                continue
            try:
                left = URLCanonicalizer.canonical(source)
                right = URLCanonicalizer.canonical(target)
            except Exception:
                continue
            self.adjacency[left].add(right)
            self.reverse[right].add(left)

    def reachable_counts(self) -> Dict[str, int]:
        roots = []
        for record in self.report.get("sources", []):
            if record.get("resource_type") == "root":
                url = record.get("raw_url") or record.get("url")
                if url:
                    try:
                        roots.append(URLCanonicalizer.canonical(url))
                    except Exception:
                        pass
        visited: Set[str] = set()
        queue = collections.deque(roots)
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            queue.extend(self.adjacency.get(node, set()) - visited)
        all_nodes = set(self.statuses) | set(self.adjacency) | set(self.reverse)
        return {"root_count": len(roots), "reachable_nodes": len(visited), "known_nodes": len(all_nodes),
                "unreachable_known_nodes": len(all_nodes - visited)}

    def find_cycles(self, maximum: int = 1000) -> List[List[str]]:
        """Return representative directed cycles; stop after maximum results."""
        color: Dict[str, int] = {}
        stack: List[str] = []
        positions: Dict[str, int] = {}
        cycles: List[List[str]] = []

        def visit(node: str) -> None:
            if len(cycles) >= maximum:
                return
            color[node] = 1
            positions[node] = len(stack)
            stack.append(node)
            for child in self.adjacency.get(node, set()):
                if len(cycles) >= maximum:
                    break
                if color.get(child, 0) == 0:
                    visit(child)
                elif color.get(child) == 1:
                    start = positions.get(child, 0)
                    cycle = stack[start:] + [child]
                    cycles.append([redact(item) for item in cycle])
            stack.pop()
            positions.pop(node, None)
            color[node] = 2

        nodes = set(self.adjacency)
        for children in self.adjacency.values():
            nodes.update(children)
        for node in sorted(nodes):
            if color.get(node, 0) == 0:
                visit(node)
            if len(cycles) >= maximum:
                break
        return cycles

    def host_summary(self) -> List[Dict[str, Any]]:
        result = []
        for host, item in self.hosts.items():
            result.append({
                "host": host,
                "resources": item["resources"],
                "ok": item["ok"],
                "failed": item["failed"],
                "failure_ratio": round(item["failed"] / item["resources"], 6) if item["resources"] else 0,
                "statuses": dict(item["statuses"]),
            })
        return sorted(result, key=lambda row: (-row["resources"], row["host"]))

    def status_summary(self) -> Dict[str, int]:
        return dict(collections.Counter(self.statuses.values()))

    def type_summary(self) -> Dict[str, int]:
        counter: collections.Counter = collections.Counter()
        for types in self.node_types.values():
            for value in types:
                counter[value] += 1
        return dict(counter)

    def analyze(self) -> Dict[str, Any]:
        return {
            "schema_version": EXTENSION_SCHEMA_VERSION,
            "graph": self.reachable_counts(),
            "status_counts": self.status_summary(),
            "resource_type_counts": self.type_summary(),
            "cycles": self.find_cycles(),
            "hosts": self.host_summary(),
            "duplicate_edge_count": max(0, len(self.report.get("edges", [])) - len({
                (item.get("parent"), item.get("raw_url") or item.get("url"), item.get("kind"))
                for item in self.report.get("edges", [])
            })),
        }


class FFProbeRunner:
    """Optional ffprobe bootstrap and structured media inspection."""

    CANDIDATES = ("ffprobe", "/usr/bin/ffprobe", "/usr/local/bin/ffprobe")

    def __init__(self, explicit_path: str = ""):
        self.explicit_path = explicit_path
        self.path = self._find()

    def _find(self) -> Optional[str]:
        if self.explicit_path:
            return self.explicit_path if Path(self.explicit_path).exists() else _shutil.which(self.explicit_path)
        for item in self.CANDIDATES:
            found = _shutil.which(item) if "/" not in item else (item if Path(item).exists() else None)
            if found:
                return found
        return None

    @property
    def available(self) -> bool:
        return bool(self.path)

    def version(self) -> Dict[str, Any]:
        if not self.path:
            return {"available": False, "path": None, "version": None, "error": "ffprobe not found"}
        try:
            completed = _subprocess.run([self.path, "-version"], capture_output=True, text=True,
                                        timeout=10, check=False)
            first = (completed.stdout or completed.stderr).splitlines()
            return {"available": completed.returncode == 0, "path": self.path,
                    "version": first[0] if first else "unknown", "returncode": completed.returncode}
        except Exception as exc:
            return {"available": False, "path": self.path, "error": f"{type(exc).__name__}: {exc}"}

    def inspect_url(self, url: str, timeout: float = 25.0) -> Dict[str, Any]:
        if not self.path:
            return {"available": False, "ok": None, "error": "ffprobe not installed"}
        command = [self.path, "-v", "error", "-show_entries",
                   "format=format_name,duration,size,bit_rate:stream=index,codec_type,codec_name,width,height,channels,sample_rate",
                   "-of", "json", "-rw_timeout", str(max(1, int(timeout * 1_000_000)),),
                   "-headers", f"User-Agent: {UA}\r\n", url]
        started = time.monotonic()
        try:
            completed = _subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
            try:
                payload = json.loads(completed.stdout or "{}")
            except json.JSONDecodeError:
                payload = {}
            streams = payload.get("streams", []) if isinstance(payload, dict) else []
            fmt = payload.get("format", {}) if isinstance(payload, dict) else {}
            duration = None
            try:
                duration = float(fmt.get("duration")) if fmt.get("duration") not in (None, "N/A", "") else None
            except (TypeError, ValueError):
                pass
            return {
                "available": True, "ok": completed.returncode == 0,
                "returncode": completed.returncode,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "format_name": fmt.get("format_name", ""), "duration_seconds": duration,
                "size": fmt.get("size"), "bit_rate": fmt.get("bit_rate"),
                "streams": streams,
                "video_streams": sum(1 for item in streams if item.get("codec_type") == "video"),
                "audio_streams": sum(1 for item in streams if item.get("codec_type") == "audio"),
                "subtitle_streams": sum(1 for item in streams if item.get("codec_type") == "subtitle"),
                "stderr": (completed.stderr or "")[-8000:],
            }
        except _subprocess.TimeoutExpired:
            return {"available": True, "ok": False, "timeout": True,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "error": f"ffprobe timeout after {timeout}s"}
        except Exception as exc:
            return {"available": True, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


class SecondPhaseValidator:
    """Builds a terminal-candidate validation table from discovery results.

    The discovery phase is intentionally completed first. This validator does
    not stop recursive discovery when a stream is dead or slow. It uses the
    first-pass HTTP record and optionally runs ffprobe after the graph exists.
    """

    TERMINAL_KINDS = {
        "binary_or_media", "hls_segment", "dash_segment", "terminal_probe",
        "playlist_entry", "wrapper_target", "embedded_url",
    }

    def __init__(self, report: Dict[str, Any], ffprobe_path: str = "", run_ffprobe: bool = False,
                 ffprobe_timeout: float = 25.0):
        self.report = report
        self.ffprobe = FFProbeRunner(ffprobe_path)
        self.run_ffprobe = run_ffprobe
        self.ffprobe_timeout = ffprobe_timeout
        self.records_by_url: Dict[str, Dict[str, Any]] = {}
        for record in report.get("sources", []):
            raw = record.get("raw_url") or record.get("url")
            if raw:
                try:
                    self.records_by_url[URLCanonicalizer.canonical(raw)] = record
                except Exception:
                    continue

    def _is_terminal_candidate(self, record: Dict[str, Any]) -> bool:
        detected = record.get("resource_type_detected", "")
        kind = record.get("resource_type", "")
        if detected in ("m3u_playlist", "hls_master_playlist", "hls_media_playlist", "dash_mpd"):
            return False
        if record.get("status_label") == "ACCESSIBLE_TERMINAL_CANDIDATE":
            return True
        if kind in ("hls_segment", "dash_segment", "terminal_probe"):
            return True
        content_type = str(record.get("content_type", "")).lower()
        if any(token in content_type for token in ("video/", "audio/", "octet-stream", "mp2t")):
            return record.get("http_status") is not None and 200 <= int(record.get("http_status") or 0) < 400
        return False

    def validate_one(self, raw_url: str) -> Dict[str, Any]:
        try:
            key = URLCanonicalizer.canonical(raw_url)
        except Exception as exc:
            return ValidationResult(url=raw_url, status="INVALID_URL", network_error=str(exc), checked_at=now()).to_dict()
        record = self.records_by_url.get(key, {})
        status = record.get("http_status")
        content_type = str(record.get("content_type", ""))
        content_length = None
        headers = record.get("response_headers", {}) or {}
        for header, value in headers.items():
            if header.lower() == "content-length":
                try:
                    content_length = int(value)
                except (TypeError, ValueError):
                    pass
        final_raw = record.get("final_url_raw") or record.get("raw_url") or raw_url
        label = record.get("status_label", "UNKNOWN")
        if label in ("BROKEN", "UNKNOWN", "ACCESS_RESTRICTED"):
            normalized = "UNAVAILABLE_OR_RESTRICTED"
        elif status is not None and 200 <= int(status) < 300:
            normalized = "HTTP_ACCESSIBLE"
        elif status is not None and 300 <= int(status) < 400:
            normalized = "REDIRECT_ONLY"
        else:
            normalized = "UNVERIFIED"
        result = ValidationResult(
            url=raw_url, final_url=final_raw, status=normalized,
            http_status=status, content_type=content_type,
            content_length=content_length,
            byte_range_supported=self._range_support(record),
            ffprobe_available=self.ffprobe.available,
            source_record_status=label, checked_at=now(),
            network_error="; ".join(record.get("errors", [])) if normalized == "UNAVAILABLE_OR_RESTRICTED" else "",
        )
        payload = result.to_dict()
        if self.run_ffprobe and self.ffprobe.available and normalized == "HTTP_ACCESSIBLE":
            probe = self.ffprobe.inspect_url(final_raw, self.ffprobe_timeout)
            payload["ffprobe"] = probe
            payload["ffprobe_ok"] = probe.get("ok")
            payload["media_format"] = probe.get("format_name", "")
            payload["duration_seconds"] = probe.get("duration_seconds")
            payload["video_streams"] = probe.get("video_streams", 0)
            payload["audio_streams"] = probe.get("audio_streams", 0)
            payload["subtitle_streams"] = probe.get("subtitle_streams", 0)
            if probe.get("ok"):
                payload["status"] = "FFPROBE_MEDIA_CONFIRMED"
            elif probe.get("available"):
                payload["status"] = "HTTP_ACCESSIBLE_FFPROBE_UNCONFIRMED"
        return payload

    @staticmethod
    def _range_support(record: Dict[str, Any]) -> Optional[bool]:
        for attempt in record.get("attempts", []):
            strategy = str(attempt.get("strategy", "")).lower()
            headers = attempt.get("headers", {}) or {}
            if "range" in strategy:
                status = attempt.get("status")
                if status == 206:
                    return True
                if status in (200, 416):
                    return False
            for key, value in headers.items():
                if key.lower() == "accept-ranges" and str(value).lower() == "bytes":
                    return True
        return None

    def run(self) -> Dict[str, Any]:
        candidates = []
        seen = set()
        # terminal_urls in the human-facing report are redacted. Resolve them
        # back to raw URLs through source records before attempting any probe.
        raw_by_display: Dict[str, str] = {}
        for source in self.report.get("sources", []):
            raw_source = source.get("raw_url") or source.get("url")
            display_source = source.get("url")
            if raw_source and display_source:
                raw_by_display[str(display_source)] = str(raw_source)
        for displayed in self.report.get("terminal_urls", []):
            raw = raw_by_display.get(str(displayed), str(displayed))
            try:
                key = URLCanonicalizer.canonical(raw)
            except Exception:
                continue
            if key not in seen:
                seen.add(key)
                candidates.append(key)
        for record in self.report.get("sources", []):
            raw = record.get("raw_url") or record.get("url")
            if raw and self._is_terminal_candidate(record):
                try:
                    key = URLCanonicalizer.canonical(raw)
                except Exception:
                    continue
                if key not in seen:
                    seen.add(key)
                    candidates.append(key)
        results = [self.validate_one(url) for url in candidates]
        counts = collections.Counter(item.get("status", "UNKNOWN") for item in results)
        return {
            "schema_version": EXTENSION_SCHEMA_VERSION,
            "generated_at": now(),
            "ffprobe": self.ffprobe.version(),
            "ffprobe_enabled": self.run_ffprobe,
            "candidate_count": len(candidates),
            "status_counts": dict(counts),
            "results": results,
        }


class ReportAuditor:
    """Checks report consistency and emits actionable, non-destructive findings."""

    def __init__(self, report: Dict[str, Any]):
        self.report = report
        self.findings: List[Dict[str, Any]] = []

    def finding(self, severity: str, code: str, message: str, **context: Any) -> None:
        self.findings.append({"severity": severity, "code": code, "message": message, "context": context})

    def audit_sources(self) -> None:
        sources = self.report.get("sources", [])
        seen: Dict[str, int] = collections.Counter()
        for record in sources:
            raw = record.get("raw_url") or record.get("url")
            if raw:
                try:
                    seen[URLCanonicalizer.canonical(raw)] += 1
                except Exception:
                    self.finding("warning", "INVALID_SOURCE_URL", "Source URL could not be canonicalized", url=str(raw))
            status = record.get("status_label")
            if status == "PLAYLIST_PARSED" and not (record.get("m3u") or record.get("hls")):
                self.finding("warning", "PLAYLIST_WITHOUT_PARSE_PAYLOAD", "Playlist status has no parser payload", url=record.get("url"))
            if record.get("http_status") in (401, 403) and status != "ACCESS_RESTRICTED":
                self.finding("warning", "ACCESS_STATUS_MISMATCH", "HTTP access restriction is not reflected in status_label", url=record.get("url"), status=status)
            if record.get("body_truncated") and record.get("resource_type_detected") in ("m3u_playlist", "hls_master_playlist", "hls_media_playlist", "dash_mpd"):
                self.finding("warning", "TRUNCATED_MANIFEST", "Manifest body was truncated; discovery may be incomplete", url=record.get("url"))
        duplicates = {url: count for url, count in seen.items() if count > 1}
        for url, count in duplicates.items():
            self.finding("warning", "DUPLICATE_SOURCE_RECORD", "Canonical URL occurs in multiple source records", url=redact(url), count=count)

    def audit_channels(self) -> None:
        channels = self.report.get("channel_records", [])
        for index, item in enumerate(channels):
            if not item.get("raw_url") and not item.get("url"):
                self.finding("error", "CHANNEL_WITHOUT_URL", "Channel record is missing URL", index=index)
            if not item.get("original_extinf"):
                self.finding("warning", "CHANNEL_WITHOUT_EXTINF", "Channel record has no original EXTINF", index=index, name=item.get("channel_name"))
            attrs = item.get("channel_attributes")
            if attrs is not None and not isinstance(attrs, dict):
                self.finding("error", "CHANNEL_ATTRIBUTES_NOT_OBJECT", "Channel attributes must be a mapping", index=index)

    def audit_edges(self) -> None:
        known = set()
        for record in self.report.get("sources", []):
            raw = record.get("raw_url") or record.get("url")
            if raw:
                try:
                    known.add(URLCanonicalizer.canonical(raw))
                except Exception:
                    pass
        for index, edge in enumerate(self.report.get("edges", [])):
            raw = edge.get("raw_url") or edge.get("url")
            if not raw:
                self.finding("error", "EDGE_WITHOUT_TARGET", "Discovery edge has no target URL", index=index)
                continue
            try:
                key = URLCanonicalizer.canonical(raw)
            except Exception:
                self.finding("warning", "EDGE_INVALID_TARGET", "Discovery edge target is invalid", index=index, url=raw)
                continue
            if key not in known:
                self.finding("info", "EDGE_TARGET_NOT_PROCESSED", "Edge target was discovered but has no processed source record", index=index, url=redact(key))

    def audit_outputs(self) -> None:
        stats = self.report.get("stats", {})
        sources = self.report.get("sources", [])
        if stats.get("unique_urls_processed") not in (None, len(sources)):
            self.finding("warning", "SOURCE_COUNT_MISMATCH", "stats.unique_urls_processed differs from source count",
                         stats_value=stats.get("unique_urls_processed"), actual=len(sources))
        if self.report.get("scan_complete") is False and not self.report.get("stop_reason"):
            self.finding("warning", "INCOMPLETE_WITHOUT_REASON", "Scan is incomplete but stop_reason is empty")
        if self.report.get("pending_queue") and self.report.get("scan_complete"):
            self.finding("warning", "COMPLETE_WITH_PENDING_QUEUE", "Scan is marked complete while queue has pending tasks",
                         pending=len(self.report.get("pending_queue", [])))

    def run(self) -> Dict[str, Any]:
        self.audit_sources()
        self.audit_channels()
        self.audit_edges()
        self.audit_outputs()
        counts = collections.Counter(item["severity"] for item in self.findings)
        return {"schema_version": EXTENSION_SCHEMA_VERSION, "generated_at": now(),
                "finding_count": len(self.findings), "severity_counts": dict(counts),
                "findings": self.findings}


class ExtendedExporter:
    """Writes focused, machine-readable sidecar exports without dropping data."""

    def __init__(self, output_dir: str, name: str = "phoenix_report"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "phoenix_report"

    def path(self, suffix: str) -> Path:
        return self.output_dir / f"{self.stem}_{suffix}"

    def write_sources_jsonl(self, report: Dict[str, Any]) -> Path:
        path = self.path("sources.jsonl")
        AtomicWriter.jsonl(path, report.get("sources", []))
        return path

    def write_edges_jsonl(self, report: Dict[str, Any]) -> Path:
        path = self.path("edges.jsonl")
        AtomicWriter.jsonl(path, report.get("edges", []))
        return path

    def write_all_urls(self, report: Dict[str, Any]) -> Path:
        urls: Dict[str, Dict[str, Any]] = {}
        for record in report.get("sources", []):
            raw = record.get("raw_url") or record.get("url")
            if not raw:
                continue
            try:
                key = URLCanonicalizer.canonical(raw)
            except Exception:
                continue
            row = urls.setdefault(key, {"url": redact(key), "raw_url": key, "source_types": [], "statuses": [], "parents": []})
            kind = record.get("resource_type_detected") or record.get("resource_type", "unknown")
            if kind not in row["source_types"]:
                row["source_types"].append(kind)
            status = record.get("status_label", "UNKNOWN")
            if status not in row["statuses"]:
                row["statuses"].append(status)
            if record.get("parent") and record["parent"] not in row["parents"]:
                row["parents"].append(record["parent"])
        path = self.path("all_urls.jsonl")
        AtomicWriter.jsonl(path, urls.values())
        return path

    def write_channels_csv(self, report: Dict[str, Any]) -> Path:
        path = self.path("channels.csv")
        columns = ["channel_name", "url", "source_playlist", "parent_playlist", "original_extinf",
                   "channel_attributes_json", "channel_tags_json", "entry_index", "discovered_at_depth", "kind"]
        import io
        buffer = io.StringIO(newline="")
        writer = _csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for item in report.get("channel_records", []):
            row = dict(item)
            row["channel_attributes_json"] = json.dumps(row.get("channel_attributes", {}), ensure_ascii=False)
            row["channel_tags_json"] = json.dumps(row.get("channel_tags", []), ensure_ascii=False)
            writer.writerow({key: row.get(key, "") for key in columns})
        AtomicWriter.text(path, buffer.getvalue())
        return path

    def write_channels_m3u(self, report: Dict[str, Any]) -> Path:
        path = self.path("channels.m3u8")
        lines = ["#EXTM3U"]
        for item in report.get("channel_records", []):
            extinf = item.get("original_extinf") or "#EXTINF:-1," + str(item.get("channel_name") or "Unlabelled")
            raw_url = item.get("raw_url") or item.get("url")
            if not raw_url:
                continue
            lines.extend([str(extinf).rstrip("\r\n"), str(raw_url)])
        AtomicWriter.text(path, "\n".join(lines) + "\n")
        return path

    def write_hls_variants(self, report: Dict[str, Any]) -> Path:
        path = self.path("hls_variants.jsonl")
        rows = []
        for source in report.get("sources", []):
            hls = source.get("hls") or {}
            for variant in hls.get("variants", []):
                rows.append({"source_playlist": source.get("url"), "source_playlist_raw": source.get("raw_url"), **variant})
        AtomicWriter.jsonl(path, rows)
        return path

    def write_hls_segments(self, report: Dict[str, Any]) -> Path:
        path = self.path("hls_segments.jsonl")
        rows = []
        for source in report.get("sources", []):
            hls = source.get("hls") or {}
            for index, segment in enumerate(hls.get("segments", [])):
                rows.append({"source_playlist": source.get("url"), "index": index, "segment_url": redact(segment), "segment_url_raw": segment})
        AtomicWriter.jsonl(path, rows)
        return path

    def write_dash_resources(self, report: Dict[str, Any]) -> Path:
        path = self.path("dash_resources.jsonl")
        rows = []
        for source in report.get("sources", []):
            dash = source.get("dash") or {}
            for base in dash.get("base_urls", []):
                rows.append({"source_mpd": source.get("url"), "resource_type": "base_url", "url": redact(base), "raw_url": base})
            for representation in dash.get("representations", []):
                rows.append({"source_mpd": source.get("url"), "resource_type": "representation", **representation})
        AtomicWriter.jsonl(path, rows)
        return path

    def write_terminal_validation(self, validation: Dict[str, Any]) -> Path:
        path = self.path("terminal_validation.json")
        AtomicWriter.json(path, validation)
        return path

    def write_audit(self, audit: Dict[str, Any]) -> Path:
        path = self.path("report_audit.json")
        AtomicWriter.json(path, audit)
        return path

    def write_graph_analysis(self, analysis: Dict[str, Any]) -> Path:
        path = self.path("graph_analysis.json")
        AtomicWriter.json(path, analysis)
        return path

    def write_host_summary_csv(self, analysis: Dict[str, Any]) -> Path:
        path = self.path("host_summary.csv")
        import io
        buffer = io.StringIO(newline="")
        columns = ["host", "resources", "ok", "failed", "failure_ratio", "statuses_json"]
        writer = _csv.DictWriter(buffer, fieldnames=columns)
        writer.writeheader()
        for item in analysis.get("hosts", []):
            writer.writerow({"host": item.get("host"), "resources": item.get("resources"),
                             "ok": item.get("ok"), "failed": item.get("failed"),
                             "failure_ratio": item.get("failure_ratio"),
                             "statuses_json": json.dumps(item.get("statuses", {}), ensure_ascii=False)})
        AtomicWriter.text(path, buffer.getvalue())
        return path

    def write_unresolved_csv(self, report: Dict[str, Any]) -> Path:
        path = self.path("unresolved.csv")
        rows = report.get("unresolved", [])
        columns = sorted({key for row in rows if isinstance(row, dict) for key in row})
        if not columns:
            columns = ["url", "parent", "kind", "status_label", "error"]
        import io
        buffer = io.StringIO(newline="")
        writer = _csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value for key, value in row.items()})
        AtomicWriter.text(path, buffer.getvalue())
        return path

    def write_inventory(self, report: Dict[str, Any], analysis: Dict[str, Any], audit: Dict[str, Any],
                        validation: Dict[str, Any]) -> Path:
        path = self.path("inventory.json")
        inventory = {
            "tool": report.get("tool"), "version": report.get("version"),
            "generated_at": now(), "schema_version": EXTENSION_SCHEMA_VERSION,
            "scan_complete": report.get("scan_complete"), "stop_reason": report.get("stop_reason"),
            "stats": report.get("stats", {}), "graph": analysis.get("graph", {}),
            "status_counts": analysis.get("status_counts", {}),
            "resource_type_counts": analysis.get("resource_type_counts", {}),
            "host_count": len(analysis.get("hosts", [])),
            "audit_findings": audit.get("finding_count", 0),
            "terminal_candidate_count": validation.get("candidate_count", 0),
            "terminal_validation_counts": validation.get("status_counts", {}),
            "files": [],
        }
        AtomicWriter.json(path, inventory)
        return path

    def export_all(self, report: Dict[str, Any], analysis: Dict[str, Any], audit: Dict[str, Any],
                   validation: Dict[str, Any]) -> List[str]:
        paths = [
            self.write_sources_jsonl(report), self.write_edges_jsonl(report),
            self.write_all_urls(report), self.write_channels_csv(report),
            self.write_channels_m3u(report), self.write_hls_variants(report),
            self.write_hls_segments(report), self.write_dash_resources(report),
            self.write_terminal_validation(validation), self.write_audit(audit),
            self.write_graph_analysis(analysis), self.write_host_summary_csv(analysis),
            self.write_unresolved_csv(report), self.write_inventory(report, analysis, audit, validation),
        ]
        manifest = {"generated_at": now(), "files": [str(item) for item in paths]}
        manifest_path = self.path("export_manifest.json")
        AtomicWriter.json(manifest_path, manifest)
        paths.append(manifest_path)
        return [str(item) for item in paths]


class CheckpointStore:
    """Atomic scanner checkpoint serialization for future resume workflows.

    Checkpoints are snapshots, not a promise that all open HTTP transactions can
    be resumed. A restored queue contains only serializable URL work items.
    """

    FORMAT_VERSION = 1

    def __init__(self, path: str):
        self.path = Path(path)

    def save(self, scanner: Dissector, roots: Iterable[str], extra: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "format_version": self.FORMAT_VERSION,
            "saved_at": now(), "version": VERSION,
            "roots": list(roots),
            "started_elapsed_seconds": round(time.monotonic() - scanner.started, 3),
            "visited": sorted(scanner.visited),
            "records": scanner.records,
            "queue": list(scanner.queue),
            "channel_records": scanner.channel_records,
            "playlist_urls": sorted(scanner.playlist_urls),
            "terminal_urls": sorted(scanner.terminal_urls),
            "unresolved": scanner.unresolved,
            "errors": scanner.errors,
            "edge_records": scanner.edge_records,
            "processed_count": scanner.processed_count,
            "request_count": scanner.client.request_count,
            "bytes_read": scanner.client.bytes_read,
            "extra": extra or {},
        }
        AtomicWriter.json(self.path, payload)

    def load(self) -> Dict[str, Any]:
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("format_version") != self.FORMAT_VERSION:
            raise ValueError("Unsupported checkpoint format version")
        if payload.get("version") != VERSION:
            raise ValueError("Checkpoint version differs from running scanner version")
        return payload

    @staticmethod
    def restore(scanner: Dissector, payload: Dict[str, Any]) -> None:
        scanner.visited = set(payload.get("visited", []))
        scanner.records = dict(payload.get("records", {}))
        scanner.queue = collections.deque(payload.get("queue", []))
        scanner.channel_records = list(payload.get("channel_records", []))
        scanner.playlist_urls = set(payload.get("playlist_urls", []))
        scanner.terminal_urls = set(payload.get("terminal_urls", []))
        scanner.unresolved = list(payload.get("unresolved", []))
        scanner.errors = list(payload.get("errors", []))
        scanner.edge_records = list(payload.get("edge_records", []))
        scanner.processed_count = int(payload.get("processed_count", len(scanner.records)))
        # The HTTP client counters are diagnostic totals. They are restored so
        # optional explicitly configured caps, if any, remain coherent.
        scanner.client.request_count = int(payload.get("request_count", 0))
        scanner.client.bytes_read = int(payload.get("bytes_read", 0))


class URLReportIndex:
    """Fast secondary indexes over the report for programmatic use."""

    def __init__(self, report: Dict[str, Any]):
        self.report = report
        self.by_url: Dict[str, Dict[str, Any]] = {}
        self.by_host: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        self.by_status: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        self.by_type: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        self.by_parent: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        self._build()

    def _build(self) -> None:
        for record in self.report.get("sources", []):
            raw = record.get("raw_url") or record.get("url")
            if not raw:
                continue
            try:
                canonical = URLCanonicalizer.canonical(raw)
            except Exception:
                continue
            self.by_url[canonical] = record
            host = (urllib.parse.urlsplit(canonical).hostname or "").lower()
            self.by_host[host].append(record)
            self.by_status[record.get("status_label", "UNKNOWN")].append(record)
            kind = record.get("resource_type_detected") or record.get("resource_type", "unknown")
            self.by_type[kind].append(record)
            parent = record.get("parent")
            if parent:
                try:
                    self.by_parent[URLCanonicalizer.canonical(parent)].append(record)
                except Exception:
                    pass

    def get_url(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            return self.by_url.get(URLCanonicalizer.canonical(url))
        except Exception:
            return None

    def host_records(self, host: str) -> List[Dict[str, Any]]:
        return list(self.by_host.get(host.lower(), []))

    def status_records(self, status: str) -> List[Dict[str, Any]]:
        return list(self.by_status.get(status, []))

    def type_records(self, resource_type: str) -> List[Dict[str, Any]]:
        return list(self.by_type.get(resource_type, []))

    def children_of(self, url: str) -> List[Dict[str, Any]]:
        try:
            return list(self.by_parent.get(URLCanonicalizer.canonical(url), []))
        except Exception:
            return []

    def query(self, host: str = "", status: str = "", resource_type: str = "") -> List[Dict[str, Any]]:
        result = list(self.report.get("sources", []))
        if host:
            host = host.lower()
            result = [item for item in result if (urllib.parse.urlsplit(item.get("raw_url") or item.get("url", "")).hostname or "").lower() == host]
        if status:
            result = [item for item in result if item.get("status_label") == status]
        if resource_type:
            result = [item for item in result if item.get("resource_type_detected", item.get("resource_type")) == resource_type]
        return result


class FailureClusterer:
    """Groups failures into likely operational root causes."""

    PATTERNS = (
        ("DNS_FAILURE", ("name or service not known", "temporary failure in name resolution", "getaddrinfo failed", "nodename nor servname")),
        ("TCP_TIMEOUT", ("timed out", "timeout", "connection timed out")),
        ("CONNECTION_REFUSED", ("connection refused", "actively refused")),
        ("TLS_CERTIFICATE", ("certificate verify failed", "ssl: certificate", "tlsv1 alert")),
        ("TLS_HANDSHAKE", ("handshake failure", "ssl handshake", "wrong version number")),
        ("HTTP_401", ("http 401", "status 401")),
        ("HTTP_403", ("http 403", "status 403", "forbidden")),
        ("HTTP_404", ("http 404", "status 404", "not found")),
        ("HTTP_429", ("http 429", "too many requests")),
        ("HTTP_5XX", ("http 500", "http 502", "http 503", "http 504", "bad gateway", "service unavailable")),
        ("EMPTY_BODY", ("empty body", "empty response", "пустым телом")),
        ("INVALID_URL", ("invalid url", "unsupported url", "no hostname")),
        ("PRIVATE_ADDRESS_BLOCKED", ("private", "loopback", "link-local", "blocked by policy")),
    )

    @classmethod
    def classify(cls, message: str) -> str:
        normalized = (message or "").lower()
        for code, phrases in cls.PATTERNS:
            if any(phrase in normalized for phrase in phrases):
                return code
        return "OTHER"

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        groups: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
        for item in report.get("unresolved", []):
            message = str(item.get("error", ""))
            code = self.classify(message)
            groups[code].append({"url": item.get("url"), "parent": item.get("parent"),
                                 "status_label": item.get("status_label"), "error": message})
        for item in report.get("errors", []):
            message = str(item.get("error", ""))
            code = self.classify(message)
            groups[code].append({"url": item.get("url"), "error": message})
        return {"generated_at": now(), "group_count": len(groups),
                "counts": {key: len(value) for key, value in sorted(groups.items())},
                "groups": {key: value for key, value in sorted(groups.items())}}


class M3UIntegrityAnalyzer:
    """Checks M3U parser payloads for structural issues while retaining entries."""

    def analyze_playlist(self, source: Dict[str, Any]) -> Dict[str, Any]:
        payload = source.get("m3u") or {}
        entries = payload.get("entries", [])
        findings = []
        seen_urls: Dict[str, List[int]] = collections.defaultdict(list)
        seen_ids: Dict[str, List[int]] = collections.defaultdict(list)
        for position, entry in enumerate(entries):
            url = entry.get("url", "")
            if url:
                try:
                    seen_urls[URLCanonicalizer.canonical(url)].append(position)
                except Exception:
                    findings.append({"severity": "warning", "code": "INVALID_ENTRY_URL", "entry_index": position, "url": url})
            attrs = entry.get("attributes", {})
            tvg_id = attrs.get("tvg-id") if isinstance(attrs, dict) else None
            if tvg_id:
                seen_ids[str(tvg_id)].append(position)
            if not entry.get("extinf"):
                findings.append({"severity": "warning", "code": "MISSING_EXTINF", "entry_index": position})
            if not (entry.get("attributes", {}).get("display_name") if isinstance(entry.get("attributes"), dict) else None):
                findings.append({"severity": "info", "code": "MISSING_DISPLAY_NAME", "entry_index": position})
        duplicate_urls = {url: positions for url, positions in seen_urls.items() if len(positions) > 1}
        duplicate_ids = {key: positions for key, positions in seen_ids.items() if len(positions) > 1}
        if duplicate_urls:
            findings.append({"severity": "info", "code": "DUPLICATE_CHANNEL_URLS", "count": len(duplicate_urls), "details": duplicate_urls})
        if duplicate_ids:
            findings.append({"severity": "info", "code": "DUPLICATE_TVG_IDS", "count": len(duplicate_ids), "details": duplicate_ids})
        return {"source_url": source.get("url"), "valid_header": payload.get("valid_header"),
                "entry_count": len(entries), "duplicate_url_count": len(duplicate_urls),
                "duplicate_tvg_id_count": len(duplicate_ids), "findings": findings}

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        playlists = []
        for source in report.get("sources", []):
            if source.get("m3u"):
                playlists.append(self.analyze_playlist(source))
        return {"generated_at": now(), "playlist_count": len(playlists),
                "total_entries": sum(item["entry_count"] for item in playlists),
                "playlists": playlists}


class HLSIntegrityAnalyzer:
    """Aggregates HLS-specific findings from parsed manifests."""

    def analyze_manifest(self, source: Dict[str, Any]) -> Dict[str, Any]:
        hls = source.get("hls") or {}
        findings = []
        variants = hls.get("variants", [])
        segments = hls.get("segments", [])
        if hls.get("is_master") and not variants:
            findings.append({"severity": "warning", "code": "MASTER_WITHOUT_VARIANTS"})
        if hls.get("is_media") and not segments and not hls.get("is_endlist"):
            findings.append({"severity": "warning", "code": "LIVE_MEDIA_WITHOUT_SEGMENTS"})
        if hls.get("is_endlist") and not segments:
            findings.append({"severity": "warning", "code": "VOD_MEDIA_EMPTY"})
        bandwidths = []
        for variant in variants:
            try:
                bandwidths.append(int(variant.get("bandwidth") or 0))
            except (ValueError, TypeError):
                pass
        if len(bandwidths) != len(set(bandwidths)) and len(bandwidths) > 1:
            findings.append({"severity": "info", "code": "DUPLICATE_VARIANT_BANDWIDTH"})
        if hls.get("encryption_methods"):
            methods = set(hls.get("encryption_methods") or [])
            if methods - {"NONE", "AES-128", "SAMPLE-AES", "SAMPLE-AES-CTR"}:
                findings.append({"severity": "info", "code": "UNCOMMON_ENCRYPTION_METHOD", "methods": sorted(methods)})
        return {"source_url": source.get("url"), "is_master": hls.get("is_master"),
                "is_media": hls.get("is_media"), "variant_count": len(variants),
                "rendition_count": len(hls.get("renditions", [])), "segment_count": len(segments),
                "declared_duration_seconds": hls.get("declared_duration_seconds"),
                "encryption_methods": hls.get("encryption_methods", []), "findings": findings}

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        manifests = [self.analyze_manifest(source) for source in report.get("sources", []) if source.get("hls")]
        return {"generated_at": now(), "manifest_count": len(manifests),
                "master_count": sum(1 for item in manifests if item.get("is_master")),
                "media_count": sum(1 for item in manifests if item.get("is_media")),
                "manifests": manifests}


class DASHIntegrityAnalyzer:
    """Aggregates DASH MPD parser output and representation-level checks."""

    def analyze_mpd(self, source: Dict[str, Any]) -> Dict[str, Any]:
        dash = source.get("dash") or {}
        findings = []
        if not dash.get("valid_xml"):
            findings.append({"severity": "warning", "code": "INVALID_MPD_XML"})
        if not dash.get("base_urls"):
            findings.append({"severity": "info", "code": "NO_EXPLICIT_BASE_URL"})
        representations = dash.get("representations", [])
        if not representations:
            findings.append({"severity": "warning", "code": "NO_REPRESENTATIONS_PARSED"})
        return {"source_url": source.get("url"), "valid_xml": dash.get("valid_xml"),
                "base_url_count": len(dash.get("base_urls", [])),
                "representation_count": len(representations), "findings": findings,
                "representations": representations}

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        manifests = [self.analyze_mpd(source) for source in report.get("sources", []) if source.get("dash")]
        return {"generated_at": now(), "manifest_count": len(manifests), "manifests": manifests}


class ResponseHeaderAnalyzer:
    """Extracts response header capabilities without retaining cookie secrets."""

    INTERESTING = {
        "server", "content-type", "content-length", "accept-ranges", "cache-control",
        "expires", "last-modified", "etag", "content-range", "content-encoding",
        "access-control-allow-origin", "access-control-allow-methods", "access-control-allow-headers",
        "via", "age", "x-cache", "x-cache-hits", "x-served-by", "cf-cache-status",
        "alt-svc", "connection", "keep-alive", "retry-after", "location",
    }

    def analyze_record(self, source: Dict[str, Any]) -> Dict[str, Any]:
        headers = source.get("response_headers", {}) or {}
        selected = {key.lower(): value for key, value in headers.items() if key.lower() in self.INTERESTING}
        capabilities = {
            "range_supported": str(selected.get("accept-ranges", "")).lower() == "bytes",
            "cors_wildcard": selected.get("access-control-allow-origin") == "*",
            "cache_present": bool(selected.get("cache-control") or selected.get("etag") or selected.get("last-modified")),
            "retry_after": selected.get("retry-after"),
            "content_length": selected.get("content-length"),
            "content_type": selected.get("content-type"),
        }
        return {"url": source.get("url"), "http_status": source.get("http_status"),
                "headers": selected, "capabilities": capabilities}

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        rows = [self.analyze_record(item) for item in report.get("sources", [])]
        return {"generated_at": now(), "resource_count": len(rows), "resources": rows}


class RedirectAnalyzer:
    """Summarizes redirect chains and final destination changes."""

    def analyze_record(self, source: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        redirects = source.get("redirects_raw") or source.get("redirects") or []
        if not redirects and not source.get("final_url_raw"):
            return None
        return {"initial_url": source.get("url"), "initial_url_raw": source.get("raw_url"),
                "final_url": source.get("final_url"), "final_url_raw": source.get("final_url_raw"),
                "redirect_count": len(redirects), "redirects": redirects,
                "cross_host": self._cross_host(source.get("raw_url", ""), source.get("final_url_raw", ""))}

    @staticmethod
    def _cross_host(initial: str, final: str) -> Optional[bool]:
        try:
            return (urllib.parse.urlsplit(initial).hostname or "").lower() != (urllib.parse.urlsplit(final).hostname or "").lower()
        except Exception:
            return None

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        rows = [item for source in report.get("sources", []) if (item := self.analyze_record(source)) is not None]
        return {"generated_at": now(), "redirected_resource_count": len(rows),
                "cross_host_redirect_count": sum(1 for item in rows if item.get("cross_host") is True),
                "chains": rows}


class TimingAnalyzer:
    """Produces latency summaries from observed network and HTTP measurements."""

    @staticmethod
    def percentile(values: List[float], percent: float) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        if len(ordered) == 1:
            return round(ordered[0], 3)
        position = (len(ordered) - 1) * percent / 100.0
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        fraction = position - lower
        value = ordered[lower] * (1 - fraction) + ordered[upper] * fraction
        return round(value, 3)

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        http = []
        tcp = []
        tls = []
        dns = []
        for source in report.get("sources", []):
            elapsed = source.get("elapsed_ms")
            if isinstance(elapsed, (int, float)):
                http.append(float(elapsed))
            network = source.get("network") or {}
            for key, target in (("tcp", tcp), ("tls", tls), ("dns", dns)):
                block = network.get(key) or {}
                value = block.get("elapsed_ms")
                if isinstance(value, (int, float)):
                    target.append(float(value))
        def summarize(values: List[float]) -> Dict[str, Any]:
            return {"samples": len(values), "min": round(min(values), 3) if values else None,
                    "max": round(max(values), 3) if values else None,
                    "mean": round(sum(values) / len(values), 3) if values else None,
                    "p50": self.percentile(values, 50), "p90": self.percentile(values, 90),
                    "p95": self.percentile(values, 95), "p99": self.percentile(values, 99)}
        return {"generated_at": now(), "http_elapsed_ms": summarize(http),
                "tcp_elapsed_ms": summarize(tcp), "tls_elapsed_ms": summarize(tls),
                "dns_elapsed_ms": summarize(dns)}


class SourceCoverageAnalyzer:
    """Reports source coverage and whether the crawl queue was exhausted."""

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        roots = [item for item in report.get("sources", []) if item.get("resource_type") == "root"]
        processed = len(report.get("sources", []))
        edges = report.get("edges", [])
        queue = report.get("pending_queue", [])
        unresolved = report.get("unresolved", [])
        by_depth: Dict[str, int] = collections.Counter(str(item.get("depth", 0)) for item in report.get("sources", []))
        max_depth = max((int(item.get("depth", 0)) for item in report.get("sources", [])), default=0)
        return {
            "generated_at": now(), "root_count": len(roots), "processed_count": processed,
            "discovery_edge_count": len(edges), "unresolved_count": len(unresolved),
            "pending_count": len(queue), "max_observed_depth": max_depth,
            "depth_distribution": dict(sorted(by_depth.items(), key=lambda pair: int(pair[0]))),
            "queue_exhausted": not bool(queue), "scan_complete_flag": bool(report.get("scan_complete")),
            "stop_reason": report.get("stop_reason"),
            "coverage_interpretation": "complete_queue" if not queue and report.get("scan_complete") else "partial_or_stopped",
        }


class DomainRiskAnalyzer:
    """Flags unusual URL features for review; does not claim a site is malicious."""

    SUSPICIOUS_TLDS = {"zip", "mov", "click", "top", "work", "country", "gq", "tk", "ml", "cf"}

    def analyze_url(self, url: str) -> Dict[str, Any]:
        parsed = urllib.parse.urlsplit(url)
        host = (parsed.hostname or "").lower()
        labels = host.split(".") if host else []
        tld = labels[-1] if labels else ""
        findings = []
        if parsed.scheme.lower() == "http":
            findings.append("PLAINTEXT_HTTP")
        if parsed.username or parsed.password:
            findings.append("URL_USERINFO_PRESENT")
        if host.startswith("xn--") or any(label.startswith("xn--") for label in labels):
            findings.append("IDN_PUNYCODE_HOST")
        if tld in self.SUSPICIOUS_TLDS:
            findings.append("UNCOMMON_OR_HIGH_ABUSE_TLD")
        if len(host) > 100:
            findings.append("VERY_LONG_HOSTNAME")
        if len(parsed.query) > 1000:
            findings.append("VERY_LONG_QUERY")
        query_keys = [key.lower() for key, _ in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)]
        repeated = [key for key, count in collections.Counter(query_keys).items() if count > 1]
        if repeated:
            findings.append("REPEATED_QUERY_KEYS")
        return {"url": redact(url), "host": host, "scheme": parsed.scheme,
                "finding_codes": findings, "review_recommended": bool(findings)}

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        rows = []
        seen = set()
        for source in report.get("sources", []):
            url = source.get("raw_url") or source.get("url")
            if not url:
                continue
            try:
                key = URLCanonicalizer.canonical(url)
            except Exception:
                continue
            if key in seen:
                continue
            seen.add(key)
            item = self.analyze_url(key)
            if item["review_recommended"]:
                rows.append(item)
        return {"generated_at": now(), "scanned_unique_urls": len(seen),
                "flagged_url_count": len(rows), "flagged_urls": rows}


class PlaylistRelationshipAnalyzer:
    """Maps playlists to the channel entries and nested resources they expose."""

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        by_source: Dict[str, Dict[str, Any]] = {}
        for source in report.get("sources", []):
            if not (source.get("m3u") or source.get("hls") or source.get("dash")):
                continue
            key = source.get("url") or "unknown"
            by_source[key] = {
                "playlist_url": key,
                "playlist_url_raw": source.get("raw_url"),
                "resource_type": source.get("resource_type_detected"),
                "status_label": source.get("status_label"),
                "channel_entries": 0,
                "embedded_urls": len(source.get("embedded_urls", [])),
                "child_count": len(source.get("children", [])),
                "children": source.get("children", []),
            }
            if source.get("m3u"):
                by_source[key]["channel_entries"] = len(source["m3u"].get("entries", []))
            if source.get("hls"):
                by_source[key]["variant_count"] = len(source["hls"].get("variants", []))
                by_source[key]["segment_count"] = len(source["hls"].get("segments", []))
            if source.get("dash"):
                by_source[key]["representation_count"] = len(source["dash"].get("representations", []))
        return {"generated_at": now(), "playlist_count": len(by_source),
                "playlists": list(by_source.values())}


class ChannelMetadataAnalyzer:
    """Aggregates metadata while preserving the original record list."""

    METADATA_KEYS = ("tvg-id", "tvg-name", "tvg-logo", "group-title", "tvg-country", "tvg-language", "tvg-url")

    def run(self, report: Dict[str, Any]) -> Dict[str, Any]:
        counts: Dict[str, collections.Counter] = {key: collections.Counter() for key in self.METADATA_KEYS}
        missing = collections.Counter()
        groups = collections.Counter()
        names = collections.Counter()
        records = report.get("channel_records", [])
        for record in records:
            attrs = record.get("channel_attributes", {}) or {}
            for key in self.METADATA_KEYS:
                value = attrs.get(key, "") if isinstance(attrs, dict) else ""
                if value:
                    counts[key][str(value)] += 1
                else:
                    missing[key] += 1
            group = attrs.get("group-title", "") if isinstance(attrs, dict) else ""
            groups[str(group or "(без group-title)")] += 1
            name = record.get("channel_name") or "(без названия)"
            names[str(name)] += 1
        return {
            "generated_at": now(), "channel_record_count": len(records),
            "missing_metadata_counts": dict(missing),
            "distinct_value_counts": {key: len(value) for key, value in counts.items()},
            "top_groups": [{"value": key, "count": count} for key, count in groups.most_common(100)],
            "duplicate_display_names": [{"name": key, "count": count} for key, count in names.items() if count > 1],
            "top_tvg_ids": [{"value": key, "count": count} for key, count in counts["tvg-id"].most_common(100)],
        }


class DiagnosticBundleBuilder:
    """Creates a compact index of all sidecar reports and high-level metrics."""

    def build(self, report: Dict[str, Any], artifacts: Dict[str, Any]) -> Dict[str, Any]:
        stats = report.get("stats", {})
        return {
            "tool": report.get("tool", "Phoenix URL Dissector"),
            "version": report.get("version", VERSION),
            "generated_at": now(),
            "scan": {"complete": report.get("scan_complete"), "stop_reason": report.get("stop_reason"),
                     "elapsed_seconds": report.get("elapsed_seconds"), "stats": stats},
            "artifacts": artifacts,
            "interpretation_notes": [
                "HTTP reachability does not prove continuous playback.",
                "A parsed HLS master manifest is not itself a terminal media stream.",
                "A media segment response proves only that the tested segment URL responded at that time.",
                "DRM-protected resources are not decrypted or bypassed by this scanner.",
                "An incomplete scan must not be reported as exhaustive inventory.",
            ],
        }


class ExtendedAnalysisPipeline:
    """Runs all post-discovery analysis passes and exports a unified bundle."""

    def __init__(self, report: Dict[str, Any], output_dir: str, name: str,
                 ffprobe_path: str = "", run_ffprobe: bool = False, ffprobe_timeout: float = 25.0):
        self.report = report
        self.output_dir = output_dir
        self.name = name
        self.ffprobe_path = ffprobe_path
        self.run_ffprobe = run_ffprobe
        self.ffprobe_timeout = ffprobe_timeout

    def run(self) -> Dict[str, Any]:
        print("\n[PHASE 2] Post-discovery validation and report analysis", flush=True)
        ledger = ProvenanceLedger()
        graph = ledger.ingest_report(self.report)
        graph_analysis = GraphAnalyzer(self.report).analyze()
        audit = ReportAuditor(self.report).run()
        failure_clusters = FailureClusterer().run(self.report)
        m3u_integrity = M3UIntegrityAnalyzer().run(self.report)
        hls_integrity = HLSIntegrityAnalyzer().run(self.report)
        dash_integrity = DASHIntegrityAnalyzer().run(self.report)
        header_analysis = ResponseHeaderAnalyzer().run(self.report)
        redirect_analysis = RedirectAnalyzer().run(self.report)
        timing_analysis = TimingAnalyzer().run(self.report)
        coverage = SourceCoverageAnalyzer().run(self.report)
        domain_risk = DomainRiskAnalyzer().run(self.report)
        playlist_relationships = PlaylistRelationshipAnalyzer().run(self.report)
        channel_metadata = ChannelMetadataAnalyzer().run(self.report)
        validation = SecondPhaseValidator(self.report, self.ffprobe_path, self.run_ffprobe,
                                          self.ffprobe_timeout).run()
        exporter = ExtendedExporter(self.output_dir, self.name)
        exports = exporter.export_all(self.report, graph_analysis, audit, validation)
        extra_reports = {
            "provenance_graph": graph,
            "failure_clusters": failure_clusters,
            "m3u_integrity": m3u_integrity,
            "hls_integrity": hls_integrity,
            "dash_integrity": dash_integrity,
            "response_headers": header_analysis,
            "redirect_analysis": redirect_analysis,
            "timing_analysis": timing_analysis,
            "source_coverage": coverage,
            "domain_risk_review": domain_risk,
            "playlist_relationships": playlist_relationships,
            "channel_metadata": channel_metadata,
        }
        for suffix, payload in extra_reports.items():
            AtomicWriter.json(Path(self.output_dir) / f"{self.name}_{suffix}.json", payload)
        bundle = DiagnosticBundleBuilder().build(self.report, {
            "core_json": f"{self.name}.json", "core_text": f"{self.name}.txt",
            "extended_exports": exports,
            "additional_reports": [f"{self.name}_{key}.json" for key in extra_reports],
        })
        AtomicWriter.json(Path(self.output_dir) / f"{self.name}_bundle.json", bundle)
        return {
            "provenance_graph": graph,
            "graph_analysis": graph_analysis,
            "audit": audit,
            "failure_clusters": failure_clusters,
            "m3u_integrity": m3u_integrity,
            "hls_integrity": hls_integrity,
            "dash_integrity": dash_integrity,
            "response_headers": header_analysis,
            "redirect_analysis": redirect_analysis,
            "timing_analysis": timing_analysis,
            "coverage": coverage,
            "domain_risk_review": domain_risk,
            "playlist_relationships": playlist_relationships,
            "channel_metadata": channel_metadata,
            "terminal_validation": validation,
            "exports": exports,
            "bundle": bundle,
        }


# ---------------------------------------------------------------------------
# Offline regression test fixtures for parsing and report invariants
# ---------------------------------------------------------------------------

class RegressionCase:
    def __init__(self, name: str, function: _Callable[[], None]):
        self.name = name
        self.function = function

    def run(self) -> Dict[str, Any]:
        started = time.monotonic()
        try:
            self.function()
            return {"name": self.name, "ok": True, "elapsed_ms": round((time.monotonic() - started) * 1000, 3), "error": ""}
        except Exception as exc:
            return {"name": self.name, "ok": False, "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                    "error": f"{type(exc).__name__}: {exc}"}


class RegressionSuite:
    """Offline tests avoid external network dependencies and credentials."""

    def __init__(self):
        self.cases: List[RegressionCase] = []
        self._register()

    def case(self, name: str):
        def decorator(function: _Callable[[], None]):
            self.cases.append(RegressionCase(name, function))
            return function
        return decorator

    def _register(self) -> None:
        @self.case("canonicalize_scheme_and_host_case")
        def _case_01():
            assert URLCanonicalizer.canonical("HTTPS://Example.COM/a") == "https://example.com/a"

        @self.case("canonicalize_default_https_port")
        def _case_02():
            assert URLCanonicalizer.canonical("https://example.com:443/a") == "https://example.com/a"

        @self.case("canonicalize_default_http_port")
        def _case_03():
            assert URLCanonicalizer.canonical("http://example.com:80/a") == "http://example.com/a"

        @self.case("canonicalize_preserves_query_order")
        def _case_04():
            assert URLCanonicalizer.canonical("https://example.com/a?b=2&a=1") == "https://example.com/a?b=2&a=1"

        @self.case("canonicalize_drops_fragment")
        def _case_05():
            assert URLCanonicalizer.canonical("https://example.com/a#part") == "https://example.com/a"

        @self.case("canonicalize_rejects_userinfo")
        def _case_06():
            try:
                URLCanonicalizer.canonical("https://user:pass@example.com/a")
            except ValueError:
                return
            raise AssertionError("userinfo must be rejected")

        @self.case("atomic_json_roundtrip")
        def _case_07():
            with _tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "report.json"
                AtomicWriter.json(path, {"x": "тест", "n": 2})
                assert json.loads(path.read_text(encoding="utf-8"))["x"] == "тест"

        @self.case("provenance_edges_keep_distinct_relations")
        def _case_08():
            ledger = ProvenanceLedger()
            ledger.add_edge("https://a.example/root.m3u", "https://b.example/ch.m3u8", "playlist_entry")
            ledger.add_edge("https://a.example/root.m3u", "https://b.example/ch.m3u8", "wrapper_target")
            assert len(ledger.edges) == 2

        @self.case("provenance_duplicate_edge_deduplicated")
        def _case_09():
            ledger = ProvenanceLedger()
            ledger.add_edge("https://a.example/a", "https://b.example/b", "child")
            ledger.add_edge("https://a.example/a", "https://b.example/b", "child")
            assert len(ledger.edges) == 1

        @self.case("failure_cluster_dns")
        def _case_10():
            assert FailureClusterer.classify("Temporary failure in name resolution") == "DNS_FAILURE"

        @self.case("failure_cluster_403")
        def _case_11():
            assert FailureClusterer.classify("HTTP 403: Forbidden") == "HTTP_403"

        @self.case("failure_cluster_unknown")
        def _case_12():
            assert FailureClusterer.classify("something unrecognized") == "OTHER"

        @self.case("timing_percentile_empty")
        def _case_13():
            assert TimingAnalyzer.percentile([], 50) is None

        @self.case("timing_percentile_single")
        def _case_14():
            assert TimingAnalyzer.percentile([7.5], 90) == 7.5

        @self.case("timing_percentile_median")
        def _case_15():
            assert TimingAnalyzer.percentile([1, 2, 3, 4, 5], 50) == 3

        @self.case("domain_risk_flags_http")
        def _case_16():
            assert "PLAINTEXT_HTTP" in DomainRiskAnalyzer().analyze_url("http://example.com/x")["finding_codes"]

        @self.case("domain_risk_does_not_flag_plain_https_by_default")
        def _case_17():
            assert not DomainRiskAnalyzer().analyze_url("https://example.com/x")["review_recommended"]

        @self.case("channel_metadata_empty_report")
        def _case_18():
            result = ChannelMetadataAnalyzer().run({"channel_records": []})
            assert result["channel_record_count"] == 0

        @self.case("m3u_integrity_empty_report")
        def _case_19():
            result = M3UIntegrityAnalyzer().run({"sources": []})
            assert result["playlist_count"] == 0

        @self.case("hls_integrity_empty_report")
        def _case_20():
            result = HLSIntegrityAnalyzer().run({"sources": []})
            assert result["manifest_count"] == 0

        @self.case("dash_integrity_empty_report")
        def _case_21():
            result = DASHIntegrityAnalyzer().run({"sources": []})
            assert result["manifest_count"] == 0

        @self.case("redirect_analysis_empty_report")
        def _case_22():
            result = RedirectAnalyzer().run({"sources": []})
            assert result["redirected_resource_count"] == 0

        @self.case("source_coverage_empty_report")
        def _case_23():
            result = SourceCoverageAnalyzer().run({"sources": [], "edges": [], "pending_queue": [], "scan_complete": True})
            assert result["queue_exhausted"] is True

        @self.case("url_index_finds_host")
        def _case_24():
            report = {"sources": [{"raw_url": "https://a.example/x", "status_label": "OK", "resource_type": "root"}]}
            assert len(URLReportIndex(report).host_records("a.example")) == 1

        @self.case("url_index_normalizes_case")
        def _case_25():
            report = {"sources": [{"raw_url": "https://A.EXAMPLE/x", "status_label": "OK"}]}
            assert URLReportIndex(report).get_url("https://a.example/x") is not None

        @self.case("audit_empty_report")
        def _case_26():
            result = ReportAuditor({"sources": [], "channel_records": [], "edges": [], "stats": {}, "scan_complete": True, "pending_queue": []}).run()
            assert result["finding_count"] == 0

        @self.case("header_analyzer_empty")
        def _case_27():
            assert ResponseHeaderAnalyzer().run({"sources": []})["resource_count"] == 0

        @self.case("redirect_cross_host")
        def _case_28():
            assert RedirectAnalyzer._cross_host("https://a.example/x", "https://b.example/x") is True

        @self.case("redirect_same_host")
        def _case_29():
            assert RedirectAnalyzer._cross_host("https://a.example/x", "https://a.example/y") is False

        @self.case("m3u_parser_keeps_duplicate_urls")
        def _case_30():
            text = "#EXTM3U\n#EXTINF:-1,One\nhttps://x.example/a\n#EXTINF:-1,Two\nhttps://x.example/a\n"
            result = parse_m3u(text, "https://x.example/list.m3u")
            assert len(result["entries"]) == 2

        @self.case("m3u_parser_preserves_extinf")
        def _case_31():
            extinf = '#EXTINF:-1 tvg-id="id" group-title="News",Example'
            result = parse_m3u("#EXTM3U\n" + extinf + "\nhttps://x.example/a\n", "https://x.example/list.m3u")
            assert result["entries"][0]["extinf"] == extinf

        @self.case("hls_parser_master_variants")
        def _case_32():
            result = parse_hls("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=640x360\na.m3u8\n", "https://x.example/master.m3u8")
            assert result["is_master"] and len(result["variants"]) == 1

        @self.case("hls_parser_relative_segment")
        def _case_33():
            result = parse_hls("#EXTM3U\n#EXTINF:5,\nseg.ts\n#EXT-X-ENDLIST", "https://x.example/path/live.m3u8")
            assert result["segments"][0] == "https://x.example/path/seg.ts"

        @self.case("hls_parser_duration")
        def _case_34():
            result = parse_hls("#EXTM3U\n#EXTINF:6.25,\na.ts\n#EXTINF:3.75,\nb.ts\n#EXT-X-ENDLIST", "https://x.example/live.m3u8")
            assert result["declared_duration_seconds"] == 10.0

        @self.case("dash_parser_invalid_xml_is_nonfatal")
        def _case_35():
            result = parse_dash(b"<MPD><broken>", "https://x.example/manifest.mpd")
            assert result["valid_xml"] is False

        @self.case("url_extraction_removes_trailing_punctuation")
        def _case_36():
            result = extract_urls_from_text("Next: https://x.example/a.m3u8).", "https://x.example/")
            assert "https://x.example/a.m3u8" in result

        @self.case("redaction_does_not_change_host")
        def _case_37():
            value = redact("https://example.com/x?token=abc")
            assert "example.com" in value and "abc" not in value

        @self.case("report_index_type_filter")
        def _case_38():
            report = {"sources": [{"raw_url": "https://x.example/a", "resource_type_detected": "dash_mpd"}]}
            assert len(URLReportIndex(report).type_records("dash_mpd")) == 1

        @self.case("domain_risk_repeated_query_keys")
        def _case_39():
            result = DomainRiskAnalyzer().analyze_url("https://x.example/a?id=1&id=2")
            assert "REPEATED_QUERY_KEYS" in result["finding_codes"]

        @self.case("failure_cluster_empty_body")
        def _case_40():
            assert FailureClusterer.classify("Успешный HTTP-ответ с пустым телом") == "EMPTY_BODY"

        @self.case("provenance_canonical_nodes_merge_case")
        def _case_41():
            ledger = ProvenanceLedger()
            ledger.add_node("https://EXAMPLE.com/a", "first")
            ledger.add_node("https://example.com/a", "second")
            assert len(ledger.nodes) == 1

        @self.case("atomic_writer_creates_parent")
        def _case_42():
            with _tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "nested" / "report.txt"
                AtomicWriter.text(path, "ok")
                assert path.read_text(encoding="utf-8") == "ok"

        @self.case("coverage_reports_pending_queue")
        def _case_43():
            result = SourceCoverageAnalyzer().run({"sources": [], "edges": [], "pending_queue": [{"url": "x"}], "scan_complete": False})
            assert result["queue_exhausted"] is False

        @self.case("m3u_integrity_duplicate_url_count")
        def _case_44():
            source = {"url": "https://x.example/list.m3u", "m3u": {"valid_header": True, "entries": [
                {"url": "https://x.example/a", "extinf": "#EXTINF:-1,A", "attributes": {"display_name": "A"}},
                {"url": "https://x.example/a", "extinf": "#EXTINF:-1,B", "attributes": {"display_name": "B"}},
            ]}}
            result = M3UIntegrityAnalyzer().analyze_playlist(source)
            assert result["duplicate_url_count"] == 1

        @self.case("metadata_aggregates_groups")
        def _case_45():
            report = {"channel_records": [{"channel_name": "A", "channel_attributes": {"group-title": "News"}}]}
            result = ChannelMetadataAnalyzer().run(report)
            assert result["top_groups"][0]["value"] == "News"

        @self.case("failure_clusters_count_items")
        def _case_46():
            report = {"unresolved": [{"url": "https://x.example/a", "error": "HTTP 404"}], "errors": []}
            result = FailureClusterer().run(report)
            assert result["counts"]["HTTP_404"] == 1

        @self.case("report_audit_detects_missing_channel_url")
        def _case_47():
            report = {"sources": [], "channel_records": [{"channel_name": "No URL"}], "edges": [], "stats": {}, "scan_complete": True, "pending_queue": []}
            result = ReportAuditor(report).run()
            assert any(item["code"] == "CHANNEL_WITHOUT_URL" for item in result["findings"])

        @self.case("terminal_validator_empty_candidates")
        def _case_48():
            result = SecondPhaseValidator({"sources": [], "terminal_urls": []}).run()
            assert result["candidate_count"] == 0

        @self.case("host_summary_failure_ratio")
        def _case_49():
            report = {"sources": [
                {"raw_url": "https://x.example/a", "resource_type": "root", "status_label": "BROKEN"},
                {"raw_url": "https://x.example/b", "resource_type": "child", "status_label": "OK"},
            ], "edges": []}
            result = GraphAnalyzer(report).host_summary()
            assert result[0]["failure_ratio"] == 0.5

        @self.case("jsonl_writer_writes_one_object_per_line")
        def _case_50():
            with _tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "data.jsonl"
                AtomicWriter.jsonl(path, [{"a": 1}, {"a": 2}])
                assert len(path.read_text(encoding="utf-8").splitlines()) == 2

    def run(self) -> Dict[str, Any]:
        results = [case.run() for case in self.cases]
        failures = [item for item in results if not item["ok"]]
        return {"suite": "Phoenix URL Dissector offline regression suite",
                "generated_at": now(), "total": len(results), "passed": len(results) - len(failures),
                "failed": len(failures), "ok": not failures, "results": results}


def run_extended_regression_suite() -> Dict[str, Any]:
    result = RegressionSuite().run()
    if not result["ok"]:
        errors = [item for item in result["results"] if not item["ok"]]
        raise AssertionError(json.dumps(errors, ensure_ascii=False, indent=2))
    return result


def write_extended_analysis(report: Dict[str, Any], output_dir: str, name: str,
                            ffprobe_path: str = "", run_ffprobe: bool = False,
                            ffprobe_timeout: float = 25.0) -> Dict[str, Any]:
    """Public orchestration entry point used by main and downstream scripts."""
    pipeline = ExtendedAnalysisPipeline(report, output_dir, name, ffprobe_path,
                                        run_ffprobe, ffprobe_timeout)
    result = pipeline.run()
    # Persist a compact summary suitable for GitHub Actions job summaries.
    summary_lines = [
        f"# Phoenix URL Dissector {VERSION}", "",
        f"- Scan complete: **{report.get('scan_complete')}**",
        f"- Stop reason: `{report.get('stop_reason')}`",
        f"- Processed URLs: {report.get('stats', {}).get('unique_urls_processed', 0)}",
        f"- Discovery edges: {report.get('stats', {}).get('urls_discovered', 0)}",
        f"- Channel records: {len(report.get('channel_records', []))}",
        f"- Terminal candidates: {result['terminal_validation'].get('candidate_count', 0)}",
        f"- Audit findings: {result['audit'].get('finding_count', 0)}", "",
        "## Terminal validation statuses", "",
    ]
    for status, count in sorted(result["terminal_validation"].get("status_counts", {}).items()):
        summary_lines.append(f"- `{status}`: {count}")
    summary_lines.extend(["", "## Exports", ""])
    summary_lines.extend(f"- `{path}`" for path in result.get("exports", []))
    AtomicWriter.text(Path(output_dir) / f"{name}_summary.md", "\n".join(summary_lines) + "\n")
    return result


def _extension_self_test() -> None:
    result = run_extended_regression_suite()
    print(f"Extended regression suite: {result['passed']}/{result['total']} passed")


if __name__ == "__main__":
    raise SystemExit(main())
