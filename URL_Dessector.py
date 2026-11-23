
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phoenix URL Dissector 2.6.0-beta

Recursive M3U/IPTV/HLS/DASH discovery, HTTP diagnostics and redirect tracing.
Python 3.9+, standard library only.

Default sources:
  https://livem3u.tatnet.app/data/playlists/INDEX.m3u
  https://livem3u.tatnet.app/data/playlist.m3u

Pipeline:
  1. Download INDEX.m3u.
  2. Extract up to --index-playlists unique playlist URLs from the index.
  3. Parse selected playlists and recursively discover child resources.
  4. Identify HLS master playlists, HLS media playlists and DASH manifests.
  5. Check terminal candidates using HTTP status, MIME type, URL and payload.
  6. Optionally probe the first HLS media segment.
  7. Record redirect chains, source relationships, errors and budget events.
  8. Generate TXT/JSON reports and a candidate M3U playlist.

Important:
  An HTTP response alone cannot prove successful video decoding.
  This program does not perform full media decoding.
  DASH manifests are not classified as terminal media streams.
  Unknown HTTP 200 responses are not automatically accepted as streams.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import ipaddress
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
import xml.etree.ElementTree as ET

from collections import Counter
from typing import Dict, List, Optional, Set, Tuple


VERSION = "2.6.0-beta"

DEFAULT_SOURCES = [
    "https://livem3u.tatnet.app/data/playlists/INDEX.m3u",
    "https://livem3u.tatnet.app/data/playlist.m3u",
]

DEFAULT_INDEX_PLAYLISTS = 100

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

PLAYLIST_EXTENSIONS = (".m3u", ".m3u8")
DASH_EXTENSIONS = (".mpd",)

MEDIA_EXTENSIONS = (
    ".ts",
    ".m2ts",
    ".mts",
    ".m4s",
    ".mp4",
    ".m4v",
    ".mkv",
    ".webm",
    ".avi",
    ".mov",
    ".mpg",
    ".mpeg",
    ".vob",
    ".mp3",
    ".aac",
    ".ac3",
    ".ec3",
    ".m4a",
    ".ogg",
    ".oga",
    ".opus",
    ".wav",
    ".flac",
    ".mp2",
    ".m4s",
    ".cmfv",
    ".cmfa",
)

HTML_MARKERS = (
    b"<!doctype html",
    b"<html",
    b"<head",
    b"<body",
)

DASH_MARKERS = (
    b"<mpd",
    b":mpd",
)

MEDIA_CONTENT_TYPES = (
    "video/",
    "audio/",
    "application/octet-stream",
    "application/vnd.apple.mpegurl.segment",
    "application/mp4",
    "application/mpeg",
    "application/x-mpeg",
    "application/ogg",
    "application/vnd.rn-realmedia",
)

TEXT_CONTENT_TYPES = (
    "text/html",
    "text/plain",
    "application/json",
    "application/xml",
    "text/xml",
    "application/javascript",
    "text/javascript",
)

HLS_MEDIA_TAGS = (
    "#EXT-X-TARGETDURATION:",
    "#EXT-X-MEDIA-SEQUENCE:",
    "#EXT-X-PLAYLIST-TYPE:",
    "#EXT-X-ENDLIST",
    "#EXT-X-KEY:",
    "#EXT-X-MAP:",
    "#EXT-X-VERSION:",
    "#EXT-X-DISCONTINUITY",
    "#EXT-X-PROGRAM-DATE-TIME:",
    "#EXT-X-BYTERANGE:",
    "#EXT-X-DATERANGE:",
    "#EXT-X-SERVER-CONTROL:",
    "#EXT-X-PART:",
    "#EXT-X-PRELOAD-HINT:",
    "#EXT-X-SKIP:",
)

URL_RE = re.compile(
    r"https?://[^\s\"'<>]+",
    re.IGNORECASE,
)

HLS_DURATION_RE = re.compile(
    r"^#EXTINF:\s*([0-9]+(?:\.[0-9]+)?)\s*(?:,|$)",
    re.IGNORECASE,
)

HLS_BANDWIDTH_RE = re.compile(
    r"(?:^|,)\s*BANDWIDTH\s*=\s*(\d+)",
    re.IGNORECASE,
)

REDIRECT_CODES = {301, 302, 303, 307, 308}


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return dt.datetime.now(dt.timezone.utc).isoformat()


def clean_url(value: str) -> str:
    """Remove surrounding punctuation commonly found around URLs."""
    return value.strip().strip("\"'<>").rstrip("),;]")


def normalize_url(url: str) -> str:
    """
    Normalize an HTTP(S) URL without removing query parameters.

    Query parameters are preserved because IPTV URLs often contain tokens.
    URL fragments are removed because they are not sent in HTTP requests.
    """
    try:
        url = clean_url(url)
        parsed = urllib.parse.urlsplit(url)

        if parsed.scheme.lower() not in ("http", "https"):
            return ""

        if not parsed.netloc or not parsed.hostname:
            return ""

        scheme = parsed.scheme.lower()

        # Lowercase the hostname without unnecessarily changing user-info.
        hostname = parsed.hostname.lower()
        userinfo = ""

        if parsed.username is not None:
            userinfo = urllib.parse.quote(
                urllib.parse.unquote(parsed.username),
                safe="!$&'()*+,;=:",
            )

            if parsed.password is not None:
                userinfo += ":" + urllib.parse.quote(
                    urllib.parse.unquote(parsed.password),
                    safe="!$&'()*+,;=:",
                )

            userinfo += "@"

        try:
            port = parsed.port
        except ValueError:
            return ""

        if ":" in hostname and not hostname.startswith("["):
            hostname = "[" + hostname + "]"

        default_port = (
            (scheme == "http" and port == 80)
            or (scheme == "https" and port == 443)
        )

        if port is not None and not default_port:
            netloc = f"{userinfo}{hostname}:{port}"
        else:
            netloc = f"{userinfo}{hostname}"

        path = parsed.path or "/"

        return urllib.parse.urlunsplit(
            (scheme, netloc, path, parsed.query, "")
        )

    except Exception:
        return ""


def resolve_url(base_url: str, child_url: str) -> str:
    """Resolve an absolute or relative URL against its parent URL."""
    child_url = clean_url(child_url)

    if not child_url:
        return ""

    try:
        resolved = urllib.parse.urljoin(base_url, child_url)
        return normalize_url(resolved)
    except Exception:
        return ""


def path_of(url: str) -> str:
    try:
        return urllib.parse.urlsplit(url).path.lower()
    except Exception:
        return ""


def url_looks_like_playlist(url: str) -> bool:
    return path_of(url).endswith(PLAYLIST_EXTENSIONS)


def url_looks_like_dash(url: str) -> bool:
    return path_of(url).endswith(DASH_EXTENSIONS)


def url_looks_like_media(url: str) -> bool:
    return path_of(url).endswith(MEDIA_EXTENSIONS)


def url_basename(url: str) -> str:
    try:
        return os.path.basename(
            urllib.parse.urlsplit(url).path
        ).lower()
    except Exception:
        return ""


def url_is_index(url: str) -> bool:
    """
    Identify INDEX.m3u without relying on URL query parameters.

    A query string such as ?token=... does not affect this check.
    """
    return url_basename(url) == "index.m3u"


def decode_body(body: bytes) -> str:
    """Decode common playlist encodings."""
    for encoding in (
        "utf-8-sig",
        "utf-8",
        "cp1251",
        "latin-1",
    ):
        try:
            return body.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            pass

    return body.decode("utf-8", errors="replace")


def body_looks_like_html(
    body: bytes,
    content_type: str = "",
) -> bool:
    """Detect HTML error pages, login pages and proxy challenges."""
    if "text/html" in (content_type or "").lower():
        return True

    sample = (body or b"").lstrip()[:4096].lower()

    if any(sample.startswith(marker) for marker in HTML_MARKERS):
        return True

    if b"<html" in sample[:1024]:
        return True

    return False


def body_looks_like_dash(
    body: bytes,
    content_type: str = "",
    url: str = "",
) -> bool:
    """Detect a DASH MPD manifest by MIME type, extension or XML root."""
    content_type = (content_type or "").lower()

    if "dash+xml" in content_type:
        return True

    if url_looks_like_dash(url):
        sample = (body or b"").lstrip()[:4096].lower()
        if b"<mpd" in sample or b":mpd" in sample:
            return True

    sample = (body or b"").lstrip()[:4096].lower()

    if any(marker in sample for marker in DASH_MARKERS):
        if b"<mpd" in sample or b":mpd" in sample:
            return True

    return False


def looks_like_user_agent_name(value: str) -> bool:
    """
    Reject common accidental User-Agent fragments as channel names.

    This is deliberately conservative: the original metadata remains in
    the diagnostic record, but a suspicious value is not used as a name.
    """
    value = (value or "").strip().lower()

    if not value:
        return True

    markers = (
        "mozilla/",
        "applewebkit/",
        "like gecko",
        "chrome/",
        "safari/",
        "firefox/",
        "edg/",
        "android ",
        "linux x86_64",
        "windows nt ",
        "user-agent",
    )

    return any(marker in value for marker in markers)


def safe_display_name(
    candidate: str,
    fallback_url: str,
) -> str:
    candidate = (candidate or "").strip()

    if candidate and not looks_like_user_agent_name(candidate):
        return candidate

    try:
        host = urllib.parse.urlsplit(fallback_url).hostname
    except Exception:
        host = None

    return host or fallback_url


@dataclasses.dataclass
class HttpResult:
    requested_url: str
    final_url: str = ""
    status: int = 0
    headers: Dict[str, str] = dataclasses.field(
        default_factory=dict
    )
    body: bytes = b""
    error: str = ""
    elapsed: float = 0.0
    truncated: bool = False
    redirect_chain: List[Dict[str, object]] = dataclasses.field(
        default_factory=list
    )

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "")

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300 and not self.error


@dataclasses.dataclass
class M3UEntry:
    url: str
    name: str = ""
    extinf: str = ""
    directives: List[str] = dataclasses.field(
        default_factory=list
    )
    group_title: str = ""
    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    source_playlist: str = ""

    def metadata(self) -> Dict[str, str]:
        return {
            "name": self.name,
            "extinf": self.extinf,
            "group-title": self.group_title,
            "tvg-id": self.tvg_id,
            "tvg-name": self.tvg_name,
            "tvg-logo": self.tvg_logo,
            "source_playlist": self.source_playlist,
        }


@dataclasses.dataclass
class WorkItem:
    url: str
    depth: int = 0
    role: str = "child"
    entry: Optional[M3UEntry] = None
    parent_url: str = ""


@dataclasses.dataclass
class Record:
    url: str
    final_url: str
    status: int
    classification: str
    depth: int
    role: str
    elapsed: float
    bytes_read: int
    content_type: str = ""
    detail: str = ""
    parent_url: str = ""
    truncated: bool = False
    redirect_chain: List[Dict[str, object]] = dataclasses.field(
        default_factory=list
    )


class TracingRedirectHandler(
    urllib.request.HTTPRedirectHandler
):
    """
    Capture redirect hops for a single HTTP request.

    A handler instance is created per request, so concurrent requests do
    not share mutable redirect state.
    """

    def __init__(self):
        super().__init__()
        self.chain: List[Dict[str, object]] = []

    def redirect_request(
        self,
        req,
        fp,
        code,
        msg,
        headers,
        newurl,
    ):
        source = req.full_url
        target = urllib.parse.urljoin(source, newurl)

        self.chain.append({
            "status": int(code),
            "from": source,
            "to": target,
            "message": str(msg),
        })

        return super().redirect_request(
            req,
            fp,
            code,
            msg,
            headers,
            newurl,
        )


class HttpClient:
    """
    Global request and response-byte budgets.

    A byte allowance is reserved before each request. After the request,
    the reservation is released and the actual retained response bytes
    are added to the global byte counter.
    """

    def __init__(
        self,
        timeout: float,
        max_bytes_per_request: int,
        max_total_bytes: int,
        max_requests: int,
        user_agent: str,
        insecure_tls: bool = False,
        allow_private: bool = False,
    ):
        self.timeout = timeout
        self.max_bytes_per_request = max(
            1024,
            max_bytes_per_request,
        )
        self.max_total_bytes = max(1024, max_total_bytes)
        self.max_requests = max(1, max_requests)
        self.user_agent = user_agent
        self.insecure_tls = insecure_tls
        self.allow_private = allow_private

        self._lock = threading.Lock()

        self.requests_used = 0
        self.bytes_used = 0
        self.bytes_reserved = 0

        self.budget_events: List[str] = []

    def _add_budget_event(self, event: str) -> None:
        with self._lock:
            if event not in self.budget_events:
                self.budget_events.append(event)

    def _reserve(self, requested_byte_limit: int) -> int:
        with self._lock:
            if self.requests_used >= self.max_requests:
                event = (
                    "HTTP request budget reached: "
                    f"{self.requests_used}/{self.max_requests}"
                )

                if event not in self.budget_events:
                    self.budget_events.append(event)

                return -1

            remaining = (
                self.max_total_bytes
                - self.bytes_used
                - self.bytes_reserved
            )

            if remaining <= 0:
                event = "Total response byte budget exhausted"

                if event not in self.budget_events:
                    self.budget_events.append(event)

                return 0

            allowance = min(
                max(1, requested_byte_limit),
                self.max_bytes_per_request,
                remaining,
            )

            self.requests_used += 1
            self.bytes_reserved += allowance

            return allowance

    def _release_reservation(
        self,
        allowance: int,
        actual: int,
    ) -> None:
        with self._lock:
            self.bytes_reserved = max(
                0,
                self.bytes_reserved - allowance,
            )

            self.bytes_used += max(0, actual)

            if self.bytes_used > self.max_total_bytes:
                self.bytes_used = self.max_total_bytes

    def _private_host_blocked(self, hostname: str) -> bool:
        """
        Block obvious local/private destinations unless explicitly allowed.

        This check is not a complete DNS-rebinding defence. It prevents
        straightforward access to localhost and literal private IPs.
        """
        if self.allow_private:
            return False

        hostname = (hostname or "").strip().lower()

        if hostname in (
            "localhost",
            "localhost.localdomain",
        ):
            return True

        try:
            address = ipaddress.ip_address(hostname)
            return (
                address.is_private
                or address.is_loopback
                or address.is_link_local
                or address.is_reserved
                or address.is_unspecified
            )
        except ValueError:
            return False

    def get(
        self,
        url: str,
        byte_limit: Optional[int] = None,
    ) -> HttpResult:
        normalized = normalize_url(url)

        if not normalized:
            return HttpResult(
                requested_url=url,
                error="invalid_http_url",
            )

        parsed = urllib.parse.urlsplit(normalized)
        host = parsed.hostname or ""

        if self._private_host_blocked(host):
            return HttpResult(
                requested_url=normalized,
                error="private_host_blocked",
            )

        requested_limit = (
            byte_limit or self.max_bytes_per_request
        )

        allowance = self._reserve(requested_limit)

        if allowance < 0:
            return HttpResult(
                requested_url=normalized,
                error="http_request_budget_exhausted",
            )

        if allowance == 0:
            return HttpResult(
                requested_url=normalized,
                error="total_byte_budget_exhausted",
            )

        redirect_handler = TracingRedirectHandler()

        handlers = [
            redirect_handler,
            urllib.request.HTTPCookieProcessor(),
        ]

        if self.insecure_tls:
            context = ssl._create_unverified_context()

            handlers.append(
                urllib.request.HTTPSHandler(
                    context=context
                )
            )

        opener = urllib.request.build_opener(*handlers)

        request = urllib.request.Request(
            normalized,
            headers={
                "User-Agent": self.user_agent,
                "Accept": (
                    "application/vnd.apple.mpegurl,"
                    "application/x-mpegURL,"
                    "audio/mpegurl,"
                    "application/dash+xml,"
                    "application/octet-stream,*/*;q=0.8"
                ),
                "Accept-Encoding": "identity",
                "Connection": "close",
            },
            method="GET",
        )

        started = time.monotonic()
        response = None
        actual_bytes = 0

        try:
            response = opener.open(
                request,
                timeout=self.timeout,
            )

            status = int(
                getattr(response, "status", 200)
            )

            final_url = (
                normalize_url(response.geturl())
                or normalized
            )

            headers = {
                str(key).lower(): str(value)
                for key, value in response.headers.items()
            }

            # Read at most the reserved number of bytes.
            body = response.read(allowance)
            actual_bytes = len(body)

            # A full allowance does not prove truncation, but reaching
            # the configured limit is useful diagnostic information.
            truncated = (
                actual_bytes >= allowance
                and allowance < self.max_bytes_per_request
            )

            if truncated:
                self._add_budget_event(
                    "Response reached reserved byte allowance: "
                    f"{normalized} ({allowance} bytes)"
                )

            return HttpResult(
                requested_url=normalized,
                final_url=final_url,
                status=status,
                headers=headers,
                body=body,
                elapsed=time.monotonic() - started,
                truncated=truncated,
                redirect_chain=list(
                    redirect_handler.chain
                ),
            )

        except urllib.error.HTTPError as exc:
            headers = {
                str(key).lower(): str(value)
                for key, value in (
                    exc.headers.items()
                    if exc.headers
                    else []
                )
            }

            try:
                body = exc.read(
                    min(allowance, 64 * 1024)
                )
            except Exception:
                body = b""

            actual_bytes = len(body)

            return HttpResult(
                requested_url=normalized,
                final_url=(
                    normalize_url(exc.geturl())
                    or normalized
                ),
                status=int(exc.code or 0),
                headers=headers,
                body=body,
                error=f"http_error:{exc.code}",
                elapsed=time.monotonic() - started,
                redirect_chain=list(
                    redirect_handler.chain
                ),
            )

        except Exception as exc:
            return HttpResult(
                requested_url=normalized,
                final_url=normalized,
                error=(
                    f"{type(exc).__name__}: {exc}"
                ),
                elapsed=time.monotonic() - started,
                redirect_chain=list(
                    redirect_handler.chain
                ),
            )

        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass

            self._release_reservation(
                allowance,
                actual_bytes,
            )


def parse_attributes(line: str) -> Dict[str, str]:
    """Parse EXTINF-style key/value attributes."""
    result: Dict[str, str] = {}

    if not line:
        return result

    pattern = re.compile(
        r'([A-Za-z0-9_-]+)\s*=\s*'
        r'("([^"]*)"|([^,\s]*))'
    )

    for match in pattern.finditer(line):
        key = match.group(1).lower()
        value = match.group(3)

        if value is None:
            value = match.group(4) or ""

        result[key] = value

    return result


def extinf_display_name(line: str) -> str:
    """
    Extract the display name from EXTINF.

    The comma separating attributes from the display name is detected
    only when it occurs outside a quoted attribute value.
    """
    if not line:
        return ""

    quoted = False
    escaped = False

    for index, char in enumerate(line):
        if escaped:
            escaped = False
            continue

        if char == "\\" and quoted:
            escaped = True
            continue

        if char == '"':
            quoted = not quoted
            continue

        if char == "," and not quoted:
            return line[index + 1:].strip()

    return ""


def parse_m3u_entries(
    text: str,
    base_url: str,
) -> List[M3UEntry]:
    """
    Parse IPTV entries and plain URL lists.

    URL-only indexes are accepted even when no EXTINF precedes an entry.
    Suspicious User-Agent-like strings are not used as channel names.
    """
    entries: List[M3UEntry] = []

    pending_extinf = ""
    pending_directives: List[str] = []

    lines = (
        text.replace("\r\n", "\n")
        .replace("\r", "\n")
        .split("\n")
    )

    for raw_line in lines:
        line = raw_line.strip()

        if not line:
            continue

        if line.upper().startswith("#EXTINF:"):
            pending_extinf = line
            pending_directives = []
            continue

        if line.startswith("#"):
            if line.upper() != "#EXTM3U":
                pending_directives.append(line)
            continue

        resolved = resolve_url(base_url, line)

        if not resolved:
            pending_extinf = ""
            pending_directives = []
            continue

        attrs = parse_attributes(pending_extinf)

        display_name = extinf_display_name(
            pending_extinf
        )

        name = (
            attrs.get("tvg-name")
            or display_name
            or urllib.parse.urlsplit(resolved).hostname
            or resolved
        )

        name = safe_display_name(name, resolved)

        entries.append(
            M3UEntry(
                url=resolved,
                name=name,
                extinf=pending_extinf,
                directives=list(pending_directives),
                group_title=attrs.get(
                    "group-title",
                    "",
                ),
                tvg_id=attrs.get("tvg-id", ""),
                tvg_name=attrs.get("tvg-name", ""),
                tvg_logo=attrs.get("tvg-logo", ""),
                source_playlist=base_url,
            )
        )

        pending_extinf = ""
        pending_directives = []

    return entries


def parse_hls(
    text: str,
    base_url: str,
) -> Dict[str, object]:
    """
    Parse HLS master/media playlists.

    Variant bandwidth metadata is preserved so the highest-bandwidth
    variant can be selected accurately.
    """
    lines = [
        line.strip()
        for line in (
            text.replace("\r\n", "\n")
            .replace("\r", "\n")
            .split("\n")
        )
        if line.strip()
    ]

    has_header = any(
        line.upper() == "#EXTM3U"
        for line in lines
    )

    has_stream_inf = any(
        line.upper().startswith(
            "#EXT-X-STREAM-INF:"
        )
        for line in lines
    )

    variants: List[str] = []
    variant_details: List[Dict[str, object]] = []
    segments: List[str] = []

    pending_stream_inf = False
    pending_bandwidth = 0
    pending_hls_extinf = False

    hls_media_tag_seen = False
    numeric_hls_extinf_seen = False

    for line in lines:
        upper = line.upper()

        if any(
            upper.startswith(tag)
            for tag in HLS_MEDIA_TAGS
        ):
            hls_media_tag_seen = True

        if upper.startswith("#EXT-X-STREAM-INF:"):
            pending_stream_inf = True
            pending_bandwidth = 0

            match = HLS_BANDWIDTH_RE.search(
                line.split(":", 1)[1]
            )

            if match:
                try:
                    pending_bandwidth = int(
                        match.group(1)
                    )
                except ValueError:
                    pending_bandwidth = 0

            continue

        if upper.startswith("#EXTINF:"):
            match = HLS_DURATION_RE.match(line)

            if match:
                numeric_hls_extinf_seen = True
                pending_hls_extinf = True
            else:
                pending_hls_extinf = False

            continue

        if line.startswith("#"):
            continue

        resolved = resolve_url(base_url, line)

        if not resolved:
            continue

        if pending_stream_inf:
            variants.append(resolved)

            variant_details.append({
                "url": resolved,
                "bandwidth": pending_bandwidth,
            })

            pending_stream_inf = False
            pending_bandwidth = 0

        elif pending_hls_extinf:
            segments.append(resolved)
            pending_hls_extinf = False

    is_master = (
        has_header
        and has_stream_inf
        and bool(variants)
    )

    is_media = (
        has_header
        and not is_master
        and hls_media_tag_seen
        and numeric_hls_extinf_seen
        and bool(segments)
    )

    return {
        "has_header": has_header,
        "is_master": is_master,
        "is_media": is_media,
        "variants": variants,
        "variant_details": variant_details,
        "segments": segments,
    }


def parse_dash_manifest(
    text: str,
    base_url: str,
) -> Dict[str, object]:
    """
    Parse a DASH MPD manifest.

    The parser extracts explicit BaseURL, SegmentURL and initialization
    references. SegmentTemplate expressions are reported but are not
    blindly expanded because their numbering and timing rules vary.
    """
    result: Dict[str, object] = {
        "valid_xml": False,
        "is_mpd": False,
        "base_urls": [],
        "segment_urls": [],
        "initialization_urls": [],
        "segment_templates": 0,
        "periods": 0,
        "adaptation_sets": 0,
        "representations": 0,
        "detail": "",
    }

    try:
        root = ET.fromstring(text)
    except Exception as exc:
        result["detail"] = (
            f"XML parse failed: {type(exc).__name__}: {exc}"
        )
        return result

    result["valid_xml"] = True

    root_name = root.tag.rsplit("}", 1)[-1].lower()

    if root_name != "mpd":
        result["detail"] = (
            f"XML root is {root_name!r}, not MPD"
        )
        return result

    result["is_mpd"] = True

    base_urls: List[str] = []
    segment_urls: List[str] = []
    initialization_urls: List[str] = []

    def child_elements(element, local_name):
        return [
            child
            for child in list(element)
            if child.tag.rsplit("}", 1)[-1].lower()
            == local_name.lower()
        ]

    def collect_base_urls(element, parent_url):
        children = child_elements(
            element,
            "BaseURL",
        )

        if not children:
            return [parent_url]

        resolved_values = []

        for child in children:
            if child.text and child.text.strip():
                resolved = resolve_url(
                    parent_url,
                    child.text.strip(),
                )

                if resolved:
                    resolved_values.append(resolved)

        return resolved_values or [parent_url]

    root_bases = collect_base_urls(
        root,
        base_url,
    )

    base_urls.extend(root_bases)

    periods = child_elements(root, "Period")
    result["periods"] = len(periods)

    for period in periods:
        period_bases = collect_base_urls(
            period,
            base_url,
        )

        if period_bases == [base_url]:
            period_bases = root_bases

        adaptation_sets = child_elements(
            period,
            "AdaptationSet",
        )

        result["adaptation_sets"] += len(
            adaptation_sets
        )

        for adaptation in adaptation_sets:
            adaptation_bases = collect_base_urls(
                adaptation,
                period_bases[0] if period_bases else base_url,
            )

            if adaptation_bases == [
                period_bases[0]
                if period_bases
                else base_url
            ]:
                adaptation_bases = period_bases

            representations = child_elements(
                adaptation,
                "Representation",
            )

            result["representations"] += len(
                representations
            )

            for representation in representations:
                parent = (
                    adaptation_bases[0]
                    if adaptation_bases
                    else base_url
                )

                representation_bases = collect_base_urls(
                    representation,
                    parent,
                )

                for resolved in representation_bases:
                    if resolved not in base_urls:
                        base_urls.append(resolved)

                segment_lists = (
                    child_elements(
                        representation,
                        "SegmentList",
                    )
                    + child_elements(
                        adaptation,
                        "SegmentList",
                    )
                )

                for segment_list in segment_lists:
                    for segment_url in child_elements(
                        segment_list,
                        "SegmentURL",
                    ):
                        media = segment_url.attrib.get(
                            "media",
                            "",
                        )

                        if media:
                            resolved = resolve_url(
                                representation_bases[0]
                                if representation_bases
                                else parent,
                                media,
                            )

                            if (
                                resolved
                                and resolved not in segment_urls
                            ):
                                segment_urls.append(resolved)

                    for initialization in child_elements(
                        segment_list,
                        "Initialization",
                    ):
                        source_url = initialization.attrib.get(
                            "sourceURL",
                            "",
                        )

                        if source_url:
                            resolved = resolve_url(
                                representation_bases[0]
                                if representation_bases
                                else parent,
                                source_url,
                            )

                            if (
                                resolved
                                and resolved
                                not in initialization_urls
                            ):
                                initialization_urls.append(
                                    resolved
                                )

                templates = (
                    child_elements(
                        representation,
                        "SegmentTemplate",
                    )
                    + child_elements(
                        adaptation,
                        "SegmentTemplate",
                    )
                )

                result["segment_templates"] += len(
                    templates
                )

                for template in templates:
                    initialization = template.attrib.get(
                        "initialization",
                        "",
                    )

                    media = template.attrib.get(
                        "media",
                        "",
                    )

                    if initialization and "$" not in initialization:
                        resolved = resolve_url(
                            representation_bases[0]
                            if representation_bases
                            else parent,
                            initialization,
                        )

                        if (
                            resolved
                            and resolved
                            not in initialization_urls
                        ):
                            initialization_urls.append(
                                resolved
                            )

                    # Do not expand $Number$, $Time$ or other template
                    # expressions without their timing/numbering context.
                    if media and "$" not in media:
                        resolved = resolve_url(
                            representation_bases[0]
                            if representation_bases
                            else parent,
                            media,
                        )

                        if (
                            resolved
                            and resolved not in segment_urls
                        ):
                            segment_urls.append(resolved)

    result["base_urls"] = list(dict.fromkeys(base_urls))
    result["segment_urls"] = list(
        dict.fromkeys(segment_urls)
    )
    result["initialization_urls"] = list(
        dict.fromkeys(initialization_urls)
    )

    result["detail"] = (
        f"periods={result['periods']}, "
        f"adaptation_sets={result['adaptation_sets']}, "
        f"representations={result['representations']}, "
        f"segment_templates={result['segment_templates']}, "
        f"explicit_segments={len(segment_urls)}"
    )

    return result


def extract_playlist_urls(
    text: str,
    base_url: str,
) -> List[str]:
    """
    Extract unique playlist URLs from a plain-text or M3U index.

    Relative paths are resolved against the source playlist URL.
    """
    found: List[str] = []
    seen: Set[str] = set()

    for match in URL_RE.findall(text):
        candidate = clean_url(match)

        parsed = urllib.parse.urlsplit(candidate)

        if not parsed.path.lower().endswith(
            PLAYLIST_EXTENSIONS
        ):
            continue

        resolved = resolve_url(
            base_url,
            candidate,
        )

        if resolved and resolved not in seen:
            seen.add(resolved)
            found.append(resolved)

    for raw_line in text.splitlines():
        line = clean_url(raw_line.strip())

        if (
            not line
            or line.startswith("#")
            or "://" in line
        ):
            continue

        parsed = urllib.parse.urlsplit(line)

        if not parsed.path.lower().endswith(
            PLAYLIST_EXTENSIONS
        ):
            continue

        resolved = resolve_url(
            base_url,
            line,
        )

        if resolved and resolved not in seen:
            seen.add(resolved)
            found.append(resolved)

    return found


def looks_like_playlist_response(
    result: HttpResult,
) -> bool:
    """Detect M3U content independently of the URL extension."""
    if not result.body:
        return False

    if body_looks_like_html(
        result.body,
        result.content_type,
    ):
        return False

    if body_looks_like_dash(
        result.body,
        result.content_type,
        result.final_url or result.requested_url,
    ):
        return False

    content_type = result.content_type.lower()

    if any(
        marker in content_type
        for marker in (
            "mpegurl",
            "m3u",
            "application/vnd.apple",
        )
    ):
        return True

    text = decode_body(result.body).lstrip()

    if text.upper().startswith("#EXTM3U"):
        return True

    if "#EXTINF:" in text[:65536]:
        return True

    return False


def payload_looks_like_media(body: bytes) -> bool:
    """
    Check common media payload signatures.

    This is a heuristic, not a substitute for a media decoder.
    """
    if not body:
        return False

    sample = body[:8192]

    # MPEG transport stream: sync byte at 188-byte intervals.
    if len(sample) >= 376:
        if (
            sample[0] == 0x47
            and sample[188] == 0x47
        ):
            return True

    # ISO Base Media File Format / fragmented MP4.
    if len(sample) >= 8:
        box_type = sample[4:8]

        if box_type in (
            b"ftyp",
            b"styp",
            b"moof",
            b"mdat",
            b"sidx",
            b"moov",
        ):
            return True

    # MPEG audio frame sync.
    if len(sample) >= 2:
        if (
            sample[0] == 0xFF
            and (sample[1] & 0xE0) == 0xE0
        ):
            return True

    # ADTS AAC frame sync.
    if len(sample) >= 2:
        if (
            sample[0] == 0xFF
            and (sample[1] & 0xF6) == 0xF0
        ):
            return True

    # Ogg container.
    if sample.startswith(b"OggS"):
        return True

    # Matroska/WebM EBML header.
    if sample.startswith(b"\x1A\x45\xDF\xA3"):
        return True

    # RIFF/WAVE.
    if sample.startswith(b"RIFF") and b"WAVE" in sample[:16]:
        return True

    # FLAC.
    if sample.startswith(b"fLaC"):
        return True

    # MPEG program stream.
    if sample.startswith(b"\x00\x00\x01\xBA"):
        return True

    return False


def classify_candidate(
    result: HttpResult,
) -> Tuple[str, str]:
    """
    Classify a non-playlist HTTP response.

    A generic HTTP 200 is not sufficient to call a response a media stream.
    """
    if result.error:
        return "http_error", result.error

    if not result.status:
        return "connection_error", "No HTTP status"

    if not 200 <= result.status < 300:
        return (
            "http_error",
            f"HTTP status {result.status}",
        )

    if not result.body:
        return "empty_response", "Response body is empty"

    if body_looks_like_html(
        result.body,
        result.content_type,
    ):
        return (
            "html_or_block_page",
            "HTML response, login page or proxy challenge",
        )

    if body_looks_like_dash(
        result.body,
        result.content_type,
        result.final_url or result.requested_url,
    ):
        return (
            "dash_manifest",
            "DASH MPD manifest; not terminal media data",
        )

    if looks_like_playlist_response(result):
        return (
            "playlist_response",
            "Response contains an M3U/HLS playlist",
        )

    content_type = result.content_type.lower()
    media_url = (
        result.final_url
        or result.requested_url
    )

    if any(
        marker in content_type
        for marker in MEDIA_CONTENT_TYPES
    ):
        return (
            "media_data_http_checked",
            "Media-like MIME type and non-empty HTTP response; "
            "decoding not tested",
        )

    if url_looks_like_media(media_url):
        if payload_looks_like_media(result.body):
            return (
                "media_data_http_checked",
                "Media extension and recognizable media payload; "
                "decoding not tested",
            )

        return (
            "media_candidate_unconfirmed",
            "Media-like URL but payload signature was not recognized",
        )

    if payload_looks_like_media(result.body):
        return (
            "media_data_http_checked",
            "Recognizable media payload signature; decoding not tested",
        )

    if any(
        marker in content_type
        for marker in TEXT_CONTENT_TYPES
    ):
        return (
            "non_media_response",
            f"Non-media Content-Type: {content_type}",
        )

    return (
        "unrecognized_payload",
        "HTTP response is non-empty, but media type or payload "
        "could not be confirmed",
    )


class Dissector:
    def __init__(self, args):
        self.args = args

        self.client = HttpClient(
            timeout=args.timeout,
            max_bytes_per_request=args.max_bytes,
            max_total_bytes=args.max_total_bytes,
            max_requests=args.max_requests,
            user_agent=args.user_agent,
            insecure_tls=args.insecure_tls,
            allow_private=args.allow_private,
        )

        self.started = time.monotonic()
        self.created = utc_now()

        self.lock = threading.RLock()
        self.work_queue: queue.Queue = queue.Queue()
        self.stop_token = object()

        self.seen: Set[str] = set()
        self.inflight: Set[str] = set()
        self.root_urls: List[str] = []

        self.records: List[Record] = []
        self.errors: List[Dict[str, str]] = []
        self.events: List[str] = []
        self.validated: List[M3UEntry] = []

        self.stats = Counter()

        self.runtime_exceeded = False
        self.max_depth_reached = 0

        self.index_urls_found: List[str] = []
        self.index_urls_selected: List[str] = []

    def log_error(
        self,
        url: str,
        message: str,
    ) -> None:
        with self.lock:
            self.errors.append({
                "time": utc_now(),
                "url": url,
                "error": message,
            })

    def log_event(self, message: str) -> None:
        with self.lock:
            self.events.append(message)

    def increment(
        self,
        key: str,
        amount: int = 1,
    ) -> None:
        with self.lock:
            self.stats[key] += amount

    def enqueue(
        self,
        url: str,
        depth: int,
        role: str,
        entry: Optional[M3UEntry] = None,
        parent_url: str = "",
    ) -> bool:
        normalized = normalize_url(url)

        if not normalized:
            self.log_error(url, "invalid_url")
            return False

        if depth > self.args.max_depth:
            self.increment("depth_limited")
            self.log_event(
                f"Depth limit reached: {normalized}"
            )
            return False

        with self.lock:
            if (
                normalized in self.seen
                or normalized in self.inflight
            ):
                self.stats["duplicate_urls_skipped"] += 1
                return False

            if (
                len(self.seen) + len(self.inflight)
                >= self.args.max_sources
            ):
                self.stats["source_limited"] += 1
                self.log_event(
                    "Source limit reached: "
                    f"{self.args.max_sources}"
                )
                return False

            self.inflight.add(normalized)

            self.work_queue.put(
                WorkItem(
                    url=normalized,
                    depth=depth,
                    role=role,
                    entry=entry,
                    parent_url=parent_url,
                )
            )

            self.stats["queued"] += 1

        return True

    def mark_finished(self, url: str) -> None:
        with self.lock:
            self.inflight.discard(url)
            self.seen.add(url)

    def record(
        self,
        item: WorkItem,
        result: HttpResult,
        classification: str,
        detail: str = "",
    ) -> None:
        rec = Record(
            url=item.url,
            final_url=result.final_url or item.url,
            status=result.status,
            classification=classification,
            depth=item.depth,
            role=item.role,
            elapsed=round(result.elapsed, 4),
            bytes_read=len(result.body),
            content_type=result.content_type,
            detail=detail,
            parent_url=item.parent_url,
            truncated=result.truncated,
            redirect_chain=list(result.redirect_chain),
        )

        with self.lock:
            self.records.append(rec)

            self.stats[
                f"classification:{classification}"
            ] += 1

            self.max_depth_reached = max(
                self.max_depth_reached,
                item.depth,
            )

            if result.redirect_chain:
                self.stats["requests_with_redirects"] += 1
                self.stats["redirect_hops"] += len(
                    result.redirect_chain
                )

    def output_entry(
        self,
        url: str,
        inherited: Optional[M3UEntry],
    ) -> M3UEntry:
        if inherited:
            return dataclasses.replace(
                inherited,
                url=url,
            )

        host = (
            urllib.parse.urlsplit(url).hostname
            or url
        )

        return M3UEntry(
            url=url,
            name=host,
        )

    def emit_validated(
        self,
        url: str,
        inherited: Optional[M3UEntry],
        classification: str,
    ) -> None:
        """
        Add a structurally recognized playback candidate.

        The report distinguishes candidate validation from actual decoding.
        """
        normalized = normalize_url(url)

        if not normalized:
            return

        entry = self.output_entry(
            normalized,
            inherited,
        )

        entry.name = safe_display_name(
            entry.name,
            normalized,
        )

        with self.lock:
            self.validated.append(entry)
            self.stats["validated_terminal_candidates"] += 1

        self.log_event(
            f"Playback candidate [{classification}]: {normalized}"
        )

    def enqueue_entry(
        self,
        entry: M3UEntry,
        depth: int,
    ) -> None:
        url = normalize_url(entry.url)

        if not url:
            return

        if url_looks_like_playlist(url):
            role = (
                "child_playlist"
            )

        elif url_looks_like_dash(url):
            role = "dash_manifest"

        else:
            role = "channel_candidate"

        self.enqueue(
            url,
            depth=depth,
            role=role,
            entry=entry,
            parent_url=entry.source_playlist,
        )

    def handle_index_playlist(
        self,
        item: WorkItem,
        result: HttpResult,
    ) -> None:
        """
        Special handling for INDEX.m3u.

        The index itself is parsed, but only the first N unique playlist
        URLs are selected for recursive processing. This avoids blindly
        enqueuing every entry in a large index.
        """
        text = decode_body(result.body)
        base_url = result.final_url or item.url

        entries = parse_m3u_entries(
            text,
            base_url,
        )

        playlist_urls: List[str] = []
        seen_urls: Set[str] = set()

        for entry in entries:
            candidate = normalize_url(entry.url)

            if (
                candidate
                and url_looks_like_playlist(candidate)
                and candidate not in seen_urls
            ):
                seen_urls.add(candidate)
                playlist_urls.append(candidate)

        for candidate in extract_playlist_urls(
            text,
            base_url,
        ):
            if candidate not in seen_urls:
                seen_urls.add(candidate)
                playlist_urls.append(candidate)

        self.index_urls_found = list(playlist_urls)

        limit = max(
            0,
            self.args.index_playlists,
        )

        selected = playlist_urls[:limit]

        self.index_urls_selected = list(selected)

        self.record(
            item,
            result,
            "index_playlist",
            (
                f"playlist_urls_found={len(playlist_urls)}, "
                f"selected={len(selected)}, "
                f"selection_limit={limit}"
            ),
        )

        self.increment(
            "index_playlist_urls_found",
            len(playlist_urls),
        )

        self.increment(
            "index_playlist_urls_selected",
            len(selected),
        )

        for index, child in enumerate(selected, 1):
            self.enqueue(
                child,
                depth=item.depth + 1,
                role="index_child_playlist",
                parent_url=base_url,
            )

            if index % 25 == 0:
                self.log_event(
                    f"INDEX selection progress: "
                    f"{index}/{len(selected)}"
                )

        if len(playlist_urls) > len(selected):
            self.log_event(
                "INDEX selection limited: "
                f"{len(selected)} of {len(playlist_urls)} "
                "playlist URLs selected"
            )

        if not selected:
            self.log_error(
                item.url,
                "INDEX.m3u contained no recognized playlist URLs",
            )

    def handle_dash_manifest(
        self,
        item: WorkItem,
        result: HttpResult,
    ) -> None:
        """
        Record a DASH manifest and enqueue explicitly referenced media.

        The MPD URL itself is never emitted as a terminal video stream.
        """
        text = decode_body(result.body)
        base_url = result.final_url or item.url

        parsed = parse_dash_manifest(
            text,
            base_url,
        )

        if not parsed["is_mpd"]:
            self.record(
                item,
                result,
                "dash_manifest_parse_failed",
                str(parsed.get("detail", "")),
            )

            self.log_error(
                item.url,
                (
                    "DASH-like response could not be parsed as MPD: "
                    f"{parsed.get('detail', '')}"
                ),
            )

            return

        base_urls = list(parsed["base_urls"])
        segment_urls = list(parsed["segment_urls"])
        initialization_urls = list(
            parsed["initialization_urls"]
        )

        self.record(
            item,
            result,
            "dash_manifest",
            str(parsed["detail"]),
        )

        self.increment("dash_manifests_parsed")

        # BaseURL entries may be directories or representation roots.
        # Queue only explicit media-looking URLs or explicit segment URLs.
        children: List[Tuple[str, str]] = []

        for segment_url in segment_urls:
            children.append(
                (segment_url, "dash_segment")
            )

        for initialization_url in initialization_urls:
            children.append(
                (
                    initialization_url,
                    "dash_initialization_segment",
                )
            )

        for base_url_candidate in base_urls:
            if url_looks_like_media(base_url_candidate):
                children.append(
                    (
                        base_url_candidate,
                        "dash_base_media_candidate",
                    )
                )

        unique_children: List[Tuple[str, str]] = []
        seen_children: Set[str] = set()

        for child_url, role in children:
            normalized = normalize_url(child_url)

            if (
                normalized
                and normalized not in seen_children
            ):
                seen_children.add(normalized)
                unique_children.append(
                    (normalized, role)
                )

        selected_children = unique_children[
            :self.args.max_children
        ]

        for child_url, role in selected_children:
            self.enqueue(
                child_url,
                depth=item.depth + 1,
                role=role,
                entry=item.entry,
                parent_url=base_url,
            )

        self.increment(
            "dash_explicit_media_references",
            len(unique_children),
        )

        if len(unique_children) > len(selected_children):
            self.log_event(
                f"DASH child limit for {base_url}: "
                f"{len(unique_children)} references; "
                f"processing {len(selected_children)}"
            )

        if (
            parsed["segment_templates"]
            and not segment_urls
        ):
            self.log_event(
                "DASH SegmentTemplate expressions found but not "
                "expanded automatically: "
                f"{base_url}"
            )

    def handle_playlist(
        self,
        item: WorkItem,
        result: HttpResult,
    ) -> None:
        text = decode_body(result.body)
        base_url = result.final_url or item.url

        self.increment("playlists_parsed")

        # INDEX.m3u is special: select a fixed number of playlist URLs.
        if (
            item.role == "index_playlist"
            or url_is_index(item.url)
        ):
            self.handle_index_playlist(
                item,
                result,
            )
            return

        hls = parse_hls(
            text,
            base_url,
        )

        # HLS master playlist.
        if hls["is_master"]:
            variant_details = list(
                hls["variant_details"]
            )

            original_variant_count = len(
                variant_details
            )

            if (
                self.args.hls_variant_policy
                == "highest"
            ):
                variant_details = sorted(
                    variant_details,
                    key=lambda value: int(
                        value.get("bandwidth", 0)
                    ),
                    reverse=True,
                )[:1]

            elif (
                self.args.hls_variant_policy
                == "lowest"
            ):
                variant_details = sorted(
                    variant_details,
                    key=lambda value: int(
                        value.get("bandwidth", 0)
                    ),
                )[:1]

            variants = [
                str(value["url"])
                for value in variant_details
            ]

            self.record(
                item,
                result,
                "hls_master_playlist",
                (
                    f"variants_found={original_variant_count}, "
                    f"variants_selected={len(variants)}, "
                    f"policy={self.args.hls_variant_policy}"
                ),
            )

            for variant in variants[
                :self.args.max_children
            ]:
                self.enqueue(
                    variant,
                    depth=item.depth + 1,
                    role="hls_variant",
                    entry=item.entry,
                    parent_url=base_url,
                )

            self.increment(
                "hls_variants_discovered",
                len(variants),
            )

            return

        # HLS media playlist.
        if hls["is_media"]:
            segments = list(hls["segments"])
            probe_succeeded = True
            probe_detail = ""

            if (
                self.args.check_hls_segment
                and segments
            ):
                segment_url = segments[0]

                probe = self.client.get(
                    segment_url,
                    byte_limit=self.args.segment_probe_bytes,
                )

                probe_class, probe_detail = (
                    classify_candidate(probe)
                )

                self.record(
                    WorkItem(
                        url=segment_url,
                        depth=item.depth + 1,
                        role="hls_segment_probe",
                        entry=item.entry,
                        parent_url=base_url,
                    ),
                    probe,
                    "hls_segment_probe:" + probe_class,
                    probe_detail,
                )

                probe_succeeded = (
                    probe.ok
                    and probe_class
                    == "media_data_http_checked"
                )

                if not probe_succeeded:
                    self.log_error(
                        segment_url,
                        (
                            "First HLS segment probe did not confirm "
                            f"media data: {probe_class}: {probe_detail}"
                        ),
                    )

                    self.increment(
                        "hls_segment_probe_unconfirmed"
                    )

            self.record(
                item,
                result,
                "hls_media_playlist",
                (
                    f"segments={len(segments)}, "
                    f"first_segment_probe="
                    f"{'confirmed' if probe_succeeded else 'unconfirmed'}"
                ),
            )

            # A structurally valid HLS media playlist is a playback
            # candidate. It is not claimed to have decoded successfully.
            self.emit_validated(
                base_url,
                item.entry,
                "hls_media_playlist",
            )

            self.increment(
                "hls_media_playlists_discovered"
            )

            return

        # Normal IPTV M3U, including EXTINF:-1.
        entries = parse_m3u_entries(
            text,
            base_url,
        )

        if entries:
            self.record(
                item,
                result,
                "m3u_playlist",
                f"entries={len(entries)}",
            )

            self.increment(
                "m3u_entries_discovered",
                len(entries),
            )

            selected_entries = entries[
                :self.args.max_children
            ]

            for entry in selected_entries:
                self.enqueue_entry(
                    entry,
                    item.depth + 1,
                )

            if len(entries) > len(selected_entries):
                self.log_event(
                    f"Child limit for {base_url}: "
                    f"{len(entries)} entries; "
                    f"processing {len(selected_entries)}"
                )

            return

        # Fallback for plain-text indexes or embedded playlist URLs.
        playlist_urls = extract_playlist_urls(
            text,
            base_url,
        )

        if playlist_urls:
            selected = playlist_urls[
                :self.args.max_children
            ]

            self.record(
                item,
                result,
                "playlist_index",
                (
                    f"children_found={len(playlist_urls)}, "
                    f"children_selected={len(selected)}"
                ),
            )

            for child in selected:
                self.enqueue(
                    child,
                    depth=item.depth + 1,
                    role="child_playlist",
                    entry=item.entry,
                    parent_url=base_url,
                )

            self.increment(
                "playlist_index_children",
                len(playlist_urls),
            )

            return

        self.record(
            item,
            result,
            "playlist_empty_or_unrecognized",
            (
                "No entries, HLS variants, segments or child "
                "playlist URLs were recognized"
            ),
        )

        self.log_error(
            item.url,
            (
                "Playlist downloaded but no recognized entries "
                "or child playlist URLs were found"
            ),
        )

    def process_item(
        self,
        item: WorkItem,
    ) -> None:
        if (
            time.monotonic() - self.started
            > self.args.max_runtime
        ):
            self.runtime_exceeded = True

            self.log_event(
                "Runtime limit reached before request: "
                f"{item.url}"
            )

            self.increment("runtime_limited_items")
            return

        result = self.client.get(
            item.url,
            byte_limit=self.args.max_bytes,
        )

        if result.error and not result.status:
            self.record(
                item,
                result,
                "request_failed",
                result.error,
            )

            self.log_error(
                item.url,
                result.error,
            )

            return

        if not result.ok:
            self.record(
                item,
                result,
                "http_error",
                (
                    result.error
                    or f"HTTP status {result.status}"
                ),
            )

            self.log_error(
                item.url,
                (
                    result.error
                    or f"http_status_{result.status}"
                ),
            )

            return

        if not result.body:
            self.record(
                item,
                result,
                "empty_response",
                "Empty response body",
            )

            self.log_error(
                item.url,
                "empty_response",
            )

            return

        if body_looks_like_html(
            result.body,
            result.content_type,
        ):
            self.record(
                item,
                result,
                "html_or_block_page",
                (
                    "HTML response, login page, proxy challenge "
                    "or anti-bot page"
                ),
            )

            self.log_error(
                item.url,
                "html_or_block_page",
            )

            return

        if body_looks_like_dash(
            result.body,
            result.content_type,
            result.final_url or item.url,
        ):
            self.handle_dash_manifest(
                item,
                result,
            )
            return

        if looks_like_playlist_response(result):
            self.handle_playlist(
                item,
                result,
            )
            return

        classification, detail = classify_candidate(
            result
        )

        self.record(
            item,
            result,
            classification,
            detail,
        )

        if classification == "media_data_http_checked":
            final_url = (
                result.final_url
                or item.url
            )

            self.emit_validated(
                final_url,
                item.entry,
                classification,
            )

        elif classification in (
            "dash_manifest",
            "playlist_response",
        ):
            # Defensive branch. Manifests should normally have been
            # dispatched above, but they must never be emitted as media.
            self.log_event(
                f"Non-terminal response not exported: {item.url}"
            )

        else:
            self.log_error(
                item.url,
                f"{classification}: {detail}",
            )

    def worker(self) -> None:
        while True:
            item = self.work_queue.get()

            try:
                if item is self.stop_token:
                    return

                try:
                    self.process_item(item)

                except Exception as exc:
                    self.log_error(
                        item.url,
                        (
                            f"unexpected_{type(exc).__name__}: "
                            f"{exc}"
                        ),
                    )

                    self.increment("unexpected_worker_errors")

                finally:
                    self.mark_finished(item.url)

            finally:
                self.work_queue.task_done()

    def run(
        self,
        sources: List[str],
    ) -> None:
        self.root_urls = list(
            dict.fromkeys(
                normalize_url(url)
                for url in sources
                if normalize_url(url)
            )
        )

        for url in self.root_urls:
            role = (
                "index_playlist"
                if url_is_index(url)
                else "root_playlist"
            )

            self.enqueue(
                url,
                depth=0,
                role=role,
            )

        worker_count = max(
            1,
            self.args.workers,
        )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count
        ) as executor:
            futures = [
                executor.submit(self.worker)
                for _ in range(worker_count)
            ]

            # Workers may enqueue children while processing parents.
            # Queue.join waits for those children as well.
            self.work_queue.join()

            for _ in range(worker_count):
                self.work_queue.put(self.stop_token)

            self.work_queue.join()

            for future in futures:
                future.result()

        self.deduplicate_validated()

    def deduplicate_validated(self) -> None:
        """Deduplicate playback candidates by normalized URL."""
        def richness(entry: M3UEntry) -> int:
            return sum(
                bool(value)
                for value in (
                    entry.name,
                    entry.extinf,
                    entry.group_title,
                    entry.tvg_id,
                    entry.tvg_name,
                    entry.tvg_logo,
                    entry.source_playlist,
                )
            )

        by_url: Dict[str, M3UEntry] = {}

        for entry in self.validated:
            url = normalize_url(entry.url)

            if not url:
                continue

            current = by_url.get(url)

            if (
                current is None
                or richness(entry) > richness(current)
            ):
                by_url[url] = entry

        self.validated = list(
            by_url.values()
        )

        self.stats["validated_unique_urls"] = len(
            self.validated
        )

    def summary(self) -> Dict[str, object]:
        return {
            "version": VERSION,
            "created": self.created,
            "finished": utc_now(),
            "elapsed_seconds": round(
                time.monotonic() - self.started,
                3,
            ),
            "root_urls": self.root_urls,
            "root_url_count": len(self.root_urls),
            "urls_seen": len(self.seen),
            "http_records": len(self.records),
            "http_requests_used": self.client.requests_used,
            "http_requests_limit": self.client.max_requests,
            "bytes_read": self.client.bytes_used,
            "bytes_limit": self.client.max_total_bytes,
            "runtime_limit_seconds": self.args.max_runtime,
            "max_depth_configured": self.args.max_depth,
            "max_depth_reached": self.max_depth_reached,
            "max_sources_configured": self.args.max_sources,
            "index_playlists_limit": self.args.index_playlists,
            "index_playlist_urls_found": len(
                self.index_urls_found
            ),
            "index_playlist_urls_selected": len(
                self.index_urls_selected
            ),
            "validated_terminal_candidates": len(
                self.validated
            ),
            "errors": len(self.errors),
            "stats": dict(self.stats),
            "runtime_exceeded": self.runtime_exceeded,
            "budget_events": list(
                self.client.budget_events
            ),
            "events": list(self.events),
        }

    def write_outputs(self) -> None:
        """Write JSON, TXT and M3U output files."""
        os.makedirs(
            self.args.output_dir,
            exist_ok=True,
        )

        summary = self.summary()

        report_json = {
            "summary": summary,
            "root_urls": self.root_urls,
            "index": {
                "playlist_urls_found": self.index_urls_found,
                "playlist_urls_selected": self.index_urls_selected,
            },
            "records": [
                dataclasses.asdict(record)
                for record in self.records
            ],
            "validated": [
                {
                    "url": entry.url,
                    "name": entry.name,
                    "metadata": entry.metadata(),
                }
                for entry in self.validated
            ],
            "errors": self.errors,
        }

        json_path = os.path.join(
            self.args.output_dir,
            "report.json",
        )

        with open(
            json_path,
            "w",
            encoding="utf-8",
        ) as output:
            json.dump(
                report_json,
                output,
                ensure_ascii=False,
                indent=2,
            )

        txt_path = os.path.join(
            self.args.output_dir,
            "report.txt",
        )

        with open(
            txt_path,
            "w",
            encoding="utf-8",
        ) as output:
            output.write(
                f"Phoenix URL Dissector {VERSION}\n"
            )

            output.write("=" * 76 + "\n")
            output.write(
                f"Created: {self.created}\n"
            )
            output.write(
                f"Finished: {utc_now()}\n\n"
            )

            output.write("ROOT SOURCES\n")
            output.write("-" * 76 + "\n")

            for url in self.root_urls:
                output.write(f"- {url}\n")

            output.write("\nINDEX PLAYLIST SELECTION\n")
            output.write("-" * 76 + "\n")

            output.write(
                "Configured selection limit: "
                f"{self.args.index_playlists}\n"
            )

            output.write(
                "Playlist URLs found: "
                f"{len(self.index_urls_found)}\n"
            )

            output.write(
                "Playlist URLs selected: "
                f"{len(self.index_urls_selected)}\n"
            )

            for index, url in enumerate(
                self.index_urls_selected,
                1,
            ):
                output.write(
                    f"{index}. {url}\n"
                )

            output.write("\nSUMMARY\n")
            output.write("-" * 76 + "\n")

            output.write(
                f"URLs seen: {summary['urls_seen']}\n"
            )

            output.write(
                f"Root URLs: {summary['root_url_count']}\n"
            )

            output.write(
                f"HTTP records: {len(self.records)}\n"
            )

            output.write(
                "Validated terminal candidates: "
                f"{len(self.validated)}\n"
            )

            output.write(
                f"Errors: {len(self.errors)}\n"
            )

            output.write(
                "Maximum depth reached: "
                f"{self.max_depth_reached}\n"
            )

            output.write(
                "Elapsed: "
                f"{summary['elapsed_seconds']} seconds\n"
            )

            output.write(
                "Runtime limit exceeded: "
                f"{self.runtime_exceeded}\n"
            )

            output.write("\nHTTP BUDGET\n")
            output.write("-" * 76 + "\n")

            output.write(
                f"Requests: {self.client.requests_used}/"
                f"{self.client.max_requests}\n"
            )

            output.write(
                f"Bytes: {self.client.bytes_used}/"
                f"{self.client.max_total_bytes}\n"
            )

            output.write("\nCLASSIFICATIONS\n")
            output.write("-" * 76 + "\n")

            counts = Counter(
                record.classification
                for record in self.records
            )

            for key, value in sorted(
                counts.items()
            ):
                output.write(
                    f"{key}: {value}\n"
                )

            output.write("\nCOUNTERS\n")
            output.write("-" * 76 + "\n")

            for key, value in sorted(
                self.stats.items()
            ):
                output.write(
                    f"{key}: {value}\n"
                )

            output.write("\nVALIDATED PLAYBACK CANDIDATES\n")
            output.write("-" * 76 + "\n")

            if not self.validated:
                output.write(
                    "No playback candidates were recognized.\n"
                )

            else:
                for index, entry in enumerate(
                    self.validated,
                    1,
                ):
                    output.write(
                        f"{index}. {entry.url}\n"
                    )

                    output.write(
                        f"   Name: {entry.name}\n"
                    )

                    if entry.group_title:
                        output.write(
                            f"   Group: {entry.group_title}\n"
                        )

                    if entry.tvg_id:
                        output.write(
                            f"   tvg-id: {entry.tvg_id}\n"
                        )

                    if entry.tvg_name:
                        output.write(
                            f"   tvg-name: {entry.tvg_name}\n"
                        )

                    if entry.tvg_logo:
                        output.write(
                            f"   tvg-logo: {entry.tvg_logo}\n"
                        )

                    if entry.source_playlist:
                        output.write(
                            "   Source playlist: "
                            f"{entry.source_playlist}\n"
                        )

            output.write("\nREDIRECT CHAINS\n")
            output.write("-" * 76 + "\n")

            found_redirects = False

            for record in self.records:
                for hop_number, hop in enumerate(
                    record.redirect_chain,
                    1,
                ):
                    found_redirects = True

                    output.write(
                        f"{record.url} [hop {hop_number}]\n"
                    )

                    output.write(
                        f"  HTTP {hop.get('status')}: "
                        f"{hop.get('from')}\n"
                    )

                    output.write(
                        f"  -> {hop.get('to')}\n"
                    )

            if not found_redirects:
                output.write(
                    "No redirects observed.\n"
                )

            output.write("\nERRORS\n")
            output.write("-" * 76 + "\n")

            if not self.errors:
                output.write(
                    "No errors recorded.\n"
                )

            else:
                for error in self.errors:
                    output.write(
                        f"- {error['url']}: "
                        f"{error['error']}\n"
                    )

            output.write("\nBUDGET EVENTS\n")
            output.write("-" * 76 + "\n")

            if not self.client.budget_events:
                output.write(
                    "No budget limits reached.\n"
                )

            else:
                for event in self.client.budget_events:
                    output.write(
                        f"- {event}\n"
                    )

            output.write("\nEVENTS\n")
            output.write("-" * 76 + "\n")

            if not self.events:
                output.write(
                    "No additional events recorded.\n"
                )

            else:
                for event in self.events:
                    output.write(
                        f"- {event}\n"
                    )

            output.write("\nDETAILED RECORDS\n")
            output.write("-" * 76 + "\n")

            for record in self.records:
                output.write(
                    f"[{record.classification}] "
                    f"HTTP {record.status} "
                    f"depth={record.depth} "
                    f"role={record.role}\n"
                )

                output.write(
                    f"  URL: {record.url}\n"
                )

                output.write(
                    f"  Final URL: {record.final_url}\n"
                )

                output.write(
                    f"  Parent URL: {record.parent_url}\n"
                )

                if record.content_type:
                    output.write(
                        "  Content-Type: "
                        f"{record.content_type}\n"
                    )

                output.write(
                    f"  Bytes: {record.bytes_read}; "
                    f"Elapsed: {record.elapsed}s\n"
                )

                output.write(
                    f"  Truncated: {record.truncated}\n"
                )

                if record.detail:
                    output.write(
                        f"  Detail: {record.detail}\n"
                    )

                for hop_number, hop in enumerate(
                    record.redirect_chain,
                    1,
                ):
                    output.write(
                        f"  Redirect {hop_number}: "
                        f"HTTP {hop.get('status')} "
                        f"{hop.get('from')} -> "
                        f"{hop.get('to')}\n"
                    )

            output.write("\nLIMITATIONS\n")
            output.write("-" * 76 + "\n")

            output.write(
                "HTTP success does not prove successful video decoding.\n"
            )

            output.write(
                "Media candidates are checked using HTTP metadata, "
                "URL patterns and limited payload signatures.\n"
            )

            output.write(
                "No full media decoding is performed.\n"
            )

            output.write(
                "DASH SegmentTemplate expressions requiring numbering "
                "or timing context are not expanded automatically.\n"
            )

            output.write(
                "When enabled, HLS segment probing checks only the first "
                "referenced segment.\n"
            )

        m3u_path = os.path.join(
            self.args.output_dir,
            "validated_streams.m3u",
        )

        with open(
            m3u_path,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as output:
            output.write("#EXTM3U\n")

            for entry in self.validated:
                if entry.extinf.startswith("#EXTINF:"):
                    output.write(
                        entry.extinf + "\n"
                    )

                else:
                    name = (
                        entry.name
                        or entry.url
                    )

                    output.write(
                        f"#EXTINF:-1,{name}\n"
                    )

                for directive in entry.directives:
                    if directive.startswith("#EXTINF:"):
                        continue

                    if directive.upper() == "#EXTM3U":
                        continue

                    output.write(
                        directive + "\n"
                    )

                output.write(
                    entry.url + "\n"
                )

        print(f"Report: {txt_path}")
        print(f"JSON:   {json_path}")
        print(f"M3U:    {m3u_path}")


def self_test() -> int:
    """Run offline tests without making network requests."""
    failures: List[str] = []
    checks = 0

    def check(
        condition: bool,
        name: str,
    ) -> None:
        nonlocal checks

        checks += 1

        if not condition:
            failures.append(name)

    regular_m3u = """#EXTM3U
#EXTINF:-1 tvg-id="news.one" tvg-name="News One" group-title="News",News One HD
https://example.invalid/news/index.m3u8
#EXTINF:-1 group-title="Movies",Movie Channel
https://example.invalid/movie/stream.ts
"""

    entries = parse_m3u_entries(
        regular_m3u,
        "https://example.invalid/list.m3u",
    )

    parsed_iptv_as_hls = parse_hls(
        regular_m3u,
        "https://example.invalid/list.m3u",
    )

    check(
        len(entries) == 2,
        "iptv_entries",
    )

    check(
        entries[0].tvg_id == "news.one",
        "tvg_id",
    )

    check(
        entries[0].group_title == "News",
        "group_title",
    )

    check(
        not parsed_iptv_as_hls["is_media"],
        "iptv_not_hls_media",
    )

    master_text = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1280000,RESOLUTION=1280x720
720/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=640000,RESOLUTION=640x360
360/index.m3u8
"""

    master = parse_hls(
        master_text,
        "https://example.invalid/master.m3u8",
    )

    check(
        master["is_master"],
        "hls_master",
    )

    check(
        len(master["variants"]) == 2,
        "hls_variants",
    )

    variant_details = master["variant_details"]

    highest = max(
        variant_details,
        key=lambda item: int(item["bandwidth"]),
    )

    check(
        highest["url"]
        == "https://example.invalid/720/index.m3u8",
        "hls_highest_bandwidth",
    )

    media_text = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:9.5,
segment0.ts
#EXTINF:8.0,
segment1.ts
#EXT-X-ENDLIST
"""

    media = parse_hls(
        media_text,
        "https://example.invalid/live/index.m3u8",
    )

    check(
        media["is_media"],
        "hls_media",
    )

    check(
        len(media["segments"]) == 2,
        "hls_segments",
    )

    check(
        normalize_url(
            "HTTPS://Example.COM/a.m3u8?token=1"
        )
        == "https://example.com/a.m3u8?token=1",
        "normalize_url",
    )

    check(
        resolve_url(
            "https://example.invalid/a/index.m3u",
            "../b/list.m3u8",
        )
        == "https://example.invalid/b/list.m3u8",
        "relative_url",
    )

    check(
        url_is_index(
            "https://example.invalid/data/INDEX.m3u?token=1"
        ),
        "index_detection",
    )

    check(
        not url_is_index(
            "https://example.invalid/data/playlist.m3u"
        ),
        "non_index_detection",
    )

    check(
        extinf_display_name(
            '#EXTINF:-1 tvg-name="A,B",Channel Name'
        )
        == "Channel Name",
        "quoted_comma_extinf",
    )

    check(
        looks_like_user_agent_name(
            "like Gecko) Chrome/126.0 Safari/537.36"
        ),
        "user_agent_name_detection",
    )

    check(
        safe_display_name(
            "like Gecko) Chrome/126.0 Safari/537.36",
            "https://example.invalid/live.ts",
        )
        == "example.invalid",
        "user_agent_name_fallback",
    )

    check(
        payload_looks_like_media(
            b"\x00\x00\x00\x18ftypisom"
        ),
        "mp4_signature",
    )

    check(
        not payload_looks_like_media(
            b'{"message":"not a stream"}'
        ),
        "json_not_media",
    )

    dash_text = """<?xml version="1.0"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"
     type="static">
  <Period>
    <AdaptationSet mimeType="video/mp4">
      <Representation id="1" bandwidth="1000000">
        <BaseURL>video/</BaseURL>
        <SegmentList>
          <Initialization sourceURL="init.mp4"/>
          <SegmentURL media="segment1.m4s"/>
          <SegmentURL media="segment2.m4s"/>
        </SegmentList>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>
"""

    dash = parse_dash_manifest(
        dash_text,
        "https://example.invalid/manifest.mpd",
    )

    check(
        dash["is_mpd"],
        "dash_mpd_detected",
    )

    check(
        len(dash["segment_urls"]) == 2,
        "dash_segment_urls",
    )

    check(
        len(dash["initialization_urls"]) == 1,
        "dash_initialization_url",
    )

    dash_result = HttpResult(
        requested_url=(
            "https://example.invalid/manifest.mpd"
        ),
        final_url=(
            "https://example.invalid/manifest.mpd"
        ),
        status=200,
        headers={
            "content-type": "application/dash+xml"
        },
        body=dash_text.encode("utf-8"),
    )

    check(
        classify_candidate(dash_result)[0]
        == "dash_manifest",
        "dash_not_terminal_stream",
    )

    html_result = HttpResult(
        requested_url="https://example.invalid/live",
        final_url="https://example.invalid/live",
        status=200,
        headers={"content-type": "text/html"},
        body=b"<html><body>Access denied</body></html>",
    )

    check(
        classify_candidate(html_result)[0]
        == "html_or_block_page",
        "html_not_stream",
    )

    json_result = HttpResult(
        requested_url="https://example.invalid/api",
        final_url="https://example.invalid/api",
        status=200,
        headers={"content-type": "application/json"},
        body=b'{"ok":true}',
    )

    check(
        classify_candidate(json_result)[0]
        == "non_media_response",
        "json_not_terminal_stream",
    )

    if failures:
        print("SELF-TEST FAILED")

        for failure in failures:
            print(f"  FAIL: {failure}")

        print(
            f"Failed {len(failures)} of {checks} checks"
        )

        return 1

    print(f"Phoenix URL Dissector {VERSION}")
    print(
        f"SELF-TEST PASSED ({checks} checks)"
    )

    return 0


def read_sources(args) -> List[str]:
    """Read command-line, file, stdin and default source URLs."""
    sources: List[str] = []

    if args.url:
        sources.extend(args.url)

    if args.input_file:
        with open(
            args.input_file,
            "r",
            encoding="utf-8-sig",
        ) as source_file:
            for line in source_file:
                line = line.strip()

                if line and not line.startswith("#"):
                    sources.append(line)

    if args.stdin:
        for line in sys.stdin:
            line = line.strip()

            if line and not line.startswith("#"):
                sources.append(line)

    if args.discover_livem3u:
        sources.extend(DEFAULT_SOURCES)

    if not sources:
        sources.extend(DEFAULT_SOURCES)

    unique: List[str] = []
    seen: Set[str] = set()

    for source in sources:
        normalized = normalize_url(source)

        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)

    return unique


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            f"Phoenix URL Dissector {VERSION}: recursive "
            "M3U/HLS/DASH discovery and HTTP redirect tracing"
        )
    )

    parser.add_argument(
        "--url",
        action="append",
        help="Source URL; may be supplied more than once",
    )

    parser.add_argument(
        "--input-file",
        help="Read source URLs from a text file",
    )

    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read source URLs from standard input",
    )

    parser.add_argument(
        "--discover-livem3u",
        action="store_true",
        help="Add the default LiveM3U INDEX and playlist URLs",
    )

    parser.add_argument(
        "--index-playlists",
        type=int,
        default=DEFAULT_INDEX_PLAYLISTS,
        help=(
            "Maximum number of unique playlist URLs selected "
            "from INDEX.m3u (default: 100)"
        ),
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="HTTP request timeout in seconds",
    )

    parser.add_argument(
        "--max-bytes",
        type=int,
        default=2 * 1024 * 1024,
        help="Maximum response bytes retained per request",
    )

    parser.add_argument(
        "--max-total-bytes",
        type=int,
        default=20 * 1024 * 1024,
        help="Maximum total response bytes retained",
    )

    parser.add_argument(
        "--max-runtime",
        type=float,
        default=300.0,
        help="Maximum run time in seconds",
    )

    parser.add_argument(
        "--max-requests",
        type=int,
        default=100,
        help="Maximum number of HTTP requests",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=12,
        help="Maximum recursive discovery depth",
    )

    parser.add_argument(
        "--max-sources",
        type=int,
        default=2000,
        help="Maximum number of unique queued sources",
    )

    parser.add_argument(
        "--max-children",
        type=int,
        default=1000,
        help="Maximum children processed from one playlist",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent worker threads",
    )

    parser.add_argument(
        "--segment-probe-bytes",
        type=int,
        default=64 * 1024,
        help="Byte limit for the first HLS segment probe",
    )

    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent header",
    )

    parser.add_argument(
        "--hls-variant-policy",
        choices=("all", "highest", "lowest"),
        default="all",
        help=(
            "HLS variant selection policy; highest/lowest use "
            "BANDWIDTH metadata"
        ),
    )

    parser.add_argument(
        "--check-hls-segment",
        action="store_true",
        help="Probe the first HLS media segment",
    )

    parser.add_argument(
        "--allow-private",
        action="store_true",
        help=(
            "Allow literal localhost/private IP destinations "
            "(use only for trusted input)"
        ),
    )

    parser.add_argument(
        "--insecure-tls",
        action="store_true",
        help="Disable TLS certificate verification",
    )

    parser.add_argument(
        "--output-dir",
        default="phoenix_url_dissector_output",
        help="Directory for TXT, JSON and M3U reports",
    )

    parser.add_argument(
        "--fail-on-errors",
        action="store_true",
        help=(
            "Exit with code 1 if diagnostic errors were recorded. "
            "By default, errors are reported without failing the job."
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run offline self-tests and exit",
    )

    return parser


def validate_args(
    parser: argparse.ArgumentParser,
    args,
) -> None:
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    if args.max_runtime <= 0:
        parser.error("--max-runtime must be positive")

    if args.max_requests < 1:
        parser.error("--max-requests must be at least 1")

    if args.max_bytes < 1024:
        parser.error("--max-bytes must be at least 1024")

    if args.max_total_bytes < 1024:
        parser.error("--max-total-bytes must be at least 1024")

    if args.max_depth < 0:
        parser.error("--max-depth must not be negative")

    if args.max_sources < 1:
        parser.error("--max-sources must be at least 1")

    if args.max_children < 1:
        parser.error("--max-children must be at least 1")

    if args.workers < 1:
        parser.error("--workers must be at least 1")

    if args.segment_probe_bytes < 1024:
        parser.error(
            "--segment-probe-bytes must be at least 1024"
        )

    if args.index_playlists < 0:
        parser.error(
            "--index-playlists must not be negative"
        )


def main(
    argv: Optional[List[str]] = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    validate_args(
        parser,
        args,
    )

    sources = read_sources(args)

    if not sources:
        print(
            "No valid HTTP/HTTPS source URLs.",
            file=sys.stderr,
        )

        return 2

    print(f"Phoenix URL Dissector {VERSION}")
    print("=" * 76)

    print("ROOT SOURCES")

    for source in sources:
        print(f"  {source}")

    print()

    print(
        f"Limits: requests={args.max_requests}, "
        f"bytes={args.max_total_bytes}, "
        f"runtime={args.max_runtime}s, "
        f"depth={args.max_depth}, "
        f"sources={args.max_sources}, "
        f"workers={args.workers}, "
        f"index_playlists={args.index_playlists}"
    )

    print()

    dissector = Dissector(args)

    try:
        dissector.run(sources)

    except KeyboardInterrupt:
        print(
            "\nInterrupted. Attempting to write diagnostic reports.",
            file=sys.stderr,
        )

        dissector.log_event(
            "Execution interrupted by KeyboardInterrupt"
        )

    except Exception as exc:
        dissector.log_error(
            "internal",
            (
                f"fatal_{type(exc).__name__}: "
                f"{exc}"
            ),
        )

        dissector.log_event(
            "Fatal execution error; reports will be written"
        )

    try:
        dissector.write_outputs()

    except Exception as exc:
        print(
            f"Failed to write reports: {exc}",
            file=sys.stderr,
        )

        return 2

    summary = dissector.summary()

    print()
    print("SUMMARY")
    print("-" * 76)

    print(
        f"URLs seen: {summary['urls_seen']}"
    )

    print(
        f"HTTP records: {summary['http_records']}"
    )

    print(
        f"HTTP requests: {summary['http_requests_used']}"
    )

    print(
        f"Bytes read: {summary['bytes_read']}"
    )

    print(
        f"Redirect hops: "
        f"{dissector.stats['redirect_hops']}"
    )

    print(
        "INDEX playlist URLs found: "
        f"{summary['index_playlist_urls_found']}"
    )

    print(
        "INDEX playlist URLs selected: "
        f"{summary['index_playlist_urls_selected']}"
    )

    print(
        "Validated terminal candidates: "
        f"{summary['validated_terminal_candidates']}"
    )

    print(
        f"Errors: {summary['errors']}"
    )

    print(
        f"Elapsed: {summary['elapsed_seconds']} seconds"
    )

    if not dissector.validated:
        print(
            "\nWARNING: no terminal candidates were recognized. "
            "Review report.txt and report.json."
        )

    if summary["runtime_exceeded"]:
        print(
            "\nWARNING: runtime limit was reached; "
            "some queued sources may not have been processed."
        )

    if args.fail_on_errors and dissector.errors:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
