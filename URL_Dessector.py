
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phoenix URL Dissector 2.5.0-beta

Recursive M3U/IPTV/HLS discovery and HTTP redirect tracing.
Python 3.9+, standard library only.

Default sources:
  https://livem3u.tatnet.app/data/playlists/INDEX.m3u
  https://livem3u.tatnet.app/data/playlist.m3u

Features:
  * Recursive discovery of M3U/M3U8 playlists.
  * Persistent multithreaded work queue.
  * HTTP redirect chain capture: 301/302/303/307/308.
  * HLS master and media playlist parsing.
  * IPTV EXTINF metadata preservation.
  * Terminal stream candidate validation.
  * Optional first HLS segment probe.
  * Global request, byte, source, depth and runtime limits.
  * TXT/JSON reports and validated M3U output.

Limitations:
  * HTTP success is not proof of successful video decoding.
  * No full media decoding is performed.
  * Redirects are followed by urllib; every observed hop is recorded.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import json
import os
import queue
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from typing import Dict, List, Optional, Set, Tuple


VERSION = "2.5.0-beta"

DEFAULT_SOURCES = [
    "https://livem3u.tatnet.app/data/playlists/INDEX.m3u",
    "https://livem3u.tatnet.app/data/playlist.m3u",
]

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

PLAYLIST_EXTENSIONS = (".m3u", ".m3u8")

HTML_MARKERS = (
    b"<!doctype html",
    b"<html",
    b"<head",
    b"<body",
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

URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)

HLS_DURATION_RE = re.compile(
    r"^#EXTINF:\s*([0-9]+(?:\.[0-9]+)?)\s*(?:,|$)",
    re.IGNORECASE,
)

REDIRECT_CODES = {301, 302, 303, 307, 308}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def clean_url(value: str) -> str:
    return value.strip().strip("\"'<>").rstrip("),;]")


def normalize_url(url: str) -> str:
    try:
        url = clean_url(url)
        p = urllib.parse.urlsplit(url)

        if p.scheme.lower() not in ("http", "https") or not p.netloc:
            return ""

        scheme = p.scheme.lower()
        netloc = p.netloc.lower()
        path = p.path or "/"

        # Query parameters are preserved because they may contain tokens.
        return urllib.parse.urlunsplit(
            (scheme, netloc, path, p.query, "")
        )
    except Exception:
        return ""


def resolve_url(base_url: str, child_url: str) -> str:
    child_url = clean_url(child_url)
    if not child_url:
        return ""

    try:
        return normalize_url(urllib.parse.urljoin(base_url, child_url))
    except Exception:
        return ""


def path_of(url: str) -> str:
    try:
        return urllib.parse.urlsplit(url).path.lower()
    except Exception:
        return ""


def url_looks_like_playlist(url: str) -> bool:
    return path_of(url).endswith(PLAYLIST_EXTENSIONS)


def decode_body(body: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            return body.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            pass
    return body.decode("utf-8", errors="replace")


def body_looks_like_html(body: bytes, content_type: str = "") -> bool:
    if "text/html" in (content_type or "").lower():
        return True

    sample = (body or b"").lstrip()[:2048].lower()
    return any(sample.startswith(marker) for marker in HTML_MARKERS)


@dataclasses.dataclass
class HttpResult:
    requested_url: str
    final_url: str = ""
    status: int = 0
    headers: Dict[str, str] = dataclasses.field(default_factory=dict)
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
    directives: List[str] = dataclasses.field(default_factory=list)
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
    redirect_chain: List[Dict[str, object]] = dataclasses.field(
        default_factory=list
    )


class TracingRedirectHandler(urllib.request.HTTPRedirectHandler):
    """
    Capture redirect hops for a single HTTP request.

    A handler instance is created per request, so concurrent requests do not
    share mutable redirect state.
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
    unused reserved bytes are returned to the global budget. This prevents
    parallel workers from each consuming the same remaining allowance.
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
        self.max_bytes_per_request = max(1024, max_bytes_per_request)
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

    def _reserve(self, requested_byte_limit: int) -> int:
        with self._lock:
            if self.requests_used >= self.max_requests:
                event = (
                    f"HTTP request budget reached: "
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

    def _release_reservation(self, allowance: int, actual: int) -> None:
        with self._lock:
            self.bytes_reserved = max(
                0,
                self.bytes_reserved - allowance,
            )
            self.bytes_used += max(0, actual)

            if self.bytes_used > self.max_total_bytes:
                self.bytes_used = self.max_total_bytes

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

        host = urllib.parse.urlsplit(normalized).hostname or ""
        if not self.allow_private and host.lower() in (
            "localhost",
            "localhost.localdomain",
        ):
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
                urllib.request.HTTPSHandler(context=context)
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

            status = int(getattr(response, "status", 200))
            final_url = normalize_url(response.geturl()) or normalized
            headers = {
                str(k).lower(): str(v)
                for k, v in response.headers.items()
            }

            body = response.read(allowance + 1)
            truncated = len(body) > allowance

            if truncated:
                body = body[:allowance]

            actual_bytes = len(body)

            if truncated:
                with self._lock:
                    self.budget_events.append(
                        f"Response truncated at {allowance} bytes: "
                        f"{normalized}"
                    )

            return HttpResult(
                requested_url=normalized,
                final_url=final_url,
                status=status,
                headers=headers,
                body=body,
                elapsed=time.monotonic() - started,
                truncated=truncated,
                redirect_chain=list(redirect_handler.chain),
            )

        except urllib.error.HTTPError as exc:
            headers = {
                str(k).lower(): str(v)
                for k, v in (
                    exc.headers.items() if exc.headers else []
                )
            }

            try:
                body = exc.read(min(allowance, 64 * 1024))
            except Exception:
                body = b""

            actual_bytes = len(body)

            return HttpResult(
                requested_url=normalized,
                final_url=normalize_url(exc.geturl()) or normalized,
                status=int(exc.code or 0),
                headers=headers,
                body=body,
                error=f"http_error:{exc.code}",
                elapsed=time.monotonic() - started,
                redirect_chain=list(redirect_handler.chain),
            )

        except Exception as exc:
            return HttpResult(
                requested_url=normalized,
                final_url=normalized,
                error=f"{type(exc).__name__}: {exc}",
                elapsed=time.monotonic() - started,
                redirect_chain=list(redirect_handler.chain),
            )

        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass

            self._release_reservation(allowance, actual_bytes)


def parse_attributes(line: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not line:
        return result

    pattern = re.compile(
        r'([A-Za-z0-9_-]+)\s*=\s*("([^"]*)"|([^,\s]*))'
    )

    for match in pattern.finditer(line):
        key = match.group(1).lower()
        value = match.group(3)
        if value is None:
            value = match.group(4) or ""
        result[key] = value

    return result


def extinf_display_name(line: str) -> str:
    if "," in line:
        return line.split(",", 1)[1].strip()
    return ""


def parse_m3u_entries(
    text: str,
    base_url: str,
) -> List[M3UEntry]:
    """
    Parse IPTV entries. URLs are accepted even if no EXTINF precedes them,
    allowing plain-text indexes containing playlist URLs.
    """
    entries: List[M3UEntry] = []
    pending_extinf = ""
    pending_directives: List[str] = []

    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("#EXTINF:"):
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
        name = (
            attrs.get("tvg-name")
            or extinf_display_name(pending_extinf)
            or urllib.parse.urlsplit(resolved).hostname
            or resolved
        )

        entries.append(
            M3UEntry(
                url=resolved,
                name=name,
                extinf=pending_extinf,
                directives=list(pending_directives),
                group_title=attrs.get("group-title", ""),
                tvg_id=attrs.get("tvg-id", ""),
                tvg_name=attrs.get("tvg-name", ""),
                tvg_logo=attrs.get("tvg-logo", ""),
                source_playlist=base_url,
            )
        )

        pending_extinf = ""
        pending_directives = []

    return entries


def parse_hls(text: str, base_url: str) -> Dict[str, object]:
    lines = [
        line.strip()
        for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        if line.strip()
    ]

    has_header = any(line.upper() == "#EXTM3U" for line in lines)
    has_stream_inf = any(
        line.upper().startswith("#EXT-X-STREAM-INF:")
        for line in lines
    )

    variants: List[str] = []
    segments: List[str] = []

    pending_stream_inf = False
    pending_hls_extinf = False
    hls_media_tag_seen = False
    numeric_hls_extinf_seen = False

    for line in lines:
        upper = line.upper()

        if any(upper.startswith(tag) for tag in HLS_MEDIA_TAGS):
            hls_media_tag_seen = True

        if upper.startswith("#EXT-X-STREAM-INF:"):
            pending_stream_inf = True
            continue

        if line.startswith("#EXTINF:"):
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
            pending_stream_inf = False
        elif pending_hls_extinf:
            segments.append(resolved)
            pending_hls_extinf = False

    is_master = has_header and has_stream_inf and bool(variants)

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
        "segments": segments,
    }


def extract_playlist_urls(text: str, base_url: str) -> List[str]:
    found: List[str] = []
    seen: Set[str] = set()

    for match in URL_RE.findall(text):
        candidate = clean_url(match)
        if not urllib.parse.urlsplit(candidate).path.lower().endswith(
            PLAYLIST_EXTENSIONS
        ):
            continue

        resolved = resolve_url(base_url, candidate)
        if resolved and resolved not in seen:
            seen.add(resolved)
            found.append(resolved)

    for raw_line in text.splitlines():
        line = clean_url(raw_line.strip())
        if not line or line.startswith("#") or "://" in line:
            continue

        if not urllib.parse.urlsplit(line).path.lower().endswith(
            PLAYLIST_EXTENSIONS
        ):
            continue

        resolved = resolve_url(base_url, line)
        if resolved and resolved not in seen:
            seen.add(resolved)
            found.append(resolved)

    return found


def looks_like_playlist_response(result: HttpResult) -> bool:
    if not result.body:
        return False

    content_type = result.content_type.lower()
    if any(marker in content_type for marker in (
        "mpegurl",
        "m3u",
        "application/vnd.apple",
    )):
        return True

    text = decode_body(result.body).lstrip()

    if text.upper().startswith("#EXTM3U"):
        return True

    if "#EXTINF:" in text[:65536]:
        return True

    return False


def classify_candidate(result: HttpResult) -> Tuple[str, str]:
    if result.error:
        return "http_error", result.error

    if not result.status:
        return "connection_error", "No HTTP status"

    if not 200 <= result.status < 300:
        return "http_error", f"HTTP status {result.status}"

    if not result.body:
        return "empty_response", "Response body is empty"

    if body_looks_like_html(result.body, result.content_type):
        return "html_or_block_page", "HTML response, not a stream"

    return (
        "validated_terminal_stream",
        "HTTP 2xx, non-empty, non-HTML response; decoding not tested",
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

    def log_error(self, url: str, message: str) -> None:
        with self.lock:
            self.errors.append({
                "time": utc_now(),
                "url": url,
                "error": message,
            })

    def log_event(self, message: str) -> None:
        with self.lock:
            self.events.append(message)

    def enqueue(
        self,
        url: str,
        depth: int,
        role: str,
        entry: Optional[M3UEntry] = None,
    ) -> bool:
        normalized = normalize_url(url)
        if not normalized:
            self.log_error(url, "invalid_url")
            return False

        if depth > self.args.max_depth:
            self.stats["depth_limited"] += 1
            self.log_event(f"Depth limit reached: {normalized}")
            return False

        with self.lock:
            if normalized in self.seen or normalized in self.inflight:
                self.stats["duplicate_urls_skipped"] += 1
                return False

            if len(self.seen) + len(self.inflight) >= self.args.max_sources:
                self.stats["source_limited"] += 1
                self.log_event(
                    f"Source limit reached: {self.args.max_sources}"
                )
                return False

            self.inflight.add(normalized)

        self.work_queue.put(
            WorkItem(
                url=normalized,
                depth=depth,
                role=role,
                entry=entry,
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
            redirect_chain=list(result.redirect_chain),
        )

        with self.lock:
            self.records.append(rec)
            self.stats[f"classification:{classification}"] += 1
            self.max_depth_reached = max(
                self.max_depth_reached,
                item.depth,
            )

            if result.redirect_chain:
                self.stats["requests_with_redirects"] += 1
                self.stats["redirect_hops"] += len(result.redirect_chain)

    def output_entry(
        self,
        url: str,
        inherited: Optional[M3UEntry],
    ) -> M3UEntry:
        if inherited:
            return dataclasses.replace(inherited, url=url)

        host = urllib.parse.urlsplit(url).hostname or url
        return M3UEntry(url=url, name=host)

    def emit_validated(
        self,
        url: str,
        inherited: Optional[M3UEntry],
        classification: str,
    ) -> None:
        normalized = normalize_url(url)
        if not normalized:
            return

        entry = self.output_entry(normalized, inherited)

        with self.lock:
            self.validated.append(entry)
            self.stats["validated_terminal_candidates"] += 1

        self.log_event(
            f"Validated candidate [{classification}]: {normalized}"
        )

    def enqueue_entry(self, entry: M3UEntry, depth: int) -> None:
        url = normalize_url(entry.url)
        if not url:
            return

        if url_looks_like_playlist(url):
            self.enqueue(
                url,
                depth=depth,
                role="child_playlist",
                entry=entry,
            )
        else:
            self.enqueue(
                url,
                depth=depth,
                role="channel_candidate",
                entry=entry,
            )

    def handle_playlist(
        self,
        item: WorkItem,
        result: HttpResult,
    ) -> None:
        text = decode_body(result.body)
        base_url = result.final_url or item.url

        with self.lock:
            self.stats["playlists_parsed"] += 1

        hls = parse_hls(text, base_url)

        # HLS master: enqueue every discovered variant by default.
        if hls["is_master"]:
            variants = list(hls["variants"])

            if self.args.hls_variant_policy == "highest":
                variants = variants[-1:] if variants else []
            elif self.args.hls_variant_policy == "lowest":
                variants = variants[:1]

            self.record(
                item,
                result,
                "hls_master_playlist",
                f"variants={len(hls['variants'])}",
            )

            for variant in variants[:self.args.max_children]:
                self.enqueue(
                    variant,
                    depth=item.depth + 1,
                    role="hls_variant",
                    entry=item.entry,
                )

            self.stats["hls_variants_discovered"] += len(variants)
            return

        # HLS media: the media playlist URL is the playback URL.
        if hls["is_media"]:
            segments = list(hls["segments"])

            if self.args.check_hls_segment and segments:
                segment_url = segments[0]
                probe = self.client.get(
                    segment_url,
                    byte_limit=self.args.segment_probe_bytes,
                )

                probe_class, probe_detail = classify_candidate(probe)

                self.record(
                    WorkItem(
                        url=segment_url,
                        depth=item.depth + 1,
                        role="hls_segment_probe",
                        entry=item.entry,
                    ),
                    probe,
                    "hls_segment_probe:" + probe_class,
                    probe_detail,
                )

                if not probe.ok or not probe.body:
                    self.record(
                        item,
                        result,
                        "hls_media_segment_probe_failed",
                        f"{segment_url}: {probe_detail}",
                    )
                    self.log_error(
                        item.url,
                        f"First HLS segment probe failed: {probe_detail}",
                    )
                    return

            self.record(
                item,
                result,
                "hls_media_playlist",
                f"segments={len(segments)}",
            )

            self.emit_validated(
                base_url,
                item.entry,
                "hls_media_playlist",
            )
            return

        # Normal IPTV M3U, including EXTINF:-1.
        entries = parse_m3u_entries(text, base_url)

        if entries:
            self.record(
                item,
                result,
                "m3u_playlist",
                f"entries={len(entries)}",
            )

            self.stats["m3u_entries_discovered"] += len(entries)

            for entry in entries[:self.args.max_children]:
                self.enqueue_entry(entry, item.depth + 1)

            if len(entries) > self.args.max_children:
                self.log_event(
                    f"Child limit for {base_url}: {len(entries)} entries; "
                    f"processing {self.args.max_children}"
                )
            return

        # Index fallback: scan for embedded or relative playlist URLs.
        playlist_urls = extract_playlist_urls(text, base_url)

        if playlist_urls:
            self.record(
                item,
                result,
                "playlist_index",
                f"children={len(playlist_urls)}",
            )

            for child in playlist_urls[:self.args.max_children]:
                self.enqueue(
                    child,
                    depth=item.depth + 1,
                    role="child_playlist",
                    entry=item.entry,
                )

            self.stats["playlist_index_children"] += len(playlist_urls)
            return

        self.record(
            item,
            result,
            "playlist_empty_or_unrecognized",
            "No entries, HLS variants, segments or child playlist URLs",
        )
        self.log_error(
            item.url,
            "Playlist downloaded but no recognized entries were found",
        )

    def process_item(self, item: WorkItem) -> None:
        if time.monotonic() - self.started > self.args.max_runtime:
            self.runtime_exceeded = True
            self.log_event("Runtime limit reached")
            return

        result = self.client.get(
            item.url,
            byte_limit=self.args.max_bytes,
        )

        if result.error:
            self.record(item, result, "request_failed", result.error)
            self.log_error(item.url, result.error)
            return

        if not result.ok:
            self.record(
                item,
                result,
                "http_error",
                f"HTTP status {result.status}",
            )
            self.log_error(item.url, f"http_status_{result.status}")
            return

        if not result.body:
            self.record(item, result, "empty_response", "Empty body")
            self.log_error(item.url, "empty_response")
            return

        if body_looks_like_html(result.body, result.content_type):
            self.record(
                item,
                result,
                "html_or_block_page",
                "HTML response, login page or proxy challenge",
            )
            self.log_error(item.url, "html_or_block_page")
            return

        if looks_like_playlist_response(result):
            self.handle_playlist(item, result)
            return

        # Non-playlist candidate: it is only a preliminary HTTP-level check.
        classification, detail = classify_candidate(result)
        self.record(item, result, classification, detail)

        if classification == "validated_terminal_stream":
            final_url = result.final_url or item.url
            self.emit_validated(
                final_url,
                item.entry,
                classification,
            )
        else:
            self.log_error(item.url, detail)

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
                        f"unexpected_{type(exc).__name__}: {exc}",
                    )
                finally:
                    self.mark_finished(item.url)

            finally:
                self.work_queue.task_done()

    def run(self, sources: List[str]) -> None:
        self.root_urls = list(dict.fromkeys(
            normalize_url(url)
            for url in sources
            if normalize_url(url)
        ))

        for url in self.root_urls:
            self.enqueue(
                url,
                depth=0,
                role="root_playlist",
            )

        worker_count = max(1, self.args.workers)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=worker_count
        ) as executor:
            futures = [
                executor.submit(self.worker)
                for _ in range(worker_count)
            ]

            # Queue.join waits for every task, including children enqueued
            # by workers while they process their parents.
            self.work_queue.join()

            for _ in range(worker_count):
                self.work_queue.put(self.stop_token)

            self.work_queue.join()

            for future in futures:
                future.result()

        self.deduplicate_validated()

    def deduplicate_validated(self) -> None:
        def richness(entry: M3UEntry) -> int:
            return sum(bool(x) for x in (
                entry.name,
                entry.extinf,
                entry.group_title,
                entry.tvg_id,
                entry.tvg_name,
                entry.tvg_logo,
            ))

        by_url: Dict[str, M3UEntry] = {}

        for entry in self.validated:
            url = normalize_url(entry.url)
            if not url:
                continue

            current = by_url.get(url)
            if current is None or richness(entry) > richness(current):
                by_url[url] = entry

        self.validated = list(by_url.values())
        self.stats["validated_unique_urls"] = len(self.validated)

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
            "http_requests_used": self.client.requests_used,
            "http_requests_limit": self.client.max_requests,
            "bytes_read": self.client.bytes_used,
            "bytes_limit": self.client.max_total_bytes,
            "runtime_limit_seconds": self.args.max_runtime,
            "validated_terminal_candidates": len(self.validated),
            "errors": len(self.errors),
            "stats": dict(self.stats),
            "runtime_exceeded": self.runtime_exceeded,
            "budget_events": list(self.client.budget_events),
            "events": list(self.events),
        }

    def write_outputs(self) -> None:
        os.makedirs(self.args.output_dir, exist_ok=True)

        summary = self.summary()

        report_json = {
            "summary": summary,
            "records": [dataclasses.asdict(r) for r in self.records],
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

        json_path = os.path.join(self.args.output_dir, "report.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                report_json,
                f,
                ensure_ascii=False,
                indent=2,
            )

        txt_path = os.path.join(self.args.output_dir, "report.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(f"Phoenix URL Dissector {VERSION}\n")
            f.write("=" * 76 + "\n")
            f.write(f"Created: {self.created}\n")
            f.write(f"Finished: {utc_now()}\n\n")

            f.write("ROOT SOURCES\n")
            f.write("-" * 76 + "\n")
            for url in self.root_urls:
                f.write(f"- {url}\n")

            f.write("\nSUMMARY\n")
            f.write("-" * 76 + "\n")
            f.write(f"URLs seen: {summary['urls_seen']}\n")
            f.write(f"Root URLs: {summary['root_url_count']}\n")
            f.write(f"HTTP records: {len(self.records)}\n")
            f.write(
                "Validated terminal candidates: "
                f"{len(self.validated)}\n"
            )
            f.write(f"Errors: {len(self.errors)}\n")
            f.write(
                f"Elapsed: {summary['elapsed_seconds']} seconds\n"
            )

            f.write("\nHTTP BUDGET\n")
            f.write("-" * 76 + "\n")
            f.write(
                f"Requests: {self.client.requests_used}/"
                f"{self.client.max_requests}\n"
            )
            f.write(
                f"Bytes: {self.client.bytes_used}/"
                f"{self.client.max_total_bytes}\n"
            )

            f.write("\nCLASSIFICATIONS\n")
            f.write("-" * 76 + "\n")
            counts = Counter(r.classification for r in self.records)
            for key, value in sorted(counts.items()):
                f.write(f"{key}: {value}\n")

            f.write("\nCOUNTERS\n")
            f.write("-" * 76 + "\n")
            for key, value in sorted(self.stats.items()):
                f.write(f"{key}: {value}\n")

            f.write("\nVALIDATED TERMINAL CANDIDATES\n")
            f.write("-" * 76 + "\n")
            if not self.validated:
                f.write("No validated terminal candidates.\n")
            else:
                for i, entry in enumerate(self.validated, 1):
                    f.write(f"{i}. {entry.url}\n")
                    f.write(f"   Name: {entry.name}\n")
                    if entry.group_title:
                        f.write(f"   Group: {entry.group_title}\n")
                    if entry.tvg_id:
                        f.write(f"   tvg-id: {entry.tvg_id}\n")
                    if entry.source_playlist:
                        f.write(
                            f"   Source playlist: {entry.source_playlist}\n"
                        )

            f.write("\nREDIRECT CHAINS\n")
            f.write("-" * 76 + "\n")
            found_redirects = False
            for record in self.records:
                for hop_num, hop in enumerate(record.redirect_chain, 1):
                    found_redirects = True
                    f.write(
                        f"{record.url} [hop {hop_num}]\n"
                        f"  HTTP {hop.get('status')}: "
                        f"{hop.get('from')}\n"
                        f"  -> {hop.get('to')}\n"
                    )
            if not found_redirects:
                f.write("No redirects observed.\n")

            f.write("\nERRORS\n")
            f.write("-" * 76 + "\n")
            if not self.errors:
                f.write("No errors recorded.\n")
            else:
                for error in self.errors:
                    f.write(
                        f"- {error['url']}: {error['error']}\n"
                    )

            f.write("\nBUDGET EVENTS\n")
            f.write("-" * 76 + "\n")
            if not self.client.budget_events:
                f.write("No budget limits reached.\n")
            else:
                for event in self.client.budget_events:
                    f.write(f"- {event}\n")

            f.write("\nDETAILED RECORDS\n")
            f.write("-" * 76 + "\n")
            for record in self.records:
                f.write(
                    f"[{record.classification}] "
                    f"HTTP {record.status} "
                    f"depth={record.depth} role={record.role}\n"
                )
                f.write(f"  URL: {record.url}\n")
                f.write(f"  Final URL: {record.final_url}\n")
                if record.content_type:
                    f.write(f"  Content-Type: {record.content_type}\n")
                f.write(
                    f"  Bytes: {record.bytes_read}; "
                    f"Elapsed: {record.elapsed}s\n"
                )
                if record.detail:
                    f.write(f"  Detail: {record.detail}\n")

            f.write("\nLIMITATIONS\n")
            f.write("-" * 76 + "\n")
            f.write(
                "HTTP success is not equivalent to successful video decoding.\n"
            )
            f.write(
                "Direct candidates are checked at HTTP level only. HLS "
                "media playlists are checked structurally.\n"
            )
            f.write(
                "When enabled, HLS segment probing checks only the first "
                "referenced segment.\n"
            )

        m3u_path = os.path.join(
            self.args.output_dir,
            "validated_streams.m3u",
        )

        with open(m3u_path, "w", encoding="utf-8", newline="\n") as f:
            f.write("#EXTM3U\n")

            for entry in self.validated:
                if entry.extinf.startswith("#EXTINF:"):
                    f.write(entry.extinf + "\n")
                else:
                    f.write(
                        f"#EXTINF:-1,{entry.name or entry.url}\n"
                    )

                for directive in entry.directives:
                    if directive.startswith("#EXTINF:"):
                        continue
                    if directive.upper() == "#EXTM3U":
                        continue
                    f.write(directive + "\n")

                f.write(entry.url + "\n")

        print(f"Report: {txt_path}")
        print(f"JSON:   {json_path}")
        print(f"M3U:    {m3u_path}")


def self_test() -> int:
    failures: List[str] = []

    def check(condition: bool, name: str) -> None:
        if not condition:
            failures.append(name)

    regular_m3u = """#EXTM3U
#EXTINF:-1 tvg-id="news.one" tvg-name="News One" group-title="News",News One HD
https://example.invalid/news/index.m3u8
#EXTINF:-1 group-title="Movies",Movie Channel
https://example.invalid/movie/stream
"""

    entries = parse_m3u_entries(
        regular_m3u,
        "https://example.invalid/list.m3u",
    )
    parsed_iptv_as_hls = parse_hls(
        regular_m3u,
        "https://example.invalid/list.m3u",
    )

    check(len(entries) == 2, "iptv_entries")
    check(entries[0].tvg_id == "news.one", "tvg_id")
    check(entries[0].group_title == "News", "group_title")
    check(not parsed_iptv_as_hls["is_media"], "iptv_not_hls_media")

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

    check(master["is_master"], "hls_master")
    check(len(master["variants"]) == 2, "hls_variants")

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

    check(media["is_media"], "hls_media")
    check(len(media["segments"]) == 2, "hls_segments")

    check(
        normalize_url("HTTPS://Example.COM/a.m3u8?token=1")
        == "https://example.com/a.m3u8?token=1",
        "normalize_url",
    )

    check(
        resolve_url(
            "https://example.invalid/a/index.m3u",
            "../b/list.m3u8",
        ) == "https://example.invalid/b/list.m3u8",
        "relative_url",
    )

    if failures:
        print("SELF-TEST FAILED")
        for failure in failures:
            print(f"  FAIL: {failure}")
        return 1

    print(f"Phoenix URL Dissector {VERSION}")
    print("SELF-TEST PASSED (8 checks)")
    return 0


def read_sources(args) -> List[str]:
    sources: List[str] = []

    if args.url:
        sources.extend(args.url)

    if args.input_file:
        with open(args.input_file, "r", encoding="utf-8-sig") as f:
            for line in f:
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
            f"Phoenix URL Dissector {VERSION}: recursive M3U/HLS "
            "discovery and redirect tracing"
        )
    )

    parser.add_argument("--url", action="append")
    parser.add_argument("--input-file")
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--discover-livem3u", action="store_true")

    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=2 * 1024 * 1024,
    )
    parser.add_argument(
        "--max-total-bytes",
        type=int,
        default=20 * 1024 * 1024,
    )
    parser.add_argument("--max-runtime", type=float, default=300.0)
    parser.add_argument("--max-requests", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=12)
    parser.add_argument("--max-sources", type=int, default=2000)
    parser.add_argument("--max-children", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--segment-probe-bytes",
        type=int,
        default=64 * 1024,
    )
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)

    parser.add_argument(
        "--hls-variant-policy",
        choices=("all", "highest", "lowest"),
        default="all",
    )
    parser.add_argument(
        "--check-hls-segment",
        action="store_true",
    )
    parser.add_argument(
        "--allow-private",
        action="store_true",
    )
    parser.add_argument(
        "--insecure-tls",
        action="store_true",
    )
    parser.add_argument(
        "--output-dir",
        default="phoenix_url_dissector_output",
    )
    parser.add_argument("--self-test", action="store_true")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    if args.max_runtime <= 0:
        parser.error("--max-runtime must be positive")

    if args.max_requests < 1:
        parser.error("--max-requests must be at least 1")

    if args.max_total_bytes < 1024:
        parser.error("--max-total-bytes must be at least 1024")

    sources = read_sources(args)
    if not sources:
        print("No valid HTTP/HTTPS source URLs.", file=sys.stderr)
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
        f"workers={args.workers}"
    )
    print()

    dissector = Dissector(args)
    dissector.run(sources)
    dissector.write_outputs()

    summary = dissector.summary()

    print()
    print("SUMMARY")
    print("-" * 76)
    print(f"URLs seen: {summary['urls_seen']}")
    print(f"HTTP requests: {summary['http_requests_used']}")
    print(f"Bytes read: {summary['bytes_read']}")
    print(f"Redirect hops: {dissector.stats['redirect_hops']}")
    print(
        "Validated terminal candidates: "
        f"{summary['validated_terminal_candidates']}"
    )
    print(f"Errors: {summary['errors']}")

    if not dissector.validated:
        print(
            "\nWARNING: no terminal candidates were validated. "
            "Review report.txt and report.json."
        )

    return 0 if not dissector.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
