# -*- coding: utf-8 -*-
"""
Fetching utilities:
- RSS/Atom ingestion
- HTML index page link extraction
- Full-text extraction (HTML/PDF) with robust fallbacks

This file is intentionally defensive:
- network failures return empty results rather than raising
- key entry points accept **kwargs for forward compatibility
"""
from __future__ import annotations

import hashlib
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup

# These deps are installed in your GH Action (per requirements.txt)
import feedparser
import trafilatura
from trafilatura import metadata as trafi_metadata

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None


# -----------------------------
# Config
# -----------------------------
UA = os.getenv(
    "HTTP_UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)

CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "25"))
MAX_BYTES = int(os.getenv("HTTP_MAX_BYTES", str(2_000_000)))  # 2MB safety cap for HTML
MAX_PDF_BYTES = int(os.getenv("HTTP_MAX_PDF_BYTES", str(6_000_000)))  # 6MB cap for PDFs
RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
BACKOFF = float(os.getenv("HTTP_BACKOFF", "1.4"))

MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "60"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "1"))

# If you later add date-resolution-by-fetch logic, this cap prevents runaway time.
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "8"))


# -----------------------------
# Data model
# -----------------------------
@dataclass
class Item:
    url: str
    title: str
    summary: str = ""
    source: str = ""  # publisher/site name (not section)
    section: str = ""  # logical digest section (Energy Transition, etc.)

    # date signals
    published_iso: Optional[str] = None
    published_ts: Optional[float] = None
    published_source: Optional[str] = None
    published_confidence: Optional[float] = None

    index_url: Optional[str] = None


# -----------------------------
# Helpers
# -----------------------------
def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close",
    }
    if extra:
        h.update(extra)
    return h


def _timeout():
    return (CONNECT_TIMEOUT, READ_TIMEOUT)


def _clean_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    u, _frag = urldefrag(u)
    return u


def is_probably_taxonomy_or_hub(url: str) -> bool:
    u = (url or "").lower()
    bad_parts = [
        "/tag/", "/tags/", "/category/", "/categories/", "/topic/", "/topics/",
        "/author/", "/authors/",
        "/search", "?s=", "/page/", "/index",
        "/events", "/event", "/webinars", "/webinar",
        "/newsroom", "/media-centre", "/media-center", "/press",
    ]
    return any(p in u for p in bad_parts)


def _http_get(url: str) -> Optional[requests.Response]:
    if not url:
        return None

    last_err: Optional[Exception] = None
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(
                url,
                headers=_headers(),
                timeout=_timeout(),
                allow_redirects=True,
                stream=True,
            )
            # don't raise: treat non-200 as recoverable upstream
            return r
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep((BACKOFF ** attempt))
                continue
            return None
    return None


def _read_limited(resp: requests.Response, cap: int) -> bytes:
    if resp is None:
        return b""
    out = bytearray()
    try:
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            out.extend(chunk)
            if len(out) >= cap:
                break
    except Exception:
        return bytes(out)
    return bytes(out)


def _content_type(resp: Optional[requests.Response]) -> str:
    if resp is None:
        return ""
    return (resp.headers.get("Content-Type") or "").lower()


def _parse_epoch_from_struct(tstruct: Any) -> Optional[float]:
    try:
        return time.mktime(tstruct)
    except Exception:
        return None


def _parse_datetime_like(s: str) -> Optional[datetime]:
    if not s:
        return None
    # dateutil is not required here; trafilatura/htmldate already tries.
    # Use a light ISO attempt.
    try:
        # common RSS formats are handled by feedparser, so keep simple here
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# -----------------------------
# Public API
# -----------------------------
def fetch_rss(feed_url: str, source_name: str = "", **kwargs) -> List[Item]:
    feed_url = _clean_url(feed_url)
    if not feed_url:
        return []

    resp = _http_get(feed_url)
    if resp is None:
        return []

    raw = _read_limited(resp, MAX_BYTES)
    if not raw:
        return []

    parsed = feedparser.parse(raw)
    items: List[Item] = []

    for e in parsed.entries or []:
        link = _clean_url(getattr(e, "link", "") or "")
        title = (getattr(e, "title", "") or "").strip()
        if not link or not title:
            continue

        summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "").strip()

        published_ts = None
        published_iso = None

        if getattr(e, "published_parsed", None):
            published_ts = _parse_epoch_from_struct(e.published_parsed)
        if getattr(e, "updated_parsed", None) and published_ts is None:
            published_ts = _parse_epoch_from_struct(e.updated_parsed)

        if published_ts is not None:
            published_iso = datetime.fromtimestamp(published_ts, tz=timezone.utc).isoformat()

        it = Item(
            url=link,
            title=title,
            summary=summary,
            source=(source_name or (getattr(parsed.feed, "title", "") or "")).strip(),
            published_iso=published_iso,
            published_ts=published_ts,
            published_source="rss",
            published_confidence=0.9 if published_ts else None,
            index_url=feed_url,
        )
        items.append(it)

    return items


def fetch_html_index(index_url: str, source_name: str = "", **kwargs) -> List[Item]:
    """
    Extract candidate article links from an index/listing page.
    Lightweight (no per-link fetch). Titles come from the anchor text associated with each href.
    """
    index_url = _clean_url(index_url)
    if not index_url:
        return []

    resp = _http_get(index_url)
    if resp is None:
        return []

    raw = _read_limited(resp, MAX_BYTES)
    if not raw:
        return []

    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    soup = BeautifulSoup(html, "html.parser")

    # Common junk anchor texts
    junk_title_rx = re.compile(
        r"^(skip to (main )?content|skip navigation|menu|home)$|"
        r"(cookie|privacy|terms|subscribe|sign\s?up|login|log in|register|careers|jobs|sitemap|accessibility)",
        re.I,
    )

    url_to_title: Dict[str, str] = {}

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith(("mailto:", "javascript:", "tel:")):
            continue

        abs_url = _clean_url(urljoin(index_url, href))
        if not abs_url:
            continue
        if urlparse(abs_url).scheme not in ("http", "https"):
            continue

        # Avoid self-links / same-page fragments
        if abs_url.lower() == index_url.lower():
            continue

        text = " ".join(a.get_text(" ", strip=True).split())
        if not text or len(text) < 12:
            continue
        if junk_title_rx.search(text):
            continue

        # Keep the longest (usually most descriptive) anchor text for a URL
        prev = url_to_title.get(abs_url)
        if (prev is None) or (len(text) > len(prev)):
            url_to_title[abs_url] = text

        if len(url_to_title) >= MAX_LINKS_PER_INDEX:
            break

    items: List[Item] = []
    for u, t in url_to_title.items():
        items.append(
            Item(
                url=u,
                title=t,
                summary="",
                # Prefer the target URL’s domain as publisher
                source=(urlparse(u).netloc or source_name or urlparse(index_url).netloc),
                index_url=index_url,
            )
        )

    return items

from dateutil import parser as dtparser
import json as _json

def fetch_full_text(url: str, return_meta: bool = False, **kwargs):
    """
    Return extracted text for a URL (HTML or PDF).
    Returns empty string on failure.
    """
    url = _clean_url(url)
    if not url:
        return ""

    resp = _http_get(url)
    if resp is None:
        return ""

    ctype = _content_type(resp)
    is_pdf = ("application/pdf" in ctype) or url.lower().endswith(".pdf")

    cap = MAX_PDF_BYTES if is_pdf else MAX_BYTES
    raw = _read_limited(resp, cap)
    if not raw:
        return ""

        if is_pdf:
        text = ...
        return (text, {}) if return_meta else text

    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    meta = {}
    text = ""

    # Prefer JSON output to capture title/date when possible
    try:
        j = trafilatura.extract(html, output_format="json", include_images=False, include_comments=False)
        if j:
            obj = _json.loads(j)
            text = (obj.get("text") or "").strip()
            meta["title"] = (obj.get("title") or "").strip() or None
            date_s = (obj.get("date") or obj.get("date_published") or "").strip()
            if date_s:
                try:
                    dt = dtparser.parse(date_s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    dt = dt.astimezone(timezone.utc)
                    meta["published_iso"] = dt.isoformat()
                    meta["published_ts"] = dt.timestamp()
                    meta["published_source"] = "trafilatura"
                except Exception:
                    pass
    except Exception:
        pass

    if not text:
        try:
            text = (trafilatura.extract(html, include_images=False, include_comments=False) or "").strip()
        except Exception:
            text = ""

    # Fallback metadata extractor
    if (not meta.get("title") or not meta.get("published_ts")):
        try:
            md = trafi_metadata.extract_metadata(html, default_url=url)
            if md:
                if not meta.get("title") and getattr(md, "title", None):
                    meta["title"] = md.title
                if not meta.get("published_ts") and getattr(md, "date", None):
                    try:
                        dt = dtparser.parse(md.date)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        dt = dt.astimezone(timezone.utc)
                        meta["published_iso"] = dt.isoformat()
                        meta["published_ts"] = dt.timestamp()
                        meta["published_source"] = "metadata"
                    except Exception:
                        pass
        except Exception:
            pass

    return (text, meta) if return_meta else text
