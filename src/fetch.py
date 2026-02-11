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
    We intentionally avoid heavy per-link fetches here to keep runtime bounded.
    """
    index_url = _clean_url(index_url)
    if not index_url:
        return []

    resp = _http_get(index_url)
    if resp is None:
        return []

    ctype = _content_type(resp)
    raw = _read_limited(resp, MAX_BYTES)
    if not raw:
        return []

    # crude decode
    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    soup = BeautifulSoup(html, "html.parser")

    links: List[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        abs_url = _clean_url(urljoin(index_url, href))
        if not abs_url:
            continue
        # keep only http(s)
        if urlparse(abs_url).scheme not in ("http", "https"):
            continue
        if is_probably_taxonomy_or_hub(abs_url):
            continue
        links.append(abs_url)

    # de-dupe, keep order
    seen = set()
    uniq = []
    for u in links:
        key = u.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(u)

    uniq = uniq[:MAX_LINKS_PER_INDEX]

    items: List[Item] = []
    for u in uniq:
        # title: best-effort from anchor text, else empty (filtered later)
        t = ""
        try:
            # pick first matching anchor
            a = soup.find("a", href=True, string=True)
            if a and a.string:
                t = a.string.strip()
        except Exception:
            t = ""

        items.append(
            Item(
                url=u,
                title=(t or u),
                summary="",
                source=(source_name or urlparse(index_url).netloc),
                index_url=index_url,
            )
        )

    return items


def fetch_full_text(url: str, **kwargs) -> str:
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
        if fitz is None:
            return ""
        try:
            doc = fitz.open(stream=raw, filetype="pdf")
            parts = []
            for i in range(min(doc.page_count, 25)):
                parts.append(doc.load_page(i).get_text("text"))
            doc.close()
            text = "\n".join(parts).strip()
            return text
        except Exception:
            return ""

    # HTML extraction
    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    # trafilatura first (best signal)
    try:
        extracted = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            favor_recall=True,
        )
        if extracted and extracted.strip():
            return extracted.strip()
    except Exception:
        pass

    # fallback: soup text
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        txt = soup.get_text("\n")
        txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
        return txt
    except Exception:
        return ""
