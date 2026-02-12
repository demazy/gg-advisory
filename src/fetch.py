# -*- coding: utf-8 -*-
"""
Fetching utilities:
- RSS/Atom ingestion
- HTML index page link extraction
- Full-text extraction (HTML/PDF) with robust fallbacks

Design goals:
- defensive (network failures return empty results rather than raising)
- bounded runtime (caps on bytes, links, and optional per-link metadata)
- stable API (key entry points accept **kwargs for forward compatibility)

Incremental improvements (Feb 2026):
- Fix HTML index title extraction bug (each candidate URL now keeps its own anchor text).
- Reduce garbage candidates (navigation/auth/utility/social/tracking URLs filtered early).
- Optionally restrict HTML index extraction to same-site links by default.
- Infer publish month from common URL patterns (YYYY/MM[/DD], YYYY/<monthname>/) to improve date filtering downstream.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urldefrag, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

# These deps are installed in your GH Action (per requirements.txt)
import feedparser

try:
    import trafilatura
except Exception:  # pragma: no cover
    trafilatura = None

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
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "1"))  # reserved for future pagination
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "0"))

# By default, only keep links on the same site as the index page.
# Set to 1 if you intentionally want external links pulled from index pages.
ALLOW_EXTERNAL_LINKS_FROM_INDEX = os.getenv("ALLOW_EXTERNAL_LINKS_FROM_INDEX", "0") == "1"


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


def _norm_host(netloc: str) -> str:
    n = (netloc or "").strip().lower()
    if n.startswith("www."):
        n = n[4:]
    return n


def _same_site(a: str, b: str) -> bool:
    """True if URLs are on same registrable host or subdomain (best-effort)."""
    ha = _norm_host(urlparse(a).netloc)
    hb = _norm_host(urlparse(b).netloc)
    if not ha or not hb:
        return False
    return ha == hb or ha.endswith("." + hb) or hb.endswith("." + ha)


# Common non-content patterns that should never become digest items
_DENY_URL_SUBSTRINGS = [
    "oauth-redirect", "j_security_check", "login", "signin", "sign-in", "sign_in",
    "account", "subscribe", "newsletter", "cart", "checkout", "/shop", "store.",
    "policies.google.", "safelinks.protection.outlook.com",
    "facebook.com", "twitter.com", "x.com", "linkedin.com", "youtube.com", "instagram.com", "tiktok.com",
    "open.spotify.com", "spotify.com", "mailto:",
]

# Static/asset extensions that are very unlikely to be digest items
_DENY_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".css", ".js", ".json", ".xml", ".ico",
    ".mp4", ".mp3", ".wav",
    ".zip", ".gz", ".tar", ".tgz",
)


_MONTHS = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}


def infer_published_ts_from_url(url: str) -> Optional[float]:
    """
    Best-effort inference of publish timestamp from URL path.
    Used to improve month-based filtering when RSS dates are missing.

    We intentionally keep this conservative (month-level at best).
    """
    u = (url or "").strip()
    if not u:
        return None

    path = urlparse(u).path.lower()

    # YYYY-MM-DD
    m = re.search(r"/(20\d{2})-(\d{2})-(\d{2})(?:/|$)", path)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()
        except Exception:
            return None

    # /YYYY/MM/DD/
    m = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)", path)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()
        except Exception:
            return None

    # /YYYY/<monthname>/
    m = re.search(r"/(20\d{2})/([a-z]{3,9})(?:/|$)", path)
    if m:
        y = int(m.group(1))
        mo = _MONTHS.get(m.group(2).lower())
        if mo:
            try:
                return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()
            except Exception:
                return None

    # /YYYY/MM/
    m = re.search(r"/(20\d{2})/(\d{1,2})(?:/|$)", path)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            try:
                return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()
            except Exception:
                return None

    return None


def is_probably_taxonomy_or_hub(url: str) -> bool:
    """
    Heuristic: return True for URLs that are unlikely to be *content items* (listing pages,
    taxonomy pages, nav/utility pages, auth flows).

    NOTE: This function is used both during index extraction and during selection filtering,
    so keep it focused on obvious non-content URLs.
    """
    u = (url or "").strip()
    if not u:
        return True
    ul = u.lower()
    parsed = urlparse(ul)

    # obvious auth/redirect/tracking flows
    if any(s in ul for s in ("oauth-redirect", "j_security_check", "sso", "signin", "login")):
        return True

    # query-based searches / pagination
    q = parse_qs(parsed.query or "")
    if "page" in q and (parsed.path.endswith("/news") or parsed.path.endswith("/news/")):
        return True
    if "s" in q or "q" in q and parsed.path.endswith("/search"):
        return True

    path = parsed.path or "/"
    # nav/utility endpoints
    utility_segments = {
        "about", "contact", "privacy", "terms", "cookies", "accessibility", "sitemap",
        "careers", "jobs", "vacancies", "pressroom", "newsroom",
        "events", "event", "webinars", "webinar",
        "tag", "tags", "category", "categories", "topic", "topics",
        "author", "authors",
        "help", "support", "faq",
    }
    segs = [s for s in path.split("/") if s]
    if segs and segs[-1] in utility_segments:
        return True
    # taxonomy/listing patterns anywhere in path
    bad_parts = [
        "/tag/", "/tags/", "/category/", "/categories/", "/topic/", "/topics/",
        "/author/", "/authors/",
        "/search", "?s=", "/page/", "/index",
        "/events", "/event", "/webinars", "/webinar",
    ]
    if any(p in ul for p in bad_parts):
        return True

    # file/asset endpoints
    if any(ul.endswith(ext) for ext in _DENY_EXTENSIONS):
        return True

    # social/tracking domains
    if any(s in ul for s in _DENY_URL_SUBSTRINGS):
        return True

    return False


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
            # Basic status filtering
            if r.status_code >= 400:
                r.close()
                last_err = Exception(f"HTTP {r.status_code}")
                if attempt < RETRIES:
                    time.sleep(BACKOFF ** attempt)
                    continue
                return None
            return r
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF ** attempt)
                continue
    return None


def _read_limited(resp: requests.Response, cap: int) -> bytes:
    try:
        buf = bytearray()
        for chunk in resp.iter_content(chunk_size=64_000):
            if not chunk:
                continue
            buf.extend(chunk)
            if len(buf) >= cap:
                break
        return bytes(buf)
    except Exception:
        return b""
    finally:
        try:
            resp.close()
        except Exception:
            pass


def _content_type(resp: requests.Response) -> str:
    try:
        return (resp.headers.get("Content-Type") or "").lower()
    except Exception:
        return ""


def _parse_epoch_from_struct(st: Any) -> Optional[float]:
    try:
        # feedparser uses time.struct_time
        return time.mktime(st)
    except Exception:
        return None


def _clean_anchor_text(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t).strip()
    # very common boilerplate link labels
    if t.lower() in {"skip to content", "skip to main content", "read more", "learn more", "more"}:
        return ""
    return t


def _looks_like_asset_url(u: str) -> bool:
    ul = (u or "").lower()
    return any(ul.endswith(ext) for ext in _DENY_EXTENSIONS)


def _deny_from_index(u: str) -> bool:
    ul = (u or "").lower()
    if any(s in ul for s in _DENY_URL_SUBSTRINGS):
        return True
    if _looks_like_asset_url(ul):
        return True
    return False


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
        if _deny_from_index(link) or is_probably_taxonomy_or_hub(link):
            continue

        summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "").strip()

        published_ts = None
        published_iso = None

        if getattr(e, "published_parsed", None):
            published_ts = _parse_epoch_from_struct(e.published_parsed)
        if getattr(e, "updated_parsed", None) and published_ts is None:
            published_ts = _parse_epoch_from_struct(e.updated_parsed)

        if published_ts is None:
            # lightweight fallback: infer month from URL if possible
            published_ts = infer_published_ts_from_url(link)
            if published_ts is not None:
                published_iso = datetime.fromtimestamp(published_ts, tz=timezone.utc).isoformat()

        if published_ts is not None and published_iso is None:
            published_iso = datetime.fromtimestamp(published_ts, tz=timezone.utc).isoformat()

        it = Item(
            url=link,
            title=title,
            summary=summary,
            source=(source_name or (getattr(parsed.feed, "title", "") or "")).strip(),
            published_iso=published_iso,
            published_ts=published_ts,
            published_source="rss" if getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None) else ("url" if published_ts else None),
            published_confidence=0.9 if getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None) else (0.4 if published_ts else None),
            index_url=feed_url,
        )
        items.append(it)

    return items


def fetch_html_index(index_url: str, source_name: str = "", **kwargs) -> List[Item]:
    """
    Extract candidate content links from an index/listing page.

    NOTE: This function should be conservative: it is better to return fewer, higher-signal
    candidates than hundreds of navigation links.
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

    # crude decode
    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    soup = BeautifulSoup(html, "html.parser")

    # Collect (url -> best_title) from anchors
    title_by_url: Dict[str, str] = {}
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
        if urlparse(abs_url).scheme not in ("http", "https"):
            continue

        # Filter obvious non-content
        if _deny_from_index(abs_url) or is_probably_taxonomy_or_hub(abs_url):
            continue

        if (not ALLOW_EXTERNAL_LINKS_FROM_INDEX) and (not _same_site(abs_url, index_url)):
            continue

        t = _clean_anchor_text(a.get_text(" ", strip=True) or "")
        # Keep best (longest) anchor text seen for a URL
        if abs_url not in title_by_url or len(t) > len(title_by_url.get(abs_url, "")):
            if t:
                title_by_url[abs_url] = t

        links.append(abs_url)

    # De-dupe, keep order
    seen = set()
    uniq: List[str] = []
    for u in links:
        key = u.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(u)

    uniq = uniq[:MAX_LINKS_PER_INDEX]

    items: List[Item] = []
    for u in uniq:
        inferred_ts = infer_published_ts_from_url(u)
        inferred_iso = datetime.fromtimestamp(inferred_ts, tz=timezone.utc).isoformat() if inferred_ts else None

        # Source: prefer explicit source_name for same-site links, else fall back to the URL's host.
        src = source_name or _norm_host(urlparse(index_url).netloc)
        if not _same_site(u, index_url):
            src = _norm_host(urlparse(u).netloc)

        title = title_by_url.get(u, "") or u

        items.append(
            Item(
                url=u,
                title=title,
                summary="",
                source=src,
                published_iso=inferred_iso,
                published_ts=inferred_ts,
                published_source="url" if inferred_ts else None,
                published_confidence=0.35 if inferred_ts else None,
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

    # Hard stop for obvious non-content URLs (prevents wasting fetch budget downstream)
    if _deny_from_index(url) or is_probably_taxonomy_or_hub(url):
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

    # trafilatura first (best signal) – optional dependency in some environments
    if trafilatura is not None:
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
