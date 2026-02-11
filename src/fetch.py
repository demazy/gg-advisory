#!/usr/bin/env python3
"""
fetch.py

Network + extraction utilities for GG Advisory monthly digest.

This version is intentionally "robust-first" for GitHub Actions:
- retries transient failures (timeouts, 5xx, 429, some CDN errors)
- uses browser-ish headers to reduce 403s
- bounds downloads to avoid huge payloads
- improves HTML index link extraction to reduce noise
- improves published date resolution from HTML (meta, <time>, JSON-LD)
- adds optional PDF text extraction (PyMuPDF) while keeping API compatible

IMPORTANT COMPATIBILITY NOTES
- Keeps Item model fields, especially `published_ts` (not `published`).
- fetch_rss() returns List[Item]
- fetch_html_index() returns List[Item]
- fetch_full_text(url) returns str ONLY (as before)
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Set, Dict
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from trafilatura import extract as trafi_extract

# Optional PDF extraction (PyMuPDF)
try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover
    fitz = None


# ----------------------------
# Public data model (KEEP)
# ----------------------------

@dataclass
class Item:
    url: str
    title: str = ""
    summary: str = ""
    published_ts: Optional[datetime] = None
    source: str = ""
    section: str = ""
    domain: str = ""
    fetched_text: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["published_ts"] = self.published_ts.isoformat() if self.published_ts else None
        return d


# ----------------------------
# HTTP plumbing (enhanced, compatible)
# ----------------------------

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9,fr-FR;q=0.8,fr;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# Avoid advertising brotli unless you know requests can decode it (depends on brotli install)
DEFAULT_HEADERS["Accept-Encoding"] = "gzip, deflate"

RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504, 522, 524}
DEFAULT_TIMEOUT = (8.0, 35.0)  # connect, read

_SAFARI_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.2 Safari/605.1.15"
)


def _sess() -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    return s


def _http_get(
    url: str,
    *,
    timeout: Tuple[float, float] = DEFAULT_TIMEOUT,
    max_retries: int = 2,
    backoff_s: float = 1.0,
    allow_redirects: bool = True,
    stream: bool = False,
    max_bytes: Optional[int] = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> requests.Response:
    """
    Robust GET:
    - retries transient failures/timeouts
    - uses browser-ish headers to reduce 403s
    - on 403, retries once with Safari UA (common WAF heuristic)
    - optional max_bytes to bound download size
    """
    s = _sess()
    if extra_headers:
        s.headers.update(extra_headers)

    last_err: Optional[Exception] = None
    used_safari = False

    for attempt in range(max_retries + 1):
        try:
            r = s.get(url, timeout=timeout, allow_redirects=allow_redirects, stream=stream)

            # 403: retry once with different UA + referer
            if r.status_code == 403 and (attempt < max_retries) and not used_safari:
                used_safari = True
                s.headers["User-Agent"] = _SAFARI_UA
                s.headers.setdefault("Referer", f"{urlparse(url).scheme}://{urlparse(url).netloc}/")
                time.sleep(backoff_s * (attempt + 1))
                continue

            if r.status_code in RETRY_STATUSES and attempt < max_retries:
                time.sleep(backoff_s * (attempt + 1))
                continue

            r.raise_for_status()

            # Bound content if requested
            if max_bytes is not None:
                content = b""
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        break
                    content += chunk
                    if len(content) >= max_bytes:
                        break
                r._content = content[:max_bytes]
                r.headers["X-Truncated"] = "1" if len(content) >= max_bytes else "0"
            else:
                _ = r.content  # force download

            return r

        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt >= max_retries:
                break
            time.sleep(backoff_s * (attempt + 1))

    assert last_err is not None
    raise last_err


def _safe_read_text(r: requests.Response) -> str:
    """
    Requests will guess encoding; some gov sites lie/miss headers.
    We try requests' decoded text first, then fall back to utf-8 replace.
    """
    try:
        t = r.text
        if t:
            return t
    except Exception:
        pass
    try:
        return r.content.decode("utf-8", errors="replace")
    except Exception:
        return ""


# ----------------------------
# URL hygiene / canonicalisation (KEEP + enhanced)
# ----------------------------

DROP_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid", "mkt_tok",
}


def canonicalize_url(url: str) -> str:
    """Remove fragments + common tracking params; keep meaningful query params."""
    try:
        p = urlparse(url)
        q = [
            (k, v)
            for (k, v) in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in DROP_QUERY_KEYS
        ]
        new_q = urlencode(q, doseq=True)
        p2 = p._replace(query=new_q, fragment="")
        return urlunparse(p2)
    except Exception:
        return url.split("#", 1)[0]


def domain_of(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


# ----------------------------
# Date extraction helpers (enhanced)
# ----------------------------

_DATE_META_KEYS = (
    ("meta", {"property": "article:published_time"}),
    ("meta", {"name": "article:published_time"}),
    ("meta", {"property": "og:updated_time"}),
    ("meta", {"name": "date"}),
    ("meta", {"name": "publish-date"}),
    ("meta", {"name": "publication_date"}),
    ("meta", {"name": "pubdate"}),
    ("meta", {"property": "og:pubdate"}),
    ("meta", {"itemprop": "datePublished"}),
    ("meta", {"itemprop": "dateCreated"}),
    ("meta", {"itemprop": "dateModified"}),
)


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = dtparser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def extract_published_dt_from_html(html: str) -> Optional[datetime]:
    """
    Extract published/updated date from HTML.
    Priority:
      1) <time datetime=...>
      2) common meta tags
      3) JSON-LD datePublished/dateCreated/dateModified (+ @graph)
      4) visible "Published/Updated" patterns
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    t = soup.find("time", attrs={"datetime": True})
    if t and t.get("datetime"):
        dt = _parse_dt(t.get("datetime", ""))
        if dt:
            return dt

    for tag, attrs in _DATE_META_KEYS:
        m = soup.find(tag, attrs=attrs)
        if m and m.get("content"):
            dt = _parse_dt(m.get("content", ""))
            if dt:
                return dt

    for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        raw = (script.get_text(strip=True) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        blobs = data if isinstance(data, list) else [data]
        for blob in blobs:
            if not isinstance(blob, dict):
                continue

            for key in ("datePublished", "dateCreated", "dateModified"):
                dt = _parse_dt(str(blob.get(key, "")))
                if dt:
                    return dt

            if "@graph" in blob and isinstance(blob["@graph"], list):
                for g in blob["@graph"]:
                    if isinstance(g, dict):
                        for key in ("datePublished", "dateCreated", "dateModified"):
                            dt = _parse_dt(str(g.get(key, "")))
                            if dt:
                                return dt

    text = soup.get_text(" ", strip=True)
    m = re.search(
        r"\b(Published|Posted|Updated)\b[^0-9]{0,20}(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},\s+\d{4})",
        text,
        re.I,
    )
    if m:
        return _parse_dt(m.group(2))

    return None


# ----------------------------
# HTML index extraction (KEEP + improved signal)
# ----------------------------

_BAD_EXT = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".mp4", ".mov", ".avi", ".zip", ".rar", ".7z")
_BAD_SCHEME = ("mailto:", "javascript:", "tel:")

# Keep the spirit of your earlier filtering but improve precision
_BAD_PATH_PAT = re.compile(
    r"/(tag|tags|category|categories|topics|topic|search|subscribe|privacy|terms|cookies|contact|about|careers|jobs|events)/",
    re.I,
)


def _is_probably_article_url(u: str) -> bool:
    if not u:
        return False
    ul = u.lower()
    if ul.startswith(_BAD_SCHEME):
        return False
    if ul.endswith(_BAD_EXT):
        return False
    if _BAD_PATH_PAT.search(ul):
        return False

    # Common news/article URL shapes
    if re.search(r"/20\d{2}/\d{1,2}/", ul):
        return True
    if re.search(r"/(news|media|press|insights|blog|articles|publications|updates|reports)/", ul):
        return True

    # Long slug heuristic (avoid very short nav links)
    if re.search(r"/[a-z0-9\-]{12,}(/|$)", ul):
        return True

    return False


def _extract_links(soup: BeautifulSoup, base_url: str) -> List[Tuple[str, str]]:
    """
    Return list of (url, anchor_text) with strong preference for article-like links.
    """
    out: List[Tuple[str, str]] = []

    def add(a_tag):
        href = (a_tag.get("href") or "").strip()
        if not href or href.startswith("#"):
            return
        url = canonicalize_url(urljoin(base_url, href))
        if not url.startswith("http"):
            return
        text = " ".join((a_tag.get_text(" ", strip=True) or "").split())
        out.append((url, text))

    # Prefer <article> and <main>
    for a in soup.select("article a[href]"):
        add(a)
    for a in soup.select("main a[href]"):
        add(a)

    # Some sites use list cards
    for a in soup.select(".card a[href], .teaser a[href], .views-row a[href]"):
        add(a)

    # Fallback: all links (only if we found nothing)
    if not out:
        for a in soup.select("a[href]"):
            add(a)

    # De-dupe preserving order
    seen: Set[str] = set()
    cleaned: List[Tuple[str, str]] = []
    for url, text in out:
        if url in seen:
            continue
        seen.add(url)
        cleaned.append((url, text))
    return cleaned


def _find_next_page(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    # rel="next"
    link = soup.find("a", rel=lambda v: v and "next" in v)
    if link and link.get("href"):
        return canonicalize_url(urljoin(base_url, link["href"]))

    # common pager patterns
    for a in soup.select("a[href]"):
        t = (a.get_text(" ", strip=True) or "").lower()
        if t in {"next", "older", "older posts", "more", "load more"}:
            return canonicalize_url(urljoin(base_url, a["href"]))

    return None


def fetch_html_index(
    index_url: str,
    *,
    max_links: int = 250,
    max_pages: int = 5,
    date_resolve_budget: int = 75,
) -> List[Item]:
    """
    Fetch an HTML index page (and some pagination) and return candidate Items.
    Preserves previous semantics (returns Items, sometimes without dates).
    Enhancements:
    - better filtering for article-like links
    - bounded date resolution using improved HTML date extraction
    """
    items: List[Item] = []
    visited_pages: Set[str] = set()
    next_url: Optional[str] = index_url

    while next_url and len(visited_pages) < max_pages and len(items) < max_links:
        page_url = next_url
        next_url = None
        if page_url in visited_pages:
            break
        visited_pages.add(page_url)

        r = _http_get(page_url, timeout=(8.0, 35.0), max_retries=2)
        html = _safe_read_text(r)
        soup = BeautifulSoup(html, "html.parser")

        links = _extract_links(soup, page_url)
        for url, text in links:
            if len(items) >= max_links:
                break
            if not _is_probably_article_url(url):
                continue

            it = Item(url=url, title=text or "", summary="")
            it.domain = domain_of(url)
            items.append(it)

        next_url = _find_next_page(soup, page_url)

    # Resolve dates for a bounded subset.
    budget = min(date_resolve_budget, len(items))
    to_resolve: List[Item] = []

    for it in items:
        if len(to_resolve) >= budget:
            break
        if it.published_ts is not None:
            continue

        # quick date from URL pattern
        m = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})/", it.url)
        if m:
            try:
                y, mo, d = map(int, m.groups())
                it.published_ts = datetime(y, mo, d, tzinfo=timezone.utc)
                continue
            except Exception:
                pass

        to_resolve.append(it)

    if to_resolve:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _resolve_one(url: str) -> Optional[datetime]:
            try:
                rr = _http_get(url, timeout=(6.0, 25.0), max_retries=1)
                return extract_published_dt_from_html(_safe_read_text(rr))
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(_resolve_one, it.url): it for it in to_resolve}
            for fut in as_completed(futs):
                it = futs[fut]
                try:
                    it.published_ts = fut.result()
                except Exception:
                    it.published_ts = None

    return items


# ----------------------------
# RSS (KEEP + more tolerant)
# ----------------------------

def fetch_rss(feed_url: str) -> List[Item]:
    r = _http_get(feed_url, timeout=(8.0, 35.0), max_retries=2)
    parsed = feedparser.parse(r.content)

    out: List[Item] = []
    for e in parsed.entries:
        url = canonicalize_url(getattr(e, "link", "") or "")
        if not url:
            continue

        title = getattr(e, "title", "") or ""
        summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""

        published = None
        for key in ("published", "updated", "pubDate", "date"):
            val = getattr(e, key, None)
            if val:
                published = _parse_dt(str(val))
                if published:
                    break

        it = Item(url=url, title=title, summary=summary, published_ts=published)
        it.domain = domain_of(url)
        out.append(it)

    return out


# ----------------------------
# Full-text fetch (KEEP signature; enhanced)
# ----------------------------

_PDF_EXT = re.compile(r"\.pdf(\?|$)", re.I)


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return ""
    if fitz is None:
        return ""  # PyMuPDF not available
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        parts: List[str] = []
        # hard cap pages to avoid pathological PDFs
        max_pages = int(os.getenv("MAX_PDF_PAGES", "60"))
        for i, page in enumerate(doc):
            if i >= max_pages:
                break
            parts.append(page.get_text("text") or "")
        doc.close()
        text = "\n".join(parts).strip()
        return text
    except Exception:
        return ""


def fetch_full_text(url: str) -> str:
    """
    Fetch main text content for a URL.

    COMPAT: returns text str ONLY.

    Enhancements:
    - if PDF and within MAX_PDF_BYTES, extract text via PyMuPDF
    - for HTML, extract via trafilatura; fallback to cleaned visible text
    - robust GET with retries and timeouts
    """
    url = canonicalize_url(url)

    # Allow configuration but keep safe defaults
    max_pdf_bytes = int(os.getenv("MAX_PDF_BYTES", str(5 * 1024 * 1024)))

    try:
        # Fast path: PDF by extension
        if _PDF_EXT.search(url):
            r = _http_get(
                url,
                timeout=(10.0, 60.0),
                max_retries=1,
                stream=True,
                max_bytes=max_pdf_bytes,
                extra_headers={"Accept": "application/pdf,*/*;q=0.8"},
            )
            ctype = (r.headers.get("content-type") or "").lower()
            pdf_bytes = r.content or b""
            # If truncated, text may be garbage; still attempt but it might be incomplete
            if "pdf" in ctype or url.lower().endswith(".pdf"):
                return _extract_pdf_text(pdf_bytes)
            return ""

        # HTML path
        r = _http_get(url, timeout=(10.0, 50.0), max_retries=1)
        html = _safe_read_text(r)

        text = trafi_extract(html, url=url, include_comments=False, include_tables=False) or ""
        text = text.strip()
        if text:
            return text

        # fallback: visible text (very conservative)
        soup = BeautifulSoup(html, "html.parser")
        fallback = soup.get_text(" ", strip=True)
        fallback = re.sub(r"\s+", " ", fallback).strip()
        return fallback

    except Exception:
        return ""


def fetch_date_only(url: str) -> Optional[datetime]:
    """Cheaper helper to resolve published date without full-text extraction."""
    try:
        r = _http_get(url, timeout=(6.0, 25.0), max_retries=1)
        return extract_published_dt_from_html(_safe_read_text(r))
    except Exception:
        return None
