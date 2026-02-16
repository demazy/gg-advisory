# -*- coding: utf-8 -*-
"""
Fetching utilities:
- RSS/Atom ingestion
- HTML index page link extraction
- Full-text extraction (HTML/PDF) with robust fallbacks

Design goals:
- Defensive: network failures return empty results rather than raising
- Bounded runtime: caps on bytes, links, and optional per-link metadata resolution
- Stable API: entry points accept **kwargs for forward compatibility

Incremental improvements (Feb 2026, run-6 -> run-7):
1) Reduce "navigation noise" from HTML index pages:
   - remove <header>/<nav>/<footer>/<aside>/<form> blocks before anchor extraction
   - treat generic anchors ("Read more", icon labels) as empty and attempt nearby heading fallback
2) Improve publish-date inference:
   - extend URL-based inference to catch month names inside slugs (e.g., ".../2026/issb-update-january-2026.html")
   - optional per-link metadata fetch (bounded by MAX_DATE_RESOLVE_FETCHES_PER_INDEX) to extract meta/JSON-LD dates
3) Make downstream month filtering feasible for HTML-only sources by setting Item.published_ts/iso when detected.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urldefrag, urlparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup

import feedparser

try:
    import trafilatura
except Exception:  # pragma: no cover
    trafilatura = None

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

try:
    from dateutil import parser as dtparser  # type: ignore
except Exception:  # pragma: no cover
    dtparser = None


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
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "0"))

# By default, only keep links on the same site as the index page.
ALLOW_EXTERNAL_LINKS_FROM_INDEX = os.getenv("ALLOW_EXTERNAL_LINKS_FROM_INDEX", "0") == "1"

# PDFs are expensive; optionally only allow PDFs from trusted domains.
PDF_TRUSTED = {d.strip().lower() for d in os.getenv("PDF_TRUSTED", "").split(",") if d.strip()}


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
_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


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
    """
    Normalise URL by stripping fragments and common tracking parameters.
    (Keeps non-tracking query parameters as some sites use them for canonical routing.)
    """
    u = (u or "").strip()
    if not u:
        return ""
    u, _frag = urldefrag(u)

    try:
        p = urlparse(u)
        if not p.query:
            return u
        q = parse_qs(p.query, keep_blank_values=True)
        # drop common tracking params
        drop = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "mc_cid", "mc_eid"}
        q2 = {k: v for k, v in q.items() if k not in drop and not k.lower().startswith("utm_")}
        query = urlencode([(k, vv) for k, vs in q2.items() for vv in (vs or [""])], doseq=True)
        return p._replace(query=query).geturl()
    except Exception:
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


def _clean_anchor_text(t: str) -> str:
    """
    Clean anchor text. If it is boilerplate ("Read more") or icon glyph text, treat as empty.
    """
    t = (t or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t).strip()

    tl = t.lower()
    if tl in {"skip to content", "skip to main content", "read more", "learn more", "more"}:
        return ""

    # Common icon font labels leaking into text nodes
    if tl in {"arrow_right_alt", "arrow_forward", "chevron_right", "chevron_left"}:
        return ""

    # If "Read more ..." keep only if it contains other meaningful words
    if tl.startswith("read more"):
        rest = tl.replace("read more", "").strip(" -–—:")
        if len(rest) < 8:
            return ""

    # Remove obvious icon word tails
    t = re.sub(r"\barrow_(right|left|forward|back)(?:_alt)?\b", "", t, flags=re.I).strip()
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # normalise Z
    s2 = s.replace("Z", "+00:00")
    try:
        # fromisoformat accepts many ISO variants but not all
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    if dtparser is not None:
        try:
            dt = dtparser.parse(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def infer_published_ts_from_url(url: str) -> Optional[float]:
    """
    Best-effort inference of publish timestamp from URL path.
    Used to improve month-based filtering when RSS dates are missing.

    Conservative (month-level at best):
    - YYYY-MM-DD
    - /YYYY/MM/DD/
    - /YYYY/<monthname>/
    - /YYYY/MM/
    - /YYYY/<slug containing monthname>/   (NEW)
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

    # /YYYY/<slug with monthname>  (e.g., ".../2026/issb-update-january-2026.html")
    m = re.search(r"/(20\d{2})/([^/]+)", path)
    if m:
        y = int(m.group(1))
        slug = m.group(2)
        for name, mo in _MONTHS.items():
            if re.search(rf"(^|[-_\.]){re.escape(name)}($|[-_\.])", slug):
                try:
                    return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()
                except Exception:
                    return None

    return None


def is_probably_taxonomy_or_hub(url: str) -> bool:
    """
    Return True for URLs that are unlikely to be *content items* (listing pages,
    taxonomy pages, nav/utility pages, auth flows).

    NOTE: used both during index extraction and selection filtering.
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
    # EFRAG-style filter parameters (e.g., ?f[0]=category:...) indicate listing pages
    if any(k.startswith("f[") for k in q.keys()):
        return True
    if "page" in q and (parsed.path.endswith("/news") or parsed.path.endswith("/news/")):
        return True
    if ("s" in q or "q" in q) and parsed.path.endswith("/search"):
        return True

    path = parsed.path or "/"
    # nav/utility endpoints (only if the *last* segment is utility-ish)
    utility_segments = {
        "about", "contact", "privacy", "terms", "cookies", "accessibility", "sitemap",
        "careers", "jobs", "vacancies",
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
            # treat 4xx/5xx as failure (but don't raise)
            if r.status_code >= 400:
                try:
                    r.close()
                except Exception:
                    pass
                return None
            return r
        except Exception as e:  # pragma: no cover
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF ** attempt)
                continue
            return None


def _read_limited(resp: requests.Response, cap: int) -> bytes:
    """
    Read up to cap bytes from streaming response.
    """
    out = bytearray()
    try:
        for chunk in resp.iter_content(chunk_size=64_000):
            if not chunk:
                continue
            out.extend(chunk)
            if len(out) >= cap:
                break
    except Exception:  # pragma: no cover
        pass
    try:
        resp.close()
    except Exception:
        pass
    return bytes(out)


def _strip_nav_blocks(soup: BeautifulSoup) -> None:
    """
    Remove typical navigation/utility blocks so we don't harvest header/footer links.
    """
    for sel in ("header", "nav", "footer", "aside", "form"):
        for el in soup.select(sel):
            try:
                el.decompose()
            except Exception:
                try:
                    el.extract()
                except Exception:
                    pass


def _heading_fallback(anchor) -> str:
    """
    If anchor text is generic, look for a nearby heading in the card/article/list item.
    """
    try:
        # 1) heading inside the anchor
        for tag in anchor.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            t = _clean_anchor_text(tag.get_text(" ", strip=True))
            if t:
                return t

        # 2) closest container and pick first heading
        container = anchor.find_parent(["article", "li", "div", "section"])
        if container:
            h = container.find(["h1", "h2", "h3", "h4", "h5", "h6"])
            if h:
                t = _clean_anchor_text(h.get_text(" ", strip=True))
                if t:
                    return t

        # 3) previous sibling headings
        prev = anchor
        for _ in range(4):
            prev = prev.find_previous(["h1", "h2", "h3", "h4", "h5", "h6"])
            if not prev:
                break
            t = _clean_anchor_text(prev.get_text(" ", strip=True))
            if t:
                return t
    except Exception:
        return ""
    return ""


def _extract_title_and_date_from_html(html: str) -> Tuple[Optional[str], Optional[float]]:
    """
    Extract best-effort title + published_ts from HTML (meta tags, JSON-LD, <time datetime>).
    """
    if not html:
        return None, None

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None, None

    title: Optional[str] = None
    published_ts: Optional[float] = None

    # Title: og:title > twitter:title > <title>
    for sel, attr, key in [
        ("meta[property='og:title']", "content", "og"),
        ("meta[name='twitter:title']", "content", "twitter"),
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get(attr):
            t = _clean_anchor_text(str(tag.get(attr)))
            if t:
                title = t
                break
    if not title:
        ttag = soup.find("title")
        if ttag:
            t = _clean_anchor_text(ttag.get_text(" ", strip=True))
            if t:
                title = t

    # Published date from meta tags
    meta_candidates = []
    for tag in soup.find_all("meta"):
        k = (tag.get("property") or tag.get("name") or tag.get("itemprop") or "").strip().lower()
        v = (tag.get("content") or "").strip()
        if not k or not v:
            continue
        if any(x in k for x in ("published", "pubdate", "datepublished", "datecreated", "dc.date", "dcterms.issued", "article:published_time")):
            meta_candidates.append(v)
    for v in meta_candidates:
        dt = _parse_dt(v)
        if dt:
            published_ts = dt.timestamp()
            break

    # <time datetime="...">
    if published_ts is None:
        for t in soup.find_all("time"):
            dt_s = (t.get("datetime") or "").strip()
            if not dt_s:
                continue
            dt = _parse_dt(dt_s)
            if dt:
                published_ts = dt.timestamp()
                break

    # JSON-LD
    if published_ts is None:
        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(s.get_text(" ", strip=True) or "{}")
            except Exception:
                continue
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict):
                    continue
                typ = obj.get("@type") or obj.get("['@type']")
                # @type may be list
                types = typ if isinstance(typ, list) else [typ]
                if not any(t in ("NewsArticle", "Article", "Report", "WebPage") for t in types if isinstance(t, str)):
                    # Some pages embed multiple blocks; don't require strict type
                    pass
                for k in ("datePublished", "dateCreated", "dateModified"):
                    if k in obj:
                        dt = _parse_dt(str(obj.get(k)))
                        if dt:
                            published_ts = dt.timestamp()
                            break
                if published_ts is not None:
                    break
            if published_ts is not None:
                break

    return title, published_ts


def _looks_content_url(u: str) -> bool:
    """
    Cheap heuristic to decide whether a link is worth per-link date resolution.
    """
    if not u:
        return False
    ul = u.lower()
    if is_probably_taxonomy_or_hub(ul):
        return False
    if _deny_from_index(ul):
        return False
    path = urlparse(ul).path
    segs = [s for s in path.split("/") if s]
    if not segs:
        return False
    # evergreen / nav heavy sections
    evergreen = {"about", "governance", "board", "leadership", "executive", "team", "contact", "privacy", "terms", "cookie", "legal"}
    if any(s in evergreen for s in segs):
        return False
    # positive signals
    if re.search(r"(20\d{2})", path):
        return True
    if any(s in {"news", "media", "press", "blog", "insights", "updates", "publication", "publications", "knowledge-bank", "articles", "announcements"} for s in segs):
        return True
    # long slug
    if segs and len(segs[-1]) >= 18:
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

        published_ts = None
        published_iso = None

        # feedparser populates published_parsed when possible
        for attr in ("published_parsed", "updated_parsed"):
            st = getattr(e, attr, None)
            if st:
                try:
                    # time.struct_time -> seconds (UTC best-effort)
                    published_ts = time.mktime(st)
                    published_iso = datetime.fromtimestamp(published_ts, tz=timezone.utc).isoformat()
                    break
                except Exception:
                    pass

        # fallback: infer from URL
        if published_ts is None:
            published_ts = infer_published_ts_from_url(link)
            if published_ts:
                published_iso = datetime.fromtimestamp(published_ts, tz=timezone.utc).isoformat()

        items.append(
            Item(
                url=link,
                title=title,
                summary=(getattr(e, "summary", "") or "").strip(),
                source=source_name or _norm_host(urlparse(link).netloc),
                published_iso=published_iso,
                published_ts=published_ts,
                published_source="rss" if published_ts else None,
                published_confidence=0.8 if published_ts else None,
                index_url=feed_url,
            )
        )

    return items


def fetch_html_index(index_url: str, source_name: str = "", **kwargs) -> List[Item]:
    """
    Extract candidate content links from an index/listing page.

    Important: be conservative. It's better to return fewer, higher-signal candidates
    than hundreds of header/footer navigation links.
    """
    index_url = _clean_url(index_url)
    if not index_url:
        return []

    # Pagination support: attempt up to MAX_INDEX_PAGES by following rel=next.
    pages: List[str] = [index_url]
    visited = {index_url.lower()}
    if MAX_INDEX_PAGES > 1:
        # We'll discover next links as we go.
        pass

    title_by_url: Dict[str, str] = {}
    links_order: List[str] = []

    def harvest_from_html(html: str, base_url: str) -> Optional[str]:
        """
        Harvest anchors and return a 'next' URL if present.
        """
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        # NEW: strip header/footer/nav/aside/form
        _strip_nav_blocks(soup)

        # Find <link rel="next"> or <a rel="next">
        next_url: Optional[str] = None
        link_next = soup.find("link", attrs={"rel": re.compile(r"\bnext\b", re.I)})
        if link_next and link_next.get("href"):
            next_url = _clean_url(urljoin(base_url, str(link_next.get("href"))))
        if not next_url:
            a_next = soup.find("a", attrs={"rel": re.compile(r"\bnext\b", re.I)})
            if a_next and a_next.get("href"):
                next_url = _clean_url(urljoin(base_url, str(a_next.get("href"))))

        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("mailto:") or href.startswith("javascript:"):
                continue

            abs_url = _clean_url(urljoin(base_url, href))
            if not abs_url:
                continue
            if urlparse(abs_url).scheme not in ("http", "https"):
                continue

            # Filter obvious non-content
            if _deny_from_index(abs_url) or is_probably_taxonomy_or_hub(abs_url):
                continue

            if (not ALLOW_EXTERNAL_LINKS_FROM_INDEX) and (not _same_site(abs_url, base_url)):
                continue

            # anchor text with fallback
            t = _clean_anchor_text(a.get_text(" ", strip=True) or "")
            if not t:
                t = _heading_fallback(a)
            if not t:
                # still keep the URL but with empty title; we'll try per-link title later if enabled
                t = ""

            # Keep best (longest) anchor text seen for a URL
            if abs_url not in title_by_url or len(t) > len(title_by_url.get(abs_url, "")):
                if t:
                    title_by_url[abs_url] = t

            links_order.append(abs_url)

        return next_url

    def fetch_html(url: str) -> str:
        resp = _http_get(url)
        if resp is None:
            return ""
        raw = _read_limited(resp, MAX_BYTES)
        if not raw:
            return ""
        try:
            return raw.decode(resp.encoding or "utf-8", errors="replace")
        except Exception:
            return raw.decode("utf-8", errors="replace")

    next_url = None
    for page_i in range(MAX_INDEX_PAGES):
        cur = pages[-1]
        html = fetch_html(cur)
        if not html:
            break
        next_url = harvest_from_html(html, cur)

        if not next_url:
            break
        if next_url.lower() in visited:
            break
        if not _same_site(next_url, index_url):
            break
        visited.add(next_url.lower())
        pages.append(next_url)

    # De-dupe, keep order
    seen = set()
    uniq: List[str] = []
    for u in links_order:
        key = u.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(u)

    uniq = uniq[:MAX_LINKS_PER_INDEX]

    # Optional per-link metadata resolution (bounded).
    # We only do this for likely content URLs lacking URL-inferred dates (and/or generic titles).
    resolve_budget = max(0, int(MAX_DATE_RESOLVE_FETCHES_PER_INDEX))
    resolved_meta: Dict[str, Tuple[Optional[str], Optional[float]]] = {}
    if resolve_budget > 0:
        for u in uniq:
            if resolve_budget <= 0:
                break
            # If we already inferred a date, we can skip resolving date (but may still need title if missing).
            inferred_ts = infer_published_ts_from_url(u)
            needs_title = not bool(title_by_url.get(u))
            if inferred_ts is not None and not needs_title:
                continue
            if not _looks_content_url(u):
                continue

            # fetch small page HTML; avoid PDFs here (handled later in full-text extraction)
            if u.lower().endswith(".pdf"):
                continue

            html = fetch_html(u)
            if not html:
                continue
            t2, ts2 = _extract_title_and_date_from_html(html)
            if t2 or ts2:
                resolved_meta[u] = (t2, ts2)
            resolve_budget -= 1

    items: List[Item] = []
    for u in uniq:
        inferred_ts = infer_published_ts_from_url(u)
        t_meta, ts_meta = resolved_meta.get(u, (None, None))

        # Prefer meta date over URL inference (higher confidence)
        final_ts = ts_meta if ts_meta is not None else inferred_ts
        final_iso = datetime.fromtimestamp(final_ts, tz=timezone.utc).isoformat() if final_ts else None

        # Source: prefer explicit source_name for same-site links, else fall back to the URL's host.
        src = source_name or _norm_host(urlparse(index_url).netloc)
        if not _same_site(u, index_url):
            src = _norm_host(urlparse(u).netloc)

        title = title_by_url.get(u, "") or (t_meta or "") or u

        items.append(
            Item(
                url=u,
                title=title,
                summary="",
                source=src,
                published_iso=final_iso,
                published_ts=final_ts,
                published_source=("meta" if ts_meta else ("url" if inferred_ts else None)),
                published_confidence=(0.75 if ts_meta else (0.35 if inferred_ts else None)),
                index_url=index_url,
            )
        )

    return items


def _pdf_allowed(url: str) -> bool:
    if not url.lower().endswith(".pdf"):
        return False
    if not PDF_TRUSTED:
        return True
    host = _norm_host(urlparse(url).netloc)
    return any(host == d or host.endswith("." + d) for d in PDF_TRUSTED)


def fetch_full_text(url: str, **kwargs) -> str:
    """
    Fetch full text for a URL (HTML or PDF).

    Returns extracted plain text (best-effort). Never raises.
    """
    url = _clean_url(url)
    if not url:
        return ""

    # PDFs
    if url.lower().endswith(".pdf"):
        if not _pdf_allowed(url):
            return ""
        return _fetch_pdf_text(url)

    # HTML
    return _fetch_html_text(url)


def _fetch_html_text(url: str) -> str:
    resp = _http_get(url)
    if resp is None:
        return ""
    raw = _read_limited(resp, MAX_BYTES)
    if not raw:
        return ""

    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    if trafilatura is not None:
        try:
            txt = trafilatura.extract(html, include_comments=False, include_tables=True) or ""
            return (txt or "").strip()
        except Exception:
            pass

    # fallback: crude soup get_text
    try:
        soup = BeautifulSoup(html, "html.parser")
        _strip_nav_blocks(soup)
        txt = soup.get_text(" ", strip=True)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt
    except Exception:
        return ""


def _fetch_pdf_text(url: str) -> str:
    resp = _http_get(url)
    if resp is None:
        return ""
    raw = _read_limited(resp, MAX_PDF_BYTES)
    if not raw:
        return ""

    if fitz is None:
        return ""

    try:
        doc = fitz.open(stream=raw, filetype="pdf")
    except Exception:
        return ""

    out_parts: List[str] = []
    try:
        for page in doc:
            try:
                out_parts.append(page.get_text("text"))
            except Exception:
                continue
    finally:
        try:
            doc.close()
        except Exception:
            pass

    txt = "\n".join(out_parts)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt
