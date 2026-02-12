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
import json
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

    # Drop common tracking query params to reduce duplicates.
    try:
        p = urlparse(u)
        if p.query:
            from urllib.parse import parse_qsl, urlencode, urlunparse
            keep = []
            for k, v in parse_qsl(p.query, keep_blank_values=True):
                kl = (k or "").lower()
                if kl.startswith("utm_") or kl in {"fbclid", "gclid", "mc_cid", "mc_eid"}:
                    continue
                keep.append((k, v))
            new_q = urlencode(keep, doseq=True)
            u = urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, ""))  # fragment already removed
    except Exception:
        pass

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
    Best-effort month inference from URL patterns.

    CHANGE (Feb 2026):
    - Handle "monthname-year" in slugs (e.g. .../issb-update-january-2026.html).
      IFRS Updates commonly use this pattern.
    """
    u = (url or "").strip()
    if not u:
        return None
    ul = u.lower()

    # Common YYYY/MM(/DD) in path
    m = re.search(r"/(20\d{2})/([01]?\d)(?:/([0-3]?\d))?/", ul)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3) or 1)
        try:
            return datetime(y, mo, max(1, min(28, d)), tzinfo=timezone.utc).timestamp()
        except Exception:
            return None

    # Common YYYY-MM or YYYY_MM
    m = re.search(r"(20\d{2})[-_](0?[1-9]|1[0-2])(?:[-_](0?[1-9]|[12]\d|3[01]))?", ul)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3) or 1)
        try:
            return datetime(y, mo, max(1, min(28, d)), tzinfo=timezone.utc).timestamp()
        except Exception:
            return None

    # Common /YYYY/<monthname>/ in path
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    m = re.search(r"/(20\d{2})/(january|february|march|april|may|june|july|august|september|october|november|december)/", ul)
    if m:
        y = int(m.group(1))
        mo = month_map.get(m.group(2), 0)
        if mo:
            return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()

    # Slug patterns: <monthname>-YYYY or YYYY-<monthname>
    m = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december)[-_](20\d{2})", ul)
    if m:
        mo = month_map.get(m.group(1), 0)
        y = int(m.group(2))
        if mo:
            return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()

    m = re.search(r"(20\d{2})[-_](january|february|march|april|may|june|july|august|september|october|november|december)", ul)
    if m:
        y = int(m.group(1))
        mo = month_map.get(m.group(2), 0)
        if mo:
            return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()

    return None


def is_probably_taxonomy_or_hub(url: str) -> bool:
    """
    Heuristic: return True for URLs that are unlikely to be *content items* (listing pages,
    taxonomy/facet pages, nav/utility pages, auth flows).

    CHANGE (Feb 2026):
    - Treat "faceted listing" URLs (e.g., EFRAG f[0]=..., ?type=..., ?category=...) as hubs.
      These were previously mis-classified as content items, then "dated" via first <time> tag,
      which polluted selection.
    """
    u = (url or "").strip()
    if not u:
        return True
    ul = u.lower()
    parsed = urlparse(ul)

    # obvious auth/redirect/tracking flows
    if any(s in ul for s in ("oauth-redirect", "j_security_check", "sso", "signin", "login")):
        return True

    q = parse_qs(parsed.query or "")

    # query-based searches / pagination / facets
    # - EFRAG uses f[0]=category:... (facets); treat as hubs/taxonomy.
    # - Many sites use ?type=media+release / ?category=... for listings.
    if q:
        # common "listing" params
        listing_keys = {"page", "paged", "offset", "start", "from", "to", "q", "s", "search", "category", "tag", "topic", "type"}
        if any(k in q for k in listing_keys):
            # allow lightweight tracking params only
            tracking_keys = {"utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "fbclid", "gclid"}
            non_tracking = [k for k in q.keys() if k not in tracking_keys]
            if non_tracking:
                return True
        # facet keys like f[0], f[1]...
        if any(k.startswith("f[") or k.startswith("f%5b") for k in q.keys()):
            return True
        # special-case: some sites put facets in repeated "f" keys
        if "f" in q and len(q.get("f") or []) > 0:
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
        "board", "governance", "leadership", "executive", "executives", "management", "team", "teams",
        "people", "our-people", "who-we-are", "organisation", "organization",
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
        "/board", "/governance", "/leadership", "/executive", "/executives", "/team", "/our-people", "/people",
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
    # strip common icon tokens
    t = t.replace("arrow_right_alt", "").strip()

    # very common boilerplate link labels
    if t.lower() in {"skip to content", "skip to main content", "read more", "learn more", "more", "continue reading"}:
        return ""
    return t

_GENERIC_ANCHOR_RE = re.compile(r"^(read more|learn more|more|continue reading)$", re.I)


def _best_anchor_title(a: Any) -> str:
    """Recover a meaningful title for an <a>, even when the anchor text is generic."""
    try:
        txt = _clean_anchor_text(a.get_text(" ", strip=True) or "")
    except Exception:
        txt = ""
    if txt and (not _GENERIC_ANCHOR_RE.match(txt)):
        return txt

    try:
        parent = a
        for _ in range(7):
            parent = getattr(parent, "parent", None)
            if parent is None:
                break
            if getattr(parent, "name", "") in {"article", "li", "div", "section"}:
                h = parent.find(["h1", "h2", "h3", "h4"])
                if h is not None:
                    ht = _clean_anchor_text(h.get_text(" ", strip=True) or "")
                    if ht and (not _GENERIC_ANCHOR_RE.match(ht)):
                        return ht
    except Exception:
        pass

    return txt or ""


def _parse_dt_like(s: str) -> Optional[datetime]:
    """
    Parse a date/datetime string in common web formats (best-effort, UTC).

    CHANGE (Feb 2026):
    - Support HTTP-date (RFC 2822 / RFC 1123) via email.utils.parsedate_to_datetime.
    - Support common human formats like "20 January 2026" and "January 20, 2026".
    """
    if not s:
        return None
    ss = str(s).strip()
    if not ss:
        return None

    # HTTP date (e.g. "Tue, 30 Jan 2026 08:11:12 GMT")
    try:
        from email.utils import parsedate_to_datetime  # stdlib
        dt = parsedate_to_datetime(ss)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        pass

    ss = ss.replace("Z", "+00:00")
    ss = re.sub(r"(\.\d{3,6})\+00:00$", "+00:00", ss)

    # ISO date
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ss):
        try:
            return datetime.fromisoformat(ss).replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # ISO datetime
    try:
        dt = datetime.fromisoformat(ss)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Human dates: "20 January 2026" / "20 Jan 2026"
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y"):
        try:
            dt = datetime.strptime(ss, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    # "January 20, 2026"
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            dt = datetime.strptime(ss, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    # Extract embedded yyyy-mm-dd
    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", ss)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        except Exception:
            return None

    # Extract embedded "dd Month yyyy"
    m = re.search(r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})", ss, re.I)
    if m:
        try:
            dt = datetime.strptime(f"{int(m.group(1))} {m.group(2)} {int(m.group(3))}", "%d %B %Y").replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    return None


def _resolve_published_ts_from_html(url: str) -> Optional[float]:
    """
    Fetch a small slice of HTML and extract published date from meta/time/JSON-LD.

    CHANGE (Feb 2026):
    - Much broader metadata support (dc/dcterms/article meta).
    - Parse <time> inner text when datetime attr is absent.
    - Parse nested JSON-LD (@graph, lists, nested objects).
    - Regex fallback against visible text for "Published/Released" date patterns.

    This materially increases dated candidates for domains like aemc.gov.au and arena.gov.au,
    where URLs rarely embed dates.
    """
    resp = _http_get(url)
    if resp is None:
        return None
    ctype = _content_type(resp)
    raw = _read_limited(resp, min(MAX_BYTES, 450_000))
    if not raw:
        return None

    last_mod = None
    try:
        lm = resp.headers.get("Last-Modified") or ""
        if lm:
            dt = _parse_dt_like(lm)
            if dt:
                last_mod = dt.timestamp()
    except Exception:
        pass

    if ("application/pdf" in ctype) or url.lower().endswith(".pdf"):
        return last_mod

    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    soup = BeautifulSoup(html, "html.parser")

    # ---- meta tags ----
    meta_candidates = [
        # OpenGraph / article
        {"property": "article:published_time"},
        {"property": "article:modified_time"},
        {"property": "og:published_time"},
        {"property": "og:updated_time"},
        # Common name-based tags
        {"name": "article:published_time"},
        {"name": "pubdate"},
        {"name": "publishdate"},
        {"name": "publish_date"},
        {"name": "date"},
        {"name": "dc.date"},
        {"name": "dc.date.issued"},
        {"name": "dc.date.created"},
        {"name": "dcterms.date"},
        {"name": "dcterms.created"},
        {"name": "dcterms.issued"},
        {"name": "last-modified"},
    ]
    for attrs in meta_candidates:
        el = soup.find("meta", attrs=attrs)
        if el and el.get("content"):
            dt = _parse_dt_like(el.get("content"))
            if dt:
                return dt.timestamp()

    # ---- <time> tag ----
    t = soup.find("time")
    if t is not None:
        dtattr = (t.get("datetime") or "").strip()
        if dtattr:
            dt = _parse_dt_like(dtattr)
            if dt:
                return dt.timestamp()
        # inner text (e.g. "20 January 2026")
        inner = t.get_text(" ", strip=True)
        dt = _parse_dt_like(inner)
        if dt:
            return dt.timestamp()

    # ---- JSON-LD ----
    def walk(obj) -> List[str]:
        out: List[str] = []
        if isinstance(obj, dict):
            # unwrap graphs
            if "@graph" in obj and isinstance(obj.get("@graph"), list):
                for it in obj["@graph"]:
                    out.extend(walk(it))
            for k, v in obj.items():
                if k in ("datePublished", "dateCreated", "dateModified", "uploadDate"):
                    if isinstance(v, str):
                        out.append(v)
                else:
                    out.extend(walk(v))
        elif isinstance(obj, list):
            for it in obj:
                out.extend(walk(it))
        return out

    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            txt = (s.string or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
            for cand in walk(data):
                dt = _parse_dt_like(cand)
                if dt:
                    return dt.timestamp()
        except Exception:
            continue

    # ---- visible text fallback ----
    try:
        text = soup.get_text(" ", strip=True)
        # Published: 20 January 2026
        m = re.search(r"(Published|Publication date|Released|Posted|Date)\s*[:\-]?\s*(\d{1,2}\s+[A-Za-z]{3,9}\s+20\d{2})", text, re.I)
        if m:
            dt = _parse_dt_like(m.group(2))
            if dt:
                return dt.timestamp()
        # 2026-01-20 (rare)
        m = re.search(r"(Published|Released|Posted|Date)\s*[:\-]?\s*(20\d{2}-\d{2}-\d{2})", text, re.I)
        if m:
            dt = _parse_dt_like(m.group(2))
            if dt:
                return dt.timestamp()
    except Exception:
        pass

    return last_mod


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
    Fetch a hub/listing HTML page and extract likely content links.

    CHANGE (Feb 2026):
    - Actually honour MAX_INDEX_PAGES by following rel="next" (or common next-page patterns).
      The workflow already sets MAX_INDEX_PAGES, but the previous implementation fetched only
      the first page, which reduced coverage and amplified per-domain caps.
    - Share a single date-resolution budget across all fetched pages for that index.
    """
    index_url = _clean_url(index_url)
    if not index_url:
        return []

    max_pages = int(kwargs.get("max_index_pages") or MAX_INDEX_PAGES)
    max_links = int(kwargs.get("max_links_per_index") or MAX_LINKS_PER_INDEX)
    date_budget = int(kwargs.get("max_date_resolve_fetches") or MAX_DATE_RESOLVE_FETCHES_PER_INDEX)

    collected: List[Tuple[str, str]] = []  # (url, title)
    visited_pages: set = set()
    cur = index_url

    for _page in range(max(1, max_pages)):
        if not cur or cur in visited_pages:
            break
        visited_pages.add(cur)

        resp = _http_get(cur)
        if resp is None:
            break

        raw = _read_limited(resp, MAX_BYTES)
        if not raw:
            break

        try:
            html = raw.decode(resp.encoding or "utf-8", errors="replace")
        except Exception:
            html = raw.decode("utf-8", errors="replace")

        soup = BeautifulSoup(html, "html.parser")
        # strip scripts/styles
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # Extract candidate links from anchors
        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            href = href.strip()
            if not href:
                continue
            abs_url = urljoin(cur, href)
            abs_url = _clean_url(abs_url)
            if not abs_url:
                continue
            if not _same_site(index_url, abs_url):
                # keep only same-site by default to avoid scraping irrelevant external links
                continue
            if _deny_from_index(abs_url):
                continue
            if is_probably_taxonomy_or_hub(abs_url):
                continue

            title = _best_anchor_title(a)
            if not title:
                continue

            collected.append((abs_url, title))
            if len(collected) >= max_links:
                break
        if len(collected) >= max_links:
            break

        # Find "next" page (rel=next, or common pager patterns)
        next_url = None
        try:
            # <link rel="next" href="...">
            lnk = soup.find("link", rel=lambda v: v and "next" in (v if isinstance(v, list) else [v]))
            if lnk and lnk.get("href"):
                next_url = urljoin(cur, lnk.get("href"))
            if not next_url:
                # <a rel="next" href="...">
                a_next = soup.find("a", rel=lambda v: v and "next" in (v if isinstance(v, list) else [v]))
                if a_next and a_next.get("href"):
                    next_url = urljoin(cur, a_next.get("href"))
            if not next_url:
                # fallback: anchor text / class contains next
                for a in soup.find_all("a", href=True):
                    txt = (a.get_text(" ", strip=True) or "").lower()
                    cls = " ".join(a.get("class") or []).lower()
                    if ("next" in txt) or (txt in {"›", "→", "older"}) or ("next" in cls):
                        cand = urljoin(cur, a.get("href"))
                        cand = _clean_url(cand)
                        if not cand:
                            continue
                        if not _same_site(index_url, cand):
                            continue
                        # heuristically require pagination signal
                        if any(s in cand.lower() for s in ("page=", "paged=", "/page/", "offset=", "start=")):
                            next_url = cand
                            break
        except Exception:
            next_url = None

        if not next_url:
            break

        cur = next_url

    # Deduplicate while preserving order
    seen: set = set()
    dedup: List[Tuple[str, str]] = []
    for u, t in collected:
        key = u.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append((u, t))
        if len(dedup) >= max_links:
            break

    items: List[Item] = []
    for url, title in dedup:
        it = Item(url=url, title=title, summary="", source=source_name or _norm_host(url))

        # date inference (best effort)
        ts = infer_published_ts_from_url(url)
        if ts is not None:
            it.published_ts = ts
            it.published_iso = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            it.published_source = "url"
            it.published_confidence = 0.35
        elif date_budget > 0:
            ts2 = _resolve_published_ts_from_html(url)
            date_budget -= 1
            if ts2 is not None:
                it.published_ts = ts2
                it.published_iso = datetime.fromtimestamp(ts2, tz=timezone.utc).date().isoformat()
                it.published_source = "html"
                it.published_confidence = 0.55

        items.append(it)

    return items


def fetch_full_text(url: str, **kwargs) -> str:
    """
    Return extracted text for a URL (HTML or PDF).
    Returns empty string on failure.

    CHANGE (Feb 2026):
    - Improve fallback HTML extraction by prioritising <article>/<main> (or largest content-like container)
      and incorporating meta descriptions when the extracted text is too short. This reduces the number
      of "Insufficient extract" summaries when the LLM is unavailable.
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

    # fallback: soup-based main-content heuristic
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # capture meta description (helps for JS-heavy pages)
        desc = ""
        try:
            m = soup.find("meta", attrs={"property": "og:description"}) or soup.find("meta", attrs={"name": "description"})
            if m and m.get("content"):
                desc = str(m.get("content")).strip()
        except Exception:
            desc = ""

        # strip common chrome
        for tag in soup.find_all(["header", "footer", "nav", "aside", "form"]):
            try:
                tag.decompose()
            except Exception:
                pass

        candidates = []
        for sel in ["article", "main"]:
            el = soup.find(sel)
            if el is not None:
                candidates.append(el)

        # also consider common content containers
        for el in soup.find_all(attrs={"class": re.compile(r"(content|article|post|entry|body|story|main)", re.I)}):
            candidates.append(el)

        def _text_len(el) -> int:
            try:
                return len((el.get_text(" ", strip=True) or "").strip())
            except Exception:
                return 0

        best = None
        if candidates:
            best = max(candidates, key=_text_len)
            if _text_len(best) < 200:
                best = None

        base = best if best is not None else soup
        txt = base.get_text("\n")
        txt = re.sub(r"\n{3,}", "\n\n", txt).strip()

        if desc and len(txt) < 600:
            title = ""
            try:
                title = (soup.title.string or "").strip() if soup.title else ""
            except Exception:
                title = ""
            prefix = "\n".join([x for x in [title, desc] if x]).strip()
            if prefix:
                txt = (prefix + "\n\n" + txt).strip()

        return txt
    except Exception:
        return ""


