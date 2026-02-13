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

# -----------------------------
# Indirect / wrapper URL resolution
# -----------------------------
_GOOGLE_NEWS_HOSTS = {"news.google.com"}
_GOOGLE_REDIRECT_HOSTS = {"www.google.com", "google.com"}

def _resolve_google_redirect(u: str) -> str:
    """Resolve classic Google redirect URLs (e.g., https://www.google.com/url?url=...)."""
    try:
        p = urlparse(u)
        if _norm_host(p.netloc) not in _GOOGLE_REDIRECT_HOSTS:
            return u
        if not p.path.startswith("/url"):
            return u
        q = parse_qs(p.query)
        cand = (q.get("url") or q.get("q") or [""])[0]
        return cand or u
    except Exception:
        return u

def resolve_indirect_url(u: str) -> str:
    """
    Attempt to unwrap known aggregator/wrapper URLs to their canonical target.
    Currently supports Google redirect URLs and Google News article wrappers.
    Returns the original URL on failure.
    """
    u = (u or "").strip()
    if not u:
        return u

    u2 = _resolve_google_redirect(u)
    if u2 and u2 != u:
        return _clean_url(u2)

    try:
        p = urlparse(u)
        host = _norm_host(p.netloc)
        if host in _GOOGLE_NEWS_HOSTS and (p.path.startswith("/rss/articles") or p.path.startswith("/articles")):
            # Fetch the wrapper page and try to extract a canonical external URL
            resp = _http_get(u)
            if resp is None:
                return u
            raw = _read_limited(resp, min(MAX_BYTES, 800_000))
            if not raw:
                return u
            try:
                html = raw.decode(resp.encoding or "utf-8", errors="replace")
            except Exception:
                html = raw.decode("utf-8", errors="replace")
            soup = BeautifulSoup(html, "html.parser")

            # Prefer explicit canonical/og:url
            for sel in [
                ("meta", {"property": "og:url"}),
                ("meta", {"name": "og:url"}),
            ]:
                tag = soup.find(sel[0], sel[1])
                if tag and tag.get("content"):
                    cand = tag.get("content", "").strip()
                    if cand and "news.google.com" not in cand:
                        return _clean_url(cand)

            link = soup.find("link", {"rel": "canonical"})
            if link and link.get("href"):
                cand = link.get("href", "").strip()
                if cand and "news.google.com" not in cand:
                    return _clean_url(cand)

            # Heuristic: first external https link
            for a in soup.find_all("a", href=True):
                href = a.get("href", "").strip()
                if not href.startswith("http"):
                    continue
                hhost = _norm_host(urlparse(href).netloc)
                if not hhost or hhost in _GOOGLE_NEWS_HOSTS or hhost in _GOOGLE_REDIRECT_HOSTS:
                    continue
                if any(bad in href.lower() for bad in ["facebook.com", "twitter.com", "x.com", "linkedin.com"]):
                    continue
                return _clean_url(href)

            # Regex fallback: look for url=https%3A%2F%2F... patterns
            m = re.search(r"url=(https?%3A%2F%2F[^&\"'>]+)", html)
            if m:
                cand = unquote(m.group(1))
                if cand and cand.startswith("http") and "news.google.com" not in cand:
                    return _clean_url(cand)
    except Exception:
        return u

    return u


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




def _find_date_in_text(text: str) -> Optional[datetime]:
    """Last-resort date extraction from visible text.

    Looks for common patterns like:
    - 29 January 2026 / 29 Jan 2026
    - January 29, 2026
    - 2026-01-29
    Returns a UTC datetime (midnight) on success.
    """
    t = (text or "").strip()
    if not t:
        return None
    # collapse whitespace and keep it bounded
    t = re.sub(r"\s+", " ", t)
    t = t[:5000]

    # ISO yyyy-mm-dd
    m = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b", t)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except Exception:
            pass

    # 29 January 2026 / 29 Jan 2026
    m = re.search(r"\b(0?[1-9]|[12]\d|3[01])\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(20\d{2})\b", t, re.I)
    if m:
        d = int(m.group(1))
        mon = m.group(2).lower()
        y = int(m.group(3))
        mon_key = mon if mon in _MONTHS else mon[:3]
        mo = _MONTHS.get(mon_key)
        if mo:
            try:
                return datetime(y, mo, d, tzinfo=timezone.utc)
            except Exception:
                pass

    # January 29, 2026
    m = re.search(r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(0?[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?[,]?\s+(20\d{2})\b", t, re.I)
    if m:
        mon = m.group(1).lower()
        d = int(m.group(2))
        y = int(m.group(3))
        mon_key = mon if mon in _MONTHS else mon[:3]
        mo = _MONTHS.get(mon_key)
        if mo:
            try:
                return datetime(y, mo, d, tzinfo=timezone.utc)
            except Exception:
                pass

    return None
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

    # monthname-YYYY or YYYY-monthname in slug (e.g., .../issb-update-january-2026/)
    m = re.search(r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[-_ ]?(20\d{2})", path)
    if m:
        y = int(m.group(2))
        mo = _MONTHS.get(m.group(1).lower()) or _MONTHS.get(m.group(1)[:3].lower())
        if mo:
            try:
                return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()
            except Exception:
                return None

    m = re.search(r"(20\d{2})[-_ ]?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)", path)
    if m:
        y = int(m.group(1))
        mo = _MONTHS.get(m.group(2).lower()) or _MONTHS.get(m.group(2)[:3].lower())
        if mo:
            try:
                return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()
            except Exception:
                return None

    # YYYYMM (e.g., .../202601/...)
    m = re.search(r"/(20\d{2})(0[1-9]|1[0-2])(?:/|$)", path)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
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

    # facet/filter query params (common on EFRAG and other CMS listings)
    if any(k.startswith("f[") for k in q.keys()):
        return True
    if any(k in {"category", "categories", "topic", "topics", "tag", "tags", "type", "types", "filter"} for k in q.keys()):
        return True
    if "page" in q and (parsed.path.endswith("/news") or parsed.path.endswith("/news/")):
        return True
    if "s" in q or "q" in q and parsed.path.endswith("/search"):
        return True

    path = parsed.path or "/"
    # nav/utility endpoints
    utility_segments = {
        "about", "contact", "privacy", "terms", "cookies", "accessibility", "sitemap",
        "careers", "jobs", "vacancies", "pressroom", "newsroom", "media-releases", "media-release", "media-centre", "media-center", "news-centre", "news-center", "press-releases", "press-release", "announcements", "statement", "statements",
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
        "/media-releases", "/media-release", "/press-releases", "/press-release", "/newsroom", "/pressroom", "/announcements",
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
    """Parse a date/datetime string in common web formats (best-effort, UTC)."""
    if not s:
        return None
    ss = str(s).strip()
    if not ss:
        return None
    ss = ss.replace("Z", "+00:00")
    ss = re.sub(r"(\.\d{3,6})\+00:00$", "+00:00", ss)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ss):
        try:
            return datetime.fromisoformat(ss).replace(tzinfo=timezone.utc)
        except Exception:
            return None
    try:
        dt = datetime.fromisoformat(ss)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", ss)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
            except Exception:
                return None
    return None


def _resolve_published_ts_from_html(url: str) -> Optional[float]:
    """Fetch a small slice of HTML and extract published date from meta/time/JSON-LD."""
    resp = _http_get(url)
    if resp is None:
        return None
    ctype = _content_type(resp)
    raw = _read_limited(resp, min(MAX_BYTES, 400_000))
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

    meta_attrs = [
        {"property": "article:published_time"},
        {"name": "article:published_time"},
        {"name": "pubdate"},
        {"name": "publishdate"},
        {"name": "date"},
        {"name": "dc.date"},
        {"name": "dc.date.issued"},
        {"property": "og:updated_time"},
    ]
    for attrs in meta_attrs:
        el = soup.find("meta", attrs=attrs)
        if el and el.get("content"):
            dt = _parse_dt_like(el.get("content"))
            if dt:
                return dt.timestamp()

    t = soup.find("time")
    if t is not None:
        dtattr = t.get("datetime") or ""
        dt = _parse_dt_like(dtattr)
        if dt:
            return dt.timestamp()

    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            txt = (s.string or "").strip()
            if not txt:
                continue
            data = json.loads(txt)
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                for key in ("datePublished", "dateCreated"):
                    if key in obj:
                        dt = _parse_dt_like(obj.get(key))
                        if dt:
                            return dt.timestamp()
        except Exception:
            continue

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
    """Fetch a hub/listing HTML page (optionally paginated) and extract likely content links."""
    index_url = _clean_url(index_url)
    if not index_url:
        return []

    title_by_url: Dict[str, str] = {}
    ordered: List[str] = []

    next_url: Optional[str] = index_url
    pages_seen: set[str] = set()

    for page_no in range(max(1, MAX_INDEX_PAGES)):
        if not next_url:
            break
        nu = _clean_url(next_url)
        if not nu or nu.lower() in pages_seen:
            break
        pages_seen.add(nu.lower())

        resp = _http_get(nu)
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

        # Drop obvious boilerplate containers to reduce navigation links
        try:
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            _strip_nav_blocks(soup)
        except Exception:
            pass

        # 1) JSON-LD ItemList (some sites hide links this way)
        try:
            for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
                try:
                    data = json.loads(s.get_text(" ", strip=True) or "{}")
                except Exception:
                    continue
                objs = data if isinstance(data, list) else [data]
                for obj in objs:
                    if not isinstance(obj, dict):
                        continue
                    if str(obj.get("@type") or "").lower() != "itemlist":
                        continue
                    elems = obj.get("itemListElement") or []
                    if not isinstance(elems, list):
                        continue
                    for el in elems:
                        if isinstance(el, dict):
                            u = el.get("url") or (el.get("item") or {}).get("@id") if isinstance(el.get("item"), dict) else None
                            if u:
                                abs_url = _clean_url(urljoin(nu, str(u)))
                                if abs_url.lower().startswith(("http://", "https://")):
                                    ordered.append(abs_url)
        except Exception:
            pass

        # 2) Anchor harvest
        for a in soup.find_all("a"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("#") or href.lower().startswith(("mailto:", "javascript:")):
                continue

            abs_url = _clean_url(urljoin(nu, href))
            if not abs_url.lower().startswith(("http://", "https://")):
                continue

            if _deny_from_index(abs_url):
                continue
            if is_probably_taxonomy_or_hub(abs_url):
                continue
            if (not ALLOW_EXTERNAL_LINKS_FROM_INDEX) and (not _same_site(abs_url, index_url)):
                continue

            t = _best_anchor_title(a)
            if not t:
                continue

            if abs_url not in title_by_url or (len(t) > len(title_by_url.get(abs_url, ""))):
                title_by_url[abs_url] = t

            ordered.append(abs_url)

        # Find "next" page link (rel=next or common pagination patterns)
        next_url_found: Optional[str] = None
        try:
            lnk = soup.find("link", attrs={"rel": re.compile(r"\bnext\b", re.I)})
            if lnk and lnk.get("href"):
                next_url_found = _clean_url(urljoin(nu, lnk.get("href")))
            if not next_url_found:
                a_next = soup.find("a", attrs={"rel": re.compile(r"\bnext\b", re.I)})
                if a_next and a_next.get("href"):
                    next_url_found = _clean_url(urljoin(nu, a_next.get("href")))
            if not next_url_found:
                # heuristic: anchor text "Next"
                for a_next in soup.find_all("a"):
                    txt = (a_next.get_text(" ", strip=True) or "").strip().lower()
                    if txt in {"next", "next ›", "older", "older posts"} and a_next.get("href"):
                        next_url_found = _clean_url(urljoin(nu, a_next.get("href")))
                        break
        except Exception:
            next_url_found = None

        next_url = next_url_found
        # stop if we've hit cap already
        if len(ordered) >= MAX_LINKS_PER_INDEX:
            break

    # De-duplicate in-order
    uniq: List[str] = []
    seen: set[str] = set()
    for u in ordered:
        ul = (u or "").lower()
        if not ul or ul in seen:
            continue
        seen.add(ul)
        uniq.append(u)

    uniq = uniq[:MAX_LINKS_PER_INDEX]

    items: List[Item] = []
    resolve_budget = MAX_DATE_RESOLVE_FETCHES_PER_INDEX

    # Prioritise higher-signal URLs for date resolution
    def _prio(u: str) -> int:
        p = urlparse(u).path.lower()
        if re.search(r"/20\d{2}/", p) or re.search(r"/20\d{2}-\d{2}-\d{2}", p):
            return 0
        if any(seg in p for seg in ("/news/", "/media/", "/press/", "/blog/", "/insights/", "/updates/", "/publication", "/publications/")):
            return 1
        return 2

    uniq_sorted = sorted(uniq, key=_prio)
    resolved_ts: Dict[str, Tuple[Optional[float], bool]] = {}

    for u in uniq_sorted:
        inferred_ts = infer_published_ts_from_url(u)
        used_meta = False
        if inferred_ts is None and resolve_budget > 0 and _looks_content_url(u):
            try:
                ts2 = _resolve_published_ts_from_html(u)
            except Exception:
                ts2 = None
            resolve_budget -= 1
            if ts2:
                inferred_ts = ts2
                used_meta = True
        resolved_ts[u] = (inferred_ts, used_meta)

    for u in uniq:
        inferred_ts, used_meta = resolved_ts.get(u, (infer_published_ts_from_url(u), False))
        inferred_iso = datetime.fromtimestamp(inferred_ts, tz=timezone.utc).date().isoformat() if inferred_ts else None

        title = title_by_url.get(u, "") or u

        src = source_name or _norm_host(urlparse(index_url).netloc)
        if not _same_site(u, index_url):
            src = _norm_host(urlparse(u).netloc)

        items.append(
            Item(
                url=u,
                title=title,
                summary="",
                source=src,
                published_iso=inferred_iso,
                published_ts=inferred_ts,
                published_source=("meta" if used_meta else ("url" if inferred_ts else None)),
                published_confidence=(0.6 if used_meta else (0.35 if inferred_ts else None)),
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
    # Unwrap known aggregator URLs (e.g., Google News RSS wrappers)
    url = resolve_indirect_url(url)
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
