# src/fetch.py
from __future__ import annotations

import os
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import trafilatura
from htmldate import find_date


@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: Optional[float] = None
    summary: str = ""
    text: str = ""


# -------------------- Tunables (env) --------------------

TIMEOUT_FAST = int(os.getenv("TIMEOUT_FAST", "15"))
TIMEOUT_PRIORITY = int(os.getenv("TIMEOUT_PRIORITY", "30"))
TIMEOUT_RSS = int(os.getenv("TIMEOUT_RSS", "25"))

MAX_BYTES = int(os.getenv("MAX_BYTES", str(2_000_000)))

# Runtime bounds for index crawling
MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "250"))
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "75"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "5"))

# Simple in-memory cache (per run). Big win.
FETCH_CACHE_MAX = int(os.getenv("FETCH_CACHE_MAX", "800"))

PRIORITY_DOMAINS = {
    d.strip().lower()
    for d in os.getenv(
        "PRIORITY_DOMAINS",
        "aemo.com.au,arena.gov.au,cefc.com.au,ifrs.org,efrag.org,dcceew.gov.au,ec.europa.eu,commission.europa.eu",
    ).split(",")
    if d.strip()
}

# Browser-like headers to reduce basic bot blocks
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Connection": "keep-alive",
}

# Per-domain CSS selectors (optional; default is "a[href]")
SELECTORS: dict[str, str] = {
    # "aemo.com.au": "a[href]",
}

# -------------------- Helpers --------------------


def _is_http_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def _norm_url(u: str) -> str:
    return (u or "").strip()


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _is_priority_domain(url: str) -> bool:
    dom = _domain(url)
    return any(dom == d or dom.endswith("." + d) for d in PRIORITY_DOMAINS)


def _dedupe_by_url(items: Iterable[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _jina_proxy(url: str) -> str:
    """
    Jina proxy can help around simple bot blocks.
    Treat any proxy error as soft-fail.
    """
    u = url.strip()
    if u.startswith("https://"):
        return "https://r.jina.ai/https://" + u[len("https://") :]
    if u.startswith("http://"):
        return "https://r.jina.ai/http://" + u[len("http://") :]
    return "https://r.jina.ai/https://" + u


def _build_session(retries: int) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=0.7,
        status_forcelist=(403, 408, 429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


# Use fewer retries for fast profile; more for priority profile
_SESSION_FAST = _build_session(int(os.getenv("RETRIES_FAST", "2")))
_SESSION_PRIORITY = _build_session(int(os.getenv("RETRIES_PRIORITY", "4")))

# Simple FIFO-ish cache
_FETCH_CACHE: Dict[str, str] = {}
_FETCH_CACHE_ORDER: List[str] = []


def _cache_get(url: str) -> Optional[str]:
    return _FETCH_CACHE.get(url)


def _cache_put(url: str, text: str) -> None:
    if url in _FETCH_CACHE:
        return
    if len(_FETCH_CACHE_ORDER) >= FETCH_CACHE_MAX:
        old = _FETCH_CACHE_ORDER.pop(0)
        _FETCH_CACHE.pop(old, None)
    _FETCH_CACHE[url] = text
    _FETCH_CACHE_ORDER.append(url)


def _read_limited(resp: requests.Response) -> str:
    resp.encoding = resp.encoding or "utf-8"
    text = resp.text or ""
    if len(text) > MAX_BYTES:
        return text[:MAX_BYTES]
    return text


def fetch_url(url: str, timeout_s: Optional[int] = None) -> str:
    """
    Fetch a URL robustly, with caching:
      1) direct fetch
      2) (if bot/edge codes) try jina proxy
    Never raises; returns "" on failure.
    """
    url = _norm_url(url)
    if not _is_http_url(url):
        return ""

    cached = _cache_get(url)
    if cached is not None:
        return cached

    is_priority = _is_priority_domain(url)
    sess = _SESSION_PRIORITY if is_priority else _SESSION_FAST
    timeout = timeout_s if timeout_s is not None else (TIMEOUT_PRIORITY if is_priority else TIMEOUT_FAST)

    # 1) normal fetch
    try:
        resp = sess.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if 200 <= resp.status_code < 300:
            txt = _read_limited(resp)
            _cache_put(url, txt)
            return txt

        # only attempt proxy for common bot/edge errors
        if resp.status_code not in (403, 408, 429, 500, 502, 503, 504):
            _cache_put(url, "")
            return ""
    except Exception:
        pass

    # 2) proxy fetch (best-effort)
    try:
        proxy_url = _jina_proxy(url)

        cached2 = _cache_get(proxy_url)
        if cached2 is not None:
            _cache_put(url, cached2)
            return cached2

        resp2 = sess.get(proxy_url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if 200 <= resp2.status_code < 300:
            txt2 = _read_limited(resp2)
            _cache_put(proxy_url, txt2)
            _cache_put(url, txt2)
            return txt2

        _cache_put(proxy_url, "")
        _cache_put(url, "")
        return ""
    except Exception:
        _cache_put(url, "")
        return ""


# -------------------- Date extraction --------------------


def _infer_published_ts_from_url(u: str) -> Optional[float]:
    """
    Infer YYYY-MM-DD from URL patterns -> UTC timestamp.
    """
    m = re.search(r"[?&]date=(\d{4})-(\d{2})-(\d{2})\b", u)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})\b", u)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    m = re.search(r"/(\d{4})-(\d{2})-(\d{2})\b", u)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    m = re.search(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)", u)
    if m:
        y, mo, d = map(int, m.groups())
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    return None


def _parse_any_date_to_ts(date_str: str) -> Optional[float]:
    if not date_str:
        return None
    try:
        dt = dtparser.parse(date_str)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    except Exception:
        return None


def _extract_date_from_jsonld(soup: BeautifulSoup) -> Optional[float]:
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (tag.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        candidates = []
        if isinstance(data, dict):
            candidates = [data]
        elif isinstance(data, list):
            candidates = [x for x in data if isinstance(x, dict)]

        for obj in candidates:
            for k in ("datePublished", "dateCreated", "dateModified"):
                v = obj.get(k)
                if isinstance(v, str):
                    ts = _parse_any_date_to_ts(v)
                    if ts:
                        return ts
    return None


def _extract_date_from_meta(soup: BeautifulSoup) -> Optional[float]:
    meta_keys = [
        ("property", "article:published_time"),
        ("property", "og:published_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "publish-date"),
        ("name", "date"),
        ("name", "dc.date"),
        ("name", "dc.date.issued"),
        ("name", "datePublished"),
        ("itemprop", "datePublished"),
        ("itemprop", "dateCreated"),
        ("itemprop", "dateModified"),
    ]
    for attr, key in meta_keys:
        tag = soup.find("meta", attrs={attr: key})
        if tag and tag.get("content"):
            ts = _parse_any_date_to_ts(tag["content"])
            if ts:
                return ts
    return None


def _extract_date_from_time_tag(soup: BeautifulSoup) -> Optional[float]:
    t = soup.find("time")
    if not t:
        return None
    if t.get("datetime"):
        ts = _parse_any_date_to_ts(t["datetime"])
        if ts:
            return ts
    txt = t.get_text(" ", strip=True)
    return _parse_any_date_to_ts(txt)


def _resolve_published_ts_from_article(url: str, html: Optional[str] = None) -> Optional[float]:
    ts = _infer_published_ts_from_url(url)
    if ts:
        return ts

    if html is None:
        html = fetch_url(url)
        if not html:
            return None

    soup = BeautifulSoup(html, "html.parser")

    ts = _extract_date_from_jsonld(soup)
    if ts:
        return ts

    ts = _extract_date_from_meta(soup)
    if ts:
        return ts

    ts = _extract_date_from_time_tag(soup)
    if ts:
        return ts

    try:
        dt_str = find_date(html, extensive_search=True, original_date=True)
        ts = _parse_any_date_to_ts(dt_str or "")
        if ts:
            return ts
    except Exception:
        pass

    return None


def _try_nearby_time_tag_ts(a_tag) -> Optional[float]:
    try:
        parent = a_tag.parent
        for _ in range(4):
            if parent is None:
                break
            t = parent.find("time")
            if t is not None:
                dt_attr = t.get("datetime") or t.get_text(" ", strip=True)
                ts = _parse_any_date_to_ts(dt_attr or "")
                if ts:
                    return ts
            parent = parent.parent
    except Exception:
        pass
    return None


def _find_next_index_page(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    a = soup.select_one('a[rel="next"][href]')
    if a and a.get("href"):
        nxt = urljoin(base_url, a.get("href"))
        if _is_http_url(nxt):
            return nxt

    for cand in soup.select("a[href]"):
        txt = (cand.get_text(" ", strip=True) or "").lower()
        if txt in ("next", "older", "›", "»", ">"):
            href = _norm_url(cand.get("href") or "")
            if not href:
                continue
            nxt = urljoin(base_url, href)
            if _is_http_url(nxt):
                return nxt

    a2 = soup.select_one('a[href].next, a[href][class*="next"], a[href][aria-label*="Next"]')
    if a2 and a2.get("href"):
        nxt = urljoin(base_url, a2.get("href"))
        if _is_http_url(nxt):
            return nxt

    return None


def _looks_like_article(url: str) -> bool:
    """
    Cheap heuristic to avoid resolving dates for obvious hub pages.
    """
    p = urlparse(url)
    path = (p.path or "").lower().strip("/")
    if not path:
        return False
    # lots of CMS articles have a year or slug depth
    if re.search(r"/\d{4}/\d{2}/\d{2}/", p.path):
        return True
    if len(path.split("/")) >= 2:
        return True
    # avoid obvious hubs
    if any(k in path for k in ("tag/", "category/", "topics/", "search", "events", "calendar")):
        return False
    return True


# -------------------- Public API used by generator --------------------


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    label = (source_name or "").strip() or url
    # feedparser does its own fetching; we accept it, but keep it bounded
    feed = feedparser.parse(url)
    out: list[Item] = []

    for e in feed.entries or []:
        link = _norm_url(getattr(e, "link", "") or "")
        if not _is_http_url(link):
            continue
        title = (getattr(e, "title", "") or "").strip()
        if not title:
            continue

        ts: Optional[float] = None
        for k in ("published", "updated", "created"):
            v = getattr(e, k, None)
            if isinstance(v, str):
                ts = _parse_any_date_to_ts(v)
                if ts:
                    break

        summary = (getattr(e, "summary", "") or "").strip()
        summary = re.sub(r"\s+", " ", summary).strip()

        out.append(Item(title=title, url=link, source=label, published_ts=ts, summary=summary))

    return _dedupe_by_url(out)


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    HTML index crawler:
    - bounded pagination
    - bounded date resolution (fetch article HTML only for items missing ts)
    - skips obvious non-articles for date resolution
    """
    label = (source_name or "").strip() or url
    candidates: list[Item] = []
    seen: set[str] = set()

    dom = _domain(url)
    selector = SELECTORS.get(dom, "a[href]")

    page_url = url
    visited_pages: set[str] = set()

    for _page in range(MAX_INDEX_PAGES):
        if page_url in visited_pages:
            break
        visited_pages.add(page_url)

        html = fetch_url(page_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select(selector):
            href = _norm_url(a.get("href") or "")
            if not href:
                continue
            abs_u = urljoin(page_url, href)
            if not _is_http_url(abs_u):
                continue

            low = abs_u.lower()
            if any(low.endswith(ext) for ext in (".pdf", ".jpg", ".jpeg", ".png", ".zip")):
                continue

            title = a.get_text(" ", strip=True) or ""
            title = re.sub(r"\s+", " ", title).strip()
            if not title:
                continue

            if abs_u in seen:
                continue
            seen.add(abs_u)

            ts = _infer_published_ts_from_url(abs_u)
            if ts is None:
                ts = _try_nearby_time_tag_ts(a)

            candidates.append(Item(title=title, url=abs_u, source=label, published_ts=ts))

            if len(candidates) >= MAX_LINKS_PER_INDEX:
                break

        if len(candidates) >= MAX_LINKS_PER_INDEX:
            break

        nxt = _find_next_index_page(soup, page_url)
        if not nxt:
            break
        page_url = nxt

    resolve_budget = MAX_DATE_RESOLVE_FETCHES_PER_INDEX
    for it in candidates:
        if resolve_budget <= 0:
            break
        if it.published_ts is not None:
            continue
        if not _looks_like_article(it.url):
            continue

        html = fetch_url(it.url)
        resolve_budget -= 1
        if not html:
            continue

        ts = _resolve_published_ts_from_article(it.url, html=html)
        if ts is not None:
            it.published_ts = ts

    return _dedupe_by_url(candidates)


def fetch_full_text(url: str, timeout_s: Optional[int] = None) -> str:
    """
    Returns extracted main article text (best-effort).
    Uses cached fetch_url().
    """
    # Use profile timeout by default
    if timeout_s is None:
        timeout_s = TIMEOUT_PRIORITY if _is_priority_domain(url) else TIMEOUT_FAST

    html = fetch_url(url, timeout_s=timeout_s)
    if not html:
        return ""

    try:
        extracted = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
        )
        if extracted:
            extracted = re.sub(r"\n{3,}", "\n\n", extracted).strip()
            if extracted:
                return extracted
    except Exception:
        pass

    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text("\n", strip=True)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


# Optional alias
fetch_fulltext = fetch_full_text
