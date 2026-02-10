# src/fetch.py
from __future__ import annotations

import os
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# trafilatura is already in your requirements
import trafilatura

# htmldate comes via trafilatura deps (you already install it in CI)
from htmldate import find_date


@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: Optional[float] = None
    summary: str = ""
    text: str = ""


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

TIMEOUT = 35
MAX_BYTES = 2_000_000  # protect against huge responses

# Per-domain CSS selectors (optional; default is "a[href]")
SELECTORS: dict[str, str] = {
    # "aemo.com.au": "a[href]",
}

# Limits to keep runtime bounded (env-configurable)
MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "300"))
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "80"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "6"))


def _now_ts() -> float:
    return time.time()


def _is_http_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def _norm_url(u: str) -> str:
    return (u or "").strip()


def _dedupe_by_url(items: Iterable[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _jina_proxy(url: str) -> str:
    """
    Jina AI proxy is often effective against basic bot blocks (403) and some CDNs.
    It returns a text-rendered version of the page.
    """
    u = url.strip()
    if u.startswith("https://"):
        return "https://r.jina.ai/https://" + u[len("https://") :]
    if u.startswith("http://"):
        return "https://r.jina.ai/http://" + u[len("http://") :]
    return "https://r.jina.ai/https://" + u


def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.8,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


_SESSION = _build_session()


def _read_limited(resp: requests.Response, max_bytes: int = MAX_BYTES) -> str:
    resp.encoding = resp.encoding or "utf-8"
    text = resp.text
    if len(text) > max_bytes:
        return text[:max_bytes]
    return text


def fetch_url(url: str, timeout_s: int = TIMEOUT) -> str:
    """
    Fetch a URL robustly:
      1) normal request
      2) if that fails, try Jina proxy
    NEVER raise on proxy failure; return empty string so callers can drop the item.
    """
    url = _norm_url(url)
    if not _is_http_url(url):
        return ""

    # 1) Normal fetch
    try:
        resp = _SESSION.get(url, headers=HEADERS, timeout=timeout_s, allow_redirects=True)
        if 200 <= resp.status_code < 300:
            return _read_limited(resp)

        # for non-2xx: only attempt proxy for common bot/edge cases
        if resp.status_code not in (403, 408, 429, 500, 502, 503, 504):
            # don't explode the whole run on 404/410/etc.
            return ""
    except Exception:
        # proceed to proxy attempt
        pass

    # 2) Proxy fetch (best-effort)
    try:
        proxy_url = _jina_proxy(url)
        resp2 = _SESSION.get(proxy_url, headers=HEADERS, timeout=timeout_s, allow_redirects=True)

        if 200 <= resp2.status_code < 300:
            return _read_limited(resp2)

        # If proxy returns 4xx/5xx (incl. 422), treat as a soft failure
        return ""
    except Exception:
        return ""



def _infer_published_ts_from_url(u: str) -> Optional[float]:
    """
    Heuristics to infer YYYY-MM-DD from URL patterns, returning UTC timestamp.
    Supports:
      - /YYYY/MM/DD/
      - /YYYY-MM-DD/
      - YYYYMMDD anywhere
      - ?date=YYYY-MM-DD
    """
    # query param date=YYYY-MM-DD
    m = re.search(r"[?&]date=(\d{4})-(\d{2})-(\d{2})\b", u)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # /YYYY/MM/DD/
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})\b", u)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # /YYYY-MM-DD/
    m = re.search(r"/(\d{4})-(\d{2})-(\d{2})\b", u)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # YYYYMMDD (avoid matching long numeric IDs by requiring non-digit boundary)
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
    # Look for JSON-LD datePublished / dateCreated
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (tag.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        # JSON-LD can be dict or list
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
    # Common meta tags
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
    # Prefer datetime attribute
    if t.get("datetime"):
        ts = _parse_any_date_to_ts(t["datetime"])
        if ts:
            return ts
    # Fallback text
    txt = t.get_text(" ", strip=True)
    return _parse_any_date_to_ts(txt)


def _resolve_published_ts_from_article(url: str, html: Optional[str] = None) -> Optional[float]:
    """
    Resolve publish timestamp for an article URL by fetching HTML (if not provided)
    and checking (in order):
      - URL heuristics
      - JSON-LD datePublished/dateCreated/dateModified
      - Meta tags
      - <time> tag
      - htmldate find_date()
    """
    # URL heuristic first
    ts = _infer_published_ts_from_url(url)
    if ts:
        return ts

    if html is None:
        try:
            html = fetch_url(url)
        except Exception:
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

    # htmldate last (can be slower)
    try:
        dt_str = find_date(html, extensive_search=True, original_date=True)
        ts = _parse_any_date_to_ts(dt_str or "")
        if ts:
            return ts
    except Exception:
        pass

    return None


def _try_nearby_time_tag_ts(a_tag) -> Optional[float]:
    """
    Try to pick up <time datetime="..."> near the link element (within a few ancestor hops).
    """
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
    """
    Best-effort discovery of the next page for listing/index pages.
    Supports rel="next" and common "Next/Older/›/»" anchor labels.
    """
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


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Parse RSS/Atom via feedparser.
    """
    label = (source_name or "").strip() or url
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
    Fetch items from an HTML index page:
    - extracts candidate links with titles
    - tries to infer publish date from URL or nearby <time>
    - follows pagination (best-effort) to avoid missing an entire month
    - for items still missing dates, fetches article HTML (budgeted) and extracts publish date
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

    # Second pass: resolve missing dates by fetching the article (budgeted)
    resolve_budget = MAX_DATE_RESOLVE_FETCHES_PER_INDEX
    for it in candidates:
        if resolve_budget <= 0:
            break
        if it.published_ts is not None:
            continue
        try:
            html = fetch_url(it.url)
        except Exception:
            resolve_budget -= 1
            continue

        ts = _resolve_published_ts_from_article(it.url, html=html)
        if ts is not None:
            it.published_ts = ts
        resolve_budget -= 1

    return _dedupe_by_url(candidates)

# --- Compatibility API expected by src.generate_monthly ---------------------------------

def fetch_full_text(url: str, timeout_s: int = TIMEOUT) -> str:
    """
    API required by src.generate_monthly:
      txt = fetch_full_text(it.url)

    Returns extracted main article text (best-effort).
    """
    html = fetch_url(url, timeout_s=timeout_s)
    if not html:
        return ""

    # Prefer trafilatura extraction
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

    # Fallback: strip HTML
    soup = BeautifulSoup(html, "html.parser")
    txt = soup.get_text("\n", strip=True)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt


# Optional alias if any older code uses a different name
fetch_fulltext = fetch_full_text
