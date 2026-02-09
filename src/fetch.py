# src/fetch.py
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import trafilatura
from htmldate import find_date

# ----------------------------- Model -----------------------------

@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: Optional[float] = None
    summary: str = ""


# ----------------------------- HTTP -----------------------------

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "Connection": "keep-alive",
}

TIMEOUT = 35
MAX_BYTES = 700_000  # cap downloads to keep CI stable

# Env-configurable budgets (so monthly.yml can tune without code changes)
MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "300"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "6"))
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "80"))


def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=32, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = _make_session()


def _read_limited(resp: requests.Response, max_bytes: int = MAX_BYTES) -> str:
    resp.raise_for_status()
    b = resp.content[:max_bytes]
    enc = resp.encoding or "utf-8"
    try:
        return b.decode(enc, errors="replace")
    except Exception:
        return b.decode("utf-8", errors="replace")


def _is_http_url(u: str) -> bool:
    return u.startswith("http://") or u.startswith("https://")


def fetch_url(url: str, timeout_s: int = TIMEOUT) -> str:
    """
    Fetch URL with a browser-like header set.
    If direct fetch fails, fall back to r.jina.ai proxy (helps with some 403s).
    """
    try:
        r = SESSION.get(url, headers=DEFAULT_HEADERS, timeout=timeout_s)
        return _read_limited(r)
    except Exception:
        # Jina proxy (best-effort)
        proxied = f"https://r.jina.ai/http://{url.lstrip('https://').lstrip('http://')}"
        r2 = SESSION.get(proxied, headers=DEFAULT_HEADERS, timeout=timeout_s)
        return _read_limited(r2)


# ----------------------------- Dates -----------------------------

def _to_ts(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _try_parse_dt(s: str) -> Optional[datetime]:
    try:
        dt = dtparser.parse(s)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _infer_published_ts_from_url(url: str) -> Optional[float]:
    # /YYYY/MM/DD/ or /YYYY-MM-DD/
    m = re.search(r"/(20\d{2})[/-](\d{1,2})[/-](\d{1,2})(?:/|$)", url)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return _to_ts(datetime(y, mo, d, tzinfo=timezone.utc))
        except Exception:
            return None

    m = re.search(r"/(20\d{2})-(\d{1,2})-(\d{1,2})(?:/|$)", url)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return _to_ts(datetime(y, mo, d, tzinfo=timezone.utc))
        except Exception:
            return None

    # /YYYY/MM/ (month only)
    m = re.search(r"/(20\d{2})[/-](\d{1,2})(?:/|$)", url)
    if m:
        y, mo = map(int, m.groups())
        try:
            return _to_ts(datetime(y, mo, 1, tzinfo=timezone.utc))
        except Exception:
            return None

    return None


def _try_nearby_time_tag_ts(a_tag) -> Optional[float]:
    """
    Look for a nearby <time> tag within a few ancestor hops.
    """
    try:
        parent = a_tag.parent
        for _ in range(4):
            if parent is None:
                break
            t = parent.find("time")
            if t is not None:
                dt_raw = t.get("datetime") or t.get_text(" ", strip=True)
                dt = _try_parse_dt(dt_raw or "")
                if dt is not None:
                    return _to_ts(dt)
            parent = parent.parent
    except Exception:
        pass
    return None


def _resolve_published_ts_from_article(url: str) -> Optional[float]:
    """
    Fetch article HTML and extract a date using htmldate, falling back to meta tags.
    """
    try:
        html = fetch_url(url)
    except Exception:
        return None

    # htmldate (good coverage across publishers)
    try:
        ds = find_date(html, extensive_search=True, original_date=True)
        if ds:
            dt = _try_parse_dt(ds)
            if dt is not None:
                return _to_ts(dt)
    except Exception:
        pass

    soup = BeautifulSoup(html, "html.parser")
    meta_keys = (
        ("property", "article:published_time"),
        ("name", "article:published_time"),
        ("property", "og:published_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "timestamp"),
        ("name", "date"),
        ("name", "dc.date"),
        ("name", "dc.date.issued"),
        ("name", "datePublished"),
    )
    for attr, key in meta_keys:
        m = soup.find("meta", attrs={attr: key})
        if m and m.get("content"):
            dt = _try_parse_dt(m.get("content") or "")
            if dt is not None:
                return _to_ts(dt)

    return None


# ----------------------------- RSS -----------------------------

def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    label = (source_name or "").strip() or url
    parsed = feedparser.parse(url)
    items: list[Item] = []

    for e in (parsed.entries or [])[:250]:
        link = (getattr(e, "link", "") or "").strip()
        if not link or not _is_http_url(link):
            continue

        title = (getattr(e, "title", "") or "").strip()
        if not title:
            continue

        # summary (helps if full text extraction is short)
        summary = (getattr(e, "summary", "") or "").strip()
        summary = re.sub(r"\s+", " ", summary).strip()

        ts = None
        for field in ("published", "updated", "created"):
            if hasattr(e, field):
                dt = _try_parse_dt(getattr(e, field))
                if dt is not None:
                    ts = _to_ts(dt)
                    break

        items.append(Item(title=title, url=link, source=label, published_ts=ts, summary=summary))

    # dedupe by url
    out: list[Item] = []
    seen: set[str] = set()
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


# ----------------------------- HTML index scraping (with pagination) -----------------------------

def _domain(url: str) -> str:
    return urlparse(url).netloc.lower().lstrip("www.")


# You can tune selectors per domain if needed; default is a[href]
SELECTORS: dict[str, str] = {
    "aer.gov.au": "a[href]",
    "dcceew.gov.au": "a[href]",
    "infrastructureaustralia.gov.au": "a[href]",
    "iea.org": "a[href]",
    "irena.org": "a[href]",
    "efrag.org": "a[href]",
}


def _find_next_index_page(soup: BeautifulSoup, base_url: str) -> str | None:
    """
    Best-effort discovery of a "next" page for listing indexes.
    Supports rel="next" and common next link labels.
    """
    a = soup.select_one('a[rel="next"][href]')
    if a and a.get("href"):
        nxt = urljoin(base_url, a.get("href"))
        if _is_http_url(nxt):
            return nxt

    for cand in soup.select("a[href]"):
        txt = (cand.get_text(" ", strip=True) or "").lower()
        if txt in ("next", "older", "›", "»", ">"):
            href = (cand.get("href") or "").strip()
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


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Scrape candidate article links from an HTML index page and a few subsequent pages.
    - extracts anchor text as title
    - attempts to infer/resolve published_ts
    - returns deduped list of Items
    """
    label = (source_name or "").strip() or url

    dom = _domain(url)
    selector = SELECTORS.get(dom, "a[href]")

    candidates: list[Item] = []
    seen_urls: set[str] = set()
    visited_pages: set[str] = set()

    page_url = url
    for _ in range(MAX_INDEX_PAGES):
        if page_url in visited_pages:
            break
        visited_pages.add(page_url)

        html = fetch_url(page_url)
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select(selector):
            href = (a.get("href") or "").strip()
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

            if abs_u in seen_urls:
                continue
            seen_urls.add(abs_u)

            ts = _infer_published_ts_from_url(abs_u)
            if ts is None:
                ts = _try_nearby_time_tag_ts(a)

            candidates.append(Item(title=title, url=abs_u, source=label, published_ts=ts, summary=""))

            if len(candidates) >= MAX_LINKS_PER_INDEX:
                break

        if len(candidates) >= MAX_LINKS_PER_INDEX:
            break

        nxt = _find_next_index_page(soup, page_url)
        if not nxt:
            break
        page_url = nxt

    # Resolve missing dates by fetching a limited number of articles
    budget = MAX_DATE_RESOLVE_FETCHES_PER_INDEX
    for it in candidates:
        if budget <= 0:
            break
        if it.published_ts is not None:
            continue
        ts = _resolve_published_ts_from_article(it.url)
        if ts is not None:
            it.published_ts = ts
        budget -= 1
        time.sleep(0.10)

    # dedupe (already deduped, but keep stable)
    out: list[Item] = []
    seen2: set[str] = set()
    for it in candidates:
        if it.url in seen2:
            continue
        seen2.add(it.url)
        out.append(it)
    return out


# ----------------------------- Full text extraction (API expected by generate_monthly.py) -----------------------------

def fetch_full_text(url: str) -> str:
    """
    API required by src.generate_monthly:
      txt = fetch_full_text(it.url)

    Returns main article text (best-effort) using trafilatura, falling back to BeautifulSoup.
    """
    # trafilatura preferred
    try:
        downloaded = trafilatura.fetch_url(url, timeout=TIMEOUT)
        if downloaded:
            text = trafilatura.extract(downloaded) or ""
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                return text
    except Exception:
        pass

    # fallback: requests + soup
    try:
        html = fetch_url(url)
        soup = BeautifulSoup(html, "html.parser")
        # remove obvious non-content
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        return text
    except Exception:
        return ""
