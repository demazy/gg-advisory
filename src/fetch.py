# src/fetch.py
from __future__ import annotations

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

# Limits to keep runtime bounded
MAX_LINKS_PER_INDEX = 120
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = 40


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
    Fetch a URL robustly.
    - Normal fetch first
    - On 403/429 (or certain failures), attempt Jina proxy
    """
    url = _norm_url(url)
    if not _is_http_url(url):
        raise ValueError(f"Not a valid http(s) URL: {url}")

    # 1) Normal fetch
    try:
        resp = _SESSION.get(url, headers=HEADERS, timeout=timeout_s, allow_redirects=True)
        if resp.status_code >= 200 and resp.status_code < 300:
            return _read_limited(resp)
        # fall through to proxy on 403/429, otherwise raise
        if resp.status_code not in (403, 429):
            resp.raise_for_status()
    except Exception:
        # fall through to proxy
        pass

    # 2) Proxy fetch (best-effort)
    proxy_url = _jina_proxy(url)
    resp2 = _SESSION.get(proxy_url, headers=HEADERS, timeout=timeout_s, allow_redirects=True)
    resp2.raise_for_status()
    return _read_limited(resp2)


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
    and checking JSON-LD, meta tags, <time>, and finally htmldate.
    """
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

    # last resort: htmldate heuristics
    try:
        d = find_date(html, url=url)
        ts = _parse_any_date_to_ts(d) if d else None
        return ts
    except Exception:
        return None


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch items from an RSS/Atom feed.
    Accepts source_name for compatibility with generate_monthly.
    """
    label = (source_name or "").strip() or url
    feed = feedparser.parse(url)

    items: list[Item] = []
    for entry in getattr(feed, "entries", []) or []:
        link = _norm_url(getattr(entry, "link", "") or "")
        title = (getattr(entry, "title", "") or "").strip()
        summary = (getattr(entry, "summary", "") or "").strip()

        ts: Optional[float] = None
        if getattr(entry, "published_parsed", None):
            try:
                ts = time.mktime(entry.published_parsed)
            except Exception:
                ts = None
        if ts is None and getattr(entry, "updated_parsed", None):
            try:
                ts = time.mktime(entry.updated_parsed)
            except Exception:
                ts = None
        if ts is None:
            ts = _infer_published_ts_from_url(link)

        if not link or not title:
            continue

        items.append(Item(title=title, url=link, source=label, published_ts=ts, summary=summary))

    return _dedupe_by_url(items)


def _extract_links_from_index(index_url: str, html: str) -> list[tuple[str, str]]:
    """
    Return [(title, absolute_url)] from an index HTML page.
    """
    soup = BeautifulSoup(html, "html.parser")
    dom = _domain(index_url)
    selector = SELECTORS.get(dom, "a[href]")

    out: list[tuple[str, str]] = []
    for a in soup.select(selector):
        href = _norm_url(a.get("href") or "")
        if not href:
            continue
        abs_u = urljoin(index_url, href)
        if not _is_http_url(abs_u):
            continue

        # Avoid junk
        low = abs_u.lower()
        if any(low.endswith(ext) for ext in (".pdf", ".jpg", ".jpeg", ".png", ".zip")):
            continue

        title = a.get_text(" ", strip=True) or ""
        title = re.sub(r"\s+", " ", title).strip()

        # If anchor text is empty, skip
        if not title:
            continue

        out.append((title, abs_u))

    # Dedupe while keeping order
    seen = set()
    deduped: list[tuple[str, str]] = []
    for t, u in out:
        if u in seen:
            continue
        seen.add(u)
        deduped.append((t, u))

    return deduped[:MAX_LINKS_PER_INDEX]


def _try_nearby_time_tag_ts(a_tag) -> Optional[float]:
    """
    Given a BeautifulSoup <a> tag, try to find a nearby <time> tag with a datetime or text.
    """
    try:
        parent = a_tag.parent
        if not parent:
            return None
        t = parent.find("time")
        if t:
            if t.get("datetime"):
                ts = _parse_any_date_to_ts(t["datetime"])
                if ts:
                    return ts
            txt = t.get_text(" ", strip=True)
            ts = _parse_any_date_to_ts(txt)
            if ts:
                return ts
        return None
    except Exception:
        return None


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch items from an HTML index page:
    - extracts candidate links
    - tries to infer publish date from URL or nearby <time>
    - for items still missing dates, fetches article HTML (limited) and extracts publish date
    """
    label = (source_name or "").strip() or url
    html = fetch_url(url)

    # First pass: basic link extraction with titles
    soup = BeautifulSoup(html, "html.parser")
    dom = _domain(url)
    selector = SELECTORS.get(dom, "a[href]")

    candidates: list[Item] = []
    seen = set()

    for a in soup.select(selector):
        href = _norm_url(a.get("href") or "")
        if not href:
            continue
        abs_u = urljoin(url, href)
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
            # try nearby <time> in the DOM
            ts = _try_nearby_time_tag_ts(a)

        candidates.append(Item(title=title, url=abs_u, source=label, published_ts=ts))

        if len(candidates) >= MAX_LINKS_PER_INDEX:
            break

    # Second pass: resolve missing dates by fetching the article (budgeted)
    resolve_budget = MAX_DATE_RESOLVE_FETCHES_PER_INDEX
    for it in candidates:
        if resolve_budget <= 0:
            break
        if it.published_ts is not None:
            continue
        ts = _resolve_published_ts_from_article(it.url)
        if ts is not None:
            it.published_ts = ts
        resolve_budget -= 1

    return _dedupe_by_url(candidates)


def fetch_full_text(url: str, timeout_s: int = TIMEOUT) -> str:
    """
    Fetch full text of an article, preferring trafilatura extraction.
    """
    html = fetch_url(url, timeout_s=timeout_s)
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
