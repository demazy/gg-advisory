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
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "Connection": "keep-alive",
}


def _make_session() -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=32, pool_maxsize=32)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


SESSION = _make_session()

TIMEOUT = 35
MAX_BYTES = 600_000  # safety: don't download megabytes of HTML for indexes/articles

# Limits to keep runtime bounded (env-configurable)
MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "300"))
# How many "next pages" of the index to follow (best-effort)
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "6"))
# How many article fetches we allow to resolve missing dates
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "80"))


def _domain(url: str) -> str:
    return urlparse(url).netloc.lower().lstrip("www.")


def _is_http_url(u: str) -> bool:
    return u.startswith("http://") or u.startswith("https://")


def _norm_url(u: str) -> str:
    return u.strip()


def _read_limited(resp: requests.Response, max_bytes: int = MAX_BYTES) -> str:
    resp.raise_for_status()
    content = resp.content[:max_bytes]
    # requests guesses encoding; fall back to utf-8
    enc = resp.encoding or "utf-8"
    try:
        return content.decode(enc, errors="replace")
    except Exception:
        return content.decode("utf-8", errors="replace")


def fetch_url(url: str, timeout_s: int = TIMEOUT) -> str:
    # Some sites block CI ranges; a Jina proxy can help for plain HTML
    # We only use it for HTML pages (not RSS) and only if direct fetch fails.
    try:
        resp = SESSION.get(url, headers=DEFAULT_HEADERS, timeout=timeout_s)
        return _read_limited(resp)
    except Exception:
        # try proxy
        proxied = f"https://r.jina.ai/http://{url.lstrip('https://').lstrip('http://')}"
        resp2 = SESSION.get(proxied, headers=DEFAULT_HEADERS, timeout=timeout_s)
        return _read_limited(resp2)


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
    """
    Try a few common URL patterns:
      /YYYY/MM/DD/
      /YYYY-MM-DD/
      /YYYY/MM/
    """
    m = re.search(r"/(20\d{2})[/-](\d{1,2})[/-](\d{1,2})/", url)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return _to_ts(datetime(y, mo, d, tzinfo=timezone.utc))
        except Exception:
            pass

    m = re.search(r"/(20\d{2})-(\d{1,2})-(\d{1,2})/", url)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return _to_ts(datetime(y, mo, d, tzinfo=timezone.utc))
        except Exception:
            pass

    m = re.search(r"/(20\d{2})[/-](\d{1,2})/", url)
    if m:
        y, mo = map(int, m.groups())
        try:
            return _to_ts(datetime(y, mo, 1, tzinfo=timezone.utc))
        except Exception:
            pass

    return None


def _try_nearby_time_tag_ts(a_tag) -> Optional[float]:
    """
    Try to pick up <time datetime="..."> near the link element.
    """
    try:
        # direct parent search (common card layouts)
        parent = a_tag.parent
        for _ in range(3):
            if parent is None:
                break
            t = parent.find("time")
            if t is not None:
                dt_attr = t.get("datetime") or t.get_text(" ", strip=True)
                dt = _try_parse_dt(dt_attr or "")
                if dt is not None:
                    return _to_ts(dt)
            parent = parent.parent
    except Exception:
        pass
    return None


def _resolve_published_ts_from_article(url: str) -> Optional[float]:
    """
    Fetch article HTML and extract a publish date using htmldate/find_date,
    falling back to <meta> tags and regex.
    """
    try:
        html = fetch_url(url)
    except Exception:
        return None

    try:
        # htmldate does good meta + visible date extraction
        dt_str = find_date(html, extensive_search=True, original_date=True)
        if dt_str:
            dt = _try_parse_dt(dt_str)
            if dt is not None:
                return _to_ts(dt)
    except Exception:
        pass

    soup = BeautifulSoup(html, "html.parser")

    # common meta properties
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

    # regex fallback on visible text
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", text)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return _to_ts(datetime(y, mo, d, tzinfo=timezone.utc))
        except Exception:
            pass

    return None


def _dedupe_by_url(items: Iterable[Item]) -> list[Item]:
    out: list[Item] = []
    seen: set[str] = set()
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _extract_text_and_summary(url: str) -> tuple[str, str]:
    """
    Fetch and extract main text with trafilatura. Returns (text, summary).
    """
    try:
        downloaded = trafilatura.fetch_url(url, timeout=TIMEOUT)
        if not downloaded:
            return "", ""
        text = trafilatura.extract(downloaded) or ""
        text = re.sub(r"\s+", " ", text).strip()
        summary = text[:400].strip()
        return text, summary
    except Exception:
        return "", ""


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    label = (source_name or "").strip() or url
    parsed = feedparser.parse(url)
    items: list[Item] = []
    for e in parsed.entries[:200]:
        link = (getattr(e, "link", "") or "").strip()
        if not link or not _is_http_url(link):
            continue
        title = (getattr(e, "title", "") or "").strip()
        if not title:
            continue
        ts = None
        for field in ("published", "updated", "created"):
            if hasattr(e, field):
                dt = _try_parse_dt(getattr(e, field))
                if dt is not None:
                    ts = _to_ts(dt)
                    break
        items.append(Item(title=title, url=link, source=label, published_ts=ts))
    return _dedupe_by_url(items)


# Domain-specific selectors for "index" pages; keep it conservative.
SELECTORS: dict[str, str] = {
    # You can add domains as you learn the markup
    "www.aer.gov.au": "a[href]",
    "www.dcceew.gov.au": "a[href]",
    "www.infrastructureaustralia.gov.au": "a[href]",
    "www.iea.org": "a[href]",
    "www.irena.org": "a[href]",
    "www.efrag.org": "a[href]",
}


def _find_next_index_page(soup: BeautifulSoup, base_url: str) -> str | None:
    """
    Best-effort discovery of a "next" page for news/listing indexes.
    Supports rel="next" and common "Next/›/>" anchors.
    """
    # 1) rel=next
    a = soup.select_one('a[rel="next"][href]')
    if a and a.get("href"):
        nxt = urljoin(base_url, a.get("href"))
        if _is_http_url(nxt):
            return nxt

    # 2) common next-page anchors by label
    for cand in soup.select("a[href]"):
        txt = (cand.get_text(" ", strip=True) or "").lower()
        if txt in ("next", "older", "›", "»", ">"):
            href = _norm_url(cand.get("href") or "")
            if not href:
                continue
            nxt = urljoin(base_url, href)
            if _is_http_url(nxt):
                return nxt

    # 3) heuristic: class contains "next" or aria-label
    a2 = soup.select_one('a[href].next, a[href][class*="next"], a[href][aria-label*="Next"]')
    if a2 and a2.get("href"):
        nxt = urljoin(base_url, a2.get("href"))
        if _is_http_url(nxt):
            return nxt
    return None


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch items from an HTML index page:
    - extracts candidate links
    - tries to infer publish date from URL or nearby <time>
    - for items still missing dates, fetches article HTML (limited) and extracts publish date

    This implementation follows pagination links (best-effort) to reduce the chance that an entire
    month is missed because it is not present on the first listing page.
    """
    label = (source_name or "").strip() or url

    candidates: list[Item] = []
    seen: set[str] = set()

    page_url = url
    visited_pages: set[str] = set()
    dom = _domain(url)
    selector = SELECTORS.get(dom, "a[href]")

    # First pass: basic link extraction with titles across a few index pages
    for _page in range(MAX_INDEX_PAGES):
        if page_url in visited_pages:
            break
        visited_pages.add(page_url)

        html = fetch_url(page_url)
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
                # try nearby <time> in the DOM
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
        ts = _resolve_published_ts_from_article(it.url)
        if ts is not None:
            it.published_ts = ts
        resolve_budget -= 1

    return _dedupe_by_url(candidates)


def enrich_items(items: list[Item]) -> list[Item]:
    """
    Fetch article text + summary for a list of items (best-effort, limited).
    """
    out: list[Item] = []
    for it in items:
        text, summary = _extract_text_and_summary(it.url)
        it.text = text
        it.summary = summary
        out.append(it)
        # small delay to be polite
        time.sleep(0.15)
    return out


def fetch_source(source: dict) -> list[Item]:
    """
    source schema (from sources.yaml):
      - name
      - url
      - kind: rss|html
    """
    url = source.get("url", "").strip()
    if not url:
        return []
    kind = (source.get("kind") or "rss").strip().lower()
    name = source.get("name") or url
    if kind == "html":
        return fetch_html_index(url, source_name=name)
    return fetch_rss(url, source_name=name)


def load_debug_pool(path: str) -> list[Item]:
    raw = json.loads(open(path, "r", encoding="utf-8").read())
    out: list[Item] = []
    for r in raw:
        out.append(
            Item(
                title=r.get("title", ""),
                url=r.get("url", ""),
                source=r.get("source", ""),
                published_ts=r.get("published_ts"),
                summary=r.get("summary", ""),
                text=r.get("text", ""),
            )
        )
    return out


def dump_items(path: str, items: list[Item]) -> None:
    raw = []
    for it in items:
        raw.append(
            {
                "title": it.title,
                "url": it.url,
                "source": it.source,
                "published_ts": it.published_ts,
                "summary": it.summary,
                "text": it.text,
            }
        )
    with open(path, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)
