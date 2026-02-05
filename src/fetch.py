# src/fetch.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup


@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: Optional[float] = None
    summary: str = ""
    text: str = ""


# More browser-like headers to reduce some basic bot blocks (won't solve all 403s)
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

# Optional per-domain CSS selectors
SELECTORS = {
    # "aemo.com.au": "a[href]",
}


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


def _infer_published_ts_from_url(u: str) -> Optional[float]:
    """
    Heuristics to infer YYYY-MM-DD from URL patterns, returning UTC timestamp.
    Supports:
      - /YYYY/MM/DD/
      - /YYYY-MM-DD/
      - YYYYMMDD anywhere
      - ?date=YYYY-MM-DD
    """
    u2 = u

    # query param date=YYYY-MM-DD
    m = re.search(r"[?&]date=(\d{4})-(\d{2})-(\d{2})\b", u2)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # /YYYY/MM/DD/
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})\b", u2)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # /YYYY-MM-DD/
    m = re.search(r"/(\d{4})-(\d{2})-(\d{2})\b", u2)
    if m:
        y, mo, d = map(int, m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # YYYYMMDD (avoid matching long numeric IDs by requiring non-digit boundary)
    m = re.search(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)", u2)
    if m:
        y, mo, d = map(int, m.groups())
        # crude sanity check
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    return None


def fetch_url(url: str, timeout_s: int = TIMEOUT) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=timeout_s, allow_redirects=True)
    resp.raise_for_status()
    return resp.text


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch items from an RSS/Atom feed.
    Accepts source_name for compatibility with generate_monthly.
    """
    label = (source_name or "").strip() or url

    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    resp.raise_for_status()

    feed = feedparser.parse(resp.content)
    items: list[Item] = []

    feed_title = (getattr(feed.feed, "title", "") or "").strip()
    item_source = label or feed_title or url

    for e in getattr(feed, "entries", []) or []:
        link = _norm_url(getattr(e, "link", "") or "")
        if not link:
            continue

        title = (getattr(e, "title", "") or "").strip()
        summary = (getattr(e, "summary", "") or "").strip()

        published_ts: Optional[float] = None
        st = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        if st:
            try:
                published_ts = float(time.mktime(st))
            except Exception:
                published_ts = None

        # If RSS date missing, try infer from URL
        if published_ts is None:
            published_ts = _infer_published_ts_from_url(link)

        items.append(
            Item(
                title=title[:300] if title else link,
                url=link,
                source=item_source,
                published_ts=published_ts,
                summary=summary,
                text="",
            )
        )

    return _dedupe_by_url(items)


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch candidate article links from an HTML 'news index' page.
    Critical: infer published_ts from URL so generate_monthly can filter by month.
    """
    label = (source_name or "").strip() or url

    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    domain = urlparse(url).netloc.lower().replace("www.", "")
    sel = SELECTORS.get(domain, "a[href]")

    items: list[Item] = []
    for a in soup.select(sel):
        href = _norm_url(a.get("href") or "")
        if not href:
            continue

        abs_url = urljoin(url, href)
        if not _is_http_url(abs_url):
            continue

        text = (a.get_text(" ", strip=True) or "").strip()
        if not text:
            continue

        # Skip obvious non-article links
        low = abs_url.lower()
        if any(
            x in low
            for x in (
                "javascript:",
                "mailto:",
                "/tag/",
                "/tags/",
                "/category/",
                "/categories/",
                "/author/",
                "/search",
            )
        ):
            continue

        published_ts = _infer_published_ts_from_url(abs_url)

        items.append(
            Item(
                title=text[:300],
                url=abs_url,
                source=label,
                published_ts=published_ts,
                summary="",
                text="",
            )
        )

    return _dedupe_by_url(items)


def fetch_full_text(url: str, timeout_s: int = 45) -> str:
    """
    Keep it simple: your downstream extraction can clean HTML using trafilatura/justext.
    """
    return fetch_url(url, timeout_s=timeout_s)
