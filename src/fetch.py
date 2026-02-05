# src/fetch.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
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


HEADERS = {"User-Agent": "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"}
TIMEOUT = 25

# Optional per-domain link selectors (kept as in your prior code style)
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


def fetch_url(url: str, timeout_s: int = TIMEOUT) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=timeout_s)
    resp.raise_for_status()
    return resp.text


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch items from an RSS/Atom feed.

    Compatibility:
      - generate_monthly passes source_name=...
      - we accept it (and any future kwargs) so we don't hard-fail the run.
    """
    label = (source_name or "").strip() or url

    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()

    feed = feedparser.parse(resp.content)
    items: list[Item] = []

    # Use the feed title as a fallback label if source_name isn't provided
    feed_title = (getattr(feed.feed, "title", "") or "").strip()
    item_source = label or feed_title or url

    for e in getattr(feed, "entries", []) or []:
        link = _norm_url(getattr(e, "link", "") or "")
        title = (getattr(e, "title", "") or "").strip()
        summary = (getattr(e, "summary", "") or "").strip()

        if not link:
            continue

        published_ts: Optional[float] = None
        st = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        if st:
            try:
                published_ts = float(time.mktime(st))
            except Exception:
                published_ts = None

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

    Compatibility:
      - generate_monthly passes source_name=...
      - we accept it (and any future kwargs) so we don't hard-fail the run.
    """
    label = (source_name or "").strip() or url

    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
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
        if any(
            x in abs_url.lower()
            for x in (
                "#",
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

        items.append(
            Item(
                title=text[:300],
                url=abs_url,
                source=label,
                published_ts=None,
                summary="",
                text="",
            )
        )

    return _dedupe_by_url(items)


def fetch_full_text(url: str, timeout_s: int = 30) -> str:
    """
    Keep as a simple HTML fetch; your summarisation stage can extract/clean via trafilatura/justext elsewhere.
    """
    return fetch_url(url, timeout_s=timeout_s)
