# src/fetch.py
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup


@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: float
    published_iso: str


def _to_iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _safe_ts_from_struct_time(st) -> Optional[float]:
    if not st:
        return None
    try:
        return float(time.mktime(st))
    except Exception:
        return None


def _norm_url(u: str) -> str:
    return u.strip()


def _looks_like_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False


def _unique_by_url(items: Iterable[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _find_links_html(url: str, html: str) -> List[str]:
    """
    Extract candidate links from an index page.
    Keep it intentionally permissive; downstream filtering handles domain/title rules.
    """
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        abs_url = urljoin(url, href)
        if _looks_like_url(abs_url):
            links.append(abs_url)

    return links


def fetch_url(url: str, timeout_s: int = 30) -> str:
    headers = {"User-Agent": "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"}
    resp = requests.get(url, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    return resp.text


def fetch_rss(url: str, source_name: str | None = None, **_: object) -> List[Item]:
    """
    Robust RSS fetch using requests + feedparser.

    NOTE: This function now accepts `source_name` (and extra kwargs)
    because upstream code passes `source_name=` for debugging/attribution.
    """
    out: List[Item] = []
    headers = {"User-Agent": "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"}

    attempts, data = 0, None
    while attempts < 3:
        attempts += 1
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.content
            break
        except Exception:
            if attempts >= 3:
                raise
            time.sleep(1.0 * attempts)

    feed = feedparser.parse(data)

    for entry in getattr(feed, "entries", []) or []:
        link = getattr(entry, "link", None) or getattr(entry, "id", None)
        if not link:
            continue

        title = (getattr(entry, "title", "") or "").strip() or "(untitled)"
        published_ts = (
            _safe_ts_from_struct_time(getattr(entry, "published_parsed", None))
            or _safe_ts_from_struct_time(getattr(entry, "updated_parsed", None))
        )

        # If feed doesn’t provide a date, default to “now” (downstream time-window filter will drop if needed)
        if published_ts is None:
            published_ts = time.time()

        out.append(
            Item(
                title=title,
                url=_norm_url(link),
                source=(source_name or url),
                published_ts=float(published_ts),
                published_iso=_to_iso_utc(float(published_ts)),
            )
        )

    return _unique_by_url(out)


def fetch_html_index(url: str, source_name: str | None = None, **_: object) -> List[Item]:
    """
    Fetch an HTML index page and produce Item candidates.

    NOTE: This function now accepts `source_name` (and extra kwargs)
    because upstream code passes `source_name=`.
    """
    html = fetch_url(url)

    # 1) extract links
    links = _find_links_html(url, html)

    # 2) build items; dates are unknown at index-level, so we set to "now"
    now_ts = time.time()
    items: List[Item] = []
    for link in links:
        items.append(
            Item(
                title="(index link)",
                url=_norm_url(link),
                source=(source_name or url),
                published_ts=float(now_ts),
                published_iso=_to_iso_utc(float(now_ts)),
            )
        )

    return _unique_by_url(items)


def fetch_text(url: str, timeout_s: int = 30) -> str:
    """
    Placeholder: keep your existing implementation if you have trafilatura/justext extraction elsewhere.
    """
    return fetch_url(url, timeout_s=timeout_s)

