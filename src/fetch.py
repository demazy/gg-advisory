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
from dateutil import parser as dateutil_parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -----------------------------
# HTTP session (retries/backoff)
# -----------------------------
TIMEOUT = 35

HEADERS = {
    # Use a more browser-like UA; many govt/international sites WAF-block botty UAs.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Connection": "keep-alive",
}

_session = requests.Session()
_retry = Retry(
    total=3,
    backoff_factor=0.8,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "HEAD"),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.mount("http://", HTTPAdapter(max_retries=_retry))


@dataclass
class Item:
    source: str
    url: str
    title: str
    published_ts: Optional[float]
    summary: str = ""


def _looks_like_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False


def _infer_published_ts(s: str) -> Optional[float]:
    """
    Best-effort date inference from URL or text.
    Supports YYYY-MM-DD, YYYY/MM/DD, DD Month YYYY, Month DD, YYYY.
    """
    if not s:
        return None

    # 2026-01-15 or 2026/01/15
    m = re.search(r"\b(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])\b", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # 15-01-2026 or 15/01/2026 (dayfirst)
    m = re.search(r"\b(0?[1-9]|[12]\d|3[01])[-/](0?[1-9]|1[0-2])[-/](20\d{2})\b", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()

    # Try fuzzy parse (handles "15 January 2026", "Jan 15, 2026", etc.)
    try:
        dt = dateutil_parser.parse(s, fuzzy=True, dayfirst=False)
        if dt.year >= 2000:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).timestamp()
    except Exception:
        pass

    return None


def _parse_time_tag_to_ts(tag) -> Optional[float]:
    if not tag:
        return None
    dt_str = (tag.get("datetime") or "").strip()
    if dt_str:
        try:
            dt = dateutil_parser.parse(dt_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).timestamp()
        except Exception:
            pass
    # fallback: parse visible text in <time>
    txt = tag.get_text(" ", strip=True)
    return _infer_published_ts(txt)


def _extract_date_near_anchor(a) -> Optional[float]:
    """
    Find a date near the anchor:
    - <time datetime="..."> within the anchor, its parent, or grandparent
    - otherwise try to parse from the anchor text itself
    """
    if a is None:
        return None

    # 1) time tag inside anchor
    t = a.find("time")
    ts = _parse_time_tag_to_ts(t)
    if ts:
        return ts

    # 2) time tag near anchor: parent / grandparent
    parent = a.parent
    if parent is not None:
        t = parent.find("time")
        ts = _parse_time_tag_to_ts(t)
        if ts:
            return ts
        gp = parent.parent
        if gp is not None:
            t = gp.find("time")
            ts = _parse_time_tag_to_ts(t)
            if ts:
                return ts

    # 3) parse from anchor text
    txt = a.get_text(" ", strip=True)
    ts = _infer_published_ts(txt)
    if ts:
        return ts

    return None


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    label = source_name or url
    try:
        resp = _session.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)

        items: list[Item] = []
        for e in feed.entries:
            link = getattr(e, "link", None) or ""
            title = (getattr(e, "title", "") or "").strip()
            summary = (getattr(e, "summary", "") or "").strip()

            published_ts: Optional[float] = None
            if getattr(e, "published_parsed", None):
                published_ts = time.mktime(e.published_parsed)
            elif getattr(e, "updated_parsed", None):
                published_ts = time.mktime(e.updated_parsed)
            else:
                # fallback: try parsing published/updated strings
                published_ts = _infer_published_ts(getattr(e, "published", "") or "") or \
                               _infer_published_ts(getattr(e, "updated", "") or "")

            items.append(Item(
                source=url,
                url=link,
                title=title,
                published_ts=published_ts,
                summary=summary,
            ))
        return items

    except Exception as ex:
        raise RuntimeError(f"fetch_rss failed for {label}: {ex}") from ex


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch candidate article links from an HTML 'news index' page.
    Now attempts to extract a publish date near each link (time tag / text / URL).
    """
    label = source_name or url
    try:
        resp = _session.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        items: list[Item] = []

        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            abs_url = urljoin(url, href)
            if not _looks_like_url(abs_url):
                continue

            text = (a.get_text(" ", strip=True) or "").strip()
            if not text:
                continue

            # Date extraction
            ts = _extract_date_near_anchor(a)
            if ts is None:
                # fall back to URL inference
                ts = _infer_published_ts(abs_url)

            items.append(Item(
                source=url,
                url=abs_url,
                title=text[:180],
                published_ts=ts,
                summary="",
            ))

        # de-dupe by URL
        seen = set()
        deduped: list[Item] = []
        for it in items:
            if it.url in seen:
                continue
            seen.add(it.url)
            deduped.append(it)

        return deduped

    except Exception as ex:
        raise RuntimeError(f"fetch_html_index failed for {label}: {ex}") from ex
