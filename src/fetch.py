# src/fetch.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable, Dict, Tuple, List
import datetime as dt
import re
import time

import feedparser
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

import dateparser  # installed via requirements
from htmldate import find_date  # installed via trafilatura deps


# -----------------------------
# Models
# -----------------------------
@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: Optional[float]  # unix ts (UTC)
    summary: str = ""
    text: str = ""


# -----------------------------
# HTTP (robust session)
# -----------------------------
DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

_SESSION: Optional[requests.Session] = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-AU,en;q=0.9",
            "Connection": "keep-alive",
        }
    )

    # Simple retry logic (avoid bringing in urllib3 Retry complexity / version issues)
    # We'll implement retries manually in fetch_url().
    _SESSION = s
    return s


def fetch_url(
    url: str,
    timeout_s: int = 35,
    max_retries: int = 2,
    backoff_s: float = 1.0,
) -> str:
    """
    Fetch URL with small retries/backoff. Returns text (decoded by requests).
    Raises last exception on repeated failure.
    """
    last_exc: Exception | None = None
    s = _session()

    for attempt in range(max_retries + 1):
        try:
            resp = s.get(url, timeout=timeout_s, allow_redirects=True)
            # Treat 403/429/5xx as retryable
            if resp.status_code in (403, 429) or resp.status_code >= 500:
                raise requests.HTTPError(
                    f"{resp.status_code} for url: {url}", response=resp
                )
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(backoff_s * (2**attempt))
                continue
            raise

    # should not reach
    raise last_exc if last_exc else RuntimeError("fetch_url failed without exception")


# -----------------------------
# Date parsing helpers
# -----------------------------
_URL_DATE_PATTERNS: List[re.Pattern] = [
    # /2026/01/31/  or /2026-01-31/
    re.compile(r"(?P<y>20\d{2})[/-](?P<m>\d{2})[/-](?P<d>\d{2})"),
    # /2026/01/
    re.compile(r"(?P<y>20\d{2})[/-](?P<m>\d{2})(?![/-]\d{2})"),
]


def _dt_to_ts_utc(d: dt.datetime) -> float:
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    else:
        d = d.astimezone(dt.timezone.utc)
    return d.timestamp()


def _parse_date_to_ts(date_str: str) -> Optional[float]:
    if not date_str or not date_str.strip():
        return None

    # Use dateparser because it handles "6 January 2026", "Jan 6, 2026", etc.
    parsed = dateparser.parse(
        date_str,
        settings={
            "RETURN_AS_TIMEZONE_AWARE": True,
            "TO_TIMEZONE": "UTC",
            "TIMEZONE": "UTC",
            "PREFER_DAY_OF_MONTH": "first",
        },
    )
    if parsed is None:
        return None

    # Normalize to midnight UTC if time missing
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return _dt_to_ts_utc(parsed)


def _infer_published_ts_from_url(url: str) -> Optional[float]:
    path = urlparse(url).path
    for pat in _URL_DATE_PATTERNS:
        m = pat.search(path)
        if not m:
            continue
        y = int(m.group("y"))
        mth = int(m.group("m"))
        d = int(m.groupdict().get("d") or 1)
        try:
            return _dt_to_ts_utc(dt.datetime(y, mth, d, tzinfo=dt.timezone.utc))
        except ValueError:
            return None
    return None


_MONTHS_RX = (
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)

# Examples:
# 6 January 2026
# January 6, 2026
# 06 Jan 2026
_TEXT_DATE_PATTERNS: List[re.Pattern] = [
    re.compile(rf"\b(\d{{1,2}})\s+{_MONTHS_RX}\s+(20\d{{2}})\b", re.I),
    re.compile(rf"\b{_MONTHS_RX}\s+(\d{{1,2}})(?:st|nd|rd|th)?\,?\s+(20\d{{2}})\b", re.I),
    re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b"),
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b"),
]


def _extract_date_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    t = " ".join(text.split())
    for pat in _TEXT_DATE_PATTERNS:
        m = pat.search(t)
        if not m:
            continue
        return _parse_date_to_ts(m.group(0))
    return None


def _date_from_time_tag(a_tag) -> Optional[float]:
    """
    Look near the link for <time datetime="..."> or <time>text</time>.
    """
    # Search in a small window: parents + siblings
    candidates = []

    # parent chain (up to ~3 levels)
    cur = a_tag
    for _ in range(3):
        cur = cur.parent
        if cur is None:
            break
        candidates.append(cur)

    for node in candidates:
        time_tags = node.find_all("time")
        for tt in time_tags:
            dt_attr = (tt.get("datetime") or "").strip()
            if dt_attr:
                ts = _parse_date_to_ts(dt_attr)
                if ts:
                    return ts
            ts = _parse_date_to_ts(tt.get_text(" ", strip=True))
            if ts:
                return ts

    return None


def _date_from_context_text(a_tag) -> Optional[float]:
    """
    Extract date-like strings from the surrounding element text.
    """
    # Try parent text (often contains date + title)
    parent = a_tag.parent
    if parent is not None:
        ts = _extract_date_from_text(parent.get_text(" ", strip=True))
        if ts:
            return ts

    # Try nearby siblings (common layouts)
    for sib in list(a_tag.parent.children) if a_tag.parent is not None else []:
        if getattr(sib, "get_text", None) is None:
            continue
        ts = _extract_date_from_text(sib.get_text(" ", strip=True))
        if ts:
            return ts

    # As a last lightweight attempt, scan the link text itself
    ts = _extract_date_from_text(a_tag.get_text(" ", strip=True))
    if ts:
        return ts

    return None


# Cache article-date lookups so the job doesn't re-fetch the same URL repeatedly
_ARTICLE_DATE_CACHE: Dict[str, Optional[float]] = {}


def _date_from_article(url: str, timeout_s: int = 35) -> Optional[float]:
    """
    Fallback: fetch article and infer published date using htmldate.
    This is heavier, so only used when all other methods fail.
    """
    if url in _ARTICLE_DATE_CACHE:
        return _ARTICLE_DATE_CACHE[url]

    try:
        html = fetch_url(url, timeout_s=timeout_s, max_retries=1, backoff_s=1.0)
    except Exception:
        _ARTICLE_DATE_CACHE[url] = None
        return None

    try:
        # htmldate returns 'YYYY-MM-DD' or None
        d = find_date(html, url=url, outputformat="%Y-%m-%d")
        if not d:
            _ARTICLE_DATE_CACHE[url] = None
            return None
        ts = _parse_date_to_ts(d)
        _ARTICLE_DATE_CACHE[url] = ts
        return ts
    except Exception:
        _ARTICLE_DATE_CACHE[url] = None
        return None


# -----------------------------
# RSS
# -----------------------------
def fetch_rss(url: str, label: str, timeout_s: int = 35) -> List[Item]:
    """
    Fetch RSS/Atom feeds. Dates typically come from the feed.
    """
    xml = fetch_url(url, timeout_s=timeout_s)
    feed = feedparser.parse(xml)

    items: List[Item] = []
    for e in feed.entries[:200]:
        link = e.get("link") or ""
        title = (e.get("title") or "").strip()
        if not link or not title:
            continue

        published_ts: Optional[float] = None
        # feedparser provides multiple date fields
        for key in ("published_parsed", "updated_parsed"):
            if e.get(key):
                t = e[key]
                try:
                    published_ts = _dt_to_ts_utc(
                        dt.datetime(t.tm_year, t.tm_mon, t.tm_mday, tzinfo=dt.timezone.utc)
                    )
                    break
                except Exception:
                    pass

        if published_ts is None:
            # Fallback: try parsing date strings
            for key in ("published", "updated"):
                if e.get(key):
                    published_ts = _parse_date_to_ts(str(e.get(key)))
                    if published_ts:
                        break

        items.append(
            Item(
                title=title[:300],
                url=link,
                source=label,
                published_ts=published_ts,
                summary="",
                text="",
            )
        )

    return _dedupe_by_url(items)


# -----------------------------
# HTML index scraping
# -----------------------------
def _domain_selectors(url: str) -> Tuple[str, str]:
    """
    Return (link_selector, title_attr) by domain.
    Default is 'a' + inner text.
    """
    host = urlparse(url).netloc.lower()

    # Domain-specific heuristics can be extended as needed
    if "ifrs.org" in host:
        return "a", ""
    if "efrag.org" in host:
        return "a", ""
    if "globalreporting.org" in host:
        return "a", ""
    if "asic.gov.au" in host:
        return "a", ""
    if "presscorner" in host or "ec.europa.eu" in host:
        return "a", ""
    if "aemo.com.au" in host:
        return "a", ""
    if "aer.gov.au" in host:
        return "a", ""
    if "arena.gov.au" in host:
        return "a", ""

    return "a", ""


def fetch_html_index(url: str, label: str, timeout_s: int = 35) -> List[Item]:
    """
    Scrape a listing page and attempt to attach a publish date to each candidate.
    """
    html = fetch_url(url, timeout_s=timeout_s)
    soup = BeautifulSoup(html, "html.parser")

    link_selector, _ = _domain_selectors(url)

    items: List[Item] = []
    for a in soup.select(link_selector):
        href = a.get("href") or ""
        text = a.get_text(" ", strip=True) or ""
        if not href:
            continue
        if not text or len(text) < 6:
            continue

        abs_url = urljoin(url, href)

        # Basic skip patterns to avoid tag/category/search pages
        p = urlparse(abs_url).path.lower()
        if any(
            p.startswith(prefix)
            for prefix in (
                "/tag/",
                "/tags/",
                "/category/",
                "/categories/",
                "/author/",
            )
        ) or "/search" in abs_url.lower():
            continue

        # Determine publish timestamp (critical for your pipeline)
        published_ts = (
            _date_from_time_tag(a)
            or _date_from_context_text(a)
            or _infer_published_ts_from_url(abs_url)
        )

        # If still unknown, do a *single* article fetch + htmldate inference
        if published_ts is None:
            published_ts = _date_from_article(abs_url, timeout_s=timeout_s)

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


# -----------------------------
# Full text fetch
# -----------------------------
def fetch_full_text(url: str, timeout_s: int = 45) -> str:
    """
    Raw fetch; downstream summarisation/extraction cleans HTML using trafilatura/justext.
    """
    return fetch_url(url, timeout_s=timeout_s)


# -----------------------------
# Utilities
# -----------------------------
def _dedupe_by_url(items: Iterable[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        if not it.url:
            continue
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out
