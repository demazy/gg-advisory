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

# Optional (already in your requirements); used as a fallback for date + text extraction.
try:
    import htmldate  # type: ignore
except Exception:  # pragma: no cover
    htmldate = None

try:
    import trafilatura  # type: ignore
except Exception:  # pragma: no cover
    trafilatura = None

try:
    from dateutil import parser as dateutil_parser  # type: ignore
except Exception:  # pragma: no cover
    dateutil_parser = None


# ---- HTTP defaults tuned for GitHub Actions / “botty” environments ----
TIMEOUT = 35
HEADERS = {
    # More “browser-like” UA reduces 403 on a bunch of sites vs a custom bot UA.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# How many “undated” links we’ll try to enrich by fetching the article page (per index page).
MAX_DATE_ENRICH_FETCH = 30

# How many links to keep from an index page (before downstream filtering).
MAX_INDEX_LINKS = 250


@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: Optional[float] = None
    summary: str = ""
    text: str = ""


# ------------------------- helpers -------------------------


def _looks_like_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False


def _norm_url(u: str) -> str:
    return (u or "").strip()


def _unique_by_url(items: Iterable[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _safe_ts_from_struct_time(st) -> Optional[float]:
    if not st:
        return None
    try:
        return float(time.mktime(st))
    except Exception:
        return None


# Common URL date patterns: /YYYY/MM/DD/, /YYYY-MM-DD/, /YYYY/MM/, etc.
_URL_DATE_RE = re.compile(
    r"(?<!\d)(20\d{2})[\/\-\.](\d{1,2})(?:[\/\-\.](\d{1,2}))?(?!\d)"
)


def _infer_ts_from_url(u: str) -> Optional[float]:
    m = _URL_DATE_RE.search(u)
    if not m:
        return None
    y = int(m.group(1))
    mo = int(m.group(2))
    d = int(m.group(3) or "1")
    try:
        dt = datetime(y, mo, d, tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _parse_ts_from_text_date(s: str) -> Optional[float]:
    """
    Parse a date from arbitrary text. We keep it conservative:
    - prefer <time datetime="..."> values
    - otherwise use dateutil if available
    """
    s = (s or "").strip()
    if not s:
        return None

    # ISO-like quick path
    # Examples: 2026-01-15, 2026/01/15, 2026-01
    m = _URL_DATE_RE.search(s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3) or "1")
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()
        except Exception:
            pass

    if dateutil_parser:
        try:
            dt = dateutil_parser.parse(s, fuzzy=True)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).timestamp()
        except Exception:
            return None

    return None


def _extract_candidate_date_from_dom(a_tag) -> Optional[float]:
    """
    Walk up a few ancestors looking for:
      - <time datetime="...">
      - text containing a parseable date
    """
    cur = a_tag
    for _ in range(4):
        if cur is None:
            break

        # 1) <time datetime="...">
        t = cur.find("time")
        if t is not None:
            dt_attr = (t.get("datetime") or "").strip()
            ts = _parse_ts_from_text_date(dt_attr)
            if ts:
                return ts
            # sometimes time tag has text like "15 Jan 2026"
            ts = _parse_ts_from_text_date(t.get_text(" ", strip=True))
            if ts:
                return ts

        # 2) parent block text often includes the date
        block_text = cur.get_text(" ", strip=True) if hasattr(cur, "get_text") else ""
        ts = _parse_ts_from_text_date(block_text)
        if ts:
            return ts

        cur = cur.parent

    return None


def _session() -> requests.Session:
    # Keep a session for connection reuse.
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_url(url: str, timeout_s: int = TIMEOUT) -> str:
    """
    Fetch a URL with sane headers + simple retry/backoff for transient issues.
    (Does NOT attempt to bypass hard 403/JS challenges.)
    """
    url = _norm_url(url)
    if not url:
        raise ValueError("fetch_url: empty url")

    sess = _session()
    last_ex: Optional[Exception] = None

    for attempt in range(3):
        try:
            resp = sess.get(url, timeout=timeout_s)
            # If a site blocks automation, this is typically a hard 403/503.
            resp.raise_for_status()
            return resp.text
        except Exception as ex:
            last_ex = ex
            # backoff: 0.7s, 1.4s, 2.8s
            time.sleep(0.7 * (2**attempt))

    raise RuntimeError(f"fetch_url failed for {url}: {last_ex}") from last_ex


# ------------------------- fetchers -------------------------


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch items from an RSS/Atom feed.
    Must accept source_name for compatibility with generate_monthly.
    """
    label = source_name or url
    try:
        content = fetch_url(url, timeout_s=TIMEOUT)
        feed = feedparser.parse(content.encode("utf-8", errors="ignore"))

        items: list[Item] = []
        for e in feed.entries:
            link = _norm_url(getattr(e, "link", "") or "")
            if not link:
                continue

            title = (getattr(e, "title", "") or "").strip()
            summary = (getattr(e, "summary", "") or "").strip()

            ts = None
            ts = ts or _safe_ts_from_struct_time(getattr(e, "published_parsed", None))
            ts = ts or _safe_ts_from_struct_time(getattr(e, "updated_parsed", None))
            ts = ts or _infer_ts_from_url(link)

            items.append(
                Item(
                    source=url,
                    url=link,
                    title=title[:180] if title else link[:180],
                    published_ts=ts,
                    summary=summary,
                    text="",
                )
            )

        return _unique_by_url(items)

    except Exception as ex:
        raise RuntimeError(f"fetch_rss failed for {label}: {ex}") from ex


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch candidate article links from an HTML index page.
    Critical: we try hard to set published_ts (otherwise generate_monthly drops everything).
    Strategy:
      1) pull <time datetime=...> or nearby text dates from the index DOM
      2) infer from URL
      3) for a limited number of undated links, fetch article HTML and extract date via htmldate
    """
    label = source_name or url
    try:
        html = fetch_url(url, timeout_s=TIMEOUT)
        soup = BeautifulSoup(html, "html.parser")

        # Collect candidate <a href> links
        links: list[tuple[str, str, Optional[float]]] = []
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            abs_url = urljoin(url, href)
            if not _looks_like_url(abs_url):
                continue

            # ignore obvious non-articles
            if abs_url.startswith("mailto:") or abs_url.startswith("javascript:"):
                continue

            text = (a.get_text(" ", strip=True) or "").strip()
            if not text:
                # some sites use <img> links; keep a fallback title
                text = abs_url

            ts = _extract_candidate_date_from_dom(a)
            ts = ts or _infer_ts_from_url(abs_url)

            links.append((abs_url, text[:180], ts))
            if len(links) >= MAX_INDEX_LINKS:
                break

        # De-dupe by URL (keep first occurrence)
        seen = set()
        items: list[Item] = []
        for abs_url, title, ts in links:
            if abs_url in seen:
                continue
            seen.add(abs_url)
            items.append(Item(source=url, url=abs_url, title=title, published_ts=ts, summary="", text=""))

        # Enrich missing dates by fetching the article (limited)
        if htmldate is not None:
            enriched = 0
            sess = _session()

            for it in items:
                if it.published_ts is not None:
                    continue
                if enriched >= MAX_DATE_ENRICH_FETCH:
                    break

                try:
                    resp = sess.get(it.url, timeout=TIMEOUT)
                    resp.raise_for_status()

                    # htmldate.find_date returns a string like "2026-01-15"
                    d = htmldate.find_date(resp.text)  # type: ignore[attr-defined]
                    ts = _parse_ts_from_text_date(d) if d else None
                    ts = ts or _infer_ts_from_url(it.url)
                    it.published_ts = ts
                    enriched += 1
                except Exception:
                    # leave it undated; downstream will drop it
                    enriched += 1
                    continue

        return _unique_by_url(items)

    except Exception as ex:
        raise RuntimeError(f"fetch_html_index failed for {label}: {ex}") from ex


def fetch_text(url: str, timeout_s: int = TIMEOUT) -> str:
    """
    Extract readable text from a page (used by summarise pipeline).
    Uses trafilatura if available; falls back to BeautifulSoup text.
    """
    html = fetch_url(url, timeout_s=timeout_s)

    if trafilatura is not None:
        try:
            extracted = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=False,
                include_links=False,
                favor_recall=True,
            )
            if extracted and extracted.strip():
                return extracted.strip()
        except Exception:
            pass

    soup = BeautifulSoup(html, "html.parser")
    # strip scripts/styles
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = soup.get_text("\n", strip=True)
    return text.strip()
