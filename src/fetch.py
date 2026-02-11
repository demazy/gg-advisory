# src/fetch.py
# Robust fetch layer for monthly digest pipeline (RSS + HTML index + full text + PDF)
# IMPORTANT: must NOT import from .fetch anywhere in this file (avoids circular import)

from __future__ import annotations

import os
import re
import html
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter

try:
    # Comes via trafilatura deps; helps infer publish dates
    from htmldate import find_date  # type: ignore
except Exception:  # pragma: no cover
    find_date = None

try:
    import trafilatura  # type: ignore
except Exception:  # pragma: no cover
    trafilatura = None

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None


# ----------------------------- Data model -----------------------------

@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: Optional[Any] = None  # datetime | float | str | None (generate_monthly coerces)
    summary: str = ""


# ----------------------------- Environment -----------------------------

_UA = os.getenv(
    "HTTP_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
)
_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))
_MAX_BYTES = int(os.getenv("HTTP_MAX_BYTES", str(8 * 1024 * 1024)))  # max HTML bytes to read
_MAX_PDF_BYTES = int(os.getenv("MAX_PDF_BYTES", str(5 * 1024 * 1024)))

MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "200"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "1"))
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "0"))

# If set, we only parse PDFs hosted on these domains
PDF_TRUSTED = {
    d.strip().lower()
    for d in os.getenv("PDF_TRUSTED", "").split(",")
    if d.strip()
}

# By default, keep index scraping on same host to avoid pulling sitewide noise
ALLOW_CROSS_DOMAIN_INDEX = os.getenv("ALLOW_CROSS_DOMAIN_INDEX", "0") == "1"

# Retry behaviour
RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "0.6"))


# ----------------------------- HTTP helpers -----------------------------

def _host(url: str) -> str:
    return urllib.parse.urlsplit(url).netloc.lower()


def _abs_url(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href)


def _norm_url(url: str) -> str:
    """Normalise URL for de-duplication: strip fragment, preserve query."""
    u = urllib.parse.urlsplit(url.strip())
    # normalise scheme/netloc casing
    scheme = (u.scheme or "https").lower()
    netloc = u.netloc.lower()
    return urllib.parse.urlunsplit((scheme, netloc, u.path or "/", u.query or "", ""))


def _looks_like_pdf(url: str, content_type: str = "") -> bool:
    if url.lower().split("?")[0].endswith(".pdf"):
        return True
    ct = (content_type or "").lower()
    return "application/pdf" in ct or "pdf" in ct


def _session() -> requests.Session:
    s = requests.Session()

    # urllib3 retry is fine here; requests exposes it via adapter
    try:
        from urllib3.util.retry import Retry  # type: ignore

        retry = Retry(
            total=RETRIES,
            connect=RETRIES,
            read=RETRIES,
            status=RETRIES,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    except Exception:
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)

    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_SESS = _session()


def _get(url: str, *, stream: bool = False) -> requests.Response:
    r = _SESS.get(
        url,
        headers={"User-Agent": _UA, "Accept": "*/*"},
        timeout=_TIMEOUT,
        allow_redirects=True,
        stream=stream,
    )
    r.raise_for_status()
    return r


def _get_text(url: str) -> Tuple[str, str]:
    """Return (text, content_type) with size guard."""
    r = _get(url, stream=True)
    ct = r.headers.get("Content-Type", "") or ""

    # bounded read
    chunks = []
    read = 0
    for chunk in r.iter_content(chunk_size=64 * 1024):
        if not chunk:
            continue
        chunks.append(chunk)
        read += len(chunk)
        if read > _MAX_BYTES:
            break

    data = b"".join(chunks)
    # best-effort decode
    enc = r.encoding or "utf-8"
    try:
        txt = data.decode(enc, errors="replace")
    except Exception:
        txt = data.decode("utf-8", errors="replace")
    return txt, ct


# ----------------------------- Date parsing -----------------------------

_URL_DATE_PATTERNS = [
    # /YYYY/MM/DD/
    re.compile(r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])(?:/|$)"),
    # /YYYY-MM-DD/
    re.compile(r"/(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(?:/|$)"),
    # ?date=YYYY-MM-DD etc
    re.compile(r"[?&](?:date|published|pubdate)=(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b"),
]


def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        dt = dtparser.parse(s)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _date_from_url(url: str) -> Optional[datetime]:
    u = _norm_url(url)
    for pat in _URL_DATE_PATTERNS:
        m = pat.search(u)
        if m:
            y, mo, d = map(int, m.groups())
            return datetime(y, mo, d, tzinfo=timezone.utc)
    return None


def _extract_pubdate_from_html(html_txt: str) -> Optional[datetime]:
    soup = BeautifulSoup(html_txt, "html.parser")

    # common metadata fields
    meta_selectors = [
        'meta[property="article:published_time"]',
        'meta[name="article:published_time"]',
        'meta[name="pubdate"]',
        'meta[name="publish_date"]',
        'meta[name="publication_date"]',
        'meta[name="date"]',
        'meta[property="og:published_time"]',
        'meta[property="og:updated_time"]',
        'meta[name="parsely-pub-date"]',
    ]
    for sel in meta_selectors:
        tag = soup.select_one(sel)
        if tag and tag.get("content"):
            dt = _parse_dt(tag.get("content", ""))
            if dt:
                return dt

    # <time datetime="..."> or <time>text</time>
    t = soup.find("time")
    if t:
        raw = (t.get("datetime") or t.get_text(" ", strip=True) or "").strip()
        dt = _parse_dt(raw)
        if dt:
            return dt

    # htmldate fallback (very helpful for news sites)
    if find_date is not None:
        try:
            d = find_date(html_txt)
            if d:
                dt = _parse_dt(d)
                if dt:
                    return dt
        except Exception:
            pass

    return None


def _resolve_published_date(url: str) -> Optional[datetime]:
    # cheap date from URL first
    dt = _date_from_url(url)
    if dt:
        return dt

    try:
        txt, ct = _get_text(url)
        if _looks_like_pdf(url, ct):
            return None
        return _extract_pubdate_from_html(txt)
    except Exception:
        return None


# ----------------------------- RSS fetch -----------------------------

def _strip_html(s: str) -> str:
    s = html.unescape(s or "")
    # very light strip: bs4 already installed
    try:
        soup = BeautifulSoup(s, "html.parser")
        return soup.get_text(" ", strip=True)
    except Exception:
        return re.sub(r"<[^>]+>", " ", s).strip()


def fetch_rss(feed_url: str, source_name: str = "") -> List[Item]:
    txt, _ct = _get_text(feed_url)
    feed = feedparser.parse(txt)

    out: List[Item] = []
    for e in (feed.entries or []):
        url = (e.get("link") or "").strip()
        if not url:
            continue

        title = (e.get("title") or "").strip()
        summary = _strip_html(e.get("summary") or e.get("description") or "")

        published_ts: Optional[Any] = None
        # feedparser provides published_parsed sometimes
        if getattr(e, "published_parsed", None):
            dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
            published_ts = dt
        elif e.get("published"):
            published_ts = _parse_dt(e.get("published", "")) or None
        elif e.get("updated"):
            published_ts = _parse_dt(e.get("updated", "")) or None

        out.append(
            Item(
                title=title,
                url=_norm_url(url),
                source=source_name or feed_url,
                published_ts=published_ts,
                summary=summary,
            )
        )

    # newest first when dates exist
    def _key(it: Item) -> float:
        ts = it.published_ts
        if isinstance(ts, datetime):
            return ts.timestamp()
        if isinstance(ts, (int, float)):
            return float(ts)
        return 0.0

    out.sort(key=_key, reverse=True)
    return out


# ----------------------------- HTML index scraping -----------------------------

# Heuristics to drop obvious non-article links
_SKIP_URL_RE = re.compile(
    r"(/login\b|/signin\b|/sign-in\b|/subscribe\b|/newsletter\b|/privacy\b|/terms\b|/contact\b|/about\b"
    r"|/careers\b|/jobs\b|/events?\b|/tag/|/tags/|/category/|/categories/|/search\b|/sitemap\b)",
    flags=re.I,
)

# Heuristics for “next page” discovery
_NEXT_TEXTS = {"next", "older", "older posts", "more", "load more"}


def _extract_links_from_index(html_txt: str, base_url: str) -> List[Tuple[str, str, Optional[datetime]]]:
    soup = BeautifulSoup(html_txt, "html.parser")
    base_host = _host(base_url)

    links: List[Tuple[str, str, Optional[datetime]]] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("mailto:", "javascript:")):
            continue

        u = _norm_url(_abs_url(base_url, href))
        if not u.startswith(("http://", "https://")):
            continue

        if not ALLOW_CROSS_DOMAIN_INDEX and _host(u) != base_host:
            continue

        if _SKIP_URL_RE.search(u):
            continue

        # avoid pure fragments
        if urllib.parse.urlsplit(u).path in ("", "/") and urllib.parse.urlsplit(u).query == "":
            continue

        # title text from anchor
        txt = (a.get_text(" ", strip=True) or "").strip()

        # try to detect nearby date without fetching the article
        dt: Optional[datetime] = _date_from_url(u)
        if dt is None:
            # check parent container for <time> or date-like text
            parent = a.find_parent(["article", "li", "div", "section"])
            if parent:
                t = parent.find("time")
                if t:
                    raw = (t.get("datetime") or t.get_text(" ", strip=True) or "").strip()
                    dt = _parse_dt(raw)

        links.append((u, txt, dt))

        if len(links) >= MAX_LINKS_PER_INDEX:
            break

    # De-dupe preserve order
    seen = set()
    uniq: List[Tuple[str, str, Optional[datetime]]] = []
    for u, t, d in links:
        if u in seen:
            continue
        seen.add(u)
        uniq.append((u, t, d))
    return uniq


def _find_next_page(html_txt: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html_txt, "html.parser")

    # <link rel="next" href="...">
    ln = soup.find("link", attrs={"rel": lambda v: v and "next" in (v if isinstance(v, list) else [v])})
    if ln and ln.get("href"):
        return _norm_url(_abs_url(current_url, ln["href"]))

    # <a rel="next" href="...">
    an = soup.find("a", attrs={"rel": lambda v: v and "next" in (v if isinstance(v, list) else [v])})
    if an and an.get("href"):
        return _norm_url(_abs_url(current_url, an["href"]))

    # anchor text
    a2 = soup.find(
        "a",
        string=lambda s: isinstance(s, str) and s.strip().lower() in _NEXT_TEXTS,
    )
    if a2 and a2.get("href"):
        return _norm_url(_abs_url(current_url, a2["href"]))

    # class-based
    a3 = soup.select_one("a.next, a.older, a.pagination__next, a[aria-label='Next']")
    if a3 and a3.get("href"):
        return _norm_url(_abs_url(current_url, a3["href"]))

    return None


def fetch_html_index(index_url: str, source_name: str = "") -> List[Item]:
    url = _norm_url(index_url)
    collected: List[Tuple[str, str, Optional[datetime]]] = []

    for _page in range(max(1, MAX_INDEX_PAGES)):
        html_txt, _ct = _get_text(url)
        collected.extend(_extract_links_from_index(html_txt, url))

        if len(collected) >= MAX_LINKS_PER_INDEX:
            break

        nxt = _find_next_page(html_txt, url)
        if not nxt or nxt == url:
            break
        url = nxt

        # be polite on aggressive pagination
        time.sleep(0.05)

    # Build items
    items: List[Item] = []
    for u, t, d in collected[:MAX_LINKS_PER_INDEX]:
        items.append(
            Item(
                title=(t or u.rsplit("/", 1)[-1].replace("-", " ")[:140]),
                url=u,
                source=source_name or index_url,
                published_ts=d,
                summary="",
            )
        )

    # Optional date resolution: fetch N items to extract meta publish time
    if MAX_DATE_RESOLVE_FETCHES_PER_INDEX > 0:
        n = min(MAX_DATE_RESOLVE_FETCHES_PER_INDEX, len(items))
        for it in items[:n]:
            if it.published_ts is not None:
                continue
            dt = _resolve_published_date(it.url)
            if dt:
                it.published_ts = dt

    return items


# ----------------------------- Full text extraction -----------------------------

def _fetch_pdf_bytes(url: str) -> Optional[bytes]:
    # trust-gate PDFs if configured
    if PDF_TRUSTED:
        h = _host(url)
        if not any(h.endswith(d) for d in PDF_TRUSTED):
            return None

    r = _get(url, stream=True)
    data = b""
    for chunk in r.iter_content(chunk_size=128 * 1024):
        if not chunk:
            continue
        data += chunk
        if len(data) > _MAX_PDF_BYTES:
            return None
    return data


def _pdf_to_text(data: bytes) -> str:
    if fitz is None:
        return ""
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        parts: List[str] = []
        for page in doc:
            parts.append(page.get_text("text"))
        doc.close()
        return "\n".join(parts).strip()
    except Exception:
        return ""


def fetch_full_text(url: str) -> str:
    """
    Extract main content from a URL.
    - HTML: trafilatura.extract
    - PDF: PyMuPDF
    Returns "" on failure (caller applies thresholds).
    """
    try:
        txt, ct = _get_text(url)
        if _looks_like_pdf(url, ct):
            # Re-fetch bounded bytes for PDF (text-mode fetch may have truncated)
            data = _fetch_pdf_bytes(url)
            if not data:
                return ""
            return _pdf_to_text(data)

        # HTML extraction
        if trafilatura is not None:
            extracted = trafilatura.extract(txt, include_comments=False, include_tables=False)
            if extracted:
                return extracted.strip()

        # fallback: visible text
        soup = BeautifulSoup(txt, "html.parser")
        return soup.get_text(" ", strip=True)

    except Exception:
        return ""
