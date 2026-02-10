# src/fetch.py
from __future__ import annotations

import os
import re
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Set, Tuple

import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

# Optional deps you already install
import trafilatura
import fitz  # PyMuPDF


@dataclass
class Item:
    url: str
    title: str = ""
    summary: str = ""
    source: str = ""
    published_ts: Optional[datetime] = None


# ---------------------- Tunables (env) ----------------------
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "25"))
HTTP_CONNECT_TIMEOUT_S = float(os.getenv("HTTP_CONNECT_TIMEOUT_S", "8"))

MAX_RSS_ITEMS = int(os.getenv("MAX_RSS_ITEMS", "80"))

MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "250"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "5"))
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "75"))

MAX_HTML_BYTES = int(os.getenv("MAX_HTML_BYTES", str(2 * 1024 * 1024)))  # 2MB
MAX_PDF_BYTES = int(os.getenv("MAX_PDF_BYTES", str(5 * 1024 * 1024)))    # 5MB

UA = os.getenv(
    "HTTP_USER_AGENT",
    "GG-Advisory-DigestBot/1.0 (+https://github.com/demazy/gg-advisory)"
)

_session: Optional[requests.Session] = None


def _sess() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "*/*"})
        _session = s
    return _session


def _http_get(url: str, *, stream: bool = False) -> requests.Response:
    return _sess().get(
        url,
        timeout=(HTTP_CONNECT_TIMEOUT_S, HTTP_TIMEOUT_S),
        allow_redirects=True,
        stream=stream,
    )


def _safe_read_text(resp: requests.Response) -> str:
    # Guard against huge HTML responses
    content = resp.content
    if len(content) > MAX_HTML_BYTES:
        content = content[:MAX_HTML_BYTES]
    resp.encoding = resp.encoding or "utf-8"
    return content.decode(resp.encoding, errors="replace")


def _normalize_url(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href.strip())


def _same_host(a: str, b: str) -> bool:
    try:
        return urllib.parse.urlparse(a).netloc.lower() == urllib.parse.urlparse(b).netloc.lower()
    except Exception:
        return False


# ---------------------- Date parsing ----------------------
_RE_YMD = re.compile(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})")
_RE_YM = re.compile(r"(20\d{2})[/-](\d{1,2})(?!\d)")


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_date_any(s: str) -> Optional[datetime]:
    try:
        dt = dtparser.parse(s)
        if not dt:
            return None
        return _to_utc(dt)
    except Exception:
        return None


def _date_from_url(url: str) -> Optional[datetime]:
    u = url.lower()

    m = _RE_YMD.search(u)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except Exception:
            return None

    m = _RE_YM.search(u)
    if m:
        y, mo = map(int, m.groups())
        try:
            return datetime(y, mo, 1, tzinfo=timezone.utc)
        except Exception:
            return None

    return None


def _date_from_html(html: str) -> Optional[datetime]:
    soup = BeautifulSoup(html, "html.parser")

    # Common meta patterns
    meta_keys = [
        ("property", "article:published_time"),
        ("name", "date"),
        ("name", "pubdate"),
        ("name", "publication_date"),
        ("name", "DC.date"),
        ("name", "DC.Date"),
        ("itemprop", "datePublished"),
    ]
    for attr, key in meta_keys:
        tag = soup.find("meta", attrs={attr: key})
        if tag and tag.get("content"):
            dt = _parse_date_any(tag["content"])
            if dt:
                return dt

    # <time datetime="...">
    t = soup.find("time")
    if t:
        if t.get("datetime"):
            dt = _parse_date_any(t["datetime"])
            if dt:
                return dt
        # fallback: visible time text
        dt = _parse_date_any(t.get_text(" ", strip=True))
        if dt:
            return dt

    return None


# ---------------------- RSS ----------------------
def fetch_rss(url: str, *, source_name: str = "") -> List[Item]:
    resp = _http_get(url)
    resp.raise_for_status()

    parsed = feedparser.parse(resp.content)
    out: List[Item] = []

    for e in (parsed.entries or [])[:MAX_RSS_ITEMS]:
        link = (getattr(e, "link", "") or "").strip()
        if not link:
            continue

        title = (getattr(e, "title", "") or "").strip()
        summary = (getattr(e, "summary", "") or "").strip()

        # published / updated
        published = getattr(e, "published", None) or getattr(e, "updated", None) or ""
        dt = _parse_date_any(published) if published else None

        out.append(Item(url=link, title=title, summary=summary, source=source_name or url, published_ts=dt))

    return out


# ---------------------- HTML index crawling ----------------------
def _extract_links(index_url: str, html: str) -> Tuple[List[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        u = _normalize_url(index_url, href)
        # ignore obvious junk
        if u.startswith("mailto:") or u.startswith("javascript:"):
            continue
        links.append(u)

    # find a "next" page (rel=next or anchor text)
    next_url = None
    ln = soup.find("a", attrs={"rel": "next"}, href=True)
    if ln:
        next_url = _normalize_url(index_url, ln["href"])
    else:
        for a in soup.find_all("a", href=True):
            txt = (a.get_text(" ", strip=True) or "").lower()
            if txt in {"next", "older", "older posts", "more"}:
                next_url = _normalize_url(index_url, a["href"])
                break

    return links, next_url


def fetch_html_index(url: str, *, source_name: str = "") -> List[Item]:
    # Crawl up to MAX_INDEX_PAGES pages; collect up to MAX_LINKS_PER_INDEX links
    seen_pages: Set[str] = set()
    seen_links: Set[str] = set()
    collected: List[str] = []

    page_url = url
    for _ in range(MAX_INDEX_PAGES):
        if not page_url or page_url in seen_pages:
            break
        seen_pages.add(page_url)

        resp = _http_get(page_url)
        resp.raise_for_status()
        html = _safe_read_text(resp)

        links, next_url = _extract_links(page_url, html)

        # Keep only same-host links by default (prevents runaway crawling)
        for u in links:
            if not _same_host(url, u):
                continue
            if u in seen_links:
                continue
            seen_links.add(u)
            collected.append(u)
            if len(collected) >= MAX_LINKS_PER_INDEX:
                break

        if len(collected) >= MAX_LINKS_PER_INDEX:
            break

        page_url = next_url

    # Convert links into Items; resolve dates cheaply first (URL), then limited HTML fetches
    items: List[Item] = []
    unresolved: List[Item] = []

    for u in collected:
        dt = _date_from_url(u)
        it = Item(url=u, title="", summary="", source=source_name or url, published_ts=dt)
        items.append(it)
        if dt is None:
            unresolved.append(it)

    # Resolve up to MAX_DATE_RESOLVE_FETCHES_PER_INDEX by fetching the target page header HTML
    budget = min(MAX_DATE_RESOLVE_FETCHES_PER_INDEX, len(unresolved))
    for it in unresolved[:budget]:
        try:
            r = _http_get(it.url)
            r.raise_for_status()
            html = _safe_read_text(r)

            it.title = (BeautifulSoup(html, "html.parser").title.get_text(strip=True) if BeautifulSoup(html, "html.parser").title else "")[:300]
            it.published_ts = _date_from_html(html) or it.published_ts
        except Exception:
            # leave undated; generate_monthly can decide to drop based on ALLOW_UNDATED / in_range
            pass

    return items


# ---------------------- Full text extraction ----------------------
def _is_pdf_url(url: str) -> bool:
    u = url.lower()
    return u.endswith(".pdf") or "application/pdf" in u


def fetch_full_text(url: str) -> str:
    # PDF path: bounded download + PyMuPDF text extraction
    if _is_pdf_url(url):
        r = _http_get(url, stream=True)
        r.raise_for_status()

        buf = bytearray()
        total = 0
        for chunk in r.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            buf.extend(chunk)
            total += len(chunk)
            if total > MAX_PDF_BYTES:
                break

        try:
            doc = fitz.open(stream=bytes(buf), filetype="pdf")
            text_parts = []
            for page in doc:
                text_parts.append(page.get_text("text"))
            return "\n".join(text_parts).strip()
        except Exception:
            return ""

    # HTML path: prefer trafilatura, fallback to bs4 text
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            extracted = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
            if extracted and extracted.strip():
                return extracted.strip()
    except Exception:
        pass

    try:
        r = _http_get(url)
        r.raise_for_status()
        html = _safe_read_text(r)
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text("\n", strip=True)
    except Exception:
        return ""
