# src/fetch.py
from __future__ import annotations

import calendar
import os
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional, Tuple

import feedparser
import requests
import trafilatura
from bs4 import BeautifulSoup
from dateutil import parser as dateparse

# Optional MIME check (best-effort)
try:
    import magic  # python-magic / libmagic
except Exception:  # pragma: no cover
    magic = None

# PDF extraction (PyMuPDF)
try:
    import fitz  # PyMuPDF
except Exception as e:
    raise RuntimeError(
        "PyMuPDF (fitz) is required for hybrid PDF extraction. "
        "Add 'pymupdf' to requirements.txt."
    ) from e


DEBUG = os.getenv("DEBUG", "0") == "1"
TARGET_YM = os.getenv("TARGET_YM", "")  # e.g. "2025-02"

UA = "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"
DEFAULT_HEADERS = {"User-Agent": UA}

MAX_PDF_BYTES = int(os.getenv("MAX_PDF_BYTES", "5242880"))  # 5 MB
PDF_TRUSTED = tuple(
    d.strip()
    for d in os.getenv(
        "PDF_TRUSTED",
        "aemo.com.au,ifrs.org,efrag.org,dcceew.gov.au,arena.gov.au,cefc.com.au,irena.org",
    ).split(",")
    if d.strip()
)

# Domain → CSS selectors to find likely article cards/links
SELECTORS = {
    "aemo.com.au": ".newsroom a, a[href*='/newsroom/'], a[href*='/news/']",
    "aemc.gov.au": "a[href*='/news-centre/'], a[href*='/news/']",
    "aer.gov.au": "a[href*='/news']",
    "cer.gov.au": "a[href*='/news-and-media/news']",
    "arena.gov.au": "a.card, a[href*='/news/'], a[href*='/funding/']",
    "cefc.com.au": "a[href*='/media/'], a[href*='news']",
    "dcceew.gov.au": "a[href*='/news'], a[href*='/news-media/'], a[href*='/energy/news']",
    "energynetworks.com.au": "a[href*='/news/']",
    "infrastructureaustralia.gov.au": "a[href*='/news-media/']",
    "iea.org": "a[href*='/news']",
    "irena.org": "a[href*='/news'], a[href*='/Newsroom/Articles']",
    "ifrs.org": "a[href*='/news-and-events/']",
    "efrag.org": "a[href*='/news'], a[href*='/updates']",
    "globalreporting.org": "a[href*='/news/']",
    "fsb-tcfd.org": "a[href*='/publications/']",
    "asic.gov.au": "a[href*='/news-centre/']",
    "commission.europa.eu": "a[href*='presscorner'], a[href*='/presscorner/']",
    "ec.europa.eu": "a[href*='presscorner'], a[href*='/presscorner/']",
}

# Domains that often encode year/month in URL paths
MONTHY_URL_DOMAINS = {
    "irena.org",
    "energynetworks.com.au",
    "arena.gov.au",
    "aemc.gov.au",
    "aemo.com.au",
    "cefc.com.au",
    "iea.org",
    "efrag.org",
    "ifrs.org",
}


# ---------------- Models ----------------

@dataclass
class Item:
    title: str
    url: str
    source: str  # where we found the link (RSS or index page URL)
    published_ts: Optional[float]
    summary: str
    text: str = ""  # filled later by fetch_full_text


# -------------- Helpers --------------

def _netloc(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def _log(msg: str) -> None:
    if DEBUG:
        print(msg)


def _safe_get(url: str, timeout: int = 30) -> requests.Response:
    return requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)


def _unique_by_url(items: List[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        if not it.url or it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _month_hint_from_url(url: str) -> Optional[Tuple[int, int]]:
    """
    Returns (year, month) if URL path strongly suggests a year/month.
    Used only as a hint, not an absolute filter.
    """
    dom = _netloc(url)
    if dom not in MONTHY_URL_DOMAINS:
        return None

    path = urllib.parse.urlparse(url).path.lower()

    m = re.search(r"/(20\d{2})[/-](0?[1-9]|1[0-2])(?:/|$)", path)
    if m:
        return int(m.group(1)), int(m.group(2))

    m = re.search(r"/(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*[-/](20\d{2})(?:/|$)", path)
    if m:
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
        }
        return int(m.group(2)), month_map[m.group(1)]

    return None


def _parse_date_any(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        dt = dateparse.parse(s)
        if not dt:
            return None
        if not dt.tzinfo:
            # treat as UTC if missing tz
            return dt.replace(tzinfo=calendar.timegm(time.gmtime(0)).__class__).timestamp()  # not used, but keep safe
        return dt.timestamp()
    except Exception:
        return None


def _extract_pdf_text_bytes(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    parts: List[str] = []
    for page in doc:
        parts.append(page.get_text("text"))
    doc.close()
    return "\n".join(p.strip() for p in parts if p.strip())


# -------------- Public API used by generate_monthly --------------

def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> List[Item]:
    """
    Fetch items from an RSS/Atom feed.
    Accepts source_name for compatibility with generate_monthly.
    """
    label = source_name or url
    resp = _safe_get(url)
    resp.raise_for_status()

    feed = feedparser.parse(resp.content)
    items: List[Item] = []

    for e in getattr(feed, "entries", []):
        link = (getattr(e, "link", "") or "").strip()
        title = (getattr(e, "title", "") or "").strip()
        summary = (getattr(e, "summary", "") or "").strip()

        published_ts: Optional[float] = None
        if getattr(e, "published_parsed", None):
            try:
                published_ts = float(time.mktime(e.published_parsed))
            except Exception:
                published_ts = None
        elif getattr(e, "updated", None):
            published_ts = _parse_date_any(getattr(e, "updated", ""))

        if not link:
            continue

        items.append(
            Item(
                title=title[:200] if title else link,
                url=link,
                source=url,
                published_ts=published_ts,
                summary=summary[:2000],
            )
        )

    _log(f"[rss] {label}: {len(items)} items")
    return _unique_by_url(items)


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> List[Item]:
    """
    Fetch candidate article links from an HTML 'news index' page.
    Accepts source_name for compatibility with generate_monthly.
    """
    label = source_name or url
    resp = _safe_get(url)
    resp.raise_for_status()

    dom = _netloc(url)
    sel = SELECTORS.get(dom, "a[href]")

    soup = BeautifulSoup(resp.text, "html.parser")
    items: List[Item] = []

    for a in soup.select(sel):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        abs_url = urllib.parse.urljoin(url, href)
        if not abs_url.startswith("http"):
            continue

        text = (a.get_text(" ", strip=True) or "").strip()
        if not text:
            continue

        # light hygiene: skip obvious non-articles
        if any(x in abs_url.lower() for x in ["#","javascript:","/contact","/privacy","/terms"]):
            continue

        items.append(
            Item(
                title=text[:200],
                url=abs_url,
                source=url,
                published_ts=None,
                summary="",
            )
        )

    items = _unique_by_url(items)
    _log(f"[html_index] {label}: {len(items)} links")
    return items


def fetch_full_text(url: str, timeout_s: int = 30) -> str:
    """
    Fetch full text for an article.
    - If PDF and trusted/within size: extract text with PyMuPDF.
    - Else: use trafilatura.
    """
    dom = _netloc(url)

    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout_s, stream=True)
    resp.raise_for_status()

    content_type = (resp.headers.get("Content-Type") or "").lower()
    raw = resp.content

    # MIME sniffing (optional)
    if magic is not None:
        try:
            sniff = magic.from_buffer(raw[:2048], mime=True) or ""
            if sniff:
                content_type = sniff.lower()
        except Exception:
            pass

    is_pdf = ("application/pdf" in content_type) or url.lower().endswith(".pdf")

    if is_pdf:
        if dom in PDF_TRUSTED and len(raw) <= MAX_PDF_BYTES:
            try:
                return _extract_pdf_text_bytes(raw)
            except Exception as ex:
                _log(f"[pdf] extraction failed {url}: {ex}")
                # fall back to trafilatura on bytes->str if possible
        else:
            _log(f"[pdf] skipped (untrusted or too large) {url} size={len(raw)} dom={dom}")

    # HTML / general: trafilatura
    try:
        html = raw.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        html = raw.decode("utf-8", errors="replace")

    text = trafilatura.extract(html, url=url, include_comments=False, include_tables=False) or ""
    return text.strip()
