import os
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional

import requests
import feedparser
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

import calendar  # NEW

DEBUG = os.getenv("DEBUG", "0") == "1"           # NEW
TARGET_YM = os.getenv("TARGET_YM", "")           # NEW, e.g., "2025-02"

# Domains that commonly encode year/month in their URLs
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
    source: str          # where we found the link (RSS or index page URL)
    published_ts: Optional[float]
    summary: str
    text: str = ""       # filled later by fetch_full_text


# -------------- Config / Constants --------------

MAX_PDF_BYTES = int(os.getenv("MAX_PDF_BYTES", "5242880"))  # 5 MB
PDF_TRUSTED = tuple(d.strip() for d in os.getenv(
    "PDF_TRUSTED",
    "aemo.com.au,ifrs.org,efrag.org,dcceew.gov.au,arena.gov.au,cefc.com.au,irena.org"
).split(","))

# Domain → CSS selectors to find likely article cards/links
SELECTORS = {
    "aemo.com.au": ".newsroom a, a[href*='/newsroom/'], a[href*='/news/']",
    "aemc.gov.au": "a[href*='/news-centre/'], a[href*='/news/']",
    "aer.gov.au": "a[href*='/news']",
    "cer.gov.au": "a[href*='/news-and-media/news']",
    "arena.gov.au": "a.card, a[href*='/news/'], a[href*='/funding/']",
    "cefc.com.au": "a[href*='/media/'], a[href*='news']",
    "dcceew.gov.au": "a[href*='/news'], a[href*='/news-media/']",
    "energynetworks.com.au": "a[href*='/news/']",
    "infrastructureaustralia.gov.au": "a[href*='/news-media/']",
    "iea.org": "a[href*='/news']",
    "irena.org": "a[href*='/news'], a[href*='/Newsroom/Articles']",
    "ifrs.org": "a[href*='/news-and-events/news/'], a[href*='/updates/issb/']",
    "efrag.org": "a[href*='/news'], a[href*='/updates']",
    "globalreporting.org": "a[href*='/news/']",
    "fsb-tcfd.org": "a[href*='/publications']",
    "asic.gov.au": "a[href*='/news-centre']",
    "australianinvestmentcouncil.com.au": "a[href*='/news']",
    "bnef.com": "a[href*='/blog/']",
}


# -------------- Helpers --------------

def _normalize_url(u: str) -> str:
    """Strip utm_* and fragments for dedupe/canonicalization."""
    try:
        p = urllib.parse.urlparse(u)
        q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        q = [(k, v) for (k, v) in q if not k.lower().startswith("utm_")]
        new_q = urllib.parse.urlencode(q)
        p = p._replace(query=new_q, fragment="")
        return urllib.parse.urlunparse(p)
    except Exception:
        return u

def _guess_published_ts(soup: BeautifulSoup) -> Optional[float]:
    # Common meta tags
    for sel, attr in [
        ("meta[property='article:published_time']", "content"),
        ("meta[name='pubdate']", "content"),
        ("meta[name='date']", "content"),
        ("meta[property='og:updated_time']", "content"),
        ("meta[itemprop='datePublished']", "content"),
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get(attr):
            try:
                return dateparse.parse(tag.get(attr)).timestamp()
            except Exception:
                pass

    # time tag
    t = soup.find("time")
    if t and (t.get("datetime") or t.text):
        try:
            return dateparse.parse(t.get("datetime") or t.text).timestamp()
        except Exception:
            pass

    # textual YYYY-MM-DD/ YYYY/MM/DD somewhere on page
    m = re.search(r"(20\d{2}[-/.](0[1-9]|1[0-2])[-/.]([0-2]\d|3[01]))", soup.get_text(" ", strip=True))
    if m:
        try:
            return dateparse.parse(m.group(1)).timestamp()
        except Exception:
            pass

    # Labels like "Published: 12 March 2025"
    label = soup.find(string=re.compile(r"\b(Published|Updated)\b", re.I))
    if label:
        try:
            return dateparse.parse(str(label)).timestamp()
        except Exception:
            pass

    return None

def _download(url: str, headers=None, max_bytes=None) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=headers or {"User-Agent": "gg-advisory-bot/1.0"}, timeout=45, stream=True)
        r.raise_for_status()
        chunks, size = [], 0
        for chunk in r.iter_content(1024 * 64):
            if not chunk:
                break
            size += len(chunk)
            if max_bytes and size > max_bytes:
                return None
            chunks.append(chunk)
        return b"".join(chunks)
    except Exception:
        return None

def _is_trusted_pdf(url: str) -> bool:
    try:
        u = urllib.parse.urlparse(url)
        if not (u.scheme and u.netloc):
            return False
        if not u.path.lower().endswith(".pdf"):
            return False
        return any(u.netloc.endswith(dom) for dom in PDF_TRUSTED)
    except Exception:
        return False

def _extract_pdf_bytes(pdf_bytes: bytes, max_pages: int = 5) -> str:
    try:
        if magic:
            kind = magic.from_buffer(pdf_bytes, mime=True)
            if kind and kind != "application/pdf":
                return ""
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            pages = min(max_pages, len(doc))
            text = []
            for i in range(pages):
                text.append(doc.load_page(i).get_text("text"))
            return "\n".join(text).strip()
    except Exception:
        return ""
def _month_hint_ok(domain: str, path: str, target_ym: str) -> bool:
    """
    Cheap URL-based pre-filter to skip links that are obviously outside the
    target month (based on URL patterns). Conservative:
      - Only enforces on domains known to embed months in paths
      - Returns True by default when unsure (lets date extraction enforce final range)
    """
    if not target_ym or not path:
        return True

    domain = (domain or "").lower()
    if domain not in MONTHY_URL_DOMAINS:
        return True

    # Parse target year and month
    try:
        y_str, m_str = target_ym.split("-")
        y = int(y_str)
        m = int(m_str)
        mon_abbr = calendar.month_abbr[m]  # e.g., "Feb"
    except Exception:
        return True

    # Domain-specific pattern: IRENA uses /YYYY/Mon/
    if domain == "irena.org":
        ok = f"/{y}/{mon_abbr}/" in path
        if DEBUG and not ok:
            print(f"[prefilter-skip-month] {domain}{path} (expected /{y}/{mon_abbr}/)")
        return ok

    # Generic patterns: /YYYY/MM/ or /YYYY-MM
    if re.search(rf"/{y}/0?{m}/", path):
        return True
    if re.search(rf"/{y}-{m:02d}\b", path):
        return True

    # Unsure? allow it; final range filter will catch it later.
    return True


# -------------- Fetchers --------------

def fetch_rss(url: str) -> List[Item]:
    """Robust RSS fetch using requests + feedparser."""
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
            time.sleep(1.2 * attempts)
    if data is None:
        return out

    parsed = feedparser.parse(data)
    for e in (parsed.entries or []):
        link = e.get("link") or e.get("id")
        if not link:
            continue
        link = _normalize_url(link)
        # RSS usually provides published_parsed; if missing, we skip to avoid wrong-month leakage
        ts = None
        try:
            if getattr(e, "published_parsed", None):
                ts = time.mktime(e.published_parsed)
            elif getattr(e, "updated_parsed", None):
                ts = time.mktime(e.updated_parsed)
        except Exception:
            ts = None
        if ts is None:
            continue

        out.append(Item(
            title=(e.get("title") or "").strip(),
            url=link,
            source=url,
            published_ts=ts,
            summary=(e.get("summary") or "").strip()
        ))
    return out

def fetch_html_index(url: str) -> List[Item]:
    """Fetch a listing page; extract likely article links; then fetch each to capture a reliable date."""
    try:
        r = requests.get(url, timeout=30, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(r.text, "html.parser")

    domain = urllib.parse.urlparse(url).netloc.lower()
    sel = SELECTORS.get(domain, "article a, .news a, a[href*='news'], a[href*='press'], a[href*='media']")

    # Domain-specific guardrails for listing-heavy sites
    LISTING_ENDPOINTS = {
        "/news", "/newsroom", "/media", "/press", "/publications",
        "/updates", "/news-and-calendar/news", "/news-centre",
        "/about-asic/news-centre", "/publications"
    }
    REQUIRE_DETAIL = {
        # Require detail pages
        "efrag.org": re.compile(r"/news-and-calendar/news/[^/?#]+", re.I),
        "ec.europa.eu": re.compile(r"/presscorner/detail/[^/?#]+/[^/?#]+", re.I),
        "commission.europa.eu": re.compile(r"/presscorner/detail/[^/?#]+/[^/?#]+", re.I),
        "energynetworks.com.au": re.compile(r"/news/(media-releases|energy-insider)/[^/?#]+", re.I),
        "aemc.gov.au": re.compile(r"/news-centre/[^/?#]+", re.I),
        "aemo.com.au": re.compile(r"/news(room|)/[^/?#]+", re.I),
        "arena.gov.au": re.compile(r"/news/[^/?#]+", re.I),
        "cefc.com.au": re.compile(r"/media/[^/?#]+", re.I),
        "asic.gov.au": re.compile(r"/about-asic/news-centre/[^/?#]+", re.I),
    }
    DROP_QUERIES_ON = {
        "efrag.org", "irena.org", "energynetworks.com.au",
        "globalreporting.org", "aemc.gov.au"
    }

    candidates = []
    for a in soup.select(sel):
        href = a.get("href")
        if not href:
            continue

        # absolute URL
        if href.startswith("/"):
            base = f"{urllib.parse.urlparse(url).scheme}://{domain}"
            href = urllib.parse.urljoin(base, href)
        href = _normalize_url(href)

        parsed = urllib.parse.urlparse(href)
        link_domain = parsed.netloc.lower()
        path = (parsed.path or "/")
        
        # ✅ Month hint prefilter using the LINK'S domain, not the source page's
        if not _month_hint_ok(link_domain, path, os.getenv("TARGET_YM", "")):
            if DEBUG:
                print(f"[prefilter-skip-month] {href}")
            continue

        
        # ✅ Use LINK domain for the month hint filter
        if not _month_hint_ok(link_domain, path, os.getenv("TARGET_YM","")):
            if DEBUG:
                print(f"[prefilter-skip-month] {href}")
            continue


        # 0) Skip same-page anchors
        if parsed.fragment:
            continue

        # 1) Skip hub endpoints
        if path.rstrip("/") in LISTING_ENDPOINTS:
            continue

        # 2) Drop query links on listing-heavy domains (filters/pagination)
        if parsed.query and domain in DROP_QUERIES_ON:
            continue

        # 3) Require detail shape on specific domains
        req = REQUIRE_DETAIL.get(domain)
        if req and not req.search(path):
            continue

        title = (a.get_text(" ", strip=True) or "").strip()
        if len(title) < 12:
            continue

        candidates.append((title, href))

    # de-dupe
    uniq = {}
    for title, href in candidates:
        if href not in uniq:
            uniq[href] = title

    items: List[Item] = []
    for href, title in list(uniq.items())[:120]:
        try:
            ar = requests.get(href, timeout=30, allow_redirects=True)
            ar.raise_for_status()
        except Exception:
            continue
        asoup = BeautifulSoup(ar.text, "html.parser")

        # Canonical sanity check: if canonical points to a hub, skip
        canon = asoup.find("link", rel=lambda v: v and v.lower() == "canonical")
        if canon and canon.get("href"):
            chref = canon["href"]
            try:
                cparsed = urllib.parse.urlparse(chref)
                if (cparsed.netloc.lower() == domain and
                    (cparsed.path or "/").rstrip("/") in LISTING_ENDPOINTS):
                    continue
            except Exception:
                pass

        ts = _guess_published_ts(asoup)

        # Fallback: parse YYYY/MM/DD in URL
        if ts is None:
            m = re.search(r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/", href)
            if m:
                from datetime import datetime, timezone
                try:
                    ts = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).replace(
                        tzinfo=timezone.utc
                    ).timestamp()
                except Exception:
                    ts = None

        # Still unknown date? skip to avoid wrong-month leakage
        if ts is None:
            continue

        if not title:
            page_t = asoup.title.string if asoup.title else ""
            title = (page_t or href.split("/")[-1].replace("-", " "))[:140]

        summary = trafilatura.extract(ar.text, include_formatting=False, include_links=False) or ""

        items.append(Item(
            title=title.strip(),
            url=href,
            source=url,
            published_ts=ts,
            summary=(summary or "").strip()
        ))

    items.sort(key=lambda x: (x.published_ts or 0), reverse=True)
    return items


def fetch_full_text(u: str) -> str:
    """Pull main-page text, then append first pages of any trusted PDF linked from the page."""
    base_text = ""
    try:
        fetched = trafilatura.fetch_url(u, no_ssl=True)
        base_text = trafilatura.extract(fetched, include_formatting=False, include_links=False) or ""
        base_text = (base_text or "").strip()
    except Exception:
        base_text = ""

    # Try to find a trusted PDF on the page and append first 3–5 pages of text
    try:
        r = requests.get(u, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[href$='.pdf']"):
            href = a.get("href") or ""
            if not href:
                continue
            if href.startswith("/"):
                parsed = urllib.parse.urlparse(u)
                href = f"{parsed.scheme}://{parsed.netloc}{href}"
            if not _is_trusted_pdf(href):
                continue
            pdf_bytes = _download(href, max_bytes=MAX_PDF_BYTES)
            if not pdf_bytes:
                continue
            if magic:
                kind = magic.from_buffer(pdf_bytes, mime=True)
                if kind and kind != "application/pdf":
                    continue
            snippet = _extract_pdf_bytes(pdf_bytes, max_pages=5)
            if snippet and len(snippet) > 400:
                return (base_text + "\n\n[PDF excerpt]\n" + snippet).strip()
    except Exception:
        pass

    return base_text
