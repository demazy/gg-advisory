import io, os, re, time, mimetypes, urllib.parse
from dataclasses import dataclass
from typing import List, Optional

import requests
import feedparser
import trafilatura
from bs4 import BeautifulSoup
from dateutil import parser as dateparse

# --- Optional MIME check (best effort) ---
try:
    import magic  # python-magic / libmagic
except Exception:  # pragma: no cover
    magic = None

# --- PDF extraction (PyMuPDF) ---
try:
    import fitz  # PyMuPDF
except Exception as e:
    raise RuntimeError(
        "PyMuPDF (fitz) is required for hybrid PDF extraction. "
        "Add 'pymupdf' to requirements.txt."
    ) from e

# ---------------- Models ----------------

@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: float
    summary: str
    text: str = ""

# ---------------- Config ----------------

MAX_PDF_BYTES = int(os.getenv("MAX_PDF_BYTES", "5242880"))  # 5 MB
PDF_TRUSTED = tuple(d.strip() for d in os.getenv(
    "PDF_TRUSTED",
    "aemo.com.au,ifrs.org,efrag.org,dcceew.gov.au,arena.gov.au,cefc.com.au,irena.org"
).split(","))

# Domain → CSS selectors to find real article cards/links
SELECTORS = {
    "ec.europa.eu": "a[href*='/presscorner/']",
    "ifrs.org": "a[href*='/news-and-events/news/'], a[href*='/updates/issb/']",
    "efrag.org": "a[href*='/news'], a[href*='/updates']",
    "aemo.com.au": ".newsroom a, a[href*='/newsroom/']",
    "arena.gov.au": "a.card, a[href*='/news/'], a[href*='/funding/']",
    "cefc.com.au": "a[href*='/media/'], a[href*='news']",
    "dcceew.gov.au": "a[href*='/news-media/'], a[href*='/news/']",
    "aemc.gov.au": "a[href*='/news-centre/'], a[href*='/news/']",
    "aer.gov.au": "a[href*='/news']",
    "cer.gov.au": "a[href*='/news-and-media/news']",
    "energynetworks.com.au": "a[href*='/news/']",
    "infrastructureaustralia.gov.au": "a[href*='/news-media/']",
    "csiro.au": "a[href*='/news']",
    "iea.org": "a[href*='/news']",
    "irena.org": "a[href*='/news'], a[href*='/Newsroom/Articles']",
    "fsb-tcfd.org": "a[href*='/publications']",
    "globalreporting.org": "a[href*='/news/']",
    "asic.gov.au": "a[href*='/news-centre']",
}

# -------------- Helpers -----------------

def _ts_or_now(entry) -> float:
    try:
        return time.mktime(entry.published_parsed)
    except Exception:
        return time.time()

def _normalize_url(u: str) -> str:
    """Strip utm_* and fragments for dedupe/canonicals."""
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
    """Find publish date from common meta/time tags."""
    for sel, attr in [
        ("meta[property='article:published_time']", "content"),
        ("meta[name='pubdate']", "content"),
        ("meta[name='date']", "content"),
        ("time[datetime]", "datetime"),
        ("meta[itemprop='datePublished']", "content"),
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get(attr):
            try:
                dt = dateparse.parse(tag.get(attr))
                return dt.timestamp()
            except Exception:
                pass
    for t in soup.find_all("time"):
        try:
            dt = dateparse.parse(t.get_text(strip=True))
            return dt.timestamp()
        except Exception:
            continue
    return None

def _download(url: str, headers=None, max_bytes=None) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=headers or {"User-Agent": "gg-advisory-bot/1.0"}, timeout=45, stream=True)
        r.raise_for_status()
        chunks, size = [], 0
        for chunk in r.iter_content(1024 * 64):
            if not chunk: break
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

# -------------- Fetchers ----------------

def fetch_rss(url: str) -> List[Item]:
    """Robust RSS fetch with headers + backoff, parsed via feedparser from bytes."""
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
            time.sleep(1.5 * attempts)
    if data is None:
        return out

    parsed = feedparser.parse(data)
    for e in (parsed.entries or []):
        link = e.get("link") or e.get("id")
        if not link:
            continue
        link = _normalize_url(link)
        out.append(Item(
            title=(e.get("title") or "").strip(),
            url=link,
            source=url,
            published_ts=_ts_or_now(e),
            summary=(e.get("summary") or "").strip()
        ))
    return out

def fetch_html_index(url: str) -> List[Item]:
    """Fetch a listing page; extract likely article links via domain selectors; fetch each to capture date."""
    try:
        r = requests.get(url, timeout=30, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(r.text, "html.parser")

    domain = urllib.parse.urlparse(url).netloc.lower()
    sel = SELECTORS.get(domain, "article a, .news a, a[href*='news'], a[href*='press']")

    candidates = []
    for a in soup.select(sel):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            base = f"{urllib.parse.urlparse(url).scheme}://{domain}"
            href = urllib.parse.urljoin(base, href)
        href = _normalize_url(href)

        # Skip listing/category endpoints
        if re.search(r"(category|tags|/newsroom$|/news$|/media$|/press$)", href, re.I):
            continue

        title = (a.get_text(" ", strip=True) or "").strip()
        candidates.append((title, href))

    uniq = {}
    for title, href in candidates:
        if href not in uniq:
            uniq[href] = title

    items: List[Item] = []
    for href, title in list(uniq.items())[:60]:
        try:
            ar = requests.get(href, timeout=30, allow_redirects=True)
            ar.raise_for_status()
        except Exception:
            continue
        asoup = BeautifulSoup(ar.text, "html.parser")
        ts = _guess_published_ts(asoup) or time.time()

        if not title:
            page_t = asoup.title.string if asoup.title else ""
            title = (page_t or href.split("/")[-1].replace("-", " "))[:140]

        items.append(Item(
            title=title.strip(),
            url=href,
            source=url,
            published_ts=ts,
            summary=""
        ))
    items.sort(key=lambda x: x.published_ts, reverse=True)
    return items

def fetch_full_text(u: str) -> str:
    """Pull main-page text, then (hybrid) append first pages of any trusted PDF linked from the page."""
    try:
        base = trafilatura.fetch_url(u, no_ssl=True)
        base_text = trafilatura.extract(base, include_formatting=False, include_links=False) or ""
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
            # quick MIME sanity
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
