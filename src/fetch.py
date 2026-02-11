"""src/fetch.py

Robust content fetching + metadata extraction for GG Advisory digests.

Key design choices (A + B):
- A (hub avoidance): aggressively filter out taxonomy/landing/index URLs early
  to reduce the chance of selecting evergreen pages.
- B (date hygiene): only accept publication dates from strong signals
  (JSON-LD datePublished, article:published_time, explicit <time> datePublished, etc.).
  Ignore generic "date" meta tags and treat "updated" dates as NOT publication dates.
  Reject partial dates (e.g., '2026' -> 2026-01-01) to prevent false in-range items.

All functions are best-effort: they should not raise on network/parse failures.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import trafilatura
from trafilatura.settings import DEFAULT_CONFIG

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None


# ---------------------------- Config ----------------------------

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36 gg-advisory-bot/1.0",
)

REQUEST_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECS", "25"))
CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT_SECS", "15"))
MAX_BYTES = int(os.getenv("MAX_BYTES", str(6 * 1024 * 1024)))
MAX_PDF_BYTES = int(os.getenv("MAX_PDF_BYTES", str(5 * 1024 * 1024)))

# Limit expensive per-link date resolution fetches from index pages
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "75"))

# When true, allow undated items downstream (generate_monthly enforces this)
ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "0").strip().lower() in ("1", "true", "yes")


# ---------------------------- Model ----------------------------

@dataclass
class Item:
    url: str
    title: str = ""
    summary: str = ""
    source: str = ""

    published_ts: Optional[float] = None  # UTC timestamp
    published_iso: str = ""  # YYYY-MM-DD (best-effort)
    published_source: str = ""  # e.g. 'jsonld:datePublished'
    published_confidence: str = "none"  # high|medium|low|none

    # for debugging / provenance
    index_url: str = ""


# ---------------------------- HTTP ----------------------------

def _make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=4,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-AU,en;q=0.9,fr;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return sess


_SESSION = _make_session()


def http_get(url: str, *, stream: bool = False, timeout: Optional[float] = None) -> Optional[requests.Response]:
    """Best-effort GET with sane defaults. Returns None on hard failures."""
    if not url:
        return None
    try:
        r = _SESSION.get(
            url,
            timeout=(CONNECT_TIMEOUT, timeout or REQUEST_TIMEOUT),
            allow_redirects=True,
            stream=stream,
        )
        # Some sites return 403/404 but with useful bodies; caller decides.
        return r
    except Exception:
        return None


def _content_type(r: requests.Response) -> str:
    return (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()


# ---------------------------- URL helpers ----------------------------

_TAXONOMY_PAT = re.compile(
    r"/(tag|tags|topic|topics|category|categories|taxonomy|themes|theme|author|authors|search|sitemap|events|calendar)(/|$)",
    re.I,
)

_LOW_VALUE_TAIL = {
    "", "home", "index", "default", "overview", "about", "what-we-do", "who-we-are",
    "media", "news", "newsroom", "press", "publications", "resources", "insights", "updates",
    "funding", "grants", "invest", "investment", "investments",
}

def normalise_url(url: str, base: str = "") -> str:
    if not url:
        return ""
    u = url.strip()
    if base:
        u = urllib.parse.urljoin(base, u)
    p = urllib.parse.urlparse(u)
    # remove fragments
    p = p._replace(fragment="")
    # normalise scheme/host
    scheme = p.scheme or "https"
    netloc = p.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    # collapse multiple slashes in path
    path = re.sub(r"/{2,}", "/", p.path or "/")
    return urllib.parse.urlunparse((scheme, netloc, path, p.params, p.query, p.fragment))


def _path_tail(url: str) -> str:
    try:
        path = urllib.parse.urlparse(url).path or ""
        tail = path.rstrip("/").split("/")[-1].lower()
        return tail
    except Exception:
        return ""


def looks_like_pdf(url: str, ctype: str = "") -> bool:
    u = (url or "").lower()
    return u.endswith(".pdf") or ctype == "application/pdf"


def is_probably_taxonomy_or_hub(url: str) -> bool:
    """A conservative early filter for index/taxonomy pages (A)."""
    if not url:
        return True
    p = urllib.parse.urlparse(url)
    path = (p.path or "/").lower()
    # Queries are often index/pagination; allow only if obviously a document
    if p.query and not any(x in path for x in (".pdf", ".doc", ".docx")):
        if re.search(r"(page=|p=|type=|filter=|sort=|q=)", p.query, re.I):
            return True

    if _TAXONOMY_PAT.search(path):
        return True

    # root-ish / landing pages
    segs = [s for s in path.split("/") if s]
    tail = _path_tail(url)
    if len(segs) <= 1 and tail in _LOW_VALUE_TAIL:
        return True
    if tail in _LOW_VALUE_TAIL and len(segs) <= 2:
        return True

    return False


def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


# ---------------------------- Date parsing (B) ----------------------------

_RE_YEAR_ONLY = re.compile(r"^\s*(19|20)\d{2}\s*$")
_RE_YM_ONLY = re.compile(r"^\s*(19|20)\d{2}[-/](0[1-9]|1[0-2])\s*$")

def _parse_dt(value: str) -> Optional[datetime]:
    """Parse a date/time string to aware UTC datetime. Reject partial dates."""
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None

    # Reject year-only or year-month-only values that dtparser would coerce to Jan 1.
    if _RE_YEAR_ONLY.match(v) or _RE_YM_ONLY.match(v):
        return None

    try:
        dt = dtparser.parse(v, fuzzy=True)
    except Exception:
        return None

    if not dt:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # Reject obviously bogus dates
    now = datetime.now(timezone.utc)
    if dt > (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=3)):
        return None
    if dt.year < 2000:
        # You can loosen this if you truly expect older sources; for monthly signals it's usually noise.
        return None

    return dt


def _dt_to_item_fields(dt: datetime, source: str, confidence: str) -> Tuple[float, str, str, str]:
    ts = dt.timestamp()
    iso = dt.date().isoformat()
    return ts, iso, source, confidence


def _date_from_url(url: str) -> Optional[datetime]:
    """Extract a date from common URL patterns."""
    if not url:
        return None
    path = urllib.parse.urlparse(url).path
    if not path:
        return None

    # 2026/01/20 or 2026-01-20 etc
    m = re.search(r"(20\d{2})[/-](0[1-9]|1[0-2])[/-]([0-3]\d)", path)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except Exception:
            return None

    # 20260120
    m = re.search(r"(20\d{2})(0[1-9]|1[0-2])([0-3]\d)", path)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except Exception:
            return None

    return None


def _iter_jsonld_nodes(obj) -> Iterable[dict]:
    if isinstance(obj, dict):
        yield obj
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for it in obj["@graph"]:
                yield from _iter_jsonld_nodes(it)
    elif isinstance(obj, list):
        for it in obj:
            yield from _iter_jsonld_nodes(it)


def _jsonld_date_published(soup: BeautifulSoup) -> Optional[str]:
    for script in soup.find_all("script", attrs={"type": re.compile(r"application/ld\+json", re.I)}):
        txt = (script.string or script.get_text() or "").strip()
        if not txt:
            continue
        # Some sites embed multiple JSON blobs; try lenient parsing.
        candidates = []
        try:
            candidates.append(json.loads(txt))
        except Exception:
            # try to extract first {...} block(s)
            for m in re.finditer(r"\{.*?\}", txt, flags=re.S):
                try:
                    candidates.append(json.loads(m.group(0)))
                except Exception:
                    continue

        for obj in candidates:
            for node in _iter_jsonld_nodes(obj):
                t = node.get("@type")
                types = set([t] if isinstance(t, str) else (t or []))
                if not types.intersection({"Article", "NewsArticle", "BlogPosting", "Report"}):
                    continue
                dp = node.get("datePublished") or node.get("dateCreated")
                if isinstance(dp, str) and dp.strip():
                    return dp.strip()
    return None


def _extract_pubdate_from_html(html: str) -> Tuple[Optional[datetime], str, str]:
    """Return (datetime, confidence, source). Ignore 'updated' as publication."""
    if not html:
        return None, "none", ""

    soup = BeautifulSoup(html, "html.parser")

    # 1) Strongest: JSON-LD datePublished
    dp = _jsonld_date_published(soup)
    dt = _parse_dt(dp or "")
    if dt:
        return dt, "high", "jsonld:datePublished"

    # 2) Meta tags / OpenGraph / Article tags - strong signals
    meta_selectors = [
        ('meta[property="article:published_time"]', "high", "meta:article:published_time"),
        ('meta[property="og:published_time"]', "medium", "meta:og:published_time"),
        ('meta[name="parsely-pub-date"]', "medium", "meta:parsely-pub-date"),
        ('meta[name="pubdate"]', "medium", "meta:pubdate"),
        ('meta[name="publishdate"]', "medium", "meta:publishdate"),
        ('meta[itemprop="datePublished"]', "high", "meta:itemprop:datePublished"),
        ('meta[name="datePublished"]', "high", "meta:datePublished"),
    ]
    for sel, conf, src in meta_selectors:
        tag = soup.select_one(sel)
        if tag and tag.get("content"):
            dt = _parse_dt(tag.get("content", ""))
            if dt:
                return dt, conf, src

    # 3) <time> tags with publication hints
    for t in soup.find_all("time"):
        attrs = " ".join([str(v) for v in t.attrs.values()]) if t.attrs else ""
        attrs_l = attrs.lower()
        if "publish" in attrs_l or "datepublished" in attrs_l or "posted" in attrs_l:
            dt = _parse_dt(t.get("datetime") or t.get_text() or "")
            if dt:
                return dt, "high", "time:publish-hint"
    # fallback: any <time datetime=...> that is plausible
    for t in soup.find_all("time"):
        dt = _parse_dt(t.get("datetime") or "")
        if dt:
            return dt, "medium", "time:datetime"

    # 5) htmldate / heuristics (very weak). Use only if it yields a full date string and not Jan 1.
    try:
        from htmldate import find_date  # provided by trafilatura dependencies
        ds = find_date(html)
        dt = _parse_dt(ds or "")
        if dt:
            if dt.month == 1 and dt.day == 1:
                # very common false positive for year-only pages
                return None, "none", ""
            return dt, "low", "htmldate:find_date"
    except Exception:
        pass

    return None, "none", ""


# ---------------------------- RSS ----------------------------

def fetch_rss(url: str, source_name: str = "") -> List[Item]:
    url = normalise_url(url)
    out: List[Item] = []
    r = http_get(url, timeout=REQUEST_TIMEOUT)
    if not r or not r.text:
        return out

    feed = feedparser.parse(r.text)
    for e in feed.entries[:500]:
        link = normalise_url(getattr(e, "link", "") or "", base=url)
        if not link:
            continue
        title = (getattr(e, "title", "") or "").strip()
        summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "").strip()
        dt: Optional[datetime] = None
        for attr in ("published", "updated", "created"):
            v = getattr(e, attr, None)
            if v:
                dt = _parse_dt(v)
                if dt:
                    break
        if not dt and getattr(e, "published_parsed", None):
            try:
                dt = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
            except Exception:
                dt = None

        item = Item(url=link, title=title, summary=summary, source=source_name or url, index_url=url)
        if dt:
            item.published_ts, item.published_iso, item.published_source, item.published_confidence = _dt_to_item_fields(
                dt, "rss", "medium"
            )
        out.append(item)
    return out


# ---------------------------- HTML index scraping (A) ----------------------------

def _extract_index_candidates(html: str, index_url: str) -> List[Tuple[str, str, Optional[datetime]]]:
    """Return (url, title, hinted_date)."""
    soup = BeautifulSoup(html, "html.parser")
    base = index_url

    candidates: List[Tuple[str, str, Optional[datetime]]] = []

    # Prefer article-like blocks
    for art in soup.find_all(["article", "section", "div"], limit=600):
        cls = " ".join(art.get("class", [])).lower()
        if art.name != "article" and not any(k in cls for k in ("news", "media", "post", "article", "release", "update")):
            continue

        a = art.find("a", href=True)
        if not a:
            continue

        href = normalise_url(a.get("href", ""), base=base)
        if not href or href == base:
            continue

        title = (a.get_text(" ", strip=True) or "").strip()
        if not title:
            # try header text
            h = art.find(["h1", "h2", "h3", "h4"])
            if h:
                title = (h.get_text(" ", strip=True) or "").strip()

        hinted = None
        t = art.find("time")
        if t:
            hinted = _parse_dt(t.get("datetime") or t.get_text() or "")
        if not hinted:
            hinted = _date_from_url(href)

        candidates.append((href, title, hinted))

    # Fallback: all links inside main/content
    if len(candidates) < 20:
        scope = soup.find("main") or soup.find("body") or soup
        for a in scope.find_all("a", href=True, limit=3500):
            href = normalise_url(a.get("href", ""), base=base)
            if not href or href == base:
                continue
            title = (a.get_text(" ", strip=True) or "").strip()
            if len(title) < 6:
                continue
            hinted = _date_from_url(href)
            candidates.append((href, title, hinted))

    # Deduplicate by URL, keep first non-empty title
    seen = set()
    out: List[Tuple[str, str, Optional[datetime]]] = []
    for href, title, hinted in candidates:
        if href in seen:
            continue
        seen.add(href)
        out.append((href, title, hinted))
    return out


def fetch_html_index(url: str, source_name: str = "") -> List[Item]:
    """Fetch an HTML index page and return candidate Items (best-effort)."""
    index_url = normalise_url(url)
    out: List[Item] = []

    r = http_get(index_url, timeout=REQUEST_TIMEOUT)
    if not r or not r.text:
        return out

    ctype = _content_type(r)
    if looks_like_pdf(index_url, ctype):
        # Not an index, but allow downstream to treat as a document item.
        it = Item(url=index_url, title=_path_tail(index_url) or index_url, source=source_name or index_url, index_url=index_url)
        out.append(it)
        return out

    html = r.text
    candidates = _extract_index_candidates(html, index_url=index_url)

    resolve_budget = MAX_DATE_RESOLVE_FETCHES_PER_INDEX

    for href, title, hinted_dt in candidates:
        href = normalise_url(href)
        if not href:
            continue

        # Early hub/taxonomy rejection (A)
        if is_probably_taxonomy_or_hub(href):
            continue

        it = Item(url=href, title=title, source=source_name or index_url, index_url=index_url)

        # Date from URL or hinted date is decent
        dt = hinted_dt or _date_from_url(href)
        if dt:
            it.published_ts, it.published_iso, it.published_source, it.published_confidence = _dt_to_item_fields(
                dt, "url_or_index_hint", "medium"
            )
            out.append(it)
            continue

        # Only resolve expensive dates for URLs that look like articles/documents
        if resolve_budget <= 0:
            out.append(it)
            continue

        # Heuristic: avoid resolving dates for shallow/landing pages
        if is_probably_taxonomy_or_hub(href):
            out.append(it)
            continue

        # Fetch the candidate page to extract a publication date (B)
        rr = http_get(href, timeout=REQUEST_TIMEOUT)
        resolve_budget -= 1
        if not rr:
            out.append(it)
            continue

        dt2 = None
        conf = "none"
        src = ""
        ctype2 = _content_type(rr)
        if looks_like_pdf(href, ctype2):
            # Use Last-Modified as weak-ish fallback for PDFs
            lm = rr.headers.get("Last-Modified") or ""
            dt2 = _parse_dt(lm)
            if dt2:
                conf, src = "low", "header:last-modified"
        else:
            dt2, conf, src = _extract_pubdate_from_html(rr.text or "")

        if dt2:
            it.published_ts, it.published_iso, it.published_source, it.published_confidence = _dt_to_item_fields(dt2, src, conf)

        out.append(it)

    return out


# ---------------------------- Full-text extraction ----------------------------

def fetch_pdf_text(url: str) -> str:
    if fitz is None:
        return ""
    r = http_get(url, stream=True, timeout=REQUEST_TIMEOUT)
    if not r or r.status_code >= 400:
        return ""

    # cap bytes
    data = b""
    try:
        for chunk in r.iter_content(chunk_size=16384):
            if not chunk:
                continue
            data += chunk
            if len(data) >= MAX_PDF_BYTES:
                break
    except Exception:
        return ""

    try:
        doc = fitz.open(stream=data, filetype="pdf")
        parts = []
        for i in range(min(doc.page_count, 30)):
            parts.append(doc.load_page(i).get_text("text"))
        return "\n".join(parts).strip()
    except Exception:
        return ""


def fetch_full_text(url: str) -> str:
    """Return cleaned main text for URL. Best-effort, never raises."""
    u = normalise_url(url)
    if not u:
        return ""

    # First probe content-type (cheap)
    r = http_get(u, timeout=REQUEST_TIMEOUT)
    if not r:
        return ""

    ctype = _content_type(r)
    if looks_like_pdf(u, ctype):
        return fetch_pdf_text(u)

    html = r.text or ""
    if not html:
        return ""

    # Trafilatura extraction
    try:
        extracted = trafilatura.extract(
            html,
            url=u,
            include_comments=False,
            include_tables=False,
            include_images=False,
            favor_precision=True,
            config=DEFAULT_CONFIG,
        )
        if extracted and extracted.strip():
            return extracted.strip()
    except Exception:
        pass

    # Fallback: BeautifulSoup text
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        txt = soup.get_text("\n", strip=True)
        return re.sub(r"\n{3,}", "\n\n", txt).strip()
    except Exception:
        return ""
