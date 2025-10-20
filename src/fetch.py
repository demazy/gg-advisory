import time, feedparser, trafilatura, requests, re, urllib.parse
from dataclasses import dataclass
from typing import List, Optional
from bs4 import BeautifulSoup
from dateutil import parser as dateparse

@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: float
    summary: str
    text: str = ""

def _ts_or_now(entry) -> float:
    try:
        return time.mktime(entry.published_parsed)
    except Exception:
        return time.time()

def _normalize_url(u: str) -> str:
    """Remove utm_* and fragments for clean dedupe + canonical comparison."""
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
    """Try to find a publish date in common meta/time locations."""
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

# Domain-specific CSS selectors to capture real article links
SELECTORS = {
    "ec.europa.eu": "a[href*='/presscorner/']",
    "ifrs.org": "a[href*='/news-and-events/news/']",
    "efrag.org": "a[href*='/news']",
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
    "irena.org": "a[href*='/news'], a[href*='/Newsroom/Articles']",
    "fsb-tcfd.org": "a[href*='/publications']",
    "globalreporting.org": "a[href*='/news/']",
    "asic.gov.au": "a[href*='/news-centre']",
}

def fetch_rss(url: str) -> List[Item]:
    out = []
    # Fetch via requests to control headers + retry, then parse bytes with feedparser
    headers = {"User-Agent": "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"}
    attempts = 0
    data = None
    while attempts < 3:
        attempts += 1
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.content
            break
        except Exception:
            time.sleep(1.5 * attempts)  # simple backoff
    if data is None:
        return out  # return empty list instead of crashing

    parsed = feedparser.parse(data)
    for e in parsed.entries or []:
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
    """Fetch an index/news page and extract likely article links using CSS selectors."""
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

        # Skip obvious listing/category pages
        if re.search(r"(category|tags|/newsroom$|/news$|/media$|/press$)", href, re.I):
            continue

        title = (a.get_text(" ", strip=True) or "").strip()
        candidates.append((title, href))

    uniq = {}
    for title, href in candidates:
        if href not in uniq:
            uniq[href] = title

    items = []
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
    try:
        downloaded = trafilatura.fetch_url(u, no_ssl=True)
        if not downloaded:
            return ""
        text = trafilatura.extract(downloaded, include_formatting=False, include_links=False) or ""
        return text.strip()
    except Exception:
        return ""


