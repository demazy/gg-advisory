import time, feedparser, trafilatura, requests, re
from dataclasses import dataclass
from typing import List, Optional
from bs4 import BeautifulSoup  # Optional fallback if needed

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

def fetch_rss(url: str) -> List[Item]:
    out = []
    parsed = feedparser.parse(url)
    for e in parsed.entries:
        link = e.get("link") or e.get("id")
        if not link: 
            continue
        out.append(Item(
            title=(e.get("title") or "").strip(),
            url=link,
            source=url,
            published_ts=_ts_or_now(e),
            summary=(e.get("summary") or "").strip()
        ))
    return out

def fetch_html_index(url: str) -> List[Item]:
    """
    Fetch an index/news page and try to extract article cards (best-effort).
    We’ll collect links with <a> that look like news items and dedupe later.
    """
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
    except Exception:
        return []
    html = r.text
    # Simple heuristic: pick anchors that contain /news or /press or /media
    links = re.findall(r'href="([^"]+)"', html)
    items = []
    for href in links[:120]:
        if href.startswith("/"):
            full = re.sub(r"/$", "", url) + href if not url.endswith("/") else url[:-1] + href
        else:
            full = href
        if re.search(r"(news|press|media|article|release)", full, re.I):
            items.append(Item(title="", url=full, source=url, published_ts=time.time(), summary=""))
    # Attempt to fetch titles for a few of them (lightweight)
    uniq = {}
    for it in items:
        if it.url in uniq: 
            continue
        uniq[it.url] = it
    return list(uniq.values())

def fetch_full_text(u: str) -> str:
    try:
        downloaded = trafilatura.fetch_url(u, no_ssl=True)
        if not downloaded:
            return ""
        text = trafilatura.extract(downloaded, include_formatting=False, include_links=False) or ""
        return text.strip()
    except Exception:
        return ""
