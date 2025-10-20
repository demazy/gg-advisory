import time
import logging
import re
from dataclasses import dataclass
from typing import List, Optional
import requests
import feedparser
from bs4 import BeautifulSoup
import trafilatura

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

UA = "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"
DEFAULT_TIMEOUT = 20
MAX_RETRIES = 4
BACKOFF = 1.6  # seconds multiplier

@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: float
    summary: str
    text: str = ""

def _fetch_bytes(url: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[bytes]:
    delay = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            if attempt == MAX_RETRIES:
                logging.warning(f"[fetch] giving up on {url}: {e}")
                return None
            logging.warning(f"[fetch] attempt {attempt} failed for {url}: {e} — retrying in {delay:.1f}s")
            time.sleep(delay)
            delay *= BACKOFF
    return None

def _ts_or_now(entry) -> float:
    try:
        return time.mktime(getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed"))
    except Exception:
        return time.time()

def fetch_rss(url: str) -> List[Item]:
    data = _fetch_bytes(url)
    if not data:
        return []
    try:
        parsed = feedparser.parse(data)
        out: List[Item] = []
        for e in parsed.entries:
            link = getattr(e, "link", None) or getattr(e, "id", None)
            if not link:
                continue
            out.append(Item(
                title=(getattr(e, "title", "") or "").strip(),
                url=link,
                source=url,
                published_ts=_ts_or_now(e),
                summary=(getattr(e, "summary", "") or "").strip()
            ))
        return out
    except Exception as e:
        logging.warning(f"[rss] parse failed for {url}: {e}")
        return []

def fetch_html_index(url: str) -> List[Item]:
    data = _fetch_bytes(url)
    if not data:
        return []
    try:
        html = data.decode("utf-8", errors="ignore")
        links = re.findall(r'href="([^"]+)"', html)
        items: List[Item] = []
        for href in links[:150]:
            if href.startswith("#") or href.startswith("javascript:"):
                continue
            if href.startswith("/"):
                from urllib.parse import urljoin
                href = urljoin(url, href)
            if re.search(r"(news|press|media|article|release)", href, re.I):
                items.append(Item(title="", url=href, source=url, published_ts=time.time(), summary=""))
        uniq = {it.url: it for it in items}
        return list(uniq.values())
    except Exception as e:
        logging.warning(f"[html-index] parse failed for {url}: {e}")
        return []

def fetch_full_text(u: str) -> str:
    """Try trafilatura first; fall back to BeautifulSoup text."""
    try:
        downloaded = trafilatura.fetch_url(u, no_ssl=True)
        if downloaded:
            text = trafilatura.extract(downloaded, include_formatting=False, include_links=False) or ""
            if text:
                return " ".join(text.split())
    except Exception:
        pass
    # fallback
    data = _fetch_bytes(u)
    if not data:
        return ""
    try:
        soup = BeautifulSoup(data, "html.parser")
        container = soup.find("article") or soup.find("main") or soup.body or soup
        for t in container.find_all(["script", "style", "nav", "footer", "header", "noscript"]):
            t.decompose()
        return " ".join(container.get_text(separator=" ").split())
    except Exception:
        return ""

