# src/fetch.py
import time
import logging
from dataclasses import dataclass
from typing import List, Optional
import requests
import feedparser
from bs4 import BeautifulSoup  # fallback HTML parser

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

UA = "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"
DEFAULT_TIMEOUT = 20
MAX_RETRIES = 4
BACKOFF = 1.6  # seconds multiplier

@dataclass
class Item:
    url: str
    title: str
    summary: str
    text: str
    source: str
    published_ts: float  # epoch seconds

def _fetch_bytes(url: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[bytes]:
    """
    GET with retries/backoff. Returns bytes or None.
    Never raises to callers.
    """
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

def fetch_rss(url: str) -> List[Item]:
    """
    Fetch and parse an RSS/Atom feed into Items.
    Returns [] on any error.
    """
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
            title = getattr(e, "title", "") or ""
            summary = getattr(e, "summary", "") or ""
            # published_parsed may be None; default to 0 for sorting
            ts = 0.0
            pp = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
            if pp:
                # time.mktime handles struct_time
                ts = time.mktime(pp)
            out.append(Item(
                url=link,
                title=title,
                summary=summary,
                text="",
                source=url,
                published_ts=ts
            ))
        return out
    except Exception as e:
        logging.warning(f"[rss] parse failed for {url}: {e}")
        return []

def fetch_html_index(url: str) -> List[Item]:
    """
    Fetch an HTML listing page and extract article links/titles.
    Keep this conservative — return [] if we can't confidently parse.
    """
    data = _fetch_bytes(url)
    if not data:
        return []
    try:
        soup = BeautifulSoup(data, "html.parser")
        # naive: collect <a> tags that look like articles
        items: List[Item] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("#") or href.startswith("javascript:"):
                continue
            # build absolute URL if necessary
            if href.startswith("/"):
                from urllib.parse import urljoin
                href = urljoin(url, href)
            title = (a.get_text() or "").strip()
            if not title:
                continue
            items.append(Item(
                url=href,
                title=title[:140],
                summary="",
                text="",
                source=url,
                published_ts=0.0
            ))
        return items
    except Exception as e:
        logging.warning(f"[html-index] parse failed for {url}: {e}")
        return []

def fetch_full_text(url: str) -> str:
    """
    Fetch article page and extract readable text.
    Returns '' on error.
    """
    data = _fetch_bytes(url)
    if not data:
        return ""
    try:
        soup = BeautifulSoup(data, "html.parser")
        # Prefer article/main; fallback to body text
        container = soup.find("article") or soup.find("main") or soup.body or soup
        # Strip script/style/nav
        for tag in container.find_all(["script", "style", "nav", "footer", "header", "noscript"]):
            tag.decompose()
        text = " ".join(container.get_text(separator=" ").split())
        return text
    except Exception as e:
        logging.warning(f"[full-text] extract failed for {url}: {e}")
        return ""
