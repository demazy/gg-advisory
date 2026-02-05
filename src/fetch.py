# src/fetch.py
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup


@dataclass
class Item:
    title: str
    url: str
    source: str
    published_ts: float
    published_iso: str


def _to_iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _safe_ts_from_struct_time(st) -> Optional[float]:
    if not st:
        return None
    try:
        return float(time.mktime(st))
    except Exception:
        return None


def _norm_url(u: str) -> str:
    return u.strip()


def _looks_like_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False


def _unique_by_url(items: Iterable[Item]) -> List[Item]:
    seen = set()
    out: List[Item] = []
    for it in items:
        if it.url in seen:
            continue
        seen.add(it.url)
        out.append(it)
    return out


def _find_links_html(url: str, html: str) -> List[str]:
    """
    Extract candidate links from an index page.
    Keep it intentionally permissive; downstream filtering handles domain/title rules.
    """
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        abs_url = urljoin(url, href)
        if _looks_like_url(abs_url):
            links.append(abs_url)

    return links


def fetch_url(url: str, timeout_s: int = 30) -> str:
    headers = {"User-Agent": "gg-advisory-bot/1.0 (+https://www.gg-advisory.org)"}
    resp = requests.get(url, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    return resp.text


def fetch_rss(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch items from an RSS/Atom feed.
    `source_name` is accepted for compatibility with generate_monthly; it is optional.
    """
    label = source_name or url
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)

        items: list[Item] = []
        for e in feed.entries:
            link = getattr(e, "link", None) or ""
            title = (getattr(e, "title", "") or "").strip()
            summary = (getattr(e, "summary", "") or "").strip()

            published_ts = None
            if getattr(e, "published_parsed", None):
                published_ts = time.mktime(e.published_parsed)

            items.append(Item(
                source=url,
                url=link,
                title=title,
                published_ts=published_ts,
                summary=summary,
            ))
        return items

    except Exception as ex:
        raise RuntimeError(f"fetch_rss failed for {label}: {ex}") from ex


def fetch_html_index(url: str, *, source_name: str | None = None, **_kwargs) -> list[Item]:
    """
    Fetch candidate article links from an HTML 'news index' page.
    `source_name` is accepted for compatibility with generate_monthly; it is optional.
    """
    label = source_name or url
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        domain = urllib.parse.urlparse(url).netloc.lower().replace("www.", "")
        sel = SELECTORS.get(domain, "a[href]")

        items: list[Item] = []
        for a in soup.select(sel):
            href = a.get("href") or ""
            abs_url = urllib.parse.urljoin(url, href)
            text = (a.get_text(" ", strip=True) or "").strip()

            if not abs_url.startswith("http"):
                continue
            if not text:
                continue

            items.append(Item(
                source=url,
                url=abs_url,
                title=text[:180],
                published_ts=None,
                summary="",
            ))

        # de-dupe
        seen = set()
        deduped: list[Item] = []
        for it in items:
            if it.url in seen:
                continue
            seen.add(it.url)
            deduped.append(it)

        return deduped

    except Exception as ex:
        raise RuntimeError(f"fetch_html_index failed for {label}: {ex}") from ex


def fetch_text(url: str, timeout_s: int = 30) -> str:
    """
    Placeholder: keep your existing implementation if you have trafilatura/justext extraction elsewhere.
    """
    return fetch_url(url, timeout_s=timeout_s)

