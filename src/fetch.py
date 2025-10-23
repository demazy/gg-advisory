import re, time, requests, trafilatura
from bs4 import BeautifulSoup
from dateutil import parser as dateparse
from typing import List, Optional
from dataclasses import dataclass

@dataclass
class Item:
    title: str
    url: str
    summary: str
    published_ts: Optional[float]
    source: str

def _guess_published_ts(soup: BeautifulSoup) -> Optional[float]:
    # check meta tags
    for tag in ["article:published_time", "og:updated_time", "date", "pubdate", "timestamp"]:
        meta = soup.find("meta", attrs={"property": tag}) or soup.find("meta", attrs={"name": tag})
        if meta and meta.get("content"):
            try:
                return dateparse.parse(meta["content"]).timestamp()
            except Exception:
                pass
    # time tag
    t = soup.find("time")
    if t and (t.get("datetime") or t.text):
        try:
            return dateparse.parse(t.get("datetime") or t.text).timestamp()
        except Exception:
            pass
    # textual pattern
    m = re.search(r"(20\d{2}[-/](0[1-9]|1[0-2])[-/][0-3]\d)", soup.text)
    if m:
        try:
            return dateparse.parse(m.group(1)).timestamp()
        except Exception:
            pass
    # Published/Updated labels
    label = soup.find(string=re.compile(r"Published|Updated", re.I))
    if label:
        try:
            return dateparse.parse(str(label)).timestamp()
        except Exception:
            pass
    return None


def fetch_html_index(url: str) -> List[Item]:
    out: List[Item] = []
    html = requests.get(url, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")

    # find candidate links
    links = soup.find_all("a", href=True)
    for a in links:
        href = a["href"]
        if not href.startswith("http"):
            continue
        title = a.get_text(strip=True)
        if len(title) < 20:
            continue

        asoup = BeautifulSoup(requests.get(href, timeout=20).text, "html.parser")
        ts = _guess_published_ts(asoup)
        # fallback: try date in URL
        if ts is None:
            m = re.search(r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/", href)
            if m:
                from datetime import datetime, timezone
                try:
                    ts = datetime(
                        int(m.group(1)), int(m.group(2)), int(m.group(3))
                    ).replace(tzinfo=timezone.utc).timestamp()
                except Exception:
                    ts = None
        if ts is None:
            continue  # skip undated items

        summary = trafilatura.extract(asoup.prettify(), include_comments=False, include_tables=False)
        if not summary:
            continue
        out.append(Item(title=title, url=href, summary=summary[:600], published_ts=ts, source=url))
    return out
