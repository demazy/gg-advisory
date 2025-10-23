import os, json, yaml, re, calendar
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Dict, List, Tuple
from .fetch import fetch_rss, fetch_html_index, fetch_full_text, Item
from .summarise import build_digest
from .utils import sha1, normalize_whitespace

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMP = float(os.getenv("TEMP", "0.2"))
MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "700"))
ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "4"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "out"
CFG = ROOT / "config" / "sources.yaml"
OUTDIR.mkdir(parents=True, exist_ok=True)


def _month_bounds(y, m):
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    end = datetime(y, m, calendar.monthrange(y, m)[1], 23, 59, 59, tzinfo=timezone.utc)
    return start, end


def _in_range(ts, start, end):
    if not ts:
        return False
    t = datetime.fromtimestamp(ts, tz=timezone.utc)
    return start <= t <= end


def collect_items(sources: Dict) -> List[Item]:
    rss_urls = sources.get("rss", []) or []
    html_urls = sources.get("html", []) or []
    items: List[Item] = []
    for r in rss_urls:
        items.extend(fetch_rss(r))
    for h in html_urls:
        items.extend(fetch_html_index(h))
    items.sort(key=lambda x: x.published_ts or 0, reverse=True)
    return items


def fmt_iso(ts):
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
    except Exception:
        return ""


def _generate_for_range(start, end, items_per_section):
    cfg = yaml.safe_load(CFG.read_text())
    chosen: List[Dict] = []
    seen_urls = set()

    for section, sources in cfg["sections"].items():
        print(f"[section] {section}")
        pool = collect_items(sources)
        print(f"[pool] candidates: {len(pool)}")
        selected = []
        for it in pool:
            if not _in_range(it.published_ts, start, end):
                continue
            h = sha1(it.url)
            if h in seen_urls:
                continue
            txt = fetch_full_text(it.url)
            txt = normalize_whitespace(txt)
            if len(txt) < MIN_TEXT_CHARS and len(it.summary or "") < 160:
                continue
            selected.append({
                "section": section,
                "title": it.title,
                "url": it.url,
                "source": it.source,
                "summary": it.summary,
                "text": txt,
                "published": fmt_iso(it.published_ts),
            })
            seen_urls.add(h)
            if len(selected) >= items_per_section:
                break
        print(f"[selected] {len(selected)} from {section}")
        chosen.extend(selected)

    chosen = sorted(chosen, key=lambda x: (x.get("published",""), x.get("section","")), reverse=True)
    # cap per domain
    from urllib.parse import urlparse
    per_domain = {}
    filtered = []
    for row in chosen:
        dom = urlparse(row["url"]).netloc.lower()
        per_domain[dom] = per_domain.get(dom, 0) + 1
        if per_domain[dom] <= PER_DOMAIN_CAP:
            filtered.append(row)
    chosen = filtered[:12]

    return build_digest(MODEL, OPENAI_API_KEY, chosen, TEMP)


def main():
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    mode = os.getenv("MODE", "single")
    if mode == "backfill-months":
        start_ym = os.getenv("START_YM", "2025-01")
        end_ym = os.getenv("END_YM", "2025-10")
        sy, sm = map(int, start_ym.split("-"))
        ey, em = map(int, end_ym.split("-"))
        y, m = sy, sm
        while (y < ey) or (y == ey and m <= em):
            start, end = _month_bounds(y, m)
            md = _generate_for_range(start, end, ITEMS_PER_SECTION)
            path = OUTDIR / f"monthly-digest-{y:04d}-{m:02d}.md"
            path.write_text(md)
            print(f"[done] {path}")
            m = 1 if m == 12 else m + 1
            if m == 1:
                y += 1
        return

    start = datetime.fromisoformat(os.getenv("START_DATE", "2025-11-01")).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(os.getenv("END_DATE", "2025-11-30")).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    md = _generate_for_range(start, end, ITEMS_PER_SECTION)
    path = OUTDIR / f"monthly-digest-{start.date()}_{end.date()}.md"
    path.write_text(md)
    print(f"[done] {path}")


if __name__ == "__main__":
    main()
