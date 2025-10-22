import os, json, yaml, re, calendar
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, date, timezone
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

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state" / "seen_urls.json"
OUTDIR = ROOT / "out"
CFG = ROOT / "config" / "sources.yaml"
FILTERS = ROOT / "config" / "filters.yaml"

OUTDIR.mkdir(parents=True, exist_ok=True)
STATE.parent.mkdir(parents=True, exist_ok=True)
if not STATE.exists():
    STATE.write_text(json.dumps({"seen": []}, indent=2))

def _month_bounds(y: int, m: int) -> Tuple[datetime, datetime]:
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    last = calendar.monthrange(y, m)[1]
    end = datetime(y, m, last, 23, 59, 59, tzinfo=timezone.utc)
    return start, end

def _in_range(ts: float, start: datetime, end: datetime) -> bool:
    if not ts:
        return False
    t = datetime.fromtimestamp(ts, tz=timezone.utc)
    return start <= t <= end

def collect_items(s: Dict) -> List[Item]:
    rss_urls = s.get("rss", []) or []
    html_urls = s.get("html", []) or []
    items: List[Item] = []
    for r in rss_urls:
        items.extend(fetch_rss(r))
    for h in html_urls:
        items.extend(fetch_html_index(h))
    items.sort(key=lambda x: x.published_ts, reverse=True)
    return items

def fmt_iso(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
    except Exception:
        return ""

def _load_filters() -> Dict:
    filters = {"allow_domains": [], "deny_domains": [], "title_deny_regex": []}
    if FILTERS.exists():
        filters = yaml.safe_load(FILTERS.read_text()) or filters
    # compile regex
    filters["title_deny_regex"] = [re.compile(p) for p in filters.get("title_deny_regex", [])]
    return filters

def _passes_filters(it: Item, filters: Dict) -> bool:
    from urllib.parse import urlparse
    dom = urlparse(it.url).netloc.lower()
    allow = set(filters.get("allow_domains", []))
    deny = set(filters.get("deny_domains", []))
    deny_title = filters.get("title_deny_regex", [])

    if allow and not any(dom.endswith(d) for d in allow):
        return False
    if any(dom.endswith(d) for d in deny):
        return False
    title = (it.title or "")
    if any(rx.search(title) for rx in deny_title):
        return False
    return True

def _generate_for_range(start: datetime, end: datetime, items_per_section: int) -> str:
    cfg = yaml.safe_load(CFG.read_text())
    filters = _load_filters()

    # For backfill/ranged runs we de-dupe within the run only.
    chosen: List[Dict] = []
    seen_urls: set = set()

    for section, sources in cfg["sections"].items():
        pool = collect_items(sources)
        selected = []
        for it in pool:
            if not _in_range(it.published_ts, start, end):
                continue
            if not _passes_filters(it, filters):
                continue

            h = sha1(it.url)
            if h in seen_urls:
                continue

            txt = fetch_full_text(it.url)
            txt = normalize_whitespace(txt)
            if len(txt) < MIN_TEXT_CHARS and len(it.summary or "") < 160:
                continue

            title = it.title or it.url.split("/")[-1].replace("-", " ")[:100]
            row = {
                "section": section,
                "title": title,
                "url": it.url,
                "source": sources.get("rss", [])[:1] or sources.get("html", [])[:1] or ["manual"],
                "summary": it.summary or "",
                "text": txt,
                "published": fmt_iso(it.published_ts)
            }
            selected.append(row)
            seen_urls.add(h)
            if len(selected) >= items_per_section:
                break

        chosen.extend(selected)

    # newest first; cap to a manageable digest size
    chosen = sorted(chosen, key=lambda x: (x.get("published",""), x.get("section","")), reverse=True)[:12]
    return build_digest(model=MODEL, api_key=OPENAI_API_KEY, items=chosen, temp=TEMP)

def main():
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    mode = os.getenv("MODE", "single")  # 'single' | 'backfill-months'
    if mode == "backfill-months":
        start_ym = os.getenv("START_YM", "2025-01")
        end_ym   = os.getenv("END_YM",   "2025-10")
        sy, sm = map(int, start_ym.split("-"))
        ey, em = map(int, end_ym.split("-"))
        y, m = sy, sm
        while (y < ey) or (y == ey and m <= em):
            start, end = _month_bounds(y, m)
            md = _generate_for_range(start, end, items_per_section=ITEMS_PER_SECTION)
            OUT = OUTDIR / f"monthly-digest-{y:04d}-{m:02d}.md"
            OUT.write_text(md)
            print(f"[done] {OUT}")
            m = 1 if m == 12 else m + 1
            if m == 1:
                y += 1
        return

    # single-range (e.g., monthly run for last month)
    start_date = os.getenv("START_DATE", "2025-11-01")
    end_date   = os.getenv("END_DATE",   "2025-11-30")
    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end   = datetime.fromisoformat(end_date).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    md = _generate_for_range(start, end, items_per_section=ITEMS_PER_SECTION)
    OUT = OUTDIR / f"monthly-digest-{start.date().isoformat()}_{end.date().isoformat()}.md"
    OUT.write_text(md)
    print(f"[done] {OUT}")

if __name__ == "__main__":
    main()
