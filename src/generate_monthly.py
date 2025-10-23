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
DEBUG = os.getenv("DEBUG", "0") == "1"

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "out"
CFG = ROOT / "config" / "sources.yaml"
FILTERS = ROOT / "config" / "filters.yaml"

OUTDIR.mkdir(parents=True, exist_ok=True)

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
        fetched = fetch_rss(r)
        if DEBUG: print(f"[rss] {r}: {len(fetched)}")
        items.extend(fetched)
    for h in html_urls:
        fetched = fetch_html_index(h)
        if DEBUG: print(f"[html] {h}: {len(fetched)}")
        items.extend(fetched)
    items.sort(key=lambda x: (x.published_ts or 0), reverse=True)
    return items

def fmt_iso(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
    except Exception:
        return ""

def _load_filters() -> Dict:
    if not FILTERS.exists():
        return {"allow_domains": [], "deny_domains": [], "title_deny_regex": [], "keep_keywords": []}
    raw = yaml.safe_load(FILTERS.read_text()) or {}
    allow = list(raw.get("allow_domains", []) or [])
    deny = list(raw.get("deny_domains", []) or [])
    deny_pat = [re.compile(p, re.I) for p in (raw.get("title_deny_regex", []) or [])]
    keep = [str(k).lower() for k in (raw.get("keep_keywords", []) or [])]
    return {"allow_domains": allow, "deny_domains": deny, "title_deny_regex": deny_pat, "keep_keywords": keep}

def _passes_filters(it: Item, filters: Dict) -> bool:
    from urllib.parse import urlparse
    dom = urlparse(it.url).netloc.lower()
    allow = set(filters.get("allow_domains", []))
    deny = set(filters.get("deny_domains", []))
    deny_title = filters.get("title_deny_regex", [])
    keep_keywords = filters.get("keep_keywords", [])

    if allow and not any(dom.endswith(d) for d in allow):
        if DEBUG: print(f"[drop] domain not allowed: {dom} -> {it.url}")
        return False
    if any(dom.endswith(d) for d in deny):
        if DEBUG: print(f"[drop] domain denied: {dom} -> {it.url}")
        return False

    title_low = (it.title or "").lower()
    if any(kw in title_low for kw in keep_keywords):
        return True
    for rx in deny_title:
        try:
            if rx.search(it.title or ""):
                if DEBUG: print(f"[drop] title deny regex: {it.title}")
                return False
        except Exception:
            continue
    return True

def _generate_for_range(start: datetime, end: datetime, items_per_section: int) -> str:
    cfg = yaml.safe_load(CFG.read_text())
    filters = _load_filters()

    chosen: List[Dict] = []
    seen_urls: set = set()

    for section, sources in cfg["sections"].items():
        print(f"[section] {section}")
        pool = collect_items(sources)
        print(f"[pool] candidates: {len(pool)}")
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
                if DEBUG: print(f"[drop] too short: {it.url}")
                continue

            selected.append({
                "section": section,
                "title": it.title or it.url.split("/")[-1].replace("-", " ")[:100],
                "url": it.url,
                "source": sources.get("rss", [])[:1] or sources.get("html", [])[:1] or ["manual"],
                "summary": it.summary or "",
                "text": txt,
                "published": fmt_iso(it.published_ts),
            })
            seen_urls.add(h)
            if len(selected) >= items_per_section:
                break
        print(f"[selected] {len(selected)} from {section}")
        chosen.extend(selected)

    # Sort newest first
    chosen = sorted(chosen, key=lambda x: (x.get("published",""), x.get("section","")), reverse=True)

    # Per-domain cap
    from urllib.parse import urlparse
    per_domain = {}
    filtered = []
    for row in chosen:
        dom = urlparse(row["url"]).netloc.lower()
        per_domain[dom] = per_domain.get(dom, 0) + 1
        if per_domain[dom] <= PER_DOMAIN_CAP:
            filtered.append(row)

    # Final slice
    chosen = filtered[:12]

    # ---- SAFETY GUARD: if no items, do NOT call the model (prevents hallucination) ----
    if not chosen:
        print("[warn] No eligible items in range; writing placeholder and exiting.")
        placeholder = (
            "# Signals Digest — NO ITEMS IN RANGE\n\n"
            f"_Date range_ : {start.date().isoformat()} → {end.date().isoformat()}\n\n"
            "No sources produced eligible items for this period. "
            "Consider relaxing MIN_TEXT_CHARS, widening sources, or checking filters."
        )
        return placeholder

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

    # single range (e.g., prior month)
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
