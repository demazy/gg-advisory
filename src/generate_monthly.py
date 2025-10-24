# src/generate_monthly.py
# Full debug-enabled generator for monthly digests

import os
import re
import json
import calendar
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from .fetch import fetch_rss, fetch_html_index, fetch_full_text, Item
from .summarise import build_digest
from .utils import sha1, normalize_whitespace

load_dotenv()

# ----------------------- ENV / CONSTANTS -----------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMP = float(os.getenv("TEMP", "0.2"))
MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "700"))
ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "4"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "2"))  # if fewer than this, we skip model call
DEBUG = os.getenv("DEBUG", "0") == "1"

# Trusted short-content domains can have a lower character floor
PRIORITY_DOMAINS = {
    d.strip().lower()
    for d in os.getenv(
        "PRIORITY_DOMAINS",
        "aemo.com.au,arena.gov.au,cefc.com.au,ifrs.org,efrag.org,dcceew.gov.au,ec.europa.eu"
    ).split(",")
    if d.strip()
}
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "250"))

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "out"
CFG = ROOT / "config" / "sources.yaml"
FILTERS = ROOT / "config" / "filters.yaml"

OUTDIR.mkdir(parents=True, exist_ok=True)

# ----------------------- DATE HELPERS --------------------------

def _month_bounds(y: int, m: int) -> Tuple[datetime, datetime]:
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    last = calendar.monthrange(y, m)[1]
    end = datetime(y, m, last, 23, 59, 59, tzinfo=timezone.utc)
    return start, end

def _month_label(d: datetime) -> str:
    return d.strftime("%B %Y")  # e.g. "February 2025"

def _in_range(ts: float, start: datetime, end: datetime) -> bool:
    if not ts:
        return False
    t = datetime.fromtimestamp(ts, tz=timezone.utc)
    return start <= t <= end

def fmt_iso(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
    except Exception:
        return ""

# ----------------------- FILTERS -------------------------------

def _load_filters() -> Dict:
    """Load filters.yaml (optional). Compile regexes; normalise lists."""
    base = {"allow_domains": [], "deny_domains": [], "title_deny_regex": [], "keep_keywords": []}
    if not FILTERS.exists():
        return base
    raw = (yaml_safe := __import__("yaml")).safe_load(FILTERS.read_text()) or {}
    allow = list(raw.get("allow_domains", []) or [])
    deny = list(raw.get("deny_domains", []) or [])
    deny_pat = [re.compile(p, re.I) for p in (raw.get("title_deny_regex", []) or [])]
    keep = [str(k).lower() for k in (raw.get("keep_keywords", []) or [])]
    return {"allow_domains": allow, "deny_domains": deny, "title_deny_regex": deny_pat, "keep_keywords": keep}

def _passes_filters(it: Item, filters: Dict, drop_log) -> bool:
    """Domain allow/deny, title deny regex, with keep_keywords override."""
    from urllib.parse import urlparse
    dom = urlparse(it.url).netloc.lower()
    allow = set(filters.get("allow_domains", []))
    deny = set(filters.get("deny_domains", []))
    deny_title = filters.get("title_deny_regex", [])
    keep_keywords = filters.get("keep_keywords", [])

    if allow and not any(dom.endswith(d) for d in allow):
        if DEBUG: print(f"[drop] domain not allowed: {dom} -> {it.url}")
        drop_log(f"domain_not_allowed\t{dom}\t{it.url}")
        return False
    if any(dom.endswith(d) for d in deny):
        if DEBUG: print(f"[drop] domain denied: {dom} -> {it.url}")
        drop_log(f"domain_denied\t{dom}\t{it.url}")
        return False

    title_low = (it.title or "").lower()
    if any(kw in title_low for kw in keep_keywords):
        return True
    for rx in deny_title:
        try:
            if rx.search(it.title or ""):
                if DEBUG: print(f"[drop] title deny regex: {it.title}")
                drop_log(f"title_regex\t{rx.pattern}\t{it.url}")
                return False
        except Exception:
            continue
    return True

# ----------------------- FETCH / POOL --------------------------

def collect_items(s: Dict, drop_log) -> List[Item]:
    from yaml import safe_load  # local import to avoid top-level dependency confusion
    rss_urls = s.get("rss", []) or []
    html_urls = s.get("html", []) or []
    items: List[Item] = []

    # RSS
    for r in rss_urls:
        fetched = fetch_rss(r)
        if DEBUG: print(f"[rss] {r}: {len(fetched)}")
        items.extend(fetched)

    # HTML
    for h in html_urls:
        fetched = fetch_html_index(h)
        if DEBUG: print(f"[html] {h}: {len(fetched)}")
        items.extend(fetched)

    items.sort(key=lambda x: (x.published_ts or 0), reverse=True)
    return items

# ----------------------- DEBUG UTILS ---------------------------

def _mk_drop_logger(drop_path: Path):
    drop_path.parent.mkdir(parents=True, exist_ok=True)
    # Always (re)create the file with a header so it exists even if no drops happen
    with drop_path.open("w", encoding="utf-8") as f:
        f.write("# reason\tmeta\turl\n")
    def _log(line: str):
        with drop_path.open("a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")
        # Also echo to stdout when DEBUG so reasons show in the Actions log
        if DEBUG:
            print(line)
    return _log



def _dump_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

# ----------------------- CORE GENERATION -----------------------

def _generate_for_range(start: datetime, end: datetime, items_per_section: int) -> str:
    import yaml  # safe import inside function
    cfg = yaml.safe_load(CFG.read_text())
    # Provide a month hint (YYYY-MM) to fetch.py
    os.environ["TARGET_YM"] = start.strftime("%Y-%m")

    # Debug files
    ym = start.strftime("%Y-%m")
    drop_file = OUTDIR / f"debug-drops-{ym}.txt"
    if drop_file.exists():
        drop_file.unlink()
    drop_log = _mk_drop_logger(drop_file)

    filters = _load_filters()
    chosen: List[Dict] = []
    seen_urls: set = set()

    for section, sources in cfg["sections"].items():
        print(f"[section] {section}")
        pool = collect_items(sources, drop_log)
        print(f"[pool] candidates: {len(pool)}")

        # Debug dump of raw pool (first 100)
        if DEBUG:
            _dump_json(
                OUTDIR / f"debug-pool-{section.replace(' ','_')}-{ym}.json",
                [
                    {
                        "title": it.title,
                        "url": it.url,
                        "source": it.source,
                        "published_ts": it.published_ts,
                        "published_iso": fmt_iso(it.published_ts) if it.published_ts else "",
                    }
                    for it in pool[:100]
                ],
            )

        selected = []
        for it in pool:
            if not _in_range(it.published_ts, start, end):
                if DEBUG:
                    drop_log(f"out_of_range\t{fmt_iso(it.published_ts)}\t{it.url}")
                continue

            if not _passes_filters(it, filters, drop_log):
                continue

            h = sha1(it.url)
            if h in seen_urls:
                if DEBUG:
                    drop_log(f"duplicate_url\t-\t{it.url}")
                continue

            # Fetch full text
            txt = fetch_full_text(it.url)
            txt = normalize_whitespace(txt)

            # Domain-aware content threshold
            from urllib.parse import urlparse
            dom = urlparse(it.url).netloc.lower()
            threshold = PRIORITY_MIN_CHARS if any(dom.endswith(d) for d in PRIORITY_DOMAINS) else MIN_TEXT_CHARS
            if len(txt) < threshold and len(it.summary or "") < 160:
                if DEBUG:
                    drop_log(f"too_short\tlen={len(txt)}/thr={threshold}\t{it.url}")
                continue

            selected.append({
                "section": section,
                "title": it.title or it.url.split("/")[-1].replace("-", " ")[:100],
                "url": it.url,
                "sources_urls": [it.url],  # only pass the article URL to the model
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
    chosen = sorted(chosen, key=lambda x: (x.get("published", ""), x.get("section", "")), reverse=True)

    # Per-domain cap
    from urllib.parse import urlparse
    per_domain = {}
    filtered = []
    for row in chosen:
        dom = urlparse(row["url"]).netloc.lower()
        per_domain[dom] = per_domain.get(dom, 0) + 1
        if per_domain[dom] <= PER_DOMAIN_CAP:
            filtered.append(row)
        else:
            if DEBUG:
                drop_log(f"per_domain_cap\tdom={dom} cap={PER_DOMAIN_CAP}\t{row['url']}")

    chosen = filtered[:12]

    # Debug snapshot of chosen
    if True:
        _dump_json(
            OUTDIR / f"debug-selected-{ym}.json",
            [{"title": x["title"], "url": x["url"], "published": x["published"], "section": x["section"]} for x in chosen],
        )

    # Safety guard
    if len(chosen) < MIN_TOTAL_ITEMS:
        print(f"[warn] Few items in range ({len(chosen)}<{MIN_TOTAL_ITEMS}); writing placeholder.")
        return (
            f"# Signals Digest — NO ITEMS IN RANGE\n\n"
            f"_Date range_: {start.date().isoformat()} → {end.date().isoformat()}\n\n"
            "Insufficient eligible items. Consider relaxing MIN_TEXT_CHARS, checking filters, widening sources, "
            "or enabling PRIORITY_* thresholds for trusted domains."
        )

    date_label = _month_label(end)
    return build_digest(model=MODEL, api_key=OPENAI_API_KEY, items=chosen, temp=TEMP, date_label=date_label)

# ----------------------- ENTRY POINT ---------------------------

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

    # single range (manual test)
    start_date = os.getenv("START_DATE", "2025-11-01")
    end_date   = os.getenv("END_DATE",   "2025-11-30")
    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end   = datetime.fromisoformat(end_date).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    md = _generate_for_range(start, end, items_per_section=ITEMS_PER_SECTION)
    OUT = OUTDIR / f"monthly-digest-{start.date().isoformat()}_{end.date().isoformat()}.md"
    OUT.write_text(md)
    print(f"[done] {OUT}")

# Lazy import of yaml at module level (used in _load_filters)
# to avoid shadowing by variable names.
import yaml as yaml_safe  # noqa: E402

if __name__ == "__main__":
    main()
