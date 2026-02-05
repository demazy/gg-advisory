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
from .utils import sha1, normalize_whitespace, normalise_domain

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
MIN_SUBSTANCE_SCORE = int(os.getenv("MIN_SUBSTANCE_SCORE", "2"))
MAX_PER_DOMAIN_PER_SECTION = int(os.getenv("MAX_PER_DOMAIN_PER_SECTION", "1"))

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


# -------------------- SUBSTANCE / QUALITY ---------------------

_LOW_VALUE_URL_HINTS = ("/meeting", "/meetings", "/news-and-calendar", "/calendar", "/events", "/agenda")
_LOW_VALUE_TEXT_HINTS = (
    "online meeting",
    "registration required",
    "duly registered",
    "registered observers",
    "agenda will be available",
    "open to duly",
    "meeting will be held",
)

def is_low_value_notice(url: str, title: str, text: str) -> bool:
    u = (url or "").lower()
    t = (title or "").lower()
    x = (text or "").lower()
    if any(h in u for h in _LOW_VALUE_URL_HINTS):
        return True
    if any(h in t for h in _LOW_VALUE_TEXT_HINTS):
        return True
    if any(h in x for h in _LOW_VALUE_TEXT_HINTS):
        return True
    return False

def substance_score(text: str) -> int:
    t = (text or "").lower()
    score = 0
    # decision / regulatory signal
    if any(w in t for w in ("final", "decision", "approved", "determination", "rule change")):
        score += 3
    if any(w in t for w in ("consultation", "draft", "exposure draft", "guidance", "standard", "framework")):
        score += 2
    # quantitative / commercial signal
    if any(w in t for w in ("mw", "gw", "million", "billion", "$", "€", "aud", "eur")):
        score += 2
    if any(w in t for w in ("auction", "tender", "capacity", "tariff", "price", "market")):
        score += 1
    # meeting/admin penalty
    if any(w in t for w in ("meeting", "registration", "observers", "agenda")):
        score -= 3
    if len(t) < 800:
        score -= 2
    return score


# ----------------------- DEBUG HELPERS ------------------------

def _dump_json(path: Path, obj) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[warn] failed to dump json: {path}: {e}")

def _append_line(path: Path, line: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(line.rstrip("\n") + "\n")
    except Exception as e:
        print(f"[warn] failed to append: {path}: {e}")


# ----------------------- FILTER LOADERS ------------------------

def load_yaml(path: Path) -> Dict:
    import yaml
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def compile_filters(filters: Dict) -> Dict:
    allow = [d.strip().lower() for d in (filters.get("allow_domains") or []) if d.strip()]
    deny = [d.strip().lower() for d in (filters.get("deny_domains") or []) if d.strip()]
    deny_pat = [re.compile(p, re.I) for p in (filters.get("title_deny_regex") or []) if p.strip()]
    keep = [k.strip().lower() for k in (filters.get("keep_keywords") or []) if k.strip()]
    return {"allow_domains": allow, "deny_domains": deny, "title_deny_regex": deny_pat, "keep_keywords": keep}

def _passes_filters(it: Item, filters: Dict, drop_log) -> bool:
    """Domain allow/deny, title deny regex, with keep_keywords override."""
    dom = normalise_domain(it.url)
    allow = set(filters.get("allow_domains", []))
    deny = set(filters.get("deny_domains", []))
    deny_title = filters.get("title_deny_regex", [])
    keep_keywords = filters.get("keep_keywords", [])

    if allow and not any(dom.endswith(d) for d in allow):
        if DEBUG:
            print(f"[drop] domain not allowed: {dom} -> {it.url}")
        drop_log(f"domain_not_allowed\t{dom}\t{it.url}")
        return False

    if any(dom.endswith(d) for d in deny):
        if DEBUG:
            print(f"[drop] domain denied: {dom} -> {it.url}")
        drop_log(f"domain_denied\t{dom}\t{it.url}")
        return False

    title = (it.title or "").strip()
    title_l = title.lower()

    # Keep-keywords override deny-title
    if keep_keywords and any(k in title_l for k in keep_keywords):
        return True

    for pat in deny_title:
        if pat.search(title):
            if DEBUG:
                print(f"[drop] title denied: {title} -> {it.url}")
            drop_log(f"title_denied\t{title}\t{it.url}")
            return False

    return True


# ----------------------- ITEM COLLECTION -----------------------

def collect_items(sources, drop_log) -> List[Item]:
    """
    Accepts sources as:
      - list[str] (URLs)
      - list[dict] with keys like {type: rss|html, url: ..., name: ...}
      - dict forms (rare): {rss: [..], html: [..]}
    """

    def _infer_type(url: str) -> str:
        u = (url or "").lower()
        # crude but effective
        if any(x in u for x in (".xml", "/rss", "feed", "atom")):
            return "rss"
        return "html"

    def _iter_sources(sources_obj):
        if sources_obj is None:
            return
        # If someone used dict-of-lists style
        if isinstance(sources_obj, dict):
            for k, v in sources_obj.items():
                if isinstance(v, list):
                    for item in v:
                        yield (k, item)
                else:
                    yield (k, v)
            return
        # Normal list
        if isinstance(sources_obj, list):
            for item in sources_obj:
                yield (None, item)
            return
        # Single scalar
        yield (None, sources_obj)

    pool: List[Item] = []

    for forced_type, src in _iter_sources(sources):
        try:
            # Case 1: src is URL string
            if isinstance(src, str):
                url = src.strip()
                if not url:
                    continue
                stype = forced_type or _infer_type(url)
                name = url

            # Case 2: src is dict
            elif isinstance(src, dict):
                url = (src.get("url") or "").strip()
                if not url:
                    continue
                stype = (src.get("type") or forced_type or _infer_type(url)).strip().lower()
                name = src.get("name") or url

            else:
                drop_log(f"source_bad_type\t{type(src)}\t{src}")
                continue

            if stype == "rss":
                items = fetch_rss(url, source_name=name)
            elif stype == "html":
                items = fetch_html_index(url, source_name=name)
            else:
                drop_log(f"source_bad_stype\t{stype}\t{url}")
                continue

            pool.extend(items)

        except Exception as e:
            drop_log(f"source_error\t{name}\t{url}\t{e}")
            if DEBUG:
                print(f"[warn] source error: {name} -> {e}")

    # Sort candidates by recency (published_ts desc)
    pool.sort(key=lambda x: x.published_ts or 0, reverse=True)
    return pool



# ----------------------- MONTHLY GENERATION --------------------

def generate_monthly_for(ym: str) -> str:
    y, m = map(int, ym.split("-"))
    start, end = _month_bounds(y, m)

    cfg = load_yaml(CFG)
    filters = compile_filters(load_yaml(FILTERS))

    drop_file = OUTDIR / f"debug-drops-{ym}.txt"
    selected_file = OUTDIR / f"debug-selected-{ym}.json"
    meta_file = OUTDIR / f"debug-meta-{ym}.txt"

    if DEBUG:
        try:
            drop_file.unlink(missing_ok=True)
        except Exception:
            pass

    def drop_log(line: str) -> None:
        if DEBUG:
            _append_line(drop_file, line)

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
        section_dom_counts: Dict[str, int] = {}

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

            dom = normalise_domain(it.url)

            # Per-domain cap within section (prevents one org monopolising a section)
            if section_dom_counts.get(dom, 0) >= MAX_PER_DOMAIN_PER_SECTION:
                if DEBUG:
                    drop_log(
                        f"per_section_domain_cap\tsection={section}\tdom={dom} cap={MAX_PER_DOMAIN_PER_SECTION}\t{it.url}"
                    )
                continue

            # Suppress low-value calendar/meeting notices unless they are clearly substantive
            if is_low_value_notice(it.url, it.title or "", txt) and len(txt) < max(900, MIN_TEXT_CHARS):
                if DEBUG:
                    drop_log(f"low_value_notice\tlen={len(txt)}\t{it.url}")
                continue

            # Domain-aware content threshold
            threshold = PRIORITY_MIN_CHARS if any(dom.endswith(d) for d in PRIORITY_DOMAINS) else MIN_TEXT_CHARS
            if len(txt) < threshold and len(it.summary or "") < 160:
                if DEBUG:
                    drop_log(f"too_short\tlen={len(txt)}/thr={threshold}\t{it.url}")
                continue

            # Simple substance gate (prevents admin/calendar spam)
            sscore = substance_score(txt)
            if sscore < MIN_SUBSTANCE_SCORE:
                if DEBUG:
                    drop_log(f"low_substance\tscore={sscore} min={MIN_SUBSTANCE_SCORE}\t{it.url}")
                continue

            section_dom_counts[dom] = section_dom_counts.get(dom, 0) + 1

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

            if len(selected) >= ITEMS_PER_SECTION:
                break

        print(f"[selected] {len(selected)} from {section}")
        chosen.extend(selected)

    # Sort newest first
    chosen = sorted(chosen, key=lambda x: (x.get("published", ""), x.get("section", "")), reverse=True)

    # Per-domain cap (global)
    per_domain: Dict[str, int] = {}
    filtered = []
    for row in chosen:
        dom = normalise_domain(row["url"])
        per_domain[dom] = per_domain.get(dom, 0) + 1
        if per_domain[dom] <= PER_DOMAIN_CAP:
            filtered.append(row)
        else:
            if DEBUG:
                drop_log(f"per_domain_cap\tdom={dom} cap={PER_DOMAIN_CAP}\t{row['url']}")

    chosen = filtered[:12]

    # ✅ Always write a snapshot of selected items (even if empty)
    _dump_json(
        selected_file,
        [{"title": x["title"], "url": x["url"], "published": x["published"], "section": x["section"]}
         for x in chosen],
    )

    # ✅ Write a small meta summary so you can read it in the log
    try:
        with meta_file.open("w", encoding="utf-8") as mf:
            mf.write(f"month: {ym}\n")
            mf.write(f"outdir: {OUTDIR.resolve()}\n")
            mf.write(f"drops_file: {drop_file.resolve()}\n")
            mf.write(f"selected_file: {selected_file.resolve()}\n")
            mf.write(f"counts:\n")
            mf.write(f"  selected_total: {len(chosen)}\n")
            from collections import Counter
            c = Counter([x.get('section','') for x in chosen])
            for k, v in c.items():
                mf.write(f"  section[{k}]: {v}\n")
    except Exception as e:
        print(f"[warn] failed to write meta file: {e}")

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
            ym = f"{y:04d}-{m:02d}"
            print(f"\n=== {ym} ({start.date()} -> {end.date()}) ===")
            md = generate_monthly_for(ym)
            out = OUTDIR / f"monthly-digest-{ym}.md"
            out.write_text(md, encoding="utf-8")
            print(f"[write] {out}")

            # increment month
            m += 1
            if m > 12:
                m = 1
                y += 1
    else:
        # Single month from env or current UTC month
        ym = os.getenv("YM", datetime.now(timezone.utc).strftime("%Y-%m"))
        md = generate_monthly_for(ym)
        out = OUTDIR / f"monthly-digest-{ym}.md"
        out.write_text(md, encoding="utf-8")
        print(f"[write] {out}")


if __name__ == "__main__":
    main()
