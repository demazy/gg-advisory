# src/generate_monthly.py
# Full debug-enabled generator for monthly digests (robust date coercion)

from __future__ import annotations

import os
import re
import json
import calendar
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from dateutil import parser as dtparser

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
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "2"))
DEBUG = os.getenv("DEBUG", "0") == "1"

MIN_SUBSTANCE_SCORE = int(os.getenv("MIN_SUBSTANCE_SCORE", "2"))
MAX_PER_DOMAIN_PER_SECTION = int(os.getenv("MAX_PER_DOMAIN_PER_SECTION", "1"))

# Trusted short-content domains can have a lower character floor
PRIORITY_DOMAINS = {
    d.strip().lower()
    for d in os.getenv(
        "PRIORITY_DOMAINS",
        "aemo.com.au,arena.gov.au,cefc.com.au,ifrs.org,efrag.org,dcceew.gov.au,ec.europa.eu,commission.europa.eu",
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
    return d.strftime("%B %Y")


def _coerce_ts(ts: Any) -> float | None:
    """
    Normalise various representations to float epoch seconds (UTC).
    Accepts:
      - float/int
      - datetime (naive treated as UTC)
      - ISO-ish string
      - None
    """
    if ts is None:
        return None

    if isinstance(ts, (int, float)):
        return float(ts)

    if isinstance(ts, datetime):
        dt = ts
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()

    if isinstance(ts, str):
        try:
            dt = dtparser.parse(ts)
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).timestamp()
        except Exception:
            return None

    # Unknown type
    return None


def _in_range(ts: Any, start: Any, end: Any) -> bool:
    """
    Robust range predicate:
      - ts can be float/datetime/str/None
      - start/end can be float/datetime/str
    """
    ts2 = _coerce_ts(ts)
    if ts2 is None:
        # allow undated items to prevent 0-selection failures
        return True

    s2 = _coerce_ts(start)
    e2 = _coerce_ts(end)

    # If bounds are weird/missing, fail open rather than crashing the run
    if s2 is None or e2 is None:
        return True

    return s2 <= ts2 <= e2


def fmt_iso(ts: Any) -> str:
    ts2 = _coerce_ts(ts)
    if ts2 is None:
        return ""
    try:
        return datetime.fromtimestamp(ts2, tz=timezone.utc).date().isoformat()
    except Exception:
        return ""


# -------------------- QUALITY / SPAM SUPPRESSION ---------------------

_MEETING_URL_HINTS = ("/news-and-calendar/", "/calendar", "/events", "/meeting", "/meetings")
_MEETING_URL_STRONG = ("online-meeting", "online_meeting", "/agenda")
_MEETING_TITLE_HINTS = (
    "online meeting",
    "srb online meeting",
    "frb online meeting",
    "teg online meeting",
    "technical expert group meeting",
    "board meeting",
)


def is_meeting_notice(url: str, title: str) -> bool:
    u = (url or "").lower()
    t = (title or "").lower()

    if any(x in u for x in _MEETING_URL_STRONG):
        return True

    if any(x in u for x in _MEETING_URL_HINTS):
        if "meeting" in u or "meeting" in t or any(x in t for x in _MEETING_TITLE_HINTS):
            return True

    if any(x in t for x in _MEETING_TITLE_HINTS):
        return True

    return False


_HUB_PATHS = {
    "/news",
    "/newsroom",
    "/media",
    "/press",
    "/publications",
    "/updates",
    "/news-and-calendar/news",
    "/news-centre",
}


def is_hub_url(url: str) -> bool:
    try:
        p = urllib.parse.urlparse(url)
        path = (p.path or "/").rstrip("/")
        if not path:
            path = "/"
        if p.query:
            return True
        if path in _HUB_PATHS:
            return True
    except Exception:
        return False
    return False


def substance_score(text: str) -> int:
    t = (text or "").lower()
    score = 0

    if any(w in t for w in ("final", "decision", "approved", "determination", "rule change")):
        score += 3
    if any(w in t for w in ("consultation", "draft", "exposure draft", "guidance", "standard", "framework")):
        score += 2

    if any(w in t for w in ("mw", "gw", "million", "billion", "$", "€", "aud", "eur")):
        score += 2
    if any(w in t for w in ("auction", "tender", "capacity", "tariff", "price", "market")):
        score += 1

    if any(w in t for w in ("meeting", "registration", "observers", "agenda")):
        score -= 6

    if len(t) < 800:
        score -= 2

    return score


# ----------------------- DEBUG HELPERS ------------------------


def _dump_json(path: Path, obj) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
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
    dom = normalise_domain(it.url)
    allow = set(filters.get("allow_domains", []))
    deny = set(filters.get("deny_domains", []))
    deny_title = filters.get("title_deny_regex", [])
    keep_keywords = filters.get("keep_keywords", [])

    if allow and not any(dom.endswith(d) for d in allow):
        drop_log(f"domain_not_allowed\t{dom}\t{it.url}")
        return False

    if any(dom.endswith(d) for d in deny):
        drop_log(f"domain_denied\t{dom}\t{it.url}")
        return False

    title = (it.title or "").strip()
    title_l = title.lower()

    if keep_keywords and any(k in title_l for k in keep_keywords):
        return True

    for pat in deny_title:
        if pat.search(title):
            drop_log(f"title_denied\t{title}\t{it.url}")
            return False

    return True


# ----------------------- ITEM COLLECTION -----------------------


def collect_items(sources, drop_log) -> List[Item]:
    def _infer_type(url: str) -> str:
        u = (url or "").lower()
        if any(x in u for x in (".xml", "/rss", "feed", "atom")):
            return "rss"
        return "html"

    def _iter_sources(sources_obj):
        if sources_obj is None:
            return
        if isinstance(sources_obj, dict):
            for k, v in sources_obj.items():
                if isinstance(v, list):
                    for item in v:
                        yield (k, item)
                else:
                    yield (k, v)
            return
        if isinstance(sources_obj, list):
            for item in sources_obj:
                yield (None, item)
            return
        yield (None, sources_obj)

    pool: List[Item] = []

    for forced_type, src in _iter_sources(sources):
        url = ""
        name = ""
        stype = ""

        try:
            if isinstance(src, str):
                url = src.strip()
                if not url:
                    continue
                stype = (forced_type or _infer_type(url)).strip().lower()
                name = url

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

    # robust sort
    pool.sort(key=lambda x: _coerce_ts(getattr(x, "published_ts", None)) or 0.0, reverse=True)
    return pool


# ----------------------- MONTHLY GENERATION --------------------


def generate_monthly_for(ym: str) -> str:
    os.environ["TARGET_YM"] = ym

    y, m = map(int, ym.split("-"))
    start, end = _month_bounds(y, m)  # keep as datetime (safe now)

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

        if DEBUG:
            _dump_json(
                OUTDIR / f"debug-pool-{section.replace(' ','_')}-{ym}.json",
                [
                    {
                        "title": it.title,
                        "url": it.url,
                        "source": it.source,
                        "published_ts": it.published_ts,
                        "published_iso": fmt_iso(it.published_ts),
                        "published_ts_coerced": _coerce_ts(it.published_ts),
                    }
                    for it in pool[:150]
                ],
            )

        selected: List[Dict] = []
        section_dom_counts: Dict[str, int] = {}

        for it in pool:
            if not _in_range(it.published_ts, start, end):
                if DEBUG:
                    drop_log(f"out_of_range\t{fmt_iso(it.published_ts)}\t{it.url}")
                continue

            if not _passes_filters(it, filters, drop_log):
                continue

            if is_hub_url(it.url):
                if DEBUG:
                    drop_log(f"hub_url\t{it.url}")
                continue

            if is_meeting_notice(it.url, it.title or ""):
                if DEBUG:
                    drop_log(f"meeting_notice\t{it.url}")
                continue

            h = sha1(it.url)
            if h in seen_urls:
                if DEBUG:
                    drop_log(f"duplicate_url\t-\t{it.url}")
                continue

            txt = fetch_full_text(it.url)
            txt = normalize_whitespace(txt)

            dom = normalise_domain(it.url)

            if section_dom_counts.get(dom, 0) >= MAX_PER_DOMAIN_PER_SECTION:
                if DEBUG:
                    drop_log(
                        f"per_section_domain_cap\tsection={section}\tdom={dom} cap={MAX_PER_DOMAIN_PER_SECTION}\t{it.url}"
                    )
                continue

            threshold = PRIORITY_MIN_CHARS if any(dom.endswith(d) for d in PRIORITY_DOMAINS) else MIN_TEXT_CHARS
            if len(txt) < threshold and len(it.summary or "") < 160:
                if DEBUG:
                    drop_log(f"too_short\tlen={len(txt)}/thr={threshold}\t{it.url}")
                continue

            sscore = substance_score(txt)
            if sscore < MIN_SUBSTANCE_SCORE:
                if DEBUG:
                    drop_log(f"low_substance\tscore={sscore} min={MIN_SUBSTANCE_SCORE}\t{it.url}")
                continue

            section_dom_counts[dom] = section_dom_counts.get(dom, 0) + 1

            selected.append(
                {
                    "section": section,
                    "title": it.title or it.url.split("/")[-1].replace("-", " ")[:100],
                    "url": it.url,
                    "sources_urls": [it.url],
                    "summary": it.summary or "",
                    "text": txt,
                    "published": fmt_iso(it.published_ts),
                }
            )
            seen_urls.add(h)

            if len(selected) >= ITEMS_PER_SECTION:
                break

        print(f"[selected] {len(selected)} from {section}")
        chosen.extend(selected)

    chosen = sorted(chosen, key=lambda x: (x.get("published", ""), x.get("section", "")), reverse=True)

    per_domain: Dict[str, int] = {}
    filtered: List[Dict] = []
    for row in chosen:
        dom = normalise_domain(row["url"])
        per_domain[dom] = per_domain.get(dom, 0) + 1
        if per_domain[dom] <= PER_DOMAIN_CAP:
            filtered.append(row)
        else:
            if DEBUG:
                drop_log(f"per_domain_cap\tdom={dom} cap={PER_DOMAIN_CAP}\t{row['url']}")

    chosen = filtered[:12]

    _dump_json(
        selected_file,
        [{"title": x["title"], "url": x["url"], "published": x["published"], "section": x["section"]} for x in chosen],
    )

    try:
        with meta_file.open("w", encoding="utf-8") as mf:
            mf.write(f"month: {ym}\n")
            mf.write(f"outdir: {OUTDIR.resolve()}\n")
            mf.write(f"drops_file: {drop_file.resolve()}\n")
            mf.write(f"selected_file: {selected_file.resolve()}\n")
            mf.write("counts:\n")
            mf.write(f"  selected_total: {len(chosen)}\n")
            from collections import Counter

            c = Counter([x.get("section", "") for x in chosen])
            for k, v in c.items():
                mf.write(f"  section[{k}]: {v}\n")
    except Exception as e:
        print(f"[warn] failed to write meta file: {e}")

    if len(chosen) < MIN_TOTAL_ITEMS:
        print(f"[warn] Few items in range ({len(chosen)}<{MIN_TOTAL_ITEMS}); writing placeholder.")
        # Keep deterministic dates for the placeholder
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
        end_ym = os.getenv("END_YM", "2025-10")
        sy, sm = map(int, start_ym.split("-"))
        ey, em = map(int, end_ym.split("-"))
        y, m = sy, sm
        while (y < ey) or (y == ey and m <= em):
            start_dt, end_dt = _month_bounds(y, m)
            ym = f"{y:04d}-{m:02d}"
            print(f"\n=== {ym} ({start_dt.date()} -> {end_dt.date()}) ===")
            md = generate_monthly_for(ym)
            out = OUTDIR / f"monthly-digest-{ym}.md"
            out.write_text(md, encoding="utf-8")
            print(f"[write] {out}")

            m += 1
            if m > 12:
                m = 1
                y += 1
    else:
        ym = os.getenv("YM", datetime.now(timezone.utc).strftime("%Y-%m"))
        md = generate_monthly_for(ym)
        out = OUTDIR / f"monthly-digest-{ym}.md"
        out.write_text(md, encoding="utf-8")
        print(f"[write] {out}")


if __name__ == "__main__":
    main()
