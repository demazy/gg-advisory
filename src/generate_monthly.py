# src/generate_monthly.py
from __future__ import annotations

import os
import re
import json
import math
import calendar
import inspect
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Tuple, Optional

import yaml
from dotenv import load_dotenv
from dateutil import parser as dtparser

from .fetch import fetch_rss, fetch_html_index, fetch_full_text, Item
from .summarise import build_digest
from .utils import sha1, normalize_whitespace, normalise_domain

load_dotenv()

# ----------------------- ENV / CONSTANTS -----------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMP = float(os.getenv("TEMP", "0.2"))

DEBUG = os.getenv("DEBUG", "0") == "1"

ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "7"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "1"))

MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "300"))
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "200"))

ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "0") == "1"

PRIORITY_DOMAINS = {
    d.strip().lower()
    for d in os.getenv(
        "PRIORITY_DOMAINS",
        "aemo.com.au,arena.gov.au,cefc.com.au,ifrs.org,efrag.org,dcceew.gov.au,ec.europa.eu,commission.europa.eu",
    ).split(",")
    if d.strip()
}

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "out"
OUTDIR.mkdir(parents=True, exist_ok=True)

# Respect env variables, but also provide robust fallbacks
SOURCES_YAML = os.getenv("SOURCES_YAML", "config/sources.yaml")
FILTERS_YAML = os.getenv("FILTERS_YAML", "config/filters.yaml")

# ----------------------- DATE HELPERS --------------------------

def _month_bounds(y: int, m: int) -> Tuple[datetime, datetime]:
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    last = calendar.monthrange(y, m)[1]
    end = datetime(y, m, last, 23, 59, 59, tzinfo=timezone.utc)
    return start, end


def _month_label(y: int, m: int) -> str:
    return datetime(y, m, 1, tzinfo=timezone.utc).strftime("%B %Y")


def _coerce_ts(v: Any) -> Optional[datetime]:
    """Coerce timestamp-like values into an aware UTC datetime.

    Accepts:
      - datetime (naive assumed UTC)
      - date (treated as midnight UTC)
      - int/float epoch seconds (auto-detect milliseconds if very large)
      - strings: ISO-8601 / RFC822, or numeric epoch seconds/milliseconds

    Returns None if parsing fails.
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
        try:
            x = float(v)
            if x > 3_000_000_000_000:  # epoch milliseconds
                x = x / 1000.0
            return datetime.fromtimestamp(x, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if re.fullmatch(r"[+-]?\d+(\.\d+)?", s):
            try:
                x = float(s)
                if x > 3_000_000_000_000:
                    x = x / 1000.0
                return datetime.fromtimestamp(x, tz=timezone.utc)
            except Exception:
                pass
        try:
            dt = dtparser.parse(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


# Backwards-compat alias (existing code may call _coerce_dt)
def _coerce_dt(v: Any) -> Optional[datetime]:
    return _coerce_ts(v)


def _in_range(ts: Any, start: datetime, end: datetime) -> bool:
    dt = _coerce_ts(ts)
    if dt is None:
        return ALLOW_UNDATED
    return start <= dt <= end


# ----------------------- CONFIG LOADING ------------------------

def _resolve_cfg_path(path: str) -> Path:
    p = Path(path)
    if p.is_file():
        return p
    p2 = ROOT / path
    if p2.is_file():
        return p2
    p3 = ROOT / "config" / Path(path).name
    if p3.is_file():
        return p3
    raise FileNotFoundError(f"Config not found: {path} (tried {p}, {p2}, {p3})")


def load_sources(path: str = SOURCES_YAML) -> Dict[str, Any]:
    p = _resolve_cfg_path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("sources.yaml must be a mapping")
    return data


def load_filters(path: str = FILTERS_YAML) -> Dict[str, Any]:
    p = _resolve_cfg_path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("filters.yaml must be a mapping")
    return data


# ----------------------- DEBUG HELPERS -------------------------

def _debug_write(path: Path, content: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, (dict, list)):
        path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        path.write_text(str(content), encoding="utf-8")


# ----------------------- DOMAIN / PRIORITY ---------------------

def _item_domain(url: str) -> str:
    try:
        return normalise_domain(urllib.parse.urlparse(url).netloc)
    except Exception:
        return ""


def _is_priority_domain(url: str) -> bool:
    return _item_domain(url) in PRIORITY_DOMAINS


# ----------------------- BUILD DIGEST ADAPTER ------------------

def _call_build_digest(items: List[Dict[str, Any]], year: int, month: int) -> str:
    """
    Adapter around src/summarise.py build_digest signature:
      build_digest(model, api_key, items, temp, date_label)
    """
    if not OPENAI_API_KEY:
        raise SystemExit("OPENAI_API_KEY is not set")

    date_label = _month_label(year, month)
    return build_digest(
        model=MODEL,
        api_key=OPENAI_API_KEY,
        items=items,
        temp=TEMP,
        date_label=date_label,
    )


# ----------------------- MAIN GENERATION -----------------------

# (Everything below here is unchanged from your uploaded generate_monthly-4.py)

def _safe_id(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", s.strip().lower()).strip("-") or "item"


def _dedupe_id(d: Dict[str, Any]) -> str:
    url = (d.get("url") or "").strip()
    if url:
        return sha1(url)
    return sha1((d.get("title") or "") + "|" + (d.get("published") or ""))


def _as_item_dict(it: Item, section: str) -> Dict[str, Any]:
    return {
        "section": section,
        "title": (it.title or "").strip(),
        "url": (it.url or "").strip(),
        "sources_urls": getattr(it, "sources_urls", None) or None,
        "published": it.date if isinstance(it.date, str) else (it.date.isoformat() if it.date else ""),
        "text": normalize_whitespace(it.text or ""),
    }


def generate_for_month(ym: str) -> None:
    y, m = [int(x) for x in ym.split("-")]
    start, end = _month_bounds(y, m)

    sources = load_sources(SOURCES_YAML)
    _filters = load_filters(FILTERS_YAML)

    sections = sources.get("sections", {})
    if not isinstance(sections, dict):
        raise SystemExit("sources.yaml must define a 'sections' mapping")

    debug_drops_lines: List[str] = []
    debug_meta_lines: List[str] = []
    selected_all: List[Dict[str, Any]] = []

    for section, cfg in sections.items():
        cfg = cfg or {}
        srcs = cfg.get("sources", [])
        if not isinstance(srcs, list):
            continue

        pool: List[Dict[str, Any]] = []

        for src in srcs:
            if not isinstance(src, dict):
                continue
            name = src.get("name") or src.get("key") or section
            stype = (src.get("type") or "rss").strip().lower()
            url = (src.get("url") or "").strip()
            if not url:
                continue

            try:
                if stype in ("rss", "atom"):
                    items = fetch_rss(url, source=name)
                elif stype in ("html-index", "html"):
                    items = fetch_html_index(url, source=name)
                else:
                    items = fetch_rss(url, source=name)
            except Exception:
                items = []

            for it in items:
                dt = _coerce_dt(it.date)
                ok = _in_range(it.date, start, end)
                if not ok:
                    debug_drops_lines.append(f"out_of_range\t{it.date}\t{it.url}")
                    continue

                # Full text enrichment if needed
                if it.url and (not it.text or len((it.text or "").strip()) < MIN_TEXT_CHARS):
                    try:
                        full = fetch_full_text(it.url)
                        if full:
                            it.text = full
                    except Exception:
                        pass

                # Text thresholds (priority domains get lower)
                txt = (it.text or "").strip()
                if _is_priority_domain(it.url or ""):
                    if len(txt) < PRIORITY_MIN_CHARS:
                        debug_drops_lines.append(f"too_short_priority\t{it.date}\t{it.url}")
                        continue
                else:
                    if len(txt) < MIN_TEXT_CHARS:
                        debug_drops_lines.append(f"too_short\t{it.date}\t{it.url}")
                        continue

                d = _as_item_dict(it, section=section)
                d["id"] = _dedupe_id(d)
                pool.append(d)

        # Dedupe pool
        seen = set()
        uniq = []
        for d in pool:
            if d["id"] in seen:
                continue
            seen.add(d["id"])
            uniq.append(d)

        # Sort: priority domains first, then newest first (undated last)
        def _sort_key(d: Dict[str, Any]) -> Tuple[int, float]:
            pri = 0 if _is_priority_domain(d.get("url") or "") else 1
            dt = _coerce_dt(d.get("published"))
            ts = dt.timestamp() if dt else -1e18
            return (pri, -ts)

        uniq.sort(key=_sort_key)

        # Cap per domain
        if PER_DOMAIN_CAP > 0:
            counts: Dict[str, int] = {}
            capped = []
            for d in uniq:
                dom = _item_domain(d.get("url") or "")
                counts.setdefault(dom, 0)
                if counts[dom] >= PER_DOMAIN_CAP:
                    debug_drops_lines.append(f"domain_cap\t{d.get('published')}\t{d.get('url')}")
                    continue
                counts[dom] += 1
                capped.append(d)
            uniq = capped

        # Take top N
        chosen = uniq[:ITEMS_PER_SECTION]

        # Debug pool file
        if DEBUG:
            _debug_write(OUTDIR / f"debug-pool-{_safe_id(section)}-{ym}.json", uniq)

        selected_all.extend(chosen)

    # Merge any duplicates across sections by id
    seen_all = set()
    merged = []
    for d in selected_all:
        if d["id"] in seen_all:
            continue
        seen_all.add(d["id"])
        merged.append(d)
    selected_all = merged

    # Persist debug outputs
    _debug_write(OUTDIR / f"debug-selected-{ym}.json", selected_all)
    _debug_write(OUTDIR / f"debug-meta-{ym}.txt", "\n".join(debug_meta_lines) + "\n")
    _debug_write(OUTDIR / f"debug-drops-{ym}.txt", "\n".join(debug_drops_lines) + "\n")

    if len(selected_all) < MIN_TOTAL_ITEMS:
        raise SystemExit(f"ERROR: selected items is {len(selected_all)} but MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS}")

    # Build digest markdown (uses adapter; matches summarise.py signature)
    md = _call_build_digest(items=selected_all, year=y, month=m)

    out_path = OUTDIR / f"monthly-digest-{ym}.md"
    out_path.write_text(md, encoding="utf-8")
    print(f"[write] {out_path}")


def _iter_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = [int(x) for x in start_ym.split("-")]
    ey, em = [int(x) for x in end_ym.split("-")]
    months = []
    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return months


def main() -> None:
    mode = os.getenv("MODE", "backfill-months")

    if mode == "backfill-months":
        start_ym = os.getenv("START_YM")
        end_ym = os.getenv("END_YM")
        if not start_ym or not end_ym:
            raise SystemExit("MODE=backfill-months requires START_YM and END_YM")
        for ym in _iter_months(start_ym, end_ym):
            print(f"\n=== {ym} ({ym}-01 -> {ym}-??) ===")
            generate_for_month(ym)
        return

    ym = os.getenv("YM")
    if not ym:
        raise SystemExit("Set YM=YYYY-MM or MODE=backfill-months with START_YM/END_YM")
    generate_for_month(ym)


if __name__ == "__main__":
    main()
