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


def _month_bounds_utc(ym: str) -> Tuple[datetime, datetime]:
    y, m = map(int, ym.split("-"))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    last_day = calendar.monthrange(y, m)[1]
    end = datetime(y, m, last_day, 23, 59, 59, tzinfo=timezone.utc)
    return start, end


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

    # epoch seconds (or milliseconds if obviously too large)
    if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
        try:
            x = float(v)
            if x > 3_000_000_000_000:  # ~ year 2065 if interpreted as ms
                x = x / 1000.0
            return datetime.fromtimestamp(x, tz=timezone.utc)
        except Exception:
            return None

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None

        # numeric string epoch seconds / ms
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


# Backwards-compat alias (some callers may still use _coerce_dt)
def _coerce_dt(v: Any) -> Optional[datetime]:
    return _coerce_ts(v)


def _in_range(ts: Any, start: datetime, end: datetime) -> bool:
    dt = _coerce_ts(ts)
    if dt is None:
        return ALLOW_UNDATED
    return start <= dt <= end


# ----------------------- CONFIG LOADING ------------------------


def _resolve_cfg_path(path: str) -> Path:
    """
    Accept:
      - config/sources.yaml
      - sources.yaml
      - /abs/path
    Robustly resolve relative to repo root.
    """
    p = Path(path)
    if p.is_file():
        return p
    # Try relative to repo root
    p2 = ROOT / path
    if p2.is_file():
        return p2
    # Try config/ prefix if not provided
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


# ----------------------- COLLECTION ----------------------------


def _safe_slug(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s.strip().lower()).strip("-")
    return s or "source"


def _pool_debug_path(ym: str, source_key: str) -> Path:
    return OUTDIR / f"debug-pool-{_safe_slug(source_key)}-{ym}.json"


def _debug_meta_path(ym: str) -> Path:
    return OUTDIR / f"debug-meta-{ym}.txt"


def _debug_drops_path(ym: str) -> Path:
    return OUTDIR / f"debug-drops-{ym}.txt"


def _debug_selected_path(ym: str) -> Path:
    return OUTDIR / f"debug-selected-{ym}.json"


def _write_debug_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def _item_domain(it: Item) -> str:
    try:
        d = normalise_domain(urllib.parse.urlparse(it.url).netloc)
        return d
    except Exception:
        return ""


def _is_priority(it: Item) -> bool:
    d = _item_domain(it)
    return d in PRIORITY_DOMAINS


def _has_sufficient_text(it: Item) -> bool:
    if not it.text:
        return False
    return len(it.text.strip()) >= MIN_TEXT_CHARS


def _is_priority_text_ok(it: Item) -> bool:
    if not it.text:
        return False
    return len(it.text.strip()) >= PRIORITY_MIN_CHARS


def _dedupe_key(it: Item) -> str:
    # url is best, fallback to title+date
    base = it.url or ""
    if not base:
        base = (it.title or "") + "|" + (str(it.date) if it.date else "")
    return sha1(base)


def _cap_by_domain(items: List[Item], cap: int) -> List[Item]:
    if cap <= 0:
        return items
    out: List[Item] = []
    counts: Dict[str, int] = {}
    for it in items:
        d = _item_domain(it)
        counts.setdefault(d, 0)
        if counts[d] >= cap:
            continue
        counts[d] += 1
        out.append(it)
    return out


def _sort_items(items: List[Item]) -> List[Item]:
    # Priority sources first, then newest first (undated last)
    def _key(it: Item):
        pri = 0 if _is_priority(it) else 1
        dt = _coerce_dt(it.date)
        # Undated last
        ts = dt.timestamp() if dt else -1e18
        return (pri, -ts)

    return sorted(items, key=_key)


def _filter_by_month(items: List[Item], ym: str, source_key: str) -> Tuple[List[Item], List[Tuple[str, Any, str]]]:
    start, end = _month_bounds_utc(ym)
    drops: List[Tuple[str, Any, str]] = []
    kept: List[Item] = []

    for it in items:
        if _in_range(it.date, start, end):
            kept.append(it)
        else:
            drops.append(("out_of_range", it.date, it.url or ""))

    # Debug: write drops
    if DEBUG:
        dp = _debug_drops_path(ym)
        for reason, dt, url in drops:
            _write_debug_line(dp, f"{reason}\t{dt}\t{url}")

    return kept, drops


def _enrich_item(it: Item) -> Item:
    # Fetch full text if missing/short and URL exists
    if it.url and (not it.text or len(it.text.strip()) < MIN_TEXT_CHARS):
        try:
            full = fetch_full_text(it.url)
            if full:
                it.text = full
        except Exception:
            pass
    return it


def collect_items_for_month(ym: str) -> Dict[str, List[Item]]:
    sources = load_sources(SOURCES_YAML)
    filters = load_filters(FILTERS_YAML)

    sections = sources.get("sections", {}) if isinstance(sources, dict) else {}
    if not isinstance(sections, dict):
        raise ValueError("sources.yaml 'sections' must be a mapping")

    pools: Dict[str, List[Item]] = {}

    for section_name, section_cfg in sections.items():
        # section_cfg should have a list of sources
        srcs = (section_cfg or {}).get("sources", [])
        if not isinstance(srcs, list):
            continue

        section_items: List[Item] = []

        for src in srcs:
            if not isinstance(src, dict):
                continue

            name = src.get("name") or src.get("key") or section_name
            src_type = (src.get("type") or "").lower()
            url = src.get("url") or ""

            # Fetch
            items: List[Item] = []
            try:
                if src_type in ("rss", "atom"):
                    items = fetch_rss(url, source=name)
                elif src_type in ("html-index", "html"):
                    items = fetch_html_index(url, source=name)
                else:
                    # default to rss
                    items = fetch_rss(url, source=name)
            except Exception:
                items = []

            # Apply month filter early (reduces full-text fetches)
            items_in, _drops = _filter_by_month(items, ym, name)

            # Enrich texts (optional)
            enriched: List[Item] = []
            for it in items_in:
                enriched.append(_enrich_item(it))

            # Apply content filters
            enriched2: List[Item] = []
            for it in enriched:
                # priority domains can pass with smaller text threshold
                if _is_priority(it):
                    if _is_priority_text_ok(it):
                        enriched2.append(it)
                else:
                    if _has_sufficient_text(it):
                        enriched2.append(it)

            # Dedupe within source
            seen: set[str] = set()
            unique: List[Item] = []
            for it in enriched2:
                k = _dedupe_key(it)
                if k in seen:
                    continue
                seen.add(k)
                unique.append(it)

            # Debug: write pool
            if DEBUG:
                pp = _pool_debug_path(ym, name)
                pp.write_text(
                    json.dumps([it.model_dump() for it in unique], ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

            section_items.extend(unique)

        # Dedupe across section
        seen2: set[str] = set()
        deduped: List[Item] = []
        for it in section_items:
            k = _dedupe_key(it)
            if k in seen2:
                continue
            seen2.add(k)
            deduped.append(it)

        # Sort + cap
        deduped = _sort_items(deduped)
        deduped = _cap_by_domain(deduped, PER_DOMAIN_CAP)

        pools[section_name] = deduped

    # Debug meta
    if DEBUG:
        mp = _debug_meta_path(ym)
        mp.write_text(
            "\n".join(
                [
                    f"ym={ym}",
                    f"ITEMS_PER_SECTION={ITEMS_PER_SECTION}",
                    f"PER_DOMAIN_CAP={PER_DOMAIN_CAP}",
                    f"MIN_TEXT_CHARS={MIN_TEXT_CHARS}",
                    f"PRIORITY_MIN_CHARS={PRIORITY_MIN_CHARS}",
                    f"MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS}",
                    f"ALLOW_UNDATED={ALLOW_UNDATED}",
                    f"PRIORITY_DOMAINS={sorted(PRIORITY_DOMAINS)}",
                    f"MODEL={MODEL}",
                    f"TEMP={TEMP}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    return pools


# ----------------------- GENERATION ----------------------------


def _select_per_section(pools: Dict[str, List[Item]]) -> Dict[str, List[Item]]:
    selected: Dict[str, List[Item]] = {}
    for sec, items in pools.items():
        selected[sec] = items[:ITEMS_PER_SECTION]
    return selected


def generate_monthly(ym: str) -> Path:
    pools = collect_items_for_month(ym)
    selected = _select_per_section(pools)

    # Flatten selected for debug
    if DEBUG:
        sp = _debug_selected_path(ym)
        flat = []
        for sec, items in selected.items():
            for it in items:
                d = it.model_dump()
                d["section"] = sec
                flat.append(d)
        sp.write_text(json.dumps(flat, ensure_ascii=False, indent=2), encoding="utf-8")

    # Build the digest markdown
    md = build_digest(selected, ym=ym, model=MODEL, temperature=TEMP)
    out_path = OUTDIR / f"monthly-digest-{ym}.md"
    out_path.write_text(md, encoding="utf-8")
    return out_path


def _iter_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    months: List[str] = []
    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return months


def main() -> int:
    mode = os.getenv("MODE", "single").strip().lower()
    ym = os.getenv("YM", "").strip()
    start_ym = os.getenv("START_YM", "").strip()
    end_ym = os.getenv("END_YM", "").strip()

    if not OPENAI_API_KEY:
        raise SystemExit("OPENAI_API_KEY is not set")

    if mode == "single":
        if not ym:
            raise SystemExit("YM is required for MODE=single (e.g., 2026-01)")
        generate_monthly(ym)
        return 0

    if mode == "backfill-months":
        if not start_ym or not end_ym:
            raise SystemExit("START_YM and END_YM are required for MODE=backfill-months")
        for m in _iter_months(start_ym, end_ym):
            generate_monthly(m)
        return 0

    raise SystemExit(f"Unknown MODE: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())
