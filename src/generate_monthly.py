#!/usr/bin/env python3
"""
Monthly digest generator.

Robustness objectives:
- Never hard-fail because config files moved (e.g., config/sources.yaml).
- Allow explicit override via env vars SOURCES_YAML and FILTERS_YAML.
- Provide actionable error messages when config is missing.
"""
from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import yaml

from .fetch import (
    Item,
    canonicalize_url,
    domain_of,
    fetch_date_only,
    fetch_full_text,
    fetch_html_index,
    fetch_rss,
)
from .summarise import build_digest


# ----------------------------
# Environment configuration
# ----------------------------

MODE = os.getenv("MODE", "monthly")  # monthly | backfill-months
START_YM = os.getenv("START_YM", "")
END_YM = os.getenv("END_YM", "")

ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "7"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))

MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "300"))
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "200"))

MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "1"))
DEBUG = os.getenv("DEBUG", "0") == "1"

MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMP = float(os.getenv("TEMP", "0.2"))

MAX_PDF_BYTES = int(os.getenv("MAX_PDF_BYTES", str(5 * 1024 * 1024)))
PDF_TRUSTED = [d.strip().lower() for d in os.getenv("PDF_TRUSTED", "").split(",") if d.strip()]
PRIORITY_DOMAINS = [d.strip().lower() for d in os.getenv("PRIORITY_DOMAINS", "").split(",") if d.strip()]

MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "250"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "5"))
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "75"))

MAX_UNDATED_RESOLVE_PER_SECTION = int(os.getenv("MAX_UNDATED_RESOLVE_PER_SECTION", "40"))
MAX_FULLTEXT_FETCHES_PER_SECTION = int(os.getenv("MAX_FULLTEXT_FETCHES_PER_SECTION", str(ITEMS_PER_SECTION * 20)))

ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "0") == "1"


# ----------------------------
# Time helpers
# ----------------------------

def _month_bounds_utc(ym: str) -> Tuple[datetime, datetime]:
    y, m = map(int, ym.split("-"))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    if m == 12:
        nxt = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    else:
        nxt = datetime(y, m + 1, 1, tzinfo=timezone.utc)
    end = nxt - timedelta(seconds=1)
    return start, end


def _coerce_ts(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, datetime):
        dt = x if x.tzinfo else x.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(x, str) and x.strip():
        try:
            dt = datetime.fromisoformat(x.replace("Z", "+00:00"))
            dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return None
    return None


def _in_range(ts, start, end) -> bool:
    ts2 = _coerce_ts(ts)
    if ts2 is None:
        return ALLOW_UNDATED
    s2 = _coerce_ts(start)
    e2 = _coerce_ts(end)
    if s2 is None or e2 is None:
        return True
    return s2 <= ts2 <= e2


# ----------------------------
# Robust config loading
# ----------------------------

def _read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _candidate_paths(name_stem: str) -> List[Path]:
    """
    Search common locations.
    Your repo uses config/, so that’s included first among folders.
    """
    folders = ("config", "configs", ".", "src", "data", "src/config", "src/configs")
    exts = ("yaml", "yml")
    out: List[Path] = []
    for folder in folders:
        for ext in exts:
            p = Path(folder) / f"{name_stem}.{ext}" if folder != "." else Path(f"{name_stem}.{ext}")
            out.append(p)
    # De-dupe
    seen = set()
    uniq: List[Path] = []
    for p in out:
        s = str(p)
        if s in seen:
            continue
        seen.add(s)
        uniq.append(p)
    return uniq


def _resolve_config_path(env_var: str, name_stem: str) -> Path:
    """
    Priority:
      1) env var if points to existing file
      2) auto-discovery
      3) clear error listing attempted paths
    """
    raw = (os.getenv(env_var, "") or "").strip()
    tried: List[Path] = []

    if raw:
        p = Path(raw)
        tried.append(p)
        if p.exists() and p.is_file():
            return p

    for p in _candidate_paths(name_stem):
        tried.append(p)
        if p.exists() and p.is_file():
            return p

    tried_str = "\n".join(f"  - {p}" for p in tried)
    raise FileNotFoundError(
        f"Could not find {name_stem} config file.\n"
        f"{env_var}={raw!r}\n"
        f"Tried:\n{tried_str}\n"
        f"Fix: commit the file to config/ (recommended) or set {env_var} to the correct path."
    )


def _load_config() -> Tuple[dict, dict]:
    sources_path = _resolve_config_path("SOURCES_YAML", "sources")
    filters_path = _resolve_config_path("FILTERS_YAML", "filters")
    return _read_yaml(sources_path), _read_yaml(filters_path)


# ----------------------------
# Selection utilities
# ----------------------------

def _norm_domain(d: str) -> str:
    d = (d or "").strip().lower()
    return d[4:] if d.startswith("www.") else d


def _priority_bonus(domain: str) -> int:
    d = _norm_domain(domain)
    for p in PRIORITY_DOMAINS:
        p2 = _norm_domain(p)
        if d == p2 or d.endswith("." + p2):
            return 2
    return 0


def _quick_score(text: str, url: str, section_kw: List[str], global_kw: List[str]) -> int:
    t = (text or "").lower()
    u = (url or "").lower()
    score = 0
    for kw in section_kw:
        if kw and kw.lower() in t:
            score += 4
    for kw in global_kw:
        if kw and kw.lower() in t:
            score += 1
    if re.search(r"/(news|media|press|insights|blog|articles|publications|updates)/", u):
        score += 1
    if re.search(r"/20\d{2}/\d{1,2}/", u):
        score += 1
    if u.endswith(".pdf") or ".pdf?" in u:
        score -= 1
    return score


def _passes_filters(
    it: Item,
    *,
    allow_domains: List[str],
    deny_domains: List[str],
    title_deny_regex: List[str],
    keep_keywords: List[str],
    section_keywords: List[str],
) -> Tuple[bool, str, int]:
    it.url = canonicalize_url(it.url)
    it.domain = _norm_domain(it.domain or domain_of(it.url))

    # Domain allow/deny (simple suffix match)
    if deny_domains:
        for d in deny_domains:
            d2 = _norm_domain(d)
            if it.domain == d2 or it.domain.endswith("." + d2):
                return (False, "domain_denied", 0)
    if allow_domains:
        ok = False
        for d in allow_domains:
            d2 = _norm_domain(d)
            if it.domain == d2 or it.domain.endswith("." + d2):
                ok = True
                break
        if not ok:
            return (False, "domain_not_allowed", 0)

    title = (it.title or "").strip()
    for pat in title_deny_regex or []:
        try:
            if pat and re.search(pat, title, re.I):
                return (False, "title_denied", 0)
        except re.error:
            continue

    blob = " ".join([title, (it.summary or "")]).strip()
    qs = _quick_score(blob, it.url, section_keywords, keep_keywords)

    if keep_keywords:
        has_any = any(kw.lower() in blob.lower() for kw in keep_keywords if kw)
        if (not has_any) and qs <= 0 and _priority_bonus(it.domain) == 0:
            return (False, "no_keyword_signal", qs)

    return (True, "ok", qs)


# ----------------------------
# Pool building / fetching
# ----------------------------

def _build_pool_for_section(section: str, section_cfg: dict) -> List[Item]:
    pool: List[Item] = []
    for feed_url in (section_cfg.get("rss") or []):
        try:
            for it in fetch_rss(feed_url):
                it.section = section
                it.source = feed_url
                pool.append(it)
        except Exception as e:
            print(f"[warn] source error: {feed_url} -> {e}")

    for index_url in (section_cfg.get("html") or []):
        try:
            for it in fetch_html_index(
                index_url,
                max_links=MAX_LINKS_PER_INDEX,
                max_pages=MAX_INDEX_PAGES,
                date_resolve_budget=MAX_DATE_RESOLVE_FETCHES_PER_INDEX,
            ):
                it.section = section
                it.source = index_url
                pool.append(it)
        except Exception as e:
            print(f"[warn] source error: {index_url} -> {e}")

    seen = set()
    uniq: List[Item] = []
    for it in pool:
        it.url = canonicalize_url(it.url)
        if it.url in seen:
            continue
        seen.add(it.url)
        it.domain = _norm_domain(it.domain or domain_of(it.url))
        uniq.append(it)

    return uniq


def _fetch_text_for_candidates(candidates: List[Item], max_fetches: int) -> List[Item]:
    if not candidates:
        return []
    take = candidates[:max_fetches]

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _one(it: Item) -> Item:
        text, dt, _mime = fetch_full_text(it.url, max_pdf_bytes=MAX_PDF_BYTES)
        if dt and it.published is None:
            it.published = dt
        it.fetched_text = text or ""
        return it

    out: List[Item] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(_one, it) for it in take]
        for fut in as_completed(futs):
            out.append(fut.result())

    order = {it.url: i for i, it in enumerate(take)}
    out.sort(key=lambda it: order.get(it.url, 10**9))
    return out


def _resolve_undated_items(items: List[Item], budget: int) -> None:
    undated = [it for it in items if it.published is None]
    if not undated or budget <= 0:
        return

    from concurrent.futures import ThreadPoolExecutor, as_completed

    take = undated[:budget]
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(fetch_date_only, it.url): it for it in take}
        for fut in as_completed(futs):
            it = futs[fut]
            try:
                it.published = fut.result()
            except Exception:
                it.published = None


# ----------------------------
# Debug outputs
# ----------------------------

def _write_debug_pool(section: str, ym: str, pool: List[Item]) -> None:
    if not DEBUG:
        return
    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"debug-pool-{section.replace(' ', '_')}-{ym}.json"
    p.write_text(json.dumps([it.to_dict() for it in pool], ensure_ascii=False, indent=2), encoding="utf-8")


def _write_debug_selected(ym: str, selected: List[Item]) -> None:
    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"debug-selected-{ym}.json"
    p.write_text(json.dumps([it.to_dict() for it in selected], ensure_ascii=False, indent=2), encoding="utf-8")


def _write_debug_drops(ym: str, drops: List[Tuple[str, str]]) -> None:
    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"debug-drops-{ym}.txt"
    lines = ["reason\tmeta\turl"]
    for reason, url in drops:
        lines.append(f"{reason}\t\t{url}")
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_debug_meta(ym: str, meta: dict) -> None:
    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"debug-meta-{ym}.txt"
    p.write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_digest(ym: str, md: str) -> Path:
    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"monthly-digest-{ym}.md"
    p.write_text(md, encoding="utf-8")
    return p


# ----------------------------
# Selection per section
# ----------------------------

def _select_for_section(
    section: str,
    pool: List[Item],
    *,
    filters_cfg: dict,
    start_dt: datetime,
    end_dt: datetime,
) -> Tuple[List[Item], List[Tuple[str, str]]]:
    allow_domains = filters_cfg.get("allow_domains") or []
    deny_domains = filters_cfg.get("deny_domains") or []
    title_deny_regex = filters_cfg.get("title_deny_regex") or []
    keep_keywords = filters_cfg.get("keep_keywords") or []
    section_kw_map = filters_cfg.get("section_keywords") or {}
    section_keywords = section_kw_map.get(section, []) or []

    passed: List[Tuple[Item, int]] = []
    drops: List[Tuple[str, str]] = []

    for it in pool:
        ok, reason, qs = _passes_filters(
            it,
            allow_domains=allow_domains,
            deny_domains=deny_domains,
            title_deny_regex=title_deny_regex,
            keep_keywords=keep_keywords,
            section_keywords=section_keywords,
        )
        if not ok:
            drops.append((reason, it.url))
            continue
        passed.append((it, qs))

    _resolve_undated_items([it for it, _ in passed], budget=MAX_UNDATED_RESOLVE_PER_SECTION)

    in_range: List[Tuple[Item, int]] = []
    for it, qs in passed:
        if _in_range(it.published, start_dt, end_dt):
            in_range.append((it, qs))
        else:
            drops.append(("out_of_range", it.url))

    in_range.sort(
        key=lambda x: (
            -_priority_bonus(x[0].domain),
            -x[1],
            -(x[0].published.timestamp() if x[0].published else 0.0),
        )
    )

    fetched = _fetch_text_for_candidates([it for it, _ in in_range], MAX_FULLTEXT_FETCHES_PER_SECTION)

    eligible: List[Item] = []
    for it in fetched:
        n = len(it.fetched_text or "")
        if _priority_bonus(it.domain) > 0:
            if n >= PRIORITY_MIN_CHARS:
                eligible.append(it)
            else:
                drops.append(("too_short_priority", it.url))
        else:
            if n >= MIN_TEXT_CHARS:
                eligible.append(it)
            else:
                drops.append(("too_short", it.url))

    def final_key(it: Item) -> Tuple[int, int, float, int]:
        blob = " ".join([(it.title or ""), (it.summary or ""), (it.fetched_text or "")[:500]])
        qs = _quick_score(blob, it.url, section_keywords, keep_keywords)
        return (
            _priority_bonus(it.domain),
            qs,
            it.published.timestamp() if it.published else 0.0,
            len(it.fetched_text or ""),
        )

    eligible.sort(key=final_key, reverse=True)

    selected: List[Item] = []
    per_domain = defaultdict(int)
    for it in eligible:
        if len(selected) >= ITEMS_PER_SECTION:
            break
        if per_domain[it.domain] >= PER_DOMAIN_CAP:
            drops.append(("domain_cap", it.url))
            continue
        per_domain[it.domain] += 1
        selected.append(it)

    return selected, drops


# ----------------------------
# Main generation
# ----------------------------

def generate_for_month(ym: str) -> Path:
    sources_cfg, filters_cfg = _load_config()
    start_dt, end_dt = _month_bounds_utc(ym)

    print(f"\n=== {ym} ({start_dt.date()} -> {end_dt.date()}) ===")

    selected_all: List[Item] = []
    drops_all: List[Tuple[str, str]] = []

    sections = sources_cfg.get("sections") or {}
    for section, section_cfg in sections.items():
        print(f"[section] {section}")
        pool = _build_pool_for_section(section, section_cfg)
        _write_debug_pool(section, ym, pool)
        print(f"[pool] candidates: {len(pool)}")

        selected, drops = _select_for_section(
            section,
            pool,
            filters_cfg=filters_cfg,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        print(f"[selected] {len(selected)} from {section}")
        selected_all.extend(selected)
        drops_all.extend(drops)

    if len(selected_all) < MIN_TOTAL_ITEMS:
        raise SystemExit(
            f"ERROR: selected items is {len(selected_all)} but MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS}. "
            "Failing run to avoid publishing placeholder."
        )

    md = build_digest(ym=ym, items=selected_all, model=MODEL, temperature=TEMP)
    out_path = _write_digest(ym, md)

    _write_debug_selected(ym, selected_all)
    _write_debug_drops(ym, drops_all)
    _write_debug_meta(
        ym,
        meta={
            "ym": ym,
            "mode": MODE,
            "items_per_section": ITEMS_PER_SECTION,
            "per_domain_cap": PER_DOMAIN_CAP,
            "min_text_chars": MIN_TEXT_CHARS,
            "priority_min_chars": PRIORITY_MIN_CHARS,
            "priority_domains": PRIORITY_DOMAINS,
            "allow_undated": ALLOW_UNDATED,
            "max_links_per_index": MAX_LINKS_PER_INDEX,
            "max_index_pages": MAX_INDEX_PAGES,
            "max_date_resolve_fetches_per_index": MAX_DATE_RESOLVE_FETCHES_PER_INDEX,
            "max_undated_resolve_per_section": MAX_UNDATED_RESOLVE_PER_SECTION,
            "max_fulltext_fetches_per_section": MAX_FULLTEXT_FETCHES_PER_SECTION,
            "sources_yaml": str(_resolve_config_path("SOURCES_YAML", "sources")),
            "filters_yaml": str(_resolve_config_path("FILTERS_YAML", "filters")),
        },
    )

    print(f"[write] {out_path.resolve()}")
    return out_path


def _iter_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    months: List[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y += 1
            m = 1
    return months


def main() -> None:
    if MODE == "backfill-months":
        if not START_YM or not END_YM:
            raise SystemExit("MODE=backfill-months requires START_YM and END_YM")
        for ym in _iter_months(START_YM, END_YM):
            generate_for_month(ym)
        return

    ym = START_YM or datetime.now(timezone.utc).strftime("%Y-%m")
    generate_for_month(ym)


if __name__ == "__main__":
    main()
