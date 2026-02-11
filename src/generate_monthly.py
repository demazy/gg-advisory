"""src/generate_monthly.py

Monthly digest generator (collector + selector + writer).

Implements A + B hardening:
- A: avoid selecting evergreen / hub / taxonomy pages by URL classification and
     domain-specific deny rules; also avoid costly date-resolution fetches for hubs.
- B: do not accept weak publication dates (year-only -> Jan 1, updated_time masquerading
     as published). Prefer strong date signals only; treat low-confidence dates as undated.

The module is designed to be *best-effort*: it should not crash on network errors or parse issues.
"""

from __future__ import annotations

import json
import os
import re
import fnmatch
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import yaml

from .fetch import (
    Item,
    fetch_full_text,
    fetch_html_index,
    fetch_rss,
    is_probably_taxonomy_or_hub,
)
from .summarise import build_digest
from .utils import normalise_domain


# ---------------------------- Env / settings ----------------------------

ROOT = Path(__file__).resolve().parents[1]

CFG_SOURCES = Path(os.getenv("SOURCES_YAML", str(ROOT / "config" / "sources.yaml")))
CFG_FILTERS = Path(os.getenv("FILTERS_YAML", str(ROOT / "config" / "filters.yaml")))

OUTDIR = Path(os.getenv("OUTDIR", str(ROOT / "out")))
OUTDIR.mkdir(parents=True, exist_ok=True)

DEBUG = os.getenv("DEBUG", "0").strip() == "1"

MODEL = os.getenv("MODEL", "gpt-4o-mini").strip()
TEMP = float(os.getenv("TEMP", "0.2"))

ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "7"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))
MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "300"))
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "200"))
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "1"))

ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "0").strip() == "1"

PRIORITY_DOMAINS = [
    s.strip().lower()
    for s in (os.getenv("PRIORITY_DOMAINS", "") or "").split(",")
    if s.strip()
]

MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "75"))


# ---------------------------- YAML loading ----------------------------

def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _compile_filters(cfg: dict) -> dict:
    """Normalise filter config."""
    return {
        "allow_domains": [d.lower().strip() for d in (cfg.get("allow_domains") or []) if d],
        "deny_domains": [d.lower().strip() for d in (cfg.get("deny_domains") or []) if d],
        "deny_url_substrings": [s.lower() for s in (cfg.get("deny_url_substrings") or []) if s],
        "domain_deny_substrings": {
            (k or "").lower(): [s.lower() for s in (v or [])]
            for k, v in (cfg.get("domain_deny_substrings") or {}).items()
        },
    }


def _domain_match(dom: str, pat: str) -> bool:
    """Match a normalised domain against an allow/deny pattern.

    Supported patterns:
    - Exact domain (e.g., "aemo.com.au") matches that domain and subdomains.
    - Suffix (e.g., "gov.au" or ".gov.au") matches any domain ending with that suffix.
    - Wildcards via fnmatch (e.g., "*.gov.au", "energy*.com.au").
    """
    dom = (dom or "").lower().strip(".")
    pat = (pat or "").lower().strip()
    if not dom or not pat:
        return False
    if pat.startswith("www."):
        pat = pat[4:]
    if pat.startswith("."):
        return dom.endswith(pat)

    if "*" in pat or "?" in pat:
        return fnmatch.fnmatch(dom, pat)

    # Exact domain or subdomain
    if dom == pat:
        return True
    if dom.endswith("." + pat):
        return True

    # Treat bare suffixes like "gov.au"
    if "." in pat and dom.endswith("." + pat):
        return True
    return False


def _passes_filters(it: Item, flt: dict) -> Tuple[bool, str]:
    url = (it.url or "").strip()
    if not url:
        return False, "no_url"
    dom = normalise_domain(url)

    allow = flt.get("allow_domains") or []
    deny = flt.get("deny_domains") or []

    if deny and any(_domain_match(dom, d) for d in deny):
        return False, "deny_domain"
    if allow and not any(_domain_match(dom, a) for a in allow):
        return False, "domain_not_allowed"

    url_l = url.lower()
    for s in flt.get("deny_url_substrings") or []:
        if s and s in url_l:
            return False, "deny_url_substring"

    per_dom = flt.get("domain_deny_substrings") or {}
    for dom_key, subs in per_dom.items():
        if dom_key and _domain_match(dom, dom_key):
            for s in subs or []:
                if s and s in url_l:
                    return False, f"deny_url_substring:{dom_key}"

    return True, ""


# ---------------------------- Time / date hygiene ----------------------------

def _coerce_ts(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, datetime):
        dt = x
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return None
    return None


def _in_range(ts, start, end) -> bool:
    """
    Robust range predicate:
    - ts can be float/datetime/str/None
    - start/end can be datetime OR float OR str
    """
    ts2 = _coerce_ts(ts)
    if ts2 is None:
        return ALLOW_UNDATED  # fail-open for undated items
    s2 = _coerce_ts(start)
    e2 = _coerce_ts(end)
    if s2 is None or e2 is None:
        return True
    return s2 <= ts2 <= e2


# ---------------------------- Quality / ranking ----------------------------

_HUB_HINTS = (
    "/newsroom", "/news", "/media", "/insights", "/blog", "/articles", "/events",
    "/resources", "/publications", "/reports", "/updates", "/announcements",
)

def _looks_articleish(url: str) -> bool:
    u = (url or "").lower()
    if not u:
        return False
    if is_probably_taxonomy_or_hub(u):
        return False
    # Prefer URLs with some “depth” and avoid obvious listing pages
    if any(h in u for h in _HUB_HINTS) and u.rstrip("/").endswith(tuple(_HUB_HINTS)):
        return False
    return True


def _substance_score(text: str, url: str) -> int:
    """Simple heuristic score; higher = more likely a real article."""
    t = (text or "").strip()
    n = len(t)
    if n <= 0:
        return -10
    score = 0
    if n >= MIN_TEXT_CHARS:
        score += 2
    elif n >= max(120, MIN_TEXT_CHARS // 2):
        score += 1
    else:
        score -= 2

    # Penalise very “menu-like” content
    if t.count("\n") > 80 and n < 1200:
        score -= 1

    u = (url or "").lower()
    if is_probably_taxonomy_or_hub(u):
        score -= 3
    return score


def _is_priority(url: str) -> bool:
    dom = normalise_domain(url)
    return any(_domain_match(dom, d) for d in PRIORITY_DOMAINS)


def _rank_key(it: Item) -> Tuple[int, int, float]:
    """Rank by priority, then published_ts (newer first), then title length."""
    pri = 1 if _is_priority(it.url or "") else 0
    ts = _coerce_ts(it.published_ts) or 0.0
    return (pri, int(ts), len((it.title or "").strip()))


# ---------------------------- Collect ----------------------------

def _collect_section_items(section_name: str, section_cfg: dict, drops: List[dict]) -> List[Item]:
    items: List[Item] = []

    # Support both schemas:
    # - New: section_cfg['sources']={'rss':[...],'html':[...]}
    # - Current (repo): section_cfg has 'rss'/'html' at top level.
    sources = section_cfg.get("sources")
    src_cfg = sources if isinstance(sources, dict) else section_cfg

    rss = src_cfg.get("rss") or src_cfg.get("rss_feeds") or []
    html = src_cfg.get("html") or src_cfg.get("indexes") or []
    if isinstance(rss, str):
        rss = [rss]
    if isinstance(html, str):
        html = [html]

    # Guard: no sources configured (or schema mismatch)
    if not rss and not html:
        drops.append({"reason": "section_no_sources", "section": section_name})
        return []

    for src in rss:
        try:
            items.extend(fetch_rss(src, source_name=section_name))
        except Exception as e:
            drops.append({"reason": "rss_error", "section": section_name, "url": src, "error": str(e)})

    for idx_url in html:
        try:
            items.extend(
                fetch_html_index(
                    idx_url,
                    source_name=section_name,
                    max_date_resolve_fetches=MAX_DATE_RESOLVE_FETCHES_PER_INDEX,
                )
            )
        except Exception as e:
            drops.append({"reason": "html_index_error", "section": section_name, "url": idx_url, "error": str(e)})

    # de-dup by URL
    seen = set()
    out: List[Item] = []
    for it in items:
        u = (it.url or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(it)
    return out


# ---------------------------- Select ----------------------------

def _select_from_pool(
    pool: List[Item],
    start: datetime,
    end: datetime,
    flt: dict,
    per_domain_cap: int,
    items_per_section: int,
    strict: bool,
    drops: List[dict],
) -> List[Item]:
    # Filter by allow/deny + URL hygiene
    filtered: List[Item] = []
    for it in pool:
        ok, reason = _passes_filters(it, flt)
        if not ok:
            drops.append({"reason": reason, "section": it.source or "", "url": it.url})
            continue
        if strict and not _looks_articleish(it.url or ""):
            drops.append({"reason": "not_articleish", "section": it.source or "", "url": it.url})
            continue
        filtered.append(it)

    # Date range
    ranged: List[Item] = []
    for it in filtered:
        if _in_range(it.published_ts, start, end):
            ranged.append(it)
        else:
            drops.append({"reason": "out_of_range", "date": it.published_iso or "", "url": it.url})

    # Sort by rank key
    ranged.sort(key=_rank_key, reverse=True)

    # Per-domain cap + substance scoring
    by_dom = defaultdict(int)
    selected: List[Item] = []
    for it in ranged:
        dom = normalise_domain(it.url or "")
        if by_dom[dom] >= per_domain_cap:
            drops.append({"reason": "domain_cap", "domain": dom, "url": it.url})
            continue

        # fetch full text for scoring/summarising
        try:
            full = fetch_full_text(it.url or "")
        except Exception:
            full = ""

        score = _substance_score(full, it.url or "")
        min_chars = PRIORITY_MIN_CHARS if _is_priority(it.url or "") else MIN_TEXT_CHARS

        if strict and len(full) < min_chars:
            drops.append({"reason": "low_substance", "score": score, "url": it.url})
            continue
        if strict and score < 0:
            drops.append({"reason": "low_substance", "score": score, "url": it.url})
            continue

        it.full_text = full
        it.substance_score = score

        selected.append(it)
        by_dom[dom] += 1
        if len(selected) >= items_per_section:
            break

    return selected


# ---------------------------- Month runner ----------------------------

def _month_bounds(ym: str) -> Tuple[datetime, datetime]:
    dt = datetime.strptime(ym, "%Y-%m").replace(tzinfo=timezone.utc)
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month - timedelta(seconds=1)
    return start, end


def generate_for_month(ym: str, cfg_sources: dict, flt: dict) -> Tuple[str, List[Item], List[dict]]:
    start, end = _month_bounds(ym)
    drops: List[dict] = []
    selected_all: List[Item] = []

    sections = (cfg_sources.get("sections") or {})
    if not isinstance(sections, dict):
        sections = {}

    meta = {
        "ym": ym,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sections": list(sections.keys()),
    }

    print(f"\n=== {ym} ({start.date()} -> {end.date()}) ===")

    for section, sec_cfg in sections.items():
        print(f" {section}")
        sec_cfg = sec_cfg or {}
        pool = _collect_section_items(section, sec_cfg, drops)

        if DEBUG:
            (OUTDIR / f"debug-pool-{section.replace(' ', '_').replace('&','_')}-{ym}.json").write_text(
                json.dumps([asdict(i) for i in pool], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        print(f"[pool] candidates: {len(pool)}")

        # strict then relaxed
        sel = _select_from_pool(
            pool=pool,
            start=start,
            end=end,
            flt=flt,
            per_domain_cap=PER_DOMAIN_CAP,
            items_per_section=ITEMS_PER_SECTION,
            strict=True,
            drops=drops,
        )

        if not sel:
            sel = _select_from_pool(
                pool=pool,
                start=start,
                end=end,
                flt=flt,
                per_domain_cap=PER_DOMAIN_CAP,
                items_per_section=ITEMS_PER_SECTION,
                strict=False,
                drops=drops,
            )

        print(f"[selected] {len(sel)} from {section}")
        selected_all.extend(sel)

    # Global fallback if absolutely nothing selected but there were candidates.
    if not selected_all:
        print("[warn] No selected items in strict/relaxed passes; applying ±3 day fallback window.")
        start2 = start - timedelta(days=3)
        end2 = end + timedelta(days=3)

        selected_all = []
        for section, sec_cfg in (cfg_sources.get("sections") or {}).items():
            pool = _collect_section_items(section, sec_cfg or {}, drops)
            sel = _select_from_pool(
                pool=pool,
                start=start2,
                end=end2,
                flt=flt,
                per_domain_cap=PER_DOMAIN_CAP,
                items_per_section=ITEMS_PER_SECTION,
                strict=False,
                drops=drops,
            )
            selected_all.extend(sel)

    if not selected_all:
        print("[warn] Still no items after fallback; last-resort pick (ignoring dates) to prevent pipeline failure.")
        # last resort: ignore date filtering; still apply allow/deny and basic URL hygiene
        all_pool: List[Item] = []
        for section, sec_cfg in (cfg_sources.get("sections") or {}).items():
            all_pool.extend(_collect_section_items(section, sec_cfg or {}, drops))

        # allow/deny only
        tmp: List[Item] = []
        for it in all_pool:
            ok, reason = _passes_filters(it, flt)
            if not ok:
                drops.append({"reason": reason, "section": it.source or "", "url": it.url})
                continue
            tmp.append(it)

        tmp.sort(key=_rank_key, reverse=True)

        # pick at least one if possible
        if tmp:
            it = tmp[0]
            try:
                it.full_text = fetch_full_text(it.url or "")
            except Exception:
                it.full_text = ""
            it.substance_score = _substance_score(it.full_text or "", it.url or "")
            selected_all = [it]
        else:
            selected_all = []

    # Write debug files
    (OUTDIR / f"debug-selected-{ym}.json").write_text(
        json.dumps([asdict(i) for i in selected_all], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUTDIR / f"debug-drops-{ym}.txt").write_text(
        "\n".join(
            [
                "\t".join(str(x.get(k, "")) for k in ("reason", "date", "domain", "url"))
                for x in drops
            ]
        ),
        encoding="utf-8",
    )
    (OUTDIR / f"debug-meta-{ym}.txt").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Build markdown digest
    md = build_digest(
        ym=ym,
        items=selected_all,
        model=MODEL,
        temperature=TEMP,
    )
    out_path = OUTDIR / f"monthly-digest-{ym}.md"
    out_path.write_text(md, encoding="utf-8")
    print(f"[write] {out_path}")
    return str(out_path), selected_all, drops


def main() -> None:
    cfg_sources = _load_yaml(CFG_SOURCES)
    cfg_filters = _load_yaml(CFG_FILTERS)
    flt = _compile_filters(cfg_filters)

    mode = os.getenv("MODE", "").strip()
    start_ym = os.getenv("START_YM", "").strip()
    end_ym = os.getenv("END_YM", "").strip()

    if mode == "backfill-months" and start_ym and end_ym:
        # inclusive month loop
        cur = datetime.strptime(start_ym, "%Y-%m")
        endm = datetime.strptime(end_ym, "%Y-%m")
        while cur <= endm:
            ym = cur.strftime("%Y-%m")
            generate_for_month(ym, cfg_sources, flt)
            # advance one month
            cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    else:
        # single month
        ym = os.getenv("YM", "").strip()
        if not ym:
            raise SystemExit("YM not set (expected YYYY-MM)")
        generate_for_month(ym, cfg_sources, flt)


if __name__ == "__main__":
    main()
