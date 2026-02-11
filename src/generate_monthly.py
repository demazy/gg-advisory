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

DEBUG = os.getenv("DEBUG", "0").strip().lower() in ("1", "true", "yes")

MODE = os.getenv("MODE", "backfill-months")  # backfill-months | single-month
START_YM = os.getenv("START_YM", "")
END_YM = os.getenv("END_YM", "")
ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "7"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))

MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "300"))
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "200"))
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "1"))

PRIORITY_DOMAINS = [d.strip().lower() for d in os.getenv("PRIORITY_DOMAINS", "").split(",") if d.strip()]

ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "0").strip().lower() in ("1", "true", "yes")


# ---------------------------- URL classification (A) ----------------------------

# Domain-specific deny substrings for evergreen/taxonomy/landing pages that routinely get mis-dated.
# You can extend/override via filters.yaml (optional keys: domain_deny_substrings).
DEFAULT_DOMAIN_DENY: Dict[str, List[str]] = {
    "arena.gov.au": [
        "/renewable-energy/",          # taxonomy/landing pages (often evergreen)
        "/what-we-do/",
        "/who-we-work-with/",
        "/about/",
    ],
    "cefc.com.au": [
        "/where-we-invest/",
        "/investment-focus-areas/",
        "/investment-portfolio/",
        "/who-we-are/",
    ],
    "ifrs.org": [
        "/content/ifrs/home",          # home/landing pages
        "/issued-standards/list-of-standards/",
    ],
}

GENERAL_DENY_SUBSTRINGS = [
    "/sitemap", "/search", "/tag/", "/tags/", "/topic/", "/topics/", "/category/", "/categories/",
    "/author/", "/authors/", "/events", "/calendar", "/subscribe", "/newsletter",
]

LOW_VALUE_TAIL = {
    "", "home", "index", "default", "overview", "about", "contact",
    "news", "newsroom", "media", "press", "publications", "resources", "insights", "updates",
    "funding", "grants", "invest", "investment", "investments",
}

_DATE_IN_URL = re.compile(r"(20\d{2})[/-](0[1-9]|1[0-2])[/-]([0-3]\d)")


def _url_has_date(url: str) -> bool:
    try:
        from urllib.parse import urlparse
        return bool(_DATE_IN_URL.search(urlparse(url).path))
    except Exception:
        return False


def _looks_articleish(url: str) -> bool:
    """A conservative heuristic: article pages tend to have a distinct slug, a date, or be a PDF."""
    if not url:
        return False
    u = url.lower()
    if u.endswith(".pdf"):
        return True
    if _url_has_date(u):
        return True
    from urllib.parse import urlparse
    path = urlparse(u).path or "/"
    segs = [s for s in path.split("/") if s]
    if not segs:
        return False
    tail = segs[-1]
    if tail in LOW_VALUE_TAIL:
        return False
    # slugs often have hyphens or are reasonably long
    if "-" in tail and len(tail) >= 12:
        return True
    if len(segs) >= 3 and len(tail) >= 10:
        return True
    return False


def _is_low_value_url(url: str, domain: str, domain_deny: Dict[str, List[str]]) -> bool:
    u = (url or "").lower()
    if not u:
        return True
    for s in GENERAL_DENY_SUBSTRINGS:
        if s in u:
            return True
    for s in domain_deny.get(domain, []):
        if s in u:
            return True
    # also reuse fetch-level hub/taxonomy check
    if is_probably_taxonomy_or_hub(u):
        return True
    return False


# ---------------------------- Time helpers ----------------------------

def _coerce_ts(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        # ISO date or datetime
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).timestamp()
        except Exception:
            return None
    if isinstance(x, datetime):
        dt = x
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
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


def _month_bounds(ym: str) -> Tuple[datetime, datetime]:
    y, m = map(int, ym.split("-"))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    if m == 12:
        end = datetime(y + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
    else:
        end = datetime(y, m + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
    return start, end


def _iter_months(start_ym: str, end_ym: str) -> List[str]:
    y0, m0 = map(int, start_ym.split("-"))
    y1, m1 = map(int, end_ym.split("-"))
    cur = datetime(y0, m0, 1, tzinfo=timezone.utc)
    end = datetime(y1, m1, 1, tzinfo=timezone.utc)
    out = []
    while cur <= end:
        out.append(f"{cur.year:04d}-{cur.month:02d}")
        # advance 1 month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            cur = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)
    return out


# ---------------------------- Filters ----------------------------

def _load_yaml(path: Path) -> dict:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        raise SystemExit(f"Cannot read YAML: {path} ({e})")


def _compile_regex_list(xs: Iterable[str]) -> List[re.Pattern]:
    out = []
    for x in xs or []:
        try:
            out.append(re.compile(x, re.I))
        except re.error:
            continue
    return out


def _compile_filters(cfg: dict) -> dict:
    return {
        "allow_domains": [d.lower().strip() for d in (cfg.get("allow_domains") or []) if d and str(d).strip()],
        "deny_domains": [d.lower().strip() for d in (cfg.get("deny_domains") or []) if d and str(d).strip()],
        "deny_keywords": _compile_regex_list(cfg.get("deny_keywords") or []),
        "deny_title_keywords": _compile_regex_list(cfg.get("deny_title_keywords") or []),
        "allow_url_regex": _compile_regex_list(cfg.get("allow_url_regex") or []),
        "deny_url_regex": _compile_regex_list(cfg.get("deny_url_regex") or []),
        "deny_url_substrings": [s.lower() for s in (cfg.get("deny_url_substrings") or []) if s],
        # Optional domain-specific deny additions
        "domain_deny_substrings": {
            (k or "").lower(): [s.lower() for s in (v or [])]
            for k, v in (cfg.get("domain_deny_substrings") or {}).items()
        },
    }


def _passes_filters(it: Item, flt: dict) -> Tuple[bool, str]:
    url = (it.url or "").strip()
    if not url:
        return False, "no_url"
    dom = normalise_domain(url)

    allow = flt.get("allow_domains") or []
    deny = flt.get("deny_domains") or []

    if deny and any(dom.endswith(d) for d in deny):
        return False, "deny_domain"
    if allow and not any(dom.endswith(d) for d in allow):
        return False, "domain_not_allowed"

    url_l = url.lower()
    for s in flt.get("deny_url_substrings") or []:
        if s and s in url_l:
            return False, "deny_url_substring"

    for rx in flt.get("deny_url_regex") or []:
        if rx.search(url):
            return False, "deny_url_regex"

    allow_rx = flt.get("allow_url_regex") or []
    if allow_rx:
        if not any(rx.search(url) for rx in allow_rx):
            return False, "allow_url_regex_no_match"

    title = (it.title or "").strip()
    text = (it.summary or "").strip()
    blob = f"{title}\n{text}"

    for rx in flt.get("deny_title_keywords") or []:
        if rx.search(title):
            return False, "deny_title_keyword"
    for rx in flt.get("deny_keywords") or []:
        if rx.search(blob):
            return False, "deny_keyword"

    return True, "ok"


# ---------------------------- Collection ----------------------------

def _is_priority(url: str) -> bool:
    dom = normalise_domain(url)
    return any(dom.endswith(d) for d in PRIORITY_DOMAINS)


def _collect_section_items(section_name: str, section_cfg: dict, drops: List[dict]) -> List[Item]:
    items: List[Item] = []
    sources = (section_cfg.get("sources") or {})
    rss = sources.get("rss") or []
    html = sources.get("html") or []

    for src in rss:
        try:
            items.extend(fetch_rss(src, source_name=section_name))
        except Exception as e:
            drops.append({"reason": "source_error", "section": section_name, "url": src, "error": str(e)})

    for src in html:
        try:
            items.extend(fetch_html_index(src, source_name=section_name))
        except Exception as e:
            drops.append({"reason": "source_error", "section": section_name, "url": src, "error": str(e)})

    return items


# ---------------------------- Scoring / selection ----------------------------

def _effective_published_ts(it: Item) -> Optional[float]:
    """
    Use only medium/high confidence publication timestamps.
    Low confidence dates (often derived from 'updated' or year-only coercions) are treated as undated (B).
    """
    if it.published_ts is None:
        return None
    conf = (getattr(it, "published_confidence", "") or "").lower()
    if conf in ("high", "medium"):
        return it.published_ts
    # Allow low confidence only when URL includes an explicit date (strong corroboration)
    if conf == "low" and _url_has_date(it.url):
        return it.published_ts
    return None


def _substance_ok(text: str, is_priority: bool) -> bool:
    n = len(text or "")
    if is_priority:
        return n >= PRIORITY_MIN_CHARS
    return n >= MIN_TEXT_CHARS


def _score_item(it: Item, text: str, domain_deny: Dict[str, List[str]]) -> float:
    dom = normalise_domain(it.url)
    prio = _is_priority(it.url)
    n = len(text or "")
    score = 0.0
    if prio:
        score += 1000.0
    # date confidence
    conf = (getattr(it, "published_confidence", "") or "").lower()
    score += {"high": 40.0, "medium": 20.0, "low": 5.0}.get(conf, 0.0)
    # text length (log-ish)
    score += min(200.0, (n ** 0.5) * 4.0)
    # URL looks like an article
    if _looks_articleish(it.url):
        score += 15.0
    # penalise category/hub-ish URLs (should already be filtered out)
    if _is_low_value_url(it.url, dom, domain_deny):
        score -= 200.0
    return score


def _select_from_pool(
    pool: List[Item],
    start: datetime,
    end: datetime,
    *,
    filters: dict,
    domain_deny: Dict[str, List[str]],
    strict: bool,
    per_domain_cap: int,
    items_per_section: int,
    drops: List[dict],
) -> List[Item]:
    chosen: List[Item] = []
    used_domains = defaultdict(int)

    # memoize full-text fetches across candidates
    text_cache: Dict[str, str] = {}

    scored: List[Tuple[float, Item]] = []

    for it in pool:
        ok, why = _passes_filters(it, filters)
        if not ok:
            drops.append({"reason": why, "url": it.url, "title": it.title, "section": it.source})
            continue

        dom = normalise_domain(it.url)

        # A: strong URL-level exclusions
        if _is_low_value_url(it.url, dom, domain_deny):
            drops.append({"reason": "low_value_url", "url": it.url, "title": it.title, "section": it.source})
            continue

        if strict and not _looks_articleish(it.url):
            drops.append({"reason": "not_articleish", "url": it.url, "title": it.title, "section": it.source})
            continue

        ts = _effective_published_ts(it) if strict else (it.published_ts or None)
        if not _in_range(ts, start, end):
            drops.append({"reason": "out_of_range", "url": it.url, "title": it.title, "section": it.source})
            continue

        # Fetch full text (best-effort)
        if it.url in text_cache:
            text = text_cache[it.url]
        else:
            try:
                text = fetch_full_text(it.url) or ""
            except Exception:
                text = ""
            if not text and it.summary:
                text = it.summary
            text_cache[it.url] = text

        prio = _is_priority(it.url)
        if strict and not _substance_ok(text, prio):
            drops.append({"reason": "low_substance", "url": it.url, "title": it.title, "section": it.source, "chars": len(text)})
            continue

        it.summary = text  # carry forward for summariser
        scored.append((_score_item(it, text, domain_deny), it))

    # Order by score desc, then date desc
    scored.sort(key=lambda x: (x[0], _effective_published_ts(x[1]) or 0.0), reverse=True)

    for _, it in scored:
        dom = normalise_domain(it.url)
        if used_domains[dom] >= per_domain_cap:
            drops.append({"reason": "domain_cap", "url": it.url, "title": it.title, "section": it.source})
            continue
        chosen.append(it)
        used_domains[dom] += 1
        if len(chosen) >= items_per_section:
            break

    return chosen


# ---------------------------- Main monthly run ----------------------------

def generate_for_month(ym: str, sources_cfg: dict, filters_cfg: dict) -> None:
    start, end = _month_bounds(ym)
    print(f"\n=== {ym} ({start.date()} -> {end.date()}) ===")

    flt = _compile_filters(filters_cfg)

    # Merge domain deny map with optional additions from filters.yaml
    domain_deny = {k: list(v) for k, v in DEFAULT_DOMAIN_DENY.items()}
    for dom, subs in (flt.get("domain_deny_substrings") or {}).items():
        domain_deny.setdefault(dom, [])
        domain_deny[dom].extend([s for s in subs if s and s not in domain_deny[dom]])

    drops: List[dict] = []
    selected: List[Item] = []

    for section, sec_cfg in (sources_cfg.get("sections") or {}).items():
        print(f"[section] {section}")
        pool = _collect_section_items(section, sec_cfg, drops)
        if DEBUG:
            (OUTDIR / f"debug-pool-{section.replace(' ', '_').replace('&','_')}-{ym}.json").write_text(
                json.dumps([asdict(x) for x in pool], indent=2), encoding="utf-8"
            )
        print(f"[pool] candidates: {len(pool)}")

        # Pass 1 (strict): best quality
        chosen = _select_from_pool(
            pool, start, end,
            filters=flt,
            domain_deny=domain_deny,
            strict=True,
            per_domain_cap=PER_DOMAIN_CAP,
            items_per_section=ITEMS_PER_SECTION,
            drops=drops,
        )

        # Pass 2 (relaxed): fill gaps if too few (robustness)
        if len(chosen) < min(ITEMS_PER_SECTION, 3):
            fill = _select_from_pool(
                pool, start, end,
                filters=flt,
                domain_deny=domain_deny,
                strict=False,
                per_domain_cap=PER_DOMAIN_CAP,
                items_per_section=ITEMS_PER_SECTION,
                drops=drops,
            )
            # merge, preserve order, unique by URL
            seen = set(x.url for x in chosen)
            for it in fill:
                if it.url not in seen:
                    chosen.append(it)
                    seen.add(it.url)
                if len(chosen) >= ITEMS_PER_SECTION:
                    break

        print(f"[selected] {len(chosen)} from {section}")
        for it in chosen:
            it.source = section
        selected.extend(chosen)

    # Global de-duplication (by URL), keep highest confidence first
    uniq: Dict[str, Item] = {}
    for it in selected:
        if it.url not in uniq:
            uniq[it.url] = it
            continue
        # prefer higher confidence
        rank = {"high": 3, "medium": 2, "low": 1, "none": 0}
        if rank.get(getattr(it, "published_confidence", "none"), 0) > rank.get(getattr(uniq[it.url], "published_confidence", "none"), 0):
            uniq[it.url] = it
    selected = list(uniq.values())

    # Sort by section then date desc
    def _sort_key(it: Item):
        return (it.source, _effective_published_ts(it) or 0.0)
    selected.sort(key=_sort_key, reverse=True)

    # Robustness: if empty, relax range slightly (±3 days) for near-boundary items
    if not selected:
        print("[warn] No selected items in strict/relaxed passes; applying ±3 day fallback window.")
        start2 = start - timedelta(days=3)
        end2 = end + timedelta(days=3)
        for section, sec_cfg in (sources_cfg.get("sections") or {}).items():
            pool = _collect_section_items(section, sec_cfg, drops)
            chosen = _select_from_pool(
                pool, start2, end2,
                filters=flt,
                domain_deny=domain_deny,
                strict=False,
                per_domain_cap=PER_DOMAIN_CAP,
                items_per_section=1,
                drops=drops,
            )
            for it in chosen:
                it.source = section
                selected.append(it)
            if len(selected) >= 2:
                break

        if not selected:
            print("[warn] Still no items after fallback; last-resort pick (ignoring dates) to prevent pipeline failure.")
            text_cache: Dict[str, str] = {}
            for section, sec_cfg in (sources_cfg.get("sections") or {}).items():
                pool = _collect_section_items(section, sec_cfg, drops)
                best: Optional[Item] = None
                best_score = -1e9
                for it in pool:
                    ok, why = _passes_filters(it, flt)
                    if not ok:
                        continue
                    dom = normalise_domain(it.url)
                    if _is_low_value_url(it.url, dom, domain_deny):
                        continue
                    if not _looks_articleish(it.url):
                        continue
                    if it.url in text_cache:
                        text = text_cache[it.url]
                    else:
                        try:
                            text = fetch_full_text(it.url) or ""
                        except Exception:
                            text = ""
                        if not text and it.summary:
                            text = it.summary
                        text_cache[it.url] = text
                    if not text:
                        continue
                    prio = _is_priority(it.url)
                    if not _substance_ok(text, prio):
                        continue
                    it.summary = text
                    sc = _score_item(it, text, domain_deny)
                    if sc > best_score:
                        best_score = sc
                        best = it
                if best:
                    best.source = section
                    selected.append(best)
                    break

    # Write debug outputs
    drops_path = OUTDIR / f"debug-drops-{ym}.txt"
    meta_path = OUTDIR / f"debug-meta-{ym}.txt"
    sel_path = OUTDIR / f"debug-selected-{ym}.json"

    if DEBUG:
        lines = ["reason\tsection\ttitle\turl\textra"]
        for d in drops:
            title = (d.get("title") or "").replace(chr(9), " ")
            section = d.get("section") or d.get("source") or ""
            extra = d.get("chars", "")
            lines.append(f"{d.get('reason','')}\t{section}\t{title}\t{d.get('url','')}\t{extra}")
        drops_path.write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "month": ym,
        "outdir": str(OUTDIR),
        "drops_file": str(drops_path),
        "selected_file": str(sel_path),
        "counts": {"selected_total": len(selected)},
    }
    meta_path.write_text(yaml.safe_dump(meta, sort_keys=False), encoding="utf-8")
    sel_path.write_text(json.dumps([asdict(x) for x in selected], indent=2), encoding="utf-8")

    # Write digest (always)
    digest_path = OUTDIR / f"monthly-digest-{ym}.md"
    digest_md = build_digest(ym, selected)
    digest_path.write_text(digest_md, encoding="utf-8")
    print(f"[write] {digest_path}")


def main() -> None:
    sources_cfg = _load_yaml(CFG_SOURCES)
    filters_cfg = _load_yaml(CFG_FILTERS)

    if MODE == "single-month":
        ym = START_YM or END_YM
        if not ym:
            raise SystemExit("MODE=single-month requires START_YM or END_YM")
        generate_for_month(ym, sources_cfg, filters_cfg)
        return

    # backfill-months
    if not START_YM or not END_YM:
        raise SystemExit("MODE=backfill-months requires START_YM and END_YM")
    for ym in _iter_months(START_YM, END_YM):
        generate_for_month(ym, sources_cfg, filters_cfg)


if __name__ == "__main__":
    main()
