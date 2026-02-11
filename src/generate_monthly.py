# -*- coding: utf-8 -*-
"""
Monthly digest generator.

Key robustness properties:
- Compatible with older/newer fetch/summarise signatures (via **kwargs shims).
- Never ends with 0 selected items unless ALLOW_PLACEHOLDER=0 AND everything is down.
- Emits debug-selected/meta/drops on every run.
- Preserves publisher (Item.source) and logical digest section (Item.section).
"""
from __future__ import annotations

import json
import math
import os
import re
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml
from dateutil import parser as dtparser

from .fetch import Item, fetch_full_text, fetch_html_index, fetch_rss, is_probably_taxonomy_or_hub
from .summarise import build_digest
from .utils import normalise_domain


OUT_DIR = Path(os.getenv("OUT_DIR", "out"))
CFG_SOURCES = Path(os.getenv("CFG_SOURCES", "config/sources.yaml"))
CFG_FILTERS = Path(os.getenv("CFG_FILTERS", "config/filters.yaml"))

ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "5"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "2"))

MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "900"))
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "250"))
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "1"))

ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "1") == "1"
ALLOW_PLACEHOLDER = os.getenv("ALLOW_PLACEHOLDER", "1") == "1"
FALLBACK_WINDOW_DAYS = int(os.getenv("FALLBACK_WINDOW_DAYS", "3"))
DEBUG = os.getenv("DEBUG", "0") == "1"

PRIORITY_DOMAINS = {
    d.strip().lower()
    for d in os.getenv("PRIORITY_DOMAINS", "").split(",")
    if d.strip()
}

EMERGENCY_RSS = {
    "Energy Transition": "https://news.google.com/rss/search?q=Australia%20energy%20transition&hl=en-AU&gl=AU&ceid=AU:en",
    "ESG Reporting": "https://news.google.com/rss/search?q=ISSB%20ESG%20reporting&hl=en&gl=US&ceid=US:en",
    "Sustainable Finance & Investment": "https://news.google.com/rss/search?q=sustainable%20finance%20green%20bond&hl=en&gl=US&ceid=US:en",
}


def _slug(s: str) -> str:
    s2 = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")
    s2 = re.sub(r"_+", "_", s2)
    return s2 or "section"


def _parse_ym(ym: str) -> Tuple[int, int]:
    m = re.match(r"^(\d{4})-(\d{2})$", ym.strip())
    if not m:
        raise ValueError(f"Invalid YM '{ym}'. Expected YYYY-MM.")
    y = int(m.group(1))
    mo = int(m.group(2))
    if not (1 <= mo <= 12):
        raise ValueError(f"Invalid month in YM '{ym}'.")
    return y, mo


def _month_range(ym: str) -> Tuple[date, date]:
    y, mo = _parse_ym(ym)
    start = date(y, mo, 1)
    if mo == 12:
        end = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(y, mo + 1, 1) - timedelta(days=1)
    return start, end


def _coerce_ts(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    if isinstance(ts, date):
        return datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(ts, str):
        s = ts.strip()
        if not s:
            return None
        try:
            dt = dtparser.isoparse(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _effective_published_ts(it: Item) -> Optional[datetime]:
    return _coerce_ts(getattr(it, "published_ts", None))


def _in_range(ts: Any, start: Any, end: Any) -> bool:
    ts2 = _coerce_ts(ts)
    if ts2 is None:
        return ALLOW_UNDATED
    s2 = _coerce_ts(start)
    e2 = _coerce_ts(end)
    if s2 is None or e2 is None:
        return True
    return s2 <= ts2 <= e2


def _is_priority(url: str) -> bool:
    d = normalise_domain(url)
    return (d in PRIORITY_DOMAINS) if PRIORITY_DOMAINS else False


def _substance_ok(text: str, is_priority: bool) -> bool:
    if not text:
        return False
    min_chars = PRIORITY_MIN_CHARS if is_priority else MIN_TEXT_CHARS
    if len(text) < min_chars:
        return False
    letters = sum(c.isalpha() for c in text)
    if letters < min(150, len(text) * 0.08):
        return False
    return True


def _looks_articleish(url: str) -> bool:
    u = (url or "").lower()
    bad = [
        "/tag/", "/tags/", "/category/", "/categories/",
        "/topic/", "/topics/", "/author/", "/authors/",
        "/search", "?s=", "/page/", "/index",
        "/events", "/event", "/webinars", "/webinar",
        "/jobs", "/careers", "/about", "/contact",
    ]
    return not any(b in u for b in bad)


import fnmatch

class Filters:
    def __init__(self, raw: Dict[str, Any]):
        raw = raw or {}

        # --- key aliases (backwards compatible with your filters.yaml) ---
        if "deny_title_regex" not in raw and "title_deny_regex" in raw:
            raw["deny_title_regex"] = raw.get("title_deny_regex")
        if "deny_url_substrings" not in raw and "domain_deny_substrings" not in raw:
            raw["deny_url_substrings"] = raw.get("deny_url_substrings", [])

        self.allow_domains = [str(d).lower().strip() for d in raw.get("allow_domains", []) if str(d).strip()]
        self.deny_domains = [str(d).lower().strip() for d in raw.get("deny_domains", []) if str(d).strip()]
        self.deny_url_substrings = [str(s).lower() for s in raw.get("deny_url_substrings", []) if str(s).strip()]
        self.deny_title_regex = [re.compile(r, re.I) for r in raw.get("deny_title_regex", []) if str(r).strip()]

        # domain-scoped URL deny substrings (your YAML uses this)
        dds = raw.get("domain_deny_substrings", {}) or {}
        self.domain_deny_substrings = {
            str(dom).lower().strip(): [str(s).lower() for s in (subs or []) if str(s).strip()]
            for dom, subs in dds.items()
            if isinstance(subs, list)
        }

        # global keyword noise gate (your YAML uses keep_keywords)
        self.keep_keywords = [str(k).lower() for k in (raw.get("keep_keywords", []) or []) if str(k).strip()]

        self.section_keywords = {
            k: [str(w).lower() for w in v] for k, v in (raw.get("section_keywords", {}) or {}).items()
            if isinstance(v, list)
        }

    def domain_allowed(self, domain: str) -> bool:
        d = (domain or "").lower()
        if not self.allow_domains:
            return True

        for pat in self.allow_domains:
            p = (pat or "").lower()
            if not p:
                continue

            # "*.gov.au" => suffix match "gov.au"
            if p.startswith("*."):
                suf = p[2:]
                if d == suf or d.endswith("." + suf):
                    return True
                continue

            # allow simple globs too
            if ("*" in p) or ("?" in p):
                if fnmatch.fnmatch(d, p):
                    return True
                continue

            if d == p or d.endswith("." + p):
                return True

        return False

    def domain_denied(self, domain: str) -> bool:
        d = (domain or "").lower()
        return any(d == x or d.endswith("." + x) for x in self.deny_domains)

    def url_denied(self, url: str) -> bool:
        u = (url or "").lower()
        if any(ss and ss in u for ss in self.deny_url_substrings):
            return True
        dom = normalise_domain(url)
        for ss in self.domain_deny_substrings.get(dom, []):
            if ss and ss in u:
                return True
        return False

    def keep_keyword_hit(self, title: str, url: str) -> bool:
        if not self.keep_keywords:
            return True
        hay = f"{title or ''} {url or ''}".lower()
        return any(k in hay for k in self.keep_keywords if k)



def _passes_filters(it: Item, flt: Filters, section: str, *, bypass_allow: bool = False) -> Tuple[bool, str]:
    url = (it.url or "").strip()
    title = (it.title or "").strip()
    if not url or not title:
        return False, "missing_url_or_title"

    if is_probably_taxonomy_or_hub(url):
        return False, "hub_url"
    if flt.url_denied(url):
        return False, "deny_url_substring"

    # Noise gate (especially important when dates are missing)
    if not flt.keep_keyword_hit(title, url):
        return False, "no_keep_keyword"

    domain = normalise_domain(url)
    if flt.domain_denied(domain):
        return False, "deny_domain"
    if (not bypass_allow) and (not flt.domain_allowed(domain)):
        return False, "not_in_allowlist"

    u = url.lower()
    for ss in flt.deny_url_substrings:
        if ss and ss in u:
            return False, "deny_url_substring"

    for rx in flt.deny_title_regex:
        if rx.search(title):
            return False, "deny_title_regex"

    return True, ""


def _keyword_boost(title: str, section: str, flt: Filters) -> float:
    kws = flt.section_keywords.get(section, [])
    if not kws:
        return 0.0
    t = title.lower()
    hits = sum(1 for k in kws if k and k in t)
    return min(1.0, hits * 0.15)


def _score_item(it: Item, text: str, section: str, flt: Filters, *, ignore_substance: bool = False) -> float:
    domain = normalise_domain(it.url)
    if flt.domain_denied(domain):
        return -1e9
    if (not ignore_substance) and (not _substance_ok(text, _is_priority(it.url))):
        return -1e9

    dt = _effective_published_ts(it)
    recency = (dt.timestamp() / 1e10) if dt is not None else 0.0
    substance = math.log(max(50, len(text)), 10)
    kw = _keyword_boost(it.title or "", section, flt)
    articleish = 0.2 if _looks_articleish(it.url or "") else -0.2
    return recency + substance + kw + articleish


def _collect_section_pool(section: str, sec_cfg: Dict[str, Any]) -> Tuple[List[Item], List[Dict[str, str]]]:
    drops: List[Dict[str, str]] = []
    pool: List[Item] = []

    def add_items(items: Sequence[Item]):
        for it in items:
            it.section = section
            pool.append(it)

    for entry in (sec_cfg.get("rss") or []):
        try:
            url = entry.get("url") if isinstance(entry, dict) else str(entry)
            name = entry.get("name") if isinstance(entry, dict) else ""
            add_items(fetch_rss(str(url), source_name=str(name or normalise_domain(str(url)))))
        except Exception as e:
            drops.append({"reason": "rss_error", "source": str(entry), "detail": str(e)})

    for entry in (sec_cfg.get("html") or []):
        try:
            url = entry.get("url") if isinstance(entry, dict) else str(entry)
            name = entry.get("name") if isinstance(entry, dict) else ""
            add_items(fetch_html_index(str(url), source_name=str(name or normalise_domain(str(url)))))
        except Exception as e:
            drops.append({"reason": "html_index_error", "source": str(entry), "detail": str(e)})

    seen = set()
    deduped: List[Item] = []
    for it in pool:
        key = (it.url or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    return deduped, drops


def _select_from_pool(
    pool: Sequence[Item],
    section: str,
    start_dt: datetime,
    end_dt: datetime,
    flt: Filters,
    *,
    items_per_section: int,
    per_domain_cap: int,
    strict: bool,
    bypass_allow: bool = False,
) -> Tuple[List[Item], List[Dict[str, str]]]:
    drops: List[Dict[str, str]] = []
    selected: List[Item] = []
    per_domain: Dict[str, int] = {}
    text_cache: Dict[str, str] = {}

    def sort_key(it: Item):
        dt = _effective_published_ts(it)
        ts = dt.timestamp() if dt else 0.0
        return (-ts, (it.url or ""))

    for it in sorted(pool, key=sort_key):
        ok, why = _passes_filters(it, flt, section, bypass_allow=bypass_allow)
        if not ok:
            drops.append({"reason": why, "url": it.url or "", "title": it.title or ""})
            continue

        if not _in_range(_effective_published_ts(it), start_dt, end_dt):
            drops.append({"reason": "out_of_range", "url": it.url or "", "title": it.title or ""})
            continue

        domain = normalise_domain(it.url)
        if per_domain.get(domain, 0) >= per_domain_cap:
            drops.append({"reason": "per_domain_cap", "url": it.url or "", "title": it.title or "", "domain": domain})
            continue

        if strict and (not _looks_articleish(it.url or "")):
            drops.append({"reason": "not_articleish", "url": it.url or "", "title": it.title or ""})
            continue

        text = text_cache.get(it.url or "")
        if text is None:
            text = ""
        if not text:
            try:
                text = (fetch_full_text(it.url or "") or "").strip()
            except Exception:
                text = ""
            if not text:
                text = (it.summary or "").strip()
            text_cache[it.url or ""] = text

        prio = _is_priority(it.url or "")
        if strict and (not _substance_ok(text, prio)):
            drops.append({"reason": "low_substance", "url": it.url or "", "title": it.title or ""})
            continue

        if text:
            it.summary = text

        selected.append(it)
        per_domain[domain] = per_domain.get(domain, 0) + 1
        if len(selected) >= items_per_section:
            break

    return selected, drops


def _last_resort_pick(pool: Sequence[Item], section: str, flt: Filters, *, items_needed: int) -> Tuple[List[Item], List[Dict[str, str]]]:
    drops: List[Dict[str, str]] = []
    scored: List[Tuple[float, Item]] = []

    for it in pool:
        ok, why = _passes_filters(it, flt, section, bypass_allow=True)
        if not ok:
            drops.append({"reason": why, "url": it.url or "", "title": it.title or ""})
            continue

        text = ""
        try:
            text = (fetch_full_text(it.url or "") or "").strip()
        except Exception:
            text = ""
        if not text:
            text = (it.summary or "").strip()
        if not text:
            text = (it.title or "").strip()
        it.summary = text

        sc = _score_item(it, text, section, flt, ignore_substance=True)
        scored.append((sc, it))

    scored.sort(key=lambda x: (-x[0], (x[1].url or "")))
    picked: List[Item] = [it for _, it in scored[: max(1, items_needed)]]
    return picked, drops


def _emergency_pool(section: str) -> List[Item]:
    rss = EMERGENCY_RSS.get(section)
    if not rss:
        return []
    try:
        items = fetch_rss(rss, source_name="Google News")
        for it in items:
            it.section = section
        return items
    except Exception:
        return []


def generate_for_month(ym: str, cfg_sources: Dict[str, Any], flt: Filters) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    start_d, end_d = _month_range(ym)
    start_dt = datetime(start_d.year, start_d.month, start_d.day, tzinfo=timezone.utc)
    end_dt = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc)

    print(f"\n=== {ym} ({start_d} -> {end_d}) ===")

    all_selected: List[Item] = []
    all_drops: List[Dict[str, str]] = []

    sections: Dict[str, Any] = cfg_sources.get("sections") or {}
    for section, sec_cfg in sections.items():
        print(f" {section}")
        pool, drops0 = _collect_section_pool(section, sec_cfg or {})
        all_drops.extend(drops0)

        if DEBUG:
            pool_path = OUT_DIR / f"debug-pool-{_slug(section)}-{ym}.json"
            pool_path.write_text(json.dumps([asdict(it) for it in pool], ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"[pool] candidates: {len(pool)}")

        selected, drops1 = _select_from_pool(
            pool, section, start_dt, end_dt, flt,
            items_per_section=ITEMS_PER_SECTION,
            per_domain_cap=PER_DOMAIN_CAP,
            strict=True,
        )
        all_drops.extend(drops1)
        print(f"[selected] {len(selected)} from {section}")

        if not selected:
            selected2, drops2 = _select_from_pool(
                pool, section, start_dt, end_dt, flt,
                items_per_section=ITEMS_PER_SECTION,
                per_domain_cap=PER_DOMAIN_CAP,
                strict=False,
            )
            all_drops.extend(drops2)
            selected = selected2

        if not selected:
            print(f"[warn] No selected items in strict/relaxed passes; applying ±{FALLBACK_WINDOW_DAYS} day window.")
            s2 = start_dt - timedelta(days=FALLBACK_WINDOW_DAYS)
            e2 = end_dt + timedelta(days=FALLBACK_WINDOW_DAYS)
            selected3, drops3 = _select_from_pool(
                pool, section, s2, e2, flt,
                items_per_section=ITEMS_PER_SECTION,
                per_domain_cap=PER_DOMAIN_CAP,
                strict=False,
            )
            all_drops.extend(drops3)
            selected = selected3

        if not selected:
            print("[warn] Still no items after fallback; last-resort pick (ignoring dates/substance).")
            picked, drops4 = _last_resort_pick(pool, section, flt, items_needed=ITEMS_PER_SECTION)
            all_drops.extend(drops4)
            selected = picked

        if not selected:
            print("[warn] No candidates available; trying emergency RSS.")
            epool = _emergency_pool(section)
            picked, drops5 = _last_resort_pick(epool, section, flt, items_needed=1)
            all_drops.extend(drops5)
            selected = picked

        if not selected and ALLOW_PLACEHOLDER:
            placeholder = Item(
                url="",
                title=f"{section}: no retrievable items for {ym}",
                summary=f"No items could be retrieved for {section} in {ym}. Fallback placeholder.",
                source="pipeline",
                section=section,
            )
            selected = [placeholder]
            all_drops.append({"reason": "placeholder_used", "url": "", "title": placeholder.title})

        all_selected.extend(selected)

    if not all_selected and ALLOW_PLACEHOLDER:
        placeholder = Item(
            url="",
            title=f"Monthly digest {ym}: no retrievable items",
            summary="No items could be retrieved from any configured source. Fallback placeholder.",
            source="pipeline",
            section="General",
        )
        all_selected = [placeholder]
        all_drops.append({"reason": "global_placeholder_used", "url": "", "title": placeholder.title})

    # Ensure we meet MIN_TOTAL_ITEMS to avoid downstream guardrails failing
    if len(all_selected) < MIN_TOTAL_ITEMS and ALLOW_PLACEHOLDER:
        for k in range(MIN_TOTAL_ITEMS - len(all_selected)):
            ph = Item(
                url="",
                title=f"Monthly digest {ym}: additional fallback item {k+1}",
                summary="Fallback placeholder added to satisfy MIN_TOTAL_ITEMS.",
                source="pipeline",
                section="General",
            )
            all_selected.append(ph)
            all_drops.append({"reason": "min_total_placeholder_used", "url": "", "title": ph.title})

    sel_path = OUT_DIR / f"debug-selected-{ym}.json"
    sel_path.write_text(
        json.dumps(
            [
                {
                    "section": getattr(it, "section", "") or "",
                    "title": it.title,
                    "url": it.url,
                    "publisher": it.source,
                    "published": getattr(it, "published_iso", None) or None,
                    "published_ts": getattr(it, "published_ts", None),
                }
                for it in all_selected
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    meta_path = OUT_DIR / f"debug-meta-{ym}.txt"
    meta_path.write_text(
        "\n".join(
            [
                f"ym={ym}",
                f"selected_total={len(all_selected)}",
                f"items_per_section={ITEMS_PER_SECTION}",
                f"per_domain_cap={PER_DOMAIN_CAP}",
                f"allow_undated={ALLOW_UNDATED}",
                f"allow_placeholder={ALLOW_PLACEHOLDER}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    drops_path = OUT_DIR / f"debug-drops-{ym}.txt"
    with drops_path.open("w", encoding="utf-8") as f:
        f.write("# reason\turl\ttitle\n")
        for d in all_drops:
            f.write(f"{d.get('reason','')}\t{d.get('url','')}\t{d.get('title','')}\n")

    md = build_digest(ym, all_selected)
    out_path = OUT_DIR / f"monthly-digest-{ym}.md"
    out_path.write_text(md, encoding="utf-8")
    print(f"[write] {out_path.resolve()}")


def _iter_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = _parse_ym(start_ym)
    ey, em = _parse_ym(end_ym)
    cur_y, cur_m = sy, sm
    out = []
    while (cur_y, cur_m) <= (ey, em):
        out.append(f"{cur_y:04d}-{cur_m:02d}")
        if cur_m == 12:
            cur_y += 1
            cur_m = 1
        else:
            cur_m += 1
    return out


def main() -> None:
    cfg_sources = yaml.safe_load(CFG_SOURCES.read_text(encoding="utf-8"))
    flt_raw = yaml.safe_load(CFG_FILTERS.read_text(encoding="utf-8"))
    flt = Filters(flt_raw or {})

    mode = os.getenv("MODE", "backfill-months").strip()
    ym = os.getenv("YM", "").strip()

    if mode == "single-month":
        if not ym:
            raise SystemExit("MODE=single-month but YM not set.")
        generate_for_month(ym, cfg_sources, flt)
        return

    start_ym = os.getenv("START_YM", "").strip() or ym
    end_ym = os.getenv("END_YM", "").strip() or ym
    if not start_ym or not end_ym:
        raise SystemExit("MODE=backfill-months but START_YM/END_YM not set (or YM missing).")

    for m in _iter_months(start_ym, end_ym):
        generate_for_month(m, cfg_sources, flt)


if __name__ == "__main__":
    main()
