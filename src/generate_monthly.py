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
from datetime import datetime, timezone
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


def _coerce_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(v, (int, float)):
        try:
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            dt = dtparser.parse(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _in_range(ts: Any, start: datetime, end: datetime) -> bool:
    dt = _coerce_dt(ts)
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
    p2 = (ROOT / path).resolve()
    if p2.is_file():
        return p2
    # Try legacy: if someone passed just "sources.yaml" but it's in config/
    p3 = (ROOT / "config" / path).resolve()
    if p3.is_file():
        return p3
    return p2  # return best guess for error message


def _read_yaml(path: str) -> Dict[str, Any]:
    p = _resolve_cfg_path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Missing YAML config: {path} (resolved to: {p})")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _load_config() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return _read_yaml(SOURCES_YAML), _read_yaml(FILTERS_YAML)


# ----------------------- URL NORMALISATION ---------------------

_UTM_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_name", "utm_reader",
    "fbclid", "gclid", "mc_cid", "mc_eid",
}

def _canonical_url(url: str) -> str:
    try:
        u = urllib.parse.urlsplit(url.strip())
    except Exception:
        return url.strip()

    # strip fragment
    fragmentless = urllib.parse.SplitResult(u.scheme, u.netloc, u.path, u.query, "")
    # strip common tracking params
    q = urllib.parse.parse_qsl(fragmentless.query, keep_blank_values=True)
    q2 = [(k, v) for (k, v) in q if k.lower() not in _UTM_KEYS]
    query = urllib.parse.urlencode(q2, doseq=True)

    # normalise netloc
    netloc = fragmentless.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    return urllib.parse.urlunsplit((fragmentless.scheme, netloc, fragmentless.path, query, ""))


# ----------------------- RELEVANCE SCORING ---------------------

def _compile_regexes(patterns: List[str]) -> List[re.Pattern]:
    out: List[re.Pattern] = []
    for p in patterns:
        try:
            out.append(re.compile(p, re.I))
        except re.error:
            # ignore bad regexes rather than breaking the run
            continue
    return out


def _text_substance_score(text: str) -> int:
    """
    Cheap heuristic to reward “real” policy/market content vs fluff.
    """
    t = (text or "").lower()
    score = 0
    # signals
    if "consultation" in t or "exposure draft" in t or "draft" in t:
        score += 2
    if "determination" in t or "decision" in t or "final" in t:
        score += 2
    if "rule" in t or "amendment" in t or "standard" in t:
        score += 2
    if "guidance" in t:
        score += 1
    if "funding" in t or "grant" in t or "investment" in t:
        score += 2
    if "auction" in t or "tender" in t or "cfds" in t or "contract for difference" in t:
        score += 2
    # numeric density often correlates with substance
    if sum(ch.isdigit() for ch in t) >= 8:
        score += 1
    return score


def _keyword_hits(text: str, keywords: List[str]) -> int:
    t = (text or "").lower()
    hits = 0
    for kw in keywords:
        k = (kw or "").strip().lower()
        if not k:
            continue
        if k in t:
            hits += 1
    return hits


def _age_penalty(published: Optional[datetime], month_start: datetime, month_end: datetime) -> float:
    """
    Prefer items in-range; softly penalize a bit outside range (when date parsing is imperfect).
    """
    if published is None:
        return 0.25 if ALLOW_UNDATED else 1.0
    if month_start <= published <= month_end:
        return 0.0
    # distance in days
    d = abs((published - month_end).total_seconds()) / 86400.0
    return min(2.0, 0.15 * math.log1p(d))


def _score_item(
    it: Item,
    *,
    section: str,
    month_start: datetime,
    month_end: datetime,
    allow_domains: set[str],
    deny_domains: set[str],
    title_deny: List[re.Pattern],
    keep_keywords: List[str],
) -> float:
    url = it.url or ""
    dom = normalise_domain(url)

    # domain allow/deny
    if allow_domains and dom not in allow_domains:
        return -9999.0
    if deny_domains and dom in deny_domains:
        return -9999.0

    title = normalize_whitespace(it.title or "")
    summ = normalize_whitespace(it.summary or "")
    blob = f"{title}\n{summ}".lower()

    # title deny patterns
    for rx in title_deny:
        if rx.search(title):
            return -9999.0

    score = 0.0

    # priority domain boost
    if dom in PRIORITY_DOMAINS:
        score += 2.0

    # keyword hits boost
    kh = _keyword_hits(blob, keep_keywords)
    score += min(3.0, 0.75 * kh)

    # section hint: encourage section label words (cheap but helps)
    sec_tokens = [w.lower() for w in re.findall(r"[A-Za-z]{3,}", section)]
    score += min(1.0, 0.2 * _keyword_hits(blob, sec_tokens))

    # substance boost
    score += 0.5 * _text_substance_score(blob)

    # prefer longer summaries a bit (but cap)
    score += min(1.25, len(summ) / 800.0)

    # recency/in-range
    pub = _coerce_dt(it.published_ts)
    score -= _age_penalty(pub, month_start, month_end)

    return score


# ----------------------- DIGEST BUILD ADAPTER ------------------

def _call_build_digest(*, items: List[Dict[str, Any]], year: int, month: int) -> str:
    """
    Your summarise.build_digest currently expects:
      build_digest(model, api_key, items, temp, date_label)

    But we keep this adapter robust if you later change the signature.
    """
    date_label = _month_label(year, month)

    sig = None
    try:
        sig = inspect.signature(build_digest)
    except Exception:
        sig = None

    # Try keyword route first if supported
    if sig is not None:
        params = set(sig.parameters.keys())
        kw: Dict[str, Any] = {}

        # Common names we’ve seen / might use later
        if "model" in params:
            kw["model"] = MODEL
        if "api_key" in params:
            kw["api_key"] = OPENAI_API_KEY
        if "items" in params:
            kw["items"] = items
        if "temp" in params:
            kw["temp"] = TEMP
        if "temperature" in params:
            kw["temperature"] = TEMP
        if "date_label" in params:
            kw["date_label"] = date_label
        if "ym" in params:
            kw["ym"] = f"{year:04d}-{month:02d}"

        # Only call with kwargs if it looks compatible
        if "items" in params:
            try:
                return build_digest(**kw)  # type: ignore[arg-type]
            except TypeError:
                pass  # fallback to positional

    # Positional fallback for the known-good signature
    return build_digest(MODEL, OPENAI_API_KEY, items, TEMP, date_label)  # type: ignore[misc]


# ----------------------- MAIN PIPELINE -------------------------

def _debug_write(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, (dict, list)):
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        path.write_text(str(data), encoding="utf-8")


def _item_to_dict(it: Item, section: str, full_text: str | None = None) -> Dict[str, Any]:
    return {
        "section": section,
        "url": it.url,
        "domain": normalise_domain(it.url),
        "title": normalize_whitespace(it.title or ""),
        "summary": normalize_whitespace(it.summary or ""),
        "published": (_coerce_dt(it.published_ts).isoformat() if _coerce_dt(it.published_ts) else None),
        "full_text": (full_text or ""),
        "id": sha1(_canonical_url(it.url)),
    }


def _passes_length_floor(dom: str, text: str) -> bool:
    n = len(text or "")
    if dom in PRIORITY_DOMAINS:
        return n >= PRIORITY_MIN_CHARS
    return n >= MIN_TEXT_CHARS


def _select_for_section(
    section: str,
    items: List[Item],
    *,
    month_start: datetime,
    month_end: datetime,
    filters_cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    allow_domains = {d.lower() for d in (filters_cfg.get("allow_domains") or [])}
    deny_domains = {d.lower() for d in (filters_cfg.get("deny_domains") or [])}
    title_deny = _compile_regexes(filters_cfg.get("title_deny_regex") or [])
    keep_keywords = filters_cfg.get("keep_keywords") or []

    scored: List[Tuple[float, Item]] = []
    for it in items:
        it.url = _canonical_url(it.url)
        s = _score_item(
            it,
            section=section,
            month_start=month_start,
            month_end=month_end,
            allow_domains=allow_domains,
            deny_domains=deny_domains,
            title_deny=title_deny,
            keep_keywords=keep_keywords,
        )
        if s > -1000:
            scored.append((s, it))

    scored.sort(key=lambda x: x[0], reverse=True)

    chosen: List[Dict[str, Any]] = []
    per_domain: Dict[str, int] = {}
    drops: List[str] = []

    # Controlled relaxation: if not enough selected, we do a second pass later.
    for s, it in scored:
        if len(chosen) >= ITEMS_PER_SECTION:
            break

        dom = normalise_domain(it.url)
        per_domain.setdefault(dom, 0)
        if per_domain[dom] >= max(1, PER_DOMAIN_CAP):
            drops.append(f"cap_domain\t{dom}\t{it.url}")
            continue

        # Fetch full text (main cost) only when item is a plausible candidate
        try:
            full_text = fetch_full_text(it.url) or ""
        except Exception as e:
            drops.append(f"fetch_error\t{dom}\t{it.url}\t{type(e).__name__}:{e}")
            continue

        if not _in_range(it.published_ts, month_start, month_end):
            drops.append(f"out_of_range\t{dom}\t{it.url}")
            continue

        if not _passes_length_floor(dom, full_text):
            drops.append(f"too_short\t{dom}\t{it.url}\tlen={len(full_text)}")
            continue

        chosen.append(_item_to_dict(it, section, full_text=full_text))
        per_domain[dom] += 1

    meta = {
        "section": section,
        "candidates": len(items),
        "scored": len(scored),
        "selected": len(chosen),
        "drops": len(drops),
    }
    return chosen, {"meta": meta, "drops": drops}


def _fetch_section_pool(section_cfg: Dict[str, Any]) -> List[Item]:
    pool: List[Item] = []
    for url in section_cfg.get("rss", []) or []:
        pool.extend(fetch_rss(url))
    for url in section_cfg.get("html", []) or []:
        pool.extend(fetch_html_index(url))
    return pool


def generate_for_month(ym: str) -> None:
    y, m = [int(x) for x in ym.split("-")]
    start, end = _month_bounds(y, m)
    sources_cfg, filters_cfg = _load_config()

    sections = (sources_cfg.get("sections") or {})
    selected_all: List[Dict[str, Any]] = []
    debug_meta_lines: List[str] = []
    debug_drops_lines: List[str] = []

    # First pass (strict)
    for section, cfg in sections.items():
        pool = _fetch_section_pool(cfg)
        print(f"[pool] {section}: candidates: {len(pool)}")

        chosen, dbg = _select_for_section(
            section,
            pool,
            month_start=start,
            month_end=end,
            filters_cfg=filters_cfg,
        )
        selected_all.extend(chosen)

        debug_meta_lines.append(f"{section}\tcandidates={dbg['meta']['candidates']}\tselected={dbg['meta']['selected']}\tdrops={dbg['meta']['drops']}")
        debug_drops_lines.extend([f"{section}\t{line}" for line in dbg["drops"]])

        if DEBUG:
            _debug_write(OUTDIR / f"debug-pool-{section.replace(' ', '_')}-{ym}.json", [ _item_to_dict(it, section) for it in pool ])

        print(f"[selected] {len(chosen)} from {section}")

    # If too few items overall, relax in a controlled way:
    #  1) allow undated temporarily
    #  2) lower length floor slightly
    # But do NOT remove title/domain filters.
    if len(selected_all) < MIN_TOTAL_ITEMS:
        print(f"[warn] Only {len(selected_all)} items selected; attempting controlled relaxation")

        global ALLOW_UNDATED
        prev_allow_undated = ALLOW_UNDATED
        ALLOW_UNDATED = True

        prev_min = MIN_TEXT_CHARS
        prev_prio_min = PRIORITY_MIN_CHARS
        # small reduction only
        relaxed_min = max(150, int(prev_min * 0.75))
        relaxed_prio = max(120, int(prev_prio_min * 0.8))

        try:
            # Temporarily override floors
            globals()["MIN_TEXT_CHARS"] = relaxed_min
            globals()["PRIORITY_MIN_CHARS"] = relaxed_prio

            selected_all_2: List[Dict[str, Any]] = []
            debug_meta_lines.append(f"RELAX\tALLOW_UNDATED=1\tMIN_TEXT_CHARS={relaxed_min}\tPRIORITY_MIN_CHARS={relaxed_prio}")

            for section, cfg in sections.items():
                pool = _fetch_section_pool(cfg)
                chosen, _dbg = _select_for_section(
                    section,
                    pool,
                    month_start=start,
                    month_end=end,
                    filters_cfg=filters_cfg,
                )
                selected_all_2.extend(chosen)

            # Merge dedup by canonical URL id
            seen = set()
            merged: List[Dict[str, Any]] = []
            for d in (selected_all + selected_all_2):
                if d["id"] in seen:
                    continue
                seen.add(d["id"])
                merged.append(d)

            selected_all = merged
        finally:
            ALLOW_UNDATED = prev_allow_undated
            globals()["MIN_TEXT_CHARS"] = prev_min
            globals()["PRIORITY_MIN_CHARS"] = prev_prio_min

    # Persist debug outputs
    _debug_write(OUTDIR / f"debug-selected-{ym}.json", selected_all)
    _debug_write(OUTDIR / f"debug-meta-{ym}.txt", "\n".join(debug_meta_lines) + "\n")
    _debug_write(OUTDIR / f"debug-drops-{ym}.txt", "\n".join(debug_drops_lines) + "\n")

    if len(selected_all) < MIN_TOTAL_ITEMS:
        raise SystemExit(f"ERROR: selected items is {len(selected_all)} but MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS}")

    # Build digest markdown (FIXED: robust adapter)
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

    # Default: single month from YM env
    ym = os.getenv("YM")
    if not ym:
        raise SystemExit("Set YM=YYYY-MM or MODE=backfill-months with START_YM/END_YM")
    generate_for_month(ym)


if __name__ == "__main__":
    main()
