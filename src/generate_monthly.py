# === BEGIN src/generate_monthly.py ===
from __future__ import annotations

import inspect as _inspect
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import feedparser
import yaml
from dateutil import parser as dateparser

from .fetch import fetch_url
from .summarise import build_digest


# ----------------------------
# Config / env
# ----------------------------

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "out"
OUTDIR.mkdir(parents=True, exist_ok=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o-mini").strip()
TEMP = float(os.getenv("TEMP", "0.2"))

MODE = os.getenv("MODE", "monthly").strip()
START_YM = os.getenv("START_YM", "").strip()
END_YM = os.getenv("END_YM", "").strip()

ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "7"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))
MAX_PER_DOMAIN_PER_SECTION = int(os.getenv("MAX_PER_DOMAIN_PER_SECTION", str(PER_DOMAIN_CAP)))

MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "300"))
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "200"))
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "1"))

MIN_SUBSTANCE_SCORE = int(os.getenv("MIN_SUBSTANCE_SCORE", "2"))
ALLOW_UNDATED = int(os.getenv("ALLOW_UNDATED", "0"))

DEBUG = int(os.getenv("DEBUG", "0"))

PRIORITY_DOMAINS = [d.strip() for d in os.getenv("PRIORITY_DOMAINS", "").split(",") if d.strip()]

MAX_LINKS_PER_INDEX = int(os.getenv("MAX_LINKS_PER_INDEX", "220"))
MAX_INDEX_PAGES = int(os.getenv("MAX_INDEX_PAGES", "4"))
MAX_DATE_RESOLVE_FETCHES_PER_INDEX = int(os.getenv("MAX_DATE_RESOLVE_FETCHES_PER_INDEX", "55"))
MAX_FULLTEXT_FETCHES_PER_SECTION = int(os.getenv("MAX_FULLTEXT_FETCHES_PER_SECTION", "55"))
MAX_FULLTEXT_FETCHES_TOTAL = int(os.getenv("MAX_FULLTEXT_FETCHES_TOTAL", "150"))


# ----------------------------
# Compatibility wrapper (fixes build_digest signature drift)
# ----------------------------

def _call_build_digest(selected_items: List[Dict[str, Any]], ym: str, *, model: str, temperature: float, api_key: str) -> str:
    """
    Call build_digest with a signature-compatible keyword mapping.

    Ensures we NEVER pass duplicate values (e.g. model twice).
    """
    sig = _inspect.signature(build_digest)
    params = sig.parameters
    kwargs: Dict[str, Any] = {}

    # items argument name
    preferred_item_keys = ["items", "selected", "entries", "articles", "records", "chosen"]
    item_key = next((k for k in preferred_item_keys if k in params), None)
    if item_key is None:
        # fallback: first positional param name
        item_key = next(iter(params.keys()), "items")
    kwargs[item_key] = selected_items

    # month argument
    if "ym" in params:
        kwargs["ym"] = ym
    elif "month" in params:
        kwargs["month"] = ym

    # model
    if "model" in params:
        kwargs["model"] = model

    # temperature
    if "temperature" in params:
        kwargs["temperature"] = temperature
    elif "temp" in params:
        kwargs["temp"] = temperature

    # api key
    if "api_key" in params:
        kwargs["api_key"] = api_key
    elif "openai_api_key" in params:
        kwargs["openai_api_key"] = api_key

    # If build_digest doesn't accept **kwargs, strip unknown keys
    if not any(p.kind == p.VAR_KEYWORD for p in params.values()):
        allowed = set(params.keys())
        kwargs = {k: v for k, v in kwargs.items() if k in allowed}

    # Call ONLY with kwargs (prevents "multiple values for argument X")
    return build_digest(**kwargs)


# ----------------------------
# Data structures
# ----------------------------

@dataclass
class Item:
    section: str
    source: str
    title: str
    url: str
    domain: str
    ts: Optional[float]
    text: str
    substance_score: int = 0


# ----------------------------
# Helpers
# ----------------------------

def time_now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ym_to_range(ym: str) -> Tuple[datetime, datetime]:
    year, month = [int(x) for x in ym.split("-")]
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def _domain(url: str) -> str:
    m = re.match(r"^https?://([^/]+)/?", url.strip())
    return (m.group(1).lower() if m else "").replace("www.", "")


def _coerce_ts(ts: Any) -> Optional[float]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.timestamp()
    if isinstance(ts, str):
        s = ts.strip()
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return None
    return None


def _in_range(ts: Any, start_dt: datetime, end_dt: datetime) -> bool:
    t = _coerce_ts(ts)
    if t is None:
        return bool(ALLOW_UNDATED)
    return start_dt.timestamp() <= t < end_dt.timestamp()


def _parse_source(src: Any) -> Tuple[str, str, str]:
    """
    src may be:
      - string URL
      - dict {name, url}
      - dict {url}
      - dict {name, feed} or {feed}
    Returns: (name, url, kind) where kind is "feed" or "page"
    """
    if isinstance(src, str):
        return (src, src, _guess_kind(src, src))
    if isinstance(src, dict):
        url = (src.get("url") or src.get("link") or "").strip()
        feed = (src.get("feed") or src.get("rss") or "").strip()
        chosen = feed or url
        name = (src.get("name") or chosen or "source").strip()
        kind = "feed" if feed else _guess_kind(chosen, name)
        return (name, chosen, kind)
    return ("source", str(src), "page")


def _guess_kind(url: str, name: str) -> str:
    u = (url or "").lower()
    n = (name or "").lower()
    if any(k in u for k in ["/feed", "rss", "atom"]) or u.endswith(".xml"):
        return "feed"
    if any(k in n for k in ["rss", "atom", "feed"]):
        return "feed"
    return "page"


def _load_yaml_sources() -> Dict[str, Any]:
    candidates = [
        ROOT / "config" / "monthly.yml",
        ROOT / "config" / "monthly.yaml",
        ROOT / "monthly.yml",
        ROOT / "monthly.yaml",
    ]
    for p in candidates:
        if p.exists():
            data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            return data
    raise SystemExit("ERROR: could not find monthly sources YAML (tried config/monthly.yml and monthly.yml)")


def write_debug(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, (dict, list)):
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        path.write_text(str(data), encoding="utf-8")
    if DEBUG:
        print(f"[write] {path}")


# ----------------------------
# Date inference (critical for ALLOW_UNDATED=0)
# ----------------------------

_URL_YMD = re.compile(r"(?P<y>20\d{2})[/-](?P<m>0?[1-9]|1[0-2])[/-](?P<d>0?[1-9]|[12]\d|3[01])")
_URL_YM = re.compile(r"(?P<y>20\d{2})[/-](?P<m>0?[1-9]|1[0-2])")
_URL_YMD_DASH = re.compile(r"(?P<y>20\d{2})-(?P<m>0[1-9]|1[0-2])-(?P<d>0[1-9]|[12]\d|3[01])")

def infer_ts_from_url(url: str) -> Optional[float]:
    u = url or ""
    m = _URL_YMD_DASH.search(u) or _URL_YMD.search(u)
    if m:
        y = int(m.group("y"))
        mo = int(m.group("m"))
        d = int(m.group("d"))
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc).timestamp()
        except Exception:
            return None
    m2 = _URL_YM.search(u)
    if m2:
        y = int(m2.group("y"))
        mo = int(m2.group("m"))
        try:
            return datetime(y, mo, 1, tzinfo=timezone.utc).timestamp()
        except Exception:
            return None
    return None


_TEXT_DATE_HINT = re.compile(
    r"\b(20\d{2}[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01]))\b"
)

def infer_ts_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    head = text[:1200]
    m = _TEXT_DATE_HINT.search(head)
    if m:
        s = m.group(1)
        try:
            dt = dateparser.parse(s)
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
        except Exception:
            return None
    return None


def ts_from_feed_entry(entry: Any) -> Optional[float]:
    # feedparser gives published_parsed / updated_parsed (time.struct_time)
    for key in ("published_parsed", "updated_parsed"):
        st = getattr(entry, key, None) or (entry.get(key) if isinstance(entry, dict) else None)
        if st:
            try:
                return datetime(*st[:6], tzinfo=timezone.utc).timestamp()
            except Exception:
                pass
    # sometimes published/updated is a string
    for key in ("published", "updated"):
        s = getattr(entry, key, None) or (entry.get(key) if isinstance(entry, dict) else None)
        if s:
            try:
                dt = dateparser.parse(str(s))
                if dt:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.timestamp()
            except Exception:
                pass
    return None


# ----------------------------
# Collection
# ----------------------------

def collect_items(section: str, sources: Any, ym: str, drop_log: List[str], counters: Dict[str, int]) -> List[Item]:
    """
    Produces items WITH timestamps wherever possible.
    This is required because workflow sets ALLOW_UNDATED=0.
    """
    # normalise sources container
    if isinstance(sources, str):
        sources = [sources]
    elif isinstance(sources, dict):
        if "sources" in sources and isinstance(sources["sources"], list):
            sources = sources["sources"]
        else:
            sources = [sources]
    elif not isinstance(sources, list):
        sources = [sources]

    start_dt, end_dt = _ym_to_range(ym)

    items: List[Item] = []

    for src in sources:
        name, url, kind = _parse_source(src)
        if not url:
            drop_log.append(f"bad_source\t{section}\t{name}\t(empty_url)")
            continue

        dom = _domain(url)
        is_priority = dom in PRIORITY_DOMAINS

        if kind == "feed":
            # feedparser fetches the feed itself; we use it ONLY for indexing + dates.
            parsed = feedparser.parse(url)
            if getattr(parsed, "bozo", False) and not getattr(parsed, "entries", None):
                drop_log.append(f"feed_parse_fail\t{section}\t{name}\t{url}")
                continue

            for entry in (parsed.entries or [])[:MAX_LINKS_PER_INDEX]:
                link = (getattr(entry, "link", None) or entry.get("link") or "").strip()
                title = (getattr(entry, "title", None) or entry.get("title") or link or "").strip()
                if not link:
                    continue

                ts = ts_from_feed_entry(entry) or infer_ts_from_url(link)

                # Enforce month boundary here (saves fulltext fetch budget)
                if not _in_range(ts, start_dt, end_dt):
                    drop_log.append(f"out_of_range\t{section}\t{name}\t{link}")
                    continue

                # Full text fetch budget guards
                if counters["fulltext_total"] >= MAX_FULLTEXT_FETCHES_TOTAL:
                    drop_log.append(f"budget_fulltext_total\t{section}\t{name}\t{link}")
                    continue
                if counters["fulltext_section"] >= MAX_FULLTEXT_FETCHES_PER_SECTION:
                    drop_log.append(f"budget_fulltext_section\t{section}\t{name}\t{link}")
                    continue

                counters["fulltext_total"] += 1
                counters["fulltext_section"] += 1

                fr = fetch_url(link, priority=is_priority)
                if not fr.ok:
                    drop_log.append(f"fetch_fail\t{section}\t{name}\t{link}")
                    continue

                text = (fr.text or "").strip()
                if len(text) < (PRIORITY_MIN_CHARS if is_priority else MIN_TEXT_CHARS):
                    drop_log.append(f"too_short\t{section}\t{name}\t{link}")
                    continue

                # If still missing date, try text inference
                if ts is None:
                    ts = infer_ts_from_text(text)

                items.append(
                    Item(
                        section=section,
                        source=name,
                        title=title or link,
                        url=link,
                        domain=_domain(link),
                        ts=ts,
                        text=text,
                        substance_score=0,
                    )
                )

        else:
            # "page" sources are treated as single articles/pages (not a feed).
            fr = fetch_url(url, priority=is_priority)
            if not fr.ok:
                drop_log.append(f"fetch_fail\t{section}\t{name}\t{url}")
                continue

            text = (fr.text or "").strip()
            if len(text) < (PRIORITY_MIN_CHARS if is_priority else MIN_TEXT_CHARS):
                drop_log.append(f"too_short\t{section}\t{name}\t{url}")
                continue

            ts = infer_ts_from_url(url) or infer_ts_from_text(text)

            # if still no timestamp and ALLOW_UNDATED=0, this will be dropped in selection
            items.append(
                Item(
                    section=section,
                    source=name,
                    title=name,
                    url=url,
                    domain=_domain(url),
                    ts=ts,
                    text=text,
                    substance_score=0,
                )
            )

    # final: keep only those in month if ALLOW_UNDATED=0
    filtered: List[Item] = []
    for it in items:
        if _in_range(it.ts, start_dt, end_dt):
            filtered.append(it)
        else:
            drop_log.append(f"out_of_range\t{section}\t{it.source}\t{it.url}")
    return filtered


def score_substance(text: str) -> int:
    t = (text or "").lower()
    score = 0
    if len(t) >= 800:
        score += 1
    if any(k in t for k in ["aemo", "arena", "cefc", "ifrs", "efrag", "safeguard", "taxonomy", "inertia", "system strength"]):
        score += 1
    if any(k in t for k in ["consultation", "determination", "draft", "rule change", "final report", "exposure draft"]):
        score += 1
    return score


def select_items(pool: List[Item]) -> List[Item]:
    # annotate + filter by substance score
    candidates: List[Item] = []
    for it in pool:
        it.substance_score = score_substance(it.text)
        if it.substance_score < MIN_SUBSTANCE_SCORE:
            continue
        candidates.append(it)

    # stable-ish ordering: higher substance first, then longer text
    candidates.sort(key=lambda x: (x.substance_score, len(x.text)), reverse=True)

    selected: List[Item] = []
    per_dom: Dict[str, int] = {}

    for it in candidates:
        if len(selected) >= ITEMS_PER_SECTION:
            break
        c = per_dom.get(it.domain, 0)
        if c >= MAX_PER_DOMAIN_PER_SECTION:
            continue
        per_dom[it.domain] = c + 1
        selected.append(it)

    return selected


# ----------------------------
# Main generation
# ----------------------------

def generate_monthly_for(ym: str) -> None:
    cfg = _load_yaml_sources()
    sections = cfg.get("sections", {}) or {}

    all_selected: List[Dict[str, Any]] = []
    meta_lines: List[str] = []
    drop_log: List[str] = []

    meta_lines.append(f"ym={ym}")
    meta_lines.append(f"MODEL={MODEL}")
    meta_lines.append(f"ITEMS_PER_SECTION={ITEMS_PER_SECTION}")
    meta_lines.append(f"MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS}")
    meta_lines.append(f"ALLOW_UNDATED={ALLOW_UNDATED}")
    meta_lines.append(f"PRIORITY_DOMAINS={','.join(PRIORITY_DOMAINS)}")
    meta_lines.append(f"MAX_FULLTEXT_FETCHES_PER_SECTION={MAX_FULLTEXT_FETCHES_PER_SECTION}")
    meta_lines.append(f"MAX_FULLTEXT_FETCHES_TOTAL={MAX_FULLTEXT_FETCHES_TOTAL}")

    counters_global = {"fulltext_total": 0}

    for section, sources in sections.items():
        print(f"[section] {section}")

        counters = {"fulltext_total": counters_global["fulltext_total"], "fulltext_section": 0}
        pool = collect_items(section, sources, ym, drop_log, counters)
        counters_global["fulltext_total"] = counters["fulltext_total"]

        write_debug(OUTDIR / f"debug-pool-{section.replace(' ', '_')}-{ym}.json", [it.__dict__ for it in pool])
        print(f"[pool] candidates: {len(pool)}")

        chosen = select_items(pool)
        print(f"[selected] {len(chosen)} from {section}")

        for it in chosen:
            all_selected.append(it.__dict__)

    write_debug(OUTDIR / f"debug-selected-{ym}.json", all_selected)
    write_debug(OUTDIR / f"debug-drops-{ym}.txt", "\n".join(drop_log))
    write_debug(OUTDIR / f"debug-meta-{ym}.txt", "\n".join(meta_lines))

    if len(all_selected) < MIN_TOTAL_ITEMS:
        out_md = OUTDIR / f"monthly-digest-{ym}.md"
        out_md.write_text(
            f"# Monthly Digest ({ym})\n\n_No items met the selection criteria for this month._\n",
            encoding="utf-8",
        )
        print(f"[write] {out_md}")
        raise SystemExit(f"ERROR: selected {len(all_selected)} items for {ym} (MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS})")

    md = _call_build_digest(all_selected, ym, model=MODEL, temperature=TEMP, api_key=OPENAI_API_KEY)

    out_md = OUTDIR / f"monthly-digest-{ym}.md"
    out_md.write_text(md, encoding="utf-8")
    print(f"[write] {out_md}")


def _iter_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = [int(x) for x in start_ym.split("-")]
    ey, em = [int(x) for x in end_ym.split("-")]
    out: List[str] = []
    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def main() -> None:
    if MODE == "backfill-months":
        if not START_YM or not END_YM:
            raise SystemExit("ERROR: MODE=backfill-months requires START_YM and END_YM")
        months = _iter_months(START_YM, END_YM)
    else:
        months = [START_YM] if START_YM else [datetime.now().strftime("%Y-%m")]

    for ym in months:
        print(f"\n=== {ym} ({ym}-01 -> ...) ===")
        generate_monthly_for(ym)


if __name__ == "__main__":
    main()

# === END src/generate_monthly.py ===
