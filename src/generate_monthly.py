# === BEGIN src/generate_monthly.py ===
from __future__ import annotations

import inspect as _inspect
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

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


# ----------------------------
# Compatibility wrapper (fixes build_digest signature drift)
# ----------------------------

def _call_build_digest(selected_items, ym, *, model, temperature, api_key):
    """
    Call build_digest with a signature-compatible keyword mapping.

    build_digest() has had a few signature variants in this repo; introspection
    keeps the workflow stable when that changes.
    """
    sig = _inspect.signature(build_digest)
    params = list(sig.parameters.values())
    kwargs: Dict[str, Any] = {}

    # Map the 'items' argument (whatever it's called)
    preferred_item_keys = ["items", "selected", "entries", "articles", "records", "chosen"]
    item_key = None
    for k in preferred_item_keys:
        if k in sig.parameters:
            item_key = k
            break
    if item_key is None:
        item_key = params[0].name if params else "items"
    kwargs[item_key] = selected_items

    # Common optional params
    if "ym" in sig.parameters:
        kwargs["ym"] = ym
    elif "month" in sig.parameters:
        kwargs["month"] = ym

    if "model" in sig.parameters:
        kwargs["model"] = model

    if "temperature" in sig.parameters:
        kwargs["temperature"] = temperature
    elif "temp" in sig.parameters:
        kwargs["temp"] = temperature

    if "api_key" in sig.parameters:
        kwargs["api_key"] = api_key
    elif "openai_api_key" in sig.parameters:
        kwargs["openai_api_key"] = api_key

    # If build_digest doesn't accept **kwargs, strip unknown keys
    if not any(p.kind == p.VAR_KEYWORD for p in params):
        allowed = set(sig.parameters.keys())
        kwargs = {k: v for k, v in kwargs.items() if k in allowed}

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
        # try ISO
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


def _parse_source(src: Any) -> Tuple[str, str]:
    """
    src may be:
      - string URL
      - dict {name, url}
      - dict {url}
    """
    if isinstance(src, str):
        return (src, src)
    if isinstance(src, dict):
        url = src.get("url") or src.get("feed") or src.get("link") or ""
        name = src.get("name") or url or "source"
        return (name, url)
    return ("source", str(src))


def _load_yaml_sources() -> Dict[str, Any]:
    # This repo commonly keeps the monthly sources config under config/monthly.yml
    # or monthly.yml at root. We tolerate both.
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


def _resolve_index_links(url: str, *, priority: bool) -> List[str]:
    fr = fetch_url(url, priority=priority)
    if not fr.ok or not fr.text:
        return []
    # best-effort parse: links in HTML only
    links = re.findall(r'https?://[^\s"<>]+', fr.text)
    # very light de-dup
    out: List[str] = []
    seen = set()
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def collect_items(sources: Any, drop_log: List[str]) -> List[Item]:
    # Normalise sources: YAML can provide a single string, a dict, or a list.
    if isinstance(sources, str):
        sources = [sources]
    elif isinstance(sources, dict):
        # tolerate {"sources":[...]} / {"url": "..."} shapes
        if "sources" in sources and isinstance(sources["sources"], list):
            sources = sources["sources"]
        else:
            sources = [sources]

    items: List[Item] = []
    now = time_now_utc()

    for src in sources:
        name, url = _parse_source(src)
        if not url:
            drop_log.append(f"bad_source\t{name}\t(empty_url)")
            continue

        dom = _domain(url)
        is_priority = dom in PRIORITY_DOMAINS

        # Index sources can be expanded (optional heuristic)
        if url.endswith("/") or "index" in url.lower():
            links = _resolve_index_links(url, priority=is_priority)
            for u in links[:220]:
                fr = fetch_url(u, priority=is_priority)
                if not fr.ok:
                    drop_log.append(f"fetch_fail\t{name}\t{u}")
                    continue
                text = (fr.text or "").strip()
                if len(text) < MIN_TEXT_CHARS:
                    drop_log.append(f"too_short\t{name}\t{u}")
                    continue
                items.append(
                    Item(
                        section="",
                        source=name,
                        title=u,
                        url=u,
                        domain=_domain(u),
                        ts=None,
                        text=text,
                        substance_score=0,
                    )
                )
            continue

        # Normal fetch of feed/page
        fr = fetch_url(url, priority=is_priority)
        if not fr.ok:
            drop_log.append(f"fetch_fail\t{name}\t{url}")
            continue

        text = (fr.text or "").strip()
        if len(text) < (PRIORITY_MIN_CHARS if is_priority else MIN_TEXT_CHARS):
            drop_log.append(f"too_short\t{name}\t{url}")
            continue

        items.append(
            Item(
                section="",
                source=name,
                title=name,
                url=url,
                domain=_domain(url),
                ts=None,
                text=text,
                substance_score=0,
            )
        )

    return items


def score_substance(text: str) -> int:
    # extremely cheap heuristic
    t = text.lower()
    score = 0
    if len(t) >= 800:
        score += 1
    if any(k in t for k in ["aemo", "arena", "cefc", "ifrs", "efrag", "safeguard", "taxonomy", "inertia", "system strength"]):
        score += 1
    if any(k in t for k in ["consultation", "determination", "draft", "rule change", "final report", "exposure draft"]):
        score += 1
    return score


def select_items(pool: List[Item], section: str, ym: str) -> List[Item]:
    start_dt, end_dt = _ym_to_range(ym)

    # annotate
    candidates: List[Item] = []
    for it in pool:
        it.section = section
        it.substance_score = score_substance(it.text)
        if it.substance_score < MIN_SUBSTANCE_SCORE:
            continue
        if not _in_range(it.ts, start_dt, end_dt):
            continue
        candidates.append(it)

    # per-domain cap
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


def time_now_utc() -> datetime:
    return datetime.now(timezone.utc)


def write_debug(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, (dict, list)):
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        path.write_text(str(data), encoding="utf-8")
    if DEBUG:
        print(f"[write] {path}")


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

    for section, sources in sections.items():
        print(f"[section] {section}")

        pool = collect_items(sources, drop_log)
        write_debug(OUTDIR / f"debug-pool-{section.replace(' ', '_')}-{ym}.json", [it.__dict__ for it in pool])

        print(f"[pool] candidates: {len(pool)}")

        chosen = select_items(pool, section, ym)
        print(f"[selected] {len(chosen)} from {section}")

        for it in chosen:
            all_selected.append(it.__dict__)

    write_debug(OUTDIR / f"debug-selected-{ym}.json", all_selected)
    write_debug(OUTDIR / f"debug-drops-{ym}.txt", "\n".join(drop_log))
    write_debug(OUTDIR / f"debug-meta-{ym}.txt", "\n".join(meta_lines))

    if len(all_selected) < MIN_TOTAL_ITEMS:
        # Fail fast but still emit a placeholder digest file for artefacts.
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
        # default: run for START_YM if provided, else current month
        months = [START_YM] if START_YM else [datetime.now().strftime("%Y-%m")]

    for ym in months:
        print(f"\n=== {ym} ({ym}-01 -> ...) ===")
        generate_monthly_for(ym)


if __name__ == "__main__":
    main()

# === END src/generate_monthly.py ===
