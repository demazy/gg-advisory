# src/generate_monthly.py
# Monthly digest generator (robust date coercion + fetch optimisation)

from __future__ import annotations

import os
import re
import json
import calendar
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

MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "300"))
PRIORITY_MIN_CHARS = int(os.getenv("PRIORITY_MIN_CHARS", "200"))

ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "7"))
PER_DOMAIN_CAP = int(os.getenv("PER_DOMAIN_CAP", "3"))
MIN_TOTAL_ITEMS = int(os.getenv("MIN_TOTAL_ITEMS", "1"))

DEBUG = os.getenv("DEBUG", "0") == "1"

MIN_SUBSTANCE_SCORE = int(os.getenv("MIN_SUBSTANCE_SCORE", "2"))
MAX_PER_DOMAIN_PER_SECTION = int(os.getenv("MAX_PER_DOMAIN_PER_SECTION", "3"))

ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "0") == "1"

# Hard runtime budgets (key optimisation)
MAX_FULLTEXT_FETCHES_PER_SECTION = int(os.getenv("MAX_FULLTEXT_FETCHES_PER_SECTION", "60"))
MAX_FULLTEXT_FETCHES_TOTAL = int(os.getenv("MAX_FULLTEXT_FETCHES_TOTAL", "160"))

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
CFG = ROOT / "config" / "sources.yaml"
FILTERS = ROOT / "config" / "filters.yaml"
OUTDIR.mkdir(parents=True, exist_ok=True)

# ----------------------- DATE HELPERS --------------------------


def _month_bounds(y: int, m: int) -> Tuple[datetime, datetime]:
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    last = calendar.monthrange(y, m)[1]
    end = datetime(y, m, last, 23, 59, 59, tzinfo=timezone.utc)
    return start, end


def _coerce_ts(x) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, datetime):
        dt = x
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            dt = dtparser.parse(s)
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).timestamp()
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
        return ALLOW_UNDATED

    s2 = _coerce_ts(start)
    e2 = _coerce_ts(end)
    if s2 is None or e2 is None:
        return True

    return s2 <= ts2 <= e2


def fmt_iso(ts: float | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


# ----------------------- YAML / FILTERS ------------------------


def load_yaml(path: Path) -> Dict[str, Any]:
    import yaml

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def compile_filters(cfg: Dict[str, Any]) -> Dict[str, Any]:
    # pass-through structure; you already do most logic in generator
    return cfg or {}


def is_meeting_notice(url: str, title: str) -> bool:
    low = (url + " " + title).lower()
    return any(k in low for k in ("public meeting", "webinar", "register", "agenda", "minutes", "event", "/events"))


def is_hub_url(url: str) -> bool:
    p = url.lower()
    return any(
        k in p
        for k in (
            "/tag/",
            "/category/",
            "/topics/",
            "search?",
            "/search/",
            "/newsroom",  # can be index-y depending on site
        )
    )


# -------------------- Cheap prescreen (new) --------------------


_PROMISING_TITLE_WORDS = re.compile(
    r"\b(rule|reform|decision|consultation|draft|standard|guidance|framework|taxonomy|report|plan|roadmap|policy|market|auction|cfds?|safeguard|disclosure|assurance)\b",
    re.IGNORECASE,
)


def _looks_like_article_url(url: str) -> bool:
    u = url.lower()
    if any(u.endswith(ext) for ext in (".pdf", ".zip", ".jpg", ".png")):
        return False
    # common article patterns
    if re.search(r"/\d{4}/\d{2}/\d{2}/", u):
        return True
    if re.search(r"/news|/media|/articles|/insights|/publications|/blog", u):
        return True
    # avoid obvious landing pages
    if u.rstrip("/").endswith(("/news", "/newsroom", "/media", "/publications")):
        return False
    return True


def _should_fetch_fulltext(it: Item) -> bool:
    """
    Avoid expensive fetch_full_text() unless a candidate looks worth it.
    """
    dom = normalise_domain(it.url)
    is_priority = any(dom == d or dom.endswith("." + d) for d in PRIORITY_DOMAINS)

    title = (it.title or "").strip()
    summary = (it.summary or "").strip()

    if is_priority:
        return True  # priority domains: always fetch, within budgets

    # non-priority: require some signal
    if len(summary) >= 160:
        return True
    if _PROMISING_TITLE_WORDS.search(title or ""):
        return True
    if _looks_like_article_url(it.url):
        return True

    return False


# ----------------------- Scoring / selection -------------------


def substance_score(txt: str) -> int:
    """
    Very cheap heuristic score:
    - longer text and presence of some structural cues increases score
    """
    if not txt:
        return 0
    score = 0
    if len(txt) >= 500:
        score += 1
    if len(txt) >= 1200:
        score += 1
    if re.search(r"\b(consultation|submission|determination|standard|guidance|rule change|draft)\b", txt, re.I):
        score += 1
    return score


def _append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def _dump_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
def _parse_source(src: Any) -> Dict[str, Any]:
    """
    Accepts either:
      - "https://..." (string)
      - {"name": "...", "url": "...", "type": "rss|html"} (dict)
    Returns a normalized dict with keys: name, url, type
    """
    if isinstance(src, str):
        url = src.strip()
        # heuristic: treat xml feeds as rss unless explicitly html
        kind = "rss" if (url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower()) else "html"
        return {"name": url, "url": url, "type": kind}

    if isinstance(src, dict):
        url = (src.get("url") or "").strip()
        name = (src.get("name") or url or "source").strip()
        kind = (src.get("type") or "").strip().lower()

        if not kind:
            # if type omitted in dict, infer
            kind = "rss" if (url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower()) else "html"

        # normalize kind
        if kind not in ("rss", "html"):
            kind = "html"

        return {"name": name, "url": url, "type": kind}

    # unknown structure
    return {"name": "source", "url": "", "type": "html"}


def collect_items(sources: List[Any], drop_log) -> List[Item]:
    pool: List[Item] = []

    for raw in sources:
        src = _parse_source(raw)
        name = src["name"]
        url = src["url"]
        kind = src["type"]

        if not url:
            drop_log(f"source_invalid\t{name}\t{url}\t(empty_url)")
            continue

        try:
            if kind == "rss":
                items = fetch_rss(url, source_name=name)
            else:
                items = fetch_html_index(url, source_name=name)

            pool.extend(items)

        except Exception as e:
            drop_log(f"source_error\t{name}\t{url}\t{e}")
            if DEBUG:
                print(f"[warn] source error: {url} -> {e}")

    pool.sort(key=lambda x: _coerce_ts(getattr(x, "published_ts", None)) or 0.0, reverse=True)
    return pool



# ----------------------- MONTHLY GENERATION --------------------


def generate_monthly_for(ym: str) -> str:
    os.environ["TARGET_YM"] = ym

    y, m = map(int, ym.split("-"))
    start, end = _month_bounds(y, m)

    cfg = load_yaml(CFG)
    _ = compile_filters(load_yaml(FILTERS))  # kept for compatibility

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

    chosen: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    total_fulltext_fetches = 0

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
                        "published": fmt_iso(it.published_ts),
                        "published_ts": it.published_ts,
                        "summary_len": len(it.summary or ""),
                    }
                    for it in pool
                ],
            )

        selected: List[Dict[str, Any]] = []
        section_dom_counts: Dict[str, int] = {}
        fulltext_fetches_section = 0

        for it in pool:
            if len(selected) >= ITEMS_PER_SECTION:
                break

            # hard caps (most important runtime guard)
            if fulltext_fetches_section >= MAX_FULLTEXT_FETCHES_PER_SECTION:
                if DEBUG:
                    drop_log(f"section_fulltext_budget_exhausted\tsection={section}")
                break
            if total_fulltext_fetches >= MAX_FULLTEXT_FETCHES_TOTAL:
                if DEBUG:
                    drop_log("global_fulltext_budget_exhausted\t-")
                break

            if not it.url or it.url in seen_urls:
                continue

            if is_hub_url(it.url):
                if DEBUG:
                    drop_log(f"hub_url\t{it.url}")
                continue

            if is_meeting_notice(it.url, it.title or ""):
                if DEBUG:
                    drop_log(f"meeting_notice\t{it.url}")
                continue

            # date range filter first (cheap)
            if not _in_range(it.published_ts, start, end):
                if DEBUG:
                    drop_log(f"out_of_range\t{fmt_iso(it.published_ts)}\t{it.url}")
                continue

            # dedupe by hash
            h = sha1(it.url)
            if h in seen_urls:
                if DEBUG:
                    drop_log(f"duplicate_url\t-\t{it.url}")
                continue

            dom = normalise_domain(it.url)

            if section_dom_counts.get(dom, 0) >= MAX_PER_DOMAIN_PER_SECTION:
                if DEBUG:
                    drop_log(
                        f"per_section_domain_cap\tsection={section}\tdom={dom} cap={MAX_PER_DOMAIN_PER_SECTION}\t{it.url}"
                    )
                continue

            # pre-screen before fulltext fetch (new, major win)
            if not _should_fetch_fulltext(it):
                if DEBUG:
                    drop_log(f"prescreen_skip\tdom={dom}\t{it.url}")
                continue

            # full text
            txt = fetch_full_text(it.url)
            fulltext_fetches_section += 1
            total_fulltext_fetches += 1

            txt = normalize_whitespace(txt)

            threshold = PRIORITY_MIN_CHARS if any(dom == d or dom.endswith("." + d) for d in PRIORITY_DOMAINS) else MIN_TEXT_CHARS

            # if full text is too short, allow summary to rescue (rare but useful)
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
                    "published": fmt_iso(_coerce_ts(it.published_ts)),
                }
            )
            seen_urls.add(h)

        print(f"[selected] {len(selected)} from {section}")
        chosen.extend(selected)

        if DEBUG:
            _append_line(
                meta_file,
                f"{section}\tpool={len(pool)}\tselected={len(selected)}\tfulltext_fetches={fulltext_fetches_section}",
            )

    _dump_json(selected_file, chosen)

    md = build_digest(
        chosen,
        model=MODEL,
        temperature=TEMP,
        api_key=OPENAI_API_KEY,
        ym=ym,
    )

    out_md = OUTDIR / f"monthly-digest-{ym}.md"
    out_md.write_text(md, encoding="utf-8")
    print(f"[write] {out_md}")

    if DEBUG:
        _append_line(meta_file, f"TOTAL\tselected={len(chosen)}\tfulltext_fetches={total_fulltext_fetches}")

    return md


def _iter_months(start_ym: str, end_ym: str) -> List[str]:
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    out = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def main() -> None:
    mode = os.getenv("MODE", "single-month")

    if mode == "backfill-months":
        start_ym = os.getenv("START_YM")
        end_ym = os.getenv("END_YM") or start_ym
        assert start_ym, "START_YM is required in backfill-months mode"
        for ym in _iter_months(start_ym, end_ym):
            print(f"\n=== {ym} ({ym}-01 -> ...) ===")
            generate_monthly_for(ym)
        return

    ym = os.getenv("TARGET_YM")
    if not ym:
        now = datetime.now(tz=timezone.utc)
        ym = f"{now.year:04d}-{now.month:02d}"
    generate_monthly_for(ym)


if __name__ == "__main__":
    main()
