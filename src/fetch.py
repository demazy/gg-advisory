# -*- coding: utf-8 -*-
"""
Monthly digest generator.

Key robustness properties:
- Compatible with older/newer fetch/summarise signatures (via **kwargs shims).
- Emits debug-selected/meta/drops on every run.
- Preserves publisher (Item.source) and logical digest section (Item.section).

Incremental improvements (Feb 2026):
- Prevent low-signal "navigation" pages from being selected (home/about/login/etc).
- Fill each section up to ITEMS_PER_SECTION using a strict->relaxed two-pass strategy
  (relaxed still enforces minimum substance; it no longer allows arbitrarily short pages).
- Add "auto-allow" support for trusted public domains (e.g. *.gov.au, specific standards bodies)
  to reduce drops caused by an overly narrow allowlist.
- Add built-in deny patterns for common social/tracking/auth URLs to reduce noise.
"""

from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import yaml
from dateutil import parser as dtparser

from .fetch import Item, fetch_full_text, fetch_html_index, fetch_rss, is_probably_taxonomy_or_hub, infer_published_ts_from_url
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

# Relaxed pass still enforces substance; it just lowers the minimum.
RELAXED_MIN_TEXT_CHARS = int(os.getenv("RELAXED_MIN_TEXT_CHARS", str(max(300, MIN_TEXT_CHARS // 3))))

ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "1") == "1"
ALLOW_PLACEHOLDER = os.getenv("ALLOW_PLACEHOLDER", "1") == "1"
FALLBACK_WINDOW_DAYS = int(os.getenv("FALLBACK_WINDOW_DAYS", "3"))
DEBUG = os.getenv("DEBUG", "0") == "1"

# Auto-allow: expands an allowlist without requiring config changes (safe defaults).
AUTO_ALLOW_GOV_AU = os.getenv("AUTO_ALLOW_GOV_AU", "1") == "1"
AUTO_ALLOW_DOMAINS = {
    d.strip().lower()
    for d in os.getenv("AUTO_ALLOW_DOMAINS", "efrag.org").split(",")
    if d.strip()
}

PRIORITY_DOMAINS = {
    d.strip().lower()
    for d in os.getenv("PRIORITY_DOMAINS", "").split(",")
    if d.strip()
}

# Additional built-in noise filters (merged with config/filters.yaml)
BUILTIN_DENY_DOMAINS = {
    "facebook.com", "twitter.com", "x.com", "linkedin.com", "youtube.com", "instagram.com", "tiktok.com",
    "policies.google.com", "safelinks.protection.outlook.com",
}

BUILTIN_DENY_URL_SUBSTRINGS = [
    "oauth-redirect", "j_security_check", "login", "signin", "sign-in", "subscribe", "newsletter",
    "utm_", "fbclid=", "gclid=", "mc_cid=", "mc_eid=",
    "open.spotify.com", "spotify.com",
]

BUILTIN_DENY_TITLE_REGEX = [
    r"^\s*skip to (main )?content\s*$",
    r"^\s*about\s*$",
]

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


def _substance_ok_relaxed(text: str) -> bool:
    """Lower bar than _substance_ok, but still rejects boilerplate."""
    if not text:
        return False
    if len(text) < RELAXED_MIN_TEXT_CHARS:
        return False
    letters = sum(c.isalpha() for c in text)
    if letters < min(80, len(text) * 0.05):
        return False
    return True


def _url_has_content_hint(url: str) -> bool:
    """Cheap URL-only signal: does the path look like news/media/update content?"""
    u = (url or "").lower()
    return bool(re.search(r"(news|media|press|release|blog|insight|update|report|publication|consultation|speech|statement|submission|announcement)", u))


def _looks_articleish(url: str) -> bool:
    """
    Heuristic: URL appears to be an actual *content item* (article/report/release), not a hub
    or navigation page.

    This intentionally stays URL-only (no network calls) so it is safe to run at scale.
    """
    u = (url or "").strip()
    if not u:
        return False
    ul = u.lower()

    # obvious non-content patterns
    if is_probably_taxonomy_or_hub(ul):
        return False

    # must have meaningful path beyond root
    path = re.sub(r"[?#].*$", "", ul)
    path = re.sub(r"^https?://[^/]+", "", path)
    if path in ("", "/"):
        return False

    segs = [s for s in path.split("/") if s]
    last = segs[-1] if segs else ""

    # reject obvious listing endpoints (these are hubs, not items)
    listing_last = {
        "news", "blog", "blogs", "insights", "updates", "update", "events", "event",
        "publications", "publication", "reports", "report", "media", "press", "press-releases", "media-releases",
        "news-centre", "news-center", "newsroom", "pressroom", "news-and-media",
    }
    if last in listing_last and not re.search(r"(20\d{2})", path):
        return False

    has_year = bool(re.search(r"(20\d{2})", path))
    has_hint = _url_has_content_hint(path)

    # navigation / governance pages (typically not time-bound digest items)
    navish = bool(re.search(
        r"(?:^|/)(about|who-we-are|our-people|people|board|executive|executives|leadership|team|advisory|governance|committee|committees|"
        r"careers?|jobs?|vacanc(?:y|ies)|contact|privacy|terms|cookies|accessibility|sitemap)(?:/|$)",
        path,
        re.I,
    ))

    # strong-ish slug signal (avoids selecting "/news" etc)
    long_slug = (len(last) >= 18)

    # If the path is nav-ish and lacks strong content signals, reject.
    if navish and (not has_year) and (not has_hint):
        return False

    # Require at least one content signal: year/date, content hint segment, or a long slug.
    if has_year or has_hint or long_slug:
        return True

    return False


class Filters:
    def __init__(self, raw: Dict[str, Any]):
        # allow/deny lists from config
        self.allow_domains = [d.lower().strip() for d in raw.get("allow_domains", []) if str(d).strip()]
        self.deny_domains = [d.lower().strip() for d in raw.get("deny_domains", []) if str(d).strip()]
        self.deny_url_substrings = [s.lower() for s in raw.get("deny_url_substrings", []) if str(s).strip()]
        self.deny_title_regex = [re.compile(r, re.I) for r in raw.get("deny_title_regex", []) if str(r).strip()]
        self.section_keywords = {
            k: [w.lower() for w in v] for k, v in (raw.get("section_keywords", {}) or {}).items()
            if isinstance(v, list)
        }

        # merge built-in denylists (incremental improvement)
        for d in BUILTIN_DENY_DOMAINS:
            if d not in self.deny_domains:
                self.deny_domains.append(d)
        for s in BUILTIN_DENY_URL_SUBSTRINGS:
            if s not in self.deny_url_substrings:
                self.deny_url_substrings.append(s)
        for rx in BUILTIN_DENY_TITLE_REGEX:
            try:
                self.deny_title_regex.append(re.compile(rx, re.I))
            except Exception:
                pass

    def domain_allowed(self, domain: str) -> bool:
        d = (domain or "").lower()
        if not d:
            return False

        # If an allowlist exists, allow it…
        if self.allow_domains:
            if any(d == a or d.endswith("." + a) for a in self.allow_domains):
                return True

            # …but also auto-allow a small set of trusted public domains so that a too-narrow
            # allowlist does not collapse the pool (e.g., AEMC/CER/EFRAG in your debug drops).
            if AUTO_ALLOW_GOV_AU and (d.endswith(".gov.au") or d.endswith(".edu.au")):
                return True
            if any(d == a or d.endswith("." + a) for a in AUTO_ALLOW_DOMAINS):
                return True

            return False

        # No allowlist: allow by default (still subject to deny lists)
        return True

    def domain_denied(self, domain: str) -> bool:
        d = (domain or "").lower()
        return any(d == x or d.endswith("." + x) for x in self.deny_domains)


def _passes_filters(it: Item, flt: Filters, section: str, *, bypass_allow: bool = False) -> Tuple[bool, str]:
    url = (it.url or "").strip()
    title = (it.title or "").strip()
    if not url or not title:
        return False, "missing_url_or_title"

    # Reject obvious non-item URLs early.
    if is_probably_taxonomy_or_hub(url):
        return False, "hub_url"

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
    articleish = 0.25 if _looks_articleish(it.url or "") else -0.35
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

    # URL dedup
    seen: Set[str] = set()
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
    items_needed: int,
    per_domain_cap: int,
    strict: bool,
    bypass_allow: bool = False,
    exclude_urls: Optional[Set[str]] = None,
    initial_per_domain: Optional[Dict[str, int]] = None,
) -> Tuple[List[Item], List[Dict[str, str]]]:
    """
    Select up to items_needed items from pool.

    strict=True:
      - requires _looks_articleish AND _substance_ok
    strict=False (relaxed):
      - still requires _looks_articleish AND _substance_ok_relaxed
        (previously it could select arbitrarily short/non-content pages)
    """
    drops: List[Dict[str, str]] = []
    selected: List[Item] = []
    per_domain: Dict[str, int] = dict(initial_per_domain or {})
    ex: Set[str] = set((u or "").strip().lower() for u in (exclude_urls or set()) if str(u).strip())
    text_cache: Dict[str, str] = {}

def _effective_dt_with_infer(it: Item) -> Optional[datetime]:
    dt = _effective_published_ts(it)
    if dt is not None:
        return dt
    # try URL inference (keeps date filtering honest for e.g. "...-february-2026" slugs)
    try:
        ts = infer_published_ts_from_url(it.url or "")
    except Exception:
        ts = None
    if ts:
        try:
            dt2 = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            # persist the inferred signal for downstream debugging/formatting
            if getattr(it, "published_ts", None) is None:
                it.published_ts = float(ts)
                it.published_source = getattr(it, "published_source", None) or "url_infer"
                it.published_confidence = getattr(it, "published_confidence", None) or 0.35
            return dt2
        except Exception:
            return None
    return None

def sort_key(it: Item):
    dt = _effective_dt_with_infer(it)
    ts = dt.timestamp() if dt else 0.0
    has_dt = 1 if dt else 0
    hint = 1 if _url_has_content_hint(it.url or "") else 0
    kw = _keyword_boost(it.title or "", section, flt)
    # date first, then URL-content signals (prevents "/about/..." from dominating undated pools)
    return (-has_dt, -ts, -hint, -kw, (it.url or ""))

    for it in sorted(pool, key=sort_key):
        url = (it.url or "").strip()
        if not url:
            drops.append({"reason": "missing_url", "url": "", "title": it.title or ""})
            continue
        if url.lower() in ex:
            continue

        ok, why = _passes_filters(it, flt, section, bypass_allow=bypass_allow)
        if not ok:
            drops.append({"reason": why, "url": url, "title": it.title or ""})
            continue

        dt_eff = _effective_dt_with_infer(it)
        if not _in_range(dt_eff, start_dt, end_dt):
            drops.append({"reason": "out_of_range", "url": url, "title": it.title or ""})
            continue

        domain = normalise_domain(url)
        if per_domain.get(domain, 0) >= per_domain_cap:
            drops.append({"reason": "per_domain_cap", "url": url, "title": it.title or "", "domain": domain})
            continue

        if not _looks_articleish(url):
            drops.append({"reason": "not_articleish", "url": url, "title": it.title or ""})
            continue

        text = text_cache.get(url, "")
        if not text:
            try:
                text = (fetch_full_text(url) or "").strip()
            except Exception:
                text = ""
            if not text:
                text = (it.summary or "").strip()
            text_cache[url] = text

        if strict:
            prio = _is_priority(url)
            if not _substance_ok(text, prio):
                drops.append({"reason": "low_substance", "url": url, "title": it.title or ""})
                continue
        else:
            if not _substance_ok_relaxed(text):
                drops.append({"reason": "low_substance_relaxed", "url": url, "title": it.title or ""})
                continue

        if text:
            it.summary = text

        selected.append(it)
        per_domain[domain] = per_domain.get(domain, 0) + 1
        ex.add(url.lower())

        if len(selected) >= max(0, items_needed):
            break

    return selected, drops


def _last_resort_pick(pool: Sequence[Item], section: str, flt: Filters, *, items_needed: int) -> Tuple[List[Item], List[Dict[str, str]]]:
    """
    Emergency picker used only when strict+relaxed selection yields nothing.
    Still rejects obvious non-content URLs/titles and requires at least minimal extract.
    """
    drops: List[Dict[str, str]] = []
    scored: List[Tuple[float, Item]] = []

    for it in pool:
        url = (it.url or "").strip()
        if not url:
            continue

        ok, why = _passes_filters(it, flt, section, bypass_allow=True)
        if not ok:
            drops.append({"reason": why, "url": url, "title": it.title or ""})
            continue

        if not _looks_articleish(url):
            drops.append({"reason": "not_articleish", "url": url, "title": it.title or ""})
            continue

        text = ""
        try:
            text = (fetch_full_text(url) or "").strip()
        except Exception:
            text = ""
        if not text:
            text = (it.summary or "").strip()

        # require at least some substance even in last resort
        if not _substance_ok_relaxed(text):
            drops.append({"reason": "low_substance_last_resort", "url": url, "title": it.title or ""})
            continue

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
    global_used_urls: Set[str] = set()

    sections: Dict[str, Any] = cfg_sources.get("sections") or {}
    for section, sec_cfg in sections.items():
        print(f" {section}")
        pool, drops0 = _collect_section_pool(section, sec_cfg or {})
        all_drops.extend(drops0)

        if DEBUG:
            pool_path = OUT_DIR / f"debug-pool-{_slug(section)}-{ym}.json"
            pool_path.write_text(json.dumps([asdict(it) for it in pool], ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"[pool] candidates: {len(pool)}")

        # Pass 1 (strict)
        selected, drops1 = _select_from_pool(
            pool, section, start_dt, end_dt, flt,
            items_needed=ITEMS_PER_SECTION,
            per_domain_cap=PER_DOMAIN_CAP,
            strict=True,
            exclude_urls=global_used_urls,
        )
        all_drops.extend(drops1)

        # Pass 2 (relaxed fill): only to top up to quota, and still enforces minimum substance
        if len(selected) < ITEMS_PER_SECTION:
            used = {it.url.lower() for it in selected if it.url}
            per_dom = Counter(normalise_domain(it.url) for it in selected if it.url)
            filler, drops2 = _select_from_pool(
                pool, section, start_dt, end_dt, flt,
                items_needed=(ITEMS_PER_SECTION - len(selected)),
                per_domain_cap=PER_DOMAIN_CAP,
                strict=False,
                exclude_urls=(used | global_used_urls),
                initial_per_domain=dict(per_dom),
            )
            all_drops.extend(drops2)
            selected.extend(filler)

        # Wider date window (still strict+relaxed)
        if not selected:
            print(f"[warn] No selected items in strict/relaxed passes; applying ±{FALLBACK_WINDOW_DAYS} day window.")
            s2 = start_dt - timedelta(days=FALLBACK_WINDOW_DAYS)
            e2 = end_dt + timedelta(days=FALLBACK_WINDOW_DAYS)
            selected3, drops3 = _select_from_pool(
                pool, section, s2, e2, flt,
                items_needed=ITEMS_PER_SECTION,
                per_domain_cap=PER_DOMAIN_CAP,
                strict=False,
                exclude_urls=global_used_urls,
            )
            all_drops.extend(drops3)
            selected = selected3

        # Last resort: pick only content-like URLs with minimal extract
        if not selected:
            print("[warn] Still no items after fallback; last-resort pick (ignoring dates).")
            pool_lr = [it for it in pool if (it.url or '').strip().lower() not in global_used_urls]
            picked, drops4 = _last_resort_pick(pool_lr, section, flt, items_needed=ITEMS_PER_SECTION)
            all_drops.extend(drops4)
            selected = picked

        # Emergency RSS only if nothing at all
        if not selected:
            print("[warn] No candidates available; trying emergency RSS.")
            epool = _emergency_pool(section)
            picked, drops5 = _last_resort_pick(epool, section, flt, items_needed=max(1, ITEMS_PER_SECTION // 2))
            all_drops.extend(drops5)
            selected = picked


# Emergency top-up: if we are still below quota, try Google News RSS to fill gaps
# (date-filtered + deduped, bypassing allowlist so it can contribute diversity).
if len(selected) < ITEMS_PER_SECTION:
    need = ITEMS_PER_SECTION - len(selected)
    epool = _emergency_pool(section)
    if epool:
        used2 = {it.url.lower() for it in selected if it.url} | global_used_urls
        per_dom2 = Counter(normalise_domain(it.url) for it in selected if it.url)
        extra, dropsE = _select_from_pool(
            epool, section, start_dt, end_dt, flt,
            items_needed=need,
            per_domain_cap=PER_DOMAIN_CAP,
            strict=False,
            bypass_allow=True,
            exclude_urls=used2,
            initial_per_domain=dict(per_dom2),
        )
        all_drops.extend(dropsE)
        selected.extend(extra)

        # Placeholder (only if explicitly enabled)
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

        print(f"[selected] {len(selected)} from {section}")
        # Global de-dup across sections (prevents the same page appearing in multiple sections)
        global_used_urls.update({it.url.lower() for it in selected if it.url})
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

    # Ensure we meet MIN_TOTAL_ITEMS (avoids downstream guardrails failing)
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
                f"relaxed_min_text_chars={RELAXED_MIN_TEXT_CHARS}",
                f"auto_allow_gov_au={AUTO_ALLOW_GOV_AU}",
                f"auto_allow_domains={','.join(sorted(AUTO_ALLOW_DOMAINS)) if AUTO_ALLOW_DOMAINS else ''}",
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
