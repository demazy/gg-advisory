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
from urllib.parse import urlparse, parse_qs
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

# Relaxed pass still enforces substance; it just lowers the minimum.
RELAXED_MIN_TEXT_CHARS = int(os.getenv("RELAXED_MIN_TEXT_CHARS", str(max(300, MIN_TEXT_CHARS // 3))))


ALLOW_UNDATED = os.getenv("ALLOW_UNDATED", "0") == "1"
# Allow placeholder digest generation even if item count is low/zero.
# This should not hard-fail unless FAIL_ON_BELOW_MIN_TOTAL=1 is set.
ALLOW_PLACEHOLDER = os.getenv("ALLOW_PLACEHOLDER", "1") == "1"

# Date window padding around the target month.
# Use asymmetric defaults: allow late previous-month releases, but avoid pulling future-month content.
RANGE_PAD_BEFORE_DAYS = int(os.getenv("RANGE_PAD_BEFORE_DAYS", os.getenv("RANGE_PAD_DAYS", "14")))
RANGE_PAD_AFTER_DAYS = int(os.getenv("RANGE_PAD_AFTER_DAYS", os.getenv("RANGE_PAD_DAYS", "2")))

# Scoring/selection budget: how many candidates per section we spend full-text fetch on.
MAX_SCORE_FETCHES_PER_SECTION = int(os.getenv("MAX_SCORE_FETCHES_PER_SECTION", "60"))

# Last-resort backfill (only used when a section returns zero items after strict+relaxed).
LAST_RESORT_BACKFILL_DAYS = int(os.getenv("LAST_RESORT_BACKFILL_DAYS", "45"))
LAST_RESORT_MAX_STALENESS_DAYS = int(os.getenv("LAST_RESORT_MAX_STALENESS_DAYS", "120"))
LAST_RESORT_MAX_FETCHES = int(os.getenv("LAST_RESORT_MAX_FETCHES", "40"))

# If selected_total < MIN_TOTAL_ITEMS, only fail if explicitly configured.
FAIL_ON_BELOW_MIN_TOTAL = os.getenv("FAIL_ON_BELOW_MIN_TOTAL", "0") == "1"

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
    """Return a UTC datetime if we can determine a publish timestamp for the item.

    Be schema-tolerant: prefer published_ts but fall back to published_iso / published (if present).
    """
    for attr in ("published_ts", "published_iso", "published"):
        dt = _coerce_ts(getattr(it, attr, None))
        if dt is not None:
            return dt
    return None

def _item_is_undated(it: Item) -> bool:
    """True if item has no usable publish timestamp."""
    return _effective_published_ts(it) is None




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


def _looks_articleish(url: str) -> bool:
    """
    Heuristic: URL appears to be an actual *content item* (article/report/media release),
    not a hub/listing, navigation, governance, or utility page.

    This is intentionally conservative because false positives create low-quality digests
    and increase hallucination risk downstream.
    """
    u = (url or "").strip()
    if not u:
        return False
    ul = u.lower()

    if is_probably_taxonomy_or_hub(ul):
        return False

    clean = re.sub(r"[?#].*$", "", ul)
    path = re.sub(r"^https?://[^/]+", "", clean) or "/"
    if path in ("", "/"):
        return False

    segs = [s for s in path.split("/") if s]
    if not segs:
        return False

    evergreen_segments = {
        "about", "contact", "privacy", "terms", "cookies", "cookie-policy", "accessibility", "sitemap",
        "careers", "jobs", "vacancies",
        "search", "login", "signin", "sign-in", "subscribe", "newsletter",
        "board", "governance", "leadership", "executive", "executives", "management", "team", "teams",
        "advisory", "advisory-panel", "advisorypanel",
        "our-people", "people", "who-we-are", "organisation", "organization",
        "media-centre", "media-center", "pressroom", "newsroom",
    }
    if any(seg in evergreen_segments for seg in segs):
        return False

    slug = segs[-1]
    if re.search(
        r"(executive[-_]?leadership|leadership[-_]?team|board[-_]?members?|advisory[-_]?panel|our[-_]?people|executive[-_]?team)",
        slug,
    ):
        return False

    is_pdf = slug.endswith(".pdf")

    positive_parts = {
        "news", "media", "press", "releases", "release", "announcements", "announcement",
        "updates", "insights", "blog",
        "publications", "publication", "reports", "report",
        "consultations", "consultation",
        "statements", "statement",
    }
    has_positive = any(seg in positive_parts for seg in segs)
    has_year = bool(re.search(r"(20\d{2})", path))
    has_long_slug = len(slug) >= 18

    if is_pdf:
        return has_positive or has_year or has_long_slug

    if has_positive or has_year:
        return True

    if len(segs) <= 1:
        return has_long_slug

    return has_long_slug


    # must have meaningful path beyond root
    path = re.sub(r"[?#].*$", "", ul)
    # strip scheme+domain
    path = re.sub(r"^https?://[^/]+", "", path)
    if path in ("", "/"):
        return False

    # deny common nav segments (segment-based, to avoid false positives like ".../about-energy...")
    bad_segments = {
        "about", "contact", "privacy", "terms", "cookies", "accessibility", "sitemap",
        "careers", "jobs", "vacancies",
    }
    segs = [s for s in path.split("/") if s]
    if segs and segs[-1] in bad_segments:
        return False

    # require at least one "signal": depth, date-like digits, or a long slug
    depth = len(segs)
    if depth >= 2:
        return True
    if re.search(r"(20\d{2})", path):
        return True
    if segs and len(segs[-1]) >= 18:
        return True

    return False


class Filters:
    def __init__(self, raw: Dict[str, Any]):
        # allow/deny lists from config (supporting alias keys)
        self.allow_domains = [str(d).lower().strip() for d in (raw.get("allow_domains") or []) if str(d).strip()]
        self.deny_domains = [str(d).lower().strip() for d in (raw.get("deny_domains") or []) if str(d).strip()]

        self.deny_url_substrings = [str(s).lower() for s in (raw.get("deny_url_substrings") or []) if str(s).strip()]

        deny_title_list = (raw.get("deny_title_regex") or raw.get("title_deny_regex") or [])
        self.deny_title_regex = []
        for r in deny_title_list:
            rr = str(r).strip()
            if not rr:
                continue
            try:
                self.deny_title_regex.append(re.compile(rr, re.I))
            except Exception:
                pass

        self.section_keywords = {
            k: [str(w).lower() for w in v]
            for k, v in (raw.get("section_keywords", {}) or {}).items()
            if isinstance(v, list)
        }

        # Per-domain URL substring denylists (domain_pattern -> [url_substring,...])
        self.domain_deny_substrings: Dict[str, List[str]] = {}
        dd = raw.get("domain_deny_substrings", {}) or {}
        if isinstance(dd, dict):
            for k, v in dd.items():
                key = str(k).strip().lower()
                if not key:
                    continue
                if isinstance(v, list):
                    subs = [str(x).strip().lower() for x in v if str(x).strip()]
                elif isinstance(v, str) and v.strip():
                    subs = [v.strip().lower()]
                else:
                    subs = []
                if subs:
                    self.domain_deny_substrings[key] = subs

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

    @staticmethod
    def _match_domain_pattern(domain: str, pattern: str) -> bool:
        d = (domain or "").lower()
        p = (pattern or "").lower().strip()
        if not d or not p:
            return False
        if p.startswith("*."):
            suf = p[1:]  # ".gov.au"
            return d.endswith(suf) or d == suf.lstrip(".")
        return d == p or d.endswith("." + p)

    def domain_allowed(self, domain: str) -> bool:
        d = (domain or "").lower()
        if not d:
            return False

        if self.allow_domains:
            if any(self._match_domain_pattern(d, a) for a in self.allow_domains):
                return True

            # Auto-allow a small set of trusted public domains so that an overly narrow allowlist
            # does not collapse the pool (still subject to deny rules).
            if AUTO_ALLOW_GOV_AU and (d.endswith(".gov.au") or d.endswith(".edu.au")):
                return True
            if any(self._match_domain_pattern(d, a) for a in AUTO_ALLOW_DOMAINS):
                return True

            return False

        return True

    def domain_denied(self, domain: str) -> bool:
        d = (domain or "").lower()
        return any(self._match_domain_pattern(d, x) for x in self.deny_domains)

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

    # Per-domain URL substring denylists (filters.yaml)
    for dom_pat, subs in (flt.domain_deny_substrings or {}).items():
        if flt._match_domain_pattern(domain, dom_pat):
            for ss in subs:
                if ss and ss in u:
                    return False, "domain_deny_substring"

    for ss in flt.deny_url_substrings:
        if ss and ss in u:
            return False, "deny_url_substring"

    for rx in flt.deny_title_regex:
        if rx.search(title):
            return False, "deny_title_regex"

    
    # Reject known evergreen program/listing pages that pollute monthly digests.
    # (soft rules with explicit reasons, for traceability)
    if domain.endswith("arena.gov.au") and _item_is_undated(it):
        if any(s in u for s in ("/funding", "/opportunities", "/programs", "/initiative", "/grants")):
            return False, "evergreen_program_page"
    if domain.endswith("efrag.org") and (urlparse(url.lower()).path.rstrip("/") in ("/en/news-and-calendar/news", "/en/news-and-calendar/events")):
        return False, "hub_url"

    # Generic / non-informative titles should not be selected even if URL looks OK.
    if title.strip().lower() in ("read more", "news", "media release", "press release"):
        return False, "generic_title"

    return True, ""


def _keyword_boost(title: str, section: str, flt: Filters) -> float:
    kws = flt.section_keywords.get(section, [])
    if not kws:
        return 0.0
    t = title.lower()
    hits = sum(1 for k in kws if k and k in t)
    return min(1.0, hits * 0.15)



def _kw_hits(text: str, kws: Sequence[str]) -> int:
    t = (text or "").lower()
    if not t or not kws:
        return 0
    hits = 0
    for w in kws:
        ww = (w or "").strip().lower()
        if not ww:
            continue
        if ww in t:
            hits += 1
    return hits


def _title_quality_penalty(title: str) -> float:
    tl = (title or "").strip().lower()
    if not tl:
        return -1.0
    if tl in {"read more", "news", "media release", "press release"}:
        return -1.0
    # meeting/webinar/event style titles (penalise, not hard-drop)
    if any(k in tl for k in ("meeting", "webinar", "workshop", "agenda", "minutes", "calendar", "event")):
        return -0.6
    # overly short / non-descriptive
    if len(tl) < 12:
        return -0.4
    return 0.0


def _url_type_penalty(url: str) -> float:
    ul = (url or "").lower()
    if not ul:
        return -1.0
    parsed = urlparse(ul)
    q = parse_qs(parsed.query or "")
    # listing-like query facets/pagination
    if any(k.startswith("f[") for k in q.keys()) or any(k in q for k in ("facet", "facets", "filter", "filters", "page")):
        return -0.5
    path = parsed.path or "/"
    # known non-article paths
    if any(seg in path for seg in ("/events", "/event", "/webinars", "/webinar", "/calendar")):
        return -0.6
    if is_probably_taxonomy_or_hub(url):
        return -0.8
    # weak signal: top-level /news index
    if path.rstrip("/") in ("/news", "/media", "/press", "/updates"):
        return -0.3
    return 0.0


def _recency_score(ts: Optional[datetime], start_dt: datetime, end_dt: datetime) -> float:
    if ts is None:
        return -0.25  # undated penalty (soft)
    # Prefer items within the month; allow padded window but discount it.
    if start_dt <= ts <= end_dt:
        days_from_end = (end_dt - ts).total_seconds() / 86400.0
        # within-month: 0.8..0.2 roughly over a month
        return max(0.2, 0.8 - (days_from_end / 45.0))
    # padded window (should be rare): lower score
    days = abs((ts - end_dt).total_seconds()) / 86400.0
    return max(-0.4, 0.15 - (days / 60.0))


def _text_signal(text: str) -> float:
    t = (text or "").strip()
    if not t:
        return -0.8
    n = len(t)
    # log-like growth; cap at ~1.0
    sig = min(1.0, math.log(max(50, n), 10))
    # reward presence of numbers/units (often indicates substance)
    if re.search(r"\b\d{2,}\b", t):
        sig += 0.15
    if re.search(r"\b(MW|GW|MWh|GWh|A\$|€|USD|AUD|%|\btonnes?\b|\btCO2e\b)\b", t, re.I):
        sig += 0.15
    return sig


def _pre_score(it: Item, section: str, flt: Filters, start_dt: datetime, end_dt: datetime) -> float:
    # Cheap score for deciding fetch budget.
    title = (it.title or "")
    url = (it.url or "")
    ts = _effective_published_ts(it)
    rec = _recency_score(ts, start_dt, end_dt)
    prio = 0.35 if _is_priority(url) else 0.0
    kw = 0.05 * _kw_hits(title + " " + url, flt.section_keywords.get(section, []))
    tq = _title_quality_penalty(title)
    ut = _url_type_penalty(url)
    return rec + prio + kw + tq + ut


def _score_item(it: Item, text: str, section: str, flt: Filters, start_dt: datetime, end_dt: datetime) -> Tuple[float, Dict[str, Any]]:
    url = it.url or ""
    title = it.title or ""
    ts = _effective_published_ts(it)

    rec = _recency_score(ts, start_dt, end_dt)
    prio = 0.35 if _is_priority(url) else 0.0
    kw_hits = _kw_hits((title or "") + " " + (text or "") + " " + (url or ""), flt.section_keywords.get(section, []))
    kw = min(0.6, 0.06 * kw_hits)  # cap
    tq = _title_quality_penalty(title)
    ut = _url_type_penalty(url)
    sig = _text_signal(text)
    agg = -0.45 if "news.google.com" in (url or "").lower() else 0.0

    total = rec + prio + kw + tq + ut + sig + agg
    meta = {
        "recency": rec,
        "priority": prio,
        "kw_hits": kw_hits,
        "kw": kw,
        "title_q": tq,
        "url_t": ut,
        "signal": sig,
        "agg": agg,
        "text_chars": len((text or "").strip()),
        "published_ts": ts.timestamp() if ts else None,
    }
    return total, meta
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
    Score-driven selector (deterministic):
    - Filters first (deny lists, hub URLs, allowlist unless bypassed)
    - Enforces date window (unless ALLOW_UNDATED)
    - Uses a bounded fetch budget to retrieve full text for top pre-scored candidates
    - Scores candidates and selects greedily by score while respecting PER_DOMAIN_CAP and dedupe

    strict=True: enforces MIN_TEXT_CHARS / PRIORITY_MIN_CHARS via _substance_ok()
    strict=False: enforces RELAXED_MIN_TEXT_CHARS via _substance_ok_relaxed()
    """
    drops: List[Dict[str, str]] = []
    selected: List[Item] = []
    per_domain: Dict[str, int] = dict(initial_per_domain or {})
    ex: Set[str] = set((u or "").strip().lower() for u in (exclude_urls or set()) if str(u).strip())
    text_cache: Dict[str, str] = {}
    seen_keys: Set[str] = set()

    window_start = start_dt - timedelta(days=max(0, RANGE_PAD_BEFORE_DAYS))
    window_end = end_dt + timedelta(days=max(0, RANGE_PAD_AFTER_DAYS))

    # 1) Filter + pre-score
    cand: List[Tuple[float, str, Item]] = []
    for it in pool:
        url = (it.url or "").strip()
        if not url:
            drops.append({"reason": "missing_url", "url": "", "title": it.title or ""})
            continue
        ul = url.lower()
        if ul in ex:
            continue

        ok, why = _passes_filters(it, flt, section, bypass_allow=bypass_allow)
        if not ok:
            drops.append({"reason": why, "url": url, "title": it.title or ""})
            continue

        ts_eff = _effective_published_ts(it)
        if ts_eff is None and (not ALLOW_UNDATED):
            drops.append({"reason": "undated", "url": url, "title": it.title or ""})
            continue
        if ts_eff is not None and (not _in_range(ts_eff, window_start, window_end)):
            drops.append({"reason": "out_of_range", "url": url, "title": it.title or ""})
            continue

        ps = _pre_score(it, section, flt, start_dt, end_dt)
        cand.append((ps, ul, it))

    # deterministic ordering: score desc, url asc
    cand.sort(key=lambda x: (-x[0], x[1]))

    # 2) Fetch budget: attempt full text for top candidates (only if needed)
    budget = max(0, MAX_SCORE_FETCHES_PER_SECTION)
    to_fetch = cand[:budget]
    for _, _, it in to_fetch:
        url = (it.url or "").strip()
        if not url:
            continue
        if url in text_cache:
            continue
        # If we already have a reasonable summary, we may skip fetch unless strict.
        base_text = (it.summary or "").strip()
        need_fetch = strict or (len(base_text) < max(200, RELAXED_MIN_TEXT_CHARS))
        text = base_text
        if need_fetch:
            try:
                text = (fetch_full_text(url) or "").strip()
            except Exception:
                text = ""
            if not text:
                text = base_text
        text_cache[url] = text

    # 3) Full scoring
    scored: List[Tuple[float, str, Item, Dict[str, Any], str]] = []
    for _, ul, it in cand:
        url = (it.url or "").strip()
        text = text_cache.get(url, "") or (it.summary or "")
        text = (text or "").strip()

        # substance gates (still deterministic and section-agnostic)
        if strict:
            if not _substance_ok(text, _is_priority(url)):
                drops.append({"reason": "low_substance", "url": url, "title": it.title or ""})
                continue
        else:
            if not _substance_ok_relaxed(text):
                drops.append({"reason": "low_substance_relaxed", "url": url, "title": it.title or ""})
                continue

        sc, meta = _score_item(it, text, section, flt, start_dt, end_dt)

        # Stable dedupe key: domain + published day + normalised title (fallback to url)
        dt = _effective_published_ts(it)
        day = dt.strftime("%Y-%m-%d") if dt else "undated"
        key = f"{normalise_domain(url)}|{day}|{_norm_title(it.title or '')}"
        scored.append((sc, ul, it, meta, key))

    scored.sort(key=lambda x: (-x[0], x[1]))

    # 4) Greedy pick by score with caps + dedupe
    for sc, ul, it, meta, key in scored:
        url = (it.url or "").strip()
        if not url:
            continue
        domain = normalise_domain(url)
        if per_domain.get(domain, 0) >= per_domain_cap:
            drops.append({"reason": "per_domain_cap", "url": url, "title": it.title or "", "domain": domain})
            continue
        if key in seen_keys:
            drops.append({"reason": "dedup_key", "url": url, "title": it.title or ""})
            continue

        # attach score/meta for debug writer
        setattr(it, "_score", float(sc))
        setattr(it, "_score_meta", meta)
        setattr(it, "_used_text_chars", meta.get("text_chars"))

        # keep the best available text as summary for downstream digest
        text = text_cache.get(url, "") or (it.summary or "")
        it.summary = (text or "").strip()

        selected.append(it)
        per_domain[domain] = per_domain.get(domain, 0) + 1
        seen_keys.add(key)
        ex.add(ul)

        if len(selected) >= max(0, items_needed):
            break

    return selected, drops


def _last_resort_pick(
    pool: Sequence[Item],
    section: str,
    flt: Filters,
    *,
    start_dt: datetime,
    end_dt: datetime,
    items_needed: int,
) -> Tuple[List[Item], List[Dict[str, str]]]:
    """
    Last-resort picker used only when strict+relaxed yield zero for a section.

    It still:
    - avoids hub URLs and deny patterns
    - enforces a bounded *backfill* window (prefer earlier, not future)
    - enforces relaxed substance

    Goal: avoid 'selected=0' while not pulling obvious garbage.
    """
    drops: List[Dict[str, str]] = []
    scored: List[Tuple[float, str, Item, Dict[str, Any]]] = []

    backfill_start = start_dt - timedelta(days=max(0, LAST_RESORT_BACKFILL_DAYS))
    backfill_end = end_dt  # do not go into the future

    fetches = 0

    for it in pool:
        url = (it.url or "").strip()
        if not url:
            continue
        ul = url.lower()

        ok, why = _passes_filters(it, flt, section, bypass_allow=True)
        if not ok:
            drops.append({"reason": why, "url": url, "title": it.title or ""})
            continue

        ts_eff = _effective_published_ts(it)
        if ts_eff is not None:
            # avoid extremely stale content in last resort
            if (end_dt - ts_eff).total_seconds() / 86400.0 > max(0, LAST_RESORT_MAX_STALENESS_DAYS) and (not _is_priority(url)):
                drops.append({"reason": "too_stale_last_resort", "url": url, "title": it.title or ""})
                continue
            if not _in_range(ts_eff, backfill_start, backfill_end):
                drops.append({"reason": "out_of_range_last_resort", "url": url, "title": it.title or ""})
                continue
        elif not ALLOW_UNDATED:
            drops.append({"reason": "undated_last_resort", "url": url, "title": it.title or ""})
            continue

        text = (it.summary or "").strip()
        if (len(text) < max(150, RELAXED_MIN_TEXT_CHARS)) and fetches < max(0, LAST_RESORT_MAX_FETCHES):
            fetches += 1
            try:
                ft = (fetch_full_text(url) or "").strip()
            except Exception:
                ft = ""
            if ft:
                text = ft

        if not _substance_ok_relaxed(text):
            drops.append({"reason": "low_substance_last_resort", "url": url, "title": it.title or ""})
            continue

        sc, meta = _score_item(it, text, section, flt, backfill_start, backfill_end)
        meta["last_resort"] = True
        scored.append((sc, ul, it, meta))

    scored.sort(key=lambda x: (-x[0], x[1]))
    picked: List[Item] = []
    seen: Set[str] = set()
    per_dom: Dict[str, int] = {}

    for sc, ul, it, meta in scored:
        url = (it.url or "").strip()
        dom = normalise_domain(url)
        if per_dom.get(dom, 0) >= PER_DOMAIN_CAP:
            continue
        k = f"{dom}|{_norm_title(it.title or '')}|{_effective_published_ts(it).strftime('%Y-%m-%d') if _effective_published_ts(it) else 'undated'}"
        if k in seen:
            continue
        setattr(it, "_score", float(sc))
        setattr(it, "_score_meta", meta)
        it.summary = (it.summary or "").strip()
        picked.append(it)
        seen.add(k)
        per_dom[dom] = per_dom.get(dom, 0) + 1
        if len(picked) >= max(1, items_needed):
            break

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
        for it in selected:
            if it.url:
                global_used_urls.add(it.url.strip().lower())

        # Pass 2 (relaxed fill): only to top up to quota, and still enforces minimum substance
        if len(selected) < ITEMS_PER_SECTION:
            used = {it.url.lower() for it in selected if it.url}
            per_dom = Counter(normalise_domain(it.url) for it in selected if it.url)
            filler, drops2 = _select_from_pool(
                pool, section, start_dt, end_dt, flt,
                items_needed=(ITEMS_PER_SECTION - len(selected)),
                per_domain_cap=PER_DOMAIN_CAP,
                strict=False,
                exclude_urls=global_used_urls,
                initial_per_domain=dict(per_dom),
            )
            all_drops.extend(drops2)
            selected.extend(filler)
            for it in filler:
                if it.url:
                    global_used_urls.add(it.url.strip().lower())

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
            for it in selected3:
                if it.url:
                    global_used_urls.add(it.url.strip().lower())

        # Last resort: pick only content-like URLs with minimal extract
        if not selected:
            print("[warn] Still no items after fallback; last-resort pick (bounded backfill, no future).")
            picked, drops4 = _last_resort_pick(pool, section, flt, start_dt=start_dt, end_dt=end_dt, items_needed=ITEMS_PER_SECTION)
            all_drops.extend(drops4)
            picked2: List[Item] = []
            for it in picked:
                u2 = (it.url or "").strip().lower()
                if u2 and u2 in global_used_urls:
                    continue
                picked2.append(it)
                if u2:
                    global_used_urls.add(u2)
            selected = picked2

        # Emergency RSS only if nothing at all
        if not selected:
            print("[warn] No candidates available; trying emergency RSS.")
            epool = _emergency_pool(section)
            picked, drops5 = _last_resort_pick(epool, section, flt, start_dt=start_dt, end_dt=end_dt, items_needed=max(1, ITEMS_PER_SECTION // 2))
            all_drops.extend(drops5)
            picked2: List[Item] = []
            for it in picked:
                u2 = (it.url or "").strip().lower()
                if u2 and u2 in global_used_urls:
                    continue
                picked2.append(it)
                if u2:
                    global_used_urls.add(u2)
            selected = picked2

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
    # MIN_TOTAL_ITEMS guard: never fabricate items. Only hard-fail if explicitly configured.
    below_min_total = len(all_selected) < MIN_TOTAL_ITEMS
    if below_min_total:
        all_drops.append({"reason": "below_min_total", "url": "", "title": f"selected={len(all_selected)} < MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS}"})
        if FAIL_ON_BELOW_MIN_TOTAL:
            raise SystemExit(f"ERROR: selected items is {len(all_selected)} but MIN_TOTAL_ITEMS={MIN_TOTAL_ITEMS}")

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
f"fail_on_below_min_total={FAIL_ON_BELOW_MIN_TOTAL}",
f"range_pad_before_days={RANGE_PAD_BEFORE_DAYS}",
f"range_pad_after_days={RANGE_PAD_AFTER_DAYS}",
f"max_score_fetches_per_section={MAX_SCORE_FETCHES_PER_SECTION}",
f"last_resort_backfill_days={LAST_RESORT_BACKFILL_DAYS}",
f"last_resort_max_staleness_days={LAST_RESORT_MAX_STALENESS_DAYS}",
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
