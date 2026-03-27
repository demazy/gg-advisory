# -*- coding: utf-8 -*-
"""
ARK monthly sources updater.

Runs BEFORE generate_ark.py each month to discover new relevant sources
and produce an augmented `ark-sources-current.yaml` for that run.

Discovery pipeline:
  1. Query Google News RSS for CCUS/industrial decarbonisation terms in AU/APAC.
  2. Collect domains that appear 2+ times across queries.
  3. Filter out domains already in the base sources or in a permanent deny list.
  4. Ask OpenAI to classify each new domain by section relevance.
  5. Merge new domains into the base sources and write `ark-sources-current.yaml`.
  6. The current file is committed monthly so the source list grows over time.

Run:
    python -m src.ark_update_sources

Env vars:
    ARK_SOURCES_BASE   path to base sources YAML  (default: config/ark-sources.yaml)
    ARK_SOURCES_OUT    path to write updated YAML  (default: config/ark-sources-current.yaml)
    OPENAI_API_KEY     for domain classification
    MODEL              OpenAI model (default: gpt-4o)
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests
import yaml

# ── Config ────────────────────────────────────────────────────────────────────

BASE_PATH = Path(os.getenv("ARK_SOURCES_BASE", "config/ark-sources.yaml"))
OUT_PATH  = Path(os.getenv("ARK_SOURCES_OUT",  "config/ark-sources-current.yaml"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o")

# How many times a domain must appear across queries to be a candidate
MIN_APPEARANCES = 2

# Max new domains to evaluate per run (keeps OpenAI call small)
MAX_CANDIDATES = 20

# ── Google News RSS search queries ───────────────────────────────────────────
# Each query targets a different angle of ARK's intelligence needs in AU/APAC.

_QUERIES = [
    # Grants & Funding
    "carbon capture grant Australia",
    "CCUS funding Australia",
    "industrial decarbonisation grant Australia",
    "carbon capture ARENA grant",
    "CCS CEFC Australia",
    # Market & Policy
    "carbon capture Australia policy",
    "CCUS Australia Safeguard Mechanism",
    "industrial emissions policy Australia",
    "CCS regulation Australia",
    "carbon capture APAC",
    # Competitors
    "carbon capture company Australia",
    "CCS project Australia",
    "carbon capture startup APAC",
    "Calix carbon capture",
    "point source carbon capture",
    # Partners & Buyers
    "steel decarbonisation Australia",
    "cement decarbonisation Australia",
    "biogas carbon capture Australia",
    "gas power plant emissions Australia",
    "industrial CO2 Australia",
]

# Domains to always exclude from discovered sources
_PERMANENT_DENY = {
    "news.google.com",
    "google.com",
    "apple.news",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "tiktok.com",
    "prnewswire.com",
    "businesswire.com",
    "globenewswire.com",
    "accesswire.com",
    "prweb.com",
    "marketwatch.com",
    "benzinga.com",
    "finance.yahoo.com",
    "seekingalpha.com",
    "investopedia.com",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _google_news_rss(query: str) -> str:
    q = query.strip().replace(" ", "+")
    return f"https://news.google.com/rss/search?q={q}&hl=en-AU&gl=AU&ceid=AU:en"


def _extract_domain(url: str) -> str:
    """Return bare domain (no www.) from a URL string."""
    try:
        netloc = urlparse(url).netloc.lower()
        return re.sub(r"^www\.", "", netloc)
    except Exception:
        return ""


def _existing_domains(cfg: dict) -> set[str]:
    """Extract all domains already tracked in the base sources config."""
    domains: set[str] = set()
    for section_data in cfg.get("sections", {}).values():
        for feed in section_data.get("rss", []) or []:
            d = _extract_domain(str(feed))
            if d:
                domains.add(d)
        for source in section_data.get("html", []) or []:
            url = source if isinstance(source, str) else (source or {}).get("url", "")
            d = _extract_domain(str(url))
            if d:
                domains.add(d)
    return domains


def _fetch_rss_domains(query: str) -> list[str]:
    """Fetch a Google News RSS feed and return the domains of all entries."""
    url = _google_news_rss(query)
    domains: list[str] = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.get("entries", []):
            link = entry.get("link", "")
            d = _extract_domain(link)
            if d:
                domains.append(d)
    except Exception as e:
        print(f"[ark-sources] RSS fetch failed for '{query}': {e}")
    return domains


def _classify_domains_with_openai(domains: list[str]) -> dict[str, list[str]]:
    """
    Ask OpenAI which section(s) each new domain belongs to.
    Returns {domain: [section_names]} for approved domains only.
    """
    if not OPENAI_API_KEY or not domains:
        return {}

    sections = ["Grants & Funding", "Market & Policy", "Competitors", "Partners & Buyers"]

    prompt = f"""
You are an analyst helping track AU/APAC carbon capture (CCUS/CCS) intelligence for ARK Capture Solutions.

ARK makes modular point-source carbon capture technology for low-concentration industrial flue gases
(biogas plants, gas-fired power, petrochemicals, glass, steel).

The following domains appeared in Google News searches about CCUS, carbon capture, and industrial
decarbonisation in Australia and APAC:

{json.dumps(domains, indent=2)}

For each domain that is genuinely relevant to ARK's intelligence needs, assign it to one or more
of these sections:
- "Grants & Funding": government funders, grant programs, innovation bodies, clean energy agencies
- "Market & Policy": policy analysis sites, government regulators, research bodies, CCUS think tanks
- "Competitors": carbon capture companies, cleantech startups, CCUS project operators
- "Partners & Buyers": industrial company newsrooms (steel, glass, cement, biogas, gas power, petrochemicals)

Exclude:
- General news aggregators (news.google.com, etc.)
- Social media (linkedin.com, twitter.com, facebook.com, etc.)
- PR distribution services (prnewswire.com, businesswire.com, etc.)
- General finance/investment sites without CCUS focus
- Paywalled sites with no free news content
- Domains unrelated to CCUS or AU/APAC industrial decarbonisation

Return ONLY a JSON object — no other text:
{{"domain.com": ["Section Name"], "other.com.au": ["Section1", "Section2"]}}

If none of the domains are relevant, return {{}}.
"""

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 600,
            },
            timeout=45,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            result = json.loads(match.group())
            # Validate: only keep entries with valid section names
            valid: dict[str, list[str]] = {}
            valid_sections = set(sections)
            for domain, assigned_sections in result.items():
                filtered = [s for s in (assigned_sections or []) if s in valid_sections]
                if filtered:
                    valid[domain] = filtered
            return valid
    except Exception as e:
        print(f"[ark-sources] OpenAI classification failed: {e}")
    return {}


def _url_for_domain(domain: str) -> str:
    """Construct a best-guess news index URL for a discovered domain."""
    for path in ("/news", "/news-and-media", "/media", "/newsroom", "/media-releases", ""):
        return f"https://{domain}{path}"
    return f"https://{domain}"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"[ark-sources] Loading base sources from {BASE_PATH}")
    if not BASE_PATH.exists():
        print(f"[ark-sources] ERROR: base file not found: {BASE_PATH}")
        raise SystemExit(1)

    base_cfg = yaml.safe_load(BASE_PATH.read_text(encoding="utf-8")) or {}
    existing = _existing_domains(base_cfg)
    print(f"[ark-sources] Found {len(existing)} existing tracked domains.")

    # ── Step 1: Collect domains from Google News RSS ──────────────────────────
    print(f"[ark-sources] Querying Google News RSS ({len(_QUERIES)} queries)…")
    counter: Counter[str] = Counter()
    for query in _QUERIES:
        domains = _fetch_rss_domains(query)
        counter.update(domains)
        time.sleep(0.3)   # be polite

    print(f"[ark-sources] Collected {len(counter)} distinct domains from RSS.")

    # ── Step 2: Filter to candidates ─────────────────────────────────────────
    candidates = [
        d for d, count in counter.most_common(MAX_CANDIDATES * 2)
        if count >= MIN_APPEARANCES
        and d not in existing
        and d not in _PERMANENT_DENY
        and "." in d   # skip bare hostnames
    ][:MAX_CANDIDATES]

    print(f"[ark-sources] {len(candidates)} new candidate domains: {candidates}")

    if not candidates:
        print("[ark-sources] No new candidates — copying base to current.")
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(BASE_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        return

    # ── Step 3: Classify with OpenAI ─────────────────────────────────────────
    classified = _classify_domains_with_openai(candidates)
    print(f"[ark-sources] OpenAI approved {len(classified)} new domains: {list(classified)}")

    if not classified:
        print("[ark-sources] No new domains approved — copying base to current.")
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(BASE_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        return

    # ── Step 4: Merge into a working copy of the config ──────────────────────
    # Deep-copy the sections to avoid mutating the base dict
    import copy
    cfg = copy.deepcopy(base_cfg)
    sections_cfg = cfg.setdefault("sections", {})

    added: list[str] = []
    for domain, assigned_sections in classified.items():
        url = _url_for_domain(domain)
        for section_name in assigned_sections:
            if section_name not in sections_cfg:
                sections_cfg[section_name] = {"rss": [], "html": []}
            html_list = sections_cfg[section_name].setdefault("html", [])
            # Avoid duplicates (check both str and dict entries)
            already_there = any(
                (isinstance(s, str) and domain in s)
                or (isinstance(s, dict) and domain in s.get("url", ""))
                for s in html_list
            )
            if not already_there:
                html_list.append(url)
                added.append(f"{domain} → {section_name}")

    print(f"[ark-sources] Added {len(added)} new source entries: {added}")

    # ── Step 5: Write augmented sources file ─────────────────────────────────
    header_comment = (
        f"# ark-sources-current.yaml\n"
        f"# Auto-generated by ark_update_sources.py\n"
        f"# Base: {BASE_PATH}  |  Discovered additions: {len(classified)}\n"
        f"# New domains: {', '.join(classified.keys()) or 'none'}\n\n"
    )
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(
        header_comment + yaml.dump(cfg, allow_unicode=True, default_flow_style=False),
        encoding="utf-8",
    )
    print(f"[ark-sources] Written: {OUT_PATH}  ({len(added)} new entries added)")


if __name__ == "__main__":
    main()
