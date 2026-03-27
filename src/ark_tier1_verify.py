# -*- coding: utf-8 -*-
"""
ARK Tier-1 Baseline Verifier
─────────────────────────────
Reads config/ark-baseline.yaml, fetches the source_url for every
stability: dynamic entry, and records whether each URL is still live and
what content was found.

Output:  state/ark-tier1-verify-{YM}.json

Env vars:
    CFG_BASELINE=config/ark-baseline.yaml
    TIER1_RESULTS_FILE=state/ark-tier1-verify-2026-03.json
    TIER1_TIMEOUT=20          # seconds per fetch
    TIER1_MAX_CHARS=2000      # chars of extracted text to retain per entry
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
import yaml

try:
    import trafilatura
    _HAS_TRAFILATURA = True
except ImportError:
    _HAS_TRAFILATURA = False

# ── Config ───────────────────────────────────────────────────────────────────
CFG_BASELINE      = os.getenv("CFG_BASELINE",      "config/ark-baseline.yaml")
TIER1_RESULTS_FILE = os.getenv("TIER1_RESULTS_FILE", "")
TIER1_TIMEOUT     = int(os.getenv("TIER1_TIMEOUT",  "20"))
TIER1_MAX_CHARS   = int(os.getenv("TIER1_MAX_CHARS", "2000"))
YM                = os.getenv("START_YM", os.getenv("YM", "")).strip()

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; GGAdvisoryBot/1.0; "
        "+https://gg-advisory.org/bot)"
    )
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _fetch_url(url: str) -> Dict[str, Any]:
    """Fetch a URL and return a result dict."""
    result: Dict[str, Any] = {
        "url": url,
        "fetch_date": datetime.now(timezone.utc).isoformat(),
        "fetch_status": "error",
        "http_status": None,
        "content_sample": "",
        "error": None,
    }
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=TIER1_TIMEOUT,
                            allow_redirects=True)
        result["http_status"] = resp.status_code
        if resp.status_code == 404:
            result["fetch_status"] = "not_found"
            result["error"] = "HTTP 404"
            return result
        if resp.status_code >= 400:
            result["fetch_status"] = "error"
            result["error"] = f"HTTP {resp.status_code}"
            return result

        # Try trafilatura for clean text extraction
        text = ""
        if _HAS_TRAFILATURA:
            text = trafilatura.extract(resp.text) or ""
        if not text:
            # Fallback: strip HTML tags crudely
            import re
            text = re.sub(r"<[^>]+>", " ", resp.text)
            text = re.sub(r"\s+", " ", text).strip()

        result["content_sample"] = text[:TIER1_MAX_CHARS]
        result["fetch_status"] = "ok"
    except requests.exceptions.Timeout:
        result["fetch_status"] = "timeout"
        result["error"] = f"Timed out after {TIER1_TIMEOUT}s"
    except Exception as e:
        result["fetch_status"] = "error"
        result["error"] = str(e)[:200]
    return result


def _load_baseline(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _collect_dynamic_entries(baseline: Dict) -> List[Dict[str, Any]]:
    """Return all stability=dynamic entries with a source_url."""
    entries = []
    sections = baseline.get("sections", {})
    for section_key, section in sections.items():
        for entry in section.get("entries", []):
            if entry.get("stability") == "dynamic" and entry.get("source_url"):
                entries.append({
                    "id": entry["id"],
                    "label": entry["label"],
                    "section": section_key,
                    "source_url": entry["source_url"],
                    "status": entry.get("status", "active"),
                })
    return entries


# ── Main ─────────────────────────────────────────────────────────────────────

def run() -> None:
    baseline = _load_baseline(CFG_BASELINE)
    dynamic_entries = _collect_dynamic_entries(baseline)

    if not dynamic_entries:
        print("[ark_tier1_verify] No dynamic entries found — nothing to verify.")
        return

    print(f"[ark_tier1_verify] Verifying {len(dynamic_entries)} dynamic entries...")

    results = []
    seen_urls: Dict[str, Dict] = {}  # deduplicate fetches for shared URLs

    for entry in dynamic_entries:
        url = entry["source_url"]
        print(f"  → {entry['id']} ({entry['label'][:50]})")

        if url in seen_urls:
            fetch_result = dict(seen_urls[url])  # reuse cached fetch
        else:
            time.sleep(1.0)  # polite crawl delay
            fetch_result = _fetch_url(url)
            seen_urls[url] = fetch_result

        results.append({
            "entry_id": entry["id"],
            "label": entry["label"],
            "section": entry["section"],
            "status": entry["status"],
            **fetch_result,
        })

        status_icon = "✓" if fetch_result["fetch_status"] == "ok" else "✗"
        print(f"    {status_icon} {fetch_result['fetch_status']}"
              f" (HTTP {fetch_result.get('http_status', 'n/a')})")

    # Determine output path
    out_file = TIER1_RESULTS_FILE
    if not out_file:
        ym = YM or datetime.now(timezone.utc).strftime("%Y-%m")
        out_file = f"state/ark-tier1-verify-{ym}.json"

    Path(out_file).parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({
            "period": YM,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "baseline_version": baseline.get("meta", {}).get("version", "unknown"),
            "entries_verified": len(results),
            "results": results,
        }, f, ensure_ascii=False, indent=2)

    ok    = sum(1 for r in results if r["fetch_status"] == "ok")
    error = len(results) - ok
    print(f"[ark_tier1_verify] Done: {ok} ok, {error} errors → {out_file}")


if __name__ == "__main__":
    run()
