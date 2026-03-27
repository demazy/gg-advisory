# -*- coding: utf-8 -*-
"""
ARK Baseline Delta Applicator
──────────────────────────────
Reads the BASELINE_DELTA JSON block from the monthly digest and applies
confidence-gated changes to config/ark-baseline.yaml.

Confidence gates:
  >= 0.85  → auto-apply to baseline (status unchanged; changelog entry added)
  0.60–0.85 → apply as new status: draft (shown in internal review report)
  < 0.60   → flag only — not applied; surfaced in review report

Outputs:
  - Updated config/ark-baseline.yaml (in-place)
  - state/ark-delta-log-{YM}.json  (log of all changes made/flagged)

Env vars:
    CFG_BASELINE=config/ark-baseline.yaml
    DIGEST_FILE=out/ark/monthly-digest-{YM}.md
    DELTA_LOG_FILE=state/ark-delta-log-{YM}.json
    YM=2026-03
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

CFG_BASELINE   = os.getenv("CFG_BASELINE",  "config/ark-baseline.yaml")
DIGEST_FILE    = os.getenv("DIGEST_FILE",   "")
DELTA_LOG_FILE = os.getenv("DELTA_LOG_FILE","")
YM             = os.getenv("YM", os.getenv("START_YM", "")).strip()

AUTO_APPLY_MIN   = 0.85
DRAFT_GATE_MIN   = 0.60

_TODAY = date.today().isoformat()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_yaml(path: str) -> Dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _save_yaml(path: str, data: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False,
                  default_flow_style=False, width=120)


def _extract_baseline_delta(digest_text: str) -> Optional[Dict]:
    m = re.search(
        r"---BASELINE_DELTA_START---\s*(.*?)\s*---BASELINE_DELTA_END---",
        digest_text,
        re.DOTALL,
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception as e:
        print(f"[ark_apply_delta] Could not parse BASELINE_DELTA JSON: {e}")
        return None


def _find_entry(baseline: Dict, entry_id: str, section_key: str) -> Optional[Dict]:
    """Find a baseline entry by ID (or by section if ID is empty)."""
    section = baseline.get("sections", {}).get(section_key, {})
    for entry in section.get("entries", []):
        if entry.get("id") == entry_id:
            return entry
    return None


def _make_changelog_entry(delta_item: Dict, action_taken: str, period: str) -> Dict:
    return {
        "date":         _TODAY,
        "period":       period,
        "action":       action_taken,
        "description":  delta_item.get("description", "")[:300],
        "change_type":  delta_item.get("change_type", "other"),
        "confidence":   delta_item.get("confidence", 0.0),
        "source_url":   delta_item.get("source_url", ""),
    }


def _apply_update_bullet(entry: Dict, delta_item: Dict, period: str) -> str:
    """Replace a bullet that contains similar text with the new bullet."""
    new_bullet = delta_item.get("current_bullet", "").strip()
    if not new_bullet:
        return "skipped_empty_bullet"
    # Try to find an existing bullet to replace based on content similarity
    # Use the first 40 chars of the new bullet as a match key
    match_prefix = new_bullet[:40].lower()
    bullets: List[str] = entry.get("bullets", [])
    replaced = False
    for i, b in enumerate(bullets):
        if b[:40].lower() == match_prefix or delta_item.get("change_type") == "price_update":
            # For price updates, replace the first matching price-pattern bullet
            bullets[i] = new_bullet
            replaced = True
            break
    if not replaced:
        # Append as a new bullet if no match found
        bullets.append(new_bullet)
    entry["bullets"] = bullets
    entry["last_verified"] = _TODAY
    entry.setdefault("changelog", []).append(
        _make_changelog_entry(delta_item, "bullet_updated", period)
    )
    return "applied"


def _apply_add_bullet(entry: Dict, delta_item: Dict, period: str) -> str:
    """Add a new bullet to an existing entry."""
    new_bullet = delta_item.get("current_bullet", "").strip()
    if not new_bullet:
        return "skipped_empty_bullet"
    entry.setdefault("bullets", []).append(new_bullet)
    entry["last_verified"] = _TODAY
    entry.setdefault("changelog", []).append(
        _make_changelog_entry(delta_item, "bullet_added", period)
    )
    return "applied"


def _apply_add_entry(baseline: Dict, delta_item: Dict, period: str, as_draft: bool = True) -> str:
    """Add a new entry to the baseline section (always as draft)."""
    section_key = delta_item.get("section", "")
    if section_key not in baseline.get("sections", {}):
        return "skipped_unknown_section"
    section = baseline["sections"][section_key]

    label = delta_item.get("new_entry_label") or delta_item.get("description", "")[:80]
    if not label:
        return "skipped_no_label"

    # Generate a unique ID
    existing_ids = {e.get("id", "") for e in section.get("entries", [])}
    prefix_map = {
        "grants_funding": "gf",
        "market_policy":  "mp",
        "competitors":    "co",
        "partners_buyers":"pb",
    }
    prefix = prefix_map.get(section_key, "xx")
    for i in range(1, 100):
        candidate = f"{prefix}-{i:03d}"
        if candidate not in existing_ids:
            new_id = candidate
            break
    else:
        return "skipped_no_id_available"

    new_entry: Dict = {
        "id":            new_id,
        "label":         label,
        "stability":     "dynamic",
        "status":        "draft",   # always draft for new entries
        "tag":           None,
        "tag_color":     None,
        "last_verified": _TODAY,
        "source_url":    delta_item.get("source_url", ""),
        "changelog": [_make_changelog_entry(delta_item, "entry_added_as_draft", period)],
        "bullets": [delta_item.get("current_bullet", "").strip()],
    }
    section.setdefault("entries", []).append(new_entry)
    return f"draft_added:{new_id}"


def _apply_deprecate_entry(entry: Dict, delta_item: Dict, period: str) -> str:
    """Mark an entry as deprecated."""
    entry["status"] = "deprecated"
    entry["last_verified"] = _TODAY
    entry.setdefault("changelog", []).append(
        _make_changelog_entry(delta_item, "deprecated", period)
    )
    return "deprecated"


def _apply_flag_contradiction(entry: Dict, delta_item: Dict, period: str) -> str:
    """Add a contradiction flag to a stable entry (surfaced in review report only)."""
    entry.setdefault("contradiction_flags", []).append({
        "date":        _TODAY,
        "period":      period,
        "description": delta_item.get("description", "")[:300],
        "source_url":  delta_item.get("source_url", ""),
        "confidence":  delta_item.get("confidence", 0.0),
    })
    return "flagged"


# ── Main ─────────────────────────────────────────────────────────────────────

def run() -> None:
    # Resolve DIGEST_FILE
    digest_file = DIGEST_FILE
    if not digest_file:
        ym = YM or datetime.now(timezone.utc).strftime("%Y-%m")
        digest_file = f"out/ark/monthly-digest-{ym}.md"

    digest_path = Path(digest_file)
    if not digest_path.exists():
        print(f"[ark_apply_delta] Digest not found: {digest_file} — nothing to apply.")
        sys.exit(0)

    digest_text = digest_path.read_text(encoding="utf-8")
    delta = _extract_baseline_delta(digest_text)

    if not delta:
        print("[ark_apply_delta] No BASELINE_DELTA block found in digest — nothing to apply.")
        # Write empty log
        _write_log([], YM, digest_file, "no_delta_block")
        sys.exit(0)

    delta_items = delta.get("items", [])
    period = delta.get("period", YM)

    if not delta_items:
        print(f"[ark_apply_delta] BASELINE_DELTA contains 0 items for period {period}.")
        _write_log([], period, digest_file, "empty_delta")
        sys.exit(0)

    print(f"[ark_apply_delta] Processing {len(delta_items)} delta item(s) for {period}...")

    baseline = _load_yaml(CFG_BASELINE)
    log: List[Dict] = []

    for item in delta_items:
        entry_id   = item.get("entry_id", "")
        section_key= item.get("section", "")
        action     = item.get("action", "")
        confidence = float(item.get("confidence", 0.0))

        log_entry: Dict = {
            "entry_id":    entry_id,
            "section":     section_key,
            "action":      action,
            "confidence":  confidence,
            "change_type": item.get("change_type", "other"),
            "description": item.get("description", "")[:300],
            "source_url":  item.get("source_url", ""),
            "result":      None,
            "gate":        None,
        }

        # Determine gate
        if confidence >= AUTO_APPLY_MIN:
            gate = "auto_apply"
        elif confidence >= DRAFT_GATE_MIN:
            gate = "draft"
        else:
            gate = "flag_only"

        log_entry["gate"] = gate

        # Always flag contradictions regardless of gate
        if action == "flag_contradiction":
            if entry_id:
                entry = _find_entry(baseline, entry_id, section_key)
                if entry:
                    result = _apply_flag_contradiction(entry, item, period)
                    log_entry["result"] = result
                else:
                    log_entry["result"] = "skipped_entry_not_found"
            else:
                log_entry["result"] = "skipped_no_entry_id"
            log.append(log_entry)
            print(f"  [flag] {entry_id or '?'} — {item.get('description','')[:60]}")
            continue

        # Skip below draft gate
        if gate == "flag_only":
            log_entry["result"] = "flagged_only"
            log.append(log_entry)
            print(f"  [skip/low confidence {confidence:.2f}] {entry_id or '?'} — {item.get('description','')[:60]}")
            continue

        # Apply
        if action == "add_entry":
            result = _apply_add_entry(baseline, item, period, as_draft=True)
            log_entry["result"] = result

        elif action in ("update_bullet", "add_bullet", "deprecate_entry"):
            if not entry_id:
                log_entry["result"] = "skipped_no_entry_id"
                log.append(log_entry)
                continue

            entry = _find_entry(baseline, entry_id, section_key)
            if not entry:
                log_entry["result"] = "skipped_entry_not_found"
                log.append(log_entry)
                print(f"  [skip/not found] {entry_id}")
                continue

            # Draft gate: mark as draft if confidence is 0.60–0.85
            if gate == "draft" and action != "deprecate_entry":
                entry.setdefault("pending_updates", []).append({
                    "date":       _TODAY,
                    "period":     period,
                    "action":     action,
                    "bullet":     item.get("current_bullet", ""),
                    "confidence": confidence,
                    "source_url": item.get("source_url", ""),
                })
                entry.setdefault("changelog", []).append(
                    _make_changelog_entry(item, f"draft_{action}", period)
                )
                log_entry["result"] = "draft_pending_review"
            else:
                # Auto-apply
                if action == "update_bullet":
                    result = _apply_update_bullet(entry, item, period)
                elif action == "add_bullet":
                    result = _apply_add_bullet(entry, item, period)
                elif action == "deprecate_entry":
                    result = _apply_deprecate_entry(entry, item, period)
                else:
                    result = "unknown_action"
                log_entry["result"] = result
        else:
            log_entry["result"] = f"unknown_action:{action}"

        gate_icon = "✓" if gate == "auto_apply" else "~"
        print(f"  [{gate_icon} {confidence:.2f}] {action} {entry_id or 'new'} → "
              f"{log_entry['result']} ({item.get('description','')[:50]})")
        log.append(log_entry)

    # Save updated baseline
    _save_yaml(CFG_BASELINE, baseline)
    print(f"[ark_apply_delta] Saved updated baseline: {CFG_BASELINE}")

    _write_log(log, period, digest_file, "ok")


def _write_log(log: List[Dict], period: str, digest_file: str, status: str) -> None:
    out_file = DELTA_LOG_FILE
    if not out_file:
        ym = period or YM or datetime.now(timezone.utc).strftime("%Y-%m")
        out_file = f"state/ark-delta-log-{ym}.json"
    Path(out_file).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "period":       period,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "digest_file":  digest_file,
        "status":       status,
        "changes":      log,
        "summary": {
            "auto_applied":      sum(1 for c in log if c.get("gate") == "auto_apply" and c.get("result") not in ("skipped_entry_not_found", "skipped_no_entry_id")),
            "draft_pending":     sum(1 for c in log if c.get("result") in ("draft_pending_review", "draft_added")),
            "flagged":           sum(1 for c in log if c.get("gate") == "flag_only" or c.get("result") == "flagged"),
            "skipped":           sum(1 for c in log if (c.get("result") or "").startswith("skipped")),
        },
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[ark_apply_delta] Delta log saved: {out_file}")
    s = payload["summary"]
    print(f"  Applied: {s['auto_applied']} | Draft: {s['draft_pending']} | "
          f"Flagged: {s['flagged']} | Skipped: {s['skipped']}")


if __name__ == "__main__":
    run()
