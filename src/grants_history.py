# -*- coding: utf-8 -*-
"""Stable change ledger for the GG Advisory Grants Radar registry.

`config/grants.yaml` remains the canonical registry. Current facts stay as flat fields on
individual records so the PDF builder and downstream tools remain simple. Material changes
are appended to each record's `history` array; nothing is silently deleted.
"""
from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

PIPELINE_VERSION = "5.0-canonical-ledger-layout"

# Only fields that describe the programme/pathway itself are versioned. Editorial prose is
# intentionally excluded so harmless rewriting does not create fake historical events.
HISTORY_FIELDS: Tuple[str, ...] = (
    "name",
    "admin",
    "level",
    "type",
    "status",
    "amount",
    "deadline",
    "deadline_type",
    "deadline_label",
    "target_stage",
    "url",
    "signals",
    "include_in_report",
)

VALID_HISTORY_EVENTS = {
    "added",
    "updated",
    "status_changed",
    "renamed_or_superseded",
    "archived",
    "reopened",
}


def _clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def _normalise_value(field: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if field in {"level", "type", "status", "deadline_type"}:
        return _clean(value).lower()
    if field == "url":
        s = _clean(value).rstrip("/")
        return s.lower()
    return _clean(value)


def snapshot(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact, deterministic factual snapshot used for change detection."""
    out: Dict[str, Any] = {}
    for field in HISTORY_FIELDS:
        if field in record:
            v = record.get(field)
            # Keep YAML-friendly original values in history, including explicit nulls.
            out[field] = deepcopy(v)
    return out


def changes_between(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    changes: Dict[str, Dict[str, Any]] = {}
    for field in HISTORY_FIELDS:
        before_present = field in old
        after_present = field in new
        before = old.get(field)
        after = new.get(field)
        if before_present != after_present or _normalise_value(field, before) != _normalise_value(field, after):
            changes[field] = {"before": deepcopy(before) if before_present else None, "after": deepcopy(after) if after_present else None}
    return changes


def classify_event(old: Optional[Dict[str, Any]], new: Dict[str, Any], changes: Dict[str, Dict[str, Any]]) -> str:
    if old is None:
        return "added"
    old_status = _clean(old.get("status")).lower()
    new_status = _clean(new.get("status")).lower()
    if new_status == "archived" and old_status != "archived":
        return "archived"
    if old_status in {"archived", "closed, monitor", "paused"} and new_status in {"open now", "rolling", "opening soon"}:
        return "reopened"
    if "name" in changes or "url" in changes:
        return "renamed_or_superseded"
    if "status" in changes:
        return "status_changed"
    return "updated"


def _event_signature(event: Dict[str, Any]) -> str:
    payload = {
        "verified_date": event.get("verified_date"),
        "event": event.get("event"),
        "changes": event.get("changes"),
        "source_urls": sorted(event.get("source_urls") or []),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()


def apply_history(
    old: Optional[Dict[str, Any]],
    new: Dict[str, Any],
    *,
    verified_date: str,
    source_urls: Sequence[str],
) -> Tuple[Dict[str, Any], str, List[str], bool]:
    """Preserve existing history and append one material change event when required.

    Returns `(record, disposition, changed_fields, history_event_added)`.
    `disposition` is always explicit, including `unchanged`, which lets the audit prove
    that no previously tracked programme silently disappeared.
    """
    out = deepcopy(new)
    existing_history = deepcopy((old or {}).get("history") or [])
    if not isinstance(existing_history, list):
        existing_history = []
    out["history"] = existing_history

    if old is None:
        changed = {field: {"before": None, "after": deepcopy(value)} for field, value in snapshot(out).items()}
        disposition = "added"
    else:
        changed = changes_between(old, out)
        disposition = "unchanged" if not changed else classify_event(old, out, changed)

    if not changed:
        return out, disposition, [], False

    event = {
        "verified_date": verified_date,
        "event": disposition,
        "changes": changed,
        "source_urls": sorted({_clean(u) for u in source_urls if _clean(u)}),
    }
    sig = _event_signature(event)
    if not any(isinstance(x, dict) and _event_signature(x) == sig for x in existing_history):
        out["history"].append(event)
        return out, disposition, sorted(changed), True
    return out, disposition, sorted(changed), False


def validate_history(record: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    hist = record.get("history")
    if hist is None:
        return issues
    if not isinstance(hist, list):
        return ["history_not_list"]
    seen = set()
    last_date = ""
    for i, event in enumerate(hist):
        prefix = f"history[{i}]"
        if not isinstance(event, dict):
            issues.append(f"{prefix}:not_object")
            continue
        verified = _clean(event.get("verified_date"))
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", verified):
            issues.append(f"{prefix}:invalid_verified_date")
        if last_date and verified < last_date:
            issues.append(f"{prefix}:out_of_order")
        last_date = max(last_date, verified)
        kind = _clean(event.get("event"))
        if kind not in VALID_HISTORY_EVENTS:
            issues.append(f"{prefix}:invalid_event:{kind}")
        changes = event.get("changes")
        if not isinstance(changes, dict) or not changes:
            issues.append(f"{prefix}:missing_changes")
        else:
            unknown = sorted(set(changes) - set(HISTORY_FIELDS))
            if unknown:
                issues.append(f"{prefix}:unknown_fields:{','.join(unknown)}")
        sig = _event_signature(event)
        if sig in seen:
            issues.append(f"{prefix}:duplicate_event")
        seen.add(sig)
    return issues
