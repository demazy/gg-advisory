# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any, Dict

from grants_core import (
    PIPELINE_VERSION,
    canonical_url,
    discover_source,
    factual_fingerprint,
    fetch_url,
    http_count,
    json_dump,
    record_visible,
    reset_http_counter,
    verify_record,
    yaml_dump,
    yaml_load,
)
from grants_history import apply_history


def _cached_fetcher():
    cache = {}

    def fetch(url: str):
        key = canonical_url(url)
        if key not in cache:
            cache[key] = fetch_url(url)
        return cache[key]

    return fetch


def _refresh_snapshot(old_snapshot: Dict[str, Any], record: Dict[str, Any], verified: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """Refresh a snapshot only after full live sentinel confirmation.

    The field/claim evidence remains the previously curated evidence.  The new date says that
    the current official page still satisfied the complete sentinel contract for the unchanged
    factual fingerprint; it does not fabricate new quotations.
    """
    snap = deepcopy(old_snapshot or {})
    snap["verified_date"] = verified
    snap["record_fingerprint"] = factual_fingerprint(record)
    live_urls = [x for x in (result.get("live_source_urls") or []) if x]
    if live_urls:
        snap["source_urls"] = sorted(set((snap.get("source_urls") or []) + live_urls))
    snap["refresh_basis"] = "full_live_sentinel_confirmation"
    return snap


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--sources", type=Path, required=True)
    ap.add_argument("--contracts", type=Path, required=True)
    ap.add_argument("--snapshots", type=Path, required=True)
    ap.add_argument("--discovery-baseline", type=Path, required=True)
    ap.add_argument("--decisions", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--snapshot-output", type=Path, required=True)
    ap.add_argument("--baseline-output", type=Path, required=True)
    ap.add_argument("--evidence", type=Path, required=True)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--coverage", type=Path, required=True)
    ap.add_argument("--verified", required=True)
    args = ap.parse_args()

    verified = date.fromisoformat(args.verified)
    registry = yaml_load(args.input)
    source_cfg = yaml_load(args.sources)
    contracts_doc = yaml_load(args.contracts)
    snapshots_doc = yaml_load(args.snapshots)
    baseline_doc = yaml_load(args.discovery_baseline)
    decisions_doc = yaml_load(args.decisions)

    contracts = contracts_doc.get("records") or {}
    snapshots = snapshots_doc.get("records") or {}
    baselines = baseline_doc.get("sources") or {}
    decisions = {canonical_url(u): v for u, v in (decisions_doc.get("decisions") or {}).items()}

    original_grants = list(registry.get("grants") or [])
    registry_urls = [g.get("url", "") for g in original_grants]
    out_grants = []
    records: Dict[str, Any] = {}
    verification_failures = []
    dispositions: Dict[str, Any] = {}
    snapshot_candidates = deepcopy(snapshots)

    reset_http_counter()
    fetcher = _cached_fetcher()

    for old in original_grants:
        g = deepcopy(old)
        gid = g.get("id")
        if record_visible(g, verified):
            contract = contracts.get(gid)
            snapshot = snapshots.get(gid) or {}
            if not contract:
                result = {
                    "id": gid,
                    "name": g.get("name"),
                    "pass": False,
                    "issues": ["missing_evidence_contract"],
                    "warnings": [],
                    "verification_mode": "failed",
                    "live_confirmation": False,
                    "sources": [],
                }
            else:
                result = verify_record(g, contract, snapshot, verified, fetcher=fetcher)

            if result.get("pass"):
                # URL normalisation is the only automated registry fact mutation in v7.  All
                # other current facts remain canonical and must be changed deliberately.
                preferred = (contract or {}).get("preferred_url")
                if preferred and (contract or {}).get("apply_preferred_url_to_registry"):
                    g["url"] = preferred
                # Do not pretend a technically blocked source was freshly reconfirmed.  A
                # snapshot bridge passes the issue-level gate but preserves the record's own
                # last_verified date until full live sentinels succeed.
                if result.get("live_confirmation"):
                    g["last_verified"] = args.verified
                source_urls = [x.get("final_url") or x.get("requested_url") for x in result.get("sources") or [] if x.get("ok")]
                g, disposition, changed_fields, history_added = apply_history(
                    old,
                    g,
                    verified_date=args.verified,
                    source_urls=source_urls or (snapshot.get("source_urls") or []),
                )
                dispositions[gid] = {
                    "disposition": disposition,
                    "changed_fields": changed_fields,
                    "history_event_added": history_added,
                }
                if result.get("live_confirmation"):
                    snapshot_candidates[gid] = _refresh_snapshot(snapshot, g, args.verified, result)
            else:
                verification_failures.append(gid)
                dispositions[gid] = {"disposition": "verification_failed", "changed_fields": [], "history_event_added": False}

            result["disposition"] = dispositions[gid]["disposition"]
            records[gid] = result
        else:
            # Non-visible historical/current-registry records remain untouched and explicit.
            dispositions[gid] = {"disposition": "not_visible_this_issue", "changed_fields": [], "history_event_added": False}
        out_grants.append(g)

    # Use the potentially normalised candidate URLs for discovery matching.
    candidate_registry_urls = [g.get("url", "") for g in out_grants]
    coverage_rows = []
    all_candidates = []
    baseline_candidates = deepcopy(baselines)
    for cfg in source_cfg.get("sources") or []:
        sid = cfg.get("id")
        row = discover_source(
            cfg,
            source_cfg.get("scope") or {},
            baselines.get(sid) or {"verified_date": baseline_doc.get("verified_date"), "known_urls": []},
            decisions,
            candidate_registry_urls,
            verified,
            fetcher=fetcher,
        )
        coverage_rows.append(row)
        baseline_candidates[sid] = row.get("baseline_candidate") or baselines.get(sid) or {}
        for c in row.get("candidates") or []:
            item = dict(c)
            item["source_id"] = sid
            item["jurisdiction"] = row.get("jurisdiction")
            all_candidates.append(item)

    unresolved = [c for c in all_candidates if c.get("resolution") == "unresolved"]
    required_failed = [r.get("source_id") for r in coverage_rows if r.get("required") and not r.get("covered")]

    out_registry = deepcopy(registry)
    out_registry["grants"] = out_grants
    yaml_dump(args.output, out_registry)

    out_snapshots = deepcopy(snapshots_doc)
    out_snapshots["version"] = PIPELINE_VERSION
    out_snapshots["records"] = snapshot_candidates
    yaml_dump(args.snapshot_output, out_snapshots)

    out_baseline = deepcopy(baseline_doc)
    out_baseline["version"] = PIPELINE_VERSION
    out_baseline["sources"] = baseline_candidates
    if all((r.get("covered") for r in coverage_rows)) and not unresolved:
        out_baseline["verified_date"] = args.verified
    yaml_dump(args.baseline_output, out_baseline)

    json_dump(
        args.evidence,
        {
            "verified_date": args.verified,
            "pipeline": PIPELINE_VERSION,
            "records": records,
            "verification_failures": verification_failures,
            "dispositions": dispositions,
            "baseline_ids": [g.get("id") for g in original_grants],
            "candidate_ids": [g.get("id") for g in out_grants],
            "http_requests": http_count(),
        },
    )
    json_dump(
        args.candidates,
        {
            "verified_date": args.verified,
            "pipeline": PIPELINE_VERSION,
            "candidates": all_candidates,
            "unresolved_count": len(unresolved),
        },
    )
    json_dump(
        args.coverage,
        {
            "verified_date": args.verified,
            "pipeline": PIPELINE_VERSION,
            "sources": coverage_rows,
            "required_failed": required_failed,
            "http_requests": http_count(),
        },
    )

    visible_count = len(records)
    live = sum(1 for r in records.values() if r.get("verification_mode") == "live_confirmed")
    bridged = sum(1 for r in records.values() if r.get("verification_mode") in {"live_plus_fresh_snapshot", "fresh_snapshot_fallback"})
    print(f"[pipeline] version={PIPELINE_VERSION} verification=official-http-plus-fresh-snapshot discovery=dated-inventory-delta")
    print(f"[verify] visible={visible_count} failures={len(verification_failures)} live_confirmed={live} snapshot_bridged={bridged}")
    print(f"[discovery] unresolved_new={len(unresolved)} required_source_failures={len(required_failed)}")
    print(f"[http] unique requests={http_count()}")
    print(f"[write] {args.output}")
    if verification_failures or unresolved or required_failed:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
