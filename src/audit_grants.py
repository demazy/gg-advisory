# -*- coding: utf-8 -*-
"""Deterministic publication audit for the snapshot-sentinel Grants Radar pipeline.

This stage performs no network calls.  It independently checks the captured verification
and discovery evidence emitted by update_grants.py, registry continuity, history integrity,
uniqueness, snapshot freshness modes and jurisdiction/source coverage.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from grants_core import PIPELINE_VERSION, canonical_url, json_dump, record_visible, yaml_load
from grants_history import validate_history


def _md(a: Dict[str, Any]) -> str:
    lines = [
        "# GG Advisory Grants Radar Audit",
        "",
        f"**Verification date:** {a['verified_date']}",
        f"**Publication gate:** {'PASS' if a['publishable'] else 'FAIL'}",
        "",
        "## Gate summary",
        "",
        "| Gate | Result | Detail |",
        "|---|---|---|",
    ]
    for k, v in a["gates"].items():
        lines.append(f"| {k} | {'PASS' if v['pass'] else 'FAIL'} | {str(v['detail']).replace('|', '/')} |")
    lines += ["", "## Programme audit", "", "| Programme | Result | Verification mode | Issues / warnings |", "|---|---|---|---|"]
    for r in a["records"]:
        notes = "; ".join((r.get("issues") or []) + (r.get("warnings") or []))
        lines.append(
            f"| {str(r.get('name','')).replace('|','/')} | {'PASS' if r.get('pass') else 'FAIL'} | "
            f"{r.get('verification_mode','')} | {notes.replace('|','/')} |"
        )
    lines += [
        "",
        "## Method",
        "",
        "No paid model/API is used. The verification stage attempts current official/administering-source HTTP checks against deterministic identity/status/amount/deadline sentinels. Temporary source blocking may be bridged only by a matching evidence snapshot not older than the configured freshness window. Discovery compares monitored official indexes with a dated link inventory and blocks only genuinely new, unresolved high-signal links. This audit makes no second network crawl; it validates the captured evidence, continuity, history and coverage fail-closed.",
    ]
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grants", type=Path, required=True)
    ap.add_argument("--evidence", type=Path, required=True)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--coverage", type=Path, required=True)
    ap.add_argument("--verified", required=True)
    ap.add_argument("--output-json", type=Path, required=True)
    ap.add_argument("--output-md", type=Path, required=True)
    args = ap.parse_args()

    verified = date.fromisoformat(args.verified)
    registry = yaml_load(args.grants)
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    candidates = json.loads(args.candidates.read_text(encoding="utf-8"))
    coverage = json.loads(args.coverage.read_text(encoding="utf-8"))

    visible = [g for g in registry.get("grants") or [] if record_visible(g, verified)]
    erows = evidence.get("records") or {}
    rows: List[Dict[str, Any]] = []
    for g in visible:
        row = dict(erows.get(g.get("id")) or {})
        if not row:
            row = {"id": g.get("id"), "name": g.get("name"), "pass": False, "issues": ["missing_captured_verification_result"], "warnings": [], "verification_mode": "failed"}
        rows.append(row)

    record_fail = [r for r in rows if not r.get("pass")]
    unresolved = [c for c in candidates.get("candidates") or [] if c.get("resolution") == "unresolved"]
    required_rows = [r for r in coverage.get("sources") or [] if r.get("required")]
    required_failed = [r.get("source_id") for r in required_rows if not r.get("covered")]

    expected = {"national", "act", "nsw", "nt", "qld", "sa", "tas", "vic", "wa"}
    covered_jurisdictions = {
        str(r.get("jurisdiction") or "").lower()
        for r in coverage.get("sources") or []
        if r.get("covered")
    }
    missing_jurisdictions = sorted(expected - covered_jurisdictions)

    baseline_ids = [x for x in evidence.get("baseline_ids") or [] if x]
    candidate_ids = [g.get("id") for g in registry.get("grants") or [] if g.get("id")]
    missing_ids = sorted(set(baseline_ids) - set(candidate_ids))
    dispositions = evidence.get("dispositions") or {}
    missing_disposition = sorted(x for x in baseline_ids if x not in dispositions)

    history_issues = {}
    for g in registry.get("grants") or []:
        issues = validate_history(g)
        if issues:
            history_issues[g.get("id") or "<missing-id>"] = issues

    ids = [g.get("id") for g in registry.get("grants") or []]
    urls = [canonical_url(g.get("url", "")) for g in registry.get("grants") or []]
    dup_ids = sorted({x for x in ids if x and ids.count(x) > 1})
    dup_urls = sorted({x for x in urls if x and urls.count(x) > 1})

    stale_modes = [
        r.get("id") for r in rows
        if not r.get("pass") and any("snapshot_stale" in str(x) for x in (r.get("issues") or []))
    ]

    gates = {
        "mandatory_source_coverage": {
            "pass": not required_failed,
            "detail": "all required source groups covered by live source or fresh dated inventory" if not required_failed else f"failed: {', '.join(required_failed)}",
        },
        "jurisdiction_source_coverage": {
            "pass": not missing_jurisdictions,
            "detail": "9/9 national/state/territory jurisdictions covered by monitored source groups" if not missing_jurisdictions else "missing: " + ", ".join(missing_jurisdictions),
        },
        "candidate_reconciliation": {
            "pass": not unresolved,
            "detail": "0 genuinely new unresolved high-signal links" if not unresolved else f"{len(unresolved)} genuinely new unresolved high-signal links",
        },
        "published_record_verification": {
            "pass": not record_fail,
            "detail": f"{len(rows)-len(record_fail)}/{len(rows)} visible records passed captured official-source/snapshot verification",
        },
        "snapshot_freshness": {
            "pass": not stale_modes,
            "detail": "all snapshot bridges within configured freshness window" if not stale_modes else "stale snapshot failures: " + ", ".join(stale_modes),
        },
        "registry_continuity": {
            "pass": not missing_ids and not missing_disposition,
            "detail": f"{len(baseline_ids)}/{len(baseline_ids)} baseline records preserved with explicit dispositions" if not missing_ids and not missing_disposition else f"missing_records={missing_ids}; missing_dispositions={missing_disposition}",
        },
        "history_ledger": {
            "pass": not history_issues,
            "detail": "history ledger valid" if not history_issues else f"history issues in {len(history_issues)} records",
        },
        "uniqueness": {
            "pass": not dup_ids and not dup_urls,
            "detail": "no duplicate IDs/URLs" if not dup_ids and not dup_urls else f"duplicate_ids={dup_ids}; duplicate_urls={dup_urls}",
        },
        "zero_paid_api": {
            "pass": True,
            "detail": "no paid model/API dependency; audit performs zero network requests",
        },
    }

    publishable = all(v["pass"] for v in gates.values())
    modes = {}
    for r in rows:
        m = r.get("verification_mode") or "unknown"
        modes[m] = modes.get(m, 0) + 1
    summary = {
        "visible_programmes": len(rows),
        "programmes_passed": len(rows) - len(record_fail),
        "verification_modes": modes,
        "mandatory_sources_total": len(required_rows),
        "mandatory_sources_covered": len(required_rows) - len(required_failed),
        "unresolved_new_candidates": len(unresolved),
        "baseline_programmes": len(baseline_ids),
        "baseline_programmes_preserved": len(baseline_ids) - len(missing_ids),
        "audit_network_requests": 0,
    }
    out = {
        "verified_date": args.verified,
        "pipeline": PIPELINE_VERSION,
        "publishable": publishable,
        "gates": gates,
        "summary": summary,
        "records": rows,
        "unresolved_candidates": unresolved,
        "history_issues": history_issues,
    }
    json_dump(args.output_json, out)
    args.output_md.write_text(_md(out), encoding="utf-8")
    print(f"[pipeline] version={PIPELINE_VERSION} audit=captured-evidence-no-second-http")
    print(f"[audit] visible={len(rows)} failed={len(record_fail)} unresolved_new={len(unresolved)}")
    print(f"[audit] OVERALL {'PASS' if publishable else 'FAIL'}")
    print("[audit] network requests=0")
    if not publishable:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
