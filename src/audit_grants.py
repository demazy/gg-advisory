# -*- coding: utf-8 -*-
"""Independent fail-closed audit for GG Advisory Grants Radar v5.1.

The updater performs one live official-domain search per mandatory source group. This auditor
then performs a SECOND live-web pass, but in small jurisdiction batches rather than one API
search per programme. Batching preserves independence and field-by-field adversarial checking
while keeping the API-call budget bounded.
"""
from __future__ import annotations

PIPELINE_VERSION = "5.1-batched-source-audit"

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from grants_core import (
    WEB_AUDIT_MODEL,
    canonical_url,
    clean,
    domain,
    independent_validate_batch_via_web,
    json_dump,
    normalise_for_match,
    parse_date,
    scope_text,
    url_on_allowed_domain,
    validate_record_schema,
    yaml_load,
)
from grants_history import validate_history


def _visible(entry: Dict[str, Any], verified: date) -> bool:
    if entry.get("include_in_report") is False:
        return False
    sf = parse_date(entry.get("show_from"))
    su = parse_date(entry.get("show_until"))
    if sf and verified < sf:
        return False
    if su and verified > su and not clean(entry.get("status")):
        return False
    if clean(entry.get("status")) == "Archived" and not entry.get("include_archived"):
        return False
    return True


def _source_for_url(url: str, sources: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    d = domain(url)
    for s in sources:
        allowed = [str(x).lower().removeprefix("www.") for x in (s.get("allowed_domains") or [])]
        if any(d == a or d.endswith("." + a) for a in allowed):
            return s
    return None


def _allowed_domains_for_record(entry: Dict[str, Any], sources: Sequence[Dict[str, Any]]) -> List[str]:
    """Allow the record's current source plus other official/administering domains in its jurisdiction."""
    level = clean(entry.get("level") or "national").lower()
    domains: List[str] = []
    d = domain(clean(entry.get("url")))
    if d:
        domains.append(d)
    for s in sources:
        if clean(s.get("jurisdiction") or "national").lower() == level:
            domains.extend(s.get("allowed_domains") or [])
    return sorted({str(x).lower().removeprefix("www.") for x in domains if clean(x)})


def _md_escape(v: Any) -> str:
    return clean(v).replace("|", "\\|")


def _write_markdown(path: Path, audit: Dict[str, Any]) -> None:
    lines = [
        "# GG Advisory Grants Radar Audit",
        "",
        f"**Verification date:** {audit.get('verified_date')}",
        f"**Publication gate:** {'PASS' if audit.get('publishable') else 'FAIL'}",
        "",
        "## Gate summary",
        "",
        "| Gate | Result | Detail |",
        "|---|---|---|",
    ]
    for name, g in (audit.get("gates") or {}).items():
        lines.append(f"| {_md_escape(name)} | {'PASS' if g.get('pass') else 'FAIL'} | {_md_escape(g.get('detail'))} |")
    lines += ["", "## Programme audit", "", "| Programme | Result | Confidence | Issues |", "|---|---|---:|---|"]
    for r in audit.get("records") or []:
        lines.append(
            f"| {_md_escape(r.get('name') or r.get('id'))} | {'PASS' if r.get('pass') else 'FAIL'} | "
            f"{float(r.get('validator_confidence') or 0):.3f} | {_md_escape('; '.join(r.get('issues') or []))} |"
        )
    continuity = audit.get("continuity") or {}
    lines += [
        "",
        "## Registry continuity",
        "",
        f"Baseline tracked programmes: {len(continuity.get('baseline_visible_ids') or [])}",
        f"Explicit dispositions: {len(continuity.get('dispositions') or [])}",
        f"Unexplained disappearances: {len(continuity.get('unexplained_disappearances') or [])}",
        "",
        "## Completeness statement",
        "",
        "A PASS means every programme published in the Radar passed a second independent live-web verification restricted to official or administering-body domains; every mandatory configured source group completed its own live source search; every Australian jurisdiction is covered by its complete configured mandatory-source set; and every candidate surfaced by those searches received an explicit disposition. This is an auditable completeness claim against the configured source/search universe, not a mathematical claim that an unannounced, private, unindexed or newly published programme outside that universe cannot exist.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _compare_material_field(field: str, proposed: Any, audited: Any) -> bool:
    if field == "deadline":
        return clean(proposed or "") == clean(audited or "")
    if field == "status":
        return clean(proposed) == clean(audited)
    if field in {"name", "admin", "amount", "deadline_label", "target_stage", "type"}:
        return normalise_for_match(proposed) == normalise_for_match(audited)
    return True


def _chunks(seq: Sequence[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    return [list(seq[i:i+n]) for i in range(0, len(seq), n)]


def main() -> None:
    print(f"[pipeline] version={PIPELINE_VERSION} audit=independent-batched-responses-web-search")
    ap = argparse.ArgumentParser()
    ap.add_argument("--grants", type=Path, required=True)
    ap.add_argument("--sources", type=Path, required=True)
    ap.add_argument("--evidence", type=Path, required=True)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--coverage", type=Path, required=True)
    ap.add_argument("--verified", required=True)
    ap.add_argument("--output-json", type=Path, required=True)
    ap.add_argument("--output-md", type=Path, required=True)
    args = ap.parse_args()

    verified = parse_date(args.verified)
    if not verified:
        raise SystemExit("--verified must be YYYY-MM-DD")

    grants_raw = yaml_load(args.grants)
    sources_raw = yaml_load(args.sources)
    source_list = list(sources_raw.get("sources") or [])
    scope = scope_text(sources_raw)
    thresholds = sources_raw.get("thresholds") or {}
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    candidates = json.loads(args.candidates.read_text(encoding="utf-8"))
    coverage = json.loads(args.coverage.read_text(encoding="utf-8"))

    audit: Dict[str, Any] = {
        "verified_date": verified.isoformat(),
        "scope_definition": scope,
        "pipeline": "audited-v5.1-batched-source-audit",
        "publishable": False,
        "gates": {},
        "records": [],
        "unresolved_candidates": [x for x in (candidates.get("candidates") or []) if x.get("resolution") == "unresolved"],
        "continuity": evidence.get("continuity") or {},
    }

    # Gate A: every mandatory configured source search actually ran and returned official provenance.
    required_rows = [x for x in (coverage.get("sources") or []) if x.get("required")]
    source_failed = [x for x in required_rows if not x.get("ok")]
    audit["gates"]["mandatory_source_searches"] = {
        "pass": bool(required_rows) and not source_failed,
        "detail": f"{len(required_rows)-len(source_failed)}/{len(required_rows)} mandatory official-domain source searches completed",
        "failed_sources": [clean(x.get("source_id")) for x in source_failed],
    }

    # Gate B: each jurisdiction is covered by all configured mandatory source searches. This is
    # deterministic aggregation, not nine extra model calls.
    jur_rows = list(coverage.get("jurisdiction_crosschecks") or [])
    required_jur = sorted({clean(x.get("jurisdiction") or "national") for x in source_list if x.get("required", True)})
    jur_map = {clean(x.get("jurisdiction")): x for x in jur_rows}
    failed_jur = [j for j in required_jur if j not in jur_map or not jur_map[j].get("ok")]
    audit["gates"]["jurisdiction_coverage"] = {
        "pass": bool(required_jur) and not failed_jur,
        "detail": f"{len(required_jur)-len(failed_jur)}/{len(required_jur)} jurisdictions covered by all mandatory configured source searches",
        "failed_jurisdictions": failed_jur,
    }

    # Gate C: every discovered candidate must have an explicit non-unresolved disposition.
    unresolved = audit["unresolved_candidates"]
    audit["gates"]["candidate_adjudication"] = {
        "pass": not unresolved,
        "detail": f"{len(unresolved)} unresolved discovery candidates",
    }

    # Gate D: updater found no hard baseline extraction/evidence failures.
    pre_failures = list(evidence.get("verification_failures") or [])
    audit["gates"]["fresh_extraction"] = {
        "pass": not pre_failures,
        "detail": f"{len(pre_failures)} tracked programme verification failures",
    }

    # Gate E: canonical registry continuity.
    continuity = evidence.get("continuity") or {}
    baseline_ids = [clean(x) for x in (continuity.get("baseline_visible_ids") or []) if clean(x)]
    dispositions = [x for x in (continuity.get("dispositions") or []) if isinstance(x, dict)]
    disp_by_id = {clean(x.get("id")): x for x in dispositions if clean(x.get("id"))}
    registry_entries = [dict(x) for x in (grants_raw.get("grants") or []) if isinstance(x, dict)]
    registry_by_id = {clean(x.get("id")): x for x in registry_entries if clean(x.get("id"))}
    unexplained = sorted(set(continuity.get("unexplained_disappearances") or []))
    missing_registry = sorted(x for x in baseline_ids if x not in registry_by_id)
    missing_disposition = sorted(x for x in baseline_ids if x not in disp_by_id)
    bad_change_events = []
    for rid in baseline_ids:
        drow = disp_by_id.get(rid) or {}
        if clean(drow.get("disposition")) != "unchanged" and not drow.get("history_event_added"):
            bad_change_events.append(rid)
    continuity_pass = bool(baseline_ids) and not (unexplained or missing_registry or missing_disposition or bad_change_events)
    audit["gates"]["registry_continuity"] = {
        "pass": continuity_pass,
        "detail": f"{len(baseline_ids)-len(set(missing_registry+missing_disposition+bad_change_events+unexplained))}/{len(baseline_ids)} baseline programmes have explicit, preserved dispositions",
        "missing_registry": missing_registry,
        "missing_disposition": missing_disposition,
        "missing_history_event": bad_change_events,
        "unexplained_disappearances": unexplained,
    }

    # Gate F: history ledger validity.
    history_issues = []
    for rec in registry_entries:
        rid = clean(rec.get("id"))
        for issue in validate_history(rec):
            history_issues.append(f"{rid}:{issue}")
    audit["gates"]["history_ledger"] = {
        "pass": not history_issues,
        "detail": "history ledger valid" if not history_issues else f"{len(history_issues)} history ledger issues",
        "issues": history_issues[:100],
    }

    # Gate G: second independent live-web audit, batched by jurisdiction.
    visible = [x for x in registry_entries if _visible(x, verified)]
    baseline_set = set(baseline_ids)
    audit_targets: List[Dict[str, Any]] = []
    for rec in registry_entries:
        rid = clean(rec.get("id"))
        if _visible(rec, verified) or rid in baseline_set:
            audit_targets.append(rec)

    hard_validator_min = float(thresholds.get("validator_hard_min_confidence", 0.70))
    audit_model = clean(os.getenv("GRANTS_WEB_AUDIT_MODEL", WEB_AUDIT_MODEL)) or WEB_AUDIT_MODEL
    batch_size = max(1, min(10, int(os.getenv("GRANTS_WEB_AUDIT_BATCH_SIZE", "6"))))
    workers = max(1, min(3, int(os.getenv("GRANTS_WEB_AUDIT_WORKERS", "2"))))

    batches: List[List[Dict[str, Any]]] = []
    for jur in sorted({clean(x.get("level") or "national") for x in audit_targets}):
        rows = [x for x in audit_targets if clean(x.get("level") or "national") == jur]
        batches.extend(_chunks(rows, batch_size))

    batch_outputs: List[Dict[str, Any]] = []

    def run_batch(batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        allowed = sorted({d for rec in batch for d in _allowed_domains_for_record(rec, source_list)})
        ids = [clean(x.get("id")) for x in batch]
        try:
            res = independent_validate_batch_via_web(
                records=batch,
                allowed_domains=allowed,
                verified=verified,
                scope_text=scope,
                model=audit_model,
            )
            return {"ids": ids, "allowed_domains": allowed, "result": res, "error": ""}
        except Exception as exc:
            return {"ids": ids, "allowed_domains": allowed, "result": None, "error": clean(exc)}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(run_batch, b) for b in batches]
        for fut in as_completed(futs):
            batch_outputs.append(fut.result())

    validator_by_id: Dict[str, Dict[str, Any]] = {}
    batch_error_by_id: Dict[str, str] = {}
    batch_tool_urls_by_id: Dict[str, List[str]] = {}
    batch_response_id_by_id: Dict[str, str] = {}
    for out in batch_outputs:
        if out.get("error"):
            for rid in out["ids"]:
                batch_error_by_id[rid] = out["error"]
            continue
        res = out.get("result") or {}
        data = dict(res.get("data") or {})
        rows = [x for x in (data.get("records") or []) if isinstance(x, dict)]
        for row in rows:
            rid = clean(row.get("id"))
            if rid:
                validator_by_id[rid] = row
                batch_tool_urls_by_id[rid] = list(res.get("tool_source_urls") or [])
                batch_response_id_by_id[rid] = clean(res.get("response_id"))

    results: List[Dict[str, Any]] = []
    for rec in audit_targets:
        rid = clean(rec.get("id"))
        issues = validate_record_schema(rec)
        warnings: List[str] = []
        dl = parse_date(rec.get("deadline"))
        if dl and clean(rec.get("status")) in {"Open now", "Opening soon"} and dl < verified:
            issues.append("status_deadline_inconsistent")
        if clean(rec.get("last_verified")) != verified.isoformat():
            issues.append("last_verified_not_current_run")

        ledger = (evidence.get("records") or {}).get(rid)
        if not ledger:
            issues.append("missing_evidence_ledger")
        else:
            # Updater hard problems are already enforced by Gate D. Keep diagnostic confidence
            # warnings visible without making a second copy of them a record-level veto.
            warnings.extend(ledger.get("preaudit_warnings") or [])
            allowed_rec = _allowed_domains_for_record(rec, source_list)
            for u in ledger.get("source_urls") or []:
                if clean(u) and not url_on_allowed_domain(u, allowed_rec):
                    issues.append(f"updater_source_outside_allowed_domains:{u}")

        data = validator_by_id.get(rid) or {}
        if rid in batch_error_by_id:
            issues.append(f"validator_error:{batch_error_by_id[rid]}")
        if not data:
            issues.append("independent_validator_missing_record_result")
        conf = float(data.get("confidence") or 0)
        if conf < hard_validator_min:
            issues.append(f"validator_confidence_too_low:{conf:.3f}")
        elif conf < 0.90:
            warnings.append(f"validator_confidence_warning:{conf:.3f}")
        if not data.get("supported"):
            issues.append("independent_validator_rejected")

        checks = data.get("field_checks") or {}
        required_checks = ["name", "admin", "type", "status", "amount", "deadline_label", "target_stage", "description", "why_it_matters"]
        if clean(rec.get("signals")):
            required_checks.append("signals")
        if clean(rec.get("deadline_type")).lower() == "fixed" or clean(rec.get("deadline")):
            required_checks.append("deadline")
        for field in required_checks:
            chk = checks.get(field) or {}
            if not chk.get("supported"):
                issues.append(f"field_not_supported:{field}:{clean(chk.get('reason'))}")
            if field in {"name", "admin", "type", "status", "amount", "deadline", "deadline_label", "target_stage"} and "current_value" in chk:
                if not _compare_material_field(field, rec.get(field), chk.get("current_value")):
                    issues.append(f"independent_value_disagreement:{field}:auditor={clean(chk.get('current_value'))}")

        allowed_rec = _allowed_domains_for_record(rec, source_list)
        validator_urls = list(data.get("source_urls") or [])
        for chk in checks.values():
            if isinstance(chk, dict) and clean(chk.get("source_url")):
                validator_urls.append(clean(chk.get("source_url")))
        if not validator_urls:
            issues.append("independent_validator_no_record_source_urls")
        for u in validator_urls:
            if clean(u) and not url_on_allowed_domain(u, allowed_rec):
                issues.append(f"validator_source_outside_allowed_domains:{u}")
        if rid not in batch_error_by_id and not batch_tool_urls_by_id.get(rid):
            issues.append("independent_validator_no_official_web_search_sources")

        for x in data.get("contradictions") or []:
            if clean(x):
                issues.append(f"contradiction:{clean(x)}")
        for x in data.get("material_issues") or []:
            if clean(x) and clean(x).lower() not in {"none", "n/a", "no material issues"}:
                issues.append(f"validator:{clean(x)}")

        row = {
            "id": rid,
            "name": rec.get("name"),
            "pass": not issues,
            "issues": sorted(set(issues)),
            "warnings": sorted(set(warnings)),
            "validator_confidence": conf,
            "field_checks": checks,
            "validator_source_urls": sorted(set(validator_urls)),
            "validator_model": audit_model,
            "validator_response_id": batch_response_id_by_id.get(rid),
            "included_in_report": _visible(rec, verified),
            "baseline_tracked": rid in baseline_set,
        }
        results.append(row)
        print(f"[audit] {'PASS' if row['pass'] else 'FAIL'} {rid} issues={len(row['issues'])} warnings={len(row['warnings'])}")

    order = {clean(x.get("id")): i for i, x in enumerate(audit_targets)}
    results.sort(key=lambda r: order.get(clean(r.get("id")), 999999))
    audit["records"] = results
    published_results = [r for r in results if r.get("included_in_report")]
    failed_published = [r for r in published_results if not r.get("pass")]
    baseline_results = [r for r in results if r.get("baseline_tracked")]
    failed_baseline = [r for r in baseline_results if not r.get("pass")]
    audit["gates"]["all_published_records"] = {
        "pass": bool(visible) and len(published_results) == len(visible) and not failed_published,
        "detail": f"{len(published_results)-len(failed_published)}/{len(visible)} visible records passed independent batched live-web audit",
    }
    audit["gates"]["baseline_continuity_audit"] = {
        "pass": bool(baseline_ids) and len(baseline_results) == len(baseline_ids) and not failed_baseline,
        "detail": f"{len(baseline_results)-len(failed_baseline)}/{len(baseline_ids)} previously tracked records passed independent audit, including archived transitions",
    }

    # Gate H: uniqueness.
    ids = [clean(x.get("id")) for x in visible]
    urls = [canonical_url(clean(x.get("url"))) for x in visible]
    dupes = sorted({x for x in ids if x and ids.count(x) > 1} | {u for u in urls if u and urls.count(u) > 1})
    audit["gates"]["uniqueness"] = {
        "pass": not dupes,
        "detail": "no duplicate IDs/URLs" if not dupes else "duplicates: " + ", ".join(dupes),
    }

    audit["publishable"] = all(bool(g.get("pass")) for g in audit["gates"].values())
    audit["summary"] = {
        "visible_programmes": len(visible),
        "programmes_passed": len(published_results) - len(failed_published),
        "baseline_programmes": len(baseline_ids),
        "baseline_programmes_audited": len(baseline_results) - len(failed_baseline),
        "history_events_total": sum(len(x.get("history") or []) for x in registry_entries),
        "unresolved_candidates": len(unresolved),
        "mandatory_sources_total": len(required_rows),
        "mandatory_sources_ok": len(required_rows) - len(source_failed),
        "jurisdictions_total": len(required_jur),
        "jurisdictions_ok": len(required_jur) - len(failed_jur),
        "independent_audit_batches": len(batches),
    }

    json_dump(args.output_json, audit)
    _write_markdown(args.output_md, audit)
    print(f"[audit] OVERALL {'PASS' if audit['publishable'] else 'FAIL'}")
    print(f"[write] {args.output_json}")
    print(f"[write] {args.output_md}")
    if not audit["publishable"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
