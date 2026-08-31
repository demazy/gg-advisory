# -*- coding: utf-8 -*-
"""Independent fail-closed audit for GG Advisory Grants Radar v3.

The updater and this auditor use separate prompts and, by default, different models.
Both are restricted to live official/administering domains. Publication requires all
coverage, reconciliation and programme-level gates to pass.
"""
from __future__ import annotations

PIPELINE_VERSION = "3.1-web-search"

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
    independent_validate_via_web,
    json_dump,
    normalise_for_match,
    parse_date,
    scope_text,
    url_on_allowed_domain,
    validate_record_schema,
    yaml_load,
)


def _visible(entry: Dict[str, Any], verified: date) -> bool:
    if entry.get("include_in_report") is False:
        return False
    sf = parse_date(entry.get("show_from"))
    su = parse_date(entry.get("show_until"))
    if sf and verified < sf:
        return False
    if su and verified > su and not clean(entry.get("status")):
        return False
    # Definitively ended programmes do not belong in a current radar unless explicitly retained.
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
    src = _source_for_url(clean(entry.get("url")), sources)
    if src:
        return sorted(set(src.get("allowed_domains") or []))
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
    lines += [
        "",
        "## Completeness statement",
        "",
        "A PASS means every programme published in the Radar passed an independent live-web verification restricted to official or administering-body domains; every mandatory source group completed a source-level completeness search; every Australian jurisdiction completed an independent cross-check search; and every candidate surfaced by those searches was explicitly reconciled. This is a strong, auditable completeness claim against the configured source and search universe. No automated method can mathematically prove that an unannounced, private, unindexed or newly published programme outside that universe does not exist.",
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


def main() -> None:
    print(f"[pipeline] version={PIPELINE_VERSION} audit=independent-responses-web-search")
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
        "pipeline": "audited-v3-web-primary",
        "publishable": False,
        "gates": {},
        "records": [],
        "unresolved_candidates": [x for x in (candidates.get("candidates") or []) if x.get("resolution") == "unresolved"],
    }

    # Gate A: every mandatory source-level search completed successfully.
    required_rows = [x for x in (coverage.get("sources") or []) if x.get("required")]
    source_failed = [x for x in required_rows if not x.get("ok")]
    source_pass = bool(required_rows) and not source_failed
    audit["gates"]["mandatory_source_searches"] = {
        "pass": source_pass,
        "detail": f"{len(required_rows)-len(source_failed)}/{len(required_rows)} mandatory official-domain source searches passed",
        "failed_sources": [clean(x.get("source_id")) for x in source_failed],
    }

    # Gate B: one independent cross-check search for every configured jurisdiction.
    jur_rows = list(coverage.get("jurisdiction_crosschecks") or [])
    required_jur = sorted({clean(x.get("jurisdiction") or "national") for x in source_list if x.get("required", True)})
    jur_map = {clean(x.get("jurisdiction")): x for x in jur_rows}
    failed_jur = [j for j in required_jur if j not in jur_map or not jur_map[j].get("ok")]
    jur_pass = bool(required_jur) and not failed_jur
    audit["gates"]["jurisdiction_crosschecks"] = {
        "pass": jur_pass,
        "detail": f"{len(required_jur)-len(failed_jur)}/{len(required_jur)} jurisdiction cross-check searches passed",
        "failed_jurisdictions": failed_jur,
    }

    # Gate C: all discovered candidates adjudicated.
    unresolved = audit["unresolved_candidates"]
    audit["gates"]["candidate_adjudication"] = {
        "pass": not unresolved,
        "detail": f"{len(unresolved)} unresolved discovery candidates",
    }

    # Gate D: update/extraction stage itself found no failures.
    pre_failures = list(evidence.get("verification_failures") or [])
    audit["gates"]["fresh_extraction"] = {
        "pass": not pre_failures,
        "detail": f"{len(pre_failures)} tracked programme extraction failures",
    }

    # Gate E: independent programme-by-programme web audit.
    visible = [dict(x) for x in (grants_raw.get("grants") or []) if isinstance(x, dict) and _visible(x, verified)]
    validator_min = float(thresholds.get("validator_min_confidence", 0.95))
    audit_model = clean(os.getenv("GRANTS_WEB_AUDIT_MODEL", WEB_AUDIT_MODEL)) or WEB_AUDIT_MODEL
    workers = max(1, min(5, int(os.getenv("GRANTS_WEB_AUDIT_WORKERS", "3"))))

    def audit_one(rec: Dict[str, Any]) -> Dict[str, Any]:
        rid = clean(rec.get("id"))
        issues = validate_record_schema(rec)
        dl = parse_date(rec.get("deadline"))
        if dl and clean(rec.get("status")) in {"Open now", "Opening soon"} and dl < verified:
            issues.append("status_deadline_inconsistent")
        if clean(rec.get("last_verified")) != verified.isoformat():
            issues.append("last_verified_not_current_run")
        ledger = (evidence.get("records") or {}).get(rid)
        if not ledger:
            issues.append("missing_evidence_ledger")
        else:
            issues.extend([f"preaudit:{x}" for x in (ledger.get("preaudit_issues") or [])])

        allowed = _allowed_domains_for_record(rec, source_list)
        # The updater's provenance must stay on official/administering domains.
        if ledger:
            for u in ledger.get("source_urls") or []:
                if clean(u) and not url_on_allowed_domain(u, allowed):
                    issues.append(f"updater_source_outside_allowed_domains:{u}")

        validator: Dict[str, Any] = {"data": {"supported": False, "confidence": 0, "material_issues": ["not_run"]}, "tool_source_urls": []}
        try:
            validator = independent_validate_via_web(
                record=rec, allowed_domains=allowed, verified=verified, scope_text=scope, model=audit_model
            )
        except Exception as exc:
            issues.append(f"validator_error:{clean(exc)}")
        data = dict(validator.get("data") or {})
        conf = float(data.get("confidence") or 0)
        if not data.get("supported"):
            issues.append("independent_validator_rejected")
        if conf < validator_min:
            issues.append(f"validator_confidence_below_gate:{conf:.3f}")
        for u in (data.get("source_urls") or []) + (validator.get("tool_source_urls") or []):
            if clean(u) and not url_on_allowed_domain(u, allowed):
                issues.append(f"validator_source_outside_allowed_domains:{u}")
        checks = data.get("field_checks") or {}
        required_checks = ["name", "admin", "type", "status", "amount", "deadline_label", "target_stage", "description", "why_it_matters"]
        if clean(rec.get("signals")):
            required_checks.append("signals")
        if clean(rec.get("deadline_type")).lower() == "fixed" or clean(rec.get("deadline")):
            required_checks.append("deadline")
        for f in required_checks:
            chk = checks.get(f) or {}
            if not chk.get("supported"):
                issues.append(f"field_not_supported:{f}:{clean(chk.get('reason'))}")
            if f in {"name", "admin", "type", "status", "amount", "deadline", "deadline_label", "target_stage"} and "current_value" in chk:
                if not _compare_material_field(f, rec.get(f), chk.get("current_value")):
                    issues.append(f"independent_value_disagreement:{f}:auditor={clean(chk.get('current_value'))}")
        for x in data.get("contradictions") or []:
            issues.append(f"contradiction:{clean(x)}")
        for x in data.get("material_issues") or []:
            if clean(x) and clean(x).lower() not in {"none", "n/a", "no material issues"}:
                issues.append(f"validator:{clean(x)}")
        return {
            "id": rid,
            "name": rec.get("name"),
            "pass": not issues,
            "issues": sorted(set(issues)),
            "validator_confidence": conf,
            "field_checks": checks,
            "validator_source_urls": sorted(set((data.get("source_urls") or []) + (validator.get("tool_source_urls") or []))),
            "validator_model": validator.get("model") or audit_model,
            "validator_response_id": validator.get("response_id"),
        }

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(audit_one, rec) for rec in visible]
        for fut in as_completed(futs):
            row = fut.result()
            results.append(row)
            print(f"[audit] {'PASS' if row['pass'] else 'FAIL'} {row['id']} issues={len(row['issues'])}")
    order = {clean(x.get("id")): i for i, x in enumerate(visible)}
    results.sort(key=lambda r: order.get(clean(r.get("id")), 999999))
    audit["records"] = results
    failed_records = [r for r in results if not r.get("pass")]
    audit["gates"]["all_published_records"] = {
        "pass": bool(visible) and not failed_records,
        "detail": f"{len(visible)-len(failed_records)}/{len(visible)} visible records passed independent live-web audit",
    }

    # Gate F: uniqueness.
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
        "programmes_passed": len(visible) - len(failed_records),
        "unresolved_candidates": len(unresolved),
        "mandatory_sources_total": len(required_rows),
        "mandatory_sources_ok": len(required_rows) - len(source_failed),
        "jurisdictions_total": len(required_jur),
        "jurisdictions_ok": len(required_jur) - len(failed_jur),
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
