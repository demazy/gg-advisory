# -*- coding: utf-8 -*-
"""Independent fail-closed audit for the Grants & Accelerators Radar.

A PASS means:
1. 100% of report-visible programmes passed schema and primary-source evidence checks.
2. 100% of critical factual fields have literal evidence still present on a live source.
3. 100% of report-visible records passed an independent adversarial model review.
4. 100% of mandatory discovery sources were scanned successfully.
5. 100% of candidates surfaced by that discovery universe were adjudicated; none remain unresolved.
6. No material source contradiction remains unresolved.

It intentionally does not claim omniscience beyond the mandatory discovery universe.
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from grants_core import (
    ALLOWED_LEVELS,
    CRITICAL_FIELDS,
    canonical_url,
    clean,
    domain,
    evidence_quote_is_literal,
    factual_sentences,
    fetch_url,
    independent_validate,
    json_dump,
    parse_date,
    required_evidence_fields,
    scope_text,
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
    return True


def _source_group(url: str, sources: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    d = domain(url)
    for s in sources:
        allowed = [str(x).lower().removeprefix("www.") for x in (s.get("allowed_domains") or [])]
        if any(d == a or d.endswith("." + a) for a in allowed):
            return s
    return None


def _evidence_literal_issues(record: Dict[str, Any], ledger: Dict[str, Any], fetched: Dict[str, str]) -> List[str]:
    record_id = clean(record.get("id"))
    issues: List[str] = []
    ev = ledger.get("evidence") or {}
    for field in required_evidence_fields(record):
        item = ev.get(field) or {}
        u = canonical_url(clean(item.get("source_url")))
        q = clean(item.get("quote"))
        if not u or not q:
            issues.append(f"{record_id}:missing_evidence:{field}")
            continue
        text = fetched.get(u)
        if not text or not evidence_quote_is_literal(q, text):
            issues.append(f"{record_id}:evidence_not_literal_now:{field}")

    claim_rows = [x for x in (ledger.get("claim_evidence") or []) if isinstance(x, dict)]
    for path, sentence in factual_sentences(record):
        matches = [x for x in claim_rows if clean(x.get("path")) == path and clean(x.get("claim")) == sentence]
        if not matches:
            issues.append(f"{record_id}:missing_claim_evidence:{path}")
            continue
        if not any(
            canonical_url(clean(x.get("source_url"))) in fetched
            and evidence_quote_is_literal(clean(x.get("quote")), fetched[canonical_url(clean(x.get("source_url")))])
            for x in matches
        ):
            issues.append(f"{record_id}:claim_evidence_not_literal_now:{path}")
    return issues


def _md_escape(s: Any) -> str:
    return clean(s).replace("|", "\\|")


def _write_markdown(path: Path, audit: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append(f"# Grants Radar Audit — {audit['verified_date']}")
    lines.append("")
    lines.append(f"**Overall:** {'PASS' if audit['publishable'] else 'FAIL'}")
    lines.append("")
    lines.append("## Gate summary")
    lines.append("")
    for k, v in audit["gates"].items():
        lines.append(f"- **{k}:** {'PASS' if v.get('pass') else 'FAIL'} — {v.get('detail','')}")
    lines.append("")
    lines.append("## Programme audit")
    lines.append("")
    lines.append("| Programme | Result | Independent confidence | Issues |")
    lines.append("|---|---:|---:|---|")
    for row in audit.get("records") or []:
        lines.append(
            f"| {_md_escape(row.get('name'))} | {'PASS' if row.get('pass') else 'FAIL'} | "
            f"{float(row.get('validator_confidence') or 0):.3f} | {_md_escape('; '.join(row.get('issues') or []))} |"
        )
    lines.append("")
    lines.append("## Completeness discovery")
    lines.append("")
    lines.append(f"Mandatory sources scanned: **{audit.get('mandatory_sources_ok',0)}/{audit.get('mandatory_sources_total',0)}**")
    lines.append("")
    lines.append(f"Unresolved candidates: **{len(audit.get('unresolved_candidates') or [])}**")
    for c in audit.get("unresolved_candidates") or []:
        lines.append(f"- {_md_escape(c.get('title') or c.get('url'))}: {_md_escape(c.get('reason'))}")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append(
        "A PASS means every published factual record is supported by live primary/administering-body evidence, "
        "and all candidates found in the mandatory discovery universe were adjudicated. It is not a mathematical "
        "guarantee that an unannounced or unindexed programme does not exist outside that source universe."
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
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
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    candidates = json.loads(args.candidates.read_text(encoding="utf-8"))
    coverage = json.loads(args.coverage.read_text(encoding="utf-8"))

    audit: Dict[str, Any] = {
        "verified_date": verified.isoformat(),
        "scope_definition": scope,
        "publishable": False,
        "gates": {},
        "records": [],
        "unresolved_candidates": [x for x in (candidates.get("candidates") or []) if x.get("resolution") == "unresolved"],
        "mandatory_sources_total": 0,
        "mandatory_sources_ok": 0,
    }

    # Gate A: completeness source coverage.
    required_rows = [x for x in (coverage.get("sources") or []) if x.get("required")]
    audit["mandatory_sources_total"] = len(required_rows)
    audit["mandatory_sources_ok"] = sum(1 for x in required_rows if x.get("ok"))
    truncated_rows = [x for x in required_rows if x.get("truncated")]
    failed_candidate_fetches = sum(int(x.get("candidate_pages_failed") or 0) for x in required_rows)
    source_coverage_pass = (
        bool(required_rows)
        and audit["mandatory_sources_ok"] == audit["mandatory_sources_total"]
        and not truncated_rows
        and failed_candidate_fetches == 0
    )
    audit["gates"]["mandatory_source_coverage"] = {
        "pass": source_coverage_pass,
        "detail": (
            f"{audit['mandatory_sources_ok']}/{audit['mandatory_sources_total']} mandatory sources scanned successfully; "
            f"truncated={len(truncated_rows)}; candidate_fetch_failures={failed_candidate_fetches}"
        ),
    }

    # Gate B: all jurisdictions represented in mandatory discovery coverage.
    required_jur = {clean(x.get("jurisdiction")) for x in source_list if x.get("required", True)}
    covered_jur = {clean(x.get("jurisdiction")) for x in required_rows if x.get("ok")}
    missing_jur = sorted(j for j in required_jur if j and j not in covered_jur)
    jurisdiction_pass = not missing_jur
    audit["gates"]["jurisdiction_coverage"] = {
        "pass": jurisdiction_pass,
        "detail": "all required jurisdictions covered" if not missing_jur else "missing: " + ", ".join(missing_jur),
    }

    # Gate C: no unresolved discovery candidate.
    candidates_pass = len(audit["unresolved_candidates"]) == 0
    audit["gates"]["candidate_adjudication"] = {
        "pass": candidates_pass,
        "detail": f"{len(audit['unresolved_candidates'])} unresolved candidates",
    }

    # Gate D: every successfully discovered candidate URL was explicitly adjudicated.
    # This prevents a fetched page from disappearing silently between discovery and publication.
    expected_by_source = {}
    for row in coverage.get("sources") or []:
        sid = clean(row.get("source_id"))
        expected_by_source[sid] = {canonical_url(u) for u in (row.get("candidate_urls") or []) if clean(u)}
    resolved_by_source = {}
    for row in candidates.get("candidates") or []:
        sid = clean(row.get("source_id"))
        if clean(row.get("url")):
            resolved_by_source.setdefault(sid, set()).add(canonical_url(clean(row.get("url"))))
    reconciliation_missing = {}
    for sid, expected in expected_by_source.items():
        resolved = resolved_by_source.get(sid, set())
        missing = sorted(expected - resolved)
        if missing:
            reconciliation_missing[sid] = missing
    reconciliation_pass = not reconciliation_missing
    audit["gates"]["candidate_reconciliation"] = {
        "pass": reconciliation_pass,
        "detail": "every discovered candidate URL explicitly adjudicated" if reconciliation_pass else f"unadjudicated candidate URLs: {sum(len(v) for v in reconciliation_missing.values())}",
        "missing": reconciliation_missing,
    }

    # Gate E: updater found no pre-audit verification failure.
    pre_failures = evidence.get("verification_failures") or []
    preaudit_pass = len(pre_failures) == 0
    audit["gates"]["preaudit_extraction"] = {
        "pass": preaudit_pass,
        "detail": f"{len(pre_failures)} pre-audit programme failures",
    }

    # Gate F: independent programme-by-programme verification.
    visible = [dict(x) for x in (grants_raw.get("grants") or []) if isinstance(x, dict) and _visible(x, verified)]
    record_fail_count = 0
    validator_min = float((sources_raw.get("thresholds") or {}).get("validator_min_confidence", 0.98))

    for rec in visible:
        rid = clean(rec.get("id"))
        issues = validate_record_schema(rec)
        dl = parse_date(rec.get("deadline"))
        if dl and clean(rec.get("status")) in {"Open now", "Opening soon"} and dl < verified:
            issues.append("status_deadline_inconsistent_for_verification_date")
        if clean(rec.get("last_verified")) != verified.isoformat():
            issues.append("last_verified_not_current_run")
        ledger = (evidence.get("records") or {}).get(rid)
        if not ledger:
            issues.append("missing_evidence_ledger")
            audit["records"].append({"id": rid, "name": rec.get("name"), "pass": False, "issues": issues, "validator_confidence": 0})
            record_fail_count += 1
            continue
        if ledger.get("preaudit_issues"):
            issues.extend([f"preaudit:{x}" for x in ledger.get("preaudit_issues")])

        evidence_map = ledger.get("evidence") or {}
        source_urls = [clean(rec.get("url"))]
        source_urls += [clean((evidence_map.get(f) or {}).get("source_url")) for f in required_evidence_fields(rec)]
        source_urls += [clean(x.get("source_url")) for x in (ledger.get("claim_evidence") or []) if isinstance(x, dict)]
        source_urls += [clean(x.get("source_url")) for x in (ledger.get("supporting_claims") or []) if isinstance(x, dict)]
        source_urls = [u for i, u in enumerate(source_urls) if u and u not in source_urls[:i]]

        # Evidence must come from the programme's primary/administering domain or another allowed domain in the same source group.
        group = _source_group(clean(rec.get("url")), source_list)
        allowed = [str(x).lower().removeprefix("www.") for x in ((group or {}).get("allowed_domains") or [domain(clean(rec.get("url")))])]
        for f in required_evidence_fields(rec):
            eu = clean((evidence_map.get(f) or {}).get("source_url"))
            if eu:
                ed = domain(eu)
                if not any(ed == a or ed.endswith("." + a) for a in allowed):
                    issues.append(f"evidence_not_primary_domain:{f}:{ed}")

        fetched_text: Dict[str, str] = {}
        source_bundle: List[Tuple[str, str]] = []
        fetch_errors: List[str] = []
        for u in source_urls[:7]:
            f = fetch_url(u)
            if not f.ok:
                fetch_errors.append(f"{u}:{f.error}")
                continue
            cu = canonical_url(f.final_url)
            fetched_text[cu] = f.text
            source_bundle.append((f.final_url, f.text))
        if fetch_errors:
            issues.extend([f"source_fetch_failed:{x}" for x in fetch_errors])
        issues.extend(_evidence_literal_issues(rec, ledger, fetched_text))

        validator: Dict[str, Any] = {"supported": False, "confidence": 0, "material_issues": ["not_run"]}
        if source_bundle:
            try:
                validator = independent_validate(record=rec, source_bundle=source_bundle, verified=verified, scope_text=scope)
            except Exception as exc:
                issues.append(f"validator_error:{clean(exc)}")
        if not validator.get("supported"):
            issues.append("independent_validator_rejected")
        vconf = float(validator.get("confidence") or 0)
        if vconf < validator_min:
            issues.append(f"validator_confidence_below_gate:{vconf:.3f}")
        for x in validator.get("material_issues") or []:
            issues.append(f"validator:{clean(x)}")
        for x in validator.get("contradictions") or []:
            issues.append(f"contradiction:{clean(x)}")

        passed = not issues
        if not passed:
            record_fail_count += 1
        audit["records"].append({
            "id": rid,
            "name": rec.get("name"),
            "pass": passed,
            "issues": sorted(set(issues)),
            "validator_confidence": vconf,
            "field_checks": validator.get("field_checks") or {},
            "source_urls": [u for u, _ in source_bundle],
        })
        print(f"[audit] {'PASS' if passed else 'FAIL'} {rid} issues={len(issues)}")

    records_pass = bool(visible) and record_fail_count == 0
    audit["gates"]["all_published_records"] = {
        "pass": records_pass,
        "detail": f"{len(visible)-record_fail_count}/{len(visible)} visible records passed independent audit",
    }

    # No duplicate IDs or URLs among visible records.
    ids = [clean(x.get("id")) for x in visible]
    urls = [canonical_url(clean(x.get("url"))) for x in visible]
    dupes = sorted({x for x in ids if x and ids.count(x) > 1} | {u for u in urls if u and urls.count(u) > 1})
    dedupe_pass = not dupes
    audit["gates"]["uniqueness"] = {
        "pass": dedupe_pass,
        "detail": "no duplicate IDs/URLs" if not dupes else "duplicates: " + ", ".join(dupes),
    }

    audit["publishable"] = all(bool(g.get("pass")) for g in audit["gates"].values())
    audit["summary"] = {
        "visible_programmes": len(visible),
        "programmes_passed": len(visible) - record_fail_count,
        "unresolved_candidates": len(audit["unresolved_candidates"]),
        "mandatory_sources_total": audit["mandatory_sources_total"],
        "mandatory_sources_ok": audit["mandatory_sources_ok"],
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
