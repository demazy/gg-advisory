# -*- coding: utf-8 -*-
"""Audited v5.1 update stage for the GG Advisory Funding Radar.

v5.1 removes the API-call explosion seen in the first production v5 run. Each mandatory
source group is searched once. That one search both re-verifies the baseline programmes on
that source and discovers new programmes, with full field evidence embedded in the result.
Only a missing/invalid baseline record may trigger a targeted fallback search. New candidates
are adjudicated from the already-retrieved source evidence; they do not each trigger another
live-search call.
"""
from __future__ import annotations

PIPELINE_VERSION = "5.1-batched-source-audit"

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from grants_core import (
    WEB_MODEL,
    canonical_url,
    clean,
    discover_programmes_via_web,
    domain,
    extract_program_via_web,
    json_dump,
    parse_date,
    record_similarity,
    scope_text,
    slugify,
    url_on_allowed_domain,
    validate_record_schema,
    yaml_dump,
    yaml_load,
)
from grants_history import apply_history, validate_history


def _visible(entry: Dict[str, Any], verified: date) -> bool:
    if entry.get("include_in_report") is False:
        return False
    sf = parse_date(entry.get("show_from"))
    su = parse_date(entry.get("show_until"))
    if sf and verified < sf:
        return False
    # Historical visibility windows from the old hand-maintained radar apply only to rows
    # that have never received a live status. Once audited, status controls visibility.
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
    src = _source_for_url(clean(entry.get("url")), sources)
    if src:
        return sorted(set(src.get("allowed_domains") or []))
    level = clean(entry.get("level") or "national").lower()
    domains: List[str] = []
    current_domain = domain(clean(entry.get("url")))
    if current_domain:
        domains.append(current_domain)
    for s in sources:
        if clean(s.get("jurisdiction") or "national").lower() == level:
            domains.extend(s.get("allowed_domains") or [])
    return sorted({str(x).lower().removeprefix("www.") for x in domains if clean(x)})


def _candidate_as_result(candidate: Dict[str, Any], source_row: Dict[str, Any]) -> Dict[str, Any]:
    """Adapt one detailed programme from a source batch to the normal record validator."""
    payload = {
        "record": dict(candidate.get("record") or {}),
        "field_evidence": dict(candidate.get("field_evidence") or {}),
        "claim_evidence": list(candidate.get("claim_evidence") or []),
        "source_urls": list(candidate.get("source_urls") or []),
        "unresolved_conflict": bool(candidate.get("unresolved_conflict")),
        "conflict_notes": list(candidate.get("conflict_notes") or []),
        "overall_confidence": float(candidate.get("overall_confidence") or candidate.get("confidence") or 0),
        "in_scope": candidate.get("in_scope") is not False,
    }
    return {
        "data": payload,
        "tool_source_urls": list(source_row.get("tool_source_urls") or []),
        "model": source_row.get("model"),
        "response_id": source_row.get("response_id"),
        "search_response_id": source_row.get("search_response_id") or source_row.get("response_id"),
        "structure_response_id": source_row.get("structure_response_id"),
        "search_evidence_sha256": source_row.get("search_evidence_sha256"),
    }


def _validate_web_record(
    *,
    old: Optional[Dict[str, Any]],
    result: Dict[str, Any],
    allowed_domains: Sequence[str],
    verified: date,
    hard_min_confidence: float,
) -> Tuple[Optional[Dict[str, Any]], List[str], Dict[str, Any]]:
    payload = dict(result.get("data") or {})
    rec = dict(payload.get("record") or {})
    issues: List[str] = []
    warnings: List[str] = []

    if old:
        rec["id"] = clean(old.get("id")) or slugify(clean(rec.get("name")))
        # Preserve manual controls/history but never stale factual content.
        for k in ("include_in_report", "history"):
            if k in old:
                rec[k] = old[k]
    elif not clean(rec.get("id")):
        rec["id"] = slugify(clean(rec.get("name")))

    rec["last_verified"] = verified.isoformat()
    rec["show_until"] = None
    if not rec.get("show_from"):
        rec["show_from"] = (old or {}).get("show_from") or verified.replace(day=1).isoformat()

    issues.extend(validate_record_schema(rec))
    conf = float(payload.get("overall_confidence") or 0)
    if not result.get("tool_source_urls"):
        issues.append("no_official_web_search_sources")
    # v5's 0.97/0.98 self-confidence gates were not calibrated and created false failures.
    # Confidence is now diagnostic unless it is genuinely very low.
    if conf < hard_min_confidence:
        issues.append(f"extract_confidence_too_low:{conf:.3f}")
    elif conf < 0.90:
        warnings.append(f"extract_confidence_warning:{conf:.3f}")
    if payload.get("unresolved_conflict"):
        issues.append("unresolved_source_conflict")

    in_scope = payload.get("in_scope") is not False
    if not in_scope:
        if old:
            rec["status"] = "Archived"
            rec["include_in_report"] = False
            rec["show_until"] = verified.isoformat()
        else:
            issues.append("programme_out_of_scope")

    candidate_urls: List[str] = []
    for u in [clean(rec.get("url"))] + list(payload.get("source_urls") or []) + list(result.get("tool_source_urls") or []):
        if u and u not in candidate_urls:
            candidate_urls.append(u)
    for ev in (payload.get("field_evidence") or {}).values():
        if isinstance(ev, dict) and clean(ev.get("source_url")):
            candidate_urls.append(clean(ev.get("source_url")))
    for ev in payload.get("claim_evidence") or []:
        if isinstance(ev, dict) and clean(ev.get("source_url")):
            candidate_urls.append(clean(ev.get("source_url")))
    bad = sorted({u for u in candidate_urls if u and not url_on_allowed_domain(u, allowed_domains)})
    if bad:
        issues.append("non_official_source_urls:" + ",".join(bad[:5]))

    # Every structured field displayed in the Radar must have source-grounded evidence.
    ev = payload.get("field_evidence") or {}
    for f in ("name", "admin", "status", "amount", "deadline_label", "target_stage"):
        row = ev.get(f) or {}
        if not clean(row.get("source_url")) or not clean(row.get("support")):
            issues.append(f"missing_field_evidence:{f}")
    if clean(rec.get("deadline_type")).lower() == "fixed":
        row = ev.get("deadline") or {}
        if not clean(row.get("source_url")) or not clean(row.get("support")):
            issues.append("missing_field_evidence:deadline")

    ledger = {
        "record": rec,
        "field_evidence": ev,
        "claim_evidence": payload.get("claim_evidence") or [],
        "source_urls": sorted(set(candidate_urls)),
        "tool_source_urls": result.get("tool_source_urls") or [],
        "extract_model": result.get("model"),
        "extract_response_id": result.get("response_id"),
        "search_response_id": result.get("search_response_id"),
        "structure_response_id": result.get("structure_response_id"),
        "search_evidence_sha256": result.get("search_evidence_sha256"),
        "extract_confidence": conf,
        "conflict_notes": payload.get("conflict_notes") or [],
        "in_scope": in_scope,
        "preaudit_issues": sorted(set(issues)),
        "preaudit_warnings": sorted(set(warnings)),
    }
    return rec if rec else None, sorted(set(issues)), ledger


def _candidate_match(candidate: Dict[str, Any], entries: Sequence[Dict[str, Any]]) -> Tuple[float, Optional[Dict[str, Any]]]:
    url = clean(candidate.get("url"))
    name = clean(candidate.get("name"))
    cand_level = clean(candidate.get("level") or candidate.get("jurisdiction")).lower()
    best = 0.0
    best_entry: Optional[Dict[str, Any]] = None
    for e in entries:
        entry_level = clean(e.get("level")).lower()
        # Same-named state programmes are not duplicates across jurisdictions. Exact URL
        # remains sufficient because a cross-jurisdiction mirror can still be the same record.
        exact_url = bool(canonical_url(clean(e.get("url"))) and canonical_url(clean(e.get("url"))) == canonical_url(url))
        if cand_level and entry_level and cand_level != entry_level and not exact_url:
            continue
        score = record_similarity(e, name, url)
        if score > best:
            best = score
            best_entry = e
    return best, best_entry


def _discover_one_source(
    s: Dict[str, Any], *, scope: str, verified: date, known: Sequence[Dict[str, Any]], model: str
) -> Dict[str, Any]:
    sid = clean(s.get("id"))
    allowed = list(s.get("allowed_domains") or [])
    row: Dict[str, Any] = {
        "source_id": sid,
        "jurisdiction": clean(s.get("jurisdiction") or "national"),
        "required": bool(s.get("required", True)),
        "allowed_domains": allowed,
        "ok": False,
        "errors": [],
        "warnings": [],
        "known_ids": [clean(x.get("id")) for x in known],
        "programmes": [],
        "tool_source_urls": [],
    }
    try:
        res = discover_programmes_via_web(
            source_id=sid,
            jurisdiction=row["jurisdiction"],
            allowed_domains=allowed,
            scope_text=scope,
            verified=verified,
            known_programmes=known,
            pass_name="primary",
            model=model,
        )
        data = dict(res.get("data") or {})
        programmes = [x for x in (data.get("programmes") or []) if isinstance(x, dict)]
        conf = float(data.get("coverage_confidence") or 0)
        tool_sources = list(res.get("tool_source_urls") or [])
        row.update({
            "coverage_confidence": conf,
            "search_notes": clean(data.get("search_notes")),
            "programmes": programmes,
            "tool_source_urls": tool_sources,
            "model": res.get("model"),
            "response_id": res.get("response_id"),
            "search_response_id": res.get("search_response_id"),
            "structure_response_id": res.get("structure_response_id"),
            "search_evidence_sha256": res.get("search_evidence_sha256"),
        })
        # Completeness confidence is a subjective model score, not a calibrated probability.
        # The hard gate is that every configured source search actually ran and returned
        # official-domain web-search provenance. Low self-confidence remains visible as a warning.
        row["ok"] = bool(tool_sources)
        warn_floor = float(s.get("coverage_warning_below", 0.75))
        if conf < warn_floor:
            row["warnings"].append(f"coverage_confidence_warning:{conf:.3f}<{warn_floor:.3f}")
        if not tool_sources:
            row["errors"].append("no_official_web_search_sources")
    except Exception as exc:
        row["errors"].append(f"web_discovery_error:{clean(exc)}")
    return row


def main() -> None:
    print(f"[pipeline] version={PIPELINE_VERSION} discovery=single-search-per-mandatory-source")
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--sources", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--evidence", type=Path, required=True)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--coverage", type=Path, required=True)
    ap.add_argument("--verified", required=True)
    args = ap.parse_args()

    verified = parse_date(args.verified)
    if not verified:
        raise SystemExit("--verified must be YYYY-MM-DD")

    raw = yaml_load(args.input)
    source_cfg = yaml_load(args.sources)
    source_list = list(source_cfg.get("sources") or [])
    scope = scope_text(source_cfg)
    thresholds = source_cfg.get("thresholds") or {}
    hard_extract_min = float(thresholds.get("extract_hard_min_confidence", 0.70))
    new_candidate_min = float(thresholds.get("new_candidate_min_confidence", 0.90))
    match_min = float(thresholds.get("match_similarity", 0.93))
    web_model = clean(os.getenv("GRANTS_WEB_MODEL", WEB_MODEL)) or WEB_MODEL
    workers = max(1, min(5, int(os.getenv("GRANTS_WEB_WORKERS", "3"))))

    entries: List[Dict[str, Any]] = [dict(x) for x in (raw.get("grants") or []) if isinstance(x, dict)]
    visible = [x for x in entries if _visible(x, verified)]
    invisible = [x for x in entries if not _visible(x, verified)]

    evidence_ledger: Dict[str, Any] = {
        "verified_date": verified.isoformat(),
        "scope_definition": scope,
        "pipeline": "audited-v5.1-batched-source-audit",
        "records": {},
        "verification_failures": [],
        "continuity": {
            "baseline_visible_ids": [clean(x.get("id")) for x in visible],
            "dispositions": [],
            "unexplained_disappearances": [],
        },
    }

    # Assign every visible baseline record to the source group owning its current URL.
    known_by_source: Dict[str, List[Dict[str, Any]]] = {clean(s.get("id")): [] for s in source_list}
    unassigned: List[Dict[str, Any]] = []
    for rec in visible:
        src = _source_for_url(clean(rec.get("url")), source_list)
        if src:
            known_by_source[clean(src.get("id"))].append(rec)
        else:
            unassigned.append(rec)

    # A. Search every mandatory source exactly once. The same response includes fresh snapshots
    # for baseline records and any new discoveries.
    coverage_rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(
                _discover_one_source,
                s,
                scope=scope,
                verified=verified,
                known=known_by_source.get(clean(s.get("id")), []),
                model=web_model,
            ): s
            for s in source_list
        }
        for fut in as_completed(futs):
            row = fut.result()
            coverage_rows.append(row)
            extra = f" errors={' | '.join(row.get('errors') or [])}" if row.get("errors") else ""
            warnings = f" warnings={' | '.join(row.get('warnings') or [])}" if row.get("warnings") else ""
            print(f"[discovery] {row['source_id']}: search_ok={row['ok']} programmes={len(row.get('programmes') or [])}{extra}{warnings}")
    coverage_rows.sort(key=lambda r: clean(r.get("source_id")))
    source_rows = {clean(r.get("source_id")): r for r in coverage_rows}

    proposed_entries: List[Dict[str, Any]] = []
    verification_failures: List[Dict[str, Any]] = []
    used_candidate_keys: set[Tuple[str, int]] = set()

    # B. Freshly reverify every visible baseline record from its source-batch result. A targeted
    # fallback search is allowed only if the batch omitted or could not support that baseline.
    for old in entries:
        rid = clean(old.get("id"))
        if old in invisible:
            proposed_entries.append(old)
            continue
        src = _source_for_url(clean(old.get("url")), source_list)
        row = source_rows.get(clean((src or {}).get("id"))) if src else None
        candidates = list((row or {}).get("programmes") or [])
        chosen: Optional[Dict[str, Any]] = None
        chosen_idx = -1

        for i, c in enumerate(candidates):
            if clean(c.get("baseline_id")) == rid:
                chosen, chosen_idx = c, i
                break
        if chosen is None:
            best_score = 0.0
            for i, c in enumerate(candidates):
                score = record_similarity(old, clean(c.get("name")), clean(c.get("url")))
                if score > best_score:
                    best_score, chosen, chosen_idx = score, c, i
            if best_score < 0.84:
                chosen, chosen_idx = None, -1

        allowed = _allowed_domains_for_record(old, source_list)
        rec: Optional[Dict[str, Any]] = None
        issues: List[str] = []
        ledger: Dict[str, Any] = {}
        if chosen is not None and row is not None:
            result = _candidate_as_result(chosen, row)
            rec, issues, ledger = _validate_web_record(
                old=old,
                result=result,
                allowed_domains=allowed,
                verified=verified,
                hard_min_confidence=hard_extract_min,
            )
            used_candidate_keys.add((clean(row.get("source_id")), chosen_idx))

        # A source batch with incomplete field evidence should not permanently poison the run.
        # Make one targeted official-domain fallback for that baseline only.
        if rec is None or issues:
            try:
                fallback = extract_program_via_web(
                    current=old,
                    allowed_domains=allowed,
                    verified=verified,
                    jurisdiction_hint=clean(old.get("level") or "national"),
                    scope_text=scope,
                    model=web_model,
                )
                frec, fissues, fledger = _validate_web_record(
                    old=old,
                    result=fallback,
                    allowed_domains=allowed,
                    verified=verified,
                    hard_min_confidence=hard_extract_min,
                )
                if frec is not None and len(fissues) <= len(issues or ["missing_batch_record"]):
                    rec, issues, ledger = frec, fissues, fledger
                    ledger["verification_route"] = "targeted_fallback_after_source_batch"
            except Exception as exc:
                if rec is None:
                    rec = old
                issues = sorted(set((issues or []) + [f"fallback_extract_error:{clean(exc)}"]))
                ledger = ledger or {"preaudit_issues": issues, "allowed_domains": allowed}

        if rec is None:
            rec = old
            issues.append("empty_extracted_record")

        rec, disposition, changed_fields, event_added = apply_history(
            old, rec, verified_date=verified.isoformat(), source_urls=ledger.get("source_urls") or []
        )
        hist_issues = validate_history(rec)
        issues.extend([f"history:{x}" for x in hist_issues])
        proposed_entries.append(rec)
        ledger["continuity_disposition"] = disposition
        ledger["changed_fields"] = changed_fields
        ledger["history_event_added"] = event_added
        evidence_ledger["records"][rid] = ledger
        evidence_ledger["continuity"]["dispositions"].append({
            "id": rid,
            "disposition": disposition,
            "changed_fields": changed_fields,
            "history_event_added": event_added,
            "still_in_registry": True,
            "include_in_report": rec.get("include_in_report") is not False and clean(rec.get("status")) != "Archived",
        })
        if issues:
            issues = sorted(set(issues))
            verification_failures.append({"id": rid, "issues": issues})
            print(f"[verify] FAIL {rid} issues={len(issues)} first={issues[0]}")
        else:
            print(f"[verify] PASS {rid} disposition={disposition} changes={','.join(changed_fields) if changed_fields else '-'}")

    # Baseline records without any configured source owner are a hard continuity failure.
    for rec in unassigned:
        rid = clean(rec.get("id"))
        if not any(x.get("id") == rid for x in verification_failures):
            verification_failures.append({"id": rid, "issues": ["no_mandatory_source_group_for_baseline_url"]})

    # C. Reconcile NEW discoveries from the already-completed source searches. No per-candidate
    # live API calls are made here.
    candidates_out: List[Dict[str, Any]] = []
    new_seen: List[Dict[str, Any]] = []
    strong_scope = {"climate_specific", "explicit_priority_fit"}

    source_order = {clean(r.get("source_id")): i for i, r in enumerate(coverage_rows)}
    candidate_stream: List[Tuple[float, str, int, Dict[str, Any], Dict[str, Any]]] = []
    for row in coverage_rows:
        sid = clean(row.get("source_id"))
        for i, c in enumerate(row.get("programmes") or []):
            if (sid, i) in used_candidate_keys or clean(c.get("baseline_id")):
                continue
            candidate_stream.append((float(c.get("confidence") or 0), sid, i, c, row))
    candidate_stream.sort(key=lambda x: (-x[0], x[1], clean(x[3].get("name"))))

    for conf, sid, idx, c, row in candidate_stream:
        name = clean(c.get("name"))
        url = clean(c.get("url"))
        scope_basis = clean(c.get("scope_basis")).lower()
        scope_ev = c.get("scope_evidence") or {}
        out: Dict[str, Any] = {
            "source_id": sid,
            "jurisdiction": clean(row.get("jurisdiction") or "national"),
            "origin": "source_search",
            "name": name,
            "url": url,
            "confidence": conf,
            "reason": clean(c.get("reason")),
            "scope_basis": scope_basis,
        }
        if c.get("in_scope") is False or scope_basis == "out_of_scope":
            out["resolution"] = "excluded_out_of_scope"
            candidates_out.append(out)
            continue
        if conf < new_candidate_min:
            out["resolution"] = "excluded_unconfirmed_signal"
            out["reason"] = f"discovery_confidence_below_new_candidate_gate:{conf:.3f}<{new_candidate_min:.3f}"
            candidates_out.append(out)
            continue
        if scope_basis not in strong_scope or not clean(scope_ev.get("source_url")) or not clean(scope_ev.get("support")):
            out["resolution"] = "excluded_scope_not_proven"
            out["reason"] = "new programme lacked explicit source-grounded climate/clean-energy priority fit"
            candidates_out.append(out)
            continue
        if not name or not url or not url_on_allowed_domain(url, row.get("allowed_domains") or []):
            out["resolution"] = "unresolved"
            out["reason"] = "missing_or_nonofficial_candidate_identity"
            candidates_out.append(out)
            continue

        best, best_entry = _candidate_match({**c, "jurisdiction": out["jurisdiction"]}, proposed_entries)
        exact = bool(best_entry) and canonical_url(clean(best_entry.get("url"))) == canonical_url(url)
        if exact or best >= match_min:
            out.update({"resolution": "matched_existing", "matched_id": clean((best_entry or {}).get("id")), "similarity": round(best, 4)})
            candidates_out.append(out)
            continue

        # De-duplicate the same new programme surfaced by multiple mandatory source groups.
        dup_match = None
        dup_score = 0.0
        for prev in new_seen:
            score = _candidate_match({**c, "jurisdiction": out["jurisdiction"]}, [prev])[0]
            if score > dup_score:
                dup_score, dup_match = score, prev
        if dup_match is not None and dup_score >= match_min:
            out.update({"resolution": "duplicate_discovery", "matched_id": clean(dup_match.get("id")), "similarity": round(dup_score, 4)})
            candidates_out.append(out)
            continue

        result = _candidate_as_result(c, row)
        rec, issues, ledger = _validate_web_record(
            old=None,
            result=result,
            allowed_domains=row.get("allowed_domains") or [],
            verified=verified,
            hard_min_confidence=hard_extract_min,
        )
        if rec:
            base_id = slugify(clean(rec.get("name")))
            existing_ids = {clean(x.get("id")) for x in proposed_entries}
            rec["id"] = base_id if base_id not in existing_ids else slugify(f"{clean(rec.get('level'))}-{clean(rec.get('name'))}")
        if not rec or issues:
            out["resolution"] = "unresolved"
            out["reason"] = "new_record_evidence_incomplete_or_conflicted"
            out["issues"] = sorted(set(issues or ["empty_new_record"]))
            candidates_out.append(out)
            continue

        rec, disposition, changed_fields, event_added = apply_history(
            None, rec, verified_date=verified.isoformat(), source_urls=ledger.get("source_urls") or []
        )
        hist_issues = validate_history(rec)
        if hist_issues:
            out["resolution"] = "unresolved"
            out["reason"] = "new_record_history_invalid"
            out["issues"] = hist_issues
            candidates_out.append(out)
            continue
        proposed_entries.append(rec)
        new_seen.append(rec)
        ledger["continuity_disposition"] = "added"
        ledger["changed_fields"] = changed_fields
        ledger["history_event_added"] = event_added
        evidence_ledger["records"][rec["id"]] = ledger
        evidence_ledger["continuity"]["dispositions"].append({
            "id": rec["id"], "disposition": "added", "changed_fields": changed_fields,
            "history_event_added": event_added, "still_in_registry": True,
            "include_in_report": rec.get("include_in_report") is not False,
        })
        out.update({"resolution": "auto_added_pending_independent_audit", "record_id": rec["id"]})
        candidates_out.append(out)
        print(f"[discover] ADD {rec['id']}")

    # D. Jurisdiction completeness is now an aggregation over the mandatory source universe,
    # not nine additional expensive model searches. Every jurisdiction passes only if every
    # required configured source in that jurisdiction actually completed with official provenance.
    jurisdiction_rows: List[Dict[str, Any]] = []
    jurisdictions = sorted({clean(s.get("jurisdiction") or "national") for s in source_list})
    for jur in jurisdictions:
        ss = [s for s in source_list if clean(s.get("jurisdiction") or "national") == jur and bool(s.get("required", True))]
        ids = [clean(s.get("id")) for s in ss]
        failed = [sid for sid in ids if not (source_rows.get(sid) or {}).get("ok")]
        jurisdiction_rows.append({
            "jurisdiction": jur,
            "ok": not failed and bool(ids),
            "method": "mandatory_source_aggregation",
            "required_source_ids": ids,
            "failed_source_ids": failed,
            "detail": f"{len(ids)-len(failed)}/{len(ids)} mandatory source searches completed with official provenance",
        })
        print(f"[coverage] {jur}: {'PASS' if not failed and ids else 'FAIL'} {len(ids)-len(failed)}/{len(ids)} mandatory sources")

    evidence_ledger["verification_failures"] = verification_failures
    evidence_ledger["unresolved_candidates"] = [x for x in candidates_out if x.get("resolution") == "unresolved"]
    proposed_ids = {clean(x.get("id")) for x in proposed_entries}
    disposition_ids = {clean(x.get("id")) for x in evidence_ledger["continuity"]["dispositions"]}
    baseline_ids = set(evidence_ledger["continuity"]["baseline_visible_ids"])
    unexplained = sorted(x for x in baseline_ids if x not in proposed_ids or x not in disposition_ids)
    evidence_ledger["continuity"]["unexplained_disappearances"] = unexplained

    # Stable order: original registry order first, newly discovered records by jurisdiction/name.
    original_order = {clean(x.get("id")): i for i, x in enumerate(entries)}
    proposed_entries.sort(key=lambda x: (0, original_order[clean(x.get("id"))]) if clean(x.get("id")) in original_order else (1, clean(x.get("level")), clean(x.get("name"))))
    out_doc = dict(raw)
    out_doc["grants"] = proposed_entries
    out_doc.setdefault("metadata", {})
    out_doc["metadata"].update({
        "candidate_verified_date": verified.isoformat(),
        "pipeline": PIPELINE_VERSION,
        "registry_role": "canonical_source_of_truth_with_change_history",
    })

    args.output.parent.mkdir(parents=True, exist_ok=True)
    yaml_dump(args.output, out_doc)
    json_dump(args.evidence, evidence_ledger)
    json_dump(args.candidates, {"verified_date": verified.isoformat(), "candidates": candidates_out})
    json_dump(args.coverage, {
        "verified_date": verified.isoformat(),
        "pipeline": PIPELINE_VERSION,
        "sources": coverage_rows,
        "jurisdiction_crosschecks": jurisdiction_rows,
    })

    print(f"[write] candidate grants: {args.output}")
    print(f"[write] evidence: {args.evidence}")
    print(f"[write] candidates: {args.candidates}")
    print(f"[write] coverage: {args.coverage}")
    print(f"[summary] tracked verification failures={len(verification_failures)} unresolved discovery candidates={len(evidence_ledger['unresolved_candidates'])}")


if __name__ == "__main__":
    main()
