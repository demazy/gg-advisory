# -*- coding: utf-8 -*-
"""Audited v3 update stage for the GG Advisory Funding Radar.

Key design changes from v2:
- completeness discovery is performed with domain-restricted web search on official/administering domains;
- no whole-site sitemap crawl is treated as a programme universe;
- every tracked record is freshly re-verified with live web search, even when its stored URL is stale;
- a second jurisdiction-level completeness pass searches for programmes omitted by source-level passes;
- nothing is published here: the independent audit must still pass.
"""
from __future__ import annotations

PIPELINE_VERSION = "3.1-web-search"

import argparse
import json
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


def _visible(entry: Dict[str, Any], verified: date) -> bool:
    if entry.get("include_in_report") is False:
        return False
    sf = parse_date(entry.get("show_from"))
    su = parse_date(entry.get("show_until"))
    if sf and verified < sf:
        return False
    # Historical visibility windows are respected only until the first audited run.
    # Once a live status exists, current status controls visibility.
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


def _validate_web_record(
    *,
    old: Optional[Dict[str, Any]],
    result: Dict[str, Any],
    allowed_domains: Sequence[str],
    verified: date,
    min_confidence: float,
) -> Tuple[Optional[Dict[str, Any]], List[str], Dict[str, Any]]:
    payload = dict(result.get("data") or {})
    rec = dict(payload.get("record") or {})
    issues: List[str] = []
    if old:
        rec["id"] = clean(old.get("id")) or slugify(clean(rec.get("name")))
        # Preserve manual visibility controls but do not preserve stale factual content.
        for k in ("include_in_report",):
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
    if conf < min_confidence:
        issues.append(f"low_extract_confidence:{conf:.3f}")
    if payload.get("unresolved_conflict"):
        issues.append("unresolved_source_conflict")
    if payload.get("in_scope") is False:
        issues.append("programme_out_of_scope")

    # Every URL written into the record/evidence must remain on the constrained official domains.
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

    # Evidence is semantic rather than forced word-for-word because many official pages are dynamically rendered.
    # Critical fields must still have a source URL + support fragment, and a second independent live-web audit follows.
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
        "extract_confidence": conf,
        "conflict_notes": payload.get("conflict_notes") or [],
        "preaudit_issues": sorted(set(issues)),
    }
    return rec if rec else None, sorted(set(issues)), ledger


def _discover_one_source(
    s: Dict[str, Any],
    *,
    scope: str,
    verified: date,
    known: Sequence[Dict[str, Any]],
    model: str,
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
        "passes": [],
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
        row["passes"].append({
            "pass_name": "primary",
            "coverage_confidence": conf,
            "search_notes": clean(data.get("search_notes")),
            "programmes": programmes,
            "tool_source_urls": res.get("tool_source_urls") or [],
            "model": res.get("model"),
            "response_id": res.get("response_id"),
        })
        threshold = float(s.get("coverage_min_confidence", 0.90))
        row["ok"] = conf >= threshold
        if not row["ok"]:
            row["errors"].append(f"coverage_confidence:{conf:.3f}<{threshold:.3f}")
    except Exception as exc:
        row["errors"].append(f"web_discovery_error:{clean(exc)}")
    return row


def _candidate_match(candidate: Dict[str, Any], entries: Sequence[Dict[str, Any]]) -> Tuple[float, Optional[Dict[str, Any]]]:
    url = clean(candidate.get("url"))
    name = clean(candidate.get("name"))
    best = 0.0
    best_entry: Optional[Dict[str, Any]] = None
    for e in entries:
        score = record_similarity(e, name, url)
        if score > best:
            best = score
            best_entry = e
    return best, best_entry


def main() -> None:
    print(f"[pipeline] version={PIPELINE_VERSION} discovery=responses-web-search")
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
    extraction_min = float(thresholds.get("extract_min_confidence", 0.93))
    discovery_min = float(thresholds.get("discovery_candidate_min_confidence", 0.80))
    match_min = float(thresholds.get("match_similarity", 0.93))
    web_model = clean(__import__('os').getenv("GRANTS_WEB_MODEL", WEB_MODEL)) or WEB_MODEL
    workers = max(1, min(6, int(__import__('os').getenv("GRANTS_WEB_WORKERS", "3"))))

    entries: List[Dict[str, Any]] = [dict(x) for x in (raw.get("grants") or []) if isinstance(x, dict)]
    active_for_discovery = [x for x in entries if _visible(x, verified)]

    evidence_ledger: Dict[str, Any] = {
        "verified_date": verified.isoformat(),
        "scope_definition": scope,
        "pipeline": "audited-v3-web-primary",
        "records": {},
        "verification_failures": [],
    }

    # ------------------------------------------------------------------
    # A. Source-by-source completeness searches, in parallel.
    # ------------------------------------------------------------------
    coverage_rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_discover_one_source, s, scope=scope, verified=verified, known=active_for_discovery, model=web_model): s
            for s in source_list
        }
        for fut in as_completed(futs):
            row = fut.result()
            coverage_rows.append(row)
            print(f"[discovery] {row['source_id']}: coverage_ok={row['ok']} programmes={sum(len(p.get('programmes') or []) for p in row['passes'])}")
    coverage_rows.sort(key=lambda r: clean(r.get("source_id")))

    # ------------------------------------------------------------------
    # B. Freshly verify every tracked record, even if its saved URL is stale.
    # ------------------------------------------------------------------
    proposed_entries: List[Dict[str, Any]] = []
    verification_failures: List[Dict[str, Any]] = []

    def verify_existing(old: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], List[str], Dict[str, Any]]:
        rid = clean(old.get("id")) or slugify(clean(old.get("name")))
        if not _visible(old, verified):
            return old, None, [], {"skipped_not_visible": True}
        allowed = _allowed_domains_for_record(old, source_list)
        try:
            res = extract_program_via_web(
                current=old,
                allowed_domains=allowed,
                verified=verified,
                jurisdiction_hint=clean(old.get("level") or "national"),
                scope_text=scope,
                model=web_model,
            )
            rec, issues, ledger = _validate_web_record(
                old=old, result=res, allowed_domains=allowed, verified=verified, min_confidence=extraction_min
            )
            if rec is None:
                issues.append("empty_extracted_record")
                rec = old
            return old, rec, sorted(set(issues)), ledger
        except Exception as exc:
            return old, old, [f"extract_error:{clean(exc)}"], {"preaudit_issues": [f"extract_error:{clean(exc)}"], "allowed_domains": allowed}

    visible = [x for x in entries if _visible(x, verified)]
    invisible = [x for x in entries if not _visible(x, verified)]
    verified_rows: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]], List[str], Dict[str, Any]]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(verify_existing, old) for old in visible]
        for fut in as_completed(futs):
            verified_rows.append(fut.result())
    by_id = {clean(old.get("id")): (old, rec, issues, ledger) for old, rec, issues, ledger in verified_rows}
    for old in entries:
        rid = clean(old.get("id"))
        if old in invisible:
            proposed_entries.append(old)
            continue
        _, rec, issues, ledger = by_id.get(rid, (old, old, ["verification_result_missing"], {}))
        rec = rec or old
        proposed_entries.append(rec)
        evidence_ledger["records"][rid] = ledger
        if issues:
            verification_failures.append({"id": rid, "issues": issues})
            print(f"[verify] FAIL {rid} issues={len(issues)}")
        else:
            print(f"[verify] PASS {rid}")

    # ------------------------------------------------------------------
    # C. Reconcile source-level discovery against tracked/verified records.
    # ------------------------------------------------------------------
    candidates_out: List[Dict[str, Any]] = []
    new_candidate_pool: Dict[str, Dict[str, Any]] = {}

    def ingest_candidate(c: Dict[str, Any], sid: str, jurisdiction: str, allowed: Sequence[str], origin: str) -> None:
        name = clean(c.get("name"))
        url = clean(c.get("url"))
        conf = float(c.get("confidence") or 0)
        row: Dict[str, Any] = {
            "source_id": sid, "jurisdiction": jurisdiction, "origin": origin,
            "name": name, "url": url, "confidence": conf, "reason": clean(c.get("reason")),
        }
        if c.get("in_scope") is False:
            row["resolution"] = "excluded_out_of_scope"
            candidates_out.append(row)
            return
        if conf < discovery_min:
            row["resolution"] = "unresolved"
            row["reason"] = f"discovery_confidence_below_gate:{conf:.3f}"
            candidates_out.append(row)
            return
        if not name or not url:
            row["resolution"] = "unresolved"
            row["reason"] = "missing_candidate_name_or_url"
            candidates_out.append(row)
            return
        if not url_on_allowed_domain(url, allowed):
            row["resolution"] = "unresolved"
            row["reason"] = "candidate_url_outside_allowed_domains"
            candidates_out.append(row)
            return
        best, best_entry = _candidate_match(c, proposed_entries)
        exact = bool(best_entry) and canonical_url(clean(best_entry.get("url"))) == canonical_url(url)
        if exact or best >= match_min:
            row["resolution"] = "matched_existing"
            row["matched_id"] = clean((best_entry or {}).get("id"))
            row["similarity"] = round(best, 4)
            candidates_out.append(row)
            return
        cu = canonical_url(url)
        existing = new_candidate_pool.get(cu)
        if not existing or conf > float(existing.get("confidence") or 0):
            new_candidate_pool[cu] = {**row, "allowed_domains": list(allowed)}

    for row in coverage_rows:
        sid = clean(row.get("source_id"))
        jurisdiction = clean(row.get("jurisdiction") or "national")
        allowed = list(row.get("allowed_domains") or [])
        for p in row.get("passes") or []:
            for c in p.get("programmes") or []:
                if isinstance(c, dict):
                    ingest_candidate(c, sid, jurisdiction, allowed, "source_search")

    # ------------------------------------------------------------------
    # D. Jurisdiction-level second completeness pass, with all official domains.
    # ------------------------------------------------------------------
    jurisdiction_rows: List[Dict[str, Any]] = []
    jur_sources: Dict[str, List[Dict[str, Any]]] = {}
    for s in source_list:
        jur_sources.setdefault(clean(s.get("jurisdiction") or "national"), []).append(s)

    current_known = proposed_entries + [
        {"id": "candidate", "name": x.get("name"), "url": x.get("url")} for x in new_candidate_pool.values()
    ]

    def crosscheck_jur(jur: str, ss: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        allowed = sorted({d for s in ss for d in (s.get("allowed_domains") or [])})
        row: Dict[str, Any] = {"jurisdiction": jur, "allowed_domains": allowed, "ok": False, "errors": []}
        try:
            res = discover_programmes_via_web(
                source_id=f"jurisdiction-crosscheck-{jur}", jurisdiction=jur, allowed_domains=allowed,
                scope_text=scope, verified=verified, known_programmes=current_known,
                pass_name="crosscheck", model=web_model,
            )
            data = dict(res.get("data") or {})
            row.update({
                "coverage_confidence": float(data.get("coverage_confidence") or 0),
                "programmes": [x for x in (data.get("programmes") or []) if isinstance(x, dict)],
                "search_notes": clean(data.get("search_notes")),
                "tool_source_urls": res.get("tool_source_urls") or [],
                "model": res.get("model"), "response_id": res.get("response_id"),
            })
            threshold = float(thresholds.get("jurisdiction_coverage_min_confidence", 0.90))
            row["ok"] = row["coverage_confidence"] >= threshold
            if not row["ok"]:
                row["errors"].append(f"coverage_confidence:{row['coverage_confidence']:.3f}<{threshold:.3f}")
        except Exception as exc:
            row["errors"].append(f"crosscheck_error:{clean(exc)}")
        return row

    with ThreadPoolExecutor(max_workers=min(workers, 3)) as ex:
        futs = {ex.submit(crosscheck_jur, jur, ss): jur for jur, ss in jur_sources.items()}
        for fut in as_completed(futs):
            row = fut.result()
            jurisdiction_rows.append(row)
            print(f"[crosscheck] {row['jurisdiction']}: coverage_ok={row['ok']} programmes={len(row.get('programmes') or [])}")
    jurisdiction_rows.sort(key=lambda r: clean(r.get("jurisdiction")))

    for row in jurisdiction_rows:
        for c in row.get("programmes") or []:
            ingest_candidate(c, f"jurisdiction-crosscheck-{row['jurisdiction']}", row["jurisdiction"], row.get("allowed_domains") or [], "jurisdiction_crosscheck")

    # ------------------------------------------------------------------
    # E. Verify genuinely new programmes found by either completeness channel.
    # ------------------------------------------------------------------
    for cu, cand in sorted(new_candidate_pool.items()):
        # It may have become matched after another candidate was added.
        best, best_entry = _candidate_match(cand, proposed_entries)
        if best >= match_min:
            candidates_out.append({**cand, "resolution": "matched_existing", "matched_id": clean((best_entry or {}).get("id")), "similarity": round(best, 4)})
            continue
        seed = {
            "id": slugify(clean(cand.get("name"))),
            "name": clean(cand.get("name")),
            "url": clean(cand.get("url")),
            "level": clean(cand.get("jurisdiction") or "national"),
        }
        allowed = cand.get("allowed_domains") or [domain(clean(cand.get("url")))]
        try:
            res = extract_program_via_web(
                current=seed, allowed_domains=allowed, verified=verified,
                jurisdiction_hint=clean(seed.get("level")), scope_text=scope, model=web_model,
            )
            rec, issues, ledger = _validate_web_record(old=None, result=res, allowed_domains=allowed, verified=verified, min_confidence=extraction_min)
            if rec:
                rec["id"] = slugify(clean(rec.get("name")))
            if rec and max((record_similarity(e, clean(rec.get("name")), clean(rec.get("url"))) for e in proposed_entries), default=0.0) >= match_min:
                candidates_out.append({**cand, "resolution": "duplicate_after_extraction"})
                continue
            if not rec or issues:
                candidates_out.append({**cand, "resolution": "unresolved", "reason": "new_record_verification_failed", "issues": issues})
                continue
            proposed_entries.append(rec)
            evidence_ledger["records"][rec["id"]] = ledger
            candidates_out.append({**cand, "resolution": "auto_added_pending_independent_audit", "record_id": rec["id"]})
            print(f"[discover] ADD {rec['id']}")
        except Exception as exc:
            candidates_out.append({**cand, "resolution": "unresolved", "reason": f"new_record_extract_error:{clean(exc)}"})

    # Records that source searches matched to existing but whose fresh verification failed remain failures.
    evidence_ledger["verification_failures"] = verification_failures
    evidence_ledger["unresolved_candidates"] = [x for x in candidates_out if x.get("resolution") == "unresolved"]

    # Stable order: original IDs first; discoveries follow by level/name.
    historical_ids = [clean(x.get("id")) for x in entries]
    pos = {rid: i for i, rid in enumerate(historical_ids)}
    proposed_entries.sort(key=lambda e: (0, pos[clean(e.get("id"))]) if clean(e.get("id")) in pos else (1, clean(e.get("level")), clean(e.get("name"))))

    out_raw = dict(raw)
    out_raw["grants"] = proposed_entries
    out_raw.setdefault("metadata", {})
    out_raw["metadata"].update({
        "candidate_verified_date": verified.isoformat(),
        "verification_pipeline": "audited-v3-web-primary",
        "verification_status": "PENDING_INDEPENDENT_AUDIT",
    })

    yaml_dump(args.output, out_raw)
    json_dump(args.evidence, evidence_ledger)
    json_dump(args.candidates, {"verified_date": verified.isoformat(), "candidates": candidates_out})
    json_dump(args.coverage, {
        "verified_date": verified.isoformat(),
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
