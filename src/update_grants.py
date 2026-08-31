# -*- coding: utf-8 -*-
"""Verify, update and discover programmes for the GG Advisory Funding Radar.

This stage never publishes a report. It writes a candidate verified grants YAML plus
an evidence ledger and discovery ledger. The independent audit stage must pass
before the candidate YAML can replace config/grants.yaml or be rendered.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from grants_core import (
    CRITICAL_FIELDS,
    canonical_url,
    classify_candidate,
    clean,
    domain,
    evidence_quote_is_literal,
    factual_sentences,
    extract_links,
    extract_program_from_sources,
    fetch_url,
    json_dump,
    normalise_for_match,
    parse_date,
    record_similarity,
    required_evidence_fields,
    relevant_text_score,
    relevant_url_score,
    scope_text,
    slugify,
    validate_record_schema,
    yaml_dump,
    yaml_load,
    discover_sitemap_urls,
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


def _source_for_url(url: str, source_cfgs: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    d = domain(url)
    for s in source_cfgs:
        allowed = [str(x).lower().removeprefix("www.") for x in (s.get("allowed_domains") or [])]
        if any(d == a or d.endswith("." + a) for a in allowed):
            return s
    return None


def _doc_similarity(entry: Dict[str, Any], doc: Dict[str, Any]) -> float:
    target = normalise_for_match(entry.get("name"))
    candidate = normalise_for_match(f"{doc.get('title','')} {doc.get('anchor','')} {doc.get('url','')} {str(doc.get('text',''))[:1800]}")
    if not target or not candidate:
        return 0.0
    toks = [t for t in target.split() if len(t) >= 4 and t not in {"fund", "grant", "program", "programme", "round", "accelerator"}]
    overlap = sum(1 for t in toks if t in candidate)
    token_score = overlap / max(1, len(toks))
    # URL/name matching is separately handled by record_similarity.
    return token_score


def _collect_source_docs(source: Dict[str, Any], scope_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Collect current index + filtered sitemap candidate pages for one discovery source."""
    source_id = clean(source.get("id"))
    required = bool(source.get("required", True))
    allowed = list(source.get("allowed_domains") or [])
    min_url_score = int(source.get("min_url_score", 2))
    min_text_score = int(source.get("min_text_score", 4))
    max_pages = int(source.get("max_candidate_pages", 45))

    coverage: Dict[str, Any] = {
        "source_id": source_id,
        "jurisdiction": clean(source.get("jurisdiction") or "national"),
        "required": required,
        "index_fetches": [],
        "sitemap_fetches": [],
        "candidate_urls": [],
        "candidate_urls_total": 0,
        "candidate_pages_fetched": 0,
        "candidate_pages_failed": 0,
        "candidate_pages_retained": 0,
        "truncated": False,
        "ok": True,
        "errors": [],
    }
    url_anchor: Dict[str, str] = {}
    docs: Dict[str, Dict[str, Any]] = {}

    # Index pages are authoritative directory pages and are always retained as support docs.
    index_url_set = {canonical_url(str(u)) for u in (source.get("index_urls") or [])}
    for url in source.get("index_urls") or []:
        f = fetch_url(str(url))
        coverage["index_fetches"].append(f.public_dict())
        if not f.ok:
            coverage["ok"] = False
            coverage["errors"].append(f"index_fetch_failed:{url}:{f.error}")
            continue
        cu = canonical_url(f.final_url)
        docs[cu] = {"url": f.final_url, "title": f.title, "text": f.text, "anchor": "", "fetch": f}
        for link, anchor in extract_links(f, allowed):
            if canonical_url(link) in index_url_set:
                continue
            if relevant_url_score(link, anchor, source) >= min_url_score:
                url_anchor.setdefault(link, anchor)

    # Sitemap scanning broadens completeness beyond what a directory happens to render.
    for sm in source.get("sitemaps") or []:
        pages, sm_meta = discover_sitemap_urls(str(sm), allowed)
        coverage["sitemap_fetches"].extend(sm_meta)
        if not pages and bool(source.get("sitemap_required", False)):
            coverage["ok"] = False
            coverage["errors"].append(f"sitemap_empty:{sm}")
        for link in pages:
            if canonical_url(link) in index_url_set:
                continue
            if relevant_url_score(link, "", source) >= min_url_score:
                url_anchor.setdefault(link, "")

    # Prioritise explicit index links, then URLs with higher deterministic relevance.
    ranked_all = sorted(
        url_anchor.items(),
        key=lambda kv: (relevant_url_score(kv[0], kv[1], source), len(kv[1])),
        reverse=True,
    )
    coverage["candidate_urls_total"] = len(ranked_all)
    min_candidates = int(source.get("min_candidate_urls", 0))
    if len(ranked_all) < min_candidates:
        coverage["ok"] = False
        coverage["errors"].append(f"too_few_candidates:{len(ranked_all)}<{min_candidates}")
    if len(ranked_all) > max_pages:
        coverage["truncated"] = True
        coverage["ok"] = False
        coverage["errors"].append(f"candidate_scan_truncated:{len(ranked_all)}>{max_pages}")
    ranked = ranked_all[:max_pages]
    coverage["candidate_urls"] = [u for u, _ in ranked]

    for url, anchor in ranked:
        if url in docs:
            continue
        f = fetch_url(url)
        coverage["candidate_pages_fetched"] += 1
        if not f.ok:
            coverage["candidate_pages_failed"] += 1
            # For a mandatory completeness source, an unreadable candidate means we cannot prove completeness.
            if required:
                coverage["ok"] = False
                coverage["errors"].append(f"candidate_fetch_failed:{url}:{f.error}")
            continue
        # Do not silently discard a discovered URL because a deterministic text score is low.
        # Every successfully fetched candidate in the discovery universe is retained and must
        # later be explicitly matched, included, or excluded by the dual classifiers.
        text_score = relevant_text_score(f.text, scope_cfg.get("scope") or {})
        docs[url] = {
            "url": f.final_url, "title": f.title, "text": f.text, "anchor": anchor, "fetch": f,
            "text_score": text_score, "below_text_hint": text_score < min_text_score,
        }
        coverage["candidate_pages_retained"] += 1

    if required and not coverage["index_fetches"] and not coverage["sitemap_fetches"]:
        coverage["ok"] = False
        coverage["errors"].append("no_discovery_mechanism")
    return {"coverage": coverage, "docs": list(docs.values())}


def _support_bundle(entry: Dict[str, Any], source: Optional[Dict[str, Any]], source_docs: Dict[str, List[Dict[str, Any]]]) -> Tuple[List[Tuple[str, str]], List[Dict[str, Any]]]:
    fetched_meta: List[Dict[str, Any]] = []
    bundle: List[Tuple[str, str]] = []
    seen: set[str] = set()

    primary_urls = [clean(entry.get("url"))] + [clean(x) for x in (entry.get("verification_urls") or [])]
    for url in [u for u in primary_urls if u]:
        f = fetch_url(url)
        fetched_meta.append(f.public_dict())
        if f.ok:
            cu = canonical_url(f.final_url)
            if cu not in seen:
                seen.add(cu)
                bundle.append((f.final_url, f.text))

    if source:
        docs = source_docs.get(clean(source.get("id")), [])
        # Parent/index pages can supply explicit current status even when programme pages are stale.
        for d in docs:
            if canonical_url(d.get("url", "")) in seen:
                continue
            score = _doc_similarity(entry, d)
            is_index = canonical_url(d.get("url", "")) in {canonical_url(u) for u in (source.get("index_urls") or [])}
            if is_index or score >= 0.34:
                seen.add(canonical_url(d.get("url", "")))
                bundle.append((d.get("url", ""), d.get("text", "")))
            if len(bundle) >= 5:
                break
    return bundle[:5], fetched_meta


def _literal_evidence_check(record: Dict[str, Any], extracted: Dict[str, Any], bundle: Sequence[Tuple[str, str]]) -> List[str]:
    source_map = {canonical_url(u): text for u, text in bundle}
    issues: List[str] = []
    evidence = extracted.get("evidence") or {}
    for field in required_evidence_fields(record):
        ev = evidence.get(field) or {}
        u = canonical_url(clean(ev.get("source_url")))
        q = clean(ev.get("quote"))
        if not u or not q:
            issues.append(f"missing_evidence:{field}")
            continue
        text = source_map.get(u)
        if text is None:
            text = next((t for su, t in source_map.items() if su == u or su.endswith(url_path(u))), None)
        if not text or not evidence_quote_is_literal(q, text):
            issues.append(f"nonliteral_evidence:{field}")

    # Every factual sentence in description/signals must have its own literal-source evidence.
    claim_rows = [x for x in (extracted.get("claim_evidence") or []) if isinstance(x, dict)]
    for path, sentence in factual_sentences(record):
        matches = [x for x in claim_rows if clean(x.get("path")) == path and normalise_for_match(x.get("claim")) == normalise_for_match(sentence)]
        if not matches:
            issues.append(f"missing_claim_evidence:{path}")
            continue
        valid = False
        for ev in matches:
            u = canonical_url(clean(ev.get("source_url")))
            q = clean(ev.get("quote"))
            text = source_map.get(u)
            if text and evidence_quote_is_literal(q, text):
                valid = True
                break
        if not valid:
            issues.append(f"nonliteral_claim_evidence:{path}")
    return issues


def url_path(url: str) -> str:
    from urllib.parse import urlparse
    return urlparse(url).path.rstrip("/")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--sources", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--evidence", type=Path, required=True)
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--coverage", type=Path, required=True)
    ap.add_argument("--verified", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    verified = parse_date(args.verified)
    if not verified:
        raise SystemExit("--verified must be YYYY-MM-DD")

    raw = yaml_load(args.input)
    source_cfg = yaml_load(args.sources)
    scope = scope_text(source_cfg)
    source_list = list(source_cfg.get("sources") or [])
    entries: List[Dict[str, Any]] = [dict(x) for x in (raw.get("grants") or []) if isinstance(x, dict)]

    # 1) Completeness discovery universe.
    per_source_docs: Dict[str, List[Dict[str, Any]]] = {}
    coverage_rows: List[Dict[str, Any]] = []
    for s in source_list:
        result = _collect_source_docs(s, source_cfg)
        sid = clean(s.get("id"))
        per_source_docs[sid] = result["docs"]
        coverage_rows.append(result["coverage"])
        print(f"[discovery] {sid}: docs={len(result['docs'])} coverage_ok={result['coverage']['ok']}")

    # 2) Verify/update every currently visible tracked programme.
    evidence_ledger: Dict[str, Any] = {
        "verified_date": verified.isoformat(),
        "scope_definition": scope,
        "records": {},
    }
    proposed_entries: List[Dict[str, Any]] = []
    verification_failures: List[Dict[str, Any]] = []

    for old in entries:
        if not _visible(old, verified):
            proposed_entries.append(old)
            continue
        src = _source_for_url(clean(old.get("url")), source_list)
        bundle, fetch_meta = _support_bundle(old, src, per_source_docs)
        rid = clean(old.get("id")) or slugify(clean(old.get("name")))
        if not bundle:
            verification_failures.append({"id": rid, "issues": ["no_verifiable_source"], "fetches": fetch_meta})
            proposed_entries.append(old)
            continue
        try:
            extracted = extract_program_from_sources(
                current=old,
                source_bundle=bundle,
                verified=verified,
                jurisdiction_hint=clean(old.get("level") or (src or {}).get("jurisdiction") or "national"),
                scope_text=scope,
            )
        except Exception as exc:
            verification_failures.append({"id": rid, "issues": [f"extract_error:{clean(exc)}"], "fetches": fetch_meta})
            proposed_entries.append(old)
            continue

        record = dict(extracted.get("record") or {})
        record["id"] = rid  # IDs are stable identifiers and never model-controlled for existing records.
        record["last_verified"] = verified.isoformat()
        if old.get("show_from") and not record.get("show_from"):
            record["show_from"] = old.get("show_from")
        if "show_until" not in record:
            record["show_until"] = old.get("show_until")
        # Preserve additional manual metadata not controlled by extraction.
        for k in ("include_in_report", "verification_urls"):
            if k in old and k not in record:
                record[k] = old[k]

        schema_issues = validate_record_schema(record)
        literal_issues = _literal_evidence_check(record, extracted, bundle)
        confidence = float(extracted.get("overall_confidence") or 0)
        conflict = bool(extracted.get("unresolved_conflict"))
        issues = schema_issues + literal_issues
        if confidence < float(source_cfg.get("thresholds", {}).get("extract_min_confidence", 0.97)):
            issues.append(f"low_extract_confidence:{confidence:.3f}")
        if conflict:
            issues.append("unresolved_source_conflict")

        evidence_ledger["records"][rid] = {
            "record": record,
            "evidence": extracted.get("evidence") or {},
            "claim_evidence": extracted.get("claim_evidence") or [],
            "supporting_claims": extracted.get("supporting_claims") or [],
            "source_bundle": [{"url": u, "sha256": __import__('hashlib').sha256(clean(t).encode()).hexdigest()} for u, t in bundle],
            "fetches": fetch_meta,
            "extract_confidence": confidence,
            "conflict_notes": extracted.get("conflict_notes") or [],
            "preaudit_issues": issues,
        }
        if issues:
            verification_failures.append({"id": rid, "issues": issues})
            proposed_entries.append(old)
        else:
            proposed_entries.append(record)
            print(f"[verify] PASS {rid}")

    # 3) Discover programmes not already tracked. Dual adversarial classification.
    candidates_out: List[Dict[str, Any]] = []
    add_threshold = float(source_cfg.get("thresholds", {}).get("auto_add_confidence", 0.98))
    exclude_threshold = float(source_cfg.get("thresholds", {}).get("auto_exclude_confidence", 0.98))
    existing_for_match = proposed_entries[:]

    for s in source_list:
        sid = clean(s.get("id"))
        jurisdiction = clean(s.get("jurisdiction") or "national")
        index_urls = {canonical_url(x) for x in (s.get("index_urls") or [])}
        for d in per_source_docs.get(sid, []):
            url = clean(d.get("url"))
            if canonical_url(url) in index_urls:
                # Directory/index pages are discovery mechanisms, never programme candidates themselves.
                continue
            title = clean(d.get("title") or d.get("anchor"))
            text = clean(d.get("text"))
            if not url or not text:
                continue
            scored = [(record_similarity(e, title, url), e) for e in existing_for_match]
            best, best_entry = max(scored, key=lambda x: x[0], default=(0.0, None))
            exact_url_match = bool(best_entry) and canonical_url(clean(best_entry.get("url"))) == canonical_url(url)
            # Only treat a different URL as the same programme at a very high name similarity.
            # Lower similarity must be adjudicated so a new round/successor is not silently missed.
            if exact_url_match or best >= 0.97:
                candidates_out.append({
                    "url": url, "title": title, "source_id": sid,
                    "resolution": "matched_existing",
                    "matched_id": clean((best_entry or {}).get("id")),
                    "similarity": round(float(best), 4),
                })
                continue
            try:
                c1 = classify_candidate(url=url, title=title, text=text, jurisdiction=jurisdiction, scope_text=scope, reverse_prompt=False)
                c2 = classify_candidate(url=url, title=title, text=text, jurisdiction=jurisdiction, scope_text=scope, reverse_prompt=True)
            except Exception as exc:
                candidates_out.append({"url": url, "title": title, "source_id": sid, "resolution": "unresolved", "reason": f"classification_error:{clean(exc)}"})
                continue

            in1, in2 = bool(c1.get("in_scope")), bool(c2.get("in_scope"))
            conf1, conf2 = float(c1.get("confidence") or 0), float(c2.get("confidence") or 0)
            candidate_row: Dict[str, Any] = {
                "url": url, "title": title, "source_id": sid,
                "classifier_1": c1, "classifier_2": c2,
            }
            if (not in1) and (not in2) and min(conf1, conf2) >= exclude_threshold:
                candidate_row["resolution"] = "excluded_out_of_scope"
                candidates_out.append(candidate_row)
                continue
            if not (in1 and in2 and min(conf1, conf2) >= add_threshold):
                candidate_row["resolution"] = "unresolved"
                candidate_row["reason"] = "dual_classifiers_not_unanimous_high_confidence"
                candidates_out.append(candidate_row)
                continue

            # High-confidence in-scope candidate: extract from its primary page plus parent index pages.
            bundle = [(url, d.get("text", ""))]
            for parent in per_source_docs.get(sid, []):
                pu = canonical_url(parent.get("url", ""))
                if pu == canonical_url(url):
                    continue
                if pu in {canonical_url(x) for x in (s.get("index_urls") or [])}:
                    bundle.append((parent.get("url", ""), parent.get("text", "")))
                if len(bundle) >= 3:
                    break
            try:
                ext = extract_program_from_sources(
                    current=None,
                    source_bundle=bundle,
                    verified=verified,
                    jurisdiction_hint=jurisdiction,
                    scope_text=scope,
                )
                rec = dict(ext.get("record") or {})
                rec["id"] = slugify(clean(rec.get("name") or c1.get("program_name") or title))
                rec["last_verified"] = verified.isoformat()
                if not rec.get("show_from"):
                    rec["show_from"] = verified.replace(day=1).isoformat()
                rec["show_until"] = None
                issues = validate_record_schema(rec) + _literal_evidence_check(rec, ext, bundle)
                conf = float(ext.get("overall_confidence") or 0)
                if conf < float(source_cfg.get("thresholds", {}).get("extract_min_confidence", 0.97)):
                    issues.append(f"low_extract_confidence:{conf:.3f}")
                if bool(ext.get("unresolved_conflict")):
                    issues.append("unresolved_source_conflict")
                # Duplicate check after extraction by canonical URL and name.
                if max((record_similarity(e, clean(rec.get("name")), clean(rec.get("url"))) for e in existing_for_match), default=0.0) >= 0.86:
                    candidate_row["resolution"] = "duplicate_after_extraction"
                    candidates_out.append(candidate_row)
                    continue
                if issues:
                    candidate_row["resolution"] = "unresolved"
                    candidate_row["reason"] = "new_record_pre_audit_failed"
                    candidate_row["issues"] = issues
                    candidates_out.append(candidate_row)
                    continue
                proposed_entries.append(rec)
                existing_for_match.append(rec)
                evidence_ledger["records"][rec["id"]] = {
                    "record": rec,
                    "evidence": ext.get("evidence") or {},
                    "claim_evidence": ext.get("claim_evidence") or [],
                    "supporting_claims": ext.get("supporting_claims") or [],
                    "source_bundle": [{"url": u, "sha256": __import__('hashlib').sha256(clean(t).encode()).hexdigest()} for u, t in bundle],
                    "fetches": [d.get("fetch").public_dict() if d.get("fetch") else {}],
                    "extract_confidence": conf,
                    "conflict_notes": ext.get("conflict_notes") or [],
                    "preaudit_issues": [],
                    "discovered_new": True,
                }
                candidate_row["resolution"] = "auto_added_pending_independent_audit"
                candidate_row["record_id"] = rec["id"]
                candidates_out.append(candidate_row)
                print(f"[discover] ADD {rec['id']} <- {url}")
            except Exception as exc:
                candidate_row["resolution"] = "unresolved"
                candidate_row["reason"] = f"candidate_extract_error:{clean(exc)}"
                candidates_out.append(candidate_row)

    # Stable ordering: preserve historical records first, append discovered records by level/name.
    historical_ids = [clean(x.get("id")) for x in entries]
    pos = {rid: i for i, rid in enumerate(historical_ids)}
    proposed_entries.sort(key=lambda e: (0, pos[clean(e.get("id"))]) if clean(e.get("id")) in pos else (1, clean(e.get("level")), clean(e.get("name"))))
    out_raw = dict(raw)
    out_raw["grants"] = proposed_entries
    out_raw.setdefault("metadata", {})
    out_raw["metadata"].update({
        "candidate_verified_date": verified.isoformat(),
        "verification_pipeline": "audited-v2",
        "verification_status": "PENDING_INDEPENDENT_AUDIT",
    })

    evidence_ledger["verification_failures"] = verification_failures
    evidence_ledger["unresolved_candidates"] = [x for x in candidates_out if x.get("resolution") == "unresolved"]

    yaml_dump(args.output, out_raw)
    json_dump(args.evidence, evidence_ledger)
    json_dump(args.candidates, {"verified_date": verified.isoformat(), "candidates": candidates_out})
    json_dump(args.coverage, {"verified_date": verified.isoformat(), "sources": coverage_rows})

    print(f"[write] candidate grants: {args.output}")
    print(f"[write] evidence: {args.evidence}")
    print(f"[write] candidates: {args.candidates}")
    print(f"[write] coverage: {args.coverage}")
    print(f"[summary] tracked verification failures={len(verification_failures)} unresolved discovery candidates={len(evidence_ledger['unresolved_candidates'])}")


if __name__ == "__main__":
    main()
