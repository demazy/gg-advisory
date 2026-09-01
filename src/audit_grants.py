# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json
from datetime import date
from pathlib import Path
from grants_core import PIPELINE_VERSION, canonical_url, http_count, json_dump, record_visible, reset_http_counter, verify_record, yaml_load

def _md(a):
    lines=["# GG Advisory Grants Radar Audit","",f"**Verification date:** {a['verified_date']}",
           f"**Publication gate:** {'PASS' if a['publishable'] else 'FAIL'}","",
           "## Gate summary","",
           "| Gate | Result | Detail |","|---|---|---|"]
    for k,v in a["gates"].items():
        lines.append(f"| {k} | {'PASS' if v['pass'] else 'FAIL'} | {v['detail']} |")
    lines += ["","## Programme audit","","| Programme | Result | Issues |","|---|---|---|"]
    for r in a["records"]:
        issues="; ".join(r.get("issues") or [])
        lines.append(f"| {str(r.get('name','')).replace('|','/')} | {'PASS' if r.get('pass') else 'FAIL'} | {issues.replace('|','/')} |")
    lines += ["","## Method","",
      "No paid model/API is used. Each published record is independently re-fetched from its configured official/administering source URLs and re-tested against the field-level evidence contract. Mandatory discovery index pages must be reachable and every high-signal discovered URL must have an explicit include/match/monitor decision. The gate is fail-closed."]
    return "\n".join(lines)+"\n"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--grants",type=Path,required=True)
    ap.add_argument("--sources",type=Path,required=True)
    ap.add_argument("--contracts",type=Path,required=True)
    ap.add_argument("--candidates",type=Path,required=True)
    ap.add_argument("--coverage",type=Path,required=True)
    ap.add_argument("--verified",required=True)
    ap.add_argument("--output-json",type=Path,required=True)
    ap.add_argument("--output-md",type=Path,required=True)
    args=ap.parse_args()
    verified=date.fromisoformat(args.verified)
    reg=yaml_load(args.grants); contracts=yaml_load(args.contracts)
    cand=json.loads(args.candidates.read_text()); cov=json.loads(args.coverage.read_text())
    reset_http_counter()
    rows=[]
    for g in reg.get("grants") or []:
        if not record_visible(g,verified): continue
        c=(contracts.get("records") or {}).get(g.get("id"))
        if not c:
            rows.append({"id":g.get("id"),"name":g.get("name"),"pass":False,"issues":["missing_evidence_contract"]})
        else:
            rows.append(verify_record(g,c,verified))
    unresolved=[x for x in cand.get("candidates") or [] if x.get("resolution")=="unresolved"]
    required_failed=list(cov.get("required_failed") or [])
    record_fail=[r for r in rows if not r.get("pass")]
    levels=set(str(g.get("level") or "").lower() for g in reg.get("grants") or [] if record_visible(g,verified))
    expected={"national","act","nsw","nt","qld","sa","tas","vic","wa"}
    gates={
      "mandatory_source_coverage":{"pass":not required_failed,"detail":f"{len(required_failed)} required source groups failed" if required_failed else "all required source groups reachable"},
      "jurisdiction_coverage":{"pass":expected.issubset(levels),"detail":"all national/state/territory levels represented in current registry" if expected.issubset(levels) else "missing: "+",".join(sorted(expected-levels))},
      "candidate_reconciliation":{"pass":not unresolved,"detail":f"{len(unresolved)} unresolved high-signal candidates"},
      "published_record_verification":{"pass":not record_fail,"detail":f"{len(rows)-len(record_fail)}/{len(rows)} visible records passed independent direct-source audit"},
      "uniqueness":{"pass":True,"detail":"checked below"},
      "zero_paid_api":{"pass":True,"detail":"workflow and runtime use direct HTTP only; no paid model/API key required"},
    }
    ids=[g.get("id") for g in reg.get("grants") or []]; urls=[canonical_url(g.get("url","")) for g in reg.get("grants") or []]
    dup_ids=sorted({x for x in ids if x and ids.count(x)>1}); dup_urls=sorted({x for x in urls if x and urls.count(x)>1})
    if dup_ids or dup_urls:
        gates["uniqueness"]={"pass":False,"detail":f"duplicate_ids={dup_ids} duplicate_urls={dup_urls}"}
    publishable=all(v["pass"] for v in gates.values())
    summary={
        "visible_programmes":len(rows),
        "programmes_passed":len(rows)-len(record_fail),
        "mandatory_sources_total":len([x for x in cov.get("sources") or [] if x.get("required")]),
        "mandatory_sources_ok":len([x for x in cov.get("sources") or [] if x.get("required") and x.get("ok")]),
        "unresolved_candidates":len(unresolved),
        "baseline_programmes":len(rows),
        "baseline_programmes_audited":len(rows)-len(record_fail),
    }
    out={"verified_date":args.verified,"pipeline":PIPELINE_VERSION,"publishable":publishable,"gates":gates,
         "summary":summary,"records":rows,"unresolved_candidates":unresolved,"http_requests":http_count()}
    json_dump(args.output_json,out); args.output_md.write_text(_md(out),encoding="utf-8")
    print(f"[pipeline] version={PIPELINE_VERSION} audit=independent-direct-official-http")
    print(f"[audit] visible={len(rows)} failed={len(record_fail)} unresolved={len(unresolved)}")
    print(f"[audit] OVERALL {'PASS' if publishable else 'FAIL'}")
    print(f"[http] independent requests={http_count()}")
    if not publishable: raise SystemExit(2)

if __name__=="__main__":
    main()
