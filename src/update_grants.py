# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json
from datetime import date
from pathlib import Path
from typing import Any, Dict
from grants_core import (
    PIPELINE_VERSION, clean, discover_source, http_count, json_dump, record_visible,
    reset_http_counter, verify_record, yaml_dump, yaml_load, canonical_url
)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",type=Path,required=True)
    ap.add_argument("--sources",type=Path,required=True)
    ap.add_argument("--contracts",type=Path,required=True)
    ap.add_argument("--decisions",type=Path,required=True)
    ap.add_argument("--output",type=Path,required=True)
    ap.add_argument("--evidence",type=Path,required=True)
    ap.add_argument("--candidates",type=Path,required=True)
    ap.add_argument("--coverage",type=Path,required=True)
    ap.add_argument("--verified",required=True)
    args=ap.parse_args()
    verified=date.fromisoformat(args.verified)
    reg=yaml_load(args.input); src=yaml_load(args.sources); contracts=yaml_load(args.contracts); dec=yaml_load(args.decisions)
    decisions={canonical_url(u):v for u,v in (dec.get("decisions") or {}).items()}
    reset_http_counter()
    records={}; failures=[]
    out_grants=[]
    for g0 in reg.get("grants") or []:
        g=dict(g0)
        if record_visible(g,verified):
            contract=(contracts.get("records") or {}).get(g.get("id"))
            if not contract:
                res={"id":g.get("id"),"name":g.get("name"),"pass":False,"issues":["missing_evidence_contract"]}
            else:
                res=verify_record(g,contract,verified)
            records[g.get("id")]=res
            if res.get("pass"):
                g["last_verified"]=args.verified
            else:
                failures.append(g.get("id"))
        out_grants.append(g)
    coverage_rows=[]; all_candidates=[]
    for s in src.get("sources") or []:
        row=discover_source(s,src.get("scope") or {},decisions)
        coverage_rows.append(row)
        for c in row.get("candidates") or []:
            c=dict(c); c["source_id"]=row.get("source_id"); c["jurisdiction"]=row.get("jurisdiction")
            all_candidates.append(c)
    unresolved=[c for c in all_candidates if c.get("resolution")=="unresolved"]
    required_failed=[r["source_id"] for r in coverage_rows if r.get("required") and not r.get("ok")]
    yaml_dump(args.output,{"grants":out_grants})
    json_dump(args.evidence,{"verified_date":args.verified,"pipeline":PIPELINE_VERSION,"records":records,
                             "verification_failures":failures,"http_requests":http_count()})
    json_dump(args.candidates,{"verified_date":args.verified,"pipeline":PIPELINE_VERSION,"candidates":all_candidates,
                               "unresolved_count":len(unresolved)})
    json_dump(args.coverage,{"verified_date":args.verified,"pipeline":PIPELINE_VERSION,"sources":coverage_rows,
                             "required_failed":required_failed,"http_requests":http_count()})
    print(f"[pipeline] version={PIPELINE_VERSION} verification=direct-official-http discovery=index-link-reconciliation")
    print(f"[verify] visible={len(records)} failures={len(failures)}")
    print(f"[discovery] unresolved={len(unresolved)} required_source_failures={len(required_failed)}")
    print(f"[http] requests={http_count()}")
    print(f"[write] {args.output}")
    if failures or unresolved or required_failed:
        raise SystemExit(2)

if __name__=="__main__":
    main()
