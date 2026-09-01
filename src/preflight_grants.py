# -*- coding: utf-8 -*-
from __future__ import annotations
import hashlib, json, re
from pathlib import Path
import yaml
from datetime import date

ROOT=Path(__file__).resolve().parents[1]
VERSION="6.0-zero-api-official-source"

def sha(p: Path)->str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    manifest=json.loads((ROOT/"config/grants_pipeline_manifest.json").read_text())
    if manifest.get("version")!=VERSION:
        raise SystemExit(f"manifest version mismatch: {manifest.get('version')}")
    bad=[]
    for rel,expected in (manifest.get("immutable_files") or {}).items():
        p=ROOT/rel
        if not p.is_file() or sha(p)!=expected:
            bad.append(rel)
    if bad:
        raise SystemExit("immutable package mismatch: "+", ".join(bad))
    workflow=(ROOT/".github/workflows/grants-radar.yml").read_text()
    banned=["api."+"openai.com","OPENAI_"+"API_KEY","web_search_"+"json","chat/"+"completions","/v1/"+"responses"]
    hits=[x for x in banned if x in workflow]
    for p in (ROOT/"src").glob("*.py"):
        t=p.read_text()
        hits += [f"{p.name}:{x}" for x in banned if x in t]
    if hits:
        raise SystemExit("paid API markers present: "+", ".join(hits))
    seed=yaml.safe_load((ROOT/"config/grants_seed_2026-09-01.yaml").read_text()) or {}
    contracts=yaml.safe_load((ROOT/"config/grants_evidence_contracts.yaml").read_text()) or {}
    grants=seed.get("grants") or []
    ids=[g.get("id") for g in grants]
    if len(ids)!=len(set(ids)):
        raise SystemExit("duplicate IDs in seed")
    # Visible baseline at bootstrap date must all have evidence contracts.
    from grants_core import record_visible
    visible=[g for g in grants if record_visible(g,date(2026,9,1))]
    missing=sorted({g["id"] for g in visible}-set((contracts.get("records") or {}).keys()))
    if missing:
        raise SystemExit("missing evidence contracts: "+", ".join(missing))
    print(f"Zero-API package preflight PASS: {VERSION}")
    print(f"Bootstrap registry entries: {len(grants)}; visible at 2026-09-01: {len(visible)}")
    print("Paid model/API dependency: NONE")

if __name__=="__main__":
    main()
