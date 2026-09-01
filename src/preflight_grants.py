# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
VERSION = "7.0-snapshot-sentinel"


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    manifest = json.loads((ROOT / "config/grants_pipeline_manifest.json").read_text())
    if manifest.get("version") != VERSION:
        raise SystemExit(f"manifest version mismatch: {manifest.get('version')}")
    bad = []
    for rel, expected in (manifest.get("immutable_files") or {}).items():
        p = ROOT / rel
        if not p.is_file() or sha(p) != expected:
            bad.append(rel)
    if bad:
        raise SystemExit("immutable package mismatch: " + ", ".join(bad))

    banned = ["api." + "openai.com", "OPENAI_" + "API_KEY", "web_search_" + "json", "chat/" + "completions", "/v1/" + "responses"]
    hits = []
    for base in [ROOT / "src", ROOT / ".github/workflows"]:
        for p in base.rglob("*"):
            if p.is_file() and p.suffix in {".py", ".yml", ".yaml"}:
                t = p.read_text(errors="ignore")
                hits += [f"{p.relative_to(ROOT)}:{x}" for x in banned if x in t]
    if hits:
        raise SystemExit("paid API markers present: " + ", ".join(hits))

    required_files = [
        "config/grants_seed_2026-09-01.yaml",
        "config/grants_evidence_contracts.yaml",
        "config/grants_snapshots.yaml",
        "config/grants_discovery_baseline.yaml",
        "config/grants_sources.yaml",
        "config/grants_candidate_decisions.yaml",
        "assets/reference/radar-layout-reference-fallback.pdf",
        "assets/gg-advisory-logo.png",
    ]
    missing = [x for x in required_files if not (ROOT / x).is_file()]
    if missing:
        raise SystemExit("required package files missing: " + ", ".join(missing))

    seed = yaml.safe_load((ROOT / "config/grants_seed_2026-09-01.yaml").read_text()) or {}
    contracts = yaml.safe_load((ROOT / "config/grants_evidence_contracts.yaml").read_text()) or {}
    snapshots = yaml.safe_load((ROOT / "config/grants_snapshots.yaml").read_text()) or {}
    baseline = yaml.safe_load((ROOT / "config/grants_discovery_baseline.yaml").read_text()) or {}
    sources = yaml.safe_load((ROOT / "config/grants_sources.yaml").read_text()) or {}

    from grants_core import factual_fingerprint, record_visible

    grants = seed.get("grants") or []
    ids = [g.get("id") for g in grants]
    if len(ids) != len(set(ids)):
        raise SystemExit("duplicate IDs in seed")
    visible = [g for g in grants if record_visible(g, date(2026, 9, 1))]
    cids = set((contracts.get("records") or {}).keys())
    sids = set((snapshots.get("records") or {}).keys())
    missing_contracts = sorted({g["id"] for g in visible} - cids)
    missing_snapshots = sorted({g["id"] for g in visible} - sids)
    if missing_contracts or missing_snapshots:
        raise SystemExit(f"missing contracts={missing_contracts}; missing snapshots={missing_snapshots}")
    mismatch = [g["id"] for g in visible if (snapshots["records"][g["id"]].get("record_fingerprint") != factual_fingerprint(g))]
    if mismatch:
        raise SystemExit("snapshot fingerprint mismatch: " + ", ".join(mismatch))

    source_ids = {s.get("id") for s in sources.get("sources") or []}
    baseline_ids = set((baseline.get("sources") or {}).keys())
    if source_ids - baseline_ids:
        raise SystemExit("discovery baseline missing source IDs: " + ", ".join(sorted(source_ids - baseline_ids)))

    print(f"Zero-API package preflight PASS: {VERSION}")
    print(f"Bootstrap registry entries: {len(grants)}; visible at 2026-09-01: {len(visible)}")
    print(f"Dated discovery source inventories: {len(baseline_ids)}")
    print("Paid model/API dependency: NONE")
    print("Independent audit network requests: ZERO")


if __name__ == "__main__":
    main()
