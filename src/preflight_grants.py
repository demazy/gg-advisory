# -*- coding: utf-8 -*-
"""Verify package integrity while allowing the canonical registry/reference PDF to evolve.

Immutable code/config/test files are SHA-256 pinned by the package manifest. Mutable state
(`config/grants.yaml` and the approved layout reference PDF) is validated structurally but
is deliberately not hash-pinned, because successful audited runs update it.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config" / "grants_pipeline_manifest.json"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _validate_registry(path: Path) -> list[str]:
    failures: list[str] = []
    if not path.exists():
        return [f"missing:{path.relative_to(ROOT)}"]
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return [f"invalid_yaml:{path.relative_to(ROOT)}:{exc}"]
    grants = data.get("grants")
    if not isinstance(grants, list) or not grants:
        failures.append("invalid_registry:grants_missing_or_empty")
        return failures
    ids = []
    for i, row in enumerate(grants):
        if not isinstance(row, dict):
            failures.append(f"invalid_registry:grant[{i}]_not_object")
            continue
        rid = str(row.get("id") or "").strip()
        if not rid:
            failures.append(f"invalid_registry:grant[{i}]_missing_id")
        ids.append(rid)
        hist = row.get("history")
        if hist is not None and not isinstance(hist, list):
            failures.append(f"invalid_registry:{rid or i}:history_not_list")
    dupes = sorted({x for x in ids if x and ids.count(x) > 1})
    if dupes:
        failures.append("invalid_registry:duplicate_ids:" + ",".join(dupes))
    return failures


def _valid_pdf_header(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 10_000 and path.read_bytes()[:5] == b"%PDF-"
    except Exception:
        return False


def main() -> None:
    if not MANIFEST.exists():
        raise SystemExit(f"AUDITED PACKAGE PREFLIGHT FAILED\nmissing:{MANIFEST.relative_to(ROOT)}")
    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    version = str(data.get("version") or "").strip()
    files = data.get("immutable_files") or {}
    if not version or not isinstance(files, dict) or not files:
        raise SystemExit("AUDITED PACKAGE PREFLIGHT FAILED\ninvalid_manifest")

    failures = []
    observed = []
    for rel, expected in sorted(files.items()):
        p = ROOT / rel
        if not p.exists():
            failures.append(f"missing:{rel}")
            continue
        actual = sha256(p)
        observed.append((rel, actual))
        if actual != expected:
            failures.append(f"sha256_mismatch:{rel}:expected={expected}:actual={actual}")

    failures.extend(_validate_registry(ROOT / "config" / "grants.yaml"))

    approved = ROOT / "assets" / "reference" / "radar-layout-reference.pdf"
    fallback = ROOT / "assets" / "reference" / "radar-layout-reference-fallback.pdf"
    if not _valid_pdf_header(approved) and not _valid_pdf_header(fallback):
        failures.append("missing_or_invalid_layout_reference:approved_and_fallback")

    if failures:
        raise SystemExit("AUDITED PACKAGE PREFLIGHT FAILED\n" + "\n".join(failures))

    ref = approved if _valid_pdf_header(approved) else fallback
    print(f"Audited package preflight PASS: {version}")
    print(f"Canonical registry: config/grants.yaml")
    print(f"Layout reference: {ref.relative_to(ROOT)}")
    print("Immutable file fingerprints:")
    for rel, digest in observed:
        print(f"{digest}  {rel}")


if __name__ == "__main__":
    main()
