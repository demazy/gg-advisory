from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]


def test_manifest_exists_and_declares_final_pipeline():
    p = ROOT / 'config' / 'grants_pipeline_manifest.json'
    assert p.exists()
    data = json.loads(p.read_text())
    assert data['version'] == '5.0-canonical-ledger-layout'
    assert 'src/grants_core.py' in data['immutable_files']
    assert 'src/grants_history.py' in data['immutable_files']
    assert 'src/check_pdf_layout.py' in data['immutable_files']
    assert '.github/workflows/grants-radar.yml' in data['immutable_files']


def test_mutable_registry_and_layout_reference_are_not_hash_pinned():
    data = json.loads((ROOT / 'config' / 'grants_pipeline_manifest.json').read_text())
    assert 'config/grants.yaml' not in data['immutable_files']
    assert 'assets/reference/radar-layout-reference.pdf' not in data['immutable_files']


def test_preflight_uses_manifest_and_checks_mutable_state():
    text = (ROOT / 'src' / 'preflight_grants.py').read_text()
    assert 'expected = "' not in text
    assert 'grants_pipeline_manifest.json' in text
    assert 'sha256_mismatch:' in text
    assert 'config" / "grants.yaml' in text
    assert 'radar-layout-reference-fallback.pdf' in text
