import json
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import update_grants


def _record(amount='$1m'):
    return {
        'id': 'example-fund', 'name': 'Example Climate Fund', 'admin': 'Example Agency',
        'level': 'national', 'type': 'grant', 'status': 'Rolling', 'amount': amount,
        'deadline': None, 'deadline_type': 'rolling', 'deadline_label': 'Rolling',
        'target_stage': 'Commercialisation', 'url': 'https://example.gov.au/fund',
        'description': 'The fund supports climate technology commercialisation.',
        'why_it_matters': 'Relevant to climate-tech ventures seeking non-dilutive capital.',
        'signals': '', 'show_from': '2026-01-01', 'show_until': None,
        'include_in_report': True,
    }


def _extract_result(amount='$2m'):
    rec = _record(amount)
    rec['last_verified'] = '2026-08-31'
    url = rec['url']
    fields = ['name', 'admin', 'status', 'amount', 'deadline_label', 'target_stage']
    return {
        'data': {
            'record': rec,
            'field_evidence': {f: {'source_url': url, 'support': f'official {f} support'} for f in fields},
            'claim_evidence': [], 'source_urls': [url], 'unresolved_conflict': False,
            'conflict_notes': [], 'overall_confidence': 0.99, 'in_scope': True,
        },
        'tool_source_urls': [url], 'model': 'mock', 'response_id': 'r',
        'search_response_id': 's', 'structure_response_id': 'j', 'search_evidence_sha256': 'abc',
    }


def test_full_update_preserves_registry_and_appends_material_history(monkeypatch, tmp_path):
    grants = tmp_path / 'grants.yaml'
    sources = tmp_path / 'sources.yaml'
    out = tmp_path / 'candidate.yaml'
    evidence = tmp_path / 'evidence.json'
    candidates = tmp_path / 'candidates.json'
    coverage = tmp_path / 'coverage.json'

    grants.write_text(yaml.safe_dump({'grants': [_record('$1m')]}, sort_keys=False))
    sources.write_text(yaml.safe_dump({
        'scope': {'definition': 'Australian climate-tech funding pathways.'},
        'thresholds': {'extract_min_confidence': 0.93, 'discovery_candidate_min_confidence': 0.80, 'match_similarity': 0.93, 'jurisdiction_coverage_min_confidence': 0.90},
        'sources': [{'id': 'example', 'jurisdiction': 'national', 'required': True, 'allowed_domains': ['example.gov.au'], 'coverage_min_confidence': 0.90}],
    }, sort_keys=False))

    monkeypatch.setattr(update_grants, '_discover_one_source', lambda s, **kwargs: {
        'source_id': 'example', 'jurisdiction': 'national', 'required': True,
        'allowed_domains': ['example.gov.au'], 'ok': True, 'errors': [],
        'passes': [{'pass_name': 'primary', 'coverage_confidence': 0.99, 'programmes': [], 'tool_source_urls': ['https://example.gov.au/fund']}],
    })
    monkeypatch.setattr(update_grants, 'extract_program_via_web', lambda **kwargs: _extract_result('$2m'))
    monkeypatch.setattr(update_grants, 'discover_programmes_via_web', lambda **kwargs: {
        'data': {'coverage_confidence': 0.99, 'programmes': [], 'search_notes': 'complete'},
        'tool_source_urls': ['https://example.gov.au/fund'], 'model': 'mock', 'response_id': 'cross',
    })
    monkeypatch.setenv('GRANTS_WEB_WORKERS', '1')
    monkeypatch.setattr(sys, 'argv', [
        'update_grants.py', '--input', str(grants), '--sources', str(sources), '--output', str(out),
        '--evidence', str(evidence), '--candidates', str(candidates), '--coverage', str(coverage),
        '--verified', '2026-08-31',
    ])

    update_grants.main()

    data = yaml.safe_load(out.read_text())
    assert len(data['grants']) == 1
    rec = data['grants'][0]
    assert rec['id'] == 'example-fund'
    assert rec['amount'] == '$2m'
    assert rec['history'][-1]['event'] == 'updated'
    assert rec['history'][-1]['changes']['amount'] == {'before': '$1m', 'after': '$2m'}

    led = json.loads(evidence.read_text())
    assert led['continuity']['baseline_visible_ids'] == ['example-fund']
    assert led['continuity']['unexplained_disappearances'] == []
    row = led['continuity']['dispositions'][0]
    assert row['id'] == 'example-fund'
    assert row['disposition'] == 'updated'
    assert row['history_event_added'] is True
    assert led['verification_failures'] == []
