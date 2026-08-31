import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import audit_grants


def rec(rid, status='Rolling', include=True):
    r = {
        'id': rid, 'name': f'{rid} Fund', 'admin': 'Example Agency', 'level': 'national',
        'type': 'grant', 'status': status, 'amount': '$1m', 'deadline': None,
        'deadline_type': 'rolling' if status != 'Archived' else 'tbc',
        'deadline_label': 'Rolling' if status != 'Archived' else 'No future rounds',
        'target_stage': 'Commercialisation', 'url': f'https://example.gov.au/{rid}',
        'description': 'The fund supports climate technology commercialisation.',
        'why_it_matters': 'Relevant to climate-tech ventures.', 'signals': '',
        'show_from': '2026-01-01', 'show_until': '2026-08-31' if status == 'Archived' else None,
        'last_verified': '2026-08-31', 'include_in_report': include,
    }
    return r


def validator_for(record, **kwargs):
    checks = {}
    for f in ['name', 'admin', 'type', 'status', 'amount', 'deadline_label', 'target_stage']:
        checks[f] = {'supported': True, 'current_value': record.get(f), 'reason': '', 'source_url': record['url']}
    checks['description'] = {'supported': True, 'reason': '', 'source_url': record['url']}
    checks['why_it_matters'] = {'supported': True, 'reason': '', 'source_url': record['url']}
    return {
        'data': {'supported': True, 'confidence': 0.99, 'field_checks': checks, 'contradictions': [], 'material_issues': [], 'source_urls': [record['url']]},
        'tool_source_urls': [record['url']], 'model': 'mock-audit', 'response_id': 'v',
    }


def test_audit_passes_with_explicit_archived_transition_kept_in_registry(monkeypatch, tmp_path):
    current = rec('current')
    archived = rec('archived', status='Archived', include=False)
    archived['history'] = [{
        'verified_date': '2026-08-31', 'event': 'archived',
        'changes': {'status': {'before': 'Rolling', 'after': 'Archived'}, 'include_in_report': {'before': True, 'after': False}},
        'source_urls': [archived['url']],
    }]
    grants = tmp_path / 'grants.yaml'
    grants.write_text(yaml.safe_dump({'grants': [current, archived]}, sort_keys=False))
    sources = tmp_path / 'sources.yaml'
    sources.write_text(yaml.safe_dump({
        'scope': {'definition': 'Australian climate-tech funding pathways.'},
        'thresholds': {'validator_min_confidence': 0.95},
        'sources': [{'id': 'example', 'jurisdiction': 'national', 'required': True, 'allowed_domains': ['example.gov.au']}],
    }, sort_keys=False))
    evidence = tmp_path / 'evidence.json'
    evidence.write_text(json.dumps({
        'verification_failures': [],
        'records': {
            'current': {'preaudit_issues': [], 'source_urls': [current['url']]},
            'archived': {'preaudit_issues': [], 'source_urls': [archived['url']]},
        },
        'continuity': {
            'baseline_visible_ids': ['current', 'archived'],
            'dispositions': [
                {'id': 'current', 'disposition': 'unchanged', 'history_event_added': False},
                {'id': 'archived', 'disposition': 'archived', 'history_event_added': True},
            ],
            'unexplained_disappearances': [],
        },
    }))
    candidates = tmp_path / 'candidates.json'; candidates.write_text(json.dumps({'candidates': []}))
    coverage = tmp_path / 'coverage.json'; coverage.write_text(json.dumps({
        'sources': [{'source_id': 'example', 'required': True, 'ok': True}],
        'jurisdiction_crosschecks': [{'jurisdiction': 'national', 'ok': True}],
    }))
    outj = tmp_path / 'audit.json'; outm = tmp_path / 'audit.md'

    monkeypatch.setattr(audit_grants, 'independent_validate_via_web', validator_for)
    monkeypatch.setenv('GRANTS_WEB_AUDIT_WORKERS', '1')
    monkeypatch.setattr(sys, 'argv', [
        'audit_grants.py', '--grants', str(grants), '--sources', str(sources), '--evidence', str(evidence),
        '--candidates', str(candidates), '--coverage', str(coverage), '--verified', '2026-08-31',
        '--output-json', str(outj), '--output-md', str(outm),
    ])
    audit_grants.main()
    audit = json.loads(outj.read_text())
    assert audit['publishable'] is True
    assert audit['gates']['registry_continuity']['pass'] is True
    assert audit['gates']['baseline_continuity_audit']['pass'] is True
    by_id = {x['id']: x for x in audit['records']}
    assert by_id['current']['included_in_report'] is True
    assert by_id['archived']['included_in_report'] is False
