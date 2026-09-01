import json
import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import grants_core
import update_grants


class DummyResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)
    def json(self):
        return self._payload


def test_source_search_low_self_confidence_is_warning_not_failure(monkeypatch):
    monkeypatch.setattr(update_grants, 'discover_programmes_via_web', lambda **kwargs: {
        'data': {'coverage_confidence': 0.78, 'search_notes': 'broad catalogue', 'programmes': []},
        'tool_source_urls': ['https://example.gov.au/funding'],
        'model': 'mock', 'response_id': 'r', 'search_response_id': 'r',
        'structure_response_id': 's', 'search_evidence_sha256': 'abc',
    })
    row = update_grants._discover_one_source(
        {'id': 'example', 'jurisdiction': 'national', 'required': True, 'allowed_domains': ['example.gov.au']},
        scope='climate-tech funding', verified=date(2026, 9, 1), known=[], model='mock'
    )
    assert row['ok'] is True
    assert row['errors'] == []
    assert row['coverage_confidence'] == 0.78


def test_candidate_matching_does_not_merge_same_name_across_states():
    existing = [{
        'id': 'act-energy-innovation-fund', 'name': 'Energy Innovation Fund',
        'level': 'act', 'url': 'https://act.example.gov.au/eif'
    }]
    score, match = update_grants._candidate_match({
        'name': 'Energy Innovation Fund', 'jurisdiction': 'vic',
        'url': 'https://vic.example.gov.au/eif'
    }, existing)
    assert score == 0.0
    assert match is None


def test_api_quota_exhaustion_fails_immediately_without_retry(monkeypatch):
    calls = {'n': 0}
    def fake_post(*args, **kwargs):
        calls['n'] += 1
        return DummyResponse(429, {
            'error': {
                'message': 'You have no credits remaining.',
                'type': 'insufficient_quota',
                'code': 'credit_balance_exhausted',
            }
        })
    monkeypatch.setattr(grants_core.requests, 'post', fake_post)
    try:
        grants_core._post_responses({'model': 'mock'})
    except RuntimeError as exc:
        assert 'OPENAI_API_QUOTA_OR_SPEND_LIMIT' in str(exc)
    else:
        raise AssertionError('expected quota exhaustion failure')
    assert calls['n'] == 1


def test_discovery_prompt_batches_known_verification_and_new_discovery(monkeypatch):
    captured = {}
    def fake_search(**kwargs):
        captured.update(kwargs)
        return {'data': {'coverage_confidence': 1, 'programmes': []}, 'tool_source_urls': ['https://example.gov.au']}
    monkeypatch.setattr(grants_core, 'web_search_json', fake_search)
    grants_core.discover_programmes_via_web(
        source_id='example', jurisdiction='national', allowed_domains=['example.gov.au'],
        scope_text='climate funding', verified=date(2026, 9, 1),
        known_programmes=[{'id': 'known-1', 'name': 'Known Fund', 'url': 'https://example.gov.au/known'}],
        model='mock'
    )
    prompt = captured['prompt']
    assert 'Explicitly account for EVERY known programme' in prompt
    assert 'genuinely NEW or successor pathways' in prompt
    assert 'field_evidence' in prompt
    assert captured['max_output_tokens'] >= 10000
