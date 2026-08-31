from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import grants_core


class DummyResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


def _search_response():
    return {
        'id': 'resp_search_1',
        'output': [
            {
                'type': 'web_search_call',
                'action': {
                    'type': 'search',
                    'sources': [
                        {'type': 'url', 'url': 'https://business.gov.au/grants-and-programs/industry-growth-program'}
                    ],
                },
            },
            {
                'type': 'message',
                'content': [
                    {'type': 'output_text', 'text': 'The Industry Growth Program page is on business.gov.au and is currently paused.'}
                ],
            },
        ],
    }


def _parser_response(payload=None):
    return {
        'id': 'resp_structure_1',
        'output': [
            {'type': 'message', 'content': [{'type': 'output_text', 'text': json.dumps(payload or {'found': True})}]}
        ],
    }


def test_two_stage_contract_separates_web_search_and_json_mode(monkeypatch):
    calls = []

    def fake_post(url, headers, json, timeout):
        calls.append(json)
        return DummyResponse(200, _search_response() if len(calls) == 1 else _parser_response())

    monkeypatch.setattr(grants_core, 'OPENAI_API_KEY', 'test-key')
    monkeypatch.setattr(grants_core.requests, 'post', fake_post)

    result = grants_core.web_search_json(
        prompt='Find the Industry Growth Program. Return JSON only as {"found": true}.',
        allowed_domains=['business.gov.au'],
        model='gpt-5.6-terra',
        search_context_size='low',
        max_output_tokens=500,
    )

    assert len(calls) == 2
    search_payload, parser_payload = calls
    assert search_payload['tool_choice'] == 'required'
    assert search_payload['tools'][0]['type'] == 'web_search'
    assert search_payload['tools'][0]['filters']['allowed_domains'] == ['business.gov.au']
    assert 'text' not in search_payload
    assert parser_payload['text']['format']['type'] == 'json_object'
    assert 'tools' not in parser_payload
    assert result['data'] == {'found': True}
    assert result['search_response_id'] == 'resp_search_1'
    assert result['structure_response_id'] == 'resp_structure_1'
    assert result['tool_source_urls'] == ['https://business.gov.au/grants-and-programs/industry-growth-program']


def test_missing_web_search_call_fails_closed(monkeypatch):
    raw = {'id': 'x', 'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'no tool'}]}]}
    monkeypatch.setattr(grants_core, 'OPENAI_API_KEY', 'test-key')
    monkeypatch.setattr(grants_core.requests, 'post', lambda *a, **k: DummyResponse(200, raw))
    try:
        grants_core.web_search_json(prompt='x', allowed_domains=['business.gov.au'])
    except RuntimeError as exc:
        assert 'no web_search_call' in str(exc)
    else:
        raise AssertionError('expected fail-closed error')


def test_no_official_source_url_fails_closed(monkeypatch):
    raw = _search_response()
    raw['output'][0]['action']['sources'] = [{'type': 'url', 'url': 'https://example.com/not-official'}]
    monkeypatch.setattr(grants_core, 'OPENAI_API_KEY', 'test-key')
    monkeypatch.setattr(grants_core.requests, 'post', lambda *a, **k: DummyResponse(200, raw))
    try:
        grants_core.web_search_json(prompt='x', allowed_domains=['business.gov.au'])
    except RuntimeError as exc:
        assert 'no source URL on the allowed official domains' in str(exc)
    else:
        raise AssertionError('expected fail-closed error')


def test_invalid_structured_json_fails_closed(monkeypatch):
    calls = []

    def fake_post(url, headers, json, timeout):
        calls.append(json)
        if len(calls) == 1:
            return DummyResponse(200, _search_response())
        return DummyResponse(200, {
            'id': 'bad',
            'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'not-json'}]}],
        })

    monkeypatch.setattr(grants_core, 'OPENAI_API_KEY', 'test-key')
    monkeypatch.setattr(grants_core.requests, 'post', fake_post)
    try:
        grants_core.web_search_json(prompt='x', allowed_domains=['business.gov.au'])
    except RuntimeError as exc:
        assert 'structuring stage returned invalid JSON' in str(exc)
    else:
        raise AssertionError('expected fail-closed error')
