from datetime import date
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from update_grants import _validate_web_record


def full_record():
    return {
        'id': 'example', 'name': 'Example Climate Fund', 'admin': 'Example Agency',
        'level': 'national', 'type': 'grant', 'status': 'Rolling', 'amount': '$1 million',
        'deadline': None, 'deadline_type': 'rolling', 'deadline_label': 'Rolling',
        'target_stage': 'Commercialisation', 'url': 'https://example.gov.au/fund',
        'description': 'The fund supports climate technology commercialisation.',
        'why_it_matters': 'Relevant to climate-tech ventures seeking non-dilutive capital.',
        'signals': '', 'show_from': '2026-01-01', 'show_until': None, 'last_verified': '2026-07-01',
        'include_in_report': True,
    }


def evidence(url):
    fields = ['name', 'admin', 'status', 'amount', 'deadline_label', 'target_stage']
    return {f: {'source_url': url, 'support': f'official support for {f}'} for f in fields}


def test_existing_out_of_scope_pathway_is_archived_not_silently_deleted():
    old = full_record()
    updated = dict(old)
    updated.update({'status': 'Archived', 'deadline_type': 'tbc', 'deadline_label': 'No future rounds', 'last_verified': '2026-08-31'})
    result = {
        'data': {
            'record': updated,
            'field_evidence': evidence(updated['url']),
            'claim_evidence': [],
            'source_urls': [updated['url']],
            'overall_confidence': 0.99,
            'unresolved_conflict': False,
            'conflict_notes': [],
            'in_scope': False,
        },
        'tool_source_urls': [updated['url']],
        'model': 'test',
        'response_id': 'r',
        'search_response_id': 's',
        'structure_response_id': 'j',
        'search_evidence_sha256': 'abc',
    }
    rec, issues, ledger = _validate_web_record(
        old=old, result=result, allowed_domains=['example.gov.au'], verified=date(2026, 8, 31), hard_min_confidence=0.70
    )
    assert rec is not None
    assert rec['id'] == old['id']
    assert rec['status'] == 'Archived'
    assert rec['include_in_report'] is False
    assert rec['show_until'] == '2026-08-31'
    assert 'programme_out_of_scope' not in issues
    assert ledger['in_scope'] is False
