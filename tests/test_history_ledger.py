from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from grants_history import apply_history, changes_between, validate_history


def base_record():
    return {
        'id': 'x', 'name': 'Example Fund', 'admin': 'Agency', 'level': 'national',
        'type': 'grant', 'status': 'Rolling', 'amount': '$1m', 'deadline': None,
        'deadline_type': 'rolling', 'deadline_label': 'Rolling', 'target_stage': 'Seed',
        'url': 'https://example.gov.au/fund', 'signals': '', 'include_in_report': True,
        'description': 'Old prose that should not create a history event.',
    }


def test_editorial_rewrite_does_not_create_history_event():
    old = base_record()
    new = dict(old)
    new['description'] = 'Completely rewritten prose.'
    out, disposition, changed, added = apply_history(old, new, verified_date='2026-08-31', source_urls=['https://example.gov.au/fund'])
    assert disposition == 'unchanged'
    assert changed == []
    assert added is False
    assert out['history'] == []


def test_material_change_records_before_after_and_sources():
    old = base_record()
    new = dict(old)
    new['status'] = 'Paused'
    new['deadline_label'] = 'Paused to new applications'
    out, disposition, changed, added = apply_history(old, new, verified_date='2026-08-31', source_urls=['https://example.gov.au/fund'])
    assert disposition == 'status_changed'
    assert added is True
    assert changed == ['deadline_label', 'status']
    event = out['history'][-1]
    assert event['changes']['status'] == {'before': 'Rolling', 'after': 'Paused'}
    assert event['source_urls'] == ['https://example.gov.au/fund']
    assert validate_history(out) == []


def test_archived_record_is_preserved_not_deleted():
    old = base_record()
    new = dict(old)
    new['status'] = 'Archived'
    new['include_in_report'] = False
    out, disposition, changed, added = apply_history(old, new, verified_date='2026-08-31', source_urls=['https://example.gov.au/fund'])
    assert out['id'] == 'x'
    assert disposition == 'archived'
    assert added
    assert 'status' in changed
    assert 'include_in_report' in changed


def test_new_record_gets_added_history_event():
    new = base_record()
    out, disposition, changed, added = apply_history(None, new, verified_date='2026-08-31', source_urls=['https://example.gov.au/fund'])
    assert disposition == 'added'
    assert added is True
    assert out['history'][0]['event'] == 'added'
    assert 'name' in out['history'][0]['changes']
