from datetime import date
from pathlib import Path
import sys, yaml
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from grants_core import factual_fingerprint, record_visible, validate_record_schema


def docs():
    seed=yaml.safe_load((ROOT/'config/grants_seed_2026-09-01.yaml').read_text())
    con=yaml.safe_load((ROOT/'config/grants_evidence_contracts.yaml').read_text())
    snap=yaml.safe_load((ROOT/'config/grants_snapshots.yaml').read_text())
    return seed,con,snap


def test_visible_records_have_contracts_and_snapshots():
    seed,con,snap=docs()
    visible=[g for g in seed['grants'] if record_visible(g,date(2026,9,1))]
    assert len(visible)==27
    assert {g['id'] for g in visible} <= set(con['records'])
    assert {g['id'] for g in visible} <= set(snap['records'])


def test_snapshot_fingerprints_match_seed():
    seed,_,snap=docs()
    for g in seed['grants']:
        if record_visible(g,date(2026,9,1)):
            assert snap['records'][g['id']]['record_fingerprint']==factual_fingerprint(g)


def test_visible_record_schema_valid():
    seed,_,_=docs()
    bad={g['id']:validate_record_schema(g) for g in seed['grants'] if record_visible(g,date(2026,9,1)) and validate_record_schema(g)}
    assert not bad,bad


def test_contracts_have_source_and_identity_sentinels():
    _,con,_=docs()
    for gid,c in con['records'].items():
        assert c.get('source_urls'),gid
        assert c.get('name_patterns_any'),gid
        assert c.get('snapshot_max_age_days')==45


def test_no_old_token_overlap_threshold_contract():
    text=(ROOT/'config/grants_evidence_contracts.yaml').read_text()
    assert 'min_support_score' not in text
    assert 'support_score' not in text
