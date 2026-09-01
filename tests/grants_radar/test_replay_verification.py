from pathlib import Path
from datetime import date, timedelta
import json, sys, yaml
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from grants_core import FetchResult, record_visible, verify_record


def _docs():
    seed=yaml.safe_load((ROOT/'config/grants_seed_2026-09-01.yaml').read_text())
    con=yaml.safe_load((ROOT/'config/grants_evidence_contracts.yaml').read_text())['records']
    snap=yaml.safe_load((ROOT/'config/grants_snapshots.yaml').read_text())['records']
    return seed,con,snap


def fail_fetch(u):
    return FetchResult(u,u,False,403,'','','','2026-09-01T00:00:00+00:00','','HTTP 403','')


def test_exact_27_v6_failed_visible_records_pass_with_matching_fresh_snapshot_when_sources_blocked():
    seed,con,snap=_docs()
    fixture=json.loads((ROOT/'tests/fixtures/v6-failure-replay.json').read_text())
    byid={g['id']:g for g in seed['grants']}
    assert len(fixture['visible_record_ids'])==27
    for gid in fixture['visible_record_ids']:
        r=verify_record(byid[gid],con[gid],snap[gid],date(2026,9,1),fetcher=fail_fetch)
        assert r['pass'],(gid,r)
        assert r['verification_mode']=='fresh_snapshot_fallback'


def test_snapshot_45_days_old_is_allowed():
    seed,con,snap=_docs(); g=next(g for g in seed['grants'] if g['id']=='startmate-accelerator')
    s=dict(snap[g['id']]); s['verified_date']=(date(2026,9,1)-timedelta(days=45)).isoformat()
    r=verify_record(g,con[g['id']],s,date(2026,9,1),fetcher=fail_fetch)
    assert r['pass']


def test_snapshot_46_days_old_fails_closed():
    seed,con,snap=_docs(); g=next(g for g in seed['grants'] if g['id']=='startmate-accelerator')
    s=dict(snap[g['id']]); s['verified_date']=(date(2026,9,1)-timedelta(days=46)).isoformat()
    r=verify_record(g,con[g['id']],s,date(2026,9,1),fetcher=fail_fetch)
    assert not r['pass']
    assert any('snapshot_stale' in x for x in r['issues'])


def test_snapshot_fingerprint_mismatch_fails_closed_when_source_blocked():
    seed,con,snap=_docs(); g=dict(next(g for g in seed['grants'] if g['id']=='startmate-accelerator'))
    g['amount']='tampered amount'
    r=verify_record(g,con[g['id']],snap[g['id']],date(2026,9,1),fetcher=fail_fetch)
    assert not r['pass']
    assert any('snapshot_fingerprint_mismatch' in x for x in r['issues'])


def test_explicit_open_registry_vs_closed_live_source_is_hard_failure():
    seed,con,snap=_docs(); g=next(g for g in seed['grants'] if g['id']=='startmate-accelerator')
    def fetch(u):
        return FetchResult(u,u,True,200,'text/html','Startmate Accelerator','Startmate Accelerator applications are now closed. Investment is $120,000.','2026-09-01T00:00:00+00:00','x','','')
    r=verify_record(g,con[g['id']],snap[g['id']],date(2026,9,1),fetcher=fetch)
    assert not r['pass']
    assert any('live_status_contradiction' in x for x in r['issues'])


def test_different_current_application_deadline_is_hard_failure():
    seed,con,snap=_docs(); g=next(g for g in seed['grants'] if g['id']=='clean-technology-innovation-grant')
    def fetch(u):
        text='Clean Technology Innovation Grant applications are open. Funding from $500,000 to $5,000,000. Applications close 30 September 2026.'
        return FetchResult(u,u,True,200,'text/html','Clean Technology Innovation Grant',text,'2026-09-01T00:00:00+00:00','x','','')
    r=verify_record(g,con[g['id']],snap[g['id']],date(2026,9,1),fetcher=fetch)
    assert not r['pass']
    assert any('live_deadline_contradiction' in x for x in r['issues'])


def test_accessible_page_with_partial_sentinels_can_use_fresh_snapshot_without_false_token_overlap_failure():
    seed,con,snap=_docs(); g=next(g for g in seed['grants'] if g['id']=='arena-fmia-innovation-fund')
    def fetch(u):
        text='Future Made in Australia Innovation Fund. Up to $1.5 billion in grant funding. This is an ongoing open program and will remain open until funding is exhausted.'
        return FetchResult(u,u,True,200,'text/html','Future Made in Australia Innovation Fund',text,'2026-09-01T00:00:00+00:00','x','','')
    r=verify_record(g,con[g['id']],snap[g['id']],date(2026,9,1),fetcher=fetch)
    assert r['pass'],r
    assert not any('evidence_support_changed' in x for x in r['issues'])


def test_all_visible_seed_records_are_exact_snapshot_fingerprint_matches():
    from grants_core import factual_fingerprint
    seed,_,snap=_docs()
    visible=[g for g in seed['grants'] if record_visible(g,date(2026,9,1))]
    assert all(factual_fingerprint(g)==snap[g['id']]['record_fingerprint'] for g in visible)
