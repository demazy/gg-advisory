from pathlib import Path
import sys, yaml
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from grants_history import apply_history, validate_history


def test_seed_history_valid():
    d=yaml.safe_load((ROOT/'config/grants_seed_2026-09-01.yaml').read_text())
    bad={}
    for g in d['grants']:
        issues=validate_history(g)
        if issues: bad[g['id']]=issues
    assert not bad,bad


def test_unchanged_record_gets_explicit_unchanged_disposition_without_new_event():
    g={'id':'x','name':'X','level':'national','type':'grant','status':'Open now','url':'https://example.org/x','history':[]}
    out,disp,fields,added=apply_history(g,dict(g),verified_date='2026-09-01',source_urls=['https://example.org/x'])
    assert disp=='unchanged' and fields==[] and not added and out['history']==[]


def test_url_change_is_historically_recorded():
    old={'id':'x','name':'X','level':'national','type':'grant','status':'Open now','url':'https://example.org/old','history':[]}
    new=dict(old); new['url']='https://example.org/new'
    out,disp,fields,added=apply_history(old,new,verified_date='2026-09-01',source_urls=['https://example.org/new'])
    assert disp=='renamed_or_superseded' and fields==['url'] and added
    assert out['history'][-1]['changes']['url']['after']=='https://example.org/new'
