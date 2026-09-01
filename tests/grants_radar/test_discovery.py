from pathlib import Path
from datetime import date
import json, sys, yaml
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from grants_core import FetchResult, candidate_score, canonical_url, discover_source


def _fail_fetch(u):
    return FetchResult(u,u,False,403,'','','','2026-09-01T00:00:00+00:00','','HTTP 403','')


def test_v6_275_unresolved_are_all_in_dated_baseline():
    fixture=json.loads((ROOT/'tests/fixtures/v6-failure-replay.json').read_text())
    baseline=yaml.safe_load((ROOT/'config/grants_discovery_baseline.yaml').read_text())['sources']
    known={canonical_url(u) for row in baseline.values() for u in row.get('known_urls',[])}
    missing=[u for u in fixture['unresolved_candidate_urls'] if canonical_url(u) not in known]
    assert not missing


def test_all_31_source_groups_have_baseline_inventory():
    cfg=yaml.safe_load((ROOT/'config/grants_sources.yaml').read_text())
    baseline=yaml.safe_load((ROOT/'config/grants_discovery_baseline.yaml').read_text())['sources']
    assert len(cfg['sources'])==31
    assert {s['id'] for s in cfg['sources']} <= set(baseline)


def test_exact_v6_failed_sources_are_covered_by_fresh_inventory_when_http_blocked():
    fixture=json.loads((ROOT/'tests/fixtures/v6-failure-replay.json').read_text())
    cfg=yaml.safe_load((ROOT/'config/grants_sources.yaml').read_text())
    baseline=yaml.safe_load((ROOT/'config/grants_discovery_baseline.yaml').read_text())['sources']
    scope=cfg.get('scope') or {}
    byid={s['id']:s for s in cfg['sources']}
    for sid in fixture['failed_required_source_ids']:
        row=discover_source(byid[sid],scope,baseline[sid],{},[],date(2026,9,1),fetcher=_fail_fetch)
        assert row['covered'],(sid,row)
        assert row['coverage_mode']=='fresh_snapshot'
        assert not row['new_unresolved']


def test_nt_jurisdiction_is_covered_by_source_even_without_visible_nt_record():
    cfg=yaml.safe_load((ROOT/'config/grants_sources.yaml').read_text())
    baseline=yaml.safe_load((ROOT/'config/grants_discovery_baseline.yaml').read_text())['sources']
    s=next(x for x in cfg['sources'] if x['id']=='nt-innovation')
    row=discover_source(s,cfg.get('scope') or {},baseline['nt-innovation'],{},[],date(2026,9,1),fetcher=_fail_fetch)
    assert row['jurisdiction']=='nt'
    assert row['covered']


def test_new_unseen_high_signal_link_blocks():
    cfg={'id':'x','jurisdiction':'national','required':True,'allowed_domains':['example.org'],'index_urls':['https://example.org/grants'],'discovery':True,'min_url_score':2,'snapshot_max_age_days':45}
    scope={'funding_terms':['grant'],'climate_terms':['energy']}
    html='<html><body><a href="/new-energy-grant">New energy grant</a></body></html>'
    def fetch(u):
        return FetchResult(u,u,True,200,'text/html','Index','Index','2026-09-01T00:00:00+00:00','x','',html)
    row=discover_source(cfg,scope,{'verified_date':'2026-09-01','known_urls':[]},{},[],date(2026,9,1),fetcher=fetch)
    assert row['new_unresolved']==['https://example.org/new-energy-grant']


def test_preexisting_high_signal_link_does_not_block():
    cfg={'id':'x','jurisdiction':'national','required':True,'allowed_domains':['example.org'],'index_urls':['https://example.org/grants'],'discovery':True,'min_url_score':2,'snapshot_max_age_days':45}
    scope={'funding_terms':['grant'],'climate_terms':['energy']}
    html='<a href="/old-energy-grant">Old energy grant</a>'
    def fetch(u): return FetchResult(u,u,True,200,'text/html','Index','Index','2026-09-01T00:00:00+00:00','x','',html)
    row=discover_source(cfg,scope,{'verified_date':'2026-09-01','known_urls':['https://example.org/old-energy-grant']},{},[],date(2026,9,1),fetcher=fetch)
    assert not row['new_unresolved']
    assert row['candidates'][0]['resolution']=='known_baseline'


def test_candidate_score_still_requires_signal():
    src={'url_terms':['innovation'],'exclude_url_terms':['privacy']}
    scope={'funding_terms':['grant','funding'],'climate_terms':['climate','energy']}
    assert candidate_score('https://x/grants/clean-energy','Clean energy grants',src,scope)>=5
    assert candidate_score('https://x/privacy','Privacy',src,scope)<2

def test_exact_v6_275_links_replayed_as_live_indexes_produce_zero_unresolved():
    fixture=json.loads((ROOT/'tests/fixtures/v6-failure-replay.json').read_text())
    grouped={}
    for c in fixture['unresolved_candidates']:
        grouped.setdefault(c.get('source_id'),[]).append(c)
    cfg=yaml.safe_load((ROOT/'config/grants_sources.yaml').read_text())
    baseline=yaml.safe_load((ROOT/'config/grants_discovery_baseline.yaml').read_text())['sources']
    byid={s['id']:s for s in cfg['sources']}
    total=0
    for sid,items in grouped.items():
        s=byid[sid]
        if s.get('discovery') is False:
            continue
        total += len(items)
        html='<html><body>'+''.join(f'<a href="{c["url"]}">{c.get("anchor") or "Energy grant funding"}</a>' for c in items)+'</body></html>'
        def fetch(u,html=html):
            return FetchResult(u,u,True,200,'text/html','Index','Index','2026-09-01T00:00:00+00:00','x','',html)
        row=discover_source(s,cfg.get('scope') or {},baseline[sid],{},[],date(2026,9,1),fetcher=fetch)
        assert not row['new_unresolved'],(sid,row['new_unresolved'][:5])
    assert total > 200

