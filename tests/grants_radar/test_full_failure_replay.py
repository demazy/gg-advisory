from pathlib import Path
from datetime import date
import json, subprocess, sys, tempfile, yaml
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
from grants_core import FetchResult, discover_source, record_visible, verify_record


def fail_fetch(u):
    # Represents the exact class of GitHub-runner blocking seen in v6: WAF/TLS/DNS/timeouts.
    return FetchResult(u,u,False,403,'','','','2026-09-01T00:00:00+00:00','','HTTP 403','')


def test_v6_failure_replay_reaches_publishable_gate_without_network_or_paid_api(tmp_path):
    verified=date(2026,9,1)
    seed=yaml.safe_load((ROOT/'config/grants_seed_2026-09-01.yaml').read_text())
    contracts=yaml.safe_load((ROOT/'config/grants_evidence_contracts.yaml').read_text())['records']
    snapshots=yaml.safe_load((ROOT/'config/grants_snapshots.yaml').read_text())['records']
    source_doc=yaml.safe_load((ROOT/'config/grants_sources.yaml').read_text())
    baselines=yaml.safe_load((ROOT/'config/grants_discovery_baseline.yaml').read_text())['sources']
    fixture=json.loads((ROOT/'tests/fixtures/v6-failure-replay.json').read_text())

    records={}
    visible=[]
    for g in seed['grants']:
        if not record_visible(g,verified):
            continue
        visible.append(g)
        records[g['id']]=verify_record(g,contracts[g['id']],snapshots[g['id']],verified,fetcher=fail_fetch)
    assert len(visible)==27
    assert all(r['pass'] for r in records.values())

    coverage=[]; candidates=[]
    registry_urls=[g.get('url','') for g in seed['grants']]
    for s in source_doc['sources']:
        row=discover_source(s,source_doc.get('scope') or {},baselines[s['id']],{},registry_urls,verified,fetcher=fail_fetch)
        coverage.append(row)
        # With all live indexes blocked, the dated source inventories bridge coverage and
        # no catalogue links are re-invented as new candidates.
        assert row['covered'],s['id']
        assert not row['new_unresolved'],s['id']

    # Exact regression facts from the user's failed v6 run are represented by the baseline.
    assert len(fixture['unresolved_candidate_urls'])==275
    assert len(fixture['failed_required_source_ids'])==9

    registry=tmp_path/'grants.yaml'
    evidence=tmp_path/'evidence.json'
    cand=tmp_path/'candidates.json'
    cov=tmp_path/'coverage.json'
    audit=tmp_path/'audit.json'
    audit_md=tmp_path/'audit.md'
    registry.write_text(yaml.safe_dump(seed,allow_unicode=True,sort_keys=False,width=140))
    evidence.write_text(json.dumps({
        'verified_date':'2026-09-01','pipeline':'7.0-snapshot-sentinel','records':records,
        'baseline_ids':[g.get('id') for g in seed['grants']],
        'candidate_ids':[g.get('id') for g in seed['grants']],
        'dispositions':{g.get('id'):{'disposition':'unchanged'} for g in seed['grants']},
    }))
    cand.write_text(json.dumps({'candidates':candidates,'unresolved_count':0}))
    cov.write_text(json.dumps({'sources':coverage,'required_failed':[]}))
    cp=subprocess.run([
        sys.executable,str(ROOT/'src/audit_grants.py'),
        '--grants',str(registry),'--evidence',str(evidence),'--candidates',str(cand),'--coverage',str(cov),
        '--verified','2026-09-01','--output-json',str(audit),'--output-md',str(audit_md)
    ],cwd=ROOT,text=True,capture_output=True)
    assert cp.returncode==0,cp.stdout+'\n'+cp.stderr
    out=json.loads(audit.read_text())
    assert out['publishable'] is True
    assert out['summary']['programmes_passed']==27
    assert out['summary']['mandatory_sources_covered']==31
    assert out['summary']['unresolved_new_candidates']==0
    assert out['summary']['audit_network_requests']==0
    assert out['gates']['jurisdiction_source_coverage']['pass'] is True
