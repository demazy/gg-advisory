from pathlib import Path
import ast
ROOT=Path(__file__).resolve().parents[2]


def all_runtime_text():
    return '\n'.join(p.read_text(errors='ignore') for base in [ROOT/'src',ROOT/'.github/workflows'] for p in base.rglob('*') if p.is_file() and p.suffix in {'.py','.yml','.yaml'})


def test_no_paid_api_runtime_markers():
    text=all_runtime_text()
    assert 'api.openai.com' not in text
    assert 'OPENAI_API_KEY' not in text
    assert 'web_search_json' not in text
    assert '/v1/responses' not in text


def test_workflow_manual_only_and_one_network_verification_stage():
    t=(ROOT/'.github/workflows/grants-radar.yml').read_text()
    assert 'workflow_dispatch:' in t
    assert 'schedule:' not in t
    assert 'Official-source verification with fresh-snapshot resilience and discovery delta' in t
    assert 'no second network crawl' in t
    assert 'OPENAI_API_KEY' not in t


def test_audit_module_does_not_import_requests_or_fetch_url():
    p=ROOT/'src/audit_grants.py'
    tree=ast.parse(p.read_text())
    names=[]
    for n in ast.walk(tree):
        if isinstance(n,ast.Import): names.extend(a.name for a in n.names)
        if isinstance(n,ast.ImportFrom): names.append(n.module or '')
    assert 'requests' not in names
    assert 'grants_core.fetch_url' not in names
    assert 'fetch_url' not in p.read_text()


def test_http_budget_is_hard_capped_below_v6_total_double_crawl():
    t=(ROOT/'.github/workflows/grants-radar.yml').read_text()
    assert 'GRANTS_MAX_HTTP_REQUESTS: "120"' in t
    # v6 made 93+47=140; v7 has one shared/cached verification+discovery stage capped at 120.
    assert t.count('GRANTS_MAX_HTTP_REQUESTS:')==1
