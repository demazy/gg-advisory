from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / '.github' / 'workflows' / 'grants-radar.yml').read_text()
BUILDER = (ROOT / 'src' / 'build_grants_pdf.py').read_text()
CORE = (ROOT / 'src' / 'grants_core.py').read_text()
UPDATE = (ROOT / 'src' / 'update_grants.py').read_text()
AUDIT = (ROOT / 'src' / 'audit_grants.py').read_text()


def test_workflow_does_not_run_old_monthly_or_docx_pipeline():
    assert 'generate_monthly.py' not in WF
    assert 'build_grants_docx.py' not in WF
    assert 'build_grants_html.py' not in WF


def test_workflow_has_factual_gate_then_pdf_then_layout_gate_then_email():
    factual = WF.index('Hard factual publication gate')
    build = WF.index('Build audited Grants & Accelerators Radar PDF')
    layout = WF.index('Hard PDF layout continuity gate')
    email = WF.index('Email audited Grants & Accelerators Radar PDF only')
    assert factual < build < layout < email


def test_workflow_uses_canonical_yaml_and_promotes_approved_layout_reference():
    assert '--input config/grants.yaml' in WF
    assert 'cp out/grants-verified-candidate.yaml config/grants.yaml' in WF
    assert 'assets/reference/radar-layout-reference.pdf' in WF
    assert 'cp "${{ steps.when.outputs.pdf }}" assets/reference/radar-layout-reference.pdf' in WF


def test_workflow_uses_separate_web_discovery_and_audit_models():
    assert 'GRANTS_WEB_MODEL: "gpt-5.6-terra"' in WF
    assert 'GRANTS_WEB_AUDIT_MODEL: "gpt-5.6-sol"' in WF


def test_pdf_builder_refuses_missing_or_failed_audit_and_only_uses_published_audit_ids():
    assert '--audit-report' in BUILDER
    assert 'Audit report is not publishable' in BUILDER
    assert 'included_in_report' in BUILDER


def test_pipeline_markers_present():
    marker = 'PIPELINE_VERSION = "5.0-canonical-ledger-layout"'
    assert marker in CORE
    assert marker in UPDATE
    assert marker in AUDIT
    assert marker in (ROOT / 'src' / 'grants_history.py').read_text()
    assert marker in (ROOT / 'src' / 'check_pdf_layout.py').read_text()


def test_web_search_is_forced_and_two_stage():
    assert '"tool_choice": "required"' in CORE
    assert '_has_web_search_call' in CORE
    search_block = CORE[CORE.index('search_payload:'):CORE.index('raw_search = _post_responses(search_payload)')]
    parser_block = CORE[CORE.index('parser_payload:'):CORE.index('raw = _post_responses(parser_payload)')]
    assert '"text":' not in search_block
    assert '"text": {"format": {"type": "json_object"}}' in parser_block
    assert '"tools":' not in parser_block


def test_registry_continuity_is_hard_audit_gate():
    assert 'registry_continuity' in AUDIT
    assert 'history_ledger' in AUDIT
    assert 'baseline_continuity_audit' in AUDIT
    assert 'unexplained_disappearances' in UPDATE


def test_workflow_is_manual_only_for_now():
    assert 'workflow_dispatch:' in WF
    assert '\n  schedule:' not in WF
