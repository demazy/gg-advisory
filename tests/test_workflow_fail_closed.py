from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / '.github' / 'workflows' / 'grants-radar.yml').read_text()
BUILDER = (ROOT / 'src' / 'build_grants_pdf.py').read_text()


def test_workflow_does_not_run_old_monthly_or_docx_pipeline():
    assert 'generate_monthly.py' not in WF
    assert 'build_grants_docx.py' not in WF
    assert 'build_grants_html.py' not in WF


def test_workflow_has_hard_audit_gate_before_pdf():
    assert 'Hard publication gate' in WF
    assert 'Build audited Grants & Accelerators Radar PDF' in WF
    assert WF.index('Hard publication gate') < WF.index('Build audited Grants & Accelerators Radar PDF')


def test_workflow_uses_separate_web_discovery_and_audit_models():
    assert 'GRANTS_WEB_MODEL: "gpt-5.6-luna"' in WF
    assert 'GRANTS_WEB_AUDIT_MODEL: "gpt-5.6-terra"' in WF


def test_pdf_builder_refuses_missing_or_failed_audit():
    assert '--audit-report' in BUILDER
    assert 'Audit report is not publishable' in BUILDER


def test_v31_pipeline_markers_present():
    root = Path(__file__).resolve().parents[1]
    assert 'PIPELINE_VERSION = "3.1-web-search"' in (root / "src" / "grants_core.py").read_text()
    assert 'discovery=responses-web-search' in (root / "src" / "update_grants.py").read_text()
    assert 'audit=independent-responses-web-search' in (root / "src" / "audit_grants.py").read_text()
