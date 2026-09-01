
from pathlib import Path
import re
ROOT=Path(__file__).resolve().parents[2]
def test_no_paid_api_runtime_markers():
    text="\n".join(p.read_text(errors="ignore") for base in [ROOT/"src",ROOT/".github/workflows"] for p in base.rglob("*") if p.is_file() and p.suffix in {".py",".yml",".yaml"})
    assert "api.openai.com" not in text
    assert "OPENAI_API_KEY" not in text
    assert "web_search_json" not in text
def test_workflow_manual_only():
    t=(ROOT/".github/workflows/grants-radar.yml").read_text()
    assert "workflow_dispatch:" in t
    assert "schedule:" not in t
    assert "Direct official-source verification and discovery" in t
    assert "OPENAI_API_KEY" not in t
    assert "api.openai.com" not in t
