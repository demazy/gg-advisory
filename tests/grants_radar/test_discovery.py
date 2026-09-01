
from pathlib import Path
import yaml, sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src"))
from grants_core import candidate_score, canonical_url
def test_known_decisions_are_explicit():
    d=yaml.safe_load((ROOT/"config/grants_candidate_decisions.yaml").read_text())["decisions"]
    assert len(d)>=60
    assert all(v.get("action") in {"include_or_match","match_existing","monitor_not_core"} for v in d.values())
def test_candidate_score_requires_signal():
    src={"url_terms":["innovation"],"exclude_url_terms":["privacy"]}
    scope={"funding_terms":["grant","funding"],"climate_terms":["climate","energy"]}
    assert candidate_score("https://x/grants/clean-energy","Clean energy grants",src,scope) >= 5
    assert candidate_score("https://x/privacy","Privacy",src,scope) < 2
