
from pathlib import Path
from datetime import date
import yaml, sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src"))
from grants_core import record_visible, support_score, validate_record_schema
def test_seed_visible_records_all_have_contracts():
    seed=yaml.safe_load((ROOT/"config/grants_seed_2026-09-01.yaml").read_text())
    con=yaml.safe_load((ROOT/"config/grants_evidence_contracts.yaml").read_text())
    visible=[g for g in seed["grants"] if record_visible(g,date(2026,9,1))]
    assert len(visible)>=20
    assert {g["id"] for g in visible} <= set(con["records"])
    for g in visible:
        assert not validate_record_schema(g)
def test_support_score_numbers_weighted():
    s="Applications close 8 November 2026 and accepted companies receive $120,000 investment."
    page="The program is open. Applications close 8 November 2026. Investment: AUD $120,000."
    assert support_score(s,page) > 0.7
