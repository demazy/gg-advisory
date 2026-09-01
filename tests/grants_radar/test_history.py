
from pathlib import Path
import yaml, sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src"))
from grants_history import validate_history
def test_seed_history_valid():
    d=yaml.safe_load((ROOT/"config/grants_seed_2026-09-01.yaml").read_text())
    bad={}
    for g in d["grants"]:
        issues=validate_history(g)
        if issues: bad[g["id"]]=issues
    assert not bad, bad
