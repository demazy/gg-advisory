
from pathlib import Path
from datetime import date
import yaml, sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src"))
from grants_core import FetchResult, record_visible, verify_record, canonical_url
def fake_fetch_from_contract(contract):
    texts={}
    for row in contract["fields"].values():
        texts.setdefault(canonical_url(row["source_url"]),[]).append(row["support"])
    def fetch(u):
        cu=canonical_url(u)
        text=" ".join(texts.get(cu,[])) or "Official programme page with substantial current information."
        return FetchResult(u,u,True,200,"text/html","Official",text,"2026-09-01T00:00:00+00:00","x","","")
    return fetch
def test_all_visible_seed_records_pass_replay_contract():
    seed=yaml.safe_load((ROOT/"config/grants_seed_2026-09-01.yaml").read_text())
    con=yaml.safe_load((ROOT/"config/grants_evidence_contracts.yaml").read_text())["records"]
    for g in seed["grants"]:
        if not record_visible(g,date(2026,9,1)): continue
        r=verify_record(g,con[g["id"]],date(2026,9,1),fetcher=fake_fetch_from_contract(con[g["id"]]))
        assert r["pass"], (g["id"],r["issues"])
