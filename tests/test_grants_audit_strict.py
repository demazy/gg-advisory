from datetime import date
from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from grants_core import factual_sentences, required_evidence_fields, validate_record_schema


def _record():
    return {
        "id": "x",
        "name": "Example Climate Grant",
        "admin": "Example Agency",
        "level": "national",
        "type": "grant",
        "status": "Open now",
        "amount": "$1 million",
        "deadline": "2099-09-08",
        "deadline_type": "fixed",
        "deadline_label": "Applications close 8 September 2099",
        "target_stage": "Pilot and demonstration",
        "url": "https://example.gov.au/grant",
        "description": "The program funds climate technology pilots. Applicants must be Australian entities.",
        "why_it_matters": "Relevant for ventures ready to demonstrate technology.",
        "signals": "Applications close on 8 September 2099.",
    }


def test_fixed_deadline_requires_deadline_evidence():
    fields = required_evidence_fields(_record())
    assert "deadline" in fields
    assert "deadline_type" in fields
    assert "level" in fields
    assert "type" in fields


def test_rolling_program_does_not_require_null_deadline_evidence():
    r = _record()
    r["deadline"] = None
    r["deadline_type"] = "rolling"
    r["deadline_label"] = "Always open"
    assert "deadline" not in required_evidence_fields(r)


def test_description_and_signals_are_split_into_auditable_claims():
    claims = factual_sentences(_record())
    paths = [p for p, _ in claims]
    assert paths == ["description:1", "description:2", "signals:1"]


def test_source_universe_covers_all_australian_jurisdictions():
    cfg = yaml.safe_load((ROOT / "config" / "grants_sources.yaml").read_text())
    required = [s for s in cfg["sources"] if s.get("required")]
    jurisdictions = {s["jurisdiction"] for s in required}
    assert jurisdictions >= {"national", "act", "nsw", "nt", "qld", "sa", "tas", "vic", "wa"}
    assert all(s.get("index_urls") for s in required)


def test_audit_thresholds_use_confidence_as_safety_floor_not_arbitrary_veto():
    cfg = yaml.safe_load((ROOT / "config" / "grants_sources.yaml").read_text())
    t = cfg["thresholds"]
    assert 0.60 <= t["extract_hard_min_confidence"] <= 0.80
    assert 0.60 <= t["validator_hard_min_confidence"] <= 0.80
    assert t["new_candidate_min_confidence"] >= 0.90


def test_v51_matching_threshold_present():
    cfg = yaml.safe_load((ROOT / "config" / "grants_sources.yaml").read_text())
    t = cfg["thresholds"]
    assert t["match_similarity"] >= 0.90


def test_every_mandatory_source_has_domain_restriction():
    cfg = yaml.safe_load((ROOT / "config" / "grants_sources.yaml").read_text())
    required = [s for s in cfg["sources"] if s.get("required")]
    assert required
    assert all(s.get("allowed_domains") for s in required)


def test_tracked_specialist_provider_domains_are_in_source_universe():
    cfg = yaml.safe_load((ROOT / "config" / "grants_sources.yaml").read_text())
    domains = {d for s in cfg["sources"] for d in (s.get("allowed_domains") or [])}
    assert {"unswfounders.com", "newcastle.edu.au", "chiefscientist.smartygrants.com.au"} <= domains
