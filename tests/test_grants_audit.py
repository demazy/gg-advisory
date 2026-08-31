from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from grants_core import canonical_url, evidence_quote_is_literal, record_similarity, validate_record_schema


def valid_record():
    return {
        "id": "test-program",
        "name": "Test Clean Technology Grant",
        "admin": "Test Government",
        "level": "nsw",
        "type": "grant",
        "status": "Open now",
        "amount": "$500,000 to $5 million",
        "deadline": "2026-09-08",
        "deadline_type": "fixed",
        "deadline_label": "8 September 2026 at 12:00 pm AEST",
        "target_stage": "Pilot and demonstration",
        "url": "https://example.gov.au/grants/test",
        "description": "Supports pilot and demonstration projects.",
        "why_it_matters": "Relevant to climate-tech ventures seeking demonstration funding.",
    }


def test_literal_evidence_normalises_whitespace_and_punctuation():
    source = "Applications close on 8 September 2026 at 12:00 pm (AEST)."
    assert evidence_quote_is_literal("Applications close on 8 September 2026 at 12:00 pm (AEST).", source)
    assert not evidence_quote_is_literal("Applications close on 9 September 2026", source)


def test_schema_accepts_complete_record():
    assert validate_record_schema(valid_record()) == []


def test_schema_rejects_unknown_status():
    r = valid_record()
    r["status"] = "Probably open"
    assert "invalid:status" in validate_record_schema(r)


def test_fixed_deadline_requires_iso_date():
    r = valid_record()
    r["deadline"] = None
    assert "fixed_deadline_missing_date" in validate_record_schema(r)


def test_exact_canonical_url_is_duplicate():
    r = valid_record()
    assert record_similarity(r, "Different title", "https://www.example.gov.au/grants/test/") == 1.0


def test_canonical_url_strips_fragment_and_www():
    assert canonical_url("https://www.example.gov.au/grants/test/#apply") == "https://example.gov.au/grants/test"
