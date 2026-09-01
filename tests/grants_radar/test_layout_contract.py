from pathlib import Path

def test_reference_assets_present():
    root=Path(__file__).resolve().parents[2]
    assert (root/'assets/gg-advisory-logo.png').stat().st_size>1000
    assert (root/'assets/reference/radar-layout-reference-fallback.pdf').stat().st_size>10000
