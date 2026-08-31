from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import check_pdf_layout


def test_layout_checker_is_structural_not_content_source():
    text = (ROOT / 'src' / 'check_pdf_layout.py').read_text()
    assert 'presentation reference only' in text
    assert 'Grant facts never' in text
    assert 'page_geometry' in text
    assert 'cover_colour_continuity' in text
    assert 'no_off_page_text' in text


def test_fallback_layout_reference_is_a_real_pdf():
    p = ROOT / 'assets' / 'reference' / 'radar-layout-reference-fallback.pdf'
    assert p.exists()
    assert p.stat().st_size > 10_000
    assert p.read_bytes()[:5] == b'%PDF-'


def test_layout_checker_accepts_approved_reference_against_itself():
    p = ROOT / 'assets' / 'reference' / 'radar-layout-reference-fallback.pdf'
    result = check_pdf_layout.check(p, p)
    assert result['pass'] is True
    assert all(row['pass'] for row in result['checks'].values())
