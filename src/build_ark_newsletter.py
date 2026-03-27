# -*- coding: utf-8 -*-
"""
Build a professional Word newsletter for ARK Capture Solutions.

Designed to be sent directly to ARK's leadership team (CEO/CTO/BD).
Branding: "Prepared by GG Advisory for ARK Capture Solutions"

Sections:
  - Executive Summary (bullet points)
  - Grants & Funding
  - Market & Policy
  - Competitors
  - Partners & Buyers

Usage:
    python -m src.build_ark_newsletter out/ark/monthly-digest-2026-03.md
"""
from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

# ── Brand palette ─────────────────────────────────────────────────────────────
# GG Advisory teal (primary) + professional navy for headings
GG_TEAL     = RGBColor(0x1B, 0x7A, 0x6B)   # GG Advisory green/teal
GG_NAVY     = RGBColor(0x0D, 0x2D, 0x4A)   # deep navy for section headings
DARK_GREY   = RGBColor(0x2C, 0x2C, 0x2C)   # near-black body text
MID_GREY    = RGBColor(0x55, 0x55, 0x55)   # labels / dates
LIGHT_GREY  = RGBColor(0x88, 0x88, 0x88)   # footer / secondary text
LINK_BLUE   = RGBColor(0x00, 0x5E, 0xA2)   # hyperlinks
ACCENT_AMBER = RGBColor(0xE0, 0x8A, 0x00)  # subtle accent for "Why it matters" label


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _font(run, size_pt: float, bold=False, italic=False,
          color: RGBColor | None = None, underline=False):
    run.font.name = "Calibri"
    run.font.size = Pt(size_pt)
    run.font.bold = bold
    run.font.italic = italic
    run.font.underline = underline
    if color:
        run.font.color.rgb = color


def _para(doc, space_before=0, space_after=6, left_indent=0,
          alignment=WD_ALIGN_PARAGRAPH.LEFT):
    p = doc.add_paragraph()
    pf = p.paragraph_format
    pf.space_before = Pt(space_before)
    pf.space_after  = Pt(space_after)
    pf.alignment    = alignment
    if left_indent:
        pf.left_indent = Inches(left_indent)
    return p


def _hrule(doc, color_hex: str = "1B7A6B", thickness: int = 12):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(1)
    p.paragraph_format.space_after  = Pt(1)
    pPr = p._p.get_or_add_pPr()
    pBdr = OxmlElement("w:pBdr")
    bottom = OxmlElement("w:bottom")
    bottom.set(qn("w:val"),   "single")
    bottom.set(qn("w:sz"),    str(thickness))
    bottom.set(qn("w:space"), "1")
    bottom.set(qn("w:color"), color_hex)
    pBdr.append(bottom)
    pPr.append(pBdr)
    return p


def _hyperlink(paragraph, url: str, display: str):
    """Insert a clickable hyperlink run into an existing paragraph."""
    part = paragraph.part
    r_id = part.relate_to(
        url,
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink",
        is_external=True,
    )
    hl = OxmlElement("w:hyperlink")
    hl.set(qn("r:id"), r_id)
    hl.set(qn("w:history"), "1")
    run = OxmlElement("w:r")
    rPr = OxmlElement("w:rPr")
    rStyle = OxmlElement("w:rStyle")
    rStyle.set(qn("w:val"), "Hyperlink")
    rPr.append(rStyle)
    color_el = OxmlElement("w:color")
    color_el.set(qn("w:val"), "005EA2")
    rPr.append(color_el)
    for sz_tag in ("w:sz", "w:szCs"):
        sz = OxmlElement(sz_tag)
        sz.set(qn("w:val"), "20")   # 10pt
        rPr.append(sz)
    run.append(rPr)
    t = OxmlElement("w:t")
    t.text = display
    run.append(t)
    hl.append(run)
    paragraph._p.append(hl)


def _label_body(doc, label: str, body: str,
                label_color: RGBColor | None = None,
                body_color: RGBColor | None = None):
    p = _para(doc, space_before=2, space_after=3)
    rl = p.add_run(label)
    _font(rl, 10.5, bold=True, color=label_color or DARK_GREY)
    rb = p.add_run(body)
    _font(rb, 10.5, color=body_color or DARK_GREY)
    return p


# ── Section & article builders ────────────────────────────────────────────────

_SECTION_ICONS = {
    "Grants & Funding":  "GRANTS & FUNDING",
    "Market & Policy":   "MARKET & POLICY",
    "Competitors":       "COMPETITORS",
    "Partners & Buyers": "PARTNERS & BUYERS",
}

def _section_heading(doc, title: str):
    _para(doc, space_before=16, space_after=0)
    p = _para(doc, space_before=0, space_after=4)
    label = _SECTION_ICONS.get(title, title.upper())
    r = p.add_run(label)
    _font(r, 12, bold=True, color=GG_NAVY, underline=False)
    _hrule(doc, color_hex="0D2D4A", thickness=10)


def _article(doc, title: str, published: str, summary: str,
             why: str, signals: str, source_url: str):
    # Title
    p_title = _para(doc, space_before=10, space_after=2)
    r = p_title.add_run(title)
    _font(r, 11, bold=True, color=DARK_GREY)

    # Published date
    if published:
        p = _para(doc, space_before=0, space_after=2)
        rl = p.add_run("Published:  ")
        _font(rl, 10, bold=True, color=MID_GREY)
        rv = p.add_run(published)
        _font(rv, 10, italic=True, color=MID_GREY)

    # Summary
    if summary:
        _label_body(doc, "Summary:  ", summary)

    # Why it matters for ARK (accent label)
    if why:
        _label_body(doc, "Why it matters for ARK:  ", why,
                    label_color=ACCENT_AMBER)

    # Signals
    if signals:
        _label_body(doc, "Signals to watch:  ", signals, label_color=MID_GREY)

    # Source hyperlink
    if source_url:
        p = _para(doc, space_before=2, space_after=6)
        rl = p.add_run("Source:  ")
        _font(rl, 10, bold=True, color=MID_GREY)
        _hyperlink(p, source_url, source_url)


# ── Markdown parser ───────────────────────────────────────────────────────────

def _parse(md_text: str) -> dict:
    """
    Parse ARK brief markdown into:
      { 'title': str, 'exec_summary': [str], 'sections': [{'heading', 'articles': [...]}] }
    """
    lines = md_text.splitlines()
    result = {"title": "", "exec_summary": [], "sections": []}
    cur_sec = None
    cur_art = None
    in_exec = False

    def _flush():
        if cur_sec is not None and cur_art:
            cur_sec["articles"].append(dict(cur_art))

    i = 0
    while i < len(lines):
        raw = lines[i]
        s = raw.strip()

        if s.startswith("# ") and not s.startswith("## "):
            result["title"] = s[2:].strip()
            i += 1
            continue

        if s == "**Executive Summary**":
            in_exec = True
            i += 1
            continue

        if in_exec and s.startswith("- "):
            result["exec_summary"].append(s[2:].strip())
            i += 1
            continue

        if s == "---":
            in_exec = False
            i += 1
            continue

        if s.startswith("## "):
            in_exec = False
            _flush()
            cur_art = None
            cur_sec = {"heading": s[3:].strip(), "articles": []}
            result["sections"].append(cur_sec)
            i += 1
            continue

        if s.startswith("**") and s.endswith("**") and cur_sec is not None:
            in_exec = False
            _flush()
            cur_art = {
                "title": s[2:-2].strip(),
                "published": "", "summary": "", "why": "", "signals": "", "source": "",
            }
            i += 1
            continue

        if cur_art is not None:
            for prefix, field in (
                ("Published:", "published"),
                ("Summary:", "summary"),
                ("Why it matters for ARK:", "why"),
                ("Why it matters:", "why"),   # fallback
                ("Signals to watch:", "signals"),
                ("Source:", "source"),
            ):
                if s.startswith(prefix):
                    cur_art[field] = s[len(prefix):].strip()
                    break

        i += 1

    _flush()
    return result


# ── Main builder ──────────────────────────────────────────────────────────────

def build_newsletter(md_path: Path, out_path: Path) -> None:
    md_text = md_path.read_text(encoding="utf-8")
    brief   = _parse(md_text)

    doc = Document()

    # Page margins — generous for a client doc
    for sec in doc.sections:
        sec.top_margin    = Inches(1.0)
        sec.bottom_margin = Inches(1.0)
        sec.left_margin   = Inches(1.1)
        sec.right_margin  = Inches(1.1)

    # Default font
    style = doc.styles["Normal"]
    style.font.name = "Calibri"
    style.font.size = Pt(11)
    style.font.color.rgb = DARK_GREY

    # ── Letterhead ────────────────────────────────────────────────────────────
    p_from = doc.add_paragraph()
    p_from.paragraph_format.space_before = Pt(0)
    p_from.paragraph_format.space_after  = Pt(2)
    r = p_from.add_run("GG Advisory  |  APAC Carbon Capture Intelligence")
    _font(r, 9, color=MID_GREY)

    p_for = doc.add_paragraph()
    p_for.paragraph_format.space_before = Pt(0)
    p_for.paragraph_format.space_after  = Pt(6)
    r = p_for.add_run("Prepared for ARK Capture Solutions  |  Confidential")
    _font(r, 9, italic=True, color=MID_GREY)

    _hrule(doc, color_hex="1B7A6B", thickness=20)

    # ── Title ─────────────────────────────────────────────────────────────────
    p_title = doc.add_paragraph()
    p_title.paragraph_format.space_before = Pt(8)
    p_title.paragraph_format.space_after  = Pt(2)
    title_text = brief["title"] or "ARK Intelligence Brief"
    r = p_title.add_run(title_text)
    _font(r, 20, bold=True, color=GG_TEAL)

    p_sub = doc.add_paragraph()
    p_sub.paragraph_format.space_before = Pt(0)
    p_sub.paragraph_format.space_after  = Pt(10)
    r = p_sub.add_run(
        "Monthly AU/APAC intelligence on grants, policy, competitors, and industrial partners"
    )
    _font(r, 10.5, italic=True, color=MID_GREY)

    _hrule(doc, color_hex="1B7A6B", thickness=8)

    # ── Executive Summary ─────────────────────────────────────────────────────
    p_es = _para(doc, space_before=12, space_after=4)
    r = p_es.add_run("Executive Summary")
    _font(r, 12, bold=True, color=GG_NAVY)

    bullets = brief["exec_summary"]
    if not bullets:
        bullets = ["No executive summary available — see section details below."]
    for bt in bullets:
        p = doc.add_paragraph(style="List Bullet")
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after  = Pt(2)
        r = p.add_run(bt)
        _font(r, 10.5, color=DARK_GREY)

    # ── Sections ──────────────────────────────────────────────────────────────
    for sec_data in brief["sections"]:
        _section_heading(doc, sec_data["heading"])
        if not sec_data["articles"]:
            p = _para(doc, space_before=4, space_after=4)
            r = p.add_run("No in-range items selected for this section.")
            _font(r, 10.5, italic=True, color=MID_GREY)
        for art in sec_data["articles"]:
            _article(
                doc,
                title      = art["title"],
                published  = art["published"],
                summary    = art["summary"],
                why        = art["why"],
                signals    = art["signals"],
                source_url = art["source"],
            )

    # ── Footer ────────────────────────────────────────────────────────────────
    _para(doc, space_before=18, space_after=0)
    _hrule(doc, color_hex="AAAAAA", thickness=6)
    p_footer = _para(doc, space_before=4, space_after=2)
    r = p_footer.add_run(
        "© GG Advisory  |  gg-advisory.org  |  "
        "This brief is prepared exclusively for ARK Capture Solutions. "
        "Confidential — not for redistribution."
    )
    _font(r, 8.5, italic=True, color=LIGHT_GREY)

    p_date = _para(doc, space_before=0, space_after=0)
    r = p_date.add_run(f"Generated: {datetime.now().strftime('%d %B %Y')}")
    _font(r, 8.5, color=LIGHT_GREY)

    doc.save(out_path)
    print(f"[ark-newsletter] Saved: {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    md_in = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("out/ark/monthly-digest-2026-03.md")
    doc_out = Path(sys.argv[2]) if len(sys.argv) > 2 else md_in.with_name(
        md_in.stem.replace("monthly-digest", "ark-intelligence-brief") + ".docx"
    )
    build_newsletter(md_in, doc_out)
