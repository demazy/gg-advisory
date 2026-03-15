"""
Generate a self-contained HTML snippet for the Grants & Accelerators Radar page.

Designed to be pasted into a Squarespace Code Block (or any CMS HTML editor).
Uses inline styles only — no external CSS dependencies.
Typography matches gg-advisory.org: Poppins body, Crimson Text headings, navy #041D52.

Usage (CLI):
    python src/build_grants_html.py config/grants.yaml 2026-03
    → writes out/grants-radar-2026-03.html

Called from the pipeline:
    from .build_grants_html import build_grants_html
    build_grants_html(yaml_path, out_path, ym)
"""

from __future__ import annotations

import calendar
import html
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


# ── Site colour palette (matched to gg-advisory.org) ─────────────────────────
NAVY        = "#041D52"   # site H1 colour
TEAL        = "#1B7A6B"   # GG Advisory primary accent
ORANGE      = "#C05000"   # urgent / closing soon
SLATE       = "#3A526B"   # state/territory
BODY_TEXT   = "#424B5F"   # site paragraph colour
MID_GREY    = "#6B7280"   # secondary text
RULE_GREY   = "#D1D5DB"   # light divider
TEAL_BG     = "#EAF4F2"   # card header tint — teal
ORANGE_BG   = "#FEF0E8"   # card header tint — orange
SLATE_BG    = "#EDF0F4"   # card header tint — slate

FONT_BODY    = "'Poppins', sans-serif"
FONT_HEADING = "'Crimson Text', Georgia, serif"

GRANTS_URGENT_DAYS = 60

TYPE_LABELS = {
    "grant":           "Grant",
    "repayable_grant": "Repayable Grant",
    "accelerator":     "Accelerator",
    "incubator":       "Incubator",
    "equity":          "Equity Investment",
    "debt_equity":     "Debt + Equity",
}

LEVEL_LABELS = {
    "national": "🇦🇺 National",
    "vic": "VIC", "nsw": "NSW", "qld": "QLD",
    "sa": "SA",   "wa":  "WA",  "tas": "TAS",
    "act": "ACT", "nt":  "NT",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _e(s: Any) -> str:
    """HTML-escape a value."""
    return html.escape(str(s or ""), quote=True)

def _clean(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()

def _parse_date(s: Any) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None

def _format_month_year(ym: str) -> str:
    try:
        y, m = int(ym[:4]), int(ym[5:7])
        return f"{calendar.month_name[m]} {y}"
    except Exception:
        return ym


# ── Data loading & classification (same logic as build_grants_docx) ──────────

def load_and_classify(yaml_path: Path, ym: str):
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    entries = raw.get("grants") or []

    y, mo = int(ym[:4]), int(ym[5:7])
    last_day = date(y + 1, 1, 1) - timedelta(days=1) if mo == 12 else date(y, mo + 1, 1) - timedelta(days=1)
    ref_date = last_day + timedelta(days=7)

    urgent, national, state = [], [], []

    for e in entries:
        if not isinstance(e, dict):
            continue
        sf = _parse_date(e.get("show_from"))
        su = _parse_date(e.get("show_until"))
        dl = _parse_date(e.get("deadline"))
        dt = str(e.get("deadline_type") or "rolling").lower()
        lv = str(e.get("level") or "national").lower()

        if sf and ref_date < sf:
            continue
        if su and ref_date > su:
            continue
        if dt == "fixed" and dl and dl < ref_date:
            continue

        if dt == "fixed" and dl:
            days = (dl - ref_date).days
            if -7 <= days <= GRANTS_URGENT_DAYS:
                urgent.append(dict(e, _deadline_obj=dl))
                continue
        (national if lv == "national" else state).append(e)

    urgent.sort(key=lambda e: e.get("_deadline_obj") or date.max)
    national.sort(key=lambda e: str(e.get("id") or ""))
    state.sort(key=lambda e: (str(e.get("level") or ""), str(e.get("id") or "")))
    return urgent, national, state


# ── HTML rendering ─────────────────────────────────────────────────────────────

def _badge(text: str, bg: str, fg: str = "white") -> str:
    return (
        f'<span style="display:inline-block;background:{bg};color:{fg};'
        f'font-family:{FONT_BODY};font-size:11px;font-weight:600;'
        f'padding:3px 10px;border-radius:3px;margin-right:6px;'
        f'letter-spacing:0.03em;text-transform:uppercase;">{_e(text)}</span>'
    )

def _section_heading(title: str, color: str, rule_color: str) -> str:
    return f"""
<div style="margin:40px 0 16px 0;padding-bottom:10px;border-bottom:3px solid {rule_color};">
  <h2 style="font-family:{FONT_HEADING};font-size:26px;font-weight:700;
             color:{color};margin:0;letter-spacing:-0.01em;">{_e(title)}</h2>
</div>"""

def _state_subheading(label: str) -> str:
    return f"""
<div style="margin:28px 0 8px 0;">
  <span style="font-family:{FONT_BODY};font-size:13px;font-weight:700;
               color:{SLATE};text-transform:uppercase;letter-spacing:0.08em;
               border-bottom:2px solid {SLATE};padding-bottom:2px;">{_e(label)}</span>
</div>"""

def _grant_card(entry: Dict, accent: str, bg_light: str) -> str:
    name          = _clean(entry.get("name"))
    admin         = _clean(entry.get("admin"))
    amount        = _clean(entry.get("amount"))
    deadline_lbl  = _clean(entry.get("deadline_label"))
    target_stage  = _clean(entry.get("target_stage"))
    description   = _clean(entry.get("description"))
    why           = _clean(entry.get("why_it_matters"))
    signals       = _clean(entry.get("signals"))
    url           = _clean(entry.get("url"))
    prog_type     = str(entry.get("type") or "grant").lower()
    level         = str(entry.get("level") or "national").lower()
    deadline_type = str(entry.get("deadline_type") or "rolling").lower()

    type_label  = TYPE_LABELS.get(prog_type, prog_type.replace("_", " ").title())
    level_label = LEVEL_LABELS.get(level, level.upper())

    if deadline_type == "fixed":
        dl_badge = _badge("⏰ Fixed Deadline", ORANGE)
    elif deadline_type == "rolling":
        dl_badge = _badge("Rolling — always open", TEAL)
    else:
        dl_badge = _badge("Date TBC", MID_GREY)

    type_badge  = _badge(type_label, accent)
    level_badge = _badge(level_label, bg_light, accent)

    # Key facts row
    facts_parts = []
    if amount:
        facts_parts.append(
            f'<span style="font-weight:600;color:{BODY_TEXT};">Funding:</span>'
            f'<span style="color:{accent};font-weight:600;"> {_e(amount)}</span>'
        )
    if deadline_lbl:
        facts_parts.append(
            f'<span style="font-weight:600;color:{BODY_TEXT};">Deadline:</span>'
            f' <span style="color:{BODY_TEXT};">{_e(deadline_lbl)}</span>'
        )
    if target_stage:
        facts_parts.append(
            f'<span style="font-weight:600;color:{BODY_TEXT};">Stage:</span>'
            f' <span style="color:{BODY_TEXT};">{_e(target_stage)}</span>'
        )

    facts_sep = f'<span style="color:{RULE_GREY};margin:0 10px;">|</span>'
    facts_html = facts_sep.join(facts_parts) if facts_parts else ""

    # Body paragraphs
    body_parts = []
    if description:
        body_parts.append(
            f'<p style="margin:12px 0 8px 0;font-family:{FONT_BODY};'
            f'font-size:15px;line-height:1.65;color:{BODY_TEXT};">{_e(description)}</p>'
        )
    if why:
        body_parts.append(
            f'<p style="margin:8px 0;font-family:{FONT_BODY};font-size:15px;'
            f'line-height:1.65;color:{BODY_TEXT};">'
            f'<span style="font-weight:700;color:{accent};">Why it matters: </span>'
            f'{_e(why)}</p>'
        )
    if signals:
        body_parts.append(
            f'<p style="margin:8px 0;font-family:{FONT_BODY};font-size:14px;'
            f'line-height:1.6;color:{MID_GREY};font-style:italic;">'
            f'<span style="font-weight:700;font-style:normal;color:{MID_GREY};">Signals to watch: </span>'
            f'{_e(signals)}</p>'
        )
    if url:
        body_parts.append(
            f'<p style="margin:10px 0 0 0;font-family:{FONT_BODY};font-size:13px;">'
            f'<span style="font-weight:600;color:{MID_GREY};">Source: </span>'
            f'<a href="{_e(url)}" target="_blank" rel="noopener" '
            f'style="color:{accent};text-decoration:underline;">{_e(url)}</a></p>'
        )

    body_html = "\n".join(body_parts)

    admin_html = ""
    if admin:
        admin_html = (
            f'<p style="margin:4px 0 0 0;font-family:{FONT_BODY};font-size:13px;'
            f'color:{MID_GREY};font-style:italic;">Administered by: {_e(admin)}</p>'
        )

    facts_row = ""
    if facts_html:
        facts_row = (
            f'<div style="margin:10px 0 0 0;padding:8px 16px;background:#F9FAFB;'
            f'border-radius:4px;font-family:{FONT_BODY};font-size:14px;'
            f'line-height:1.5;">{facts_html}</div>'
        )

    return f"""
<div style="border:1px solid {RULE_GREY};border-top:4px solid {accent};
            border-radius:6px;padding:20px 24px;margin:16px 0;
            background:white;">
  <div style="margin-bottom:10px;">
    {type_badge}{level_badge}{dl_badge}
  </div>
  <h3 style="font-family:{FONT_HEADING};font-size:22px;font-weight:700;
             color:{NAVY};margin:0 0 2px 0;line-height:1.25;">{_e(name)}</h3>
  {admin_html}
  {facts_row}
  <div style="margin-top:14px;padding-top:12px;border-top:1px solid {RULE_GREY};">
    {body_html}
  </div>
</div>"""


# ── Main builder ──────────────────────────────────────────────────────────────

def build_grants_html(yaml_path: Path, out_path: Path, ym: str) -> None:
    month_year = _format_month_year(ym)
    urgent, national, state_programs = load_and_classify(yaml_path, ym)

    parts: List[str] = []

    # Page intro block
    parts.append(f"""
<meta charset="UTF-8">
<div style="font-family:{FONT_BODY};max-width:860px;margin:0 auto;">

<p style="font-family:{FONT_BODY};font-size:13px;color:{MID_GREY};
          text-transform:uppercase;letter-spacing:0.08em;margin:0 0 4px 0;">
  GG Advisory &nbsp;·&nbsp; Cleantech &amp; Start-up Ecosystem
</p>
<p style="font-family:{FONT_BODY};font-size:14px;color:{BODY_TEXT};
          font-style:italic;margin:0 0 24px 0;line-height:1.6;">
  A curated overview of active grants, accelerators, and investment programs
  available to Australian climate-tech founders and investors.
  Entries are grouped by urgency and updated each month.
</p>
<hr style="border:none;border-top:3px solid {TEAL};margin:0 0 32px 0;">
""")

    # Group 1: Closing Soon
    if urgent:
        parts.append(_section_heading("⏰  Closing Soon — Deadlines Within 60 Days", ORANGE, ORANGE))
        for e in urgent:
            parts.append(_grant_card(e, ORANGE, ORANGE_BG))

    # Group 2: National Programs
    if national:
        parts.append(_section_heading("🇦🇺  National Programs — Open on a Rolling Basis", TEAL, TEAL))
        for e in national:
            parts.append(_grant_card(e, TEAL, TEAL_BG))

    # Group 3: State & Territory
    if state_programs:
        parts.append(_section_heading("📍  State & Territory Programs", SLATE, SLATE))
        current_state = None
        for e in state_programs:
            lv = str(e.get("level") or "").lower()
            if lv != current_state:
                current_state = lv
                lbl = LEVEL_LABELS.get(lv, lv.upper())
                parts.append(_state_subheading(lbl))
            parts.append(_grant_card(e, SLATE, SLATE_BG))

    # Footer
    parts.append(f"""
<hr style="border:none;border-top:1px solid {RULE_GREY};margin:40px 0 12px 0;">
<p style="font-family:{FONT_BODY};font-size:12px;color:{MID_GREY};font-style:italic;margin:0;">
  © GG Advisory &nbsp;·&nbsp; gg-advisory.org &nbsp;·&nbsp; {_e(month_year)} edition &nbsp;·&nbsp;
  Information is indicative only — verify deadlines and eligibility directly with the administrator.
</p>

</div>""")

    html_out = "\n".join(parts)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_out, encoding="utf-8")
    print(f"Saved: {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    yaml_in = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("config/grants.yaml")
    ym_arg  = sys.argv[2] if len(sys.argv) > 2 else "2026-02"
    out     = Path(f"out/grants-radar-{ym_arg}.html")

    sys.path.insert(0, str(Path(__file__).parent))
    build_grants_html(yaml_in, out, ym_arg)
