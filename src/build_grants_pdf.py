# -*- coding: utf-8 -*-
"""Build the GG Advisory Climate-Tech Funding Radar as a branded PDF.

Usage:
    python src/build_grants_pdf.py config/grants.yaml 2026-08 \
        --output out/Australia-Climate-Tech-Funding-Radar-August-2026-GG-Advisory.pdf

The renderer intentionally does not fetch news or run the monthly digest pipeline.
It reads only the grants YAML and produces the polished Funding Radar report.

Optional YAML fields understood by this report builder:
    status: Open now | Rolling | Opening soon | Closed, monitor | Paused | Archived
    best_fit: short best-fit description (falls back to target_stage)
    last_verified: YYYY-MM-DD (per-entry, optional)
    include_in_report: false (to hide an entry)

If status is absent, a conservative status is inferred from deadline_type,
deadline/deadline_label and the report verification date.
"""
from __future__ import annotations

import argparse
import calendar
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    Flowable,
    Frame,
    KeepTogether,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

# -----------------------------------------------------------------------------
# Brand
# -----------------------------------------------------------------------------
NAVY = colors.HexColor("#092F56")
NAVY_DARK = colors.HexColor("#08294A")
TEAL = colors.HexColor("#1F8A84")
TEAL_DARK = colors.HexColor("#177772")
TEAL_LIGHT = colors.HexColor("#E9F3F3")
GOLD = colors.HexColor("#D2AD57")
BODY = colors.HexColor("#17385E")
MUTED = colors.HexColor("#5B6B80")
LINE = colors.HexColor("#D8E1E7")
PALE = colors.HexColor("#EFF5F7")
WHITE = colors.white

STATUS_COLORS = {
    "Open now": (colors.HexColor("#2E967C"), colors.white),
    "Rolling": (colors.HexColor("#DFF1EF"), TEAL_DARK),
    "Opening soon": (colors.HexColor("#2F78A5"), colors.white),
    "Closed, monitor": (colors.HexColor("#F6EBC8"), colors.HexColor("#A37D0B")),
    "Paused": (colors.HexColor("#E78A2C"), colors.white),
    "Archived": (colors.HexColor("#E8EDF2"), colors.HexColor("#7A8796")),
}
TYPE_COLORS = {
    "Grant": (TEAL, colors.white),
    "Accelerator": (colors.HexColor("#367BA2"), colors.white),
    "Equity Investment": (colors.HexColor("#35916F"), colors.white),
    "Debt + Equity": (NAVY, colors.white),
    "Repayable Grant": (colors.HexColor("#8D6B9F"), colors.white),
    "Incubator": (colors.HexColor("#6F7F93"), colors.white),
}
LEVEL_BG = colors.HexColor("#EAF1F5")

TYPE_LABELS = {
    "grant": "Grant",
    "repayable_grant": "Repayable Grant",
    "accelerator": "Accelerator",
    "incubator": "Incubator",
    "equity": "Equity Investment",
    "debt_equity": "Debt + Equity",
}
LEVEL_LABELS = {
    "national": "National",
    "act": "ACT",
    "nsw": "NSW",
    "qld": "QLD",
    "sa": "SA",
    "tas": "TAS",
    "vic": "VIC",
    "wa": "WA",
    "nt": "NT",
}
STATE_ORDER = ["act", "nsw", "qld", "sa", "tas", "vic", "wa", "nt"]

FONT = "Helvetica"
FONT_BOLD = "Helvetica-Bold"


def _register_fonts() -> None:
    global FONT, FONT_BOLD
    regular = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
    bold = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")
    if regular.exists() and bold.exists():
        pdfmetrics.registerFont(TTFont("GG-Regular", str(regular)))
        pdfmetrics.registerFont(TTFont("GG-Bold", str(bold)))
        FONT, FONT_BOLD = "GG-Regular", "GG-Bold"


_register_fonts()


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def parse_date(v: Any) -> Optional[date]:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except Exception:
        return None


def month_label(ym: str) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    return f"{calendar.month_name[m]} {y}"


def short_date(d: date) -> str:
    return f"{d.day} {calendar.month_abbr[d.month]} {d.year}"


def _status(entry: Dict[str, Any], verified: date) -> str:
    explicit = clean(entry.get("status"))
    if explicit:
        aliases = {
            "open": "Open now",
            "open now": "Open now",
            "rolling": "Rolling",
            "opening soon": "Opening soon",
            "closed": "Closed, monitor",
            "closed, monitor": "Closed, monitor",
            "paused": "Paused",
            "archived": "Archived",
        }
        return aliases.get(explicit.lower(), explicit)

    label = clean(entry.get("deadline_label")).lower()
    dtype = clean(entry.get("deadline_type") or "tbc").lower()
    dl = parse_date(entry.get("deadline"))

    if "paused" in label:
        return "Paused"
    if "archiv" in label or "no future" in label:
        return "Archived"
    if any(x in label for x in ("closed", "monitor", "next round")):
        return "Closed, monitor"
    if "opening soon" in label or "opens " in label or "expected" in label:
        return "Opening soon"
    if "open now" in label or "applications invited" in label:
        return "Open now"
    if dtype == "rolling":
        return "Rolling"
    if dtype == "fixed" and dl:
        return "Open now" if dl >= verified else "Closed, monitor"
    if dtype == "tbc":
        return "Opening soon"
    return "Rolling"


def load_entries(yaml_path: Path, verified: date) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    entries = []
    for e0 in raw.get("grants") or []:
        if not isinstance(e0, dict) or e0.get("include_in_report") is False:
            continue
        e = dict(e0)
        sf = parse_date(e.get("show_from"))
        su = parse_date(e.get("show_until"))
        # The report tracks current and recurring pathways. Honour the curated
        # visibility window, but explicit statuses can keep historical context.
        explicit_status = clean(e.get("status"))
        if not explicit_status:
            if sf and verified < sf:
                continue
            if su and verified > su:
                continue
        e["_status"] = _status(e, verified)
        e["_type_label"] = TYPE_LABELS.get(clean(e.get("type")).lower(), clean(e.get("type")).replace("_", " ").title() or "Grant")
        e["_level"] = clean(e.get("level") or "national").lower()
        e["_level_label"] = LEVEL_LABELS.get(e["_level"], e["_level"].upper())
        e["_best_fit"] = clean(e.get("best_fit")) or clean(e.get("target_stage"))
        entries.append(e)
    return raw, entries


# -----------------------------------------------------------------------------
# Flowables and styles
# -----------------------------------------------------------------------------
class RoundedBox(Flowable):
    def __init__(self, width: float, height: float, fill, stroke=LINE, radius=6, stroke_width=0.8):
        super().__init__()
        self.width = width
        self.height = height
        self.fill = fill
        self.stroke = stroke
        self.radius = radius
        self.stroke_width = stroke_width

    def draw(self):
        c = self.canv
        c.saveState()
        c.setLineWidth(self.stroke_width)
        c.setStrokeColor(self.stroke)
        c.setFillColor(self.fill)
        c.roundRect(0, 0, self.width, self.height, self.radius, stroke=1, fill=1)
        c.restoreState()


class Divider(Flowable):
    def __init__(self, width: float, color=TEAL, thickness=1.5, space=4):
        super().__init__()
        self.width = width
        self.height = space + thickness
        self.color = color
        self.thickness = thickness

    def draw(self):
        self.canv.setStrokeColor(self.color)
        self.canv.setLineWidth(self.thickness)
        self.canv.line(0, self.height - self.thickness, self.width, self.height - self.thickness)


class SectionHeader(Flowable):
    def __init__(self, kicker: str, title: str, width: float):
        super().__init__()
        self.width = width
        self.height = 54
        self.kicker = kicker
        self.title = title

    def draw(self):
        c = self.canv
        c.saveState()
        c.setFillColor(TEAL_DARK)
        t = c.beginText(0, 42)
        t.setFont(FONT_BOLD, 8.5)
        t.setCharSpace(1.6)
        t.textLine(self.kicker.upper())
        c.drawText(t)
        c.setFillColor(NAVY)
        c.setFont(FONT_BOLD, 16.5)
        c.drawString(0, 22, self.title)
        c.setStrokeColor(TEAL_DARK)
        c.setLineWidth(1.4)
        c.line(0, 8, self.width, 8)
        c.restoreState()


class StateHeader(Flowable):
    def __init__(self, label: str, width: float):
        super().__init__()
        self.width = width
        self.height = 28
        self.label = label

    def draw(self):
        c = self.canv
        c.saveState()
        c.setStrokeColor(TEAL_DARK)
        c.setLineWidth(3)
        c.line(1, 5, 1, 21)
        c.setFillColor(NAVY)
        c.setFont(FONT_BOLD, 12)
        c.drawString(9, 8, self.label)
        c.restoreState()


styles = getSampleStyleSheet()
P = ParagraphStyle(
    "P", fontName=FONT, fontSize=8.15, leading=11.4, textColor=BODY,
    spaceAfter=0, alignment=TA_LEFT,
)
P_SMALL = ParagraphStyle("P_SMALL", parent=P, fontSize=7.7, leading=10.5, textColor=MUTED)
P_CARD = ParagraphStyle("P_CARD", parent=P, fontSize=8.05, leading=11.0)
P_URL = ParagraphStyle("P_URL", parent=P, fontSize=7.6, leading=9.8, textColor=TEAL_DARK)
P_TITLE = ParagraphStyle("P_TITLE", fontName=FONT_BOLD, fontSize=11.0, leading=13, textColor=NAVY)
P_LABEL = ParagraphStyle("P_LABEL", fontName=FONT_BOLD, fontSize=6.8, leading=8.2, textColor=TEAL_DARK)
P_VALUE = ParagraphStyle("P_VALUE", fontName=FONT, fontSize=7.7, leading=10.3, textColor=BODY)
P_H2 = ParagraphStyle("P_H2", fontName=FONT_BOLD, fontSize=18, leading=21, textColor=NAVY)
P_KICKER = ParagraphStyle("P_KICKER", fontName=FONT_BOLD, fontSize=8.5, leading=10, textColor=TEAL_DARK, spaceAfter=4)


def badge(text: str, bg, fg) -> Table:
    t = Table([[Paragraph(f"<b>{text}</b>", ParagraphStyle("badge", fontName=FONT_BOLD, fontSize=6.2, leading=7.2, textColor=fg))]], colWidths=None)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("BOX", (0, 0), (-1, -1), 0, bg),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return t


def _card(entry: Dict[str, Any], width: float) -> KeepTogether:
    type_label = entry["_type_label"]
    level_label = entry["_level_label"]
    status = entry["_status"]
    type_bg, type_fg = TYPE_COLORS.get(type_label, (TEAL, colors.white))
    status_bg, status_fg = STATUS_COLORS.get(status, (colors.HexColor("#E8EDF2"), MUTED))

    badges = Table([[badge(type_label, type_bg, type_fg), badge(level_label, LEVEL_BG, NAVY), badge(status, status_bg, status_fg)]])
    badges.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 1.4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 1.4),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    title_row = Table(
        [[Paragraph(clean(entry.get("name")), P_TITLE), badges]],
        colWidths=[width * 0.66, width * 0.30],
    )
    title_row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
    ]))

    meta = Table([
        [Paragraph("FUNDING", P_LABEL), Paragraph(clean(entry.get("amount")), P_VALUE),
         Paragraph("DEADLINE", P_LABEL), Paragraph(clean(entry.get("deadline_label")) or "Verify with administrator", P_VALUE)],
        [Paragraph("STAGE", P_LABEL), Paragraph(clean(entry.get("target_stage")), P_VALUE),
         Paragraph("ADMINISTERED<br/>BY", P_LABEL), Paragraph(clean(entry.get("admin")), P_VALUE)],
    ], colWidths=[width * .13, width * .34, width * .15, width * .38])
    meta.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))

    body: List[Flowable] = [title_row, meta]
    description = clean(entry.get("description"))
    why = clean(entry.get("why_it_matters"))
    best = entry.get("_best_fit")
    signals = clean(entry.get("signals"))
    url = clean(entry.get("url"))
    if description:
        body.append(Paragraph(f"<b>Overview.</b> {description}", P_CARD))
        body.append(Spacer(1, 4))
    if why:
        body.append(Paragraph(f"<b>Why it matters.</b> {why}", P_CARD))
        body.append(Spacer(1, 4))
    if best:
        body.append(Paragraph(f'<font color="#177772"><b>Best fit:</b></font> {best}', P_CARD))
        body.append(Spacer(1, 4))
    if signals:
        body.append(Paragraph(f'<font color="#177772"><b>Signals to watch:</b></font> {signals}', P_SMALL))
        body.append(Spacer(1, 5))
    if url:
        body.append(Paragraph(url, P_URL))

    inner = Table([[body]], colWidths=[width], hAlign="LEFT")
    inner.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.7, LINE),
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("LEFTPADDING", (0, 0), (-1, -1), 13),
        ("RIGHTPADDING", (0, 0), (-1, -1), 13),
        ("TOPPADDING", (0, 0), (-1, -1), 11),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 11),
    ]))
    return KeepTogether([inner, Spacer(1, 10)])


# -----------------------------------------------------------------------------
# PDF document and pages
# -----------------------------------------------------------------------------
class RadarDoc(BaseDocTemplate):
    def __init__(self, filename: str, month: str, verified: date, logo: Optional[Path], **kwargs):
        super().__init__(filename, pagesize=A4, **kwargs)
        self.month = month
        self.verified = verified
        self.logo = logo
        self._page_count = 0
        frame = Frame(17 * mm, 18 * mm, A4[0] - 34 * mm, A4[1] - 36 * mm, id="normal", leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
        self.addPageTemplates(PageTemplate(id="all", frames=[frame], onPage=self._on_page))

    def _on_page(self, canvas, doc):
        page = canvas.getPageNumber()
        if page == 1:
            return
        canvas.saveState()
        canvas.setFillColor(MUTED)
        canvas.setFont(FONT, 6.8)
        footer = f"GG Advisory Pty Ltd · gg-advisory.com.au · Grants & Accelerators Radar · {self.month}"
        canvas.drawString(17 * mm, 10 * mm, footer)
        canvas.drawRightString(A4[0] - 17 * mm, 10 * mm, f"Page {page}")
        canvas.restoreState()


def cover_flowables(doc: RadarDoc, entries: List[Dict[str, Any]]) -> List[Flowable]:
    # Full-page cover is drawn as a single custom flowable.
    class Cover(Flowable):
        def __init__(self):
            super().__init__()
            self.width = A4[0] - 34 * mm
            self.height = A4[1] - 36 * mm

        def draw(self):
            c = self.canv
            x0, y0 = -17 * mm, -18 * mm
            c.saveState()
            c.setFillColor(NAVY)
            c.rect(x0, y0, A4[0], A4[1], stroke=0, fill=1)
            c.setFillColor(GOLD)
            c.rect(x0, A4[1] - 14, A4[0], 14, stroke=0, fill=1)

            # Logo in white tile
            tile_x, tile_y = 0, self.height - 66
            c.setFillColor(colors.white)
            c.roundRect(tile_x, tile_y, 120, 47, 6, stroke=0, fill=1)
            if doc.logo and doc.logo.exists():
                from reportlab.lib.utils import ImageReader
                img = ImageReader(str(doc.logo))
                iw, ih = img.getSize()
                maxw, maxh = 102, 31
                scale = min(maxw / iw, maxh / ih)
                w, h = iw * scale, ih * scale
                c.drawImage(img, tile_x + 9, tile_y + (47 - h) / 2, width=w, height=h, mask="auto")
            else:
                c.setFillColor(NAVY)
                c.setFont(FONT_BOLD, 15)
                c.drawString(tile_x + 12, tile_y + 17, "GG Advisory")

            c.setFillColor(colors.HexColor("#A8BBCB"))
            c.setFont(FONT, 7.6)
            c.drawString(0, self.height - 88, "Cleantech & start-up ecosystem intelligence")

            c.setFillColor(colors.HexColor("#57C9C1"))
            c.setFont(FONT_BOLD, 7.8)
            t = c.beginText(0, self.height - 142)
            t.setFont(FONT_BOLD, 7.8)
            t.setCharSpace(2.0)
            t.textLine("GRANTS & ACCELERATORS RADAR")
            c.drawText(t)

            c.setFillColor(colors.white)
            c.setFont(FONT_BOLD, 25)
            c.drawString(0, self.height - 171, "Funding the Climate-Tech")
            c.drawString(0, self.height - 202, "Ecosystem")

            intro = f"A curated radar of Australian climate-tech grants, accelerators and investment pathways, tagged by funding type and current status. {doc.month} verified update."
            p = Paragraph(intro, ParagraphStyle("cover_intro", fontName=FONT, fontSize=10.2, leading=14, textColor=colors.HexColor("#E5EBF0")))
            p.wrapOn(c, 360, 70)
            p.drawOn(c, 0, self.height - 267)

            # Radar motif
            cx, cy = self.width * .50, 210
            c.setStrokeColor(colors.HexColor("#315B79"))
            c.setLineWidth(0.6)
            for r in (24, 48, 72):
                c.circle(cx, cy, r, stroke=1, fill=0)
            c.line(cx - 78, cy, cx + 78, cy)
            c.line(cx, cy - 78, cx, cy + 78)
            c.setFillColor(colors.HexColor("#57C9C1"))
            pts = [(-38, 10), (-20, -5), (6, 36), (27, -28), (51, 21), (-54, -34), (10, -8)]
            for dx, dy in pts:
                c.circle(cx + dx, cy + dy, 2.1, stroke=0, fill=1)
            c.setFillColor(GOLD)
            for dx, dy in [(-6, 0), (42, 48), (-66, 51), (69, -17)]:
                c.circle(cx + dx, cy + dy, 2.3, stroke=0, fill=1)

            national = sum(1 for e in entries if e["_level"] == "national")
            state = len(entries) - national
            stats = [
                (str(len(entries)), "funding pathways\ntracked"),
                (str(national), "national pathways"),
                (str(state), "state/territory\npathways"),
                (short_date(doc.verified), "last verified"),
            ]
            box_w = (self.width - 30) / 4
            y = 82
            for i, (big, small) in enumerate(stats):
                x = i * (box_w + 10)
                c.setFillColor(colors.HexColor("#164C6A"))
                c.setStrokeColor(colors.HexColor("#4A8299"))
                c.roundRect(x, y, box_w, 39, 4, stroke=1, fill=1)
                c.setFillColor(colors.white)
                c.setFont(FONT_BOLD, 12 if i < 3 else 9)
                c.drawString(x + 8, y + 22, big)
                c.setFont(FONT, 6.7)
                for j, line in enumerate(small.split("\n")):
                    c.drawString(x + 8, y + 9 - j * 7, line)

            c.setStrokeColor(colors.HexColor("#37627C"))
            c.line(0, 58, self.width, 58)
            c.setFillColor(colors.HexColor("#B6C5D0"))
            c.setFont(FONT, 6.6)
            c.drawString(0, 38, "Prepared by GG Advisory Pty Ltd · gg-advisory.com.au")
            c.drawRightString(self.width, 38, doc.month)
            c.restoreState()

    return [Cover(), PageBreak()]


def overview_flowables(width: float, entries: List[Dict[str, Any]], verified: date) -> List[Flowable]:
    national = sum(1 for e in entries if e["_level"] == "national")
    state = len(entries) - national
    statuses = sorted(set(e["_status"] for e in entries))
    out: List[Flowable] = [
        Paragraph("OVERVIEW", P_KICKER),
        Paragraph("How to use this radar", P_H2),
        Divider(width),
        Spacer(1, 6),
        Paragraph(
            "This radar is a curated overview of current and recurring funding pathways for Australian climate-tech founders and investors: grants, accelerators, equity funds and concessional finance. Programs are grouped by national and state or territory coverage, and each is tagged by funding type and current status. Because the funding landscape moves quickly, program status, deadlines and eligibility can change.",
            P,
        ),
        Spacer(1, 8),
    ]

    verification = Table([[Paragraph(
        f'<b>Information last verified: {verified.day} {calendar.month_name[verified.month]} {verified.year}.</b> Program status, deadlines and eligibility can change without notice. Always verify directly with the administering body before applying.',
        ParagraphStyle("verify", parent=P, fontSize=7.8, leading=10.7)
    )]], colWidths=[width])
    verification.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), PALE),
        ("LINEBEFORE", (0, 0), (0, 0), 2.4, TEAL_DARK),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    out.extend([verification, Spacer(1, 11)])

    stat_vals = [(len(entries), "funding pathways<br/>tracked"), (national, "national programs"), (state, "state and territory<br/>programs"), (len(statuses), "status categories")]
    stat_cells = []
    stat_style = ParagraphStyle("stat_cell", fontName=FONT, fontSize=7.2, leading=9.2, textColor=MUTED, alignment=1)
    for value, label in stat_vals:
        stat_cells.append(Paragraph(f'<font name="{FONT_BOLD}" size="15" color="#092F56">{value}</font><br/>{label}', stat_style))
    stats_tbl = Table([stat_cells], colWidths=[width / 4] * 4, rowHeights=[43])
    stats_tbl.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.6, LINE),
        ("INNERGRID", (0, 0), (-1, -1), 8, colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    out.extend([stats_tbl, Spacer(1, 12)])

    out.append(Paragraph("<b>Status</b>", ParagraphStyle("legendhead", parent=P, fontName=FONT_BOLD, fontSize=7.5)))
    legend_items = []
    for status in ["Open now", "Rolling", "Opening soon", "Closed, monitor", "Paused", "Archived"]:
        if status not in statuses:
            continue
        bg, fg = STATUS_COLORS[status]
        legend_items.append(Table([[badge(status, bg, fg)]], colWidths=[None]))
    if legend_items:
        leg = Table([legend_items[:3], legend_items[3:6] if len(legend_items) > 3 else []], colWidths=[width / 3] * 3)
        leg.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
        out.append(leg)

    out.append(Spacer(1, 5))
    out.append(Paragraph("<b>Funding type</b>", ParagraphStyle("legendhead2", parent=P, fontName=FONT_BOLD, fontSize=7.5)))
    type_items = []
    for t in ["Grant", "Accelerator", "Equity Investment", "Debt + Equity"]:
        if any(e["_type_label"] == t for e in entries):
            bg, fg = TYPE_COLORS[t]
            type_items.append(badge(t, bg, fg))
    typet = Table([type_items], colWidths=[width / max(1, len(type_items))] * len(type_items))
    typet.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 7)]))
    out.extend([typet, Spacer(1, 12)])

    for heading, text in [
        ("READING THE FUNDING TYPES", "Grants are generally non-dilutive but competitive and milestone-based. Accelerators may be non-dilutive or equity-linked. Equity investment requires investor fit and negotiation. Debt and equity facilities are typically for larger, bankable or near-bankable projects. Each card also lists a best-fit stage and a source link. Figures are indicative: verify current deadlines, amounts and eligibility directly with the administrator before applying."),
        ("READING THE AMOUNTS", "Funding figures may refer to a total program pool, a per-project grant, an accelerator investment, or a fund size. Co-contribution means the applicant must contribute eligible cash or matched funding. TRL refers to technology readiness level, from proof-of-concept through to demonstration and deployment."),
    ]:
        tbl = Table([[Paragraph(f'<font color="#177772"><b>{heading}</b></font><br/><br/>{text}', ParagraphStyle("info", parent=P, fontSize=7.6, leading=10.2))]], colWidths=[width])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), PALE),
            ("LINEBEFORE", (0, 0), (0, 0), 2.3, TEAL_DARK),
            ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 12),
            ("TOPPADDING", (0, 0), (-1, -1), 10), ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ]))
        out.extend([tbl, Spacer(1, 10)])
    out.append(PageBreak())
    return out


def sources_page(width: float, month: str, verified: date) -> List[Flowable]:
    return [
        Paragraph("Sources and verification", P_H2),
        Divider(width),
        Spacer(1, 10),
        Paragraph(
            f"Program details in this report are drawn from the source URLs recorded in the grants configuration and should be checked against official government, agency and administering-body pages current to {verified.day} {calendar.month_name[verified.month]} {verified.year}. Fast-moving items such as deadlines, funding levels and open or closed status should be re-checked with the administering body before acting.",
            P,
        ),
        Spacer(1, 12),
        Paragraph(
            f"© GG Advisory Pty Ltd · gg-advisory.com.au · {month}. Information last verified {verified.day} {calendar.month_name[verified.month]} {verified.year}. This radar is general information only and is not legal, financial or investment advice. GG Advisory is not affiliated with, endorsed by, or acting on behalf of the programs listed. Information is indicative: verify deadlines, amounts and eligibility directly with each administering body before acting.",
            P_SMALL,
        ),
    ]


def build_report(yaml_path: Path, output: Path, ym: str, verified: date, logo: Optional[Path]) -> None:
    _, entries = load_entries(yaml_path, verified)
    if not entries:
        raise SystemExit("No grant entries are visible for the selected verification date.")

    output.parent.mkdir(parents=True, exist_ok=True)
    month = month_label(ym)
    doc = RadarDoc(str(output), month=month, verified=verified, logo=logo, leftMargin=17 * mm, rightMargin=17 * mm, topMargin=18 * mm, bottomMargin=18 * mm)
    width = A4[0] - 34 * mm

    national = sorted([e for e in entries if e["_level"] == "national"], key=lambda e: clean(e.get("name")))
    states: Dict[str, List[Dict[str, Any]]] = {k: [] for k in STATE_ORDER}
    for e in entries:
        if e["_level"] != "national":
            states.setdefault(e["_level"], []).append(e)
    for k in states:
        states[k].sort(key=lambda e: clean(e.get("name")))

    story: List[Flowable] = []
    story.extend(cover_flowables(doc, entries))
    story.extend(overview_flowables(width, entries, verified))

    story.append(SectionHeader("SECTION 1", "National programs", width))
    story.append(Spacer(1, 4))
    for e in national:
        story.append(_card(e, width))

    story.append(Spacer(1, 4))
    story.append(SectionHeader("SECTION 2", "State & territory programs", width))
    story.append(Spacer(1, 4))
    for state in STATE_ORDER:
        group = states.get(state) or []
        if not group:
            continue
        story.append(StateHeader(LEVEL_LABELS.get(state, state.upper()), width))
        for e in group:
            story.append(_card(e, width))

    # Work with us block, matching the reference report.
    work = Table([[Paragraph(
        '<font color="#177772"><b>WORK WITH US</b></font><br/><font size="16"><b>How GG Advisory can help</b></font><br/><br/>'
        'GG Advisory helps climate-tech founders and investors match to the right funding, structure competitive applications, and build the grant, incentive and investor pathways behind a credible growth plan. If a program here fits your venture, we can help you assess eligibility, prepare the funding narrative and structure the application.<br/><br/>'
        '<font color="#177772"><b>START A CONVERSATION</b></font><br/>www.gg-advisory.com.au · antonin@gg-advisory.com.au · linkedin.com/in/antonindemazy',
        ParagraphStyle("work", parent=P, fontSize=8.2, leading=11.4, textColor=BODY)
    )]], colWidths=[width])
    work.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), PALE),
        ("BOX", (0, 0), (-1, -1), 0.6, LINE),
        ("LEFTPADDING", (0, 0), (-1, -1), 13), ("RIGHTPADDING", (0, 0), (-1, -1), 13),
        ("TOPPADDING", (0, 0), (-1, -1), 12), ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
    ]))
    story.extend([Spacer(1, 8), work, PageBreak()])
    story.extend(sources_page(width, month, verified))

    doc.build(story)
    print(f"[write] {output}")


def cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("yaml_path", type=Path)
    p.add_argument("ym", help="Report month in YYYY-MM format")
    p.add_argument("--output", type=Path, default=None)
    p.add_argument("--verified", default=None, help="Verification date YYYY-MM-DD (default: today)")
    p.add_argument("--logo", type=Path, default=Path("assets/gg-advisory-logo.png"))
    args = p.parse_args()

    if not re.fullmatch(r"\d{4}-\d{2}", args.ym):
        raise SystemExit("ym must be YYYY-MM")
    verified = parse_date(args.verified) if args.verified else date.today()
    if not verified:
        raise SystemExit("--verified must be YYYY-MM-DD")
    out = args.output
    if out is None:
        label = month_label(args.ym).replace(" ", "-")
        out = Path("out") / f"Australia-Climate-Tech-Funding-Radar-{label}-GG-Advisory.pdf"
    build_report(args.yaml_path, out, args.ym, verified, args.logo)


if __name__ == "__main__":
    cli()
