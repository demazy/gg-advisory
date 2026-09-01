# -*- coding: utf-8 -*-
"""Fail-closed structural/layout continuity check for the Funding Radar PDF.

The previous approved Radar PDF is a presentation reference only. Grant facts never
come from the PDF; they come from config/grants.yaml and the live audit pipeline.

The checker deliberately avoids brittle pixel-perfect comparison. It verifies the stable
brand/layout contract: page geometry, cover treatment, required sections, key anchor
positions, logo presence, sane page count and absence of off-page text blocks.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import fitz  # PyMuPDF

PIPELINE_VERSION = "6.0-zero-api-official-source"


def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def normalise_text(text: str) -> str:
    # Remove punctuation and repeated spacing; also collapse artificial letter-spacing
    # commonly emitted by PDF text extractors for the cover kicker/title.
    s = clean(text).lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _page_text(doc: fitz.Document, page_no: int) -> str:
    return clean(doc[page_no].get_text("text"))


def _doc_text(doc: fitz.Document) -> str:
    return "\n".join(_page_text(doc, i) for i in range(len(doc)))


def _contains_any(text: str, variants: Sequence[str]) -> bool:
    n = normalise_text(text)
    return any(normalise_text(v) in n for v in variants)


def _find_anchor_bbox(doc: fitz.Document, variants: Sequence[str], page_hint: Optional[int] = None) -> Optional[Tuple[int, fitz.Rect]]:
    pages = [page_hint] if page_hint is not None and 0 <= page_hint < len(doc) else range(len(doc))
    targets = [normalise_text(v) for v in variants]
    for pno in pages:
        page = doc[pno]
        for block in page.get_text("blocks"):
            if len(block) < 5:
                continue
            text = normalise_text(block[4])
            if any(t in text for t in targets):
                return pno, fitz.Rect(block[:4])
    return None


def _norm_rect(rect: fitz.Rect, page_rect: fitz.Rect) -> Tuple[float, float, float, float]:
    return (
        rect.x0 / page_rect.width,
        rect.y0 / page_rect.height,
        rect.x1 / page_rect.width,
        rect.y1 / page_rect.height,
    )


def _cover_samples(doc: fitz.Document) -> List[Tuple[int, int, int]]:
    page = doc[0]
    pix = page.get_pixmap(matrix=fitz.Matrix(0.35, 0.35), alpha=False)
    n = pix.n
    pts = [(0.03, 0.03), (0.50, 0.03), (0.97, 0.03), (0.03, 0.50), (0.97, 0.50), (0.03, 0.97), (0.97, 0.97)]
    out: List[Tuple[int, int, int]] = []
    data = pix.samples
    for fx, fy in pts:
        x = min(pix.width - 1, max(0, int((pix.width - 1) * fx)))
        y = min(pix.height - 1, max(0, int((pix.height - 1) * fy)))
        idx = (y * pix.width + x) * n
        rgb = tuple(int(data[idx + i]) for i in range(min(3, n)))
        if len(rgb) == 3:
            out.append(rgb)  # type: ignore[arg-type]
    return out


def _median_rgb(samples: Sequence[Tuple[int, int, int]]) -> Tuple[int, int, int]:
    if not samples:
        return (0, 0, 0)
    return tuple(int(median([x[i] for x in samples])) for i in range(3))  # type: ignore[return-value]


def _rgb_distance(a: Sequence[int], b: Sequence[int]) -> float:
    return math.sqrt(sum((int(a[i]) - int(b[i])) ** 2 for i in range(3)))


def _off_page_blocks(doc: fitz.Document, tolerance: float = 3.0) -> List[Dict[str, Any]]:
    bad: List[Dict[str, Any]] = []
    for pno, page in enumerate(doc):
        pr = page.rect
        for block in page.get_text("blocks"):
            if len(block) < 5 or not clean(block[4]):
                continue
            r = fitz.Rect(block[:4])
            if r.x0 < pr.x0 - tolerance or r.y0 < pr.y0 - tolerance or r.x1 > pr.x1 + tolerance or r.y1 > pr.y1 + tolerance:
                bad.append({"page": pno + 1, "bbox": [round(x, 2) for x in r], "text": clean(block[4])[:120]})
    return bad


def check(reference: Path, candidate: Path) -> Dict[str, Any]:
    ref = fitz.open(reference)
    cand = fitz.open(candidate)
    checks: Dict[str, Dict[str, Any]] = {}

    def add(name: str, passed: bool, detail: str, **extra: Any) -> None:
        checks[name] = {"pass": bool(passed), "detail": detail, **extra}

    # 1. Geometry must remain stable.
    ref_size = (round(ref[0].rect.width, 2), round(ref[0].rect.height, 2))
    cand_sizes = {(round(p.rect.width, 2), round(p.rect.height, 2)) for p in cand}
    add("page_geometry", cand_sizes == {ref_size}, f"reference={ref_size}; candidate_sizes={sorted(cand_sizes)}")

    # 2. Page count is content-driven. A completeness refresh may legitimately add many
    # programmes, so page-count delta is not a brand/layout failure by itself. Keep only a
    # broad sanity bound; geometry, anchors, cover treatment and off-page checks enforce the
    # actual layout contract.
    max_pages = max(30, len(ref) * 4)
    add("page_count_sanity", 5 <= len(cand) <= max_pages, f"reference={len(ref)} candidate={len(cand)} allowed=5..{max_pages}")

    # 3. Required branded/structural anchors.
    text = _doc_text(cand)
    anchors = {
        "cover_title": ["Funding the Climate-Tech Ecosystem"],
        "overview": ["How to use this radar"],
        "national_section": ["National programmes", "National programs"],
        "state_section": ["State & territory programmes", "State & territory programs"],
        "work_with_us": ["WORK WITH US", "How GG Advisory can help"],
        "sources": ["Sources and verification"],
    }
    missing = [name for name, variants in anchors.items() if not _contains_any(text, variants)]
    add("required_sections", not missing, "all required layout anchors present" if not missing else "missing: " + ", ".join(missing), missing=missing)

    # 4. Cover title position stays near the approved reference.
    ref_anchor = _find_anchor_bbox(ref, ["Funding the Climate-Tech Ecosystem"], page_hint=0)
    cand_anchor = _find_anchor_bbox(cand, ["Funding the Climate-Tech Ecosystem"], page_hint=0)
    if ref_anchor and cand_anchor:
        rn = _norm_rect(ref_anchor[1], ref[0].rect)
        cn = _norm_rect(cand_anchor[1], cand[0].rect)
        pos_delta = max(abs(rn[0] - cn[0]), abs(rn[1] - cn[1]))
        add("cover_title_position", pos_delta <= 0.10, f"normalised position delta={pos_delta:.3f}", reference=rn, candidate=cn)
    else:
        add("cover_title_position", False, "could not locate cover title in reference or candidate")

    # 5. Branded cover background remains visually consistent at stable edge sample points.
    ref_rgb = _median_rgb(_cover_samples(ref))
    cand_rgb = _median_rgb(_cover_samples(cand))
    colour_delta = _rgb_distance(ref_rgb, cand_rgb)
    add("cover_colour_continuity", colour_delta <= 65.0, f"reference_rgb={ref_rgb} candidate_rgb={cand_rgb} distance={colour_delta:.1f}")

    # 6. If the approved cover includes an image/logo, the candidate must too.
    ref_images = len(ref[0].get_images(full=True))
    cand_images = len(cand[0].get_images(full=True))
    add("cover_logo_presence", ref_images == 0 or cand_images > 0, f"reference_images={ref_images} candidate_images={cand_images}")

    # 7. No extracted text block may sit beyond the media box.
    off = _off_page_blocks(cand)
    add("no_off_page_text", not off, "no off-page text blocks" if not off else f"{len(off)} off-page text blocks", examples=off[:10])

    # 8. Last page remains the verification/sources page and contains substantive content.
    last_text = _page_text(cand, len(cand) - 1) if len(cand) else ""
    last_ok = _contains_any(last_text, ["Sources and verification"]) and len(last_text) >= 250
    add("sources_last_page", last_ok, f"last_page_text_chars={len(last_text)}")

    publishable = all(x.get("pass") for x in checks.values())
    return {
        "pipeline_version": PIPELINE_VERSION,
        "reference_pdf": str(reference),
        "candidate_pdf": str(candidate),
        "reference_pages": len(ref),
        "candidate_pages": len(cand),
        "pass": publishable,
        "checks": checks,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", type=Path, required=True)
    ap.add_argument("--candidate", type=Path, required=True)
    ap.add_argument("--output-json", type=Path, required=True)
    args = ap.parse_args()
    for p in (args.reference, args.candidate):
        if not p.exists() or p.stat().st_size < 1000:
            raise SystemExit(f"PDF layout gate input missing/too small: {p}")
    result = check(args.reference, args.candidate)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    for name, row in result["checks"].items():
        print(f"[layout] {'PASS' if row['pass'] else 'FAIL'} {name}: {row['detail']}")
    print(f"[layout] OVERALL {'PASS' if result['pass'] else 'FAIL'}")
    print(f"[write] {args.output_json}")
    if not result["pass"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
