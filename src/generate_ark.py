# -*- coding: utf-8 -*-
"""
ARK Capture Solutions — monthly intelligence brief entry point.

Patches the standard generate_monthly pipeline to use:
  - ark_summarise.build_ark_digest  (ARK sections + BD-focused system prompt
                                     + BASELINE_DELTA block)
  - build_ark_newsletter.build_newsletter  (client-ready Word newsletter,
                                            reads baseline YAML for Market Background)

Run:
    python -m src.generate_ark

Required env vars (same as generate_monthly plus the ARK-specific ones):
    CFG_SOURCES=config/ark-sources-current.yaml
    CFG_FILTERS=config/ark-filters.yaml
    CFG_BASELINE=config/ark-baseline.yaml
    STATE_FILE=state/ark-seen_urls.json
    OUT_DIR=out/ark
    OPENAI_API_KEY=...
    START_YM=YYYY-MM
    END_YM=YYYY-MM

Optional (for incremental/change detection):
    TIER1_RESULTS_FILE=state/ark-tier1-verify-YYYY-MM.json
    PREV_DIGEST_FILE=out/ark/monthly-digest-YYYY-MM.md  (previous month)
"""
from __future__ import annotations

import os
from pathlib import Path

# ── 1. Import pipeline module BEFORE patching ─────────────────────────────────
import src.generate_monthly as _gm

# ── 2. Patch build_digest with the ARK-specific summariser ───────────────────
from src.ark_summarise import build_ark_digest as _ark_digest
_gm.build_digest = _ark_digest   # replaces the name in generate_monthly's namespace


# ── 3. After the pipeline writes the markdown, build the ARK newsletter ───────

def _build_newsletters() -> None:
    """Build .docx newsletter for each generated month."""
    from src.build_ark_newsletter import build_newsletter

    out_dir      = Path(os.getenv("OUT_DIR", "out"))
    start_ym     = os.getenv("START_YM", os.getenv("YM", "")).strip()
    end_ym       = os.getenv("END_YM", start_ym).strip()
    baseline_path = os.getenv("CFG_BASELINE", "config/ark-baseline.yaml")

    if not start_ym:
        print("[generate_ark] START_YM not set; skipping newsletter build.")
        return

    for ym in _gm._iter_months(start_ym, end_ym):
        md_path = out_dir / f"monthly-digest-{ym}.md"
        if not md_path.exists():
            print(f"[generate_ark] Markdown not found for {ym}: {md_path} — skipping newsletter.")
            continue
        out_path = out_dir / f"ark-intelligence-brief-{ym}.docx"
        try:
            build_newsletter(md_path, out_path, baseline_path=baseline_path)
        except Exception as e:
            print(f"[generate_ark] Newsletter build failed for {ym}: {e}")


def main() -> None:
    # Run the patched pipeline (collects, filters, scores, summarises, writes .md + generic .docx)
    _gm.main()
    # Then produce the ARK-branded newsletter on top
    _build_newsletters()


if __name__ == "__main__":
    main()
