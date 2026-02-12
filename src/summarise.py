# -*- coding: utf-8 -*-
"""
LLM summarisation for the monthly digest.

Incremental improvements (Feb 2026):
- Stronger "no hallucinations" instruction and explicit handling of low-extract items.
- Deterministic output structure (Top Lines + 3 sections) requested by the user.
- Per-item metadata passed explicitly (Publisher, Published, URL) to reduce model guessing.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import List

from openai import OpenAI

from .fetch import Item

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o-mini").strip()
TEMP = float(os.getenv("TEMP", "0.2"))

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def _month_label(ym: str) -> str:
    """YYYY-MM -> Month YYYY label."""
    try:
        dt = datetime.strptime(ym, "%Y-%m")
        return dt.strftime("%B %Y")
    except Exception:
        return ym


SYSTEM = (
    "You are an executive editor for GG Advisory. Create a concise monthly digest ONLY from the items provided.\n"
    "STRICT RULES:\n"
    "1) Do NOT invent facts, numbers, organisations, projects, dates, or quotes.\n"
    "2) Use ONLY the information contained in each item's Text (extracted content). Titles alone are not evidence.\n"
    "3) If an item's Text is too short/boilerplate to support a factual summary, mark it as 'Insufficient extract; see source.'\n"
    "4) Never merge facts across different items unless explicitly stated in the Text.\n"
    "5) Always include the URL as the only source for each item.\n"
)

USER_TMPL = """Create **Signals Digest — {date_label}** with this structure:

# Signals Digest — {date_label}

## Top Lines
- 3 bullets with macro takeaways supported by the provided item Texts.

Then for each section (use exactly these headings, even if empty):
## Energy Transition
## ESG Reporting
## Sustainable Finance & Investment

Under each section, include up to 4 items (aim for balance across sections). For each item, use this template:

### <Headline (≤10 words)>
- **PUBLISHER:** <Publisher field>
- **PUBLISHED:** <ISO date if provided else Unknown>
- **Summary:** 120–160 words, factual. If Text is insufficient, write: "Insufficient extract; see source."
- **Why it matters:** 1–2 bullets grounded in the Text.
- **Source:** <URL>

Constraints:
- Keep everything concise.
- Do not exceed 12 total items across all sections.
- If there are zero usable items across all sections, output exactly: NO_ITEMS_IN_RANGE

Items (JSON):
{items_json}
"""


def _prepare_items(items: List[Item]) -> List[dict]:
    out = []
    for it in items:
        text = (it.summary or "").strip()
        # Provide an explicit text-length signal to help the model avoid guessing.
        out.append(
            {
                "Section": it.section or "",
                "Title": it.title or "",
                "Publisher": it.source or "",
                "Published": getattr(it, "published_iso", None) or None,
                "URL": it.url or "",
                "TextChars": len(text),
                "Text": text[:6000],  # cap token usage
            }
        )
    return out


def build_digest(ym: str, items: List[Item]) -> str:
    date_label = _month_label(ym)
    payload = _prepare_items(items)
    items_json = json.dumps(payload, ensure_ascii=False, indent=2)

    if not client:
        # Deterministic fallback (no OPENAI_API_KEY)
        out = [f"# Signals Digest — {date_label}", "", "NO_OPENAI_API_KEY", ""]
        for it in items[:12]:
            out.append(f"- {it.section}: {it.title} ({it.url})")
        return "\n".join(out).strip() + "\n"

    msg = USER_TMPL.format(date_label=date_label, items_json=items_json)

    resp = client.chat.completions.create(
        model=MODEL,
        temperature=TEMP,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": msg},
        ],
    )
    return (resp.choices[0].message.content or "").strip() + "\n"


# Backwards compatible alias
build = build_digest
