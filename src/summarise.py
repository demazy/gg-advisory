# -*- coding: utf-8 -*-
"""
LLM summarisation for the monthly digest.

Why this file changed:
- Your GitHub Actions environment does NOT install the OpenAI Python SDK by default.
  My previous version imported `from openai import OpenAI`, which caused:
      ModuleNotFoundError: No module named 'openai'
- This version removes that dependency and calls the OpenAI HTTP API directly via `requests`,
  matching the pattern you previously used successfully.

Incremental improvements preserved from the previous proposal:
- Stronger anti-hallucination rules.
- Deterministic output structure (Top Lines + 3 sections).
- Explicit per-item metadata (Publisher, Published, URL) and a "TextChars" signal so the model
  can refuse to speculate on thin extracts.
- Graceful degradation: if OpenAI call fails, falls back to a deterministic structured digest.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests

from .fetch import Item

# ----------------------------
# OpenAI config (backwards compatible)
# ----------------------------
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY", "") or "").strip()

# Accept both naming conventions: OPENAI_MODEL/OPENAI_TEMPERATURE (older) and MODEL/TEMP (newer)
MODEL = (os.getenv("OPENAI_MODEL") or os.getenv("MODEL") or "gpt-4o-mini").strip()
TEMP = float(os.getenv("OPENAI_TEMPERATURE") or os.getenv("TEMP") or "0.2")

# Endpoint can be overridden (useful for proxies)
OPENAI_CHAT_URL = (os.getenv("OPENAI_CHAT_URL") or "https://api.openai.com/v1/chat/completions").strip()

REQ_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "45"))
REQ_RETRIES = int(os.getenv("OPENAI_RETRIES", "2"))
BACKOFF = float(os.getenv("OPENAI_BACKOFF", "1.8"))

# Hard cap to prevent runaway prompt size
MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "6000"))


def _month_label(ym: str) -> str:
    """YYYY-MM -> Month YYYY label (best-effort)."""
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
    "3) If an item's Text is too short/boilerplate to support a factual summary, write exactly: "
    "\"Insufficient extract; see source.\" for the Summary.\n"
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


def _prepare_items(items: List[Item]) -> List[Dict]:
    out: List[Dict] = []
    for it in items:
        text = (it.summary or "").strip()
        out.append(
            {
                "Section": (it.section or "").strip(),
                "Title": (it.title or "").strip(),
                "Publisher": (it.source or "").strip(),
                "Published": getattr(it, "published_iso", None) or None,
                "URL": (it.url or "").strip(),
                "TextChars": len(text),
                "Text": text[:MAX_TEXT_CHARS_PER_ITEM],
            }
        )
    return out


def _deterministic_structured_digest(date_label: str, items: List[Item], note: Optional[str] = None) -> str:
    """
    Deterministic fallback that never raises.
    It preserves the requested structure but does not attempt to summarise facts.
    """
    if not items:
        return "NO_ITEMS_IN_RANGE\n"

    sections = ["Energy Transition", "ESG Reporting", "Sustainable Finance & Investment"]
    by_sec: Dict[str, List[Item]] = {s: [] for s in sections}
    other: List[Item] = []

    for it in items:
        sec = (it.section or "").strip()
        if sec in by_sec:
            by_sec[sec].append(it)
        else:
            other.append(it)

    lines: List[str] = [f"# Signals Digest — {date_label}", "", "## Top Lines"]
    lines += ["- (LLM unavailable; fallback digest)"] * 3
    lines.append("")

    for sec in sections:
        lines.append(f"## {sec}")
        lines.append("")
        for it in by_sec[sec][:4]:
            pub = (it.source or "").strip() or "Unknown"
            published = getattr(it, "published_iso", None) or "Unknown"
            title = (it.title or "").strip() or "Untitled"
            url = (it.url or "").strip()
            lines += [
                f"### {title[:80]}",
                f"- **PUBLISHER:** {pub}",
                f"- **PUBLISHED:** {published}",
                '- **Summary:** Insufficient extract; see source.',
                "- **Why it matters:**",
                "  - See source.",
                f"- **Source:** {url}",
                "",
            ]

    # If everything was in "other", include a minimal appendix to avoid losing items
    if other:
        lines.append("## Appendix (Unclassified)")
        lines.append("")
        for it in other[:6]:
            lines.append(f"- {(it.title or 'Untitled').strip()} — {it.url}")

    if note:
        lines += ["", "---", "", f"> Note: {note}"]

    return "\n".join(lines).strip() + "\n"


def _openai_chat_completion(messages: List[Dict], model: str, temperature: float) -> str:
    """
    Calls OpenAI Chat Completions endpoint via HTTP (no SDK dependency).
    Raises on terminal failure; caller handles fallback.
    """
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "temperature": temperature,
        "messages": messages,
    }

    last_err: Optional[str] = None
    for attempt in range(REQ_RETRIES + 1):
        try:
            r = requests.post(
                OPENAI_CHAT_URL,
                headers=headers,
                data=json.dumps(payload, ensure_ascii=False),
                timeout=REQ_TIMEOUT,
            )
            if r.status_code == 429 and attempt < REQ_RETRIES:
                time.sleep(BACKOFF ** attempt)
                continue
            if r.status_code >= 400:
                last_err = f"{r.status_code}: {r.text[:400]}"
                if attempt < REQ_RETRIES:
                    time.sleep(BACKOFF ** attempt)
                    continue
                raise RuntimeError(last_err)

            data = r.json()
            content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
            if content and content.strip():
                return content.strip()
            last_err = "Empty completion"
        except Exception as e:
            last_err = str(e)
            if attempt < REQ_RETRIES:
                time.sleep(BACKOFF ** attempt)
                continue
            raise RuntimeError(last_err or "OpenAI call failed")

    raise RuntimeError(last_err or "OpenAI call failed")


def build_digest(ym: str, items: List[Item]) -> str:
    """
    Primary entry point used by generate_monthly.py: build_digest("YYYY-MM", items)
    """
    date_label = _month_label(ym)

    if not items:
        return "NO_ITEMS_IN_RANGE\n"

    if not OPENAI_API_KEY:
        return _deterministic_structured_digest(date_label, items, note="OPENAI_API_KEY not set; deterministic fallback used.")

    payload = _prepare_items(items)
    items_json = json.dumps(payload, ensure_ascii=False, indent=2)
    user_msg = USER_TMPL.format(date_label=date_label, items_json=items_json)

    try:
        content = _openai_chat_completion(
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            model=MODEL,
            temperature=TEMP,
        )
        return content.strip() + "\n"
    except Exception as e:
        return _deterministic_structured_digest(date_label, items, note=f"LLM summarisation failed; fallback used. Error: {e}")


# Backwards compatible alias (some older code may call summarise.build())
build = build_digest
