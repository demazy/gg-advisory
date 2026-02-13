# -*- coding: utf-8 -*-
"""LLM summarisation for the monthly digest.

This module is intentionally defensive:
- It must NEVER hard-fail the GitHub Action.
- It must tolerate different Item shapes (dataclass/object or dict) without assuming `.text`.
- If the LLM output is truncated or structurally invalid, it falls back to a deterministic,
  low-hallucination, extractive digest.

Fixes included (v13):
- Remove all references to `it.text` (Item schema mismatch).
- Fallback digest uses the same safe text getter as the LLM payload.
- Validation does not raise uncaught exceptions; it triggers fallback.
- Accepts either "Sustainable Finance" or "Sustainable Finance & Investment" headings.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

# Import Item for type hints only; do not rely on its exact fields at runtime
try:
    from .fetch import Item  # type: ignore
except Exception:  # pragma: no cover
    Item = Any  # type: ignore


# ----------------------------
# OpenAI config (backwards compatible)
# ----------------------------
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY", "") or "").strip()
MODEL = (os.getenv("OPENAI_MODEL") or os.getenv("MODEL") or "gpt-4o-mini").strip()
TEMP = float(os.getenv("OPENAI_TEMPERATURE") or os.getenv("TEMP") or "0.2")
OPENAI_CHAT_URL = (os.getenv("OPENAI_CHAT_URL") or "https://api.openai.com/v1/chat/completions").strip()

OPENAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "45"))
OPENAI_RETRIES = int(os.getenv("OPENAI_RETRIES", "2"))
OPENAI_BACKOFF = float(os.getenv("OPENAI_BACKOFF", "1.8"))

# Response length control. Use a higher default to avoid truncation.
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "2800"))

MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "3500"))


def _month_label(ym: str) -> str:
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


def _get(it: Any, key: str, default: Any = "") -> Any:
    """Safe getter for both objects and dicts."""
    if it is None:
        return default
    if isinstance(it, dict):
        return it.get(key, default)
    return getattr(it, key, default)


def _get_text(it: Any) -> str:
    """Return best-available extracted text for an item."""
    # Try common names in order of expected richness
    for k in ("text", "full_text", "content", "summary", "snippet", "raw_text"):
        v = _get(it, k, "")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _prepare_items(items: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        text = _get_text(it)
        out.append(
            {
                "Section": str(_get(it, "section", "") or "").strip(),
                "Title": str(_get(it, "title", "") or "").strip(),
                "Publisher": str(_get(it, "source", "") or "").strip(),
                "Published": _get(it, "published_iso", None) or None,
                "URL": str(_get(it, "url", "") or "").strip(),
                "TextChars": len(text),
                "Text": text[:MAX_TEXT_CHARS_PER_ITEM],
            }
        )
    return out


def _extractive_summary(raw: str, max_words: int = 140) -> str:
    if not raw:
        return "Insufficient extract; see source."
    text = re.sub(r"\s+", " ", raw).strip()
    if len(text) < 200:
        return "Insufficient extract; see source."

    sents = re.split(r"(?<=[\.\!\?])\s+", text)
    picked: List[str] = []

    def add(sent: str) -> None:
        s = sent.strip()
        if not s or s in picked:
            return
        picked.append(s)

    for s in sents[:3]:
        add(s)

    for s in sents[3:]:
        if re.search(r"\b(20\d{2}|%|\$|€|MW|GW|Mt|bn|billion|million)\b", s, re.I):
            add(s)
        if len(" ".join(picked).split()) >= max_words:
            break

    out = " ".join(picked)
    words = out.split()
    if len(words) > max_words:
        out = " ".join(words[:max_words]).rstrip(" ,;:") + "…"
    return out


def _deterministic_structured_digest(date_label: str, items: List[Any], note: Optional[str] = None) -> str:
    # Never raise.
    if not items:
        return "NO_ITEMS_IN_RANGE\n"

    sections = ["Energy Transition", "ESG Reporting", "Sustainable Finance & Investment"]
    by_sec: Dict[str, List[Any]] = {s: [] for s in sections}
    other: List[Any] = []

    for it in items:
        sec = str(_get(it, "section", "") or "").strip()
        if sec in by_sec:
            by_sec[sec].append(it)
        else:
            other.append(it)

    lines: List[str] = [f"# Signals Digest — {date_label}", "", "## Top Lines"]
    lines += [
        "- (LLM unavailable/invalid; using extractive fallback summaries)",
        "- (Summaries below are verbatim sentence extracts; consult sources for full context)",
        "- (If this persists: increase OPENAI_TIMEOUT, OPENAI_MAX_TOKENS, or reduce MAX_TEXT_CHARS_PER_ITEM)",
        "",
    ]

    for sec in sections:
        lines.append(f"## {sec}")
        lines.append("")
        for it in by_sec[sec][:4]:
            pub = str(_get(it, "source", "") or "").strip() or "Unknown"
            published = _get(it, "published_iso", None) or "Unknown"
            title = str(_get(it, "title", "") or "").strip() or "Untitled"
            url = str(_get(it, "url", "") or "").strip()
            raw = _get_text(it)
            summ = _extractive_summary(raw, max_words=140)
            lines += [
                f"### {title[:80]}",
                f"- **PUBLISHER:** {pub}",
                f"- **PUBLISHED:** {published}",
                f"- **Summary:** {summ}",
                "- **Why it matters:**",
                "  - See source.",
                f"- **Source:** {url}",
                "",
            ]

    if other:
        lines.append("## Appendix (Unclassified)")
        lines.append("")
        for it in other[:6]:
            title = str(_get(it, "title", "") or "").strip() or "Untitled"
            url = str(_get(it, "url", "") or "").strip()
            lines.append(f"- {title} — {url}")

    if note:
        lines += ["", "---", "", f"> Note: {note}"]

    return "\n".join(lines).strip() + "\n"


def _openai_chat_completion(model: str, messages: List[Dict[str, str]], temperature: float = 0.2) -> Tuple[str, Optional[str]]:
    """Returns (content, finish_reason). Raises on final failure."""
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing")

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": OPENAI_MAX_TOKENS,
    }

    last_err: Optional[Exception] = None
    for attempt in range(max(1, OPENAI_RETRIES)):
        try:
            r = requests.post(OPENAI_CHAT_URL, headers=headers, json=payload, timeout=(10, OPENAI_TIMEOUT))
            r.raise_for_status()
            data = r.json()
            choice = (data.get("choices") or [{}])[0]
            msg = (choice.get("message") or {}).get("content") or ""
            finish = choice.get("finish_reason")
            return str(msg).strip(), finish
        except Exception as e:
            last_err = e
            if attempt < max(1, OPENAI_RETRIES) - 1:
                time.sleep(max(0.5, OPENAI_BACKOFF) ** attempt)
                continue
            break

    raise RuntimeError(f"OpenAI call failed after {OPENAI_RETRIES} attempt(s): {last_err}")


def _looks_structurally_valid(out: str) -> bool:
    """Loose validation: accept either Sustainable Finance heading form."""
    if not out or len(out) < 200:
        return False
    required = [
        "# Signals Digest",
        "## Top Lines",
        "## Energy Transition",
        "## ESG Reporting",
    ]
    if any(m not in out for m in required):
        return False
    if ("## Sustainable Finance & Investment" not in out) and ("## Sustainable Finance" not in out):
        return False
    return True


def build_digest(ym: str, items: List[Any]) -> str:
    date_label = _month_label(ym)

    if not items:
        return "NO_ITEMS_IN_RANGE\n"

    # If no key, always deterministic fallback.
    if not OPENAI_API_KEY:
        return _deterministic_structured_digest(date_label, items, note="OPENAI_API_KEY not set; deterministic fallback used.")

    try:
        payload = _prepare_items(items)
        items_json = json.dumps(payload, ensure_ascii=False, indent=2)
        user_msg = USER_TMPL.format(date_label=date_label, items_json=items_json)

        content, finish = _openai_chat_completion(
            model=MODEL,
            temperature=TEMP,
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user_msg},
            ],
        )

        out = (content or "").strip()
        # If truncated or structurally invalid, fallback.
        if finish == "length" or not _looks_structurally_valid(out):
            return _deterministic_structured_digest(
                date_label,
                items,
                note=f"LLM output invalid/truncated (finish_reason={finish}); fallback used.",
            )

        return out + "\n"

    except Exception as e:
        # Absolute guarantee: never raise.
        return _deterministic_structured_digest(date_label, items, note=f"LLM summarisation failed; fallback used. Error: {e}")


# Backwards compatible alias
build = build_digest
