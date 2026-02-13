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
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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

# Read/Retry controls (backwards compatible names)
OPENAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "45"))
OPENAI_RETRIES = int(os.getenv("OPENAI_RETRIES", "2"))
OPENAI_BACKOFF = float(os.getenv("OPENAI_BACKOFF", "1.8"))

# Response length control (used by chat-completions)
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "900"))

# Hard cap to prevent runaway prompt size
MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "3500"))  # CHANGE: smaller prompt -> faster/less timeouts


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


def _extractive_summary(raw: str, max_words: int = 140) -> str:
    """
    Deterministic extractive summary (verbatim sentences) to reduce hallucination risk
    when the LLM call fails.

    It prioritises early sentences and those containing numbers/dates.
    """
    if not raw:
        return "Insufficient extract; see source."
    text = re.sub(r"\s+", " ", raw).strip()
    if len(text) < 200:
        return "Insufficient extract; see source."

    # crude sentence split (good enough for fallback)
    sents = re.split(r"(?<=[\.\!\?])\s+", text)
    picked: List[str] = []

    def add(sent: str) -> None:
        s = sent.strip()
        if not s:
            return
        if s in picked:
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


def _deterministic_structured_digest(date_label: str, items: List[Item], note: Optional[str] = None) -> str:
    """
    Deterministic fallback that never raises.

    CHANGE (Feb 2026):
    - Provide extractive (verbatim) summaries from the fetched text to preserve usefulness
      without increasing hallucination risk.
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
    lines += [
        "- (LLM unavailable; using extractive fallback summaries)",
        "- (Summaries below are verbatim sentence extracts; consult sources for full context)",
        "- (If this persists: increase OPENAI_TIMEOUT, reduce MAX_TEXT_CHARS_PER_ITEM, or enable retries)",
    ]
    lines.append("")

    for sec in sections:
        lines.append(f"## {sec}")
        lines.append("")
        for it in by_sec[sec][:4]:
            pub = (it.source or "").strip() or "Unknown"
            published = getattr(it, "published_iso", None) or "Unknown"
            title = (it.title or "").strip() or "Untitled"
            url = (it.url or "").strip()
            raw = (it.summary or "").strip()
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

    # If everything was in "other", include a minimal appendix to avoid losing items
    if other:
        lines.append("## Appendix (Unclassified)")
        lines.append("")
        for it in other[:6]:
            lines.append(f"- {(it.title or 'Untitled').strip()} — {it.url}")

    if note:
        lines += ["", "---", "", f"> Note: {note}"]

    return "\n".join(lines).strip() + "\n"


def _openai_chat_completion_raw(
    model: str, messages: List[Dict[str, str]], temperature: float = 0.2
) -> Tuple[str, str]:
    """
    Call OpenAI Chat Completions via HTTPS and return (content, finish_reason).

    Robustness:
    - Retries with exponential backoff for transient timeouts.
    - Separate connect/read timeout and increased default.
    - max_tokens is configurable via OPENAI_MAX_TOKENS.
    """
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing")

    url = OPENAI_CHAT_URL
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
            r = requests.post(url, headers=headers, json=payload, timeout=(10, OPENAI_TIMEOUT))
            r.raise_for_status()
            data = r.json()
            choice = data["choices"][0]
            content = (choice.get("message") or {}).get("content") or ""
            finish_reason = choice.get("finish_reason") or "unknown"
            return content.strip(), finish_reason
        except Exception as e:
            last_err = e
            # simple backoff
            sleep_s = min(2 ** attempt, 10)
            try:
                time.sleep(sleep_s)
            except Exception:
                pass
    raise RuntimeError(f"OpenAI request failed after retries: {last_err}")


def _openai_chat_completion(model: str, messages: List[Dict[str, str]], temperature: float = 0.2) -> str:
    """Backward-compatible wrapper returning only content."""
    content, _ = _openai_chat_completion_raw(model=model, messages=messages, temperature=temperature)
    return content
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
        content, finish_reason = _openai_chat_completion_raw(
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            model=MODEL,
            temperature=TEMP,
        )
        # Guardrails: reject truncated or structurally inconsistent output
out = (content or "").strip()
if finish_reason == "length":
    return _deterministic_structured_digest(date_label, items, note="LLM output truncated; fallback used.")
# Require key headings
required = ["# Signals Digest", "## Top Lines", "## Energy Transition", "## ESG Reporting", "## Sustainable Finance"]
if not all(r in out for r in required):
    return _deterministic_structured_digest(date_label, items, note="LLM output missing required headings; fallback used.")
# Ensure one-to-one mapping between input URLs and cited Sources to prevent omissions/duplication
input_urls = [it.get("url") for it in items if it.get("url")]
cited = re.findall(r"https?://[^\s)]+", out)
cited_urls = [u.rstrip(".,") for u in cited]
# check each input appears at least once
if any(u not in cited_urls for u in input_urls):
    return _deterministic_structured_digest(date_label, items, note="LLM output missing some sources; fallback used.")
# prevent repeated same source blocks (common failure mode)
from collections import Counter
dup = [u for u,c in Counter(cited_urls).items() if c > 3]  # allow some repeats in top lines
if dup:
    return _deterministic_structured_digest(date_label, items, note="LLM output repeated sources excessively; fallback used.")
return out + "\n"
    except Exception as e:
        return _deterministic_structured_digest(date_label, items, note=f"LLM summarisation failed; fallback used. Error: {e}")


# Backwards compatible alias (some older code may call summarise.build())
build = build_digest
