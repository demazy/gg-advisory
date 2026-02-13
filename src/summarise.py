# === BEGIN src/summarise.py ===
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

# ----------------------------
# Config / env
# ----------------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o-mini").strip()
TEMP = float(os.getenv("TEMP", "0.2"))

# Robust defaults (GH Actions often times out on cold starts)
OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "90"))          # read timeout seconds
OPENAI_RETRIES = int(os.getenv("OPENAI_RETRIES", "3"))
OPENAI_BACKOFF = float(os.getenv("OPENAI_BACKOFF", "2.0"))       # exponential backoff base
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "2800"))  # default token budget

# Limit per-item text sent to LLM to reduce timeouts and cost
MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "3500"))

# ----------------------------
# Types
# ----------------------------

@dataclass
class Item:
    title: str
    url: str
    section: str
    published_iso: Optional[str] = None
    text: str = ""


# ----------------------------
# Prompt
# ----------------------------

SYSTEM = (
    "You are an executive editor for GG Advisory. Create a concise digest ONLY from the items provided. "
    "STRICT RULES: (1) Do NOT invent items or details, (2) Use only facts contained in the items' text snippets, "
    "(3) If zero valid items are provided, respond with exactly: NO_ITEMS_IN_RANGE, "
    "(4) Include Sources with ONLY the URLs provided per item."
)

USER_TMPL = """If there are zero items, output exactly: NO_ITEMS_IN_RANGE

Otherwise, create **Signals Digest — {date_label}** across three sections:
- **Energy Transition**
- **ESG Reporting**
- **Sustainable Finance & Investment**

Start with **Top Lines** — 3 bullets (macro takeaways).

Then **Top Items** (6–12 items total across all sections):
- **Headline** (≤10 words)
- **SECTION:** one of the three above
- **PUBLISHED:** `<ISO date>` if provided
- **Summary:** 120–160 words, factual, with numbers/dates/jurisdictions present in the item text
- *Why it matters:* 1 sentence

End with **Sources**: bullet list of URLs (ONLY those provided).

Items (JSON):
{items_json}
"""


# ----------------------------
# Helpers
# ----------------------------

def _month_label(ym: str) -> str:
    # ym = "YYYY-MM"
    try:
        dt = datetime.strptime(ym + "-01", "%Y-%m-%d")
        return dt.strftime("%B %Y")
    except Exception:
        return ym

def _effective_max_tokens(n_items: int) -> int:
    # scale mildly with item count; stay within a sensible envelope
    base = OPENAI_MAX_TOKENS
    bump = max(0, n_items - 6) * 120
    return max(1200, min(3800, base + bump))

def _extractive_summary(raw: str, max_words: int = 140) -> str:
    text = re.sub(r"\s+", " ", raw or "").strip()
    if not text:
        return "No extractable text."
    # take first ~max_words words (purely extractive)
    words = text.split(" ")
    return " ".join(words[:max_words]).strip()

def _deterministic_structured_digest(date_label: str, items: List[Item], note: str) -> str:
    # Deterministic, low-hallucination fallback
    lines: List[str] = []
    lines.append(f"# Signals Digest — {date_label}")
    lines.append("")
    lines.append(f"> {note}")
    lines.append("")
    lines.append("## Top Lines")
    lines.append("- No LLM summary available; using extractive fallback.")
    lines.append("- Coverage reflects successfully fetched and filtered items.")
    lines.append("- See Sources for original URLs.")
    lines.append("")

    sections = ["Energy Transition", "ESG Reporting", "Sustainable Finance & Investment"]
    for sec in sections:
        lines.append(f"## {sec}")
        sec_items = [it for it in items if it.section == sec]
        if not sec_items:
            lines.append("_No items selected._")
            lines.append("")
            continue
        for it in sec_items:
            head = (it.title or "Untitled").strip()
            pub = it.published_iso or "N/A"
            summ = _extractive_summary(it.text, max_words=140)
            lines.append(f"**{head}**")
            lines.append(f"- SECTION: {sec}")
            lines.append(f"- PUBLISHED: {pub}")
            lines.append(f"- Summary: {summ}")
            lines.append(f"- *Why it matters:* See source for details.")
            lines.append("")
    lines.append("## Sources")
    for it in items:
        if it.url:
            lines.append(f"- {it.url}")
    lines.append("")
    return "\n".join(lines)

def _prepare_items(items: List[Item]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        out.append(
            {
                "title": it.title,
                "url": it.url,
                "section": it.section,
                "published": it.published_iso,
                "text": (it.text or "")[:MAX_TEXT_CHARS_PER_ITEM],
            }
        )
    return out

def _openai_chat_completion(
    *,
    messages: List[Dict[str, str]],
    model: str,
    temperature: float,
    max_tokens: int,
) -> Tuple[str, Optional[str]]:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    last_err: Optional[Exception] = None
    for attempt in range(max(1, OPENAI_RETRIES)):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=(10, OPENAI_TIMEOUT))
            r.raise_for_status()
            data = r.json()
            choice = (data.get("choices") or [{}])[0]
            content = (((choice.get("message") or {}).get("content")) or "")
            finish = choice.get("finish_reason")
            return (content or "").strip(), finish
        except Exception as e:
            last_err = e
            if attempt < max(1, OPENAI_RETRIES) - 1:
                # 1, 2, 4... (or base exponent if OPENAI_BACKOFF != 2.0)
                time.sleep(max(0.5, OPENAI_BACKOFF) ** attempt)
                continue
            break

    raise RuntimeError(f"OpenAI call failed after {OPENAI_RETRIES} attempt(s): {last_err}")


def build_digest(ym: str, items: List[Item]) -> str:
    """
    Entry point used by generate_monthly.py.
    """
    date_label = _month_label(ym)

    # Filter out placeholders / empty URLs just in case
    items = [it for it in (items or []) if getattr(it, "url", "")]

    if not items:
        return "NO_ITEMS_IN_RANGE\n"

    if not OPENAI_API_KEY:
        return _deterministic_structured_digest(date_label, items, note="OPENAI_API_KEY not set; deterministic fallback used.")

    payload = _prepare_items(items)
    items_json = json.dumps(payload, ensure_ascii=False, indent=2)
    user_msg = USER_TMPL.format(date_label=date_label, items_json=items_json)

    required_markers = [
        "Signals Digest",
        "Top Lines",
        "Energy Transition",
        "ESG Reporting",
        "Sustainable Finance",
        "Sources",
    ]
    input_urls = [it.url for it in items if it.url]

    try:
        content, finish = _openai_chat_completion(
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            model=MODEL,
            temperature=TEMP,
            max_tokens=_effective_max_tokens(len(items)),
        )

        out = (content or "").strip() + "\n"

        # Hard validation: structure + basic URL coverage. If it fails, fallback.
        if finish == "length":
            raise RuntimeError("OpenAI output truncated (finish_reason=length)")

        for m in required_markers:
            if m not in out:
                raise RuntimeError(f"Missing required marker: {m}")

        # Ensure at least half of URLs appear (LLM sometimes drops sources)
        present = sum(1 for u in input_urls if u in out)
        if present < max(1, len(input_urls) // 2):
            raise RuntimeError(f"Insufficient source coverage in output ({present}/{len(input_urls)} URLs present)")

        # Prevent pathological repetition of a single URL
        if input_urls:
            top = max(out.count(u) for u in input_urls)
            if top > 6:
                raise RuntimeError("Suspicious repeated sources; using deterministic fallback")

        return out

    except Exception as e:
        return _deterministic_structured_digest(date_label, items, note=f"LLM summarisation failed; fallback used. Error: {e}")


# Backwards compatible alias
build = build_digest
# === END src/summarise.py ===
