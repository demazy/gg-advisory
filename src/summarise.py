# === src/summarise.py ===
from __future__ import annotations

import json
import os
import re
import textwrap
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

# ----------------------------
# Env / config
# ----------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o-mini").strip()
TEMP = float(os.getenv("TEMP", "0.2"))
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "1800"))

MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "3200"))
DEBUG = os.getenv("DEBUG", "0") == "1"

SECTIONS = [
    "Energy Transition",
    "ESG Reporting",
    "Sustainable Finance & Investment",
]


# ----------------------------
# Schema-tolerant accessors
# ----------------------------
def _get_text(obj: Any) -> str:
    """
    Return the best available textual content from an item without assuming a fixed schema.
    Intentionally avoids attribute access named 'text' to prevent schema drift crashes.
    """
    if obj is None:
        return ""

    # Mapping-like
    if isinstance(obj, dict):
        for k in ("summary", "content", "body", "description", "excerpt", "snippet"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    # Attribute-like
    for attr in ("summary", "content", "body", "description", "excerpt", "snippet"):
        v = getattr(obj, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()

    return ""


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _iso_date(ts: Any) -> str:
    if ts is None:
        return ""
    try:
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            return dt.date().isoformat()
        if isinstance(ts, datetime):
            dt = ts.astimezone(timezone.utc)
            return dt.date().isoformat()
    except Exception:
        return ""
    # already a string?
    if isinstance(ts, str):
        return ts[:10]
    return ""


# ----------------------------
# Deterministic fallback summariser
# ----------------------------
def _extractive_summary(raw: str, max_words: int = 150) -> str:
    t = re.sub(r"\s+", " ", (raw or "")).strip()
    if not t:
        return ""
    # Use leading sentences up to word cap
    words = t.split(" ")
    if len(words) <= max_words:
        return t
    return " ".join(words[:max_words]).rstrip() + "…"


def _deterministic_structured_digest(date_label: str, items: Sequence[Any], note: str = "") -> str:
    # Stable ordering: section then published desc then url
    def key(it: Any) -> Tuple[int, str, str]:
        sec = _get(it, "section", "") or ""
        sidx = SECTIONS.index(sec) if sec in SECTIONS else 999
        # use published_iso or published_ts for ordering
        p = _get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))
        # invert by using negative string sort is awkward; use p as string and reverse later
        u = _get(it, "url", "") or ""
        return (sidx, p, u)

    rows = sorted(list(items), key=key, reverse=False)

    by_sec: Dict[str, List[Any]] = {s: [] for s in SECTIONS}
    for it in rows:
        sec = _get(it, "section", "") or ""
        if sec not in by_sec:
            # ignore unknown sections
            continue
        by_sec[sec].append(it)

    out: List[str] = []
    out.append(f"**Signals Digest — {date_label}**")
    if note:
        out.append(f"\n> {note}\n")
    out.append("\n**Top Lines**")
    out.append("- Limited high-signal items were available in this range; see section lists below.")
    out.append("- Selection is constrained by source availability and in-range publication dates.")
    out.append("- Diagnostics (debug outputs) indicate where content was dropped and why.\n")

    out.append("## Energy Transition")
    out.extend(_render_items(by_sec["Energy Transition"]))

    out.append("\n## ESG Reporting")
    out.extend(_render_items(by_sec["ESG Reporting"]))

    out.append("\n## Sustainable Finance & Investment")
    out.extend(_render_items(by_sec["Sustainable Finance & Investment"]))

    # Sources block (URLs only, deterministic)
    urls = []
    for it in items:
        u = (_get(it, "url", "") or "").strip()
        if u and u not in urls:
            urls.append(u)
    if urls:
        out.append("\n---\n**Sources**")
        for u in urls:
            out.append(f"- {u}")

    return "\n".join(out).strip() + "\n"


def _render_items(items: Sequence[Any]) -> List[str]:
    if not items:
        return ["\n_No in-range, high-signal items selected._\n"]
    lines: List[str] = []
    for it in items:
        title = (_get(it, "title", "") or "").strip()[:120]
        url = (_get(it, "url", "") or "").strip()
        pub = (_get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))).strip()
        summ = _extractive_summary(_get_text(it), max_words=150)

        if title:
            lines.append(f"\n- **{title}**")
        else:
            lines.append("\n- **(Untitled)**")
        if pub:
            lines.append(f"  - PUBLISHED: {pub}")
        if url:
            lines.append(f"  - URL: {url}")
        if summ:
            lines.append(f"  - Summary: {summ}")
    return lines


# ----------------------------
# OpenAI call (optional)
# ----------------------------
def _openai_chat_completion(messages: List[Dict[str, str]]) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing")

    payload = {
        "model": MODEL,
        "messages": messages,
        "temperature": TEMP,
        "max_tokens": OPENAI_MAX_TOKENS,
    }

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json=payload,
        timeout=90,
    )
    r.raise_for_status()
    data = r.json()
    return (data.get("choices") or [{}])[0].get("message", {}).get("content", "") or ""


def _prepare_items(items: Sequence[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        text = _get_text(it)
        if len(text) > MAX_TEXT_CHARS_PER_ITEM:
            text = text[:MAX_TEXT_CHARS_PER_ITEM] + "…"
        out.append(
            {
                "title": (_get(it, "title", "") or "").strip(),
                "url": (_get(it, "url", "") or "").strip(),
                "publisher": (_get(it, "publisher", "") or "").strip(),
                "section": (_get(it, "section", "") or "").strip(),
                "published": (_get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))).strip(),
                "text": text,
            }
        )
    return out


def build_digest(date_label: str, items: Sequence[Any]) -> str:
    """
    Build a monthly digest.
    - Uses OpenAI if configured, otherwise deterministic fallback.
    - Never raises due to formatting issues; always returns a markdown string.
    """
    items = list(items or [])
    if not items:
        return _deterministic_structured_digest(date_label, [], note="No items were selected for this range.")

    payload = _prepare_items(items)

    # If no OpenAI key, fallback deterministically
    if not OPENAI_API_KEY:
        return _deterministic_structured_digest(date_label, items, note="OpenAI not configured; deterministic fallback used.")

    system = (
        "You are an executive editor for GG Advisory. Create a concise monthly digest ONLY from the items provided. "
        "Rules: (1) Do NOT invent items or details, (2) Use only facts contained in each item's text field, "
        "(3) Keep tone factual and professional, (4) Include Sources with ONLY the URLs provided per item."
    )

    user = {
        "date_label": date_label,
        "sections": SECTIONS,
        "items": payload,
    }

    user_msg = textwrap.dedent(
        """\
        Using the JSON payload below, write a markdown digest:

        Output structure:
        - **Signals Digest — {date_label}**
        - **Top Lines** — 3 bullets (macro takeaways)
        - Then 3 sections (use exactly these headings):
          ## Energy Transition
          ## ESG Reporting
          ## Sustainable Finance & Investment

        Under each section, include 2–6 bullets (if available) where each bullet has:
        - **Headline** (<= 10 words)
        - PUBLISHED: <ISO date> (if provided)
        - Summary: 90–140 words, purely factual, using numbers/dates/jurisdictions present
        - Why it matters: 1 sentence, factual (no speculation)

        Finish with:
        ---
        **Sources**
        - <url> per item (unique)

        JSON payload:
        {json}
        """
    ).format(date_label=date_label, json=json.dumps(user, ensure_ascii=False))

    try:
        content = _openai_chat_completion(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ]
        )
        out = (content or "").strip()
        # Ensure minimum structure exists; otherwise fallback.
        missing = [h for h in ("## Energy Transition", "## ESG Reporting", "## Sustainable Finance & Investment") if h not in out]
        if missing:
            raise RuntimeError(f"LLM output missing section headings: {', '.join(missing)}")
        if "**Sources**" not in out:
            raise RuntimeError("LLM output missing Sources section")
        return out + ("\n" if not out.endswith("\n") else "")
    except Exception as e:
        return _deterministic_structured_digest(date_label, items, note=f"LLM summarisation failed; deterministic fallback used. Error: {e}")
