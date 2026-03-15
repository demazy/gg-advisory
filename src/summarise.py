# === src/summarise.py ===
from __future__ import annotations

import calendar
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
MODEL = os.getenv("MODEL", "gpt-4o").strip()
TEMP = float(os.getenv("TEMP", "0.2"))
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "4000"))

MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "8000"))
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


def _format_month_year(date_label: str) -> str:
    """
    Convert a YYYY-MM date label to a human-readable "Month YYYY" string.
    Falls back to the raw label if parsing fails.
    """
    try:
        parts = date_label.strip().split("-")
        y, m = int(parts[0]), int(parts[1])
        return f"{calendar.month_name[m]} {y}"
    except Exception:
        return date_label


def _format_pub_date(iso: str) -> str:
    """
    Convert an ISO date string (YYYY-MM-DD or timestamp) to "D Month YYYY".
    E.g. "2026-02-26" → "26 February 2026"
    """
    try:
        d = datetime.fromisoformat(iso[:10])
        return f"{d.day} {calendar.month_name[d.month]} {d.year}"
    except Exception:
        return iso


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
    # Stable ordering: section then published asc then url
    def key(it: Any) -> Tuple[int, str, str]:
        sec = _get(it, "section", "") or ""
        sidx = SECTIONS.index(sec) if sec in SECTIONS else 999
        p = _get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))
        u = _get(it, "url", "") or ""
        return (sidx, p, u)

    rows = sorted(list(items), key=key, reverse=False)

    by_sec: Dict[str, List[Any]] = {s: [] for s in SECTIONS}
    for it in rows:
        sec = _get(it, "section", "") or ""
        if sec not in by_sec:
            continue
        by_sec[sec].append(it)

    month_year = _format_month_year(date_label)
    out: List[str] = []
    out.append(f"# {month_year}")
    if note:
        out.append(f"\n> {note}\n")
    out.append("\n**Top Lines**")
    out.append("- Limited high-signal items were available in this range; see section lists below.")
    out.append("- Selection is constrained by source availability and in-range publication dates.")
    out.append("- Diagnostics (debug outputs) indicate where content was dropped and why.")

    out.append("\n---\n## Energy Transition")
    out.extend(_render_items(by_sec["Energy Transition"]))

    out.append("\n---\n## ESG Reporting")
    out.extend(_render_items(by_sec["ESG Reporting"]))

    out.append("\n---\n## Sustainable Finance & Investment")
    out.extend(_render_items(by_sec["Sustainable Finance & Investment"]))

    return "\n".join(out).strip() + "\n"


def _render_items(items: Sequence[Any]) -> List[str]:
    if not items:
        return ["\n_No in-range, high-signal items selected._\n"]
    lines: List[str] = []
    for it in items:
        title = (_get(it, "title", "") or "").strip()[:120]
        url = (_get(it, "url", "") or "").strip()
        raw_pub = (_get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))).strip()
        pub = _format_pub_date(raw_pub) if raw_pub else ""
        summ = _extractive_summary(_get_text(it), max_words=150)

        lines.append("")
        lines.append(f"**{title or '(Untitled)'}**")
        if pub:
            lines.append(f"Published: {pub}")
        if summ:
            lines.append(f"Summary: {summ}")
        # Why it matters and Signals to watch cannot be generated deterministically —
        # they will be populated by the LLM path.
        if url:
            lines.append(f"Source: {url}")
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
        "You are a senior strategic analyst and executive editor for GG Advisory, an Australian advisory firm "
        "that helps businesses, investors, and institutions navigate the energy transition, mandatory "
        "ESG/sustainability reporting, and sustainable finance markets.\n\n"
        "Your audience is Australian C-suite executives (CFOs, CEOs, Chief Sustainability Officers), board "
        "directors, institutional investors, and sustainability professionals who need precise, high-signal "
        "intelligence to make time-sensitive strategic decisions.\n\n"
        "Quality standards for every digest:\n"
        "1. Extract and cite specific data: dollar figures, MW/GW capacity, percentages, timelines, regulatory "
        "thresholds, entity names, and jurisdictions. Generic statements without specifics are unacceptable.\n"
        "2. Distinguish mandatory from voluntary developments and flag Australian applicability explicitly.\n"
        "3. For regulatory and policy items, state what changes, when it takes effect, and who is affected.\n"
        "4. The 'Why it matters' line must name a specific decision or action an Australian executive, "
        "investor, or board should take — or a concrete risk or deadline they must track. "
        "NEVER use phrases like 'this is significant', 'this highlights', 'this is important', "
        "'this will enhance', or 'stakeholders should be aware'. Instead, write: "
        "'CFOs at entities with >$50M revenue must lodge by [date]', or "
        "'Boards should review [specific exposure] given this rule change', or "
        "'Investors in [sector] face [specific risk] by [date]'.\n"
        "5. Prioritise: (a) regulatory/policy changes with binding effect, "
        "(b) significant financial transactions or market moves with quantified impact, "
        "(c) major reports or standards with specific quantified findings.\n\n"
        "Absolute rules:\n"
        "- Use ONLY facts from the provided item text fields. Do NOT invent figures, dates, or details.\n"
        "- Do NOT include items not in the payload.\n"
        "- Do NOT speculate beyond what the source text explicitly supports.\n"
        "- If an item's text field is too thin (under 100 words with no specific data), skip it entirely "
        "rather than producing a vague summary."
    )

    user = {
        "date_label": date_label,
        "sections": SECTIONS,
        "items": payload,
    }

    month_year = _format_month_year(date_label)

    user_msg = textwrap.dedent(
        """\
        Using the JSON payload below, write a monthly digest in markdown.

        Required structure — reproduce these headings exactly:

        # {month_year}

        **Top Lines**
        - [Macro takeaway 1 — cite a specific figure, policy name, or regulatory deadline]
        - [Macro takeaway 2 — cite a specific figure, policy name, or regulatory deadline]
        - [Macro takeaway 3 — cite a specific figure, policy name, or regulatory deadline]

        ---
        ## Energy Transition

        [items]

        ---
        ## ESG Reporting

        [items]

        ---
        ## Sustainable Finance & Investment

        [items]

        Each [items] block contains 2–5 entries. Every entry must follow this exact 6-field format
        with a blank line between entries:

        **[Concise headline, max 12 words]**
        Published: [D Month YYYY — e.g. "26 February 2026"; omit this line if date unavailable]
        Summary: [100–150 words. Include: what happened, specific figures/amounts/dates/entities/jurisdictions, regulatory status, geographic scope. No vague language.]
        Why it matters: [1–2 sentences. Name the concrete decision, deadline, or risk for an Australian executive, board, or investor. Never write "this is significant" or "stakeholders should be aware".]
        Signals to watch: [1 sentence. Name the next concrete trigger — an upcoming consultation close, effective date, rule decision, or policy announcement to track.]
        Source: [the URL for this item]

        Rules:
        - Only include an entry if the item text has enough substance for a meaningful summary.
        - If a section has fewer than 2 good items, write what is available — do not pad or invent.
        - Do NOT add a sources list at the bottom.
        - Use ONLY facts from the provided item text. Do not invent figures, dates, or details.

        JSON payload:
        {json}
        """
    ).format(month_year=month_year, json=json.dumps(user, ensure_ascii=False))

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
        return out + ("\n" if not out.endswith("\n") else "")
    except Exception as e:
        return _deterministic_structured_digest(date_label, items, note=f"LLM summarisation failed; deterministic fallback used. Error: {e}")
