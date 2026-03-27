# -*- coding: utf-8 -*-
"""
ARK Capture Solutions — monthly intelligence brief summariser.

Produces a 4-section markdown brief tailored for ARK's BD team:
  1. Grants & Funding
  2. Market & Policy
  3. Competitors
  4. Partners & Buyers

Audience: ARK leadership (CEO/CTO/BD) evaluating AU/APAC market entry
for their modular point-source carbon capture technology.
"""
from __future__ import annotations

import calendar
import json
import os
import re
import textwrap
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o").strip()
# Temperature 0 for ARK: maximally grounded, no creative variation.
# The env var can override for debugging but the default is strictly 0.
TEMP = float(os.getenv("ARK_TEMP", os.getenv("TEMP", "0")))
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "4000"))
MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "8000"))
DEBUG = os.getenv("DEBUG", "0") == "1"

ARK_SECTIONS = [
    "Grants & Funding",
    "Market & Policy",
    "Competitors",
    "Partners & Buyers",
]


# ── Shared helpers (copied from summarise.py to stay self-contained) ──────────

def _get_text(obj: Any) -> str:
    if obj is None:
        return ""
    if isinstance(obj, dict):
        for k in ("summary", "content", "body", "description", "excerpt", "snippet"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""
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
    try:
        parts = date_label.strip().split("-")
        y, m = int(parts[0]), int(parts[1])
        return f"{calendar.month_name[m]} {y}"
    except Exception:
        return date_label


def _format_pub_date(iso: str) -> str:
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
            return ts.astimezone(timezone.utc).date().isoformat()
    except Exception:
        return ""
    if isinstance(ts, str):
        return ts[:10]
    return ""


def _extractive_summary(raw: str, max_words: int = 150) -> str:
    t = re.sub(r"\s+", " ", (raw or "")).strip()
    if not t:
        return ""
    words = t.split(" ")
    if len(words) <= max_words:
        return t
    return " ".join(words[:max_words]).rstrip() + "…"


# ── Deterministic fallback ────────────────────────────────────────────────────

def _render_items_fallback(items: Sequence[Any]) -> List[str]:
    if not items:
        return ["\n_No in-range items selected for this section._\n"]
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
        if url:
            lines.append(f"Source: {url}")
    return lines


def _deterministic_digest(date_label: str, items: Sequence[Any], note: str = "") -> str:
    month_year = _format_month_year(date_label)

    def sort_key(it: Any) -> Tuple[int, str, str]:
        sec = _get(it, "section", "") or ""
        sidx = ARK_SECTIONS.index(sec) if sec in ARK_SECTIONS else 999
        p = _get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))
        return (sidx, p or "", _get(it, "url", "") or "")

    rows = sorted(list(items), key=sort_key)
    by_sec: Dict[str, List[Any]] = {s: [] for s in ARK_SECTIONS}
    for it in rows:
        sec = _get(it, "section", "") or ""
        if sec in by_sec:
            by_sec[sec].append(it)

    out: List[str] = [f"# ARK Intelligence Brief — {month_year}"]
    if note:
        out.append(f"\n> {note}\n")
    out.append("\n**Executive Summary**")
    out.append("- Intelligence brief for ARK Capture Solutions covering AU/APAC carbon capture opportunities.")
    out.append("- Sections: Grants & Funding · Market & Policy · Competitors · Partners & Buyers.")
    out.append("- Limited high-signal items available; see section details below.")

    for section_name in ARK_SECTIONS:
        out.append(f"\n---\n## {section_name}")
        out.extend(_render_items_fallback(by_sec[section_name]))

    return "\n".join(out).strip() + "\n"


# ── OpenAI path ───────────────────────────────────────────────────────────────

def _openai_chat(messages: List[Dict[str, str]]) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing")
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json={
            "model": MODEL,
            "messages": messages,
            "temperature": TEMP,
            "max_tokens": OPENAI_MAX_TOKENS,
        },
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
        out.append({
            "title": (_get(it, "title", "") or "").strip(),
            "url": (_get(it, "url", "") or "").strip(),
            "publisher": (_get(it, "source", "") or _get(it, "publisher", "") or "").strip(),
            "section": (_get(it, "section", "") or "").strip(),
            "published": (_get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))).strip(),
            "text": text,
        })
    return out


_SYSTEM = """\
You are a senior business intelligence analyst preparing a monthly briefing for ARK Capture Solutions, \
a Belgian company with proprietary modular point-source carbon capture technology designed for \
low-concentration industrial flue gases (biogas plants, gas-fired power, petrochemicals, glass, steel).

ARK's leadership team — CEO, CTO, and Business Development — is evaluating market entry into Australia \
and the Asia-Pacific region.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ABSOLUTE ANTI-HALLUCINATION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A. Every single fact, figure, date, company name, dollar amount, percentage, and regulatory \
   threshold you write MUST appear verbatim (or as a direct paraphrase) in the provided item text. \
   No exceptions.
B. If the item text does not contain a specific figure, DO NOT include that figure. Write \
   "amount not disclosed" or omit the claim entirely.
C. If you are uncertain whether a fact is in the source text, DO NOT include it.
D. Do NOT combine facts from different items into a single statement.
E. Do NOT extrapolate, infer, or project. Only report what the source text explicitly states.
F. If an item's text is too thin (under 80 words with no specific data), SKIP the item entirely. \
   Write nothing about it rather than producing a vague or speculative summary.
G. The Source URL for each entry MUST match exactly the URL provided in the JSON payload. \
   Do NOT alter, abbreviate, or construct URLs.
H. Treat a violation of any rule above as a critical error. When in doubt, omit.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Quality standards (within the above constraints):
1. Be specific where the source allows: name companies, dollar amounts, MW/tCO2 figures, \
   grant sizes, deadlines, regulatory thresholds.
2. For grants: state amount, eligibility, deadline — only if explicitly in the source text.
3. For policy: state what changes, when, and who is affected — only if explicitly stated.
4. For competitors: name the company/project and technology approach — only if explicitly stated.
5. For partners/buyers: name the company and their decarbonisation context — only if stated.
6. "Why it matters for ARK" must name a concrete BD action or deadline. Never write \
   "this is significant" or vague phrases. Base it only on what the source says.
"""

_USER_TEMPLATE = textwrap.dedent("""\
    Using the JSON payload below, write a monthly intelligence brief in markdown for ARK Capture Solutions.

    Required structure — reproduce these headings exactly:

    # ARK Intelligence Brief — {month_year}

    **Executive Summary**
    - [Key takeaway 1 — cite a specific grant amount, policy change, or company name]
    - [Key takeaway 2 — cite a specific grant amount, policy change, or company name]
    - [Key takeaway 3 — cite a specific grant amount, policy change, or company name]

    ---
    ## Grants & Funding

    [items]

    ---
    ## Market & Policy

    [items]

    ---
    ## Competitors

    [items]

    ---
    ## Partners & Buyers

    [items]

    Each [items] block contains 2–5 entries. Every entry must follow this exact format \
    with a blank line between entries:

    **[Concise headline, max 12 words]**
    Published: [D Month YYYY — omit if date unavailable]
    Summary: [80–130 words. What happened, specific figures, entities, geographic scope, technology context.]
    Why it matters for ARK: [1–2 sentences. Name the concrete BD action, deadline, or risk for ARK's \
    AU/APAC entry. Be specific about ARK's technology fit, grant eligibility, or competitive threat.]
    Signals to watch: [1 sentence. Next concrete trigger — application deadline, policy decision, \
    company announcement, or tender to track.]
    Source: [the URL for this item]

    Rules:
    - Only include an entry if the item text supports a meaningful, specific summary.
    - If a section has fewer than 2 good items, write what is available — do not pad.
    - Do NOT add a sources list at the bottom.
    - Use ONLY facts from the provided item text.

    JSON payload:
    {json}
""")


# ── URL hallucination guard ───────────────────────────────────────────────────

def _validate_source_urls(md_output: str, allowed_urls: set[str]) -> list[str]:
    """
    Find every 'Source: <url>' line in the output and verify the URL was in
    the input payload. Returns a list of violation strings (empty = all clean).
    """
    violations: list[str] = []
    for line in md_output.splitlines():
        s = line.strip()
        if s.lower().startswith("source:"):
            url = s[len("Source:"):].strip()
            if not url:
                continue
            # Normalise for comparison: strip trailing slash + lowercase
            norm = url.rstrip("/").lower()
            norm_set = {u.rstrip("/").lower() for u in allowed_urls}
            if norm not in norm_set:
                violations.append(f"Unknown URL not in payload: {url}")
    return violations


# ── Public entry point ────────────────────────────────────────────────────────

def build_ark_digest(date_label: str, items: Sequence[Any]) -> str:
    """
    Build the ARK monthly intelligence brief.
    Matches the same signature as summarise.build_digest so it can be
    monkey-patched into generate_monthly.
    """
    items = list(items or [])
    if not items:
        return _deterministic_digest(date_label, [], note="No items were selected for this range.")

    if not OPENAI_API_KEY:
        return _deterministic_digest(
            date_label, items,
            note="OpenAI not configured; deterministic fallback used."
        )

    payload = _prepare_items(items)
    allowed_urls = {p["url"] for p in payload if p.get("url")}
    month_year = _format_month_year(date_label)
    user_msg = _USER_TEMPLATE.format(
        month_year=month_year,
        json=json.dumps({"date_label": date_label, "sections": ARK_SECTIONS, "items": payload},
                        ensure_ascii=False),
    )

    try:
        content = _openai_chat([
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": user_msg},
        ])
        out = (content or "").strip()

        # ── Structural check ───────────────────────────────────────────────
        missing = [h for h in (
            "## Grants & Funding", "## Market & Policy",
            "## Competitors", "## Partners & Buyers",
        ) if h not in out]
        if missing:
            raise RuntimeError(f"LLM output missing headings: {', '.join(missing)}")

        # ── URL hallucination check ────────────────────────────────────────
        violations = _validate_source_urls(out, allowed_urls)
        if violations:
            # Log every violation — these are hard errors for a client brief
            for v in violations:
                print(f"[ark_summarise] HALLUCINATION DETECTED: {v}")
            raise RuntimeError(
                f"LLM invented {len(violations)} Source URL(s) not in the input payload. "
                "Falling back to deterministic (extractive-only) output to prevent hallucinations."
            )

        return out + ("\n" if not out.endswith("\n") else "")

    except Exception as e:
        print(f"[ark_summarise] LLM path failed, using deterministic fallback: {e}")
        return _deterministic_digest(
            date_label, items,
            note=f"LLM path failed; deterministic extractive-only output used. Error: {e}"
        )
