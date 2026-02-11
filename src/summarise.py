"""src/summarise.py

LLM-based summarisation for the monthly digest with guardrails.

Goals:
- Robustness: retries + deterministic fallback summary if the API fails.
- Fidelity: instruct model to use only the provided excerpts; post-validate output.
- Completeness: ensure every selected URL appears in the digest at least once.

This module intentionally avoids raising exceptions in normal operation.
"""

from __future__ import annotations

import os
import re
import time
from typing import Dict, List, Optional

import requests

from .fetch import Item


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "1400"))

REQUEST_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT_SECS", "60"))
MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "5"))


_URL_RE = re.compile(r"https?://[^\s\)\]\}<>\"']+")
_TRAILING_PUNCT = ".,;:)]}>"


def _shorten(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"


def _format_items_payload(items: List[Item]) -> str:
    lines = []
    for i, it in enumerate(items, 1):
        excerpt = _shorten(it.summary or "", 2200)
        published = getattr(it, "published_iso", "") or ""
        src = getattr(it, "source", "") or ""
        date_src = getattr(it, "published_source", "") or ""
        date_conf = getattr(it, "published_confidence", "") or ""
        title = (it.title or "").strip() or "(untitled)"
        lines.append(
            f"ITEM {i}\n"
            f"Section: {src}\n"
            f"Title: {title}\n"
            f"URL: {it.url}\n"
            f"Published: {published} (confidence={date_conf}, source={date_src})\n"
            f"Excerpt:\n{excerpt}\n"
        )
    return "\n".join(lines)


def _call_openai(messages: List[dict]) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None

    url = f"{OPENAI_API_BASE}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "temperature": OPENAI_TEMPERATURE,
        "max_tokens": OPENAI_MAX_TOKENS,
        "messages": messages,
    }

    backoff = 1.5
    for _ in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(20, backoff * 1.8)
                continue
            if r.status_code >= 400:
                return None
            data = r.json()
            return (data["choices"][0]["message"]["content"] or "").strip()
        except Exception:
            time.sleep(backoff)
            backoff = min(20, backoff * 1.8)
            continue

    return None


def _extract_urls(md: str) -> List[str]:
    urls = []
    for m in _URL_RE.finditer(md or ""):
        u = m.group(0).rstrip(_TRAILING_PUNCT)
        urls.append(u)
    return urls


def _deterministic_digest(month_ym: str, items: List[Item]) -> str:
    """Fallback summary if LLM fails or returns unusable output."""
    hdr = f"# Signals Digest — {month_ym}\n\n"
    if not items:
        return hdr + "_No items were selected for this month._\n"

    # Group by section
    by_sec: Dict[str, List[Item]] = {}
    for it in items:
        by_sec.setdefault(it.source or "Other", []).append(it)

    out = [hdr, "## Highlights\n", "- Automated fallback summary (LLM unavailable).\n", "\n"]
    for sec in sorted(by_sec.keys()):
        out.append(f"## {sec}\n")
        for it in by_sec[sec]:
            title = (it.title or "").strip() or "(untitled)"
            excerpt = _shorten(it.summary or "", 420)
            pub = getattr(it, "published_iso", "") or "undated"
            out.append(f"### {title}\n")
            out.append(f"- Date: {pub}\n")
            out.append(f"- Source: {it.url}\n")
            out.append(f"- Notes: {excerpt}\n\n")
    return "".join(out)


def _ensure_completeness(md: str, items: List[Item]) -> str:
    """Ensure every selected item URL appears at least once; append missing ones deterministically."""
    want = [it.url for it in items]
    have = set(_extract_urls(md))
    missing = [u for u in want if u not in have]
    if not missing:
        return md

    extra = ["\n\n## Additional items (auto-added)\n"]
    by_url = {it.url: it for it in items}
    for u in missing:
        it = by_url[u]
        title = (it.title or "").strip() or "(untitled)"
        excerpt = _shorten(it.summary or "", 380)
        pub = getattr(it, "published_iso", "") or "undated"
        extra.append(f"- **{title}** ({pub}) — {u}\n  - {excerpt}\n")
    return md.rstrip() + "".join(extra)


def _strip_unknown_urls(md: str, items: List[Item]) -> str:
    """Remove accidental external URLs not in the selected set."""
    allowed = set(it.url for it in items)
    found = _extract_urls(md)
    bad = [u for u in found if u not in allowed]
    if not bad:
        return md
    for u in bad:
        md = md.replace(u, "")
    return md


def build_digest(month_ym: str, items: List[Item]) -> str:
    """
    Build a monthly digest in Markdown.
    Always returns a string (never raises).
    """
    items = items or []

    # Sort items by section then date desc if possible
    def _sort_key(it: Item):
        ts = getattr(it, "published_ts", None)
        return (it.source or "", ts or 0.0)

    items = sorted(items, key=_sort_key, reverse=True)

    if not items:
        return _deterministic_digest(month_ym, items)

    system = (
        "You are a careful analyst writing a monthly signals digest for energy transition and ESG readers.\n"
        "Hard rules:\n"
        "1) Use ONLY the provided excerpts. Do NOT use outside knowledge.\n"
        "2) Include EVERY item provided. Do NOT omit any.\n"
        "3) Do NOT add any URLs other than the item URLs.\n"
        "4) If the excerpt lacks details, say so explicitly instead of guessing.\n"
    )

    user = (
        f"Create a Markdown digest for {month_ym}.\n\n"
        "Structure:\n"
        f"# Signals Digest — {month_ym}\n"
        "## Highlights (5 bullets max)\n"
        "Then, for each section, include one subsection per item:\n"
        "## <Section>\n"
        "### <Title>\n"
        "- Date: <YYYY-MM-DD or 'undated'>\n"
        "- Source: <URL>\n"
        "- Summary: 2–3 sentences strictly grounded in the excerpt.\n"
        "- Why it matters: 1 sentence grounded in the excerpt.\n"
        "- Watch: 1 brief forward-looking question (no speculation beyond excerpt).\n\n"
        "Important:\n"
        "- Every item must appear exactly once under its section.\n"
        "- Keep it concise.\n\n"
        "Items:\n\n"
        + _format_items_payload(items)
    )

    md = _call_openai(
        [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
    )

    if not md:
        return _deterministic_digest(month_ym, items)

    md = _strip_unknown_urls(md, items)
    md = _ensure_completeness(md, items)

    if not md.lstrip().startswith("#"):
        md = f"# Signals Digest — {month_ym}\n\n" + md

    return md.strip() + "\n"
