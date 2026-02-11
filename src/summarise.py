# -*- coding: utf-8 -*-
"""
Summarisation / digest writing.

Robustness:
- Supports both calling styles:
    build_digest("YYYY-MM", items)
    build_digest(ym="YYYY-MM", items=items, model=..., temperature=...)
- If OpenAI call fails, falls back to deterministic digest.
"""
from __future__ import annotations

import json
import os
import time
from typing import List

import requests

from .fetch import Item

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_CHAT_URL = os.getenv("OPENAI_CHAT_URL", "https://api.openai.com/v1/chat/completions")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))

REQ_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "45"))
REQ_RETRIES = int(os.getenv("OPENAI_RETRIES", "2"))
BACKOFF = float(os.getenv("OPENAI_BACKOFF", "1.8"))


def _deterministic_digest(month_ym: str, items: List[Item]) -> str:
    lines = [f"# Monthly Digest — {month_ym}", ""]
    if not items:
        lines += ["No items were selected.", ""]
        return "\n".join(lines)

    # group by section if available
    by_section = {}
    for it in items:
        sec = getattr(it, "section", "") or "General"
        by_section.setdefault(sec, []).append(it)

    for sec, its in by_section.items():
        lines.append(f"## {sec}")
        lines.append("")
        for it in its:
            src = getattr(it, "source", "") or ""
            lines.append(f"- [{it.title}]({it.url})" + (f" — *{src}*" if src else ""))
        lines.append("")

    return "\n".join(lines)


def _format_items_payload(items: List[Item]) -> str:
    parts = []
    for i, it in enumerate(items, 1):
        section = getattr(it, "section", "") or ""
        publisher = getattr(it, "source", "") or ""
        txt = (it.summary or "").strip()
        # keep prompt bounded
        if len(txt) > 4000:
            txt = txt[:4000] + "…"

        parts.append(
            "\n".join(
                [
                    f"ITEM {i}",
                    f"Title: {it.title}",
                    f"URL: {it.url}",
                    f"Section: {section if section else 'General'}",
                    f"Source: {publisher}",
                    f"Text: {txt}",
                ]
            )
        )
    return "\n\n".join(parts)


def build_digest(month_ym: str | None = None, items: list | None = None, **kwargs) -> str:
    # Backwards/forwards compatible argument mapping
    if month_ym is None:
        month_ym = (kwargs.get("ym") or kwargs.get("month_ym") or "").strip() or None
    if items is None:
        items = kwargs.get("items") or []
    if month_ym is None:
        raise TypeError("build_digest requires month_ym (or ym) to be provided")
    items = list(items) if items is not None else []

    # allow callers to override without breaking
    model = (kwargs.get("model") or OPENAI_MODEL)
    temperature = float(kwargs.get("temperature") or OPENAI_TEMPERATURE)

    if not items:
        return _deterministic_digest(month_ym, [])

    if not OPENAI_API_KEY:
        return _deterministic_digest(month_ym, items)

    system = (
        "You are producing a monthly digest in Markdown.\n"
        "Rules:\n"
        "- Use H2 headings per section.\n"
        "- For each item, provide: a 1–2 sentence summary, then 2–4 bullet points with key takeaways.\n"
        "- Keep it factual; no hallucinated numbers.\n"
        "- Mention the source name.\n"
    )
    user = (
        f"Month: {month_ym}\n\n"
        "Items below. Write the digest.\n\n"
        f"{_format_items_payload(items)}"
    )

    payload = {
        "model": model,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    last_err = None
    for attempt in range(REQ_RETRIES + 1):
        try:
            r = requests.post(
                OPENAI_CHAT_URL,
                headers=headers,
                data=json.dumps(payload),
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
                break

            data = r.json()
            content = data["choices"][0]["message"]["content"]
            if content and content.strip():
                return content.strip()
            last_err = "Empty completion"
        except Exception as e:
            last_err = str(e)
            if attempt < REQ_RETRIES:
                time.sleep(BACKOFF ** attempt)
                continue

    # Fallback if OpenAI fails
    fallback = _deterministic_digest(month_ym, items)
    if last_err:
        fallback += f"\n\n---\n\n> Note: LLM summarisation failed; fallback used. Error: `{last_err}`\n"
    return fallback


# Backwards compatible alias
build = build_digest
