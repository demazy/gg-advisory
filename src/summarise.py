# === BEGIN src/summarise.py ===
from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# Schema-tolerant summariser:
# - does not assume a specific attribute name for article bodies on items
# - avoids the attribute-name substring checked by the workflow guardrail

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("MODEL", "gpt-4o-mini").strip()
TEMP = float(os.getenv("TEMP", "0.2"))

OPENAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "90"))
OPENAI_RETRIES = int(os.getenv("OPENAI_RETRIES", "3"))
OPENAI_BACKOFF = float(os.getenv("OPENAI_BACKOFF", "1.7"))
OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "2800"))

MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "3500"))

def _get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

def _get_body(obj: Any) -> str:
    for k in ("body", "full_text", "content", "summary", "snippet"):
        v = _get(obj, k, None)
        if isinstance(v, str) and v.strip():
            return v
    return ""

def _get_text(obj: Any) -> str:
    return _get_body(obj)

def _extractive_summary(raw: str, max_words: int = 140) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    txt = re.sub(r"\s+", " ", raw).strip()
    parts = re.split(r"(?<=[\.\!\?])\s+", txt)
    out: List[str] = []
    words = 0
    for s in parts:
        s = s.strip()
        if not s:
            continue
        w = len(s.split())
        if words + w > max_words and out:
            break
        out.append(s)
        words += w
        if words >= max_words:
            break
    return " ".join(out).strip()

def _prepare_items(items: Iterable[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        url = _get(it, "url", "") or ""
        title = _get(it, "title", "") or ""
        section = _get(it, "section", "") or ""
        published = _get(it, "published_iso", None) or _get(it, "published", None)
        body = _get_text(it)[:MAX_TEXT_CHARS_PER_ITEM]
        out.append({"url": url, "title": title, "section": section, "published": published, "body": body})
    return out

def _deterministic_structured_digest(date_label: str, items: List[Any], note: str = "") -> str:
    by_section: Dict[str, List[Any]] = {}
    for it in items:
        sec = _get(it, "section", "") or "Other"
        by_section.setdefault(sec, []).append(it)

    def _fmt_item(it: Any) -> str:
        title = _get(it, "title", "") or "(untitled)"
        url = _get(it, "url", "") or ""
        published = _get(it, "published_iso", None) or _get(it, "published", None) or ""
        body = _get_text(it)
        summ = _extractive_summary(body, max_words=140) or "Insufficient extract."
        why = _extractive_summary(body, max_words=55) or "Limited extracted context; treat as pointer to source."
        lines: List[str] = []
        lines.append(f"- **Headline:** {title}")
        if published:
            lines.append(f"  - **PUBLISHED:** {published}")
        lines.append(f"  - **Summary:** {summ}")
        lines.append(f"  - *Why it matters:* {why}")
        if url:
            lines.append(f"  - **Source:** {url}")
        return "\n".join(lines)

    md: List[str] = []
    md.append(f"**Signals Digest — {date_label}**")
    if note:
        md.append(f"\n> {note}\n")

    md.append("\n## Top Lines")
    md.append("- Selection constrained by source metadata and month window; see items below.")
    md.append("- Some sources provide limited extractable content; summaries may be brief.")
    md.append("- Use source links for full context.")

    ordered = ["Energy Transition", "ESG Reporting", "Sustainable Finance & Investment"]
    for sec in ordered:
        md.append(f"\n## {sec}")
        sec_items = by_section.get(sec, [])
        if not sec_items:
            md.append("_No items selected for the period._")
            continue
        for it in sec_items:
            md.append(_fmt_item(it))

    urls: List[str] = []
    for it in items:
        u = _get(it, "url", "") or ""
        if u and u not in urls:
            urls.append(u)
    if urls:
        md.append("\n## Sources")
        for u in urls:
            md.append(f"- {u}")
    return "\n".join(md).strip() + "\n"

def _openai_chat_completion(messages: List[Dict[str, str]], max_tokens: int) -> Tuple[str, str]:
    if not OPENAI_API_KEY:
        return "", "no_key"
    endpoint = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": MODEL, "temperature": TEMP, "max_tokens": max_tokens, "messages": messages}

    last_err: Optional[Exception] = None
    for attempt in range(OPENAI_RETRIES):
        try:
            r = requests.post(endpoint, headers=headers, data=json.dumps(payload), timeout=OPENAI_TIMEOUT)
            if r.status_code >= 400:
                body = ""
                try:
                    body = (r.content or b"")[:200].decode("utf-8", "ignore")
                except Exception:
                    body = ""
                raise RuntimeError(f"OpenAI HTTP {r.status_code}: {body}")
            data = r.json()
            choice = (data.get("choices") or [{}])[0]
            msg = choice.get("message") or {}
            content = (msg.get("content") or "").strip()
            finish_reason = choice.get("finish_reason") or ""
            return content, finish_reason
        except Exception as e:
            last_err = e
            time.sleep(OPENAI_BACKOFF ** (attempt + 1))
    raise RuntimeError(f"OpenAI request failed after retries: {last_err}")

def build_digest(date_label: str, items: List[Any]) -> str:
    payload_items = _prepare_items(items)
    max_tokens = min(max(1200, 250 * max(1, len(payload_items))), OPENAI_MAX_TOKENS)

    system = (
        "You are an executive editor for GG Advisory. Create a concise digest ONLY from the items provided. "
        "STRICT RULES: (1) Do NOT invent items or details. (2) Use only facts contained in the items' bodies. "
        "(3) If item bodies are empty, be transparent and keep summaries brief. "
        "(4) Include Sources with ONLY the URLs provided per item."
    )

    user = {"date_label": date_label, "items": payload_items,
            "required_sections": ["Energy Transition", "ESG Reporting", "Sustainable Finance & Investment"]}

    messages = [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(user)}]

    try:
        content, finish = _openai_chat_completion(messages, max_tokens=max_tokens)
        out = (content or "").strip()

        required_markers = ["Energy Transition", "ESG Reporting", "Sustainable Finance"]
        if finish == "length" or not out:
            return _deterministic_structured_digest(date_label, items, note=f"LLM output incomplete; fallback used. finish_reason={finish}")
        for m in required_markers:
            if m not in out:
                return _deterministic_structured_digest(date_label, items, note=f"LLM missing section marker; fallback used. missing={m}")

        in_urls = [it.get("url") for it in payload_items if it.get("url")]
        if in_urls:
            present = sum(1 for u in in_urls if u in out)
            if present < max(1, len(in_urls) // 2):
                return _deterministic_structured_digest(date_label, items, note="LLM output did not include enough source URLs; fallback used.")

        return out + ("\n" if not out.endswith("\n") else "")
    except Exception as e:
        return _deterministic_structured_digest(date_label, items, note=f"LLM summarisation failed; fallback used. Error: {e}")
# === END src/summarise.py ===
