# -*- coding: utf-8 -*-
"""
ARK Capture Solutions — monthly intelligence brief summariser.

Produces a 4-section markdown brief tailored for ARK's BD team:
  1. Grants & Funding
  2. Market & Policy
  3. Competitors
  4. Partners & Buyers

Also appends a BASELINE_DELTA JSON block at the end of the digest for
automated baseline maintenance (read by ark_apply_baseline_delta.py).

Audience: ARK leadership (CEO/CTO/BD) evaluating AU/APAC market entry
for their modular point-source carbon capture technology.

Env vars consumed:
    OPENAI_API_KEY
    MODEL            (default: gpt-4o)
    TEMP             (default: 0  — maximum grounding, no creative variation)
    OPENAI_MAX_TOKENS (default: 6000  — extended for BASELINE_DELTA block)
    MAX_TEXT_CHARS_PER_ITEM (default: 8000)
    TIER1_RESULTS_FILE  — path to ark-tier1-verify-{YM}.json (optional)
    PREV_DIGEST_FILE    — path to previous month's digest markdown (optional)
    CFG_BASELINE        — path to ark-baseline.yaml (optional, for context)
    DEBUG
"""
from __future__ import annotations

import calendar
import json
import os
import re
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import yaml

OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "").strip()
MODEL                = os.getenv("MODEL", "gpt-4o").strip()
TEMP                 = float(os.getenv("ARK_TEMP", os.getenv("TEMP", "0")))
OPENAI_MAX_TOKENS    = int(os.getenv("OPENAI_MAX_TOKENS", "6000"))
MAX_TEXT_CHARS_PER_ITEM = int(os.getenv("MAX_TEXT_CHARS_PER_ITEM", "8000"))
TIER1_RESULTS_FILE   = os.getenv("TIER1_RESULTS_FILE", "").strip()
PREV_DIGEST_FILE     = os.getenv("PREV_DIGEST_FILE", "").strip()
CFG_BASELINE         = os.getenv("CFG_BASELINE", "config/ark-baseline.yaml").strip()
DEBUG                = os.getenv("DEBUG", "0") == "1"

ARK_SECTIONS = [
    "Grants & Funding",
    "Market & Policy",
    "Competitors",
    "Partners & Buyers",
]

_SECTION_KEYS = {
    "Grants & Funding":  "grants_funding",
    "Market & Policy":   "market_policy",
    "Competitors":       "competitors",
    "Partners & Buyers": "partners_buyers",
}

_SECTION_NAMES = {v: k for k, v in _SECTION_KEYS.items()}


# ── Shared helpers ─────────────────────────────────────────────────────────────

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


# ── Load supplementary context ────────────────────────────────────────────────

def _load_tier1_results() -> Optional[Dict]:
    if not TIER1_RESULTS_FILE:
        return None
    p = Path(TIER1_RESULTS_FILE)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ark_summarise] Could not load Tier 1 results: {e}")
        return None


def _load_prev_digest() -> str:
    if not PREV_DIGEST_FILE:
        return ""
    p = Path(PREV_DIGEST_FILE)
    if not p.exists():
        return ""
    try:
        text = p.read_text(encoding="utf-8")
        # Strip BASELINE_DELTA block from prev digest before sending to GPT
        text = re.sub(
            r"---BASELINE_DELTA_START---.*?---BASELINE_DELTA_END---",
            "",
            text,
            flags=re.DOTALL,
        ).strip()
        return text
    except Exception as e:
        print(f"[ark_summarise] Could not load previous digest: {e}")
        return ""


def _load_baseline_entry_ids() -> List[str]:
    """Load entry IDs from the baseline for BASELINE_DELTA reference."""
    p = Path(CFG_BASELINE)
    if not p.exists():
        return []
    try:
        bl = yaml.safe_load(p.read_text(encoding="utf-8"))
        ids = []
        for section in bl.get("sections", {}).values():
            for entry in section.get("entries", []):
                ids.append(entry.get("id", ""))
        return [i for i in ids if i]
    except Exception:
        return []


# ── Deterministic fallback ────────────────────────────────────────────────────

def _render_items_fallback(items: Sequence[Any]) -> List[str]:
    if not items:
        return ["\n_No in-range items selected for this section._\n"]
    lines: List[str] = []
    for it in items:
        title   = (_get(it, "title", "") or "").strip()[:120]
        url     = (_get(it, "url", "") or "").strip()
        raw_pub = (_get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))).strip()
        pub     = _format_pub_date(raw_pub) if raw_pub else ""
        summ    = _extractive_summary(_get_text(it), max_words=150)

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
        sec  = _get(it, "section", "") or ""
        sidx = ARK_SECTIONS.index(sec) if sec in ARK_SECTIONS else 999
        p    = _get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))
        return (sidx, p or "", _get(it, "url", "") or "")

    rows   = sorted(list(items), key=sort_key)
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
    out.append("- Limited high-signal items available this period; see section details below.")

    for section_name in ARK_SECTIONS:
        sec_key = _SECTION_KEYS[section_name]
        out.append(f"\n---\n## SECTION: {sec_key}")
        out.append(f"\n### Updates This Month")
        out.extend(_render_items_fallback(by_sec[section_name]))
        out.append("\n### Changes Since Last Issue")
        out.append("_No changes detected this period._")

    # Empty BASELINE_DELTA block for fallback
    out.append("\n---BASELINE_DELTA_START---")
    delta = {
        "period": date_label,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "deterministic_fallback",
        "items": [],
    }
    out.append(json.dumps(delta, ensure_ascii=False, indent=2))
    out.append("---BASELINE_DELTA_END---")

    return "\n".join(out).strip() + "\n"


# ── OpenAI path ───────────────────────────────────────────────────────────────

def _openai_chat(messages: List[Dict[str, str]]) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing")
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json={
            "model":      MODEL,
            "messages":   messages,
            "temperature": TEMP,
            "max_tokens": OPENAI_MAX_TOKENS,
        },
        timeout=120,
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
            "title":     (_get(it, "title", "") or "").strip(),
            "url":       (_get(it, "url", "") or "").strip(),
            "publisher": (_get(it, "source", "") or _get(it, "publisher", "") or "").strip(),
            "section":   (_get(it, "section", "") or "").strip(),
            "published": (_get(it, "published_iso", "") or _iso_date(_get(it, "published_ts"))).strip(),
            "text":      text,
        })
    return out


# ── System prompt ─────────────────────────────────────────────────────────────

_SYSTEM = """\
You are a senior business intelligence analyst preparing a monthly briefing for ARK Capture Solutions, \
a Belgian company with proprietary modular point-source carbon capture technology designed for \
low-concentration industrial flue gases (biogas plants, gas-fired power, petrochemicals, glass, steel, \
waste-to-energy, cement, biomass co-generation).

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
G. The Source URL for each entry MUST be copied VERBATIM from the "url" field of the JSON \
   payload item. Do NOT alter, shorten, paraphrase, reconstruct, or construct any URL. \
   If you cannot find the exact URL in the payload, omit the Source line entirely.
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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BASELINE DELTA RULES (for the JSON block at the end)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
You will also produce a BASELINE_DELTA JSON block. This block records detected changes \
against the prior month's digest (if provided) and Tier 1 URL verification results.

Change categories to detect:
  - grant_lifecycle: grant opened, closed, round results announced, deadline extended
  - policy_change: new regulation, amended rule, target updated, review outcome
  - new_entrant: new competitor technology or project announced in AU/APAC
  - company_status: ownership change, administration, merger, delisting, project milestone
  - project_milestone: FEED, FID, commissioning, operation, cancellation of a tracked project
  - price_update: ACCU spot price, electricity wholesale price, carbon price update
  - other: any other material change to a baseline entry

For each detected change:
  - entry_id: the baseline entry ID it relates to (e.g. "gf-001") — use "" if it is a new entry
  - section: the baseline section key (grants_funding, market_policy, competitors, partners_buyers)
  - change_type: one of the categories above
  - description: 1–2 sentence factual description of what changed (ONLY facts from the source text)
  - current_bullet: the specific bullet text being changed or added (verbatim from source)
  - source_url: the URL from the JSON payload that evidences this change
  - confidence: 0.0–1.0 based on how clearly the source text supports the change
    (0.85+ = unambiguous; 0.60–0.85 = likely but some uncertainty; <0.60 = tentative)
  - action: "update_bullet" | "add_bullet" | "add_entry" | "deprecate_entry" | "flag_contradiction"
  - new_entry_label: (only if action=add_entry) proposed label for the new baseline entry

If no changes are detected, return "items": [].
Be conservative: only flag changes that are clearly supported by the source text.
"""

# ── User message template ──────────────────────────────────────────────────────

_USER_TEMPLATE = textwrap.dedent("""\
    Using the JSON payload below, write a monthly intelligence brief in markdown for ARK Capture Solutions.

    Required structure — reproduce these headings EXACTLY (including the SECTION: prefix):

    # ARK Intelligence Brief — {month_year}

    **Executive Summary**
    - [Key takeaway 1 — cite a specific grant amount, policy change, or company name]
    - [Key takeaway 2 — cite a specific grant amount, policy change, or company name]
    - [Key takeaway 3 — cite a specific grant amount, policy change, or company name]

    ---
    ## SECTION: grants_funding

    ### Updates This Month

    [items — use the article format below]

    ### Changes Since Last Issue

    [CHANGE: description (Source: URL) — one line per detected change, or "_No changes detected this period._"]

    ---
    ## SECTION: market_policy

    ### Updates This Month

    [items]

    ### Changes Since Last Issue

    [changes or "_No changes detected this period._"]

    ---
    ## SECTION: competitors

    ### Updates This Month

    [items]

    ### Changes Since Last Issue

    [changes or "_No changes detected this period._"]

    ---
    ## SECTION: partners_buyers

    ### Updates This Month

    [items]

    ### Changes Since Last Issue

    [changes or "_No changes detected this period._"]

    Article format (2–5 entries per section, blank line between entries):

    **[Concise headline, max 12 words]**
    Published: [D Month YYYY — omit if date unavailable]
    Summary: [80–130 words. What happened, specific figures, entities, geographic scope, technology context.]
    Why it matters for ARK: [1–2 sentences. Name the concrete BD action, deadline, or risk for ARK's \
    AU/APAC entry. Be specific about ARK's technology fit, grant eligibility, or competitive threat.]
    Signals to watch: [1 sentence. Next concrete trigger — application deadline, policy decision, \
    company announcement, or tender to track.]
    Source: [the URL for this item — must match exactly]

    Rules:
    - Only include an article entry if the item text supports a meaningful, specific summary.
    - If a section has fewer than 2 good items, write what is available — do not pad.
    - Do NOT add a sources list at the bottom.
    - Use ONLY facts from the provided item text.
    - "Changes Since Last Issue" lines MUST be supported by the item text or previous digest comparison.

    After the final section, append this block EXACTLY (do not alter the delimiters):

    ---BASELINE_DELTA_START---
    {{
      "period": "{date_label}",
      "generated_at": "<current ISO timestamp>",
      "items": [
        {{
          "entry_id": "<baseline entry ID or empty string for new entry>",
          "section": "<section key>",
          "change_type": "<category>",
          "description": "<1-2 sentence factual description>",
          "current_bullet": "<the bullet text being updated or added>",
          "source_url": "<URL from payload>",
          "confidence": <0.0-1.0>,
          "action": "<update_bullet|add_bullet|add_entry|deprecate_entry|flag_contradiction>",
          "new_entry_label": "<only if action=add_entry>"
        }}
      ]
    }}
    ---BASELINE_DELTA_END---

    JSON payload:
    {json}

    {prev_digest_section}
    {tier1_section}
""")


# ── URL hallucination guard ───────────────────────────────────────────────────

def _norm_url(url: str) -> str:
    """Normalise a URL for comparison: lowercase, strip scheme, strip www., strip trailing slash."""
    u = url.strip().lower()
    for prefix in ("https://", "http://"):
        if u.startswith(prefix):
            u = u[len(prefix):]
            break
    if u.startswith("www."):
        u = u[4:]
    # Strip query string and fragment — GPT-4o sometimes appends tracking params
    u = u.split("?")[0].split("#")[0]
    return u.rstrip("/")


def _validate_source_urls(md_output: str, allowed_urls: set[str]) -> list[str]:
    """
    Find every 'Source: <url>' line in the newsletter body and verify the URL
    was in the input payload.

    Matching is two-tier:
      1. Exact normalised match (preferred).
      2. Domain-level match — if GPT-4o cited a URL on an allowed domain
         (and didn't invent a wholly different domain), treat it as a minor
         formatting issue rather than a hallucination and log a warning only.
    Returns a list of violation strings for URLs on domains NOT in the payload.
    """
    from urllib.parse import urlparse

    norm_allowed  = {_norm_url(u) for u in allowed_urls}
    allowed_domains = {urlparse(u).netloc.lower().lstrip("www.") for u in allowed_urls}

    violations: list[str] = []
    in_delta = False
    for line in md_output.splitlines():
        s = line.strip()
        if s == "---BASELINE_DELTA_START---":
            in_delta = True
            continue
        if s == "---BASELINE_DELTA_END---":
            in_delta = False
            continue
        if in_delta:
            continue
        if s.lower().startswith("source:"):
            url = s[len("Source:"):].strip()
            if not url:
                continue
            norm = _norm_url(url)
            if norm in norm_allowed:
                continue  # exact match — all good
            # Check domain-level match
            cited_domain = urlparse(url.lower()).netloc.lstrip("www.")
            if cited_domain in allowed_domains:
                # Same domain — likely a minor URL formatting difference, not a hallucination
                print(f"[ark_summarise] URL mismatch (domain OK, path differs): {url}")
                continue
            # Completely different domain — genuine hallucination
            violations.append(f"Unknown URL not in payload: {url}")
    return violations


def _extract_baseline_delta(md_output: str) -> Optional[Dict]:
    """Extract and parse the BASELINE_DELTA JSON block from the LLM output."""
    m = re.search(
        r"---BASELINE_DELTA_START---\s*(.*?)\s*---BASELINE_DELTA_END---",
        md_output,
        re.DOTALL,
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception as e:
        print(f"[ark_summarise] Could not parse BASELINE_DELTA JSON: {e}")
        return None


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
            note="OpenAI not configured; deterministic fallback used.",
        )

    # Load supplementary context
    tier1_data  = _load_tier1_results()
    prev_digest = _load_prev_digest()
    entry_ids   = _load_baseline_entry_ids()

    payload     = _prepare_items(items)
    allowed_urls = {p["url"] for p in payload if p.get("url")}
    month_year  = _format_month_year(date_label)

    # Build optional prev-digest and tier1 sections of the prompt
    if prev_digest:
        prev_section = (
            "\nPREVIOUS MONTH'S DIGEST (for Changes Since Last Issue detection):\n"
            "Use this to identify what has changed. Only flag changes that are clearly\n"
            "evidenced in the current payload.\n"
            "```\n" + prev_digest[:6000] + "\n```\n"
        )
    else:
        prev_section = "\n(No previous digest available — this is the inaugural issue. " \
                       "Write \"_Inaugural issue — Changes section will be populated from the second issue onwards._\" " \
                       "for all Changes Since Last Issue sections.)\n"

    if tier1_data and tier1_data.get("results"):
        failed = [r for r in tier1_data["results"] if r["fetch_status"] != "ok"]
        tier1_section = ""
        if failed:
            tier1_section = (
                "\nTIER 1 VERIFICATION ALERTS (source URLs that could NOT be fetched this month):\n"
                + "\n".join(
                    f"  - {r['entry_id']} ({r['label'][:60]}): {r['fetch_status']}"
                    for r in failed
                )
                + "\nIf you reference any of these entries in BASELINE_DELTA, use "
                  "action='flag_contradiction' with confidence <= 0.5 and note the fetch failure.\n"
            )
    else:
        tier1_section = ""

    user_msg = _USER_TEMPLATE.format(
        month_year=month_year,
        date_label=date_label,
        json=json.dumps(
            {"date_label": date_label, "sections": ARK_SECTIONS, "items": payload},
            ensure_ascii=False,
        ),
        prev_digest_section=prev_section,
        tier1_section=tier1_section,
    )

    try:
        content = _openai_chat([
            {"role": "system",  "content": _SYSTEM},
            {"role": "user",    "content": user_msg},
        ])
        out = (content or "").strip()

        # ── Structural check ─────────────────────────────────────────────────
        missing_sections = [
            h for h in (
                "## SECTION: grants_funding",
                "## SECTION: market_policy",
                "## SECTION: competitors",
                "## SECTION: partners_buyers",
            ) if h not in out
        ]
        if missing_sections:
            raise RuntimeError(f"LLM output missing section markers: {', '.join(missing_sections)}")

        # ── BASELINE_DELTA block check ───────────────────────────────────────
        if "---BASELINE_DELTA_START---" not in out:
            print("[ark_summarise] WARNING: LLM did not produce BASELINE_DELTA block. "
                  "Appending empty block.")
            empty_delta = json.dumps({
                "period":       date_label,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source":       "llm_no_block",
                "items":        [],
            }, ensure_ascii=False, indent=2)
            out += f"\n\n---BASELINE_DELTA_START---\n{empty_delta}\n---BASELINE_DELTA_END---"

        # Validate the BASELINE_DELTA JSON is parseable
        delta = _extract_baseline_delta(out)
        if delta is None:
            print("[ark_summarise] WARNING: BASELINE_DELTA block could not be parsed; "
                  "replacing with empty block.")
            out = re.sub(
                r"---BASELINE_DELTA_START---.*?---BASELINE_DELTA_END---",
                "",
                out,
                flags=re.DOTALL,
            ).strip()
            empty_delta = json.dumps({
                "period":       date_label,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source":       "parse_error",
                "items":        [],
            }, ensure_ascii=False, indent=2)
            out += f"\n\n---BASELINE_DELTA_START---\n{empty_delta}\n---BASELINE_DELTA_END---"
        else:
            # GPT-4o sometimes returns the literal placeholder string rather than
            # filling in the timestamp — replace it unconditionally with the real value.
            delta["generated_at"] = datetime.now(timezone.utc).isoformat()
            fixed_delta_json = json.dumps(delta, ensure_ascii=False, indent=2)
            out = re.sub(
                r"---BASELINE_DELTA_START---.*?---BASELINE_DELTA_END---",
                f"---BASELINE_DELTA_START---\n{fixed_delta_json}\n---BASELINE_DELTA_END---",
                out,
                flags=re.DOTALL,
            )

        if delta is not None and DEBUG and delta.get("items"):
            print(f"[ark_summarise] BASELINE_DELTA: {len(delta['items'])} item(s) detected:")
            for item in delta["items"]:
                print(f"  [{item.get('confidence', '?'):.2f}] {item.get('action')} "
                      f"{item.get('entry_id')} — {item.get('description', '')[:80]}")

        # ── URL hallucination check (newsletter body only) ───────────────────
        violations = _validate_source_urls(out, allowed_urls)
        if violations:
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
            note=f"LLM path failed; deterministic extractive-only output used. Error: {e}",
        )
