import os, requests, textwrap
from typing import List, Dict
from .utils import today_iso

SYSTEM = (
  "You are an executive editor for GG Advisory. Create a concise digest ONLY from the items provided. "
  "STRICT RULES: (1) Do NOT invent items or details, (2) Use only facts contained in the given items, "
  "(3) If zero valid items are provided, respond with exactly: NO_ITEMS_IN_RANGE "
  "(4) If any item is too short or unrelated to its URL, it will be omitted by the caller. "
  "Use clean Markdown and include Sources with the exact URLs supplied in the items."
)

USER_TMPL = """If there are zero items, output exactly: NO_ITEMS_IN_RANGE

Otherwise, create **Signals Digest — {date}** across three sections:
- **Energy Transition**
- **ESG Reporting**
- **Sustainable Finance & Investment**

Start with **Top Lines** — 3 bullets (macro takeaways).

Then **Top Items** (6–12 items total across all sections):
- **Headline** (≤10 words)
- **SECTION:** one of the three above
- **PUBLISHED:** `<ISO date>` if provided
- **Summary:** 120–160 words, factual, with numbers/dates/jurisdictions present in the item text
- *Why it matters* — 2 bullets (implications for AU/EU businesses, investors, or compliance)
- **Signals to watch** — 1–2 bullets (deadlines, thresholds, capacity, consultations)
- **Sources** — bullet list with the provided URL(s) ONLY

Style: Australian audience; neutral and concise; expand acronyms on first mention.
Do NOT add any item that is not represented in the Items list.

Items (raw material follows; each block contains SECTION, TITLE, URL, PUBLISHED (if known), SOURCE, and TEXT_SNIPPET):

{items}
"""

def _pack_items(items: List[Dict]) -> str:
    if not items:
        return ""
    blocks = []
    for i, it in enumerate(items, 1):
        snippet = (it.get("text") or it.get("summary") or "")
        # keep more context so we don't lose dates/numbers; cap reasonably
        snippet = textwrap.shorten(snippet, width=2200, placeholder=" …")
        blocks.append(f"""[{i}]
SECTION: {it.get('section','')}
TITLE: {it.get('title','').strip() or '(no title)'}
URL: {it['url']}
PUBLISHED: {it.get('published','')}
SOURCE: {it['source']}
TEXT_SNIPPET:
{snippet}
""")
    return "\n\n".join(blocks)

def build_digest(model: str, api_key: str, items: List[Dict], temp: float) -> str:
    user_content = USER_TMPL.format(date=today_iso(), items=_pack_items(items))
    payload = {
        "model": model,
        "temperature": temp,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user_content}
        ]
    }
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json=payload,
        timeout=180
    )
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"].strip()
    return content
