import requests, textwrap
from typing import List, Dict

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
- *Why it matters* — 2 bullets (implications for AU/EU businesses, investors, or compliance)
- **Signals to watch** — 1–2 bullets (deadlines, thresholds, capacity, consultations)
- **Sources** — bullet list with the provided URL(s) ONLY

Do NOT add any item that is not represented in the Items list.

Items (each block contains SECTION, TITLE, URLS (bullet list), PUBLISHED, and TEXT_SNIPPET):

{items}
"""

def _pack_items(items: List[Dict]) -> str:
    if not items:
        return ""
    blocks = []
    for i, it in enumerate(items, 1):
        snippet = (it.get("text") or it.get("summary") or "")
        snippet = textwrap.shorten(snippet, width=2200, placeholder=" …")
        urls = it.get("sources_urls") or [it["url"]]
        url_lines = "\n".join(f"- {u}" for u in urls)
        blocks.append(f"""[{i}]
SECTION: {it.get('section','')}
TITLE: {it.get('title','').strip() or '(no title)'}
URLS:
{url_lines}
PUBLISHED: {it.get('published','')}
TEXT_SNIPPET:
{snippet}
""")
    return "\n\n".join(blocks)

def build_digest(model: str, api_key: str, items: List[Dict], temp: float, date_label: str) -> str:
    payload = {
        "model": model,
        "temperature": temp,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": USER_TMPL.format(date_label=date_label, items=_pack_items(items))}
        ]
    }
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json=payload,
        timeout=180
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()
