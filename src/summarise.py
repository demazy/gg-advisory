import os, requests, textwrap
from typing import List, Dict
from .utils import today_iso

SYSTEM = (
  "You are an executive editor for GG Advisory. Create a concise **monthly or weekly digest** for senior readers. "
  "Prefer Australian/EU regulatory and market impacts. Only summarise verifiable facts from the provided text. "
  "If an item is too short or unrelated to its URL, output exactly: SKIP <URL>. "
  "Do not invent dates; omit if unclear. Use clean Markdown. "
  "Each item must include a short Sources list with URLs."
)

USER_TMPL = """Create **Signals Digest — {date}** across three sections:
- **Energy Transition**
- **ESG Reporting**
- **Sustainable Finance & Investment**

Start with **Top Lines** — 3 bullets (macro takeaways).

Then **Top Items** (6–12 items total across all sections):
- **Headline** (≤10 words)
- **SECTION:** one of the three above
- **PUBLISHED:** `<ISO date>` if provided
- **Summary:** 120–160 words, factual, with numbers/dates/jurisdictions
- *Why it matters* — 2 bullets (implications for AU/EU businesses, investors, or compliance)
- **Signals to watch** — 1–2 bullets (deadlines, thresholds, capacity, consultations)
- **Sources** — bullet list with 1–2 URLs

Style: Australian audience; neutral and concise; expand acronyms on first mention.
If any item was marked SKIP, omit it entirely.

Items (raw material follows; each block contains SECTION, TITLE, URL, PUBLISHED (if known), SOURCE, and TEXT_SNIPPET):

{items}
"""

def _pack_items(items: List[Dict]) -> str:
    blocks = []
    for i, it in enumerate(items, 1):
        snippet = (it.get("text") or it.get("summary") or "")
        snippet = textwrap.shorten(snippet, width=1700, placeholder=" …")
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
    payload = {
        "model": model,
        "temperature": temp,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": USER_TMPL.format(date=today_iso(), items=_pack_items(items))}
        ]
    }
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json=payload,
        timeout=180
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]
