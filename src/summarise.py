import os, requests, textwrap
from typing import List, Dict
from .utils import today_iso

SYSTEMS = {
  "Policy Pulse": """You are GG Advisory’s policy analyst. Write neutral, concise, executive-ready policy briefs for AU/EU readers. Use Markdown and include a short Sources list with URLs per item.""",
  "Strategic Signals": """You are an energy-transition strategist. Summarise system/market signals. Use Markdown with sources.""",
  "KPI Watch": """You are a sustainability reporting specialist. Explain disclosure rules and metrics succinctly. Use Markdown and sources.""",
  "Investor Radar": """You advise investors and founders on climate-tech capital. Be factual, avoid hype. Use Markdown with sources.""",
  "Tech Moves": """You are a cleantech analyst. Focus on technical substance and readiness levels. Use Markdown with sources."""
}

USERS = {
  "Policy Pulse": """Create **Policy Pulse — %s**. For each item provide:
- **Headline** (≤10 words)
- 120–160-word summary (plain facts)
- *Why it matters* — 2 bullets (business/compliance/timing)
- **Signals to watch** — 2 bullets (dates, consultations, thresholds)
- **Tags** — 3–6 (e.g., csrd, issb, cer, nger, vcm, eu, au)
- **Sources** — bullet list with URLs
Add a 2-line intro and a closing CTA: "Have a policy, ESG, or transition question? Contact GG Advisory."
""",
  "Strategic Signals": """Create **Strategic Signals — %s** with the same schema; emphasise grid adequacy, resource adequacy, project pipelines, and market risks.""",
  "KPI Watch": """Create **KPI Watch — %s**. For each item: Headline; 120–180-word summary; *What changes for reporters* — 2 bullets; *Data & controls* — 2 bullets; Tags; Sources.""",
  "Investor Radar": """Create **Investor Radar — %s**. For each deal/funding item: deal summary; *Why it matters*; *Risks/unknowns*; Tags; Sources.""",
  "Tech Moves": """Create **Tech Moves — %s**. For each item: Headline; 100–140-word summary; *TRL & readiness* — 1–2 bullets; *Commercial path* — 1–2 bullets; Tags; Sources."""
}

def call_openai(model: str, api_key: str, system: str, user: str, items: List[Dict], temp: float) -> str:
    # Compact item block to save tokens
    blocks = []
    for i, it in enumerate(items, 1):
        snippet = (it.get("text") or it.get("summary") or "")
        snippet = textwrap.shorten(snippet, width=1600, placeholder=" …")
        blocks.append(f"""[{i}]
TITLE: {it.get('title','').strip() or '(no title)'}
URL: {it['url']}
SOURCE: {it['source']}
TEXT_SNIPPET:
{snippet}
""")
    content = "\n\n".join(blocks)
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": model,
            "temperature": temp,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": USERS[user] % today_iso()},
                {"role": "user", "content": f"Items:\n{content}"}
            ]
        },
        timeout=120
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]
