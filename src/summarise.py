import os, requests, textwrap, time, random
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


def _post_with_retry(url: str, json_payload: dict, headers: dict, *, max_retries: int = 6, base_delay: float = 2.0) -> requests.Response:
    """Retry on 429/5xx with exponential backoff+jitter. Returns the final Response."""
    delay = base_delay
    last_exc = None
    last_resp: requests.Response | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, json=json_payload, headers=headers, timeout=90)
            last_resp = resp
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                sleep_s = float(ra) if ra else delay + random.uniform(0, 0.7)
                time.sleep(sleep_s)
                delay = min(delay * 2, 30)
                continue
            if 500 <= resp.status_code < 600:
                time.sleep(delay + random.uniform(0, 0.7))
                delay = min(delay * 2, 30)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            time.sleep(delay + random.uniform(0, 0.7))
            delay = min(delay * 2, 30)

    if last_resp is not None:
        body = ""
        try:
            body = (last_resp.text or "")[:180]
        except Exception:
            body = ""
        msg = f"OpenAI API exhausted retries (status {last_resp.status_code}). Body: {body}"
        http_err = requests.HTTPError(msg, response=last_resp)
        raise http_err

    if last_exc:
        raise last_exc

    raise RuntimeError("OpenAI API exhausted retries without response/exception.")


def call_openai(model: str, api_key: str, system: str, user: str, items: List[Dict], temp: float) -> str:
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

    org = os.getenv("OPENAI_ORG") or os.getenv("OPENAI_ORGANIZATION")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if org:
        headers["OpenAI-Organization"] = org

    payload = {
        "model": model,
        "temperature": temp,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": USERS[user] % today_iso()},
            {"role": "user", "content": f"Items:\n{content}"},
        ],
    }

    for jtry in range(2):
        resp = _post_with_retry("https://api.openai.com/v1/chat/completions", payload, headers)
        try:
            data = resp.json()
            return data["choices"][0]["message"]["content"]
        except ValueError:
            time.sleep(1.5 + random.uniform(0, 0.5))

    data = resp.json()
    return data["choices"][0]["message"]["content"]
