import os, json, yaml, time
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict, Set
from .fetch import fetch_rss, fetch_html_index, fetch_full_text, Item
from .summarise import call_openai, SYSTEMS
from .utils import sha1, normalize_whitespace, today_iso

# --------------------
# Env & constants
# --------------------
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMP = float(os.getenv("TEMP", "0.25"))
ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "4"))
SLEEP_BETWEEN_SECTIONS = float(os.getenv("SLEEP_BETWEEN_SECTIONS", "3.0"))
MAX_TEXT_CHARS = int(os.getenv("MAX_TEXT_CHARS", "2500"))

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state" / "seen_urls.json"
OUTDIR = ROOT / "out"
CFG = ROOT / "config" / "sources.yaml"

OUTDIR.mkdir(parents=True, exist_ok=True)
STATE.parent.mkdir(parents=True, exist_ok=True)

# --------------------
# State helpers (robust)
# --------------------
def load_seen() -> Set[str]:
    try:
        if not STATE.exists() or STATE.stat().st_size == 0:
            return set()
        data = json.loads(STATE.read_text(encoding="utf-8"))
        seen = data.get("seen", [])
        if isinstance(seen, list):
            return set(str(x) for x in seen)
    except Exception as e:
        print(f"[warn] seen_urls.json invalid or unreadable: {e}. Starting fresh.")
    return set()

def save_seen(seen: Set[str]) -> None:
    tmp = STATE.with_suffix(".tmp")
    payload = {"seen": sorted(seen)}
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(STATE)

# --------------------
# Data collection
# --------------------
def collect_items(s: Dict) -> List[Item]:
    rss_urls = s.get("rss", []) or []
    html_urls = s.get("html", []) or []
    items: List[Item] = []
    for r in rss_urls:
        try:
            items.extend(fetch_rss(r))
        except Exception as e:
            print(f"[warn] RSS source failed {r}: {e}")
    for h in html_urls:
        try:
            items.extend(fetch_html_index(h))
        except Exception as e:
            print(f"[warn] HTML index failed {h}: {e}")
    items.sort(key=lambda x: x.published_ts, reverse=True)
    return items

# --------------------
# Main
# --------------------
def main():
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set. Provide it via env or .env for summarisation.")

    cfg = yaml.safe_load(CFG.read_text(encoding="utf-8"))
    if not cfg or "sections" not in cfg or not isinstance(cfg["sections"], dict):
        raise RuntimeError("Invalid config: expected a top-level 'sections' mapping.")

    seen = load_seen()
    outputs: Dict[str, str] = {}

    for section, sources in cfg["sections"].items():
        pool = collect_items(sources)
        batch = []
        for it in pool:
            h = sha1(it.url)
            if h in seen:
                continue

            txt = fetch_full_text(it.url)
            it.text = normalize_whitespace(txt or "")
            if len(it.text) > MAX_TEXT_CHARS:
                it.text = it.text[:MAX_TEXT_CHARS]

            # Minimal quality guard
            if len(it.text) < 400 and len((it.summary or "")) < 120:
                continue

            # Title fallback
            if not it.title:
                it.title = it.url.split("/")[-1].replace("-", " ")[:100]

            batch.append({
                "title": it.title,
                "url": it.url,
                "source": it.source,
                "summary": it.summary,
                "text": it.text
            })
            if len(batch) >= ITEMS_PER_SECTION:
                break

        date_tag = today_iso()
        if not batch:
            outputs[section] = f"### {section} — {date_tag}\n_No new high-quality items today._"
            continue

        system_prompt = SYSTEMS.get(section) or (
            "You are a concise analyst. Summarise items for a professional audience, "
            "grouping logically, adding brief bullets with links, dates, and sources."
        )

        md = call_openai(
            model=MODEL,
            api_key=OPENAI_API_KEY,
            system=system_prompt,
            user=section,
            items=batch,
            temp=TEMP
        )
        outputs[section] = md

        # mark seen
        for b in batch:
            seen.add(sha1(b["url"]))

        # Throttle to avoid OpenAI 429s when multiple sections
        time.sleep(SLEEP_BETWEEN_SECTIONS)

    save_seen(seen)

    # write one file per section and a combined digest
    date_slug = today_iso()
    combined = [f"# Signals — {date_slug}\n"]
    for section, md in outputs.items():
        p = OUTDIR / f"{section.lower().replace(' ', '-')}-{date_slug}.md"
        p.write_text(md, encoding="utf-8")
        combined.append(f"\n\n## {section}\n\n{md}\n")

    (OUTDIR / f"signals-{date_slug}.md").write_text("\n".join(combined), encoding="utf-8")

if __name__ == "__main__":
    main()
