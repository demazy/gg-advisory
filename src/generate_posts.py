import os, json, time, yaml
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict
from .fetch import fetch_rss, fetch_html_index, fetch_full_text, Item
from .summarise import call_openai, SYSTEMS
from .utils import sha1, normalize_whitespace, today_iso

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMP = float(os.getenv("TEMP", "0.25"))
ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "4"))

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state" / "seen_urls.json"
OUTDIR = ROOT / "out"
CFG = ROOT / "config" / "sources.yaml"

OUTDIR.mkdir(parents=True, exist_ok=True)
STATE.parent.mkdir(parents=True, exist_ok=True)
if not STATE.exists():
    STATE.write_text(json.dumps({"seen": []}, indent=2))

def load_seen() -> set:
    return set(json.loads(STATE.read_text())["seen"])

def save_seen(seen: set):
    STATE.write_text(json.dumps({"seen": list(seen)}, indent=2))

def collect_items(s: Dict) -> List[Item]:
    rss_urls = s.get("rss", []) or []
    html_urls = s.get("html", []) or []
    items: List[Item] = []
    for r in rss_urls:
        items.extend(fetch_rss(r))
    for h in html_urls:
        items.extend(fetch_html_index(h))
    # sort newest first
    items.sort(key=lambda x: x.published_ts, reverse=True)
    return items

def main():
    cfg = yaml.safe_load(CFG.read_text())
    seen = load_seen()
    outputs = {}

    for section, sources in cfg["sections"].items():
        pool = collect_items(sources)
        batch = []
        for it in pool:
            h = sha1(it.url)
            if h in seen:
                continue
            # Try fetch full text (skip if page is trivial)
            txt = fetch_full_text(it.url)
            it.text = normalize_whitespace(txt)
            # Minimal quality guard
            if len(it.text) < 400 and len(it.summary) < 120:
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

        if not batch:
            outputs[section] = f"### {section} — {today_iso()}\n_No new high-quality items today._"
            continue

        md = call_openai(
            model=MODEL,
            api_key=OPENAI_API_KEY,
            system=SYSTEMS[section],
            user=section,
            items=batch,
            temp=TEMP
        )
        outputs[section] = md

        # mark seen
        for b in batch:
            seen.add(sha1(b["url"]))

    save_seen(seen)

    # write one file per section and a combined digest
    date_slug = today_iso()
    combined = [f"# Signals — {date_slug}\n"]
    for section, md in outputs.items():
        # individual
        p = OUTDIR / f"{section.lower().replace(' ', '-')}-{date_slug}.md"
        p.write_text(md)
        combined.append(f"\n\n## {section}\n\n{md}\n")

    (OUTDIR / f"signals-{date_slug}.md").write_text("\n".join(combined))

if __name__ == "__main__":
    main()
