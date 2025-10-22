import os, json, yaml
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict
from datetime import datetime, timezone

from .fetch import fetch_rss, fetch_html_index, fetch_full_text, Item
from .summarise import call_openai, SYSTEMS
from .utils import sha1, normalize_whitespace, today_iso

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMP = float(os.getenv("TEMP", "0.25"))
ITEMS_PER_SECTION = int(os.getenv("ITEMS_PER_SECTION", "4"))
FRESH_DAYS = int(os.getenv("FRESH_DAYS", "45"))
MIN_TEXT_CHARS = int(os.getenv("MIN_TEXT_CHARS", "600"))

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
        print(f"[fetch] RSS: {r}")
        items.extend(fetch_rss(r))
    for h in html_urls:
        print(f"[fetch] HTML index: {h}")
        items.extend(fetch_html_index(h))
    items.sort(key=lambda x: x.published_ts, reverse=True)
    return items

def main():
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")

    cfg = yaml.safe_load(CFG.read_text())
    seen = load_seen()
    outputs = {}

    now_ts = datetime.now(timezone.utc).timestamp()
    fresh_cutoff = now_ts - FRESH_DAYS * 86400

    for section, sources in cfg["sections"].items():
        print(f"\n[section] {section}")
        pool = collect_items(sources)
        print(f"[pool] candidates: {len(pool)}")
        batch = []

        for it in pool:
            if it.published_ts and it.published_ts < fresh_cutoff:
                continue
            h = sha1(it.url)
            if h in seen:
                continue

            txt = fetch_full_text(it.url)
            it.text = normalize_whitespace(txt)

            # Minimal quality guard
            if len(it.text) < MIN_TEXT_CHARS and len(it.summary) < 160:
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

        print(f"[batch] selected: {len(batch)} (limit {ITEMS_PER_SECTION})")

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
        p = OUTDIR / f"{section.lower().replace(' ', '-')}-{date_slug}.md"
        p.write_text(md)
        combined.append(f"\n\n## {section}\n\n{md}\n")

    (OUTDIR / f"signals-{date_slug}.md").write_text("\n".join(combined))
    print(f"\n[done] wrote outputs for {date_slug} → {OUTDIR}")

if __name__ == "__main__":
    main()
