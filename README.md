# GG Advisory — Auto Content

Daily GitHub Action that:
- collects items (RSS/HTML) for 5 sections,
- summarises with OpenAI into Markdown,
- writes to `/out/` and remembers seen URLs.

## 1) Configure
- Copy `.env.example` → `.env` (optional for local runs).
- Edit `config/sources.yaml` to add/remove feeds/pages.

## 2) Local run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export OPENAI_API_KEY=sk-...   # or use .env
python -m src.generate_posts
