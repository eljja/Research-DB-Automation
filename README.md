# Research DB Automation Console

Lightweight local research automation server for SerpApi sensing, OpenAlex abstract fetch, Gemini-based LLM extraction, SQLite storage, and a web GUI.

## Stack

- Backend: Flask + APScheduler + SQLite
- Data sources: SerpApi, OpenAlex
- LLM: Google Gemini
- Frontend: static HTML/CSS/JavaScript

## Quick Start

```bash
bash setup.sh
./venv/bin/python app.py
```

Open:

```text
http://127.0.0.1:5000
```

## Manual Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python app.py
```

## Environment Variables

Set these in `.env`:

- `SERPAPI_KEY`
- `GEMINI_API_KEY`
- `GEMINI_MODEL`
- `OPENALEX_API_KEY`

Default model:

```text
gemini-3.1-flash-lite-preview
```

## Notes

- Local runtime data is stored in `research.db`.
- `.env`, `research.db`, logs, cache files, and `venv/` are excluded from Git via `.gitignore`.
- Before pushing to a public GitHub repository, keep `.env` private and do not commit live API keys.

## GitHub Upload

This folder was not originally a Git repository. To prepare it for GitHub:

```bash
git init
git add .
git commit -m "Initial research automation console"
git branch -M main
git remote add origin <your-repo-url>
git push -u origin main
```
