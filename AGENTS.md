# Repository Guidelines

## Project Structure & Module Organization
- `app.py`: Flask entry point, API routes, scheduler wiring, and async task triggers.
- `services.py`: external integrations and pipeline logic for SerpApi, OpenAlex, and Gemini.
- `database.py`: SQLite schema setup, migrations, indexes, and log helpers.
- `static/`: frontend assets served by Flask.
  - `index.html`: dashboard layout
  - `app.js`: UI state, API calls, chart/table rendering
  - `styles.css`: visual system and layout
- `deploy/research-db-automation.service`: user-level `systemd` service for auto-start.
- `setup.sh`, `requirements.txt`, `README.md`: local setup and run documentation.

## Build, Test, and Development Commands
- `bash setup.sh`: create `venv`, install Python dependencies, and prepare `.env`.
- `./venv/bin/python app.py`: run the web server locally on `http://127.0.0.1:5000`.
- `./venv/bin/python -m py_compile app.py services.py database.py`: quick Python syntax check.
- `node --check static/app.js`: validate frontend JavaScript syntax.
- `systemctl --user status research-db-automation.service`: inspect the installed auto-start service.

## Coding Style & Naming Conventions
- Use 4-space indentation in Python and keep functions focused and small.
- Prefer `snake_case` for Python functions, variables, database columns, and API payload keys.
- Keep frontend code in plain JavaScript/CSS; use clear DOM ids like `btn-llm`, `topic-select`.
- Avoid hardcoding secrets in source. Put runtime keys in `.env`; commit only `.env.example`.

## Testing Guidelines
- There is no full automated test suite yet. Validate changes with targeted checks:
  - Python: `py_compile`
  - JavaScript: `node --check`
  - Runtime: open the UI and verify `/api/health`
- When adding tests later, place them under `tests/` and name files `test_<feature>.py`.
- Prefer small integration-style tests around API routes and SQLite behavior.

## Commit & Pull Request Guidelines
- Follow the existing commit style: short imperative subjects such as `Add backend server` or `Add frontend logic`.
- Keep commits scoped to one change area when possible.
- PRs should include:
  - a short summary of behavior changes
  - setup or migration notes if schema/service files changed
  - screenshots for UI changes
  - manual verification steps

## Security & Configuration Tips
- Do not commit `.env`, `research.db`, or log files.
- If API providers or models change, update `services.py`, `.env.example`, and `README.md` together.
