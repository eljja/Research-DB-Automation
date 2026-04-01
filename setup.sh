#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt

if [ ! -f .env ]; then
  cp .env.example .env
  echo ".env created from .env.example. Fill in your API keys before running the server."
else
  echo ".env already exists. Keeping current local API keys."
fi

echo "Setup complete."
echo "Run the server with: ./venv/bin/python app.py"
