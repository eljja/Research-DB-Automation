#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Check for pdftotext (poppler-utils) required for full-paper PDF extraction
if ! command -v pdftotext &>/dev/null; then
  echo "[setup] pdftotext not found. Installing poppler-utils..."
  if command -v apt-get &>/dev/null; then
    sudo apt-get install -y poppler-utils
  elif command -v brew &>/dev/null; then
    brew install poppler
  else
    echo "[setup] WARNING: Could not install poppler-utils automatically."
    echo "         Please install it manually (e.g. 'sudo apt-get install poppler-utils')."
    echo "         Full-paper PDF extraction will be skipped until pdftotext is available."
  fi
else
  echo "[setup] pdftotext found: $(command -v pdftotext)"
fi

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
