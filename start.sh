#!/usr/bin/env bash
# ── StockUpside.io startup script ─────────────────────────────────────────────
set -e

echo ""
echo "  ▲  StockUpside.io"
echo "  ─────────────────────────────"

# Check Python
if ! command -v python3 &>/dev/null; then
  echo "  ✗  Python 3 not found. Please install Python 3.9+."
  exit 1
fi

# Install Python deps if needed
echo "  →  Checking Python dependencies..."
python3 -c "import flask" 2>/dev/null || pip3 install flask --quiet

# Build TypeScript if tsc available and src newer than public/main.js
if command -v tsc &>/dev/null; then
  if [ src/main.ts -nt public/main.js ] 2>/dev/null || [ ! -f public/main.js ]; then
    echo "  →  Compiling TypeScript..."
    tsc
    echo "  ✓  TypeScript compiled."
  fi
else
  echo "  !  tsc not found — using pre-compiled public/main.js"
fi

echo "  →  Starting server on http://localhost:5000"
echo ""
python3 server/app.py
