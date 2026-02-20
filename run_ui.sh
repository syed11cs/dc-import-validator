#!/bin/bash
# Start the DC Import Validator Web UI server.
# Usage: ./run_ui.sh [port]
# Default port: 8000

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-8000}"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"
VENV_PIP="$SCRIPT_DIR/.venv/bin/pip"
VENV_UVICORN="$SCRIPT_DIR/.venv/bin/uvicorn"

cd "$SCRIPT_DIR"

# Quick check that setup was run (helpful hint for new users)
if [[ ! -f "$SCRIPT_DIR/.venv/bin/python" ]] && [[ ! -f "$SCRIPT_DIR/bin/datacommons-import-tool.jar" ]]; then
  echo "ℹ️  First time? Run ./setup.sh to initialize the environment"
  echo "   (Continuing anyway with system Python)"
  echo ""
fi

# Use project venv if available; otherwise system python/pip/uvicorn
if [[ -f "$VENV_PYTHON" ]]; then
  PYTHON="$VENV_PYTHON"
  PIP="${VENV_PIP:-$VENV_PYTHON -m pip}"
  UVICORN="${VENV_UVICORN:-$VENV_PYTHON -m uvicorn}"
elif [[ -d "$SCRIPT_DIR/.venv" ]]; then
  echo "❌ Venv directory exists but Python not found. Run ./setup.sh to fix."
  exit 1
else
  PYTHON="python3"
  PIP="python3 -m pip"
  UVICORN="python3 -m uvicorn"
fi

# Check if port is available (macOS/Linux compatible)
if command -v lsof &>/dev/null; then
  if lsof -Pi :"$PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "❌ Port $PORT is already in use. Try:"
    echo "   ./run_ui.sh $((PORT + 1))"
    exit 1
  fi
fi

# Ensure UI dependencies are available (uvicorn, python-multipart for file uploads)
if ! "$PYTHON" -c "import uvicorn" 2>/dev/null || ! "$PYTHON" -c "import python_multipart" 2>/dev/null; then
  echo "📦 Installing UI dependencies..."
  "$PIP" install -q -r ui/requirements.txt
  echo "   ✓ Dependencies installed"
fi

echo ""
echo "🚀 Starting DC Import Validator"
echo "   Local URL: http://localhost:$PORT"
echo "   Stop with: Ctrl+C"
echo ""

# Trap Ctrl+C to show friendly message
trap 'echo ""; echo "👋 Shutting down..."; exit 0' INT

"$UVICORN" ui.server:app --reload --host 0.0.0.0 --port "$PORT"