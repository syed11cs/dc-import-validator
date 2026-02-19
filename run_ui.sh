#!/bin/bash
# Start the DC Import Validator Web UI server.
# Usage: ./run_ui.sh [port]
# Default port: 8000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-8000}"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"
VENV_PIP="$SCRIPT_DIR/.venv/bin/pip"
VENV_UVICORN="$SCRIPT_DIR/.venv/bin/uvicorn"

cd "$SCRIPT_DIR"

# Use project venv if available; otherwise system python/pip/uvicorn
if [[ -f "$VENV_PYTHON" ]]; then
  PYTHON="$VENV_PYTHON"
  PIP="${VENV_PIP:-$VENV_PYTHON -m pip}"
  UVICORN="${VENV_UVICORN:-$VENV_PYTHON -m uvicorn}"
else
  PYTHON="python3"
  PIP="python3 -m pip"
  UVICORN="python3 -m uvicorn"
fi

# Ensure UI dependencies are available (uvicorn, python-multipart for file uploads)
if ! "$PYTHON" -c "import uvicorn" 2>/dev/null || ! "$PYTHON" -c "import python_multipart" 2>/dev/null; then
  echo "Installing UI dependencies..."
  "$PIP" install -r ui/requirements.txt
fi

echo "Starting DC Import Validator at http://localhost:$PORT"
"$UVICORN" ui.server:app --reload --host 0.0.0.0 --port "$PORT"
