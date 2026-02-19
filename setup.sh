#!/bin/bash
#
# Setup script for DC Import Validator.
# Run once when cloning the repo or onboarding a new team member.
#
# This script:
# 1. Creates a Python venv and installs dependencies (absl, pandas, duckdb, omegaconf)
# 2. Ensures the import tool JAR is available (for demo and real datasets)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
VENV_DIR="$SCRIPT_DIR/.venv"
JAR_NAME="datacommons-import-tool.jar"

# Import tool release to download (if building fails)
IMPORT_RELEASE_VERSION="v0.3.0"
IMPORT_JAR_URL="https://github.com/datacommonsorg/import/releases/download/${IMPORT_RELEASE_VERSION}/datacommons-import-tool-0.3.0-jar-with-dependencies.jar"

echo "=== DC Import Validator - Setup ==="
echo ""

# --- 1. Python venv (self-contained, no external dependencies) ---
echo "1. Setting up Python environment..."

if [[ -f "$VENV_DIR/bin/python" ]]; then
  if "$VENV_DIR/bin/python" -c "import absl, pandas, duckdb, omegaconf" 2>/dev/null; then
    echo "   ✓ Venv already exists with required packages"
    if ! "$VENV_DIR/bin/python" -c "from google import genai" 2>/dev/null; then
      echo "   Installing optional LLM package (google-genai)..."
      "$VENV_DIR/bin/pip" install -q google-genai 2>/dev/null || echo "   (install failed; run: .venv/bin/pip install google-genai)"
    fi
  else
    echo "   Reinstalling packages..."
    "$VENV_DIR/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"
    echo "   ✓ Packages installed"
  fi
else
  echo "   Creating venv..."
  python3 -m venv "$VENV_DIR"
  "$VENV_DIR/bin/pip" install -q -r "$SCRIPT_DIR/requirements.txt"
  echo "   ✓ Venv created and packages installed"
fi

echo "   Python: $VENV_DIR/bin/python"
echo ""

# --- 2. Import tool JAR ---
echo "2. Checking for import tool JAR..."

JAR_PATH=""

# Check if user already set IMPORT_JAR_PATH
if [[ -n "$IMPORT_JAR_PATH" && -f "$IMPORT_JAR_PATH" ]]; then
  JAR_PATH="$IMPORT_JAR_PATH"
  echo "   Found: \$IMPORT_JAR_PATH=$IMPORT_JAR_PATH"
fi

# Check bin/ directory (where we download to)
if [[ -z "$JAR_PATH" && -f "$BIN_DIR/$JAR_NAME" ]]; then
  JAR_PATH="$BIN_DIR/$JAR_NAME"
  echo "   Found: $BIN_DIR/$JAR_NAME"
fi

# Download from GitHub releases if not found
if [[ -z "$JAR_PATH" ]]; then
  if command -v curl &>/dev/null; then
    echo "   Downloading from GitHub releases..."
    mkdir -p "$BIN_DIR"
    if curl -sL -o "$BIN_DIR/$JAR_NAME" "$IMPORT_JAR_URL" 2>/dev/null && [[ -f "$BIN_DIR/$JAR_NAME" ]]; then
      JAR_PATH="$BIN_DIR/$JAR_NAME"
      echo "   Downloaded: $BIN_DIR/$JAR_NAME"
    fi
  fi
fi

if [[ -n "$JAR_PATH" ]]; then
  echo "   ✓ Import tool JAR ready"
else
  echo "   ✗ Import tool JAR not found."
  echo ""
  echo "   Download manually from: https://github.com/datacommonsorg/import/releases"
  echo "   Save to: $BIN_DIR/$JAR_NAME"
fi

chmod +x "$SCRIPT_DIR/run_e2e_test.sh" "$SCRIPT_DIR/run_ui.sh" 2>/dev/null || true

echo ""
echo "=== Setup complete ==="
echo ""
echo "Run validation:"
echo "  ./run_e2e_test.sh child_birth   # Quick test"
echo ""
echo "Start Web UI:"
echo "  ./run_ui.sh"
echo ""
