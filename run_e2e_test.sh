#!/bin/bash
#
# End-to-end validation test script for DC Import Validator.
# Orchestrates: optional dc-import lint (with stat_vars/schema MCFs when present)
#              -> dc-import genmcf -> import_validation
#
# When a dataset has stat_vars.mcf and/or stat_vars_schema.mcf, lint is run
# with those MCFs first for schema conformance; that report is used by
# import_validation. Custom runs can pass --stat-vars-mcf and --stat-vars-schema-mcf.
#
# Usage:
#   ./run_e2e_test.sh [OPTIONS] [DATASET]
#
# Datasets (child_birth from this repo's sample_data/child_birth/; rule-test variants in sample_data/):
#   child_birth              - Child birth (in-repo, clean; expect PASS)
#   child_birth_fail_min_value   - Same base, one negative value → check_min_value FAIL
#   child_birth_fail_units       - Same base, mixed units → check_unit_consistency FAIL
#   child_birth_fail_scaling_factor - Same base, inconsistent scaling → check_scaling_factor_consistency FAIL
#   child_birth_ai_demo      - TMCF with schema issues & typos → Gemini Review finds issues
#   child_birth_over_1000    - 1001 data rows → row-count check fails in Pre-Import Checks
#   custom                  - Your own TMCF + CSV (use --tmcf and --csv)
#
# Options:
#   --tmcf PATH       Path to TMCF file (for custom dataset)
#   --csv PATH        Path to CSV file (for custom dataset)
#   --stat-vars-mcf PATH       Optional stat vars MCF (for schema conformance; custom or when dataset has it)
#   --stat-vars-schema-mcf PATH  Optional stat vars schema MCF (for schema conformance)
#   --config PATH     Use custom validation config
#   --rules ID1,ID2   Run only these rules (comma-separated)
#   --skip-rules ID1  Skip these rules (comma-separated)
#   --llm-review      Run Gemini review (schema/typo) on TMCF before validation (requires GEMINI_API_KEY). Default: on.
#   --no-llm-review   Disable Gemini review for this run.
#   --ai-advisory     If Gemini review finds issues, do not stop — continue pipeline (treat Gemini blockers as non-blocking)
#   --model ID        Gemini model for Gemini review (default: gemini-2.5-flash)
#   --help            Show this help
#
# Examples:
#   ./run_e2e_test.sh child_birth
#   ./run_e2e_test.sh child_birth --rules=check_min_value,check_unit_consistency
#   ./run_e2e_test.sh child_birth --skip-rules=check_lint_error_count
#

set -e

# --- Paths (relative to script location) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_REPO="${DATA_REPO:-$PROJECTS_DIR/datacommonsorg/data}"
# Import tool behavior: passed to Java process (env). Defaults support deterministic/local runs.
export IMPORT_RESOLUTION_MODE="${IMPORT_RESOLUTION_MODE:-LOCAL}"
export IMPORT_EXISTENCE_CHECKS="${IMPORT_EXISTENCE_CHECKS:-true}"
BIN_DIR="$SCRIPT_DIR/bin"
OUTPUT_DIR="$SCRIPT_DIR/output"
IMPORT_JAR_URL="https://github.com/datacommonsorg/import/releases/download/v0.3.0/datacommons-import-tool-0.3.0-jar-with-dependencies.jar"
CONFIG_DIR="$SCRIPT_DIR/validation_configs"

# --- Defaults ---
DATASET=""
VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
CONFIG_OVERRIDE=""
RULES_FILTER=""
SKIP_RULES_FILTER=""
CUSTOM_TMCF=""
CUSTOM_CSV=""
CUSTOM_STAT_VARS_MCF=""
CUSTOM_STAT_VARS_SCHEMA_MCF=""
LLM_REVIEW=true
LLM_MODEL="gemini-2.5-flash"
AI_ADVISORY=false

# --- Parse args ---
while [[ $# -gt 0 ]]; do
  case $1 in
    --tmcf)
      CUSTOM_TMCF="$2"
      shift 2
      ;;
    --tmcf=*)
      CUSTOM_TMCF="${1#*=}"
      shift
      ;;
    --csv)
      CUSTOM_CSV="$2"
      shift 2
      ;;
    --csv=*)
      CUSTOM_CSV="${1#*=}"
      shift
      ;;
    --stat-vars-mcf)
      CUSTOM_STAT_VARS_MCF="$2"
      shift 2
      ;;
    --stat-vars-mcf=*)
      CUSTOM_STAT_VARS_MCF="${1#*=}"
      shift
      ;;
    --stat-vars-schema-mcf)
      CUSTOM_STAT_VARS_SCHEMA_MCF="$2"
      shift 2
      ;;
    --stat-vars-schema-mcf=*)
      CUSTOM_STAT_VARS_SCHEMA_MCF="${1#*=}"
      shift
      ;;
    --config)
      VALIDATION_CONFIG="$2"
      CONFIG_OVERRIDE=1
      shift 2
      ;;
    --config=*)
      VALIDATION_CONFIG="${1#*=}"
      CONFIG_OVERRIDE=1
      shift
      ;;
    --rules)
      RULES_FILTER="$2"
      shift 2
      ;;
    --rules=*)
      RULES_FILTER="${1#*=}"
      shift
      ;;
    --skip-rules)
      SKIP_RULES_FILTER="$2"
      shift 2
      ;;
    --skip-rules=*)
      SKIP_RULES_FILTER="${1#*=}"
      shift
      ;;
    --llm-review)
      LLM_REVIEW=true
      shift
      ;;
    --no-llm-review)
      LLM_REVIEW=false
      shift
      ;;
    --ai-advisory)
      AI_ADVISORY=true
      shift
      ;;
    --model)
      LLM_MODEL="$2"
      shift 2
      ;;
    --model=*)
      LLM_MODEL="${1#*=}"
      shift
      ;;
    --help|-h)
      head -28 "$0" | tail -23
      exit 0
      ;;
    child_birth|child_birth_fail_min_value|child_birth_fail_units|child_birth_fail_scaling_factor|child_birth_ai_demo|child_birth_over_1000|custom)
      DATASET="$1"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Default dataset if not specified
if [[ -z "$DATASET" ]]; then
  if [[ -n "$CUSTOM_TMCF" && -n "$CUSTOM_CSV" ]]; then
    DATASET="custom"
  else
    DATASET="child_birth"
  fi
fi

# --- Colors for output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Session ID for CLI logging (correlate logs when run independently of the web server)
SESSION_ID=$( (uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null) | tr -d '-' | cut -c1-12)
[[ -z "$SESSION_ID" ]] && SESSION_ID="$(date +%s)$$"
export SESSION_ID

log_info()  { echo -e "${GREEN}[INFO]${NC} [session=$SESSION_ID] $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} [session=$SESSION_ID] $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} [session=$SESSION_ID] $1"; }

# Emit structured failure event for UI (single line JSON). Runner forwards it; done payload uses it instead of parsing output.
emit_failure() {
  local code=$1 step=$2 msg=$3 limit=${4:-}
  if [[ -n "$limit" && "$limit" != "null" ]]; then
    echo "{\"t\":\"failure\",\"code\":\"$code\",\"step\":$step,\"message\":\"$msg\",\"limit\":$limit}"
  else
    echo "{\"t\":\"failure\",\"code\":\"$code\",\"step\":$step,\"message\":\"$msg\"}"
  fi
}

# Ensure validation_output.json and validation_report.html exist before exiting with failure (so GCS upload and /report/... work on Cloud Run).
ensure_failure_report() {
  [[ -z "$DATASET_OUTPUT" || -z "$DATASET" ]] && return
  local py="${PYTHON:-python3}"
  mkdir -p "$DATASET_OUTPUT"
  if [[ ! -f "$DATASET_OUTPUT/validation_output.json" ]]; then
    echo '[]' > "$DATASET_OUTPUT/validation_output.json"
  fi
  local ai_arg=""
  [[ "${LLM_REVIEW:-}" == "true" ]] && ai_arg="--ai-review-enabled"
  $py "$SCRIPT_DIR/scripts/generate_html_report.py" "$DATASET_OUTPUT/validation_output.json" "$DATASET_OUTPUT/validation_report.html" --dataset="$DATASET" --overall=fail $ai_arg 2>/dev/null || true
}

# --- Validation ---
if [[ ! -d "$DATA_REPO" ]]; then
  log_error "Data repo not found at $DATA_REPO"
  exit 1
fi

# Ensure Python env and JAR are ready. Skip setup when system Python already has deps (e.g. Docker/Cloud Run).
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"
SYS_PYTHON="python3"
JAR_PATH="${IMPORT_JAR_PATH:-$BIN_DIR/datacommons-import-tool.jar}"
if [[ -f "$VENV_PYTHON" ]] && "$VENV_PYTHON" -c "import absl, pandas, duckdb, omegaconf" 2>/dev/null; then
  : # venv ready
elif [[ -f "$JAR_PATH" ]] && "$SYS_PYTHON" -c "import absl, pandas, duckdb, omegaconf" 2>/dev/null; then
  log_info "Using system Python (pre-installed environment)."
else
  log_info "Python environment not ready. Running setup..."
  "$SCRIPT_DIR/setup.sh" || {
    log_error "Setup failed. Run ./setup.sh manually."
    exit 1
  }
fi

mkdir -p "$OUTPUT_DIR"

# =============================================================================
# Step 0: Dataset-specific paths (child_birth testdata in repo; rule-test variants in sample_data/)
# =============================================================================
# Child birth testdata lives in this repo so we don't depend on data repo for sample inputs
log_info "Starting run (dataset=$DATASET)"
dc_key_status="missing"
[[ -n "${DC_API_KEY:-}" ]] && dc_key_status="present"
log_info "Resolution Mode: $IMPORT_RESOLUTION_MODE | Existence Checks: $IMPORT_EXISTENCE_CHECKS | DC_API_KEY: $dc_key_status"
CB="$SCRIPT_DIR/sample_data/child_birth"
if [[ "$DATASET" == "child_birth" ]]; then
  TMCF="$CB/child_birth.tmcf"
  CSV="$CB/child_birth.csv"
  GENMCF_OUTPUT="$OUTPUT_DIR/child_birth_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  # No stat vars MCF: keep Child Birth as one clean success dataset (no AI Review stat_var warnings)
  STAT_VARS_MCF=""
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using child_birth (in-repo sample_data/child_birth, clean)"
elif [[ "$DATASET" == "child_birth_fail_min_value" ]]; then
  TMCF="$CB/child_birth.tmcf"
  CSV="$SCRIPT_DIR/sample_data/child_birth_fail_min_value/child_birth_fail_min_value.csv"
  GENMCF_OUTPUT="$OUTPUT_DIR/child_birth_fail_min_value_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="$CB/child_birth_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using child_birth_fail_min_value (one negative value → check_min_value FAIL)"
elif [[ "$DATASET" == "child_birth_fail_units" ]]; then
  TMCF="$SCRIPT_DIR/sample_data/child_birth_fail_units/child_birth_fail_units.tmcf"
  CSV="$SCRIPT_DIR/sample_data/child_birth_fail_units/child_birth_fail_units.csv"
  GENMCF_OUTPUT="$OUTPUT_DIR/child_birth_fail_units_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="$CB/child_birth_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using child_birth_fail_units (mixed units → check_unit_consistency FAIL)"
elif [[ "$DATASET" == "child_birth_fail_scaling_factor" ]]; then
  TMCF="$SCRIPT_DIR/sample_data/child_birth_fail_scaling_factor/child_birth_fail_scaling_factor.tmcf"
  CSV="$SCRIPT_DIR/sample_data/child_birth_fail_scaling_factor/child_birth_fail_scaling_factor.csv"
  GENMCF_OUTPUT="$OUTPUT_DIR/child_birth_fail_scaling_factor_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="$CB/child_birth_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using child_birth_fail_scaling_factor (inconsistent scaling → check_scaling_factor_consistency FAIL)"
elif [[ "$DATASET" == "child_birth_ai_demo" ]]; then
  TMCF="$SCRIPT_DIR/sample_data/child_birth_ai_demo/child_birth_ai_demo.tmcf"
  CSV="$SCRIPT_DIR/sample_data/child_birth_ai_demo/child_birth_ai_demo.csv"
  GENMCF_OUTPUT="$OUTPUT_DIR/child_birth_ai_demo_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="$CB/child_birth_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using child_birth_ai_demo (TMCF with schema issues & typos → Gemini Review finds issues)"
elif [[ "$DATASET" == "child_birth_over_1000" ]]; then
  TMCF="$SCRIPT_DIR/sample_data/child_birth_over_1000/child_birth_over_1000.tmcf"
  CSV="$SCRIPT_DIR/sample_data/child_birth_over_1000/child_birth_over_1000.csv"
  GENMCF_OUTPUT="$OUTPUT_DIR/child_birth_over_1000_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="$CB/child_birth_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using child_birth_over_1000 (1001 rows → check_csv_row_count FAIL)"
elif [[ "$DATASET" == "custom" ]]; then
  if [[ -z "$CUSTOM_TMCF" || -z "$CUSTOM_CSV" ]]; then
    log_error "Custom dataset requires --tmcf and --csv"
    echo "Example: ./run_e2e_test.sh --tmcf=path/to/file.tmcf --csv=path/to/file.csv"
    exit 1
  fi
  TMCF="$CUSTOM_TMCF"
  CSV="$CUSTOM_CSV"
  # CLI: fixed output dir so "latest" and docs align; Web UI sets RUN_ID so output goes to output/custom/{run_id}/
  GENMCF_OUTPUT="$OUTPUT_DIR/custom_input"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="${CUSTOM_STAT_VARS_MCF:-}"
  STAT_VARS_SCHEMA_MCF="${CUSTOM_STAT_VARS_SCHEMA_MCF:-}"
  DIFFER_OUTPUT=""  # No differ output for new imports
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using custom data: TMCF=$TMCF, CSV=$CSV"
else
  log_error "Unknown dataset: $DATASET"
  echo "Use: child_birth, child_birth_fail_min_value, child_birth_fail_units, child_birth_fail_scaling_factor, child_birth_ai_demo, child_birth_over_1000, or custom (with --tmcf and --csv)"
  exit 1
fi

# Per-run output dir when RUN_ID is set (e.g. by UI) to avoid concurrent-run overwrites
if [[ -n "${RUN_ID:-}" ]]; then
  GENMCF_OUTPUT="$OUTPUT_DIR/$DATASET/$RUN_ID"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  mkdir -p "$GENMCF_OUTPUT"
  log_info "Using per-run output: $GENMCF_OUTPUT"
fi

# --- Resolve PYTHON for filter script ---
if [[ -z "$PYTHON" ]]; then
  if [[ -f "$SCRIPT_DIR/.venv/bin/python" ]]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python"
  else
    PYTHON="python3"
  fi
fi

# --- Apply --rules or --skip-rules filter (creates temp config) ---
if [[ -n "$RULES_FILTER" || -n "$SKIP_RULES_FILTER" ]]; then
  if [[ -n "$RULES_FILTER" && -n "$SKIP_RULES_FILTER" ]]; then
    log_error "Use --rules OR --skip-rules, not both"
    exit 1
  fi
  FILTER_SCRIPT="$SCRIPT_DIR/scripts/filter_validation_config.py"
  if [[ ! -f "$FILTER_SCRIPT" ]]; then
    log_error "filter_validation_config.py not found"
    exit 1
  fi
  FILTER_ARGS="--config=$VALIDATION_CONFIG"
  if [[ -n "$RULES_FILTER" ]]; then
    FILTER_ARGS="$FILTER_ARGS --rules=$RULES_FILTER"
  else
    FILTER_ARGS="$FILTER_ARGS --skip-rules=$SKIP_RULES_FILTER"
  fi
  VALIDATION_CONFIG=$($PYTHON "$FILTER_SCRIPT" $FILTER_ARGS) || exit 1
  CONFIG_OVERRIDE=1
  log_info "Using filtered config: $(echo "$VALIDATION_CONFIG" | tr '\n' ' ')"
fi

# =============================================================================
# Step 0: Pre-Import Checks (preflight + CSV quality + row count)
# =============================================================================
if [[ -z "$PYTHON" ]]; then
  if [[ -f "$SCRIPT_DIR/.venv/bin/python" ]]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python"
  else
    PYTHON="python3"
  fi
fi
echo "::STEP::0:Pre-Import Checks"
log_info "Pre-Import Checks (files + CSV quality + row count)..."
VALIDATE_FILES_SCRIPT="$SCRIPT_DIR/scripts/validate_import_files.py"
if [[ -f "$VALIDATE_FILES_SCRIPT" && -n "$TMCF" && -n "$CSV" ]]; then
  PREFLIGHT_ARGS=(--tmcf="$TMCF" --csv="$CSV")
  [[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] && PREFLIGHT_ARGS+=(--stat-vars-mcf="$STAT_VARS_MCF")
  [[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]] && PREFLIGHT_ARGS+=(--stat-vars-schema-mcf="$STAT_VARS_SCHEMA_MCF")
  if ! $PYTHON "$VALIDATE_FILES_SCRIPT" "${PREFLIGHT_ARGS[@]}" 2>/dev/null; then
    log_error "Preflight failed: required import files missing or wrong extension."
    emit_failure "PREFLIGHT_FAILED" 0 "Preflight failed"
    $PYTHON "$VALIDATE_FILES_SCRIPT" "${PREFLIGHT_ARGS[@]}" || true
    ensure_failure_report
    exit 1
  fi
fi

VALIDATE_CSV_SCRIPT="$SCRIPT_DIR/scripts/validate_csv_quality.py"
if [[ -f "$VALIDATE_CSV_SCRIPT" && -n "$CSV" && -f "$CSV" ]]; then
  if ! $PYTHON "$VALIDATE_CSV_SCRIPT" --csv="$CSV" --value-column=value 2>/dev/null; then
    log_error "CSV quality check failed."
    emit_failure "CSV_QUALITY_FAILED" 0 "CSV quality check failed"
    $PYTHON "$VALIDATE_CSV_SCRIPT" --csv="$CSV" --value-column=value || true
    ensure_failure_report
    exit 1
  fi
fi

# CSV row count (pre-import constraint; only needs CSV)
ROW_COUNT_SCRIPT="$SCRIPT_DIR/scripts/check_csv_row_count.py"
ROW_COUNT_RESULT="$DATASET_OUTPUT/row_count_result.json"
if [[ -f "$ROW_COUNT_SCRIPT" && -n "$CSV" && -f "$CSV" ]]; then
  mkdir -p "$DATASET_OUTPUT"
  if $PYTHON "$ROW_COUNT_SCRIPT" --csv="$CSV" --threshold=1000 --output="$ROW_COUNT_RESULT"; then
    if [[ -f "$ROW_COUNT_RESULT" ]] && grep -q '"status": "FAILED"' "$ROW_COUNT_RESULT" 2>/dev/null; then
      WARN_ONLY_JSON="${CONFIG_DIR}/warn_only_rules.json"
      if [[ -f "$WARN_ONLY_JSON" ]]; then
        if ! $PYTHON -c "
import json, sys
with open('$WARN_ONLY_JSON') as f: d = json.load(f)
rules = d.get('$DATASET', [])
sys.exit(0 if 'check_csv_row_count' in rules else 1)
" 2>/dev/null; then
          log_error "CSV row count exceeds 1000. Pre-Import Checks failed."
          emit_failure "ROW_COUNT_EXCEEDED" 0 "CSV row count exceeds limit (1000 rows max)" 1000
          ensure_failure_report
          exit 1
        fi
      else
        log_error "CSV row count exceeds 1000. Pre-Import Checks failed."
        emit_failure "ROW_COUNT_EXCEEDED" 0 "CSV row count exceeds limit (1000 rows max)" 1000
        ensure_failure_report
        exit 1
      fi
    fi
  else
    log_warn "Row count check script failed or skipped"
  fi
fi

# =============================================================================
# Step 1: Schema review (deterministic checks always; LLM only when --llm-review)
# =============================================================================
if [[ -n "$TMCF" && -f "$TMCF" ]]; then
  LLM_REVIEW_SCRIPT="$SCRIPT_DIR/scripts/llm_schema_review.py"
  SCHEMA_REVIEW_OUT="$DATASET_OUTPUT/schema_review.json"
  mkdir -p "$DATASET_OUTPUT"
  if [[ -f "$LLM_REVIEW_SCRIPT" ]]; then
    STEP1_START=$(date +%s)
    LLM_EXTRA_ARGS=()
    [[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] && LLM_EXTRA_ARGS+=(--stat-vars-mcf="$STAT_VARS_MCF")
    [[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]] && LLM_EXTRA_ARGS+=(--stat-vars-schema-mcf="$STAT_VARS_SCHEMA_MCF")
    [[ -n "$CSV" && -f "$CSV" ]] && LLM_EXTRA_ARGS+=(--csv="$CSV")
    [[ "$LLM_REVIEW" == "true" ]] && LLM_EXTRA_ARGS+=(--llm-review)
    echo "::STEP::1:Gemini Review"
    if [[ "$LLM_REVIEW" == "true" ]]; then
      log_info "Step 1: Running schema review + Gemini review (model: $LLM_MODEL)..."
    else
      log_info "Step 1: Running schema review (deterministic checks only)..."
    fi
    if $PYTHON "$LLM_REVIEW_SCRIPT" --tmcf="$TMCF" --output="$SCHEMA_REVIEW_OUT" --model="$LLM_MODEL" "${LLM_EXTRA_ARGS[@]}"; then
      log_info "Step 1 passed (no blocking issues)"
    else
      if [[ -f "$SCHEMA_REVIEW_OUT" ]]; then
        log_error "Step 1 found blocking issues. See $SCHEMA_REVIEW_OUT"
        $PYTHON -c "import json; d=json.load(open('$SCHEMA_REVIEW_OUT')); print('\n'.join(str(x) for x in d))" 2>/dev/null || cat "$SCHEMA_REVIEW_OUT"
        if [[ "$LLM_REVIEW" == "true" && "$AI_ADVISORY" == "true" ]]; then
          log_info "Advisory mode: treating AI blockers as non-blocking — continuing pipeline."
        else
          ensure_failure_report
          emit_failure "GEMINI_BLOCKING" 1 "Gemini review found issues"
          exit 1
        fi
      else
        log_warn "Step 1 failed (script error or missing output)"
        ensure_failure_report
        emit_failure "GEMINI_BLOCKING" 1 "Gemini review found issues"
        exit 1
      fi
    fi
    log_info "Step 1 completed in $(( $(date +%s) - STEP1_START ))s"
  else
    log_warn "Schema review script not found: $LLM_REVIEW_SCRIPT"
  fi
fi

# =============================================================================
# Step 2: Run dc-import genmcf
# =============================================================================
STEP2_START=$(date +%s)
echo "::STEP::2:DC Import Tool"
log_info "Step 2: Running dc-import genmcf..."

# Resolve JAR: IMPORT_JAR_PATH -> bin/ -> download from GitHub
if [[ -n "$IMPORT_JAR_PATH" && -f "$IMPORT_JAR_PATH" ]]; then
  JAR_PATH="$IMPORT_JAR_PATH"
  log_info "Using import JAR: $JAR_PATH"
elif [[ -f "$BIN_DIR/datacommons-import-tool.jar" ]]; then
  JAR_PATH="$BIN_DIR/datacommons-import-tool.jar"
  log_info "Using import JAR: $JAR_PATH (from bin/)"
else
  log_info "Downloading import tool JAR from GitHub releases..."
  mkdir -p "$BIN_DIR"
  if curl -sL -o "$BIN_DIR/datacommons-import-tool.jar" "$IMPORT_JAR_URL" 2>/dev/null && [[ -f "$BIN_DIR/datacommons-import-tool.jar" ]]; then
    JAR_PATH="$BIN_DIR/datacommons-import-tool.jar"
    log_info "Downloaded: $JAR_PATH"
  else
    log_error "Import tool JAR not found. Run ./setup.sh first, or set IMPORT_JAR_PATH."
    log_error "Download URL: $IMPORT_JAR_URL"
    ensure_failure_report
    exit 1
  fi
fi

if [[ ! -f "$TMCF" || ! -f "$CSV" ]]; then
  log_error "Input files not found: TMCF=$TMCF, CSV=$CSV"
  ensure_failure_report
  exit 1
fi

mkdir -p "$GENMCF_OUTPUT"

# Optional: run lint with stat_vars / stat_vars_schema MCFs when present (schema conformance)
LINT_WITH_MCF_OUTPUT="$GENMCF_OUTPUT/lint"
if [[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] || [[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]]; then
  log_info "Running dc-import lint with schema MCF(s) for conformance check..."
  LINT_FILES=("$TMCF" "$CSV")
  [[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] && LINT_FILES+=("$STAT_VARS_MCF")
  [[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]] && LINT_FILES+=("$STAT_VARS_SCHEMA_MCF")
  if java -jar "$JAR_PATH" lint "${LINT_FILES[@]}" -o="$LINT_WITH_MCF_OUTPUT" \
      --resolution="$IMPORT_RESOLUTION_MODE" --existence-checks="$IMPORT_EXISTENCE_CHECKS" 2>/dev/null; then
    LINT_REPORT="$LINT_WITH_MCF_OUTPUT/report.json"
    if [[ -f "$LINT_REPORT" ]]; then
      log_info "Using lint report from schema MCF run: $LINT_REPORT"
    fi
  else
    log_warn "Lint with MCFs failed or produced no report; import_validation will use genmcf report.json"
  fi
fi

# genmcf: same inputs as DE when schema MCFs exist (TMCF, CSV, optional stat_vars.mcf, stat_vars_schema.mcf)
GENMCF_FILES=("$TMCF" "$CSV")
[[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] && GENMCF_FILES+=("$STAT_VARS_MCF")
[[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]] && GENMCF_FILES+=("$STAT_VARS_SCHEMA_MCF")
java -jar "$JAR_PATH" genmcf "${GENMCF_FILES[@]}" -o="$GENMCF_OUTPUT" \
  --resolution="$IMPORT_RESOLUTION_MODE" --existence-checks="$IMPORT_EXISTENCE_CHECKS" || {
  log_error "dc-import genmcf failed"
  emit_failure "DATA_PROCESSING_FAILED" 2 "Data processing failed"
  ensure_failure_report
  exit 1
}

if [[ ! -f "$STATS_SUMMARY" ]]; then
  log_error "summary_report.csv not produced at $STATS_SUMMARY"
  ensure_failure_report
  exit 1
fi
log_info "Generated: $STATS_SUMMARY, report.json"
log_info "Step 2 completed in $(( $(date +%s) - STEP2_START ))s"

# =============================================================================
# Step 3: Run import_validation
# =============================================================================
# Validate config template (structure and required keys)
STEP3_START=$(date +%s)
VALIDATE_CONFIG_SCRIPT="$SCRIPT_DIR/scripts/validate_config_template.py"
if [[ -f "$VALIDATE_CONFIG_SCRIPT" && -f "$VALIDATION_CONFIG" ]]; then
  if ! $PYTHON "$VALIDATE_CONFIG_SCRIPT" "$VALIDATION_CONFIG" 2>/dev/null; then
    log_error "Validation config failed template check. Run: $PYTHON $VALIDATE_CONFIG_SCRIPT $VALIDATION_CONFIG"
    $PYTHON "$VALIDATE_CONFIG_SCRIPT" "$VALIDATION_CONFIG" || true
    ensure_failure_report
    exit 1
  fi
fi

echo "::STEP::3:DC Import Validation"
log_info "Step 3: Running import_validation (config: $(basename "$VALIDATION_CONFIG"))..."

# Validation output goes inside dataset folder for consistency
mkdir -p "$DATASET_OUTPUT"
VALIDATION_OUTPUT="$DATASET_OUTPUT/validation_output.json"

# Build validation args - stats_summary, lint_report, differ_output may be optional per config
VALIDATION_ARGS=(
  --validation_config="$VALIDATION_CONFIG"
  --validation_output="$VALIDATION_OUTPUT"
)

if [[ -n "$STATS_SUMMARY" && -f "$STATS_SUMMARY" ]]; then
  VALIDATION_ARGS+=(--stats_summary="$STATS_SUMMARY")
fi

if [[ -n "$LINT_REPORT" && -f "$LINT_REPORT" ]]; then
  VALIDATION_ARGS+=(--lint_report="$LINT_REPORT")
fi

# differ_output is optional (not available for new imports)
# Use empty_differ.csv when no differ data - avoids DuckDB error with empty DataFrame. Override with EMPTY_DIFFER_PATH.
EMPTY_DIFFER="${EMPTY_DIFFER_PATH:-$SCRIPT_DIR/sample_data/empty_differ.csv}"
if [[ -n "$DIFFER_OUTPUT" && -f "$DIFFER_OUTPUT" ]]; then
  VALIDATION_ARGS+=(--differ_output="$DIFFER_OUTPUT")
elif [[ -f "$EMPTY_DIFFER" ]]; then
  VALIDATION_ARGS+=(--differ_output="$EMPTY_DIFFER")
else
  VALIDATION_ARGS+=(--differ_output=)
fi

cd "$DATA_REPO"
# Use project venv by default (self-contained), else PYTHON env var, else python3
if [[ -z "$PYTHON" ]]; then
  if [[ -f "$SCRIPT_DIR/.venv/bin/python" ]]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python"
  else
    PYTHON="python3"
  fi
fi
if $PYTHON -m tools.import_validation.runner "${VALIDATION_ARGS[@]}"; then
  RUNNER_EXIT=0
else
  RUNNER_EXIT=1
fi

# =============================================================================
# Step 2.2: Merge row-count result (computed in Pre-Import Checks) into validation_output
# =============================================================================
if [[ -f "$VALIDATION_OUTPUT" && -f "$ROW_COUNT_RESULT" ]]; then
  if $PYTHON -c "
import json
vo_path = '$VALIDATION_OUTPUT'
rc_path = '$ROW_COUNT_RESULT'
with open(vo_path) as f: vo = json.load(f)
with open(rc_path) as f: rc = json.load(f)
if isinstance(vo, list) and isinstance(rc, list) and rc:
  vo.extend(rc)
  with open(vo_path, 'w') as f: json.dump(vo, f, indent=2, default=str)
" 2>/dev/null; then
    :
  else
    log_warn "Failed to merge row_count_result.json into validation_output.json"
  fi
fi

# =============================================================================
# Step 2.25: Check counters match (StatVars/NumObservations vs report)
# =============================================================================
COUNTERS_CHECK_EXIT=0
if [[ -n "$STATS_SUMMARY" && -f "$STATS_SUMMARY" && -n "$LINT_REPORT" && -f "$LINT_REPORT" ]]; then
  if $PYTHON "$SCRIPT_DIR/scripts/check_counters_match.py" \
    --stats_summary="$STATS_SUMMARY" --report="$LINT_REPORT"; then
    :
  else
    COUNTERS_CHECK_EXIT=1
  fi
fi

# =============================================================================
# Step 2.5: Apply warn_only overrides (convert FAILED -> WARNING for non-blocking rules)
# Pass/fail: only Errors (FAILED) block; Warnings do not
# =============================================================================
WARN_ONLY_RULES="$CONFIG_DIR/warn_only_rules.json"
if [[ -f "$VALIDATION_OUTPUT" && -f "$WARN_ONLY_RULES" ]]; then
  log_info "Applying warn_only overrides..."
  if $PYTHON "$SCRIPT_DIR/scripts/apply_warn_only.py" "$VALIDATION_OUTPUT" \
    --warn_only_rules="$WARN_ONLY_RULES" --dataset="$DATASET" --check_blockers; then
    VALIDATION_RESULT=0
  else
    VALIDATION_RESULT=1
  fi
else
  VALIDATION_RESULT=$RUNNER_EXIT
fi
# Counters check failure also blocks
if [[ "$COUNTERS_CHECK_EXIT" -ne 0 ]]; then
  VALIDATION_RESULT=1
fi
log_info "Step 3 completed in $(( $(date +%s) - STEP3_START ))s"

# =============================================================================
# Step 4: Generate HTML report (pass overall result so report shows FAIL when run failed)
# =============================================================================
STEP4_START=$(date +%s)
HTML_REPORT="$DATASET_OUTPUT/validation_report.html"
if [[ -f "$VALIDATION_OUTPUT" ]]; then
  echo "::STEP::4:Results"
  log_info "Step 4: Generating HTML report..."
  OVERALL_ARG="--overall=pass"
  [[ "$VALIDATION_RESULT" -ne 0 ]] && OVERALL_ARG="--overall=fail"
  AI_REVIEW_ARG=""
  [[ "$LLM_REVIEW" == "true" ]] && AI_REVIEW_ARG="--ai-review-enabled"
  if $PYTHON "$SCRIPT_DIR/scripts/generate_html_report.py" "$VALIDATION_OUTPUT" "$HTML_REPORT" --dataset="$DATASET" $OVERALL_ARG $AI_REVIEW_ARG; then
    log_info "HTML report: $HTML_REPORT"
  fi
  log_info "Step 4 completed in $(( $(date +%s) - STEP4_START ))s"
fi

if [[ "$VALIDATION_RESULT" -eq 0 ]]; then
  log_info "Validation PASSED"
  echo ""
  echo "=========================================="
  echo -e "  ${GREEN}✓ Validation PASSED${NC}"
  echo "=========================================="
  echo "Output: $VALIDATION_OUTPUT"
  [[ -f "$HTML_REPORT" ]] && echo "Report: $HTML_REPORT"
  exit 0
else
  log_error "Validation FAILED"
  echo ""
  echo "=========================================="
  echo -e "  ${RED}✗ Validation FAILED${NC}"
  echo "=========================================="
  echo "Output: $VALIDATION_OUTPUT"
  [[ -f "$HTML_REPORT" ]] && echo "Report: $HTML_REPORT"
  exit 1
fi
