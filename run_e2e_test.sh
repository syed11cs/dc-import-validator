#!/bin/bash
#
# End-to-end validation test script for DC Import Validator.
# Orchestrates: dc-import genmcf -> import_validation
#
# genmcf performs all validations that lint mode performs (per DC documentation),
# so lint is not run as a separate step. genmcf's report.json is used as the
# lint report for downstream validation rules (STRUCTURAL_LINT_ERROR_COUNT,
# MISSING_REFS_COUNT). Custom runs can pass --stat-vars-mcf and --stat-vars-schema-mcf.
#
# Usage:
#   ./run_e2e_test.sh [OPTIONS] [DATASET]
#
# Datasets (from this repo's sample_data/):
#   child_birth       - Child birth (sample_data/child_birth/)
#   statistics_poland - Statistics Poland (sample_data/statistics_poland/)
#   finland_census    - Finland census (sample_data/finland_census/)
#   uae_population    - UAE population (sample_data/uae_population/: from data repo uae_bayanat/uae_population/test_data/)
#   custom            - Your own TMCF + CSV (use --tmcf and --csv)
#
# Options:
#   --tmcf PATH       Path to TMCF file (for custom dataset)
#   --csv PATH        Path to CSV file (for custom dataset; repeatable for multiple CSVs)
#   --stat-vars-mcf PATH       Optional stat vars MCF (for schema conformance; custom or when dataset has it)
#   --stat-vars-schema-mcf PATH  Optional stat vars schema MCF (for schema conformance)
#   --config PATH     Use custom validation config
#   --rules ID1,ID2   Run only these rules (comma-separated)
#   --skip-rules ID1  Skip these rules (comma-separated)
#   --llm-review      Run Gemini review (schema/typo) on TMCF before validation (requires GEMINI_API_KEY). Default: on.
#   --no-llm-review   Disable Gemini review for this run.
#   --model ID        Gemini model for Gemini review (default: gemini-2.5-pro, falls back to gemini-2.5-flash on quota/availability errors)
#   --help            Show this help
#
# Examples:
#   ./run_e2e_test.sh child_birth
#   ./run_e2e_test.sh statistics_poland
#   ./run_e2e_test.sh finland_census
#   ./run_e2e_test.sh uae_population
#   ./run_e2e_test.sh child_birth --rules=check_min_value,check_unit_consistency
#

set -euo pipefail

# --- Paths (relative to script location) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_REPO="${DATA_REPO:-$PROJECTS_DIR/datacommonsorg/data}"
export DATA_REPO
# Import tool behavior: passed to Java process (env). Defaults support deterministic/local runs.
export IMPORT_RESOLUTION_MODE="${IMPORT_RESOLUTION_MODE:-LOCAL}"
export IMPORT_EXISTENCE_CHECKS="${IMPORT_EXISTENCE_CHECKS:-true}"
JAVA_THREADS="${JAVA_THREADS:-2}"
JAVA_XMX="${JAVA_XMX:-96g}"
# CSV auto-splitting: off by default; enable to split large single-CSV imports
# into shards so genmcf --num-threads actually parallelizes (it is file-level only).
# Expected improvement from splitting: roughly 2–5x on real-world datasets
# (e.g. 30 min -> 6–15 min); actual gains depend on genmcf internals, disk I/O
# bandwidth, and JVM GC behaviour and may be lower.
CSV_SPLIT_ENABLED="${CSV_SPLIT_ENABLED:-false}"
CSV_SPLIT_ROWS="${CSV_SPLIT_ROWS:-}"                    # empty = adaptive (default); set to integer to override
CSV_SPLIT_TARGET_SHARDS_PER_THREAD="${CSV_SPLIT_TARGET_SHARDS_PER_THREAD:-2}"  # adaptive target shards = JAVA_THREADS × this; supports decimals (e.g. 1.5)
CSV_SPLIT_THRESHOLD_ROWS="${CSV_SPLIT_THRESHOLD_ROWS:-5000000}"
# Set CSV_SPLIT_CLEANUP=false to preserve shards after Step 2 (useful for debugging).
CSV_SPLIT_CLEANUP="${CSV_SPLIT_CLEANUP:-true}"
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
CUSTOM_CSVS=()
CUSTOM_STAT_VARS_MCF=""
CUSTOM_STAT_VARS_SCHEMA_MCF=""
LLM_REVIEW=true
LLM_MODEL="gemini-2.5-pro"
BASELINE_NAME=""
# Optional env-var overrides — default to empty so set -u does not abort when unset.
IMPORT_JAR_PATH="${IMPORT_JAR_PATH:-}"

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
      CUSTOM_CSVS+=("$2")
      shift 2
      ;;
    --csv=*)
      CUSTOM_CSVS+=("${1#*=}")
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
    --model)
      LLM_MODEL="$2"
      shift 2
      ;;
    --model=*)
      LLM_MODEL="${1#*=}"
      shift
      ;;
    --baseline-name)
      BASELINE_NAME="$2"
      shift 2
      ;;
    --baseline-name=*)
      BASELINE_NAME="${1#*=}"
      shift
      ;;
    --help|-h)
      head -28 "$0" | tail -23
      exit 0
      ;;
    child_birth|statistics_poland|finland_census|uae_population|custom)
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
  if [[ -n "$CUSTOM_TMCF" && ${#CUSTOM_CSVS[@]} -gt 0 ]]; then
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
# Args: code step message [limit] [details_file]
# If limit (4th) is non-empty, adds "limit" to JSON. If details_file (5th) exists, embeds its JSON as "details".
emit_failure() {
  local code=$1 step=$2 msg=$3 limit=${4:-} details_file=${5:-}
  # Escape $msg via Python so quotes, backslashes, newlines, etc. never corrupt the JSON.
  # json.dumps produces a quoted string ("…"); [1:-1] strips the outer quotes leaving only the content.
  local escaped_msg
  escaped_msg=$(${PYTHON:-python3} -c "import json,sys; print(json.dumps(sys.argv[1])[1:-1])" "$msg" 2>/dev/null) \
    || escaped_msg="(message unavailable; see logs)"
  local base="{\"t\":\"failure\",\"code\":\"$code\",\"step\":$step,\"message\":\"$escaped_msg\""
  if [[ -n "$limit" && "$limit" != "null" ]]; then
    base="${base},\"limit\":${limit}"
  fi
  if [[ -n "$details_file" && -f "$details_file" ]]; then
    local details
    details=$(cat "$details_file")
    base="${base},\"details\":${details}"
  fi
  echo "${base}}"
}

# Log container memory usage at key pipeline stages. Reads from the cgroup filesystem so the
# value reflects total container RSS (including the JVM), not just this shell process.
log_mem() {
  local step="$1"
  local rss_gb
  rss_gb=$(${PYTHON:-python3} -c "
import sys
try:
    # cgroups v2 (Cloud Run, modern kernels)
    with open('/sys/fs/cgroup/memory.current') as f:
        rss = int(f.read().strip())
except Exception:
    try:
        # cgroups v1 fallback
        with open('/sys/fs/cgroup/memory/memory.usage_in_bytes') as f:
            rss = int(f.read().strip())
    except Exception:
        sys.exit(1)
print(f'{rss / (1024**3):.1f}GB')
" 2>/dev/null) || return 0
  echo "[MEM] step=$step rss=$rss_gb"
}

# Ensure validation_output.json and validation_report.html exist before exiting with failure (so GCS upload and /report/... work on Cloud Run).
# Optional: ensure_failure_report "Stage Name" "Reason message" — writes pipeline_failure.json so the HTML report shows a clear "Pipeline failed at: ..." banner.
ensure_failure_report() {
  [[ -z "$DATASET_OUTPUT" || -z "$DATASET" ]] && return
  local py="${PYTHON:-python3}"
  mkdir -p "$DATASET_OUTPUT"
  if [[ ! -f "$DATASET_OUTPUT/validation_output.json" ]]; then
    echo '[]' > "$DATASET_OUTPUT/validation_output.json"
  fi
  local stage="$1"
  local reason="$2"
  if [[ -n "$stage" && -n "$reason" ]]; then
    stage="$stage" reason="$reason" out_path="$DATASET_OUTPUT/pipeline_failure.json" $py -c '
import os, json
s, r, p = os.environ.get("stage", ""), os.environ.get("reason", ""), os.environ.get("out_path", "")
if s and r and p:
  with open(p, "w") as f:
    json.dump({"stage": s, "reason": r}, f)
' 2>/dev/null || true
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
if [[ -f "$VENV_PYTHON" ]] && "$VENV_PYTHON" -c "import absl, pandas, duckdb, omegaconf, googleapiclient" 2>/dev/null; then
  : # venv ready
elif [[ -f "$JAR_PATH" ]] && "$SYS_PYTHON" -c "import absl, pandas, duckdb, omegaconf, googleapiclient" 2>/dev/null; then
  log_info "Using system Python (pre-installed environment)."
else
  log_info "Python environment not ready. Running setup..."
  "$SCRIPT_DIR/setup.sh" || {
    log_error "Setup failed. Run ./setup.sh manually."
    exit 1
  }
fi

mkdir -p "$OUTPUT_DIR"

# Resolve PYTHON once: honour env override, then prefer .venv, then system python3.
# Must be set before any use of $PYTHON so that set -u does not abort the script.
if [[ -z "${PYTHON:-}" ]]; then
  [[ -f "$VENV_PYTHON" ]] && PYTHON="$VENV_PYTHON" || PYTHON="python3"
fi

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
  CSVS=("$CSV")
  GENMCF_OUTPUT="$OUTPUT_DIR/child_birth_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  # Optional: set STAT_VARS_MCF="$CB/child_birth_stat_vars.mcf" to enable stat_var checks
  STAT_VARS_MCF="$CB/child_birth_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using child_birth (sample_data/child_birth/)"
elif [[ "$DATASET" == "statistics_poland" ]]; then
  SP="$SCRIPT_DIR/sample_data/statistics_poland"
  TMCF="$SP/StatisticsPoland_output.tmcf"
  CSV="$SP/StatisticsPoland_output.csv"
  CSVS=("$CSV")
  GENMCF_OUTPUT="$OUTPUT_DIR/statistics_poland_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="$SP/StatisticsPoland_output_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF="$SP/StatisticsPoland_output_stat_vars_schema.mcf"
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using statistics_poland (sample_data/statistics_poland/, from data repo test/)"
elif [[ "$DATASET" == "finland_census" ]]; then
  FC="$SCRIPT_DIR/sample_data/finland_census"
  TMCF="$FC/finland_census_output.tmcf"
  CSV="$FC/finland_census_output.csv"
  CSVS=("$CSV")
  GENMCF_OUTPUT="$OUTPUT_DIR/finland_census_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="$FC/finland_census_output_stat_vars.mcf"
  STAT_VARS_SCHEMA_MCF="$FC/finland_census_output_stat_vars_schema.mcf"
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using finland_census (sample_data/finland_census/, from data repo test_data/)"
elif [[ "$DATASET" == "uae_population" ]]; then
  UAE="$SCRIPT_DIR/sample_data/uae_population"
  TMCF="$UAE/uae_population_output.tmcf"
  CSV="$UAE/uae_population_output.csv"
  CSVS=("$CSV")
  GENMCF_OUTPUT="$OUTPUT_DIR/uae_population_genmcf"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF=""
  STAT_VARS_SCHEMA_MCF=""
  DIFFER_OUTPUT=""
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  log_info "Using uae_population (sample_data/uae_population/, from data repo uae_bayanat/uae_population/test_data/)"
elif [[ "$DATASET" == "custom" ]]; then
  if [[ -z "$CUSTOM_TMCF" || ${#CUSTOM_CSVS[@]} -eq 0 ]]; then
    log_error "Custom dataset requires --tmcf and --csv"
    echo "Example: ./run_e2e_test.sh --tmcf=path/to/file.tmcf --csv=path/to/file.csv"
    echo "         ./run_e2e_test.sh --tmcf=path/to/file.tmcf --csv=a.csv --csv=b.csv"
    exit 1
  fi
  TMCF="$CUSTOM_TMCF"
  CSVS=("${CUSTOM_CSVS[@]}")
  CSV="${CSVS[0]}"
  # CLI: fixed output dir so "latest" and docs align; Web UI sets RUN_ID so output goes to output/custom/{run_id}/
  GENMCF_OUTPUT="$OUTPUT_DIR/custom_input"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  STAT_VARS_MCF="${CUSTOM_STAT_VARS_MCF:-}"
  STAT_VARS_SCHEMA_MCF="${CUSTOM_STAT_VARS_SCHEMA_MCF:-}"
  DIFFER_OUTPUT=""  # No differ output for new imports
  [[ -z "$CONFIG_OVERRIDE" ]] && VALIDATION_CONFIG="$CONFIG_DIR/new_import_config.json"
  _csv_list="$(IFS=', '; echo "${CSVS[*]}")"
  log_info "Using custom data: TMCF=$TMCF, CSVs=$_csv_list"
else
  log_error "Unknown dataset: $DATASET"
  echo "Use: child_birth, statistics_poland, finland_census, uae_population, or custom (with --tmcf and --csv)"
  exit 1
fi

# Build --csv args array from CSVS for scripts that accept repeatable --csv
CSV_ARGS=()
for _csv_arg in "${CSVS[@]}"; do
  CSV_ARGS+=(--csv="$_csv_arg")
done

# Per-run output dir when RUN_ID is set (e.g. by UI) to avoid concurrent-run overwrites
if [[ -n "${RUN_ID:-}" ]]; then
  GENMCF_OUTPUT="$OUTPUT_DIR/$DATASET/$RUN_ID"
  DATASET_OUTPUT="$GENMCF_OUTPUT"
  STATS_SUMMARY="$GENMCF_OUTPUT/summary_report.csv"
  LINT_REPORT="$GENMCF_OUTPUT/report.json"
  mkdir -p "$GENMCF_OUTPUT"
  log_info "Using per-run output: $GENMCF_OUTPUT"
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
echo "::STEP::0:Pre-Import Checks"
log_info "Pre-Import Checks (files + CSV quality + row count)..."
mkdir -p "$DATASET_OUTPUT"
PREFLIGHT_ERRORS_JSON="$DATASET_OUTPUT/preflight_errors.json"
CSV_QUALITY_DETAILS_JSON="$DATASET_OUTPUT/csv_quality_details.json"
VALIDATE_FILES_SCRIPT="$SCRIPT_DIR/scripts/validate_import_files.py"
VALIDATE_CSV_SCRIPT="$SCRIPT_DIR/scripts/validate_csv_quality.py"
VALIDATE_AND_SPLIT_SCRIPT="$SCRIPT_DIR/scripts/validate_and_split.py"

# ── Launch Step 1 (schema/LLM review) in the background ───────────────────
# Step 1 only reads the TMCF and the CSV header — it does NOT need the full
# CSV body, so it can run concurrently with Step 0's streaming CSV scan.
# Output is captured to a temp file to prevent stdout interleaving.
_STEP1_BG_PID=""
_STEP1_BG_OUT=""
_STEP1_BG_EXIT=0
# Differ background vars — declared here (before EXIT trap) so the trap function
# can safely reference them with set -u active, even on early pipeline failure.
_DIFFER_BG_PID=""
_DIFFER_BG_LOG=""
STEP1_START=$(date +%s)
if [[ -n "$TMCF" && -f "$TMCF" ]]; then
  LLM_REVIEW_SCRIPT="$SCRIPT_DIR/scripts/llm_schema_review.py"
  SCHEMA_REVIEW_OUT="$DATASET_OUTPUT/schema_review.json"
  if [[ -f "$LLM_REVIEW_SCRIPT" ]]; then
    _STEP1_BG_OUT=$(mktemp)
    LLM_EXTRA_ARGS=()
    [[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] && LLM_EXTRA_ARGS+=(--stat-vars-mcf="$STAT_VARS_MCF")
    [[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]] && LLM_EXTRA_ARGS+=(--stat-vars-schema-mcf="$STAT_VARS_SCHEMA_MCF")
    [[ ${#CSVS[@]} -gt 0 && -f "${CSVS[0]}" ]] && LLM_EXTRA_ARGS+=(--csv="${CSVS[0]}")
    [[ "$LLM_REVIEW" == "true" ]] && LLM_EXTRA_ARGS+=(--llm-review)
    log_mem "step1_gemini"
    if [[ "$LLM_REVIEW" == "true" ]]; then
      log_info "Step 1 (background): schema review + Gemini (model: $LLM_MODEL)..."
    else
      log_info "Step 1 (background): schema review (deterministic only)..."
    fi
    $PYTHON "$LLM_REVIEW_SCRIPT" --tmcf="$TMCF" --output="$SCHEMA_REVIEW_OUT" \
      --model="$LLM_MODEL" "${LLM_EXTRA_ARGS[@]}" >"$_STEP1_BG_OUT" 2>&1 &
    _STEP1_BG_PID=$!
    log_info "Step 1 launched in background (PID $_STEP1_BG_PID)"
  fi
fi

# Kill all background processes launched by this script on early pipeline exit.
# Handles both the Step 1 LLM background process and the Step 2.4 differ background process.
_kill_bg_processes() {
  if [[ -n "$_STEP1_BG_PID" ]]; then
    kill "$_STEP1_BG_PID" 2>/dev/null || true
    wait "$_STEP1_BG_PID" 2>/dev/null || true
    _STEP1_BG_PID=""
  fi
  if [[ -n "$_STEP1_BG_OUT" && -f "$_STEP1_BG_OUT" ]]; then
    rm -f "$_STEP1_BG_OUT"
  fi
  if [[ -n "$_DIFFER_BG_PID" ]]; then
    kill "$_DIFFER_BG_PID" 2>/dev/null || true
    wait "$_DIFFER_BG_PID" 2>/dev/null || true
    _DIFFER_BG_PID=""
  fi
  if [[ -n "$_DIFFER_BG_LOG" && -f "$_DIFFER_BG_LOG" ]]; then
    rm -f "$_DIFFER_BG_LOG"
    _DIFFER_BG_LOG=""
  fi
  # Ensure the trap never exits non-zero — a non-zero EXIT trap exit code
  # overrides the script's exit code in bash, even after an explicit exit 0.
  return 0
}
# Ensure background processes are cleaned up on any exit (including set -e failures).
trap '_kill_bg_processes' EXIT

# ── Preflight check ────────────────────────────────────────────────────────
if [[ -f "$VALIDATE_FILES_SCRIPT" && -n "$TMCF" && ${#CSVS[@]} -gt 0 ]]; then
  PREFLIGHT_ARGS=(--tmcf="$TMCF" "${CSV_ARGS[@]}" --output-errors="$PREFLIGHT_ERRORS_JSON")
  [[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] && PREFLIGHT_ARGS+=(--stat-vars-mcf="$STAT_VARS_MCF")
  [[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]] && PREFLIGHT_ARGS+=(--stat-vars-schema-mcf="$STAT_VARS_SCHEMA_MCF")
  if ! $PYTHON "$VALIDATE_FILES_SCRIPT" "${PREFLIGHT_ARGS[@]}" 2>/dev/null; then
    _kill_bg_processes
    log_error "Preflight failed: required import files missing or wrong extension."
    emit_failure "PREFLIGHT_FAILED" 0 "Preflight failed" "" "$PREFLIGHT_ERRORS_JSON"
    $PYTHON "$VALIDATE_FILES_SCRIPT" "${PREFLIGHT_ARGS[@]}" || true
    ensure_failure_report "Pre-Import Checks" "Preflight failed"
    exit 1
  fi
fi

# ── CSV quality check ──────────────────────────────────────────────────────
# Single CSV + split enabled: combined validate+split script handles quality
# checking AND splitting together in Step 1.5 (one streaming pass).
# All other cases (multi-CSV, split disabled): validate quality now.
_USE_COMBINED_SCRIPT=false
if [[ "$CSV_SPLIT_ENABLED" == "true" && ${#CSVS[@]} -eq 1 && -f "${CSVS[0]}" && -f "$VALIDATE_AND_SPLIT_SCRIPT" ]]; then
  _USE_COMBINED_SCRIPT=true
  log_info "Single CSV + split enabled: CSV quality check deferred to combined validate+split pass (Step 1.5)"
elif [[ -f "$VALIDATE_CSV_SCRIPT" && ${#CSVS[@]} -gt 0 ]]; then
  CSV_QUALITY_EXTRA=(--allow-empty-columns)
  if ! $PYTHON "$VALIDATE_CSV_SCRIPT" "${CSV_ARGS[@]}" --value-column=value \
       --output-details="$CSV_QUALITY_DETAILS_JSON" "${CSV_QUALITY_EXTRA[@]}" 2>/dev/null; then
    _kill_bg_processes
    log_error "CSV quality check failed."
    emit_failure "CSV_QUALITY_FAILED" 0 "CSV quality check failed" "" "$CSV_QUALITY_DETAILS_JSON"
    $PYTHON "$VALIDATE_CSV_SCRIPT" "${CSV_ARGS[@]}" --value-column=value "${CSV_QUALITY_EXTRA[@]}" || true
    ensure_failure_report "Pre-Import Checks" "CSV quality check failed"
    exit 1
  fi
  # Warn when CSV has entirely empty column(s) (non-blocking; pipeline continues)
  if [[ -f "$CSV_QUALITY_DETAILS_JSON" ]]; then
    EMPTY_COLS="$($PYTHON -c "
import json, sys
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
    cols = d.get('empty_columns') or []
    if cols:
        print(','.join(cols))
except Exception:
    pass
" "$CSV_QUALITY_DETAILS_JSON" 2>/dev/null)"
    if [[ -n "$EMPTY_COLS" ]]; then
      log_warn "CSV has entirely empty column(s): $EMPTY_COLS (non-blocking; pipeline continues)"
    fi
  fi
fi

# =============================================================================
# Step 1.5: CSV Validate + Split
# When _USE_COMBINED_SCRIPT=true: validate_and_split.py performs CSV quality
# validation AND shard splitting in a single streaming pass (saves one full
# CSV read vs. the original sequential validate → split pipeline).
# Otherwise (multi-CSV or split disabled): quality was already validated above;
# run the standalone splitter if split is enabled.
#
# Activate: CSV_SPLIT_ENABLED=true
# Control:  CSV_SPLIT_ROWS=<N>               rows per shard (empty = adaptive, default)
#           CSV_SPLIT_THRESHOLD_ROWS=5000000  skip split if source < this
#
# After split: CSVS and CSV_ARGS are replaced with shard paths for Step 2+.
# Shards live in $DATASET_OUTPUT/csv_shards/ and are cleaned up after Step 2.
# =============================================================================
_SHARD_DIR="$DATASET_OUTPUT/csv_shards"
_SPLIT_MANIFEST="$_SHARD_DIR/manifest.json"
_CSV_WAS_SPLIT=false

# ── Adaptive rows-per-shard ────────────────────────────────────────────────
# CSV_SPLIT_ROWS (non-empty integer) → used directly (backward-compat override).
# Empty (default) → derive from file size + JAVA_THREADS so genmcf receives
# roughly CSV_SPLIT_TARGET_SHARDS_PER_THREAD×JAVA_THREADS shards,
# clamped to [500 000, 5 000 000] rows/shard.
# Assumes ~100 bytes/row for statistics CSVs (typical: 80–120 bytes/row).
_EFFECTIVE_SPLIT_ROWS=""
_TARGET_SHARDS=""
_CSV_SIZE_BYTES=""
if [[ -n "$CSV_SPLIT_ROWS" && "$CSV_SPLIT_ROWS" =~ ^[0-9]+$ && "$CSV_SPLIT_ROWS" -gt 0 ]]; then
  _EFFECTIVE_SPLIT_ROWS="$CSV_SPLIT_ROWS"
fi
if [[ -z "$_EFFECTIVE_SPLIT_ROWS" && "$CSV_SPLIT_ENABLED" == "true" \
      && ${#CSVS[@]} -eq 1 && -f "${CSVS[0]}" ]]; then
  _CSV_SIZE_BYTES=$(stat -f%z "${CSVS[0]}" 2>/dev/null \
                    || stat -c%s "${CSVS[0]}" 2>/dev/null \
                    || echo 0)
  # _TARGET_SHARDS computed via awk to support decimal multipliers (e.g. 1.5)
  _TARGET_SHARDS=$(awk -v t="$JAVA_THREADS" -v m="$CSV_SPLIT_TARGET_SHARDS_PER_THREAD" \
    'BEGIN { v = int(t * m); print (v > 4 ? v : 4) }')
  _EFFECTIVE_SPLIT_ROWS=$(awk -v size="$_CSV_SIZE_BYTES" -v target="$_TARGET_SHARDS" 'BEGIN {
    est_rows = size / 100
    rps      = (target > 0) ? int(est_rows / target) : 2000000
    if (rps < 500000)  rps = 500000
    if (rps > 5000000) rps = 5000000
    print rps
  }')
  log_info "CSV adaptive shard size: ${_EFFECTIVE_SPLIT_ROWS} rows/shard (file_bytes=${_CSV_SIZE_BYTES}, java_threads=${JAVA_THREADS}, multiplier=${CSV_SPLIT_TARGET_SHARDS_PER_THREAD}, target_shards=${_TARGET_SHARDS})"
fi
if [[ -z "$_EFFECTIVE_SPLIT_ROWS" ]]; then
  _EFFECTIVE_SPLIT_ROWS=2000000  # fallback: split disabled or file not accessible
fi

# ── CSV_DUP_CHECK policy ───────────────────────────────────────────────────
# Resolves CSV_DUP_CHECK (auto|true|false) to a concrete flag for validate_and_split.py.
# auto (default): disable dup check only when the CSV is estimated to exceed
#   CSV_SPLIT_THRESHOLD_ROWS rows (default 5M). Uses file size / 100 as a row-count
#   estimate (typical statistics CSV: 80-120 bytes/row). Preserves integrity checks
#   for smaller imports where the ~165s hashing cost is not justified.
# true:  always enable (useful when data integrity > throughput, e.g. production gate).
# false: always disable (useful for benchmarking or datasets known to be de-duped).
_DUP_CHECK_POLICY="${CSV_DUP_CHECK:-auto}"
_DUP_CHECK_ACTIVE="true"
_DUP_CHECK_FLAG=""
_DUP_CHECK_REASON=""
if [[ "$_DUP_CHECK_POLICY" == "false" ]]; then
  _DUP_CHECK_ACTIVE="false"
  _DUP_CHECK_FLAG="--no-dup-check"
  _DUP_CHECK_REASON="policy=false (always disabled)"
elif [[ "$_DUP_CHECK_POLICY" == "true" ]]; then
  _DUP_CHECK_REASON="policy=true (always enabled)"
elif [[ "$_DUP_CHECK_POLICY" == "auto" && "$CSV_SPLIT_ENABLED" == "true" \
        && ${#CSVS[@]} -eq 1 && -f "${CSVS[0]}" ]]; then
  # auto + single CSV on the split path: check estimated row count via file size.
  # Hashing costs ~165s for 38M rows; disable only when the file is large enough
  # that the threshold would trigger a split (i.e., estimated rows >= threshold).
  _DUP_THRESHOLD_ROWS="${CSV_SPLIT_THRESHOLD_ROWS:-5000000}"
  _DUP_THRESHOLD_BYTES=$(( _DUP_THRESHOLD_ROWS * 100 ))
  # _CSV_SIZE_BYTES was already computed for adaptive shard sizing above; fall back
  # to a fresh stat call if it was not set (e.g., split path not taken above).
  if [[ -z "$_CSV_SIZE_BYTES" ]]; then
    _CSV_SIZE_BYTES=$(stat -f%z "${CSVS[0]}" 2>/dev/null \
                      || stat -c%s "${CSVS[0]}" 2>/dev/null \
                      || echo 0)
  fi
  if [[ "$_CSV_SIZE_BYTES" -ge "$_DUP_THRESHOLD_BYTES" ]]; then
    _DUP_CHECK_ACTIVE="false"
    _DUP_CHECK_FLAG="--no-dup-check"
    _DUP_CHECK_REASON="auto: file_bytes=${_CSV_SIZE_BYTES} >= threshold_bytes=${_DUP_THRESHOLD_BYTES} (est. rows >= ${_DUP_THRESHOLD_ROWS})"
  else
    _DUP_CHECK_REASON="auto: file_bytes=${_CSV_SIZE_BYTES} < threshold_bytes=${_DUP_THRESHOLD_BYTES} (small file — check enabled)"
  fi
else
  _DUP_CHECK_REASON="auto: not on single-CSV split path"
fi
log_info "CSV dup check policy: ${_DUP_CHECK_POLICY} → active=${_DUP_CHECK_ACTIVE} (${_DUP_CHECK_REASON})"

if [[ "$CSV_SPLIT_ENABLED" == "true" ]]; then
  if [[ ${#CSVS[@]} -gt 1 ]]; then
    log_info "CSV auto-split skipped: ${#CSVS[@]} CSV files already provided (splitting only applies to single-CSV inputs)"
  elif [[ ${#CSVS[@]} -eq 0 || ! -f "${CSVS[0]}" ]]; then
    log_info "CSV auto-split skipped: no accessible CSV file found"
  elif [[ "$_USE_COMBINED_SCRIPT" == "true" ]]; then
    # ── Combined validate + split path (single streaming pass) ────────────
    log_info "Combined validate+split (rows_per_shard=${_EFFECTIVE_SPLIT_ROWS}, threshold=$CSV_SPLIT_THRESHOLD_ROWS, dup_check=${_DUP_CHECK_ACTIVE})..."
    mkdir -p "$_SHARD_DIR"
    set +e
    $PYTHON "$VALIDATE_AND_SPLIT_SCRIPT" \
      --input="${CSVS[0]}" \
      --output-dir="$_SHARD_DIR" \
      --rows-per-shard="$_EFFECTIVE_SPLIT_ROWS" \
      --threshold-rows="$CSV_SPLIT_THRESHOLD_ROWS" \
      --manifest="$_SPLIT_MANIFEST" \
      --output-details="$CSV_QUALITY_DETAILS_JSON" \
      --value-column=value \
      --allow-empty-columns \
      ${_DUP_CHECK_FLAG:+"$_DUP_CHECK_FLAG"} \
      2>/dev/null
    _COMBINED_EXIT=$?
    set -e

    if [[ $_COMBINED_EXIT -ne 0 ]]; then
      _kill_bg_processes
      log_error "CSV quality check failed (combined validate+split, exit $_COMBINED_EXIT)"
      # Re-run with errors printed to stderr so they appear in the log.
      $PYTHON "$VALIDATE_AND_SPLIT_SCRIPT" \
        --input="${CSVS[0]}" \
        --no-split \
        --output-details="$CSV_QUALITY_DETAILS_JSON" \
        --value-column=value \
        --allow-empty-columns || true
      emit_failure "CSV_QUALITY_FAILED" 0 "CSV quality check failed" "" "$CSV_QUALITY_DETAILS_JSON"
      ensure_failure_report "Pre-Import Checks" "CSV quality check failed"
      exit 1
    fi

    # Warn when CSV has entirely empty column(s) (non-blocking)
    if [[ -f "$CSV_QUALITY_DETAILS_JSON" ]]; then
      EMPTY_COLS="$($PYTHON -c "
import json, sys
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
    cols = d.get('empty_columns') or []
    if cols:
        print(','.join(cols))
except Exception:
    pass
" "$CSV_QUALITY_DETAILS_JSON" 2>/dev/null)"
      if [[ -n "$EMPTY_COLS" ]]; then
        log_warn "CSV has entirely empty column(s): $EMPTY_COLS (non-blocking; pipeline continues)"
      fi
    fi

    # ── Parse manifest ─────────────────────────────────────────────────────
    if [[ ! -f "$_SPLIT_MANIFEST" ]]; then
      log_warn "Combined script exited 0 but manifest not found — continuing without splitting"
    else
      _SPLIT_STATUS=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('status','unknown'))" "$_SPLIT_MANIFEST" 2>/dev/null || echo "unknown")

      if [[ "$_SPLIT_STATUS" == "done" ]]; then
        _SHARD_COUNT=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('shard_count',0))" "$_SPLIT_MANIFEST")
        _TOTAL_ROWS=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('total_rows',0))" "$_SPLIT_MANIFEST")
        _SPLIT_ELAPSED=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('elapsed_seconds',0))" "$_SPLIT_MANIFEST")
        log_info "Combined validate+split: 1 file -> ${_SHARD_COUNT} shards (${_TOTAL_ROWS} rows, ${_SPLIT_ELAPSED}s)"

        if [[ $_SHARD_COUNT -eq 0 ]]; then
          _kill_bg_processes
          log_error "Combined script reported done but shard_count=0"
          emit_failure "CSV_SPLIT_FAILED" 2 "CSV split produced no shards"
          ensure_failure_report "CSV Split" "CSV split produced no shards"
          exit 1
        fi

        # Replace CSVS array with shard paths.
        # Preserve originals: ORIGINAL_PRIMARY_CSV is used for perf logging.
        ORIGINAL_PRIMARY_CSV="${CSVS[0]}"
        ORIGINAL_CSVS=("${CSVS[@]}")
        CSVS=()
        while IFS= read -r _shard_path; do
          [[ -n "$_shard_path" ]] && CSVS+=("$_shard_path")
        done < <($PYTHON -c "
import json, sys
d = json.load(open(sys.argv[1]))
print('\n'.join(d.get('shard_paths', [])))
" "$_SPLIT_MANIFEST")

        if [[ ${#CSVS[@]} -eq 0 ]]; then
          _kill_bg_processes
          log_error "Failed to read shard paths from manifest"
          emit_failure "CSV_SPLIT_FAILED" 2 "Could not read shard paths"
          ensure_failure_report "CSV Split" "Could not read shard paths from manifest"
          exit 1
        fi

        # Rebuild CSV_ARGS for downstream scripts (Step 3 validation etc.)
        CSV_ARGS=()
        for _csv_arg in "${CSVS[@]}"; do
          CSV_ARGS+=(--csv="$_csv_arg")
        done

        _CSV_WAS_SPLIT=true

        # Compute size / shard stats for perf logging.
        _ORIG_CSV_MB=$($PYTHON -c "
import os, sys
try:
    print(int(os.path.getsize(sys.argv[1]) / (1024 * 1024)))
except Exception:
    print('unknown')
" "$ORIGINAL_PRIMARY_CSV" 2>/dev/null || echo "unknown")
        _AVG_ROWS_PER_SHARD=$(( _TOTAL_ROWS / ${#CSVS[@]} ))
        if [[ "$_ORIG_CSV_MB" != "unknown" ]]; then
          _AVG_MB_PER_SHARD=$(( _ORIG_CSV_MB / ${#CSVS[@]} ))
        else
          _AVG_MB_PER_SHARD="unknown"
        fi

        log_info "CSV split: original=$ORIGINAL_PRIMARY_CSV size=${_ORIG_CSV_MB}MB shards=${#CSVS[@]} avg_rows_per_shard=${_AVG_ROWS_PER_SHARD} avg_mb_per_shard=${_AVG_MB_PER_SHARD}"

        if (( JAVA_THREADS <= ${#CSVS[@]} )); then
          log_info "genmcf parallelism possible: threads=$JAVA_THREADS csvs=${#CSVS[@]} (all threads can be utilized)"
        else
          log_info "genmcf parallelism possible: threads=$JAVA_THREADS csvs=${#CSVS[@]} (more threads than shards; some will be idle)"
        fi

      elif [[ "$_SPLIT_STATUS" == "skipped" ]]; then
        _SKIP_ROWS=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('total_rows',0))" "$_SPLIT_MANIFEST" 2>/dev/null || echo "?")
        log_info "CSV auto-split skipped: ${_SKIP_ROWS} rows < threshold $CSV_SPLIT_THRESHOLD_ROWS (no split needed)"
      else
        log_warn "Combined script returned unexpected status='$_SPLIT_STATUS' — continuing without splitting"
      fi
    fi

  else
    # ── Standalone split path (fallback: validate_and_split.py not present) ─
    log_info "CSV auto-split (rows_per_shard=${_EFFECTIVE_SPLIT_ROWS}, threshold=$CSV_SPLIT_THRESHOLD_ROWS)..."
    mkdir -p "$_SHARD_DIR"
    set +e
    $PYTHON "$SCRIPT_DIR/scripts/split_csv_for_genmcf.py" \
      --input="${CSVS[0]}" \
      --output-dir="$_SHARD_DIR" \
      --rows-per-shard="$_EFFECTIVE_SPLIT_ROWS" \
      --threshold-rows="$CSV_SPLIT_THRESHOLD_ROWS" \
      --manifest="$_SPLIT_MANIFEST"
    _SPLIT_EXIT=$?
    set -e

    if [[ $_SPLIT_EXIT -ne 0 ]]; then
      _kill_bg_processes
      log_error "CSV auto-split failed (exit $_SPLIT_EXIT)"
      emit_failure "CSV_SPLIT_FAILED" 2 "CSV auto-splitting failed"
      ensure_failure_report "CSV Split" "CSV auto-splitting failed"
      exit 1
    fi

    if [[ ! -f "$_SPLIT_MANIFEST" ]]; then
      log_warn "CSV split exited 0 but manifest not found — continuing without splitting"
    else
      _SPLIT_STATUS=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('status','unknown'))" "$_SPLIT_MANIFEST" 2>/dev/null || echo "unknown")

      if [[ "$_SPLIT_STATUS" == "done" ]]; then
        _SHARD_COUNT=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('shard_count',0))" "$_SPLIT_MANIFEST")
        _TOTAL_ROWS=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('total_rows',0))" "$_SPLIT_MANIFEST")
        _SPLIT_ELAPSED=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('elapsed_seconds',0))" "$_SPLIT_MANIFEST")
        log_info "CSV auto-split: 1 file -> ${_SHARD_COUNT} shards (${_TOTAL_ROWS} rows, ${_SPLIT_ELAPSED}s)"

        if [[ $_SHARD_COUNT -eq 0 ]]; then
          _kill_bg_processes
          log_error "CSV split reported done but shard_count=0"
          emit_failure "CSV_SPLIT_FAILED" 2 "CSV split produced no shards"
          ensure_failure_report "CSV Split" "CSV split produced no shards"
          exit 1
        fi

        ORIGINAL_PRIMARY_CSV="${CSVS[0]}"
        ORIGINAL_CSVS=("${CSVS[@]}")
        CSVS=()
        while IFS= read -r _shard_path; do
          [[ -n "$_shard_path" ]] && CSVS+=("$_shard_path")
        done < <($PYTHON -c "
import json, sys
d = json.load(open(sys.argv[1]))
print('\n'.join(d.get('shard_paths', [])))
" "$_SPLIT_MANIFEST")

        if [[ ${#CSVS[@]} -eq 0 ]]; then
          _kill_bg_processes
          log_error "Failed to read shard paths from manifest"
          emit_failure "CSV_SPLIT_FAILED" 2 "Could not read shard paths"
          ensure_failure_report "CSV Split" "Could not read shard paths from manifest"
          exit 1
        fi

        CSV_ARGS=()
        for _csv_arg in "${CSVS[@]}"; do
          CSV_ARGS+=(--csv="$_csv_arg")
        done

        _CSV_WAS_SPLIT=true

        _ORIG_CSV_MB=$($PYTHON -c "
import os, sys
try:
    print(int(os.path.getsize(sys.argv[1]) / (1024 * 1024)))
except Exception:
    print('unknown')
" "$ORIGINAL_PRIMARY_CSV" 2>/dev/null || echo "unknown")
        _AVG_ROWS_PER_SHARD=$(( _TOTAL_ROWS / ${#CSVS[@]} ))
        if [[ "$_ORIG_CSV_MB" != "unknown" ]]; then
          _AVG_MB_PER_SHARD=$(( _ORIG_CSV_MB / ${#CSVS[@]} ))
        else
          _AVG_MB_PER_SHARD="unknown"
        fi

        log_info "CSV split: original=$ORIGINAL_PRIMARY_CSV size=${_ORIG_CSV_MB}MB shards=${#CSVS[@]} avg_rows_per_shard=${_AVG_ROWS_PER_SHARD} avg_mb_per_shard=${_AVG_MB_PER_SHARD}"

        if (( JAVA_THREADS <= ${#CSVS[@]} )); then
          log_info "genmcf parallelism possible: threads=$JAVA_THREADS csvs=${#CSVS[@]} (all threads can be utilized)"
        else
          log_info "genmcf parallelism possible: threads=$JAVA_THREADS csvs=${#CSVS[@]} (more shards than threads; some threads may process multiple shards sequentially)"
        fi

      elif [[ "$_SPLIT_STATUS" == "skipped" ]]; then
        _SKIP_ROWS=$($PYTHON -c "import json,sys; print(json.load(open(sys.argv[1])).get('total_rows',0))" "$_SPLIT_MANIFEST" 2>/dev/null || echo "?")
        log_info "CSV auto-split skipped: ${_SKIP_ROWS} rows < threshold $CSV_SPLIT_THRESHOLD_ROWS (no split needed)"
      else
        log_warn "CSV split returned unexpected status='$_SPLIT_STATUS' — continuing without splitting"
      fi
    fi
  fi
fi

# =============================================================================
# Step 1: Schema review — wait for background job and emit results
# =============================================================================
if [[ -n "$_STEP1_BG_PID" ]]; then
  log_info "Waiting for Step 1 (schema/LLM review)..."
  # Poll with a bounded wall-clock wait. Wrapping `wait` with `timeout` is unreliable
  # because `wait` is a shell builtin. Polling avoids that issue while still allowing
  # us to kill a stuck Gemini API call cleanly.
  _STEP1_TIMEOUT_SEC="${LLM_TIMEOUT_SEC:-120}"
  _STEP1_WAIT_START=$(date +%s)
  _STEP1_TIMED_OUT=false
  while kill -0 "$_STEP1_BG_PID" 2>/dev/null; do
    sleep 1
    _STEP1_WAITED=$(( $(date +%s) - _STEP1_WAIT_START ))
    if [[ $_STEP1_WAITED -ge $_STEP1_TIMEOUT_SEC ]]; then
      log_warn "Step 1: LLM/schema review timed out after ${_STEP1_TIMEOUT_SEC}s — killing process (pid=${_STEP1_BG_PID})"
      kill "$_STEP1_BG_PID" 2>/dev/null || true
      _STEP1_TIMED_OUT=true
      break
    fi
  done
  # Reap the process to avoid a zombie and capture its exit code.
  wait "$_STEP1_BG_PID" 2>/dev/null && _STEP1_BG_EXIT=0 || _STEP1_BG_EXIT=$?
  if [[ "$_STEP1_TIMED_OUT" == "true" ]]; then
    _STEP1_BG_EXIT=0  # treat timeout as non-fatal advisory pass
  fi
  _STEP1_BG_PID=""
fi

echo "::STEP::1:Gemini Review"

# Replay Step 1 output (captured to avoid interleaving with Step 0 stdout).
if [[ -n "$_STEP1_BG_OUT" && -f "$_STEP1_BG_OUT" ]]; then
  cat "$_STEP1_BG_OUT"
  rm -f "$_STEP1_BG_OUT"
  _STEP1_BG_OUT=""
fi

if [[ -n "$TMCF" && -f "$TMCF" ]]; then
  LLM_REVIEW_SCRIPT="$SCRIPT_DIR/scripts/llm_schema_review.py"
  SCHEMA_REVIEW_OUT="$DATASET_OUTPUT/schema_review.json"
  if [[ -f "$LLM_REVIEW_SCRIPT" ]]; then
    if [[ "$LLM_REVIEW" == "true" ]]; then
      log_info "Step 1: schema review + Gemini (model: $LLM_MODEL)"
    else
      log_info "Step 1: schema review (deterministic only)"
    fi
    if [[ "${_STEP1_TIMED_OUT:-false}" == "true" ]]; then
      log_warn "Step 1: timed out after ${_STEP1_TIMEOUT_SEC}s — schema review skipped (pipeline continues)"
    elif [[ $_STEP1_BG_EXIT -eq 0 ]]; then
      log_info "Step 1 passed (no blocking issues)"
    else
      if [[ -f "$SCHEMA_REVIEW_OUT" ]]; then
        log_warn "Step 1 found issues (advisory). See $SCHEMA_REVIEW_OUT — continuing pipeline."
        $PYTHON -c "import json; d=json.load(open('$SCHEMA_REVIEW_OUT')); print('\n'.join(str(x) for x in d))" 2>/dev/null || cat "$SCHEMA_REVIEW_OUT"
        # Gemini findings are always advisory; never block validation.
      else
        log_warn "Step 1 failed (script error or missing output)"
        ensure_failure_report "Gemini Review" "Step 1 failed (script error or missing output)"
        emit_failure "GEMINI_BLOCKING" 1 "Gemini review failed (script error)"
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
log_mem "step2_genmcf"
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
    ensure_failure_report "Setup" "Import tool JAR not found"
    exit 1
  fi
fi

_missing_inputs=0
[[ ! -f "$TMCF" ]] && { log_error "TMCF not found: $TMCF"; _missing_inputs=1; }
for _csv_check in "${CSVS[@]}"; do
  [[ ! -f "$_csv_check" ]] && { log_error "CSV not found: $_csv_check"; _missing_inputs=1; }
done
if [[ $_missing_inputs -ne 0 ]]; then
  ensure_failure_report "Setup" "Input files not found"
  exit 1
fi

mkdir -p "$GENMCF_OUTPUT"

# genmcf performs all validations that lint mode performs (per DC documentation), so lint
# is not run separately. genmcf's report.json is used as the lint report for downstream rules.
GENMCF_FILES=("$TMCF" "${CSVS[@]}")
[[ -n "$STAT_VARS_MCF" && -f "$STAT_VARS_MCF" ]] && GENMCF_FILES+=("$STAT_VARS_MCF")
[[ -n "$STAT_VARS_SCHEMA_MCF" && -f "$STAT_VARS_SCHEMA_MCF" ]] && GENMCF_FILES+=("$STAT_VARS_SCHEMA_MCF")
GENMCF_LOG="$GENMCF_OUTPUT/genmcf.log"
log_info "genmcf log file: $GENMCF_LOG"
log_info "genmcf config: machine=${VM_TYPE:-unknown} JAVA_THREADS=${JAVA_THREADS} JAVA_XMX=${JAVA_XMX} csv_files=${#CSVS[@]} total_input_files=${#GENMCF_FILES[@]}"
_CSV_COUNT=${#CSVS[@]}
_EFFECTIVE_PARALLELISM=$(( _CSV_COUNT < JAVA_THREADS ? _CSV_COUNT : JAVA_THREADS ))
log_info "genmcf execution mode: split=${_CSV_WAS_SPLIT} shards=${_CSV_COUNT} threads=${JAVA_THREADS} effective_parallelism=${_EFFECTIVE_PARALLELISM}"
if (( _CSV_COUNT > 0 && JAVA_THREADS > _CSV_COUNT )); then
  log_info "WARNING: JAVA_THREADS=${JAVA_THREADS} exceeds CSV file count (${_CSV_COUNT}); genmcf parallelizes per file, so extra threads will be idle"
fi

# Optional JFR + GC profiling (set GENMCF_PROFILE=true to enable).
# JFR is built into JDK 17 with ~1-5% overhead; useful for diagnosing CPU
# hotspots and lock contention in genmcf's serial finalization phase.
_JVM_PROFILE_FLAGS=()
if [[ "${GENMCF_PROFILE:-false}" == "true" ]]; then
  _JFR_OUTPUT="$GENMCF_OUTPUT/genmcf_profile.jfr"
  _GC_LOG="$GENMCF_OUTPUT/gc.log"
  _JVM_PROFILE_FLAGS=(
    "-XX:StartFlightRecording=filename=${_JFR_OUTPUT},dumponexit=true,settings=profile"
    "-Xlog:gc*:file=${_GC_LOG}:time,uptime:filecount=1"
  )
  log_info "JFR profiling enabled: $_JFR_OUTPUT"
  log_info "GC logging enabled: $_GC_LOG"
fi

set +e
java -XX:+UseG1GC -Xmx"${JAVA_XMX}" \
  "${_JVM_PROFILE_FLAGS[@]}" \
  -jar "$JAR_PATH" genmcf "${GENMCF_FILES[@]}" -o="$GENMCF_OUTPUT" \
  --num-threads="$JAVA_THREADS" \
  --resolution="$IMPORT_RESOLUTION_MODE" --existence-checks="$IMPORT_EXISTENCE_CHECKS" \
  2>&1 | tee "$GENMCF_LOG"
JAVA_EXIT=${PIPESTATUS[0]}
set -e
if [[ $JAVA_EXIT -ne 0 ]]; then
  log_error "dc-import genmcf failed (exit code ${JAVA_EXIT})"
  log_error "See genmcf log: $GENMCF_LOG"
  emit_failure "DATA_PROCESSING_FAILED" 2 "Data processing failed"
  ensure_failure_report "Data Processing" "Data processing failed"
  exit 1
fi

if [[ ! -f "$STATS_SUMMARY" ]]; then
  log_error "summary_report.csv not produced at $STATS_SUMMARY"
  ensure_failure_report "Data Processing" "summary_report.csv not produced"
  exit 1
fi
log_mem "step2_done"
log_info "Generated: $STATS_SUMMARY, report.json"
_STEP2_SECONDS=$(( $(date +%s) - STEP2_START ))
log_info "Step 2 completed in ${_STEP2_SECONDS}s"

# Structured perf log — one line, key=value, for benchmark comparisons and
# Cloud Logging queries. Emitted on every run (split and non-split) so the
# two modes can be compared directly.
#
# rows_processed source:
#   split run  — manifest total_rows (exact, pre-genmcf count)
#   non-split  — sum of NumObservations from summary_report.csv (post-genmcf;
#                excludes rows that failed to parse)
_PERF_ROWS="unknown"
if [[ "$_CSV_WAS_SPLIT" == "true" ]]; then
  _PERF_ROWS="${_TOTAL_ROWS:-unknown}"
elif [[ -f "$STATS_SUMMARY" ]]; then
  _PERF_ROWS=$($PYTHON -c "
import csv, sys
try:
    total = 0
    with open(sys.argv[1]) as f:
        for row in csv.DictReader(f):
            n = (row.get('NumObservations') or row.get('numObservations') or '0').strip()
            total += int(n) if n.isdigit() else 0
    print(total)
except Exception:
    print('unknown')
" "$STATS_SUMMARY" 2>/dev/null || echo "unknown")
fi

if [[ "$_PERF_ROWS" =~ ^[0-9]+$ && "$_STEP2_SECONDS" -gt 0 ]]; then
  _PERF_RPS=$(( _PERF_ROWS / _STEP2_SECONDS ))
else
  _PERF_RPS="unknown"
fi

_PERF_RSS=$($PYTHON -c "
try:
    with open('/sys/fs/cgroup/memory.current') as f:
        print(f'{int(f.read().strip()) / (1024**3):.1f}')
except Exception:
    try:
        with open('/sys/fs/cgroup/memory/memory.usage_in_bytes') as f:
            print(f'{int(f.read().strip()) / (1024**3):.1f}')
    except Exception:
        print('unknown')
" 2>/dev/null || echo "unknown")

# For non-split runs measure original CSV size here (for split runs it was
# measured in Step 1.5 and stored in _ORIG_CSV_MB).
if [[ -z "${_ORIG_CSV_MB:-}" ]]; then
  _PERF_CSV_SRC="${ORIGINAL_PRIMARY_CSV:-}"
  if [[ -z "$_PERF_CSV_SRC" && ${#CSVS[@]} -gt 0 ]]; then
    _PERF_CSV_SRC="${CSVS[0]}"
  fi
  if [[ -n "$_PERF_CSV_SRC" && -f "$_PERF_CSV_SRC" ]]; then
    _ORIG_CSV_MB=$($PYTHON -c "
import os, sys
try: print(int(os.path.getsize(sys.argv[1]) / (1024 * 1024)))
except: print('unknown')
" "$_PERF_CSV_SRC" 2>/dev/null || echo "unknown")
  else
    _ORIG_CSV_MB="unknown"
  fi
fi

# Ensure _CSV_SIZE_BYTES covers all run modes:
#   split runs:           already set during adaptive shard sizing
#   single-CSV non-split: already set by dup-check threshold logic
#   multi-CSV non-split:  sum all CSV sizes here (only path where it can still be empty)
if [[ -z "${_CSV_SIZE_BYTES}" && ${#CSVS[@]} -gt 0 ]]; then
  _CSV_SIZE_BYTES=0
  for _perf_f in "${CSVS[@]}"; do
    _perf_sz=$(stat -f%z "$_perf_f" 2>/dev/null \
               || stat -c%s "$_perf_f" 2>/dev/null \
               || echo 0)
    _CSV_SIZE_BYTES=$(( _CSV_SIZE_BYTES + _perf_sz ))
  done
fi

# bytes_per_row: original CSV bytes / rows_processed.
# Key diagnostic: values >150 indicate wide CSVs where the 100 bytes/row
# heuristic underestimates row count, causing shard starvation.
_BYTES_PER_ROW="na"
if [[ "${_PERF_ROWS}" =~ ^[0-9]+$ && "${_PERF_ROWS}" -gt 0 \
      && "${_CSV_SIZE_BYTES}" =~ ^[0-9]+$ && "${_CSV_SIZE_BYTES}" -gt 0 ]]; then
  _BYTES_PER_ROW=$(( _CSV_SIZE_BYTES / _PERF_ROWS ))
fi

log_info "[PERF] machine_type=${VM_TYPE:-unknown} split_enabled=${CSV_SPLIT_ENABLED} split_rows=${_EFFECTIVE_SPLIT_ROWS} configured_split_rows=${CSV_SPLIT_ROWS:-adaptive} target_shards_per_thread=${CSV_SPLIT_TARGET_SHARDS_PER_THREAD} target_shards=${_TARGET_SHARDS:-na} threshold_rows=${CSV_SPLIT_THRESHOLD_ROWS} original_csv_mb=${_ORIG_CSV_MB:-unknown} shard_count=${_SHARD_COUNT:-0} avg_rows_per_shard=${_AVG_ROWS_PER_SHARD:-na} avg_mb_per_shard=${_AVG_MB_PER_SHARD:-na} csv_count=${_CSV_COUNT:-${#CSVS[@]}} java_threads=${JAVA_THREADS} java_threads_source=${JAVA_THREADS_SOURCE:-default} java_xmx=${JAVA_XMX} split_seconds=${_SPLIT_ELAPSED:-0} step2_seconds=${_STEP2_SECONDS} effective_parallelism=${_EFFECTIVE_PARALLELISM:-na} bytes_per_row=${_BYTES_PER_ROW} rows_processed=${_PERF_ROWS} rows_per_second=${_PERF_RPS} peak_rss_gb=${_PERF_RSS} batch_provisioning_model=${BATCH_PROVISIONING_MODEL:-STANDARD} dup_check_policy=${_DUP_CHECK_POLICY:-auto} dup_check_active=${_DUP_CHECK_ACTIVE:-true}"

# Clean up CSV shards (set CSV_SPLIT_CLEANUP=false to preserve for debugging).
# Guard requires _SHARD_DIR ends with /csv_shards — defends against an empty or
# short _SHARD_DIR value somehow triggering a destructive rm -rf.
if [[ "$_CSV_WAS_SPLIT" == "true" && "${CSV_SPLIT_CLEANUP}" == "true" \
      && "$_SHARD_DIR" == */csv_shards && -d "$_SHARD_DIR" ]]; then
  rm -rf "$_SHARD_DIR"
  log_info "Cleaned up CSV shards: $_SHARD_DIR"
fi

# =============================================================================
# Step 2.4: Differ (run only when a baseline exists for this dataset)
# Produces obs_diff_summary.csv + differ_summary.json consumed by Step 3.
# Exit 0 → differ ran; exit 1 → no baseline (first run); exit 2 → error.
# All non-zero exits are non-fatal: pipeline continues with empty_differ.csv.
# =============================================================================
# Resolve differ dataset ID: named datasets use their own name; custom datasets
# require --baseline-name (no ID = no differ).
_DIFFER_DATASET_ID="${BASELINE_NAME:-}"
if [[ -z "$_DIFFER_DATASET_ID" && "$DATASET" != "custom" ]]; then
  _DIFFER_DATASET_ID="$DATASET"
fi

DIFFER_OUTPUT=""
if [[ -n "$_DIFFER_DATASET_ID" ]]; then
  echo "::STEP::2.4:Differ"
  DIFFER_OUT_DIR="$GENMCF_OUTPUT/differ_output"
  _DIFFER_START=$(date +%s)
  _DIFFER_EXIT=0
  # Bound differ execution time. DIFFER_TIMEOUT_SEC controls both GCS baseline
  # download and the differ subprocess itself. Exit 124 = timed out (non-fatal).
  _DIFFER_TIMEOUT_SEC="${DIFFER_TIMEOUT_SEC:-300}"
  if command -v timeout >/dev/null 2>&1; then
    timeout "${_DIFFER_TIMEOUT_SEC}" $PYTHON "$SCRIPT_DIR/scripts/run_differ.py" \
      --current_mcf_dir="$GENMCF_OUTPUT" \
      --dataset_id="$_DIFFER_DATASET_ID" \
      --output_dir="$DIFFER_OUT_DIR" || _DIFFER_EXIT=$?
  else
    # timeout not available (macOS without GNU coreutils) — run unbounded.
    $PYTHON "$SCRIPT_DIR/scripts/run_differ.py" \
      --current_mcf_dir="$GENMCF_OUTPUT" \
      --dataset_id="$_DIFFER_DATASET_ID" \
      --output_dir="$DIFFER_OUT_DIR" || _DIFFER_EXIT=$?
  fi
  _DIFFER_ELAPSED=$(( $(date +%s) - _DIFFER_START ))
  if [[ $_DIFFER_EXIT -eq 0 ]]; then
    DIFFER_OUTPUT="$GENMCF_OUTPUT/differ_output"
    log_info "Step 2.4: Differ complete → $DIFFER_OUTPUT (elapsed=${_DIFFER_ELAPSED}s)"
  elif [[ $_DIFFER_EXIT -eq 1 ]]; then
    log_info "Step 2.4: No baseline for '$_DIFFER_DATASET_ID' — differ skipped; first run (elapsed=${_DIFFER_ELAPSED}s)"
  elif [[ $_DIFFER_EXIT -eq 124 ]]; then
    log_warn "Step 2.4: Differ timed out after ${_DIFFER_TIMEOUT_SEC}s — continuing without differ output (elapsed=${_DIFFER_ELAPSED}s)"
  else
    log_warn "Step 2.4: Differ failed (exit $_DIFFER_EXIT) — continuing without differ output (elapsed=${_DIFFER_ELAPSED}s)"
  fi
else
  log_info "Step 2.4: Differ skipped — no dataset_id (use --baseline-name for custom datasets)"
fi

# =============================================================================
# Step 3: Run import_validation
# =============================================================================
# Validate config template (structure and required keys)
STEP3_START=$(date +%s)
log_mem "step3_validation"
VALIDATE_CONFIG_SCRIPT="$SCRIPT_DIR/scripts/validate_config_template.py"
if [[ -f "$VALIDATE_CONFIG_SCRIPT" && -f "$VALIDATION_CONFIG" ]]; then
  if ! $PYTHON "$VALIDATE_CONFIG_SCRIPT" "$VALIDATION_CONFIG" 2>/dev/null; then
    log_error "Validation config failed template check. Run: $PYTHON $VALIDATE_CONFIG_SCRIPT $VALIDATION_CONFIG"
    $PYTHON "$VALIDATE_CONFIG_SCRIPT" "$VALIDATION_CONFIG" || true
    ensure_failure_report "Validation Config" "Validation config failed template check"
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

if [[ -n "$TMCF" && -f "$TMCF" && ${#CSVS[@]} -gt 0 ]]; then
  VALIDATION_ARGS+=(--tmcf="$TMCF" "${CSV_ARGS[@]}")
fi

# differ_output is optional (not available for new imports). When no baseline comparison exists,
# we pass empty_differ.csv so the DC validation runner can create the differ table in DuckDB
# without failing; the file has a header row (StatVar,DELETED,MODIFIED,ADDED) and no data.
# Override path via EMPTY_DIFFER_PATH if needed.
EMPTY_DIFFER="${EMPTY_DIFFER_PATH:-$SCRIPT_DIR/sample_data/empty_differ.csv}"
if [[ -n "$DIFFER_OUTPUT" && -e "$DIFFER_OUTPUT" ]]; then
  VALIDATION_ARGS+=(--differ_output="$DIFFER_OUTPUT")
elif [[ -f "$EMPTY_DIFFER" ]]; then
  VALIDATION_ARGS+=(--differ_output="$EMPTY_DIFFER")
else
  VALIDATION_ARGS+=(--differ_output=)
fi

# Preprocess summary_report.csv: rewrite year-only MinDate/MaxDate (YYYY) to YYYY-01-01.
# The DC validation runner (and pandas) can interpret bare YYYY as Unix epoch or invalid;
# normalizing to YYYY-01-01 ensures date parsing behaves correctly. If the upstream
# validator changes to accept year-only values, this workaround may need revisiting.
if [[ -n "$STATS_SUMMARY" && -f "$STATS_SUMMARY" ]]; then
  if $PYTHON -c '
import pandas as pd
import re
import sys

def normalize_year_only(v):
    s = str(v).strip()
    if re.fullmatch(r"\d{4}", s):
        return f"{s}-01-01"
    return s

summary_path = sys.argv[1]
df = pd.read_csv(summary_path)

if "MaxDate" in df.columns:
    df["MaxDate"] = df["MaxDate"].apply(normalize_year_only)
if "MinDate" in df.columns:
    df["MinDate"] = df["MinDate"].apply(normalize_year_only)

df.to_csv(summary_path, index=False)
' "$STATS_SUMMARY"; then
    :
  else
    log_warn "Failed to preprocess summary_report.csv date columns"
  fi
fi

# Orchestrator runs DC framework rules + our custom rules (e.g. STRUCTURAL_LINT_ERROR_COUNT), writes validation_output.json once
if $PYTHON "$SCRIPT_DIR/scripts/run_validation.py" "${VALIDATION_ARGS[@]}"; then
  RUNNER_EXIT=0
else
  RUNNER_EXIT=1
fi

# =============================================================================
# Step 2.25: Check counters match (warn-only; use genmcf report only)
# NumObservations and NumNodeSuccesses must come from the same genmcf output.
# =============================================================================
REPORT_FOR_COUNTERS=""
if [[ -n "$STATS_SUMMARY" && -f "$STATS_SUMMARY" ]]; then
  REPORT_FOR_COUNTERS="$(dirname "$STATS_SUMMARY")/report.json"
fi
if [[ -n "$REPORT_FOR_COUNTERS" && -f "$REPORT_FOR_COUNTERS" ]]; then
  if $PYTHON "$SCRIPT_DIR/scripts/check_counters_match.py" \
    --stats_summary="$STATS_SUMMARY" --report="$REPORT_FOR_COUNTERS"; then
    :
  else
    log_warn "Counters mismatch (NumObservations vs NumNodeSuccesses). This is advisory only. Resolution or existence checks may result in differing materialized node counts. This does not affect validation rule results."
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
    # apply_warn_only confirmed no blocking FAILED entries remain after warn_only
    # conversion.  The result is a pass regardless of RUNNER_EXIT: if the runner
    # originally produced FAILEDs that were all converted to WARNINGs here, the
    # pipeline should exit 0 (warnings-only is not a failure).
    # Crashes that write no output are caught above: apply_warn_only exits 1 when
    # the file is missing or unreadable.
    VALIDATION_RESULT=0
  else
    VALIDATION_RESULT=1
  fi
else
  VALIDATION_RESULT=$RUNNER_EXIT
fi
# Counters match check is warn-only (resolution instability should not hard-fail)
log_info "Step 3 completed in $(( $(date +%s) - STEP3_START ))s"

# Update baseline only when validation fully passed (after warn_only overrides).
# Never update on failure to avoid storing a bad import as the new baseline.
# When BASELINE_AUTO_UPDATE=false (set by the UI server), skip auto-update so the
# user can manually accept the baseline via the UI approval workflow.
if [[ -n "$_DIFFER_DATASET_ID" && "${BASELINE_AUTO_UPDATE:-true}" == "true" ]]; then
  if [[ "$VALIDATION_RESULT" -eq 0 ]]; then
    log_info "Updating baseline for '$_DIFFER_DATASET_ID'..."
    $PYTHON "$SCRIPT_DIR/scripts/run_differ.py" \
      --update_baseline \
      --current_mcf_dir="$GENMCF_OUTPUT" \
      --dataset_id="$_DIFFER_DATASET_ID" \
      ${RUN_ID:+--run_id="$RUN_ID"} \
      || log_warn "Baseline update failed (non-fatal)"
  else
    log_info "Skipping baseline update due to validation failure."
  fi
elif [[ -n "$_DIFFER_DATASET_ID" && "${BASELINE_AUTO_UPDATE:-true}" == "false" ]]; then
  log_info "Baseline auto-update disabled (BASELINE_AUTO_UPDATE=false). Accept via UI to update baseline."
fi

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
  log_mem "step4_done"
  log_info "Step 4 completed in $(( $(date +%s) - STEP4_START ))s"
fi

if [[ "$VALIDATION_RESULT" -eq 0 ]]; then
  log_info "Validation PASSED (no blocking rules)"
  echo ""
  echo "=========================================="
  echo -e "  ${GREEN}✓ Validation PASSED (no blocking rules)${NC}"
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
