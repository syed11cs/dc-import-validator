#!/bin/bash
#
# batch/entrypoint.sh
#
# Cloud Batch container entrypoint for dc-import-validator.
#
# Responsibilities:
#   1. Validate required environment variables.
#   2. Write initial status.json to GCS (status: running).
#   3. Download input files from gs://<bucket>/inputs/<run_id>/ (custom datasets only).
#   4. Execute run_e2e_test.sh with the correct arguments.
#   5. Intercept ::STEP::N:Label markers from pipeline stdout and update status.json.
#   6. Capture structured failure events emitted by run_e2e_test.sh.
#   7. Upload reports to GCS via upload_reports_to_gcs() after pipeline exits.
#   8. Write final status.json (succeeded or failed) with failure detail if applicable.
#
# All stdout goes to Cloud Logging automatically when the Batch job logging policy
# is set to CLOUD_LOGGING.
#
# Required environment variables:
#   RUN_ID              - Unique run identifier (matches the GCS upload path)
#   GCS_REPORTS_BUCKET  - GCS bucket name (e.g. dc-import-validator-reports)
#   DATASET             - Dataset name: custom | child_birth | statistics_poland |
#                         finland_census | uae_population
#   TMCF_FILENAME       - TMCF filename inside inputs/<run_id>/ (required when DATASET=custom)
#   CSV_FILENAMES       - Comma-separated CSV filenames in inputs/<run_id>/ (required when DATASET=custom)
#
# Optional environment variables:
#   STAT_VARS_MCF_FILENAME        - Stat vars MCF filename (inside inputs/<run_id>/)
#   STAT_VARS_SCHEMA_MCF_FILENAME - Stat vars schema MCF filename (inside inputs/<run_id>/)
#   LLM_REVIEW                   - true|false  (default: false)
#   RULES_FILTER                  - Comma-separated rule IDs to run (passed to --rules)
#   SKIP_RULES_FILTER             - Comma-separated rule IDs to skip (passed to --skip-rules)
#   BASELINE_NAME                 - Differ baseline name (for custom datasets with differ)
#   BATCH_JOB_NAME                - Full Cloud Batch job resource name (written to status.json)
#   VM_TYPE                       - Machine type selected for this job (written to status.json)
#   IMPORT_RESOLUTION_MODE        - LOCAL|FULL  (default: LOCAL)
#   IMPORT_EXISTENCE_CHECKS       - true|false  (default: true)
#   JAVA_THREADS                  - genmcf thread count  (default: 2)
#   GEMINI_API_KEY / GOOGLE_API_KEY - Gemini API key (passed through; required if LLM_REVIEW=true)
#   DC_API_KEY                    - DC API key (required only for IMPORT_RESOLUTION_MODE=FULL)

set -uo pipefail

# ─── Required env vars ────────────────────────────────────────────────────────

RUN_ID="${RUN_ID:?RUN_ID is required}"
GCS_REPORTS_BUCKET="${GCS_REPORTS_BUCKET:?GCS_REPORTS_BUCKET is required}"
DATASET="${DATASET:?DATASET is required}"

# ─── Optional env vars with defaults ─────────────────────────────────────────

TMCF_FILENAME="${TMCF_FILENAME:-}"
CSV_FILENAMES="${CSV_FILENAMES:-}"
STAT_VARS_MCF_FILENAME="${STAT_VARS_MCF_FILENAME:-}"
STAT_VARS_SCHEMA_MCF_FILENAME="${STAT_VARS_SCHEMA_MCF_FILENAME:-}"
# GCS path mode: full gs:// URIs (alternative to GCS_INPUT_PREFIX + *_FILENAME vars).
# The Batch VM's attached service account (BATCH_SERVICE_ACCOUNT) is used for downloads
# via the GCE metadata server — not the Cloud Run service account.
TMCF_GCS_PATH="${TMCF_GCS_PATH:-}"
CSV_GCS_PATHS="${CSV_GCS_PATHS:-}"            # newline-separated full gs:// URIs
STAT_VARS_MCF_GCS_PATH="${STAT_VARS_MCF_GCS_PATH:-}"
STAT_VARS_SCHEMA_MCF_GCS_PATH="${STAT_VARS_SCHEMA_MCF_GCS_PATH:-}"
LLM_REVIEW="${LLM_REVIEW:-false}"
RULES_FILTER="${RULES_FILTER:-}"
SKIP_RULES_FILTER="${SKIP_RULES_FILTER:-}"
MERGED_CONFIG_GCS_PATH="${MERGED_CONFIG_GCS_PATH:-}"
BASELINE_NAME="${BASELINE_NAME:-}"
BATCH_JOB_NAME="${BATCH_JOB_NAME:-}"
VM_TYPE="${VM_TYPE:-}"

export IMPORT_RESOLUTION_MODE="${IMPORT_RESOLUTION_MODE:-LOCAL}"
export IMPORT_EXISTENCE_CHECKS="${IMPORT_EXISTENCE_CHECKS:-true}"
export JAVA_THREADS="${JAVA_THREADS:-2}"
# CSV auto-split controls (passed through to run_e2e_test.sh; off by default).
export CSV_SPLIT_ENABLED="${CSV_SPLIT_ENABLED:-false}"
export CSV_SPLIT_ROWS="${CSV_SPLIT_ROWS:-1000000}"
export CSV_SPLIT_THRESHOLD_ROWS="${CSV_SPLIT_THRESHOLD_ROWS:-5000000}"
export CSV_SPLIT_CLEANUP="${CSV_SPLIT_CLEANUP:-true}"

# Never auto-update baselines from Batch: user accepts via the UI.
export BASELINE_AUTO_UPDATE="false"

# Propagate RUN_ID and GCS_REPORTS_BUCKET so run_e2e_test.sh sub-invocations
# (e.g. run_differ.py, gcs_baselines.py) can resolve GCS paths correctly.
export RUN_ID
export GCS_REPORTS_BUCKET

# ─── Fixed paths ──────────────────────────────────────────────────────────────

readonly SCRIPT_DIR="/app/dc-import-validator"
WORKSPACE="/tmp/workspace/${RUN_ID}"

# run_e2e_test.sh writes local output here when RUN_ID is set.
PIPELINE_OUTPUT="${SCRIPT_DIR}/output/${DATASET}/${RUN_ID}"

# Temp file to capture the last structured failure event from pipeline stdout.
readonly FAILURE_EVENT_FILE="${WORKSPACE}/.failure_event.json"

# Record job start time once.
STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

mkdir -p "${WORKSPACE}"

# ─── Logging helper ───────────────────────────────────────────────────────────

log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [entrypoint] $*"
}

log "Starting: RUN_ID=${RUN_ID} DATASET=${DATASET} VM_TYPE=${VM_TYPE:-unset}"

# Log the active service account so GCS auth failures can be traced to the right identity.
_SA_EMAIL="$(curl -sf -H 'Metadata-Flavor: Google' \
    'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email' \
    2>/dev/null || echo 'unknown (metadata unavailable)')"
log "Auth: active_service_account=${_SA_EMAIL}"

# ─── write_status ─────────────────────────────────────────────────────────────
#
# Writes jobs/<RUN_ID>/status.json to GCS.
# All values are passed as environment variables to avoid shell quoting issues
# with special characters in failure messages.
#
# Usage: write_status <step> <step_label> <status> [failure_code] [failure_message]
#
# <step>          - Pipeline step number or string (e.g. "0", "2", "2.4", "4")
# <step_label>    - Human-readable label (e.g. "DC Import Tool")
# <status>        - "running" | "succeeded" | "failed"
# [failure_code]  - Machine-readable code (e.g. "DATA_PROCESSING_FAILED"); empty = null
# [failure_message] - Human-readable explanation; empty = null

write_status() {
    local step="$1"
    local step_label="$2"
    local status="$3"
    local failure_code="${4:-}"
    local failure_message="${5:-}"
    # failure_details_json: a JSON-encoded value (object, array, or literal null).
    # Defaults to null so intermediate "running" writes are unaffected.
    local failure_details_json="${6:-null}"

    STEP="$step" \
    STEP_LABEL="$step_label" \
    STATUS="$status" \
    FAILURE_CODE="$failure_code" \
    FAILURE_MESSAGE="$failure_message" \
    FAILURE_DETAILS_JSON="$failure_details_json" \
    STARTED_AT="$STARTED_AT" \
    python3 -c "
import json, os
from datetime import datetime, timezone
from google.cloud import storage

client = storage.Client()
bucket = client.bucket(os.environ['GCS_REPORTS_BUCKET'])

# Parse the pre-serialised details value back to a Python object so it is
# stored as structured JSON (not a string) in status.json.
_details_raw = os.environ.get('FAILURE_DETAILS_JSON', 'null')
try:
    failure_details = json.loads(_details_raw)
except (json.JSONDecodeError, ValueError):
    failure_details = None

data = {
    'run_id':          os.environ['RUN_ID'],
    'batch_job_name':  os.environ.get('BATCH_JOB_NAME', ''),
    'dataset':         os.environ['DATASET'],
    'vm_type':         os.environ.get('VM_TYPE', ''),
    'step':            os.environ['STEP'],
    'step_label':      os.environ['STEP_LABEL'],
    'status':          os.environ['STATUS'],
    'started_at':      os.environ['STARTED_AT'],
    'updated_at':      datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'failure_code':    os.environ['FAILURE_CODE'] or None,
    'failure_message': os.environ['FAILURE_MESSAGE'] or None,
    'failure_details': failure_details,
}

blob = bucket.blob('jobs/' + os.environ['RUN_ID'] + '/status.json')
blob.upload_from_string(json.dumps(data, indent=2), content_type='application/json')
print(
    '[status] step=' + data['step'] +
    ' status=' + data['status'] +
    ' label=' + data['step_label'],
    flush=True,
)
" 2>&1 || log "WARNING: write_status failed (step=${step} status=${status}) — continuing"
}

# ─── 1. Write initial status ──────────────────────────────────────────────────
#
# Use status="starting" (not "running") so the polling loop treats this as the
# pre-pipeline boot phase. The "running" status is only written once the pipeline
# emits its first ::STEP:: marker. This prevents pill 0 from prematurely consuming
# the step-0 transition with the label "Starting" before run_e2e_test.sh begins.

write_status "0" "Preparing validation environment" "starting"

# ─── 2. Validate inputs for custom datasets ───────────────────────────────────

if [[ "$DATASET" == "custom" ]]; then
    if [[ -z "$TMCF_GCS_PATH" && ( -z "$TMCF_FILENAME" || -z "$CSV_FILENAMES" ) ]]; then
        log "ERROR: TMCF_FILENAME+CSV_FILENAMES or TMCF_GCS_PATH+CSV_GCS_PATHS required when DATASET=custom"
        write_status "0" "Starting" "failed" \
            "MISSING_INPUTS" \
            "TMCF_FILENAME and CSV_FILENAMES must be set for custom datasets (or TMCF_GCS_PATH + CSV_GCS_PATHS)"
        exit 1
    fi
fi

# ─── 3. Download inputs from GCS (custom datasets only) ───────────────────────
#
# Built-in datasets (child_birth, statistics_poland, finland_census, uae_population)
# already have their source files inside the container at sample_data/; no download needed.

if [[ "$DATASET" == "custom" ]]; then
    if [[ -n "$TMCF_GCS_PATH" ]]; then
        # ── GCS path mode: download each file from its full gs:// URI. ────────────
        # Files may live in any GCS bucket. Authentication is via the Batch VM's
        # attached service account (BATCH_SERVICE_ACCOUNT), resolved through the GCE
        # metadata server — not the Cloud Run service account.
        log "GCS path mode: downloading inputs from explicit GCS URIs"

        TMCF_FILENAME="$(basename "${TMCF_GCS_PATH%%\?*}")"
        if ! python3 "${SCRIPT_DIR}/batch/gcs_download.py" \
                "$TMCF_GCS_PATH" "${WORKSPACE}/${TMCF_FILENAME}"; then
            log "ERROR: Failed to download TMCF from ${TMCF_GCS_PATH}"
            write_status "0" "Starting" "failed" \
                "DOWNLOAD_FAILED" \
                "Failed to download TMCF from ${TMCF_GCS_PATH}"
            exit 1
        fi

        _csv_basenames=()
        mapfile -t _CSV_PATH_LIST <<< "$CSV_GCS_PATHS"
        for _csv_path in "${_CSV_PATH_LIST[@]}"; do
            # Trim surrounding whitespace.
            _csv_path="${_csv_path#"${_csv_path%%[![:space:]]*}"}"
            _csv_path="${_csv_path%"${_csv_path##*[![:space:]]}"}"
            [[ -z "$_csv_path" ]] && continue
            _bn="$(basename "${_csv_path%%\?*}")"
            if ! python3 "${SCRIPT_DIR}/batch/gcs_download.py" \
                    "$_csv_path" "${WORKSPACE}/${_bn}"; then
                log "ERROR: Failed to download CSV from ${_csv_path}"
                write_status "0" "Starting" "failed" \
                    "DOWNLOAD_FAILED" \
                    "Failed to download CSV from ${_csv_path}"
                exit 1
            fi
            _csv_basenames+=("$_bn")
        done
        # Reconstruct CSV_FILENAMES from basenames for E2E_ARGS building below.
        CSV_FILENAMES="$(IFS=','; echo "${_csv_basenames[*]}")"

        # Optional stat vars files — non-fatal if download fails (pipeline will warn).
        if [[ -n "$STAT_VARS_MCF_GCS_PATH" ]]; then
            STAT_VARS_MCF_FILENAME="$(basename "${STAT_VARS_MCF_GCS_PATH%%\?*}")"
            python3 "${SCRIPT_DIR}/batch/gcs_download.py" \
                "$STAT_VARS_MCF_GCS_PATH" "${WORKSPACE}/${STAT_VARS_MCF_FILENAME}" || \
                log "WARNING: Failed to download stat vars MCF from ${STAT_VARS_MCF_GCS_PATH}"
        fi
        if [[ -n "$STAT_VARS_SCHEMA_MCF_GCS_PATH" ]]; then
            STAT_VARS_SCHEMA_MCF_FILENAME="$(basename "${STAT_VARS_SCHEMA_MCF_GCS_PATH%%\?*}")"
            python3 "${SCRIPT_DIR}/batch/gcs_download.py" \
                "$STAT_VARS_SCHEMA_MCF_GCS_PATH" "${WORKSPACE}/${STAT_VARS_SCHEMA_MCF_FILENAME}" || \
                log "WARNING: Failed to download stat vars schema MCF from ${STAT_VARS_SCHEMA_MCF_GCS_PATH}"
        fi
    else
        # ── Upload session mode: download all files from the GCS prefix. ──────────
        ACTUAL_PREFIX="${GCS_INPUT_PREFIX:-inputs/${RUN_ID}}"
        log "Downloading inputs from gs://${GCS_REPORTS_BUCKET}/${ACTUAL_PREFIX}/"

        if ! python3 "${SCRIPT_DIR}/batch/gcs_download.py" \
                "gs://${GCS_REPORTS_BUCKET}/${ACTUAL_PREFIX}/" "${WORKSPACE}/"; then
            log "ERROR: Input download failed from gs://${GCS_REPORTS_BUCKET}/${ACTUAL_PREFIX}/"
            write_status "0" "Starting" "failed" \
                "DOWNLOAD_FAILED" \
                "Failed to download input files from gs://${GCS_REPORTS_BUCKET}/${ACTUAL_PREFIX}/"
            exit 1
        fi
    fi
fi

# ─── 4. Build run_e2e_test.sh arguments ───────────────────────────────────────

E2E_ARGS=()

if [[ "$DATASET" == "custom" ]]; then
    E2E_ARGS+=("custom")
    E2E_ARGS+=("--tmcf=${WORKSPACE}/${TMCF_FILENAME}")

    # Split comma-separated CSV_FILENAMES into individual --csv flags.
    IFS=',' read -ra _CSV_LIST <<< "$CSV_FILENAMES"
    for _csv in "${_CSV_LIST[@]}"; do
        # Trim surrounding whitespace in case the caller included spaces.
        _csv="${_csv#"${_csv%%[![:space:]]*}"}"
        _csv="${_csv%"${_csv##*[![:space:]]}"}"
        [[ -n "$_csv" ]] && E2E_ARGS+=("--csv=${WORKSPACE}/${_csv}")
    done

    [[ -n "$STAT_VARS_MCF_FILENAME" ]] && \
        E2E_ARGS+=("--stat-vars-mcf=${WORKSPACE}/${STAT_VARS_MCF_FILENAME}")
    [[ -n "$STAT_VARS_SCHEMA_MCF_FILENAME" ]] && \
        E2E_ARGS+=("--stat-vars-schema-mcf=${WORKSPACE}/${STAT_VARS_SCHEMA_MCF_FILENAME}")
    [[ -n "$BASELINE_NAME" ]] && \
        E2E_ARGS+=("--baseline-name=${BASELINE_NAME}")
else
    # Built-in dataset: pass the name; run_e2e_test.sh resolves the source files.
    E2E_ARGS+=("$DATASET")
fi

# LLM review flag.
if [[ "$LLM_REVIEW" == "true" ]]; then
    E2E_ARGS+=("--llm-review")
else
    E2E_ARGS+=("--no-llm-review")
fi

# When a merged config is present it already encodes the full rule selection
# (filtered built-in rules + all custom SQL rules). Passing --rules= on top would
# re-filter the merged config by built-in IDs, silently dropping custom rules.
# Rule filter args are only needed when no merged config is in use.
if [[ -z "$MERGED_CONFIG_GCS_PATH" ]]; then
    [[ -n "$RULES_FILTER" ]] && E2E_ARGS+=("--rules=${RULES_FILTER}")
    [[ -n "$SKIP_RULES_FILTER" ]] && E2E_ARGS+=("--skip-rules=${SKIP_RULES_FILTER}")
fi

# When the UI submitted custom SQL rules or a rule filter, the server pre-merged
# them into a config JSON and uploaded it to GCS. Download it here and pass
# --config= so the pipeline uses the merged rule set without touching the baked-in
# config file. Merging is done entirely on the server; this block only downloads.
if [[ -n "$MERGED_CONFIG_GCS_PATH" ]]; then
    _MERGED_CONFIG="/tmp/validation_config_${RUN_ID}.json"
    if ! python3 "${SCRIPT_DIR}/batch/gcs_download.py" \
            "$MERGED_CONFIG_GCS_PATH" "$_MERGED_CONFIG"; then
        log "ERROR: Failed to download merged config from GCS: ${MERGED_CONFIG_GCS_PATH}"
        log "ERROR: Cannot proceed — custom rules would be silently skipped. Aborting."
        write_status "0" "Starting" "failed" \
            "CONFIG_DOWNLOAD_FAILED" \
            "Failed to download merged validation config from GCS: ${MERGED_CONFIG_GCS_PATH}"
        exit 1
    fi
    E2E_ARGS+=("--config=${_MERGED_CONFIG}")
    log "Downloaded merged config from GCS: ${MERGED_CONFIG_GCS_PATH}"
    # Log which custom SQL rules are present in the merged config for debugging.
    _custom_rule_ids="$(MERGED_CONFIG="$_MERGED_CONFIG" python3 <<'PYEOF' 2>/dev/null || echo '(parse error)'
import json, os
try:
    with open(os.environ['MERGED_CONFIG']) as f:
        rules = json.load(f).get('rules', [])
    ids = [r['rule_id'] for r in rules if r.get('validator') == 'SQL_VALIDATOR']
    print(', '.join(ids) if ids else '(none)')
except Exception as e:
    print('(could not parse: ' + str(e) + ')')
PYEOF
)"
    log "Custom SQL rules in config: ${_custom_rule_ids}"
fi

log "Running: ${SCRIPT_DIR}/run_e2e_test.sh ${E2E_ARGS[*]}"

# ─── 5. Execute pipeline with step interception ───────────────────────────────
#
# Pipe all pipeline output through a while-read loop that:
#   - Echoes every line to stdout (Cloud Logging captures it).
#   - Detects ::STEP::N:Label markers and writes status.json.
#   - Captures the last structured {"t":"failure",...} event to a temp file
#     so we can extract failure_code and failure_message after the pipe exits.
#
# set -e is intentionally NOT active here so we can capture PIPESTATUS[0].
# set -u remains active; set -o pipefail is suspended over the pipeline so the
# pipe itself does not abort the script — we handle the exit code manually.

set +o pipefail
set +e

bash "${SCRIPT_DIR}/run_e2e_test.sh" "${E2E_ARGS[@]}" 2>&1 | \
while IFS= read -r line; do
    # Pass every line through to stdout for Cloud Logging.
    echo "$line"

    # Detect step progress markers emitted by run_e2e_test.sh.
    # Format: ::STEP::N:Label  or  ::STEP::N  (label is optional)
    if [[ "$line" =~ ::STEP::([0-9.]+):?(.*) ]]; then
        _step="${BASH_REMATCH[1]}"
        _label="${BASH_REMATCH[2]}"
        [[ -z "$_label" ]] && _label="Step ${_step}"
        write_status "$_step" "$_label" "running" || true
    fi

    # Capture the last structured failure event emitted by emit_failure()
    # in run_e2e_test.sh. These are single-line JSON objects of the form:
    # {"t":"failure","code":"...","step":N,"message":"..."}
    if [[ "$line" == '{"t":"failure"'* ]]; then
        printf '%s' "$line" > "${FAILURE_EVENT_FILE}"
    fi
done

# Capture run_e2e_test.sh exit code (left side of pipe).
PIPELINE_EXIT="${PIPESTATUS[0]}"

set -o pipefail
set -e

log "Pipeline exited with code ${PIPELINE_EXIT}"

# ─── 6. Extract failure detail if pipeline failed ─────────────────────────────

FAILURE_CODE=""
FAILURE_MESSAGE=""
FAILURE_DETAILS_JSON="null"   # JSON-encoded details object, or literal null

if [[ $PIPELINE_EXIT -ne 0 ]]; then
    # Priority 1: structured failure event captured from pipeline stdout.
    # Extract code, message, and details in a single Python call (one file read).
    if [[ -f "$FAILURE_EVENT_FILE" ]]; then
        _extracted="$(python3 -c "
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    code    = d.get('code', '')
    message = d.get('message', '')
    details = d.get('details')   # present for CSV_QUALITY_FAILED, PREFLIGHT_FAILED
    # Print tab-separated so we can split on the shell side without ambiguity.
    # details is re-serialised so newlines inside values don't break the split.
    print(code + '\t' + message + '\t' + (json.dumps(details) if details is not None else 'null'))
except Exception:
    print('\t\tnull')
" "$FAILURE_EVENT_FILE" 2>/dev/null || printf '\t\tnull')"
        IFS=$'\t' read -r FAILURE_CODE FAILURE_MESSAGE FAILURE_DETAILS_JSON <<< "$_extracted"
    fi

    # Priority 2: pipeline_failure.json written by ensure_failure_report() in
    # run_e2e_test.sh. Stage maps to a code; reason is the human-readable message.
    if [[ -z "$FAILURE_CODE" && -f "${PIPELINE_OUTPUT}/pipeline_failure.json" ]]; then
        FAILURE_CODE="$(python3 -c "
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    stage = d.get('stage', 'PIPELINE_FAILED')
    print(stage.upper().replace(' ', '_'))
except Exception:
    pass
" "${PIPELINE_OUTPUT}/pipeline_failure.json" 2>/dev/null || true)"
        FAILURE_MESSAGE="$(python3 -c "
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    print(d.get('reason', ''))
except Exception:
    pass
" "${PIPELINE_OUTPUT}/pipeline_failure.json" 2>/dev/null || true)"
    fi

    # Priority 3: CONFIG_ERROR or FAILED rules in validation_output.json.
    # Surfaces the actual SQL execution error instead of the generic exit-code message.
    if [[ -z "$FAILURE_CODE" && -f "${PIPELINE_OUTPUT}/validation_output.json" ]]; then
        FAILURE_CODE="$(python3 -c "
import json, sys
try:
    results = json.load(open(sys.argv[1]))
    for r in results:
        if r.get('status') == 'CONFIG_ERROR':
            print('CONFIG_ERROR')
            sys.exit(0)
    for r in results:
        if r.get('status') == 'FAILED':
            print('VALIDATION_FAILED')
            sys.exit(0)
except Exception:
    pass
" "${PIPELINE_OUTPUT}/validation_output.json" 2>/dev/null || true)"
        FAILURE_MESSAGE="$(python3 -c "
import json, sys
try:
    results = json.load(open(sys.argv[1]))
    for r in results:
        if r.get('status') in ('CONFIG_ERROR', 'FAILED'):
            rid = r.get('validation_name', '')
            msg = r.get('message', '')
            label = 'SQL config error' if r.get('status') == 'CONFIG_ERROR' else 'Rule failed'
            parts = [label]
            if rid:
                parts.append(rid)
            if msg:
                parts.append(msg)
            print(': '.join(parts))
            sys.exit(0)
except Exception:
    pass
" "${PIPELINE_OUTPUT}/validation_output.json" 2>/dev/null || true)"
    fi

    # Fallback: generic failure with exit code.
    [[ -z "$FAILURE_CODE" ]] && FAILURE_CODE="PIPELINE_FAILED"
    [[ -z "$FAILURE_MESSAGE" ]] && FAILURE_MESSAGE="Pipeline exited with code ${PIPELINE_EXIT}"

    log "Failure detail: code=${FAILURE_CODE} message=${FAILURE_MESSAGE}"
fi

# ─── 7. Upload reports to GCS ─────────────────────────────────────────────────
#
# Runs on both success and failure paths so that partial reports (e.g. a failed
# genmcf run that still produced validation_report.html via ensure_failure_report)
# are available for the user to inspect in the UI.
#
# Uses upload_reports_to_gcs() from ui/gcs_reports.py, which already covers:
# validation_report.html, summary_report.html, validation_output.json, report.json,
# schema_review.json, validation_warnings_and_advisories.csv, input.csv, differ_output/.

log "Uploading reports from ${PIPELINE_OUTPUT}/"

PIPELINE_OUTPUT="$PIPELINE_OUTPUT" python3 -c "
import os, sys
from pathlib import Path
sys.path.insert(0, '/app/dc-import-validator')
from ui.gcs_reports import upload_reports_to_gcs

output_dir = Path(os.environ['PIPELINE_OUTPUT'])
if not output_dir.exists():
    print(
        '[upload] Output directory not found: ' + str(output_dir) +
        ' — nothing to upload',
        flush=True,
    )
    sys.exit(0)

uploaded = upload_reports_to_gcs(
    output_dir,
    os.environ['RUN_ID'],
    os.environ['DATASET'],
)
print('[upload] Reports uploaded: ' + str(uploaded), flush=True)
" 2>&1 || log "WARNING: Report upload failed — results may not be visible in the UI"

# ─── 8. Write final status ────────────────────────────────────────────────────

if [[ $PIPELINE_EXIT -eq 0 ]]; then
    log "Validation succeeded"
    write_status "4" "Results" "succeeded"
else
    log "Validation failed: code=${FAILURE_CODE}"
    write_status "4" "Results" "failed" "$FAILURE_CODE" "$FAILURE_MESSAGE" "$FAILURE_DETAILS_JSON"
fi

exit $PIPELINE_EXIT
