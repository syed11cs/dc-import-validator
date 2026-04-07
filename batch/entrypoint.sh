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
LLM_REVIEW="${LLM_REVIEW:-false}"
RULES_FILTER="${RULES_FILTER:-}"
SKIP_RULES_FILTER="${SKIP_RULES_FILTER:-}"
BASELINE_NAME="${BASELINE_NAME:-}"
BATCH_JOB_NAME="${BATCH_JOB_NAME:-}"
VM_TYPE="${VM_TYPE:-}"

export IMPORT_RESOLUTION_MODE="${IMPORT_RESOLUTION_MODE:-LOCAL}"
export IMPORT_EXISTENCE_CHECKS="${IMPORT_EXISTENCE_CHECKS:-true}"
export JAVA_THREADS="${JAVA_THREADS:-2}"

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

    STEP="$step" \
    STEP_LABEL="$step_label" \
    STATUS="$status" \
    FAILURE_CODE="$failure_code" \
    FAILURE_MESSAGE="$failure_message" \
    STARTED_AT="$STARTED_AT" \
    python3 -c "
import json, os
from datetime import datetime, timezone
from google.cloud import storage

client = storage.Client()
bucket = client.bucket(os.environ['GCS_REPORTS_BUCKET'])

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

write_status "0" "Starting" "running"

# ─── 2. Validate inputs for custom datasets ───────────────────────────────────

if [[ "$DATASET" == "custom" ]]; then
    if [[ -z "$TMCF_FILENAME" || -z "$CSV_FILENAMES" ]]; then
        log "ERROR: TMCF_FILENAME and CSV_FILENAMES are required when DATASET=custom"
        write_status "0" "Starting" "failed" \
            "MISSING_INPUTS" \
            "TMCF_FILENAME and CSV_FILENAMES must be set for custom datasets"
        exit 1
    fi
fi

# ─── 3. Download inputs from GCS (custom datasets only) ───────────────────────
#
# Built-in datasets (child_birth, statistics_poland, finland_census, uae_population)
# already have their source files inside the container at sample_data/; no download needed.

if [[ "$DATASET" == "custom" ]]; then
    ACTUAL_PREFIX="${GCS_INPUT_PREFIX:-inputs/${RUN_ID}}"
    log "Downloading inputs from gs://${GCS_REPORTS_BUCKET}/${ACTUAL_PREFIX}/"

    WORKSPACE="$WORKSPACE" python3 -c "
import os, sys
from pathlib import Path
from google.cloud import storage

client  = storage.Client()
bucket  = client.bucket(os.environ['GCS_REPORTS_BUCKET'])
# GCS_INPUT_PREFIX lets the caller (batch_runner) specify where input files
# live without constraining the upload path.  Falls back to the convention
# used by direct uploads: inputs/<run_id>/.
prefix  = os.environ.get('GCS_INPUT_PREFIX') or ('inputs/' + os.environ['RUN_ID'])
prefix  = prefix.rstrip('/') + '/'
dest    = Path(os.environ['WORKSPACE'])
dest.mkdir(parents=True, exist_ok=True)

blobs = [b for b in bucket.list_blobs(prefix=prefix) if not b.name.endswith('/')]
if not blobs:
    print(
        'ERROR: No input files found at gs://' +
        os.environ['GCS_REPORTS_BUCKET'] + '/' + prefix,
        flush=True,
    )
    sys.exit(1)

for blob in blobs:
    filename = blob.name.split('/')[-1]
    if not filename:
        continue
    local_path = dest / filename
    blob.download_to_filename(str(local_path))
    size_mb = (blob.size or 0) / (1024 * 1024)
    print(f'[download] {blob.name} -> {local_path} ({size_mb:.1f} MB)', flush=True)

print(f'[download] Complete: {len(blobs)} file(s) downloaded', flush=True)
" 2>&1

    if [[ $? -ne 0 ]]; then
        log "ERROR: Input download failed"
        write_status "0" "Starting" "failed" \
            "DOWNLOAD_FAILED" \
            "Failed to download input files from gs://${GCS_REPORTS_BUCKET}/${ACTUAL_PREFIX}/"
        exit 1
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

# Rule filters are mutually exclusive; batch_runner enforces this before submission.
[[ -n "$RULES_FILTER" ]] && E2E_ARGS+=("--rules=${RULES_FILTER}")
[[ -n "$SKIP_RULES_FILTER" ]] && E2E_ARGS+=("--skip-rules=${SKIP_RULES_FILTER}")

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

if [[ $PIPELINE_EXIT -ne 0 ]]; then
    # Priority 1: structured failure event captured from pipeline stdout.
    if [[ -f "$FAILURE_EVENT_FILE" ]]; then
        FAILURE_CODE="$(python3 -c "
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    print(d.get('code', ''))
except Exception:
    pass
" "$FAILURE_EVENT_FILE" 2>/dev/null || true)"
        FAILURE_MESSAGE="$(python3 -c "
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    print(d.get('message', ''))
except Exception:
    pass
" "$FAILURE_EVENT_FILE" 2>/dev/null || true)"
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
    write_status "4" "Results" "failed" "$FAILURE_CODE" "$FAILURE_MESSAGE"
fi

exit $PIPELINE_EXIT
