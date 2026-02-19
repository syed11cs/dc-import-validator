# DC Import Validator

End-to-end validation pipeline and web UI for Data Commons imports. Runs dc-import (genmcf) and import_validation, with optional Gemini-based schema review, CSV quality checks, and per-run reporting (local or GCS-backed).

## Overview

| | |
|--|--|
| **What it does** | Validates TMCF + CSV before submission: preflight, optional Gemini Review, dc-import genmcf, import_validation, Go/No-Go HTML report. |
| **How to run** | **Docker** (recommended): one repo clone, no extra setup. **CLI / dev:** clone this repo + [datacommonsorg/data](https://github.com/datacommonsorg/data) for the validation runner. |
| **Output** | Validation report (blockers / warnings / passed), StatVar summary, lint summary, link to import tool report. Optional upload to GCS for Cloud Run. |

---

## Quick Start (Docker — recommended)

Run the app locally with no Python/Java setup. The image includes the app, dc-import JAR, and a sparse clone of [datacommonsorg/data](https://github.com/datacommonsorg/data) (tools/import_validation only).

```bash
# Clone this repo
git clone https://github.com/syed11cs/dc-import-validator.git
cd dc-import-validator

# Build and run (first build ~few minutes)
docker build -t dc-import-validator .
docker run --rm -p 8080:8080 -e GEMINI_API_KEY=your_key dc-import-validator
```

Open **http://localhost:8080**. Use built-in datasets (e.g. Child Birth) or upload your own TMCF + CSV.

**Apple Silicon (M1/M2/M3):** Build for Cloud Run’s platform:  
`docker build --platform linux/amd64 -t dc-import-validator .`

---

## Alternative: Run without Docker (CLI / development)

If you want to run the validation script and Web UI from your host (e.g. to change code or run integration tests), you need this repo and the Data Commons data repo for the **import_validation** runner. Built-in sample data (child_birth, child_birth_fail_*) lives in this repo.

**1. Clone both repos**

```bash
# This app
git clone https://github.com/syed11cs/dc-import-validator.git
cd dc-import-validator

# Data repo (for import_validation runner only; can be sibling or set PROJECTS_DIR)
git clone https://github.com/datacommonsorg/data.git ../datacommonsorg/data
```

Default layout assumed by the script: parent of `dc-import-validator` contains `datacommonsorg/data`. To use a different path, set `PROJECTS_DIR` (see [Environment variables (script / CLI)](#environment-variables-script--cli)).

**2. One-time setup**

```bash
chmod +x setup.sh run_e2e_test.sh run_ui.sh
./setup.sh
```

This creates a Python venv (`.venv/`), installs dependencies, and downloads the import tool JAR from [GitHub releases](https://github.com/datacommonsorg/import/releases) to `bin/`.

**3. Run validation (CLI)**

```bash
./run_e2e_test.sh child_birth
./run_e2e_test.sh child_birth_fail_min_value
./run_e2e_test.sh custom --tmcf=path/to/file.tmcf --csv=path/to/file.csv
```

**4. Run Web UI (local)**

```bash
./run_ui.sh
```

Open [http://localhost:8000](http://localhost:8000).

---

## Deploy to Cloud Run

The repo includes a **GitHub Actions** workflow (`.github/workflows/deploy-cloudrun.yml`) that builds the image, pushes to **Artifact Registry**, and deploys to an existing **Cloud Run** service on every push to `main`. No manual `gcloud` deploy needed once secrets are set.

- **One-time:** Create an Artifact Registry repo, a GCP service account (Artifact Registry Writer + Cloud Run Admin), and add GitHub secrets: `GCP_PROJECT_ID`, `GCP_SA_KEY`, and optionally `AR_LOCATION` (e.g. `us` if your AR is multi-region).
- **Then:** Push to `main` (or run the workflow manually from the Actions tab).

For step-by-step (Artifact Registry, service account, secrets, GCS bucket), see **docs/DEPLOY_CLOUD_RUN.md** in this repository (when the `docs/` folder is present).

---

## Repository structure

| Path | Purpose |
|------|---------|
| `sample_data/` | Built-in datasets (child_birth, child_birth_fail_*, child_birth_ai_demo); see [sample_data/README.md](sample_data/README.md). |
| `scripts/` | Preflight, CSV quality, config validation, LLM schema review, HTML report generation. |
| `ui/` | Web UI (FastAPI app, frontend, GCS upload/serve, validation runner). |
| `validation_configs/` | Rule config (`new_import_config.json`), warn-only overrides (`warn_only_rules.json`). |
| `run_e2e_test.sh` | Main CLI entrypoint. |
| `run_ui.sh` | Start Web UI locally (port 8000). |
| `Dockerfile` | Image for local run or Cloud Run (includes sparse data repo clone). |
| `.github/workflows/deploy-cloudrun.yml` | CI/CD: build → Artifact Registry → Cloud Run on push to `main`. |

When present, **docs/** contains the deploy guide (DEPLOY_CLOUD_RUN.md), architecture overview (PROJECT_OVERVIEW.md), and checklist mapping (CL_PR_CHECKLIST_MAPPING.md).

---

## Prerequisites

- **Docker (Quick Start):** Docker installed. No Python/Java on host required.
- **CLI / dev:** **Python 3**, **Java 11+** (17 recommended; Docker image uses 17). **datacommonsorg/data** clone for the import_validation runner (see [Alternative: Run without Docker](#alternative-run-without-docker-cli--development)).

### Gemini Review (LLM)

The Gemini Review step uses Google's Gemini API to check TMCF files for typos, schema issues, and naming conventions.

**Default behavior**

- **Enabled by default** when `GEMINI_API_KEY` or `GOOGLE_API_KEY` is set. The pipeline runs Gemini Review before genmcf.
- **Skipped automatically** when no API key is set; validation continues with genmcf and import_validation.
- **Disable explicitly** with `--no-llm-review` (CLI) or by turning off the checkbox in the Web UI.

**1. Get an API key**

- Go to [Google AI Studio](https://aistudio.google.com/apikey)
- Sign in with your Google account
- Create an API key

**2. Set the environment variable**

```bash
export GEMINI_API_KEY="your-api-key-here"
# or
export GOOGLE_API_KEY="your-api-key-here"
```

For a persistent setup, add the export to your shell profile (`~/.zshrc`, `~/.bashrc`, etc.):

```bash
echo 'export GEMINI_API_KEY="your-api-key-here"' >> ~/.zshrc
source ~/.zshrc
```

**3. Install the Gemini client** (if not already installed via setup):

```bash
pip install google-genai
```

**4. Run with Gemini Review**

```bash
./run_e2e_test.sh child_birth --llm-review
./run_e2e_test.sh child_birth --llm-review --model=gemini-3-pro-preview
```

### Installing Java (macOS)

```bash
brew install openjdk@17
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
```

Verify: `java -version` shows Java 11+ (17 recommended; Docker image uses 17).

## Usage

```bash
./run_e2e_test.sh [OPTIONS] [DATASET]
```

### Datasets

Child birth testdata (TMCF, CSV, stat vars MCF) lives in this repo at `sample_data/child_birth/`. Rule-test variants live in other `sample_data/` subfolders (same structure, modified to trigger specific rules).

| Dataset   | Description |
|-----------|-------------|
| `child_birth` | Child birth from this repo’s `sample_data/child_birth/` (clean; expect PASS). |
| `child_birth_fail_min_value` | Same base, one negative value (−1) and two large fluctuations → check_min_value FAIL; Data Fluctuation: 100%, 200%, 500%. |
| `child_birth_fail_units` | Same base, mixed units → check_unit_consistency FAIL. |
| `child_birth_fail_scaling_factor` | Same base, inconsistent scaling → check_scaling_factor_consistency FAIL. |
| `child_birth_ai_demo` | TMCF with schema issues and typos (missing dcs:, duplicate, typo) → Gemini Review finds issues. |
| `custom` | Your TMCF + CSV. Requires `--tmcf` and `--csv`. |

### What “bad” data is in each child_birth variant

Each **child_birth_fail_*** dataset is the same structure as **child_birth** (from this repo’s `sample_data/child_birth/`) but with **one intentional problem** so a specific validation rule fails. Use these to test or demo the pipeline.

| Dataset | What’s wrong | Why it fails |
|---------|----------------|--------------|
| **child_birth_fail_min_value** | **One negative value (−1)** and **two large fluctuations**. In `sample_data/child_birth_fail_min_value/child_birth_fail_min_value.csv`: USA 2023-01 `Count_BirthEvent_LiveBirth` is −1; USA 2023-03 `Count_Death` is 996000 (300% from 249000); USA 2023-04 `Count_Death_Upto1Years` is 5400 (200% from 1800). | **check_min_value** fails on the one negative. Data Fluctuation shows three tiers: 100%, 200%, 500%. |
| **child_birth_fail_units** | **Mixed units for the same StatVar.** The CSV has a `unit` column. Most rows have empty unit; one row (USA, 2023-02, `Count_Death`) has `unit = Percent`. Counts should not be in “Percent”; units must be consistent per StatVar. | **check_unit_consistency** fails when the same StatVar has different units (e.g. empty vs “Percent”). |
| **child_birth_fail_scaling_factor** | **Inconsistent scaling for the same StatVar.** The CSV has a `scalingFactor` column. For `Count_BirthEvent_LiveBirth`, one month (2023-02) has `scalingFactor = 100` while all other months have `1`. Scaling factor must be the same for a StatVar. | **check_scaling_factor_consistency** fails when the same StatVar has different scaling factors (e.g. 1 vs 100). |

**child_birth** (no “fail” in the name) uses the unmodified files from this repo's `sample_data/child_birth/` and has no such issues; it should pass validation (aside from optional lint warnings).

### Options

| Option              | Description |
|---------------------|-------------|
| `--tmcf PATH`       | TMCF file (for custom) |
| `--csv PATH`        | CSV file (for custom) |
| `--stat-vars-mcf PATH` | Optional stat vars MCF for schema conformance (custom or when dataset has it). |
| `--stat-vars-schema-mcf PATH` | Optional stat vars schema MCF for schema conformance. |
| `--config PATH`     | Validation config file |
| `--rules ID1,ID2`   | Run only these rules (comma-separated). |
| `--skip-rules ID1`  | Skip these rules (comma-separated). |
| `--llm-review`      | Run Gemini Review (schema/typo) on TMCF before validation (requires API key). Default: on. Use `--no-llm-review` to disable. |
| `--ai-advisory`     | If Gemini Review finds issues, continue pipeline (treat blockers as non-blocking). |
| `--model ID`        | Gemini model for Gemini Review (default: gemini-3-flash-preview) |
| `--help`            | Show help |

### Examples

```bash
./run_e2e_test.sh child_birth
./run_e2e_test.sh child_birth_fail_min_value   # Expect FAIL
./run_e2e_test.sh child_birth_fail_units        # Expect FAIL
./run_e2e_test.sh child_birth_fail_scaling_factor   # Expect FAIL
./run_e2e_test.sh child_birth_ai_demo   # TMCF issues → Gemini Review
./run_e2e_test.sh custom --tmcf=my.tmcf --csv=my.csv
./run_e2e_test.sh custom --tmcf=my.tmcf --csv=my.csv --stat-vars-mcf=path/to/stat_vars.mcf --stat-vars-schema-mcf=path/to/schema.mcf
```

### Schema MCF conformance

When a dataset has **stat_vars.mcf** and/or **stat_vars_schema.mcf** (e.g. from this repo's `sample_data/child_birth/` for the fail/AI-demo variants), the script runs **dc-import lint** with those MCFs before genmcf. That lint report is used by import_validation so schema conformance is enforced. For custom runs, pass `--stat-vars-mcf` and/or `--stat-vars-schema-mcf` to enable the same check.

## Pipeline

```
Preflight: import files exist and have .tmcf / .csv / .mcf extensions
CSV quality: duplicate columns, empty columns, duplicate rows, non-numeric value column
[Optional: Step 0 — Gemini Review]
[Optional: dc-import lint with stat_vars/schema MCFs when present]
TMCF + CSV  →  dc-import genmcf  →  report.json, summary_report.csv
Config template check (validation_config structure)
              import_validation  →  validation_output.json (Pass/Fail) → HTML report
```

### CSV data quality

Before Step 0 (LLM) and Step 1 (genmcf), the pipeline runs CSV quality checks:

- **Duplicate column names** — Header must not repeat column names.
- **Empty columns** — No column may be entirely empty (all cells empty/whitespace).
- **Duplicate rows** — No two rows may be identical (all column values the same).
- **Non-numeric value column** — The column named `value` (or `--value-column`) must contain only numbers or empty cells. Other columns are not checked here; dc-import and lint remain authoritative for schema.

Run manually: `python scripts/validate_csv_quality.py --csv path/to/file.csv [--value-column value]`

### Config template validation

Before running import_validation, the pipeline validates `validation_config.json` (or the config in use) against an expected template:

- Required top-level: `rules` (array).
- Each rule must have: `rule_id`, `description`, `validator`, `scope`, `params`.
- `rule_id` must be snake_case (e.g. `check_min_value`).
- `scope.data_source` must be one of: `stats`, `lint`, `differ`.

Run manually: `python scripts/validate_config_template.py validation_configs/new_import_config.json`

### File preflight

Before Step 0 (LLM) and Step 1 (genmcf), the pipeline checks that required import files exist and have the expected extensions:

- TMCF: `.tmcf` or `.mcf`
- CSV: `.csv`
- Optional stat_vars.mcf / stat_vars_schema.mcf: `.mcf`

Run manually: `python scripts/validate_import_files.py --tmcf path/to/file.tmcf --csv path/to/file.csv`

### Out of scope

- **Extreme values / outlier detection** — Not implemented. **check_min_value** enforces a minimum; a max or outlier rule would require a defined config (e.g. per-StatVar bounds).
- **Gemini review of validation_config.json** — Gemini Review applies only to the TMCF (schema/typos). Rule config logic is not reviewed.

### Checklist alignment

We map the import review checklists to this pipeline in **docs/CL_PR_CHECKLIST_MAPPING.md** (in the repo). In short: we cover StatVar definitions in stat_vars MCF (when provided), percent/rate measurementDenominator in stat_vars MCF (partial), counters and unit/scaling, and partially data holes, fluctuation, and report visibility; Gemini Review (TMCF) is also part of the pipeline. The doc lists what is applied, partial, and quick wins to add next.

## Output

Each dataset writes to its own folder under `output/`:

- **CLI:** Output goes to the canonical folder (e.g. `output/child_birth_genmcf/`, `output/custom_input/`).
- **Web UI:** Each run uses a per-run directory `output/{dataset}/{run_id}/` to avoid races when multiple runs overlap; after upload to GCS, artifacts are copied to the canonical folder so “latest” APIs still work.

| Dataset     | Output folder       | Contents |
|-------------|---------------------|----------|
| child_birth | `output/child_birth_genmcf/` | report.json, summary_report.csv, table_mcf_nodes_*.mcf, validation_output.json; when stat_vars.mcf exists, lint/report.json from schema run is used. |
| child_birth_fail_* | `output/child_birth_fail_*_genmcf/` | Same; inputs from sample_data/ (child_birth base with one intentional failure). |
| child_birth_ai_demo | `output/child_birth_ai_demo_genmcf/` | Same; TMCF/CSV from sample_data/ for Gemini Review demo. |
| custom      | `output/custom_input/` | report.json, summary_report.csv, table_mcf_nodes_*.mcf, validation_output.json; add --stat-vars-mcf/--stat-vars-schema-mcf for schema lint. |

## Configuration

- **child_birth**, **child_birth_fail_***, **child_birth_ai_demo**, and **custom** use `validation_configs/new_import_config.json`.

Validation configs define which rules run (e.g. min value, num observations, date checks).

### Warning vs Error hierarchy

Only **Errors** (FAILED) block; **Warnings** do not. Edit `validation_configs/warn_only_rules.json` to mark rules as non-blocking per dataset:

```json
{
  "child_birth": ["check_lint_error_count"],
  "child_birth_fail_min_value": ["check_lint_error_count"],
  "child_birth_fail_units": ["check_lint_error_count"],
  "child_birth_fail_scaling_factor": ["check_lint_error_count"],
  "child_birth_ai_demo": ["check_lint_error_count"],
  "custom": ["check_lint_error_count"]
}
```

Rules listed under a dataset are converted from FAILED → WARNING after validation. The HTML report shows Blockers, Warnings, and Passed separately.

## Web UI

A web interface lets you run validations and view reports in the browser.

### Starting the Web UI

```bash
./run_ui.sh
# or: uvicorn ui.server:app --reload --host 0.0.0.0 --port 8000
```

Open [http://localhost:8000](http://localhost:8000) in your browser.

`run_ui.sh` automatically installs UI dependencies (FastAPI, uvicorn, python-multipart) if needed.

### Features

| Feature | Description |
|---------|-------------|
| **Dataset selector** | Choose from built-in datasets (child_birth, child_birth_fail_*, child_birth_ai_demo) or Custom |
| **Gemini Review** | Step using Gemini to check TMCF for typos and schema issues (default: on). Requires GEMINI_API_KEY or GOOGLE_API_KEY. Model dropdown: gemini-3-flash-preview (default) or gemini-3-pro-preview. |
| **Run Validation** | Runs the full pipeline (LLM → genmcf → import_validation when LLM enabled). Use **Stop** or Esc to cancel. |
| **Log tab** | Terminal output with syntax highlighting (ERROR/WARN/INFO). Copy, expand, auto-scroll. |
| **Report tab** | Combined validation report with pass/fail, StatVar summary, lint issues, and import tool details |
| **Rules** | Select which rules to run. Check/uncheck rules; use Select all / Deselect all. At least one rule must be selected. |

### Custom Upload

Upload your own TMCF and CSV files for validation:

1. Select **Custom (Upload your own files)** from the dataset dropdown
2. Choose your TMCF file (`.tmcf` or `.mcf`)
3. Choose your CSV file (`.csv`)
4. Click **Run Validation**

- **Limit:** 50MB per file
- **Staging:** Uploaded files are written to `output/custom_upload/` (input.tmcf, input.csv, etc.; overwritten per run).
- **Output:** **CLI:** results go to `output/custom_input/` (each run overwrites). **Web UI:** per-run directory `output/custom/{run_id}/`, then copy to `output/custom_input/` when the run completes so “latest” APIs work. (Same pattern as built-in datasets: `output/{dataset}/{run_id}/`.)
- **Config:** Uses `new_import_config.json`; `check_lint_error_count` is treated as a warning
- **Optional Stat vars MCF / Stat vars schema MCF:** When provided, Gemini Review runs stat_vars MCF checks (name/description/alternateName; generated vs defined StatVars; percent/rate measurementDenominator). See [sample_data/README.md](sample_data/README.md) for test files.

### Validation Report

The Report tab shows a combined validation report that includes:

- **Validation results** — P0 Blockers (errors), Warnings (non-blocking), Passed checks
- **Import Run Info** — Input files (TMCF/CSV), generation duration, tool version
- **Key Counters** — Total observations, NumRowSuccesses, NumNodeSuccesses, NumPVSuccesses
- **StatVar Summary** — Table of StatVars with NumPlaces, NumObservations, Min/Max Value, Units, Dates
- **Lint Summary** — Counts of INFO, WARNING, ERROR from the import tool
- **Top Lint Issues** — First 10 issues (errors first) with file, line, and message

A **View full import tool report** link at the bottom opens the import tool's detailed report (counters, sample places, time series charts) in a new tab. The same file (`summary_report.html`) is also in each dataset's output folder.

### Logging

When the UI server starts, it assigns a **random server session ID** and configures logging by environment:

- **Cloud Run:** Logs go to **stdout** only (Cloud Run captures them and sends to Cloud Logging). No file handler (ephemeral disk).
- **Local / VM:** Logs are written to `logs/dc_import_validator.log` (daily rotation, 30 days) and to the console. Each line includes `session=<server_session_id>` and `request_id=<per-request_id>`.
- **Request / run IDs:** Every HTTP request gets a unique `request_id`; validation runs log `run_started` and `run_finished` with that ID (and `duration_sec`) so you can trace a run in the log file or in Cloud Logging. For validation runs, the HTTP `request_id` is the same as `run_id` (used in APIs and GCS paths).
- **Errors:** Unhandled errors are logged with `logger.exception(...)` so stack traces appear for debugging.
- **Log level:** Set `LOG_LEVEL` (e.g. `LOG_LEVEL=DEBUG`) to change verbosity; default is `INFO`.

**Reports in GCS (Cloud Run):** If you set **`GCS_REPORTS_BUCKET`** to a Cloud Storage bucket name, after each run the app uploads to `gs://bucket/reports/{run_id}/{dataset}/`: the HTML reports (`validation_report.html`, `summary_report.html`), JSON artifacts (`validation_output.json`, `report.json`, `schema_review.json` when present), and the input CSV as `input.csv` (for rule-failure enrichment when serving from GCS). Any instance can serve the report at `/report/{dataset}/{run_id}`. Create the bucket (e.g. `dc-import-validator-reports`), grant the Cloud Run service account **Storage Object Admin** on it, and set the env var when deploying.

**CLI (`run_e2e_test.sh`):** When run directly (e.g. `./run_e2e_test.sh child_birth`), the script assigns a **session ID** for that run and prefixes each `[INFO]`/`[WARN]`/`[ERROR]` line with `[session=<id>]`, and logs "Starting run (dataset=...)" so CLI-only runs can be correlated too. CLI logs use `[session=<id>]` for that CLI run; server logs use `session=<server_session_id>` and `request_id=<per-request>`.

### Environment variables (UI / Cloud Run)

| Variable | Required | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` or `GOOGLE_API_KEY` | For Gemini Review | API key for Gemini schema review (see Prerequisites). |
| `GCS_REPORTS_BUCKET` | For Cloud Run reports | Bucket name for uploading reports and input CSV; enables run_id-based report serving. |
| `LOG_LEVEL` | No | Log verbosity (e.g. `DEBUG`, `INFO`); default `INFO`. |
| `PORT` | Set by Cloud Run | Port the app listens on; Cloud Run sets this; local `run_ui.sh` uses 8000. |
| `VALIDATION_RUN_TIMEOUT_SEC` | No | Max run time in seconds (e.g. `3600`). If set, runs that exceed this are stopped and reported as timeout. Unset or `0` = no timeout. |

### Environment variables (script / CLI)

Used by `run_e2e_test.sh` and `setup.sh` when running from the command line:

| Variable | Required | Description |
|----------|----------|-------------|
| `PROJECTS_DIR` | For import_validation | Parent of repo root; script sets `DATA_REPO=$PROJECTS_DIR/datacommonsorg/data`. Default: parent of script dir. |
| `IMPORT_JAR_PATH` | No | Path to dc-import JAR; if unset, script uses `bin/datacommons-import-tool.jar` or downloads from GitHub. |
| `PYTHON` | No | Python interpreter for filter/validation scripts; default: `.venv/bin/python` or `python3`. |
| `EMPTY_DIFFER_PATH` | No | Path to empty differ CSV when no differ output; default: `sample_data/empty_differ.csv`. |

## Tests

Integration tests run the pipeline for key datasets and assert exit codes and output:

```bash
python tests/run_integration_tests.py
```

Requires the DC data repo at `../datacommonsorg/data` (or `PROJECTS_DIR`) for the **import_validation** runner, and that `./setup.sh` and `./run_e2e_test.sh` have been run at least once (venv, JAR).
