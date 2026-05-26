DC Import Validator 🛡️
End-to-end validation pipeline for Data Commons imports — catch issues before they reach production.

The DC Import Validator automates end-to-end Data Commons import validation — from TMCF/CSV quality checks to schema validation — with optional AI-powered review and rich HTML reports.

Prevent bad imports from reaching production by catching schema issues, data inconsistencies, and statistical anomalies early.

![Docker](https://img.shields.io/badge/-Docker-2496ED?logo=docker&logoColor=white)
![Cloud Run](https://img.shields.io/badge/-Cloud%20Run-4285F4?logo=google-cloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3-blue?logo=python)

### ✨ Features

| Feature | Description |
|---------|-------------|
| 🔍 Comprehensive Validation | Preflight checks, CSV quality, TMCF validation, schema conformance, and Data Commons import_validation |
| 🤖 AI Schema Review | Gemini-powered review (requires API key) catches typos and schema issues before validation |
| 📊 Rich Reports | HTML reports with blockers/warnings, StatVar summaries, schema errors, and import tool integration |
| 🚀 Multiple Run Modes | Docker (zero setup), CLI (development), or Cloud Run (production) |
| ☁️ Cloud Ready | Deploy to Cloud Run with automatic GCS report storage and CI/CD via GitHub Actions |
| ⚡ Cloud Batch | Large validations automatically dispatched to Cloud Batch VMs (up to 64 vCPUs) when GCS is configured |
| 🔧 Custom Validation Rules | Add natural-language validation rules in the UI — converted to SQL by Gemini and run against your data |

### 🚀 Quick Start (Recommended: Docker)

Run the validator with zero local setup.

```bash
git clone https://github.com/syed11cs/dc-import-validator.git
cd dc-import-validator
docker build -t dc-import-validator .
docker run --rm -p 8080:8080 dc-import-validator
```

Open http://localhost:8080 and start validating.

### 🤖 Enable AI Review (Optional)

To use Gemini for schema and typo review, set a Gemini API key:

```bash
docker run --rm -p 8080:8080 \
  -e GEMINI_API_KEY=your_key \
  dc-import-validator
```

Replace `your_key` with an API key from [Google AI Studio](https://aistudio.google.com/apikey).

Without an API key the validator runs fully — the Gemini review step is skipped.

#### Platform notes

**Apple Silicon (M1/M2/M3)** — If building for Cloud Run compatibility:

```bash
docker build --platform linux/amd64 -t dc-import-validator .
```

### 📖 Table of Contents

- [Features](#-features)
- [Quick Start](#-quick-start-recommended-docker)
- [Web UI](#-web-ui)
- [CLI Usage](#-cli-usage)
- [Deployment](#-deployment)
- [Pipeline Deep Dive](#-pipeline-deep-dive)
- [Configuration](#-configuration)
- [Development](#-development)
- [Test Datasets](#-test-datasets)
- [FAQ](#-faq)

### 🖥️ Web UI

The web interface makes validation accessible to everyone:

```bash
./run_ui.sh
# Then open http://localhost:8000
```

#### UI Features

| Feature | Description |
|---------|-------------|
| Dataset Browser | Test with built-in dataset (child_birth) or upload your own |
| Interactive Rules | Select which validation rules to run with checkbox interface |
| Live Logs | Real-time terminal output with syntax highlighting and copy support |
| Rich Reports | Combined view of blockers, warnings, StatVar summaries, and schema errors |
| Gemini Integration | AI review runs when `GEMINI_API_KEY` or `GOOGLE_API_KEY` is set; uses `gemini-2.5-pro` by default (automatic fallback to `gemini-2.5-flash` on quota/availability errors); override with `--model` |
| Run Management | Cancel long-running validations, view history |

#### Upload Custom Files

1. Select **Custom (Upload your own files)** from the dataset dropdown.
2. Upload your TMCF (.tmcf/.mcf) and CSV (.csv) files.
3. (Optional) Add StatVars MCF for enhanced schema validation.
4. Click **Run Validation**.

File limit: 100 GB total per upload session (TMCF + all CSVs combined). Multiple CSV files can be uploaded.

When `GCS_REPORTS_BUCKET` is configured, files are uploaded directly to Google Cloud Storage using signed URLs, bypassing the Cloud Run 32 MB HTTP request limit.

### 💻 CLI Usage

Perfect for automation, CI/CD, or power users:

```bash
# Setup (one-time)
./setup.sh

# Validate built-in dataset
./run_e2e_test.sh child_birth                    # Sample dataset → PASS

# Validate your own data
./run_e2e_test.sh custom --tmcf=path/to/data.tmcf --csv=path/to/data.csv

# With AI review
./run_e2e_test.sh child_birth --llm-review --model=gemini-2.5-pro

# Run specific rules only
./run_e2e_test.sh child_birth --rules=check_min_value,check_unit_consistency
```

#### CLI Options

| Option | Description |
|--------|-------------|
| `--tmcf PATH` | TMCF file (for custom datasets) |
| `--csv PATH` | CSV file (for custom datasets; repeatable for multiple CSVs) |
| `--stat-vars-mcf PATH` | Optional StatVars MCF for schema conformance |
| `--stat-vars-schema-mcf PATH` | Optional schema MCF |
| `--config PATH` | Use a custom validation config file |
| `--rules ID1,ID2` | Run only these rules |
| `--skip-rules ID1,ID2` | Skip these rules |
| `--llm-review` | Enable Gemini Review (requires API key) |
| `--no-llm-review` | Disable Gemini Review |
| `--model ID` | Gemini model — default: `gemini-2.5-pro` (falls back to `gemini-2.5-flash` on quota/availability errors); stable values: `gemini-2.5-flash`, `gemini-2.5-pro` |
| `--baseline-name NAME` | Name used to identify the differ baseline for custom datasets |
| `--help` | Show help |
### ☁️ Deployment

#### Deploy to Cloud Run (Automated)

The repository includes GitHub Actions for zero-touch deployment.

**One-time setup:**

1. Create Artifact Registry repo.
2. Create GCS bucket for reports (optional but recommended).
3. Create service account with necessary permissions.
4. Add GitHub secrets: `GCP_PROJECT_ID`, `GCP_SA_KEY` (JSON key). Optional: `GCP_REGION`, `AR_LOCATION` (e.g. `us` for multi-region AR repos; defaults to `GCP_REGION` or `us-central1`).

Push to `main` — automatic build and deploy!

See [docs/DEPLOY_CLOUD_RUN.md](docs/DEPLOY_CLOUD_RUN.md) for detailed instructions.

#### Environment Variables

All supported environment variables in one place. See `.env.example` for an optional template.

| Variable | Required | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` | For AI review | Google AI Studio API key for Gemini schema review |
| `GOOGLE_API_KEY` | Alternative | Alternative API key variable (used if `GEMINI_API_KEY` not set) |
| `DC_API_KEY` | Required for FULL mode | Data Commons API key used by Java import tool for Recon and existence checks |
| `GCS_REPORTS_BUCKET` | For Cloud Run | GCS bucket for validation report storage **and** large-file upload sessions. When set, the UI uploads files directly to GCS via signed URLs before triggering validation, bypassing the Cloud Run 32 MB HTTP request limit. Upload/serve fails clearly if the bucket is not accessible. |
| `DATA_REPO` | No | Path to `datacommonsorg/data` clone (default: `../datacommonsorg/data` from project root) |
| `VALIDATION_RUN_TIMEOUT_SEC` | No | Max validation run time in seconds (e.g. `3600`); unset = no timeout |
| `MAX_CONCURRENT_RUNS` | No | Max simultaneous validation runs (default: `1`, min: `1`). Matches Cloud Run concurrency=1 (one JVM per instance). Increase on larger hosts if memory allows. Returns HTTP 429 when at capacity. |
| `JAVA_THREADS` | No | Number of threads for dc-import genmcf CSV processing (default: `2`). Parallelism is file-level — requires multiple CSV files or CSV auto-splitting to benefit. Higher values increase peak JVM memory proportionally. |
| `CSV_SPLIT_ENABLED` | No | Set to `true` to auto-split a single large CSV into shards before Step 2, enabling genmcf thread parallelism. Default: `false`. See [CSV Auto-Splitting](#csv-auto-splitting-for-large-imports). |
| `IMPORT_RESOLUTION_MODE` | No | Java import tool resolution mode (default: `LOCAL`) |
| `IMPORT_EXISTENCE_CHECKS` | No | Java import tool existence checks. The UI toggle (Import Options → Enable Data Commons existence checks) overrides this per-run; the toggle defaults to OFF for performance. Server-level default: `true` when running via CLI. |
| `LOG_LEVEL` | No | Application log level: `DEBUG`, `INFO` (default), `WARNING` |
| `KEEP_RUN_DIR` | No | Set to `1` to preserve per-run output directories after cleanup (useful for debugging) |
| `BATCH_IMAGE_URI` | No | Docker image URI used for Cloud Batch jobs. Auto-set from `GCP_PROJECT_ID`/`GCP_REGION`/`AR_LOCATION` at deploy time. Override if your image lives in a different registry. |
| `BATCH_PROVISIONING_MODEL` | No | Cloud Batch VM provisioning: `STANDARD` (default, stable) or `SPOT` (~60–90% cheaper but may be preempted) |
| `GENMCF_PROFILE` | No | Set to `true` to enable JFR CPU/lock profiling and GC logging during Step 2 (genmcf). Output: `genmcf_profile.jfr` + `gc.log`. ~1–5% overhead. |

#### CSV Auto-Splitting for Large Imports

**Why it exists:** `genmcf --num-threads` parallelizes at the *file* level — one thread per CSV file. A single large CSV (e.g. 38 M rows, ~28 GB) keeps all threads idle except one, regardless of machine size. Splitting the CSV into same-schema shards lets genmcf process them in parallel and actually use the configured thread count.

**How it works:** When enabled, CSV validation and splitting happen together in Step 1.5 (`scripts/validate_and_split.py`) — a single streaming pass that validates quality and partitions the CSV into N shards. Each shard has an identical header and `rows_per_shard` data rows. Steps 0 and 1 always run on the original file. After genmcf completes, shards are deleted.

**This is off by default.** Enable it only when you have a single large CSV and want to benchmark or improve throughput.

| Variable | Default | Description |
|---|---|---|
| `CSV_SPLIT_ENABLED` | `false` | Set to `true` to enable splitting |
| `CSV_SPLIT_ROWS` | adaptive | Data rows per shard. Leave unset for adaptive sizing (targets `CSV_SPLIT_TARGET_SHARDS_PER_THREAD × JAVA_THREADS` shards from file size; clamped to [500 000, 5 000 000]). Set to a positive integer to override. |
| `CSV_SPLIT_TARGET_SHARDS_PER_THREAD` | `2` | Adaptive shard multiplier: `target_shards = JAVA_THREADS × this`. Supports decimals. Ignored when `CSV_SPLIT_ROWS` is set explicitly. |
| `CSV_SPLIT_THRESHOLD_ROWS` | `5000000` | Skip split if source CSV has fewer rows than this |
| `CSV_SPLIT_CLEANUP` | `true` | Set to `false` to preserve shards after Step 2 (debugging) |

**CLI benchmark examples:**

```bash
# Baseline (no splitting)
CSV_SPLIT_ENABLED=false JAVA_THREADS=32 \
  ./run_e2e_test.sh custom --tmcf=data.tmcf --csv=large.csv --no-llm-review

# Split into 1 M-row shards, 32 threads
CSV_SPLIT_ENABLED=true CSV_SPLIT_ROWS=1000000 JAVA_THREADS=32 \
  ./run_e2e_test.sh custom --tmcf=data.tmcf --csv=large.csv --no-llm-review

# Preserve shards for inspection
CSV_SPLIT_ENABLED=true CSV_SPLIT_CLEANUP=false JAVA_THREADS=32 \
  ./run_e2e_test.sh custom --tmcf=data.tmcf --csv=large.csv --no-llm-review
```

**Cloud Batch:** Set `CSV_SPLIT_ENABLED=true` as an environment variable on the Cloud Run service. The value is passed through to each Batch job automatically.

**PERF log fields** (one line per run, always emitted after Step 2 succeeds):

```
[PERF] split_enabled=true  split_rows=1000000  threshold_rows=5000000
       original_csv_mb=27648  shard_count=38  avg_rows_per_shard=1000000  avg_mb_per_shard=727
       csv_count=38  java_threads=32  java_xmx=192g
       step2_seconds=1565  rows_processed=38000000  rows_per_second=24270
       peak_rss_gb=28.3
```

| Field | Source | Notes |
|---|---|---|
| `split_enabled` | env var | `true` / `false` |
| `split_rows` | env var | Target rows per shard |
| `threshold_rows` | env var | Min rows required to trigger split |
| `original_csv_mb` | `os.path.getsize` | Size of input CSV before splitting |
| `shard_count` | split manifest | 0 when splitting disabled or skipped |
| `avg_rows_per_shard` | manifest `total_rows / shard_count` | `na` when not split |
| `avg_mb_per_shard` | `original_csv_mb / shard_count` | `na` when not split |
| `csv_count` | `${#CSVS[@]}` at Step 2 | Equals shard count when split, else original count |
| `java_threads` | env / computed | Threads passed to genmcf |
| `java_xmx` | machine type / env | JVM heap ceiling |
| `step2_seconds` | wall clock | genmcf wall time only |
| `rows_processed` | manifest (split) or `NumObservations` sum (non-split) | Pre-genmcf for split; post-genmcf for non-split |
| `rows_per_second` | `rows_processed / step2_seconds` | `unknown` if either value unavailable |
| `peak_rss_gb` | cgroup `memory.current` | Container RSS at Step 2 exit; `unknown` on macOS |

**Expected gains:** Roughly 2–5x improvement on real-world large datasets (e.g. 30 min → 6–15 min for Step 2). Actual gains depend on genmcf internal behaviour, disk I/O bandwidth, and JVM GC pressure, and may be lower. Benchmark with real data before relying on this feature.

#### Cloud Batch Execution

When `GCS_REPORTS_BUCKET` is set, **all** validation runs are dispatched to Google Cloud Batch instead of executing in-process on the Cloud Run instance. This applies to both built-in datasets and custom uploads, regardless of file size.

**Why Batch?** Cloud Run has a 32 MB HTTP request limit, a 60-minute request timeout, and limited memory. Cloud Batch removes all three constraints and supports multi-core VMs for parallel genmcf processing.

**Machine tier selection** (based on total CSV byte size at dispatch time):

| CSV size | Machine type | vCPUs | Memory |
|---|---|---|---|
| < 5 GB | `n2-highmem-16` | 16 | 128 GB |
| 5–20 GB | `n2-highmem-32` | 32 | 256 GB |
| ≥ 20 GB | `n2-highmem-64` | 64 | 512 GB |

**Status polling:** The Cloud Run server polls `jobs/<run_id>/status.json` in GCS and streams updates to the browser. Progress appears as step pills ("Preflight", "AI Schema Review", "DC Import Tool", etc.) identical to the local streaming experience.

**Image:** Batch jobs run the same Docker image as Cloud Run (set via `BATCH_IMAGE_URI`, auto-configured at deploy time).

**Accept Baseline:** After a successful Batch run, the "Accept Baseline" button becomes available once all MCF artifacts finish uploading (indicated by `artifacts_ready: true` in `status.json`).

#### Health Check

The server exposes `GET /healthz` → `{"status": "ok"}` for Cloud Run and load balancer probes.

#### Recommended Modes

| Use case | `IMPORT_RESOLUTION_MODE` | `IMPORT_EXISTENCE_CHECKS` | Notes |
|----------|--------------------------|---------------------------|------|
| **CI / deterministic** | `LOCAL` | `false` | No KG/API calls; fast, reproducible. Use for tests and automation. |
| **Local development** | `LOCAL` | `true` (default) | Validates DCID references against local MCFs and optional API; good for catching reference issues. |
| **Production / DE** | `LOCAL` or `FULL` | `true` | `FULL` resolves external IDs (e.g. ISO) via DC Recon API; use when you need location resolution. |

### 🔄 Pipeline Deep Dive

```
┌─────────────┐
│   Upload    │  TMCF + CSV (+ optional StatVars MCF)
└──────┬──────┘
       ↓
┌─────────────┐
│  Preflight  │  • Files exist? • Correct extensions?
└──────┬──────┘
       ↓
┌─────────────┐
│ CSV Quality │  • Duplicate columns • Empty columns
│             │  • Duplicate rows • Non-numeric values
└──────┬──────┘
       ↓
┌─────────────┐
│    Gemini   │  OPTIONAL (requires API key): AI review of TMCF for:
│   Review    │  • Schema typos • Missing dcs: prefixes
└──────┬──────┘  • Naming convention issues
       ↓
┌─────────────┐
│  dc-import  │  • genmcf → report.json, summary_report.csv
│   genmcf    │  • Includes schema conformance when StatVars MCF provided
└──────┬──────┘
       ↓
┌─────────────┐
│   import_   │  OPTIONAL (when a baseline exists):
│   differ    │  • Compare observations vs accepted baseline
└──────┬──────┘  • Detect deleted / modified / added rows
       ↓
┌─────────────┐
│  import_    │  • Run validation rules against config
│ validation  │  • Generate validation_output.json
└──────┬──────┘
       ↓
┌─────────────┐
│    HTML     │  • Blockers (P0 errors)
│   Report    │  • Warnings (non-blocking)
└─────────────┘  • StatVar summary • Schema errors • Dataset changes
```

#### What Gets Validated

| Check | Description | Blocking? |
|-------|-------------|-----------|
| File Preflight | TMCF/CSV exist, correct extensions | ✅ Yes |
| CSV Quality | No duplicate columns/rows, numeric value column | ✅ Yes |
| Gemini Review | Schema typos, naming conventions | ⚠️ Always advisory (never blocking) |
| Min Value | Values below threshold | ✅ Yes |
| Unit Consistency | Same StatVar, same unit | ✅ Yes |
| Scaling Factor | Consistent scaling per StatVar | ✅ Yes |
| Schema Errors | Structural errors from import tool (genmcf report) | ⚠️ Warning by default |
| Data Fluctuation | Extreme changes detected | ⚠️ Warning |
| Dataset Changes | Deleted / modified / added observations vs accepted baseline (requires a baseline to exist) | ✅ Yes (⚠️ Warning for custom datasets) |
### ⚙️ Configuration

#### Rule Configuration

Edit `validation_configs/new_import_config.json` to define validation rules:

```json
{
  "rules": [
    {
      "rule_id": "check_min_value",
      "description": "Check values are >= minimum",
      "validator": "MIN_VALUE_CHECK",
      "scope": {
        "data_source": "stats"
      },
      "params": {
        "minimum": 0
      }
    }
  ]
}
```

#### Warning vs Error Hierarchy

Control which failures block the pipeline in `validation_configs/warn_only_rules.json`:

```json
{
  "child_birth": ["check_max_date_latest"],
  "custom": ["check_max_date_latest"]
}
```

Rules listed here become WARNINGS instead of ERRORS (non-blocking).

### 🧪 Predefined Datasets

| Dataset | Description | Expected Result |
|---------|-------------|-----------------|
| `child_birth` | Bundled in this repo (`sample_data/child_birth/`): TMCF, CSV, stat_vars.mcf | ✅ PASS |
| `statistics_poland` | From DC data repo (`statvar_imports/statistics_poland/test/`): TMCF, CSV, stat_vars, stat_vars_schema | ✅ PASS |
| `finland_census` | From DC data repo (`statvar_imports/finland_census/test_data/`): TMCF, CSV, stat_vars, stat_vars_schema | ✅ PASS |
| `uae_population` | From DC data repo: TMCF, CSV | ✅ PASS |

Use **Custom** (upload your own TMCF + CSV) to test failure cases or other schemas.

### 🛠️ Development

#### Local Setup (Without Docker)

```bash
# Clone repos
git clone https://github.com/syed11cs/dc-import-validator.git
cd dc-import-validator
git clone https://github.com/datacommonsorg/data.git ../datacommonsorg/data
# Optional: set DATA_REPO to use a different path (e.g. for Docker/Cloud Run)

# Setup
chmod +x setup.sh run_e2e_test.sh run_ui.sh
./setup.sh

# Run tests
python tests/run_integration_tests.py
```

#### Project Structure

```
dc-import-validator/
├── sample_data/           # Built-in test datasets
├── scripts/               # Core validation logic
│   ├── validate_csv_quality.py
│   ├── validate_config_template.py
│   └── ...
├── ui/                    # Web interface (FastAPI)
├── validation_configs/    # Rule definitions
├── output/                # Validation results
├── docs/                  # Documentation
└── .github/workflows/     # CI/CD
```

#### Logging

- **Local:** `logs/dc_import_validator.log` (rotating, 30 days)
- **Cloud Run:** stdout (captured by Cloud Logging)
- **CLI:** Console with `[session=<id>]` prefixes for correlation

Set `LOG_LEVEL=DEBUG` for verbose output.

### ❓ FAQ

**Q: Do I need a Gemini API key?**
A: Only for AI-powered schema review. The validator works without it (skips the Gemini step).

**Q: What Java version do I need?**
A: Java 17. Docker image includes Java 17 (`openjdk-17-jre-headless`).

**Q: Can I run this in CI/CD?**
A: Absolutely! Use the CLI (`./run_e2e_test.sh`) in your pipelines. Exit codes indicate pass/fail.

**Q: How do I add new validation rules?**
A: Edit `validation_configs/new_import_config.json` and implement the validator in `scripts/`.

**Q: What's the difference between BLOCKER and WARNING?**
A: Blockers (errors) stop the pipeline with exit code 1. Warnings are informational only.

**Q: Can I customize the HTML report?**
A: The report is generated by `scripts/generate_html_report.py` (no static HTML template file).

### 📚 Additional Resources

- [Data Commons Import Documentation](https://github.com/datacommonsorg/data)
- [Import Validation Tool](https://github.com/datacommonsorg/import)
- [Gemini API Documentation](https://ai.google.dev/docs)

### 📄 License

Apache 2.0

Built for the Data Commons community — contributions welcome! 🎉
