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
| 🤖 AI Schema Review | Optional Gemini-powered review catches typos and schema issues before validation |
| 📊 Rich Reports | HTML reports with blockers/warnings, StatVar summaries, lint issues, and import tool integration |
| 🚀 Multiple Run Modes | Docker (zero setup), CLI (development), or Cloud Run (production) |
| ☁️ Cloud Ready | Deploy to Cloud Run with automatic GCS report storage and CI/CD via GitHub Actions |

### 🚀 Quick Start (Recommended: Docker)

Run the validator with zero local setup.  
AI review is optional and disabled by default.

```bash
git clone https://github.com/syed11cs/dc-import-validator.git
cd dc-import-validator
docker build -t dc-import-validator .
docker run --rm -p 8080:8080 dc-import-validator
```

Open http://localhost:8080 and start validating.

### 🤖 Enable AI Review (Optional)

To use Gemini for schema and typo review:

```bash
docker run --rm -p 8080:8080 \
  -e GEMINI_API_KEY=your_key \
  dc-import-validator
```

Replace `your_key` with an API key from [Google AI Studio](https://aistudio.google.com/apikey).

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
| Dataset Browser | Test with built-in datasets (child_birth, failure cases, AI demo) or upload your own |
| Interactive Rules | Select which validation rules to run with checkbox interface |
| Live Logs | Real-time terminal output with syntax highlighting and copy support |
| Rich Reports | Combined view of blockers, warnings, StatVar summaries, and lint issues |
| Gemini Integration | Toggle AI review on/off, select model (2.5 Flash, Pro, etc.) |
| Run Management | Cancel long-running validations, view history |

#### Upload Custom Files

1. Select **Custom (Upload your own files)** from the dataset dropdown.
2. Upload your TMCF (.tmcf/.mcf) and CSV (.csv) files.
3. (Optional) Add StatVars MCF for enhanced schema validation.
4. Click **Run Validation**.

File limit: 50MB per file.

### 💻 CLI Usage

Perfect for automation, CI/CD, or power users:

```bash
# Setup (one-time)
./setup.sh

# Validate built-in datasets
./run_e2e_test.sh child_birth                    # Clean dataset → PASS
./run_e2e_test.sh child_birth_fail_min_value     # Intentional failure → FAIL
./run_e2e_test.sh child_birth_ai_demo            # Schema issues → Gemini catches them

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
| `--csv PATH` | CSV file (for custom datasets) |
| `--stat-vars-mcf PATH` | Optional StatVars MCF for schema conformance |
| `--stat-vars-schema-mcf PATH` | Optional schema MCF |
| `--rules ID1,ID2` | Run only these rules |
| `--skip-rules ID1,ID2` | Skip these rules |
| `--llm-review` | Enable Gemini Review (requires API key) |
| `--no-llm-review` | Disable Gemini Review |
| `--model ID` | Gemini model (default: gemini-2.5-flash) |
| `--help` | Show help |
### ☁️ Deployment

#### Deploy to Cloud Run (Automated)

The repository includes GitHub Actions for zero-touch deployment.

**One-time setup:**

1. Create Artifact Registry repo.
2. Create GCS bucket for reports (optional but recommended).
3. Create service account with necessary permissions.
4. Add GitHub secrets: `GCP_PROJECT_ID`, `GCP_SA_KEY` (JSON key), `AR_LOCATION` (e.g. `us`).

Push to `main` — automatic build and deploy!

See [docs/DEPLOY_CLOUD_RUN.md](docs/DEPLOY_CLOUD_RUN.md) for detailed instructions.

#### Environment Variables

All supported environment variables in one place. See `.env.example` for an optional template.

| Variable | Required | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` | For AI review | Google AI Studio API key for Gemini schema review |
| `GOOGLE_API_KEY` | Alternative | Alternative API key variable (used if `GEMINI_API_KEY` not set) |
| `DC_API_KEY` | Required for FULL mode | Data Commons API key used by Java import tool for Recon and existence checks |
| `GCS_REPORTS_BUCKET` | For Cloud Run | GCS bucket name for report storage; upload/serve fails clearly if bucket not accessible |
| `DATA_REPO` | No | Path to `datacommonsorg/data` clone (default: `../datacommonsorg/data` from project root) |
| `VALIDATION_RUN_TIMEOUT_SEC` | No | Max validation run time in seconds (e.g. `3600`); unset = no timeout |
| `IMPORT_RESOLUTION_MODE` | No | Java import tool resolution mode (default: `LOCAL`) |
| `IMPORT_EXISTENCE_CHECKS` | No | Java import tool existence checks (default: `true`) |
| `LOG_LEVEL` | No | Application log level: `DEBUG`, `INFO` (default), `WARNING` |

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
│    Gemini   │  OPTIONAL: AI review of TMCF for:
│   Review    │  • Schema typos • Missing dcs: prefixes
└──────┬──────┘  • Naming convention issues
       ↓
┌─────────────┐
│  dc-import  │  • genmcf → report.json, summary_report.csv
│   genmcf    │  • lint (if StatVars MCF provided)
└──────┬──────┘
       ↓
┌─────────────┐
│  import_    │  • Run validation rules against config
│ validation  │  • Generate validation_output.json
└──────┬──────┘
       ↓
┌─────────────┐
│    HTML     │  • Blockers (P0 errors)
│   Report    │  • Warnings (non-blocking)
└─────────────┘  • StatVar summary • Lint issues
```

#### What Gets Validated

| Check | Description | Blocking? |
|-------|-------------|-----------|
| File Preflight | TMCF/CSV exist, correct extensions | ✅ Yes |
| CSV Quality | No duplicate columns/rows, numeric value column | ✅ Yes |
| Gemini Review | Schema typos, naming conventions | ⚠️ Configurable |
| Min Value | Values below threshold | ✅ Yes |
| Unit Consistency | Same StatVar, same unit | ✅ Yes |
| Scaling Factor | Consistent scaling per StatVar | ✅ Yes |
| Lint Errors | Import tool warnings/errors | ⚠️ Warning by default |
| Data Fluctuation | Extreme changes detected | ⚠️ Warning |
### ⚙️ Configuration

#### Rule Configuration

Edit `validation_configs/new_import_config.json` to define validation rules:

```json
{
  "rules": [
    {
      "rule_id": "check_min_value",
      "description": "Check values are >= minimum",
      "validator": "MinValueValidator",
      "scope": {
        "data_source": "stats",
        "stat_var_groups": ["Count_*"]
      },
      "params": {
        "min_value": 0
      }
    }
  ]
}
```

#### Warning vs Error Hierarchy

Control which failures block the pipeline in `validation_configs/warn_only_rules.json`:

```json
{
  "child_birth": ["check_lint_error_count"],
  "custom": ["check_lint_error_count", "check_data_fluctuation"]
}
```

Rules listed here become WARNINGS instead of ERRORS (non-blocking).

### 🧪 Test Datasets

Built-in datasets for testing and demos:

| Dataset | What It Tests | Expected Result |
|---------|---------------|-----------------|
| child_birth | Clean dataset | ✅ PASS |
| child_birth_fail_min_value | Negative value + large fluctuations | ❌ FAIL (min_value) |
| child_birth_fail_units | Mixed units (empty vs "Percent") | ❌ FAIL (unit consistency) |
| child_birth_fail_scaling_factor | Inconsistent scaling (1 vs 100) | ❌ FAIL (scaling factor) |
| child_birth_ai_demo | Schema typos, missing dcs: | 🤖 Gemini catches issues |

Each failure dataset modifies ONE aspect of the clean child_birth data to trigger specific rules.

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
A: Java 11+ (17 recommended). Docker image includes Java 17.

**Q: Can I run this in CI/CD?**  
A: Absolutely! Use the CLI (`./run_e2e_test.sh`) in your pipelines. Exit codes indicate pass/fail.

**Q: How do I add new validation rules?**  
A: Edit `validation_configs/new_import_config.json` and implement the validator in `scripts/`.

**Q: What's the difference between BLOCKER and WARNING?**  
A: Blockers (errors) stop the pipeline with exit code 1. Warnings are informational only.

**Q: Can I customize the HTML report?**  
A: Yes! The report template is in `ui/templates/validation_report.html`.

### 📚 Additional Resources

- [Data Commons Import Documentation](https://github.com/datacommonsorg/data)
- [Import Validation Tool](https://github.com/datacommonsorg/import)
- [Gemini API Documentation](https://ai.google.dev/docs)

### 📄 License

Apache 2.0

Built for the Data Commons community — contributions welcome! 🎉

