#!/usr/bin/env python3
"""Web UI server for DC Import Validator.

Run with: uvicorn ui.server:app --reload --host 0.0.0.0 --port 8000

Logging: session ID + request_id; on Cloud Run logs go to stdout (captured by Cloud Logging);
locally to logs/dc_import_validator.log and console. See ui/app_logging.py.
"""

import html
import json
import os
import sys
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

# Project root for importing shared scripts and services
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile, Body
from fastapi.responses import FileResponse, Response, HTMLResponse
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

from ui.app_logging import (
    configure_logging,
    get_logger,
    set_request_id,
    clear_request_id,
)
from ui.services.validation_runner import run_validation_process as _run_validation_process
from ui.services.rule_samples import extract_rule_failure_samples, enrich_rule_failure_samples
from ui.services.fluctuation_service import (
    extract_fluctuation_samples as _extract_fluctuation_samples,
    get_gemini_api_key as _get_gemini_api_key,
    interpret_fluctuation as _interpret_fluctuation,
)
from ui.services.review_summary import (
    build_review_summary as _build_review_summary,
    build_review_summary_from_data as _build_review_summary_from_data,
    review_summary_to_markdown as _review_summary_to_markdown,
)
from ui import gcs_reports

CUSTOM_UPLOAD_DIR = APP_ROOT / "output" / "custom_upload"
MAX_UPLOAD_MB = 50
SCRIPT_DIR = APP_ROOT
OUTPUT_DIR = APP_ROOT / "output"
CONFIG_DIR = APP_ROOT / "validation_configs"

DATASET_OUTPUT_MAP = {
    "child_birth": OUTPUT_DIR / "child_birth_genmcf",
    "child_birth_fail_min_value": OUTPUT_DIR / "child_birth_fail_min_value_genmcf",
    "child_birth_fail_units": OUTPUT_DIR / "child_birth_fail_units_genmcf",
    "child_birth_fail_scaling_factor": OUTPUT_DIR / "child_birth_fail_scaling_factor_genmcf",
    "child_birth_ai_demo": OUTPUT_DIR / "child_birth_ai_demo_genmcf",
    "custom": OUTPUT_DIR / "custom_input",
}

DATASET_CONFIG_MAP = {
    "child_birth": "new_import_config.json",
    "child_birth_fail_min_value": "new_import_config.json",
    "child_birth_fail_units": "new_import_config.json",
    "child_birth_fail_scaling_factor": "new_import_config.json",
    "child_birth_ai_demo": "new_import_config.json",
    "custom": "new_import_config.json",
}

def _create_filtered_config(dataset: str, rule_ids: list[str]) -> Path | None:
    """Create temp config with only the selected rules. Returns path or None if use default."""
    if not rule_ids:
        return None
    config_name = DATASET_CONFIG_MAP.get(dataset)
    if not config_name:
        return None
    config_path = CONFIG_DIR / config_name
    if not config_path.exists():
        return None
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    rules = config.get("rules", [])
    selected = {r["rule_id"] for r in rules if r.get("rule_id") in rule_ids}
    if len(selected) == len(rules):
        return None  # All rules selected, use default
    filtered = [r for r in rules if r.get("rule_id") in rule_ids]
    if not filtered:
        return None
    config["rules"] = filtered
    fd, path = tempfile.mkstemp(suffix=".json", prefix="validation_config_")
    with open(fd, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    return Path(path)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Startup: configure logging and assign server session ID."""
    session_id = configure_logging(APP_ROOT)
    log = get_logger(__name__)
    log.info(
        "DC Import Validator started server_session_id=%s logs_dir=%s",
        session_id,
        str(APP_ROOT / "logs"),
    )
    yield
    # Shutdown: nothing to close for file logging
    log.info("DC Import Validator shutting down server_session_id=%s", session_id)


app = FastAPI(title="DC Import Validator", version="1.0", lifespan=_lifespan)
logger = get_logger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    """Assign request_id per request and log run_started for /api/run/*."""

    async def dispatch(self, request: Request, call_next):
        rid = uuid.uuid4().hex[:12]
        set_request_id(rid)
        request.state.request_id = rid
        try:
            logger.info("request_started method=%s path=%s request_id=%s", request.method, request.url.path, rid)
            if request.url.path.startswith("/api/run/"):
                # Middleware runs before route match, so path_params is not set; parse path instead
                suffix = request.url.path.split("/api/run/", 1)[-1].lstrip("/")
                dataset = suffix.split("/")[0] if suffix else "?"
                logger.info("run_started request_id=%s dataset=%s", rid, dataset)
            response = await call_next(request)
            return response
        finally:
            clear_request_id()


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security-related response headers (no CSP so inline scripts keep working)."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response


app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(LoggingMiddleware)


@app.get("/api/llm-status")
def llm_status():
    """Check if GEMINI_API_KEY or GOOGLE_API_KEY is set (for Gemini Review)."""
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    return {"key_set": bool(key and key.strip())}


@app.get("/api/datasets")
def list_datasets():
    return {
        "datasets": [
            {"id": "child_birth", "label": "Child Birth", "description": "DC repo, clean (expect PASS)"},
            {"id": "child_birth_fail_min_value", "label": "Child Birth — fail min value", "description": "One negative value (−1) and two large fluctuations → check_min_value FAIL; Data Fluctuation: 100%, 200%, 500%"},
            {"id": "child_birth_fail_units", "label": "Child Birth — fail units", "description": "Mixed units → check_unit_consistency FAIL"},
            {"id": "child_birth_fail_scaling_factor", "label": "Child Birth — fail scaling factor", "description": "Inconsistent scaling → check_scaling_factor_consistency FAIL"},
            {"id": "child_birth_ai_demo", "label": "Child Birth — AI demo", "description": "TMCF with schema issues & typos (missing dcs:, duplicate, typo) → Gemini Review finds issues"},
            {"id": "custom", "label": "Custom (Upload your own files)", "description": "Upload TMCF + CSV files to validate."},
        ]
    }


@app.get("/api/config/{dataset}")
def get_config(dataset: str):
    if dataset not in DATASET_CONFIG_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    config_name = DATASET_CONFIG_MAP[dataset]
    config_path = CONFIG_DIR / config_name
    if not config_path.exists():
        raise HTTPException(status_code=404, detail="Config not found")
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


@app.post("/api/run/custom/stream")
async def run_custom_validation_stream(
    request: Request,
    tmcf: UploadFile = File(...),
    csv: UploadFile = File(...),
    stat_vars_mcf: UploadFile | None = File(None),
    stat_vars_schema_mcf: UploadFile | None = File(None),
    rules: str | None = Form(None),
    llm_review: str | None = Form(None),
    llm_model: str | None = Form(None),
    ai_advisory: str | None = Form(None),
):
    """Run validation on uploaded TMCF + CSV files with streaming output. Optional stat_vars.mcf and stat_vars_schema.mcf enable lint-with-MCFs for schema conformance."""
    script = SCRIPT_DIR / "run_e2e_test.sh"
    if not script.exists():
        raise HTTPException(status_code=500, detail="run_e2e_test.sh not found")

    CUSTOM_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    tmcf_path = CUSTOM_UPLOAD_DIR / "input.tmcf"
    csv_path = CUSTOM_UPLOAD_DIR / "input.csv"
    stat_vars_mcf_path = CUSTOM_UPLOAD_DIR / "input_stat_vars.mcf"
    stat_vars_schema_mcf_path = CUSTOM_UPLOAD_DIR / "input_stat_vars_schema.mcf"

    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    try:
        tmcf_content = await tmcf.read()
        if len(tmcf_content) > max_bytes:
            raise HTTPException(status_code=400, detail=f"TMCF file exceeds {MAX_UPLOAD_MB}MB limit")
        tmcf_path.write_bytes(tmcf_content)

        csv_content = await csv.read()
        if len(csv_content) > max_bytes:
            raise HTTPException(status_code=400, detail=f"CSV file exceeds {MAX_UPLOAD_MB}MB limit")
        csv_path.write_bytes(csv_content)

        if stat_vars_mcf and stat_vars_mcf.filename:
            content = await stat_vars_mcf.read()
            if len(content) > max_bytes:
                raise HTTPException(status_code=400, detail="Stat vars MCF file exceeds size limit")
            stat_vars_mcf_path.write_bytes(content)
        elif stat_vars_mcf_path.exists():
            stat_vars_mcf_path.unlink(missing_ok=True)

        if stat_vars_schema_mcf and stat_vars_schema_mcf.filename:
            content = await stat_vars_schema_mcf.read()
            if len(content) > max_bytes:
                raise HTTPException(status_code=400, detail="Stat vars schema MCF file exceeds size limit")
            stat_vars_schema_mcf_path.write_bytes(content)
        elif stat_vars_schema_mcf_path.exists():
            stat_vars_schema_mcf_path.unlink(missing_ok=True)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error saving custom uploads or preparing run")
        raise HTTPException(status_code=500, detail=str(e))

    rule_ids = [x.strip() for x in (rules or "").split(",") if x.strip()] if rules else []
    config_path = _create_filtered_config("custom", rule_ids)
    try:
        args = ["bash", str(script), "custom", f"--tmcf={tmcf_path}", f"--csv={csv_path}"]
        if stat_vars_mcf_path.exists():
            args.append(f"--stat-vars-mcf={stat_vars_mcf_path}")
        if stat_vars_schema_mcf_path.exists():
            args.append(f"--stat-vars-schema-mcf={stat_vars_schema_mcf_path}")
        if config_path:
            args.extend([f"--config={config_path}"])
        if llm_review:
            args.append("--llm-review")
            if llm_model:
                args.append(f"--model={llm_model}")
        else:
            args.append("--no-llm-review")
        if ai_advisory:
            args.append("--ai-advisory")
        request_id = getattr(request.state, "request_id", "")
        output_dir = (OUTPUT_DIR / "custom" / request_id) if request_id else DATASET_OUTPUT_MAP["custom"]
        canonical_output_dir = DATASET_OUTPUT_MAP["custom"]
        return await _run_validation_process(
            args, request, config_path, stream=True, app_root=APP_ROOT,
            output_dir=output_dir, dataset="custom", canonical_output_dir=canonical_output_dir,
        )
    except HTTPException:
        raise
    except Exception as e:
        if config_path and config_path.exists():
            config_path.unlink(missing_ok=True)
        logger.exception("Error running custom validation (stream)")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run/custom")
async def run_custom_validation(
    request: Request,
    tmcf: UploadFile = File(...),
    csv: UploadFile = File(...),
    stat_vars_mcf: UploadFile | None = File(None),
    stat_vars_schema_mcf: UploadFile | None = File(None),
    rules: str | None = Form(None),
    llm_review: str | None = Form(None),
    llm_model: str | None = Form(None),
    ai_advisory: str | None = Form(None),
):
    """Run validation on uploaded TMCF + CSV files. Optional stat_vars.mcf and stat_vars_schema.mcf enable lint-with-MCFs."""
    script = SCRIPT_DIR / "run_e2e_test.sh"
    if not script.exists():
        raise HTTPException(status_code=500, detail="run_e2e_test.sh not found")

    CUSTOM_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    tmcf_path = CUSTOM_UPLOAD_DIR / "input.tmcf"
    csv_path = CUSTOM_UPLOAD_DIR / "input.csv"
    stat_vars_mcf_path = CUSTOM_UPLOAD_DIR / "input_stat_vars.mcf"
    stat_vars_schema_mcf_path = CUSTOM_UPLOAD_DIR / "input_stat_vars_schema.mcf"

    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    try:
        tmcf_content = await tmcf.read()
        if len(tmcf_content) > max_bytes:
            raise HTTPException(status_code=400, detail=f"TMCF file exceeds {MAX_UPLOAD_MB}MB limit")
        tmcf_path.write_bytes(tmcf_content)

        csv_content = await csv.read()
        if len(csv_content) > max_bytes:
            raise HTTPException(status_code=400, detail=f"CSV file exceeds {MAX_UPLOAD_MB}MB limit")
        csv_path.write_bytes(csv_content)

        if stat_vars_mcf and stat_vars_mcf.filename:
            content = await stat_vars_mcf.read()
            if len(content) > max_bytes:
                raise HTTPException(status_code=400, detail="Stat vars MCF file exceeds size limit")
            stat_vars_mcf_path.write_bytes(content)
        elif stat_vars_mcf_path.exists():
            stat_vars_mcf_path.unlink(missing_ok=True)

        if stat_vars_schema_mcf and stat_vars_schema_mcf.filename:
            content = await stat_vars_schema_mcf.read()
            if len(content) > max_bytes:
                raise HTTPException(status_code=400, detail="Stat vars schema MCF file exceeds size limit")
            stat_vars_schema_mcf_path.write_bytes(content)
        elif stat_vars_schema_mcf_path.exists():
            stat_vars_schema_mcf_path.unlink(missing_ok=True)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error saving custom uploads")
        raise HTTPException(status_code=500, detail=f"Failed to save uploads: {e}")

    rule_ids = [x.strip() for x in (rules or "").split(",") if x.strip()] if rules else []
    config_path = _create_filtered_config("custom", rule_ids)
    try:
        args = ["bash", str(script), "custom", f"--tmcf={tmcf_path}", f"--csv={csv_path}"]
        if stat_vars_mcf_path.exists():
            args.append(f"--stat-vars-mcf={stat_vars_mcf_path}")
        if stat_vars_schema_mcf_path.exists():
            args.append(f"--stat-vars-schema-mcf={stat_vars_schema_mcf_path}")
        if config_path:
            args.extend([f"--config={config_path}"])
        if llm_review:
            args.append("--llm-review")
            if llm_model:
                args.append(f"--model={llm_model}")
        else:
            args.append("--no-llm-review")
        if ai_advisory:
            args.append("--ai-advisory")
        request_id = getattr(request.state, "request_id", "")
        output_dir = (OUTPUT_DIR / "custom" / request_id) if request_id else DATASET_OUTPUT_MAP["custom"]
        canonical_output_dir = DATASET_OUTPUT_MAP["custom"]
        return await _run_validation_process(
            args, request, config_path, stream=False, app_root=APP_ROOT,
            output_dir=output_dir, dataset="custom", canonical_output_dir=canonical_output_dir,
        )
    except HTTPException:
        raise
    except Exception as e:
        if config_path and config_path.exists():
            config_path.unlink(missing_ok=True)
        logger.exception("Error running custom validation")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run/{dataset}")
async def run_validation(
    dataset: str,
    request: Request,
    rules: str | None = Query(None),
    stream: bool = Query(False),
    llm_review: str | None = Query(None),
    llm_model: str | None = Query(None),
    ai_advisory: str | None = Query(None),
):
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    script = SCRIPT_DIR / "run_e2e_test.sh"
    if not script.exists():
        raise HTTPException(status_code=500, detail="run_e2e_test.sh not found")
    rule_ids = [x.strip() for x in (rules or "").split(",") if x.strip()] if rules else []
    config_path = _create_filtered_config(dataset, rule_ids)
    if not llm_review:
        (DATASET_OUTPUT_MAP[dataset] / "schema_review.json").unlink(missing_ok=True)
    try:
        args = ["bash", str(script), dataset]
        if config_path:
            args.extend([f"--config={config_path}"])
        if llm_review:
            args.append("--llm-review")
            if llm_model:
                args.append(f"--model={llm_model}")
        else:
            args.append("--no-llm-review")
        if ai_advisory:
            args.append("--ai-advisory")
        request_id = getattr(request.state, "request_id", "")
        output_dir = (OUTPUT_DIR / dataset / request_id) if request_id else DATASET_OUTPUT_MAP[dataset]
        canonical_output_dir = DATASET_OUTPUT_MAP[dataset]
        return await _run_validation_process(
            args, request, config_path, stream, app_root=APP_ROOT,
            output_dir=output_dir, dataset=dataset, canonical_output_dir=canonical_output_dir,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error running validation for dataset %s", dataset)
        raise HTTPException(status_code=500, detail=str(e))


def _run_id_safe(run_id: str) -> bool:
    """Reject run_id that could escape OUTPUT_DIR (path traversal)."""
    return run_id is not None and "/" not in run_id and ".." not in run_id


@app.get("/api/validation-result/{dataset}")
def get_validation_result(dataset: str, run_id: str | None = Query(None)):
    """Return per-rule validation results from validation_output.json. If run_id is set and GCS is configured, use GCS; else local per-run then canonical."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if run_id and _run_id_safe(run_id):
        raw = gcs_reports.get_report_from_gcs(run_id, dataset, "validation_output.json")
        if raw is not None:
            try:
                results = json.loads(raw.decode("utf-8"))
                if not isinstance(results, list):
                    results = []
                return {"exists": True, "results": results}
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        # Local per-run fallback (e.g. run failed at Step 2; validation_output.json in run dir only)
        per_run_path = OUTPUT_DIR / dataset / run_id / "validation_output.json"
        if per_run_path.exists():
            try:
                with open(per_run_path, encoding="utf-8") as f:
                    results = json.load(f)
                if not isinstance(results, list):
                    results = []
                return {"exists": True, "results": results}
            except (json.JSONDecodeError, OSError):
                pass
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "validation_output.json"
    if not path.exists():
        return {"exists": False, "results": []}
    with open(path, encoding="utf-8") as f:
        results = json.load(f)
    return {"exists": True, "results": results}


@app.get("/api/llm-report/{dataset}")
def get_llm_report(dataset: str, run_id: str | None = Query(None)):
    """Return Gemini Review results from schema_review.json. If run_id is set and GCS is configured, use GCS."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")

    def _issues_to_response(issues):
        if not isinstance(issues, list):
            issues = [issues] if isinstance(issues, dict) else []
        def is_blocker(i):
            t = i.get("type")
            if t in ("info",):
                return False
            s = i.get("severity")
            if s == "blocker":
                return True
            if s == "warning":
                return False
            return t in ("typo", "schema", "naming", "unknown_statvar", "parse_error", "error")
        blockers = [i for i in issues if is_blocker(i)]
        return {"exists": True, "issues": issues, "passed": len(blockers) == 0}

    if run_id and _run_id_safe(run_id):
        raw = gcs_reports.get_report_from_gcs(run_id, dataset, "schema_review.json")
        if raw is not None:
            try:
                issues = json.loads(raw.decode("utf-8"))
                return _issues_to_response(issues)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        # Local per-run output (e.g. run failed at Step 0; schema_review.json exists in run dir)
        per_run_path = OUTPUT_DIR / dataset / run_id / "schema_review.json"
        if per_run_path.exists():
            try:
                with open(per_run_path, encoding="utf-8") as f:
                    issues = json.load(f)
                return _issues_to_response(issues)
            except (json.JSONDecodeError, OSError):
                pass
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "schema_review.json"
    if not path.exists():
        return {"exists": False, "issues": [], "passed": True}
    try:
        with open(path, encoding="utf-8") as f:
            issues = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"exists": True, "issues": [], "passed": False}
    return _issues_to_response(issues)


@app.get("/api/report-info/{dataset}")
def report_info(dataset: str, run_id: str | None = Query(None)):
    """Return report metadata including mtime for timestamp display. If run_id is set and GCS is configured, use GCS."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if run_id and _run_id_safe(run_id):
        mtime = gcs_reports.get_report_updated_from_gcs(run_id, dataset, "validation_report.html")
        if mtime is not None:
            return {"exists": True, "mtime": mtime}
        # Local per-run: use validation_report.html or schema_review.json mtime (e.g. run stopped at Step 0)
        per_run = OUTPUT_DIR / dataset / run_id
        for name in ("validation_report.html", "schema_review.json"):
            p = per_run / name
            if p.exists():
                return {"exists": True, "mtime": p.stat().st_mtime}
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "validation_report.html"
    if not path.exists():
        return {"exists": False}
    mtime = path.stat().st_mtime
    return {"exists": True, "mtime": mtime}


@app.get("/api/fluctuation-samples/{dataset}")
def get_fluctuation_samples(dataset: str, run_id: str | None = Query(None)):
    """Return structured fluctuation samples from report.json. If run_id is set and GCS is configured, use GCS; else local per-run then canonical."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if run_id and _run_id_safe(run_id):
        raw = gcs_reports.get_report_from_gcs(run_id, dataset, "report.json")
        if raw is not None:
            try:
                report = json.loads(raw.decode("utf-8"))
                samples = _extract_fluctuation_samples(report)
                return {"exists": True, "samples": samples}
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        # Local per-run fallback
        per_run_path = OUTPUT_DIR / dataset / run_id / "report.json"
        if per_run_path.exists():
            try:
                with open(per_run_path, encoding="utf-8") as f:
                    report = json.load(f)
                samples = _extract_fluctuation_samples(report)
                return {"exists": True, "samples": samples}
            except (json.JSONDecodeError, OSError):
                pass
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "report.json"
    if not path.exists():
        return {"exists": False, "samples": []}
    try:
        with open(path, encoding="utf-8") as f:
            report = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"exists": True, "samples": []}
    samples = _extract_fluctuation_samples(report)
    return {"exists": True, "samples": samples}


class FluctuationInterpretationRequest(BaseModel):
    """Request body for optional AI interpretation of a fluctuation sample (advisory only)."""
    statVar: str | None = None
    location: str | None = None
    period: str | None = None
    percent_change: float | None = None
    technical_signals: dict | None = None


@app.post("/api/fluctuation-interpretation")
def post_fluctuation_interpretation(body: FluctuationInterpretationRequest = Body(...)):
    """Optional, UI-triggered AI interpretation of a fluctuation sample. Advisory only; never affects validation."""
    stat_var = body.statVar or ""
    location = body.location or ""
    period = body.period or ""
    percent_change = body.percent_change
    technical_signals = body.technical_signals or {}
    interpretation = _interpret_fluctuation(
        stat_var, location, period, percent_change, technical_signals
    )
    if interpretation is None and not _get_gemini_api_key():
        return {"ai_interpretation": None, "error": "GEMINI_API_KEY or GOOGLE_API_KEY not set"}
    return {"ai_interpretation": interpretation}


@app.get("/api/rule-failure-samples/{dataset}")
def get_rule_failure_samples(dataset: str, run_id: str | None = Query(None)):
    """Return structured rule failure samples from validation_output.json. If run_id is set and GCS is configured, use GCS; else local per-run then canonical (with enrichment when input.csv/report.json present)."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if run_id and _run_id_safe(run_id):
        raw_vo = gcs_reports.get_report_from_gcs(run_id, dataset, "validation_output.json")
        if raw_vo is not None:
            try:
                results = json.loads(raw_vo.decode("utf-8"))
                if not isinstance(results, list):
                    results = []
                samples = extract_rule_failure_samples(results)
                raw_report = gcs_reports.get_report_from_gcs(run_id, dataset, "report.json")
                raw_csv = gcs_reports.get_report_from_gcs(run_id, dataset, "input.csv")
                if raw_report is not None and raw_csv is not None:
                    try:
                        report = json.loads(raw_report.decode("utf-8"))
                        with tempfile.TemporaryDirectory(prefix="gcs_rule_failure_") as tmp:
                            tmp_path = Path(tmp)
                            (tmp_path / "input.csv").write_bytes(raw_csv)
                            report["commandArgs"] = report.get("commandArgs") or {}
                            report["commandArgs"]["inputFiles"] = [str(tmp_path / "input.csv")]
                            (tmp_path / "report.json").write_text(
                                json.dumps(report), encoding="utf-8"
                            )
                            enrich_rule_failure_samples(samples, tmp_path, results)
                    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                        pass
                return {"exists": True, "samples": samples}
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        # Local per-run fallback
        per_run_dir = OUTPUT_DIR / dataset / run_id
        per_run_vo = per_run_dir / "validation_output.json"
        if per_run_vo.exists():
            try:
                with open(per_run_vo, encoding="utf-8") as f:
                    results = json.load(f)
                if not isinstance(results, list):
                    results = []
                samples = extract_rule_failure_samples(results)
                enrich_rule_failure_samples(samples, per_run_dir, results)
                return {"exists": True, "samples": samples}
            except (json.JSONDecodeError, OSError):
                pass
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "validation_output.json"
    if not path.exists():
        return {"exists": False, "samples": []}
    try:
        with open(path, encoding="utf-8") as f:
            results = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"exists": True, "samples": []}
    if not isinstance(results, list):
        results = []
    samples = extract_rule_failure_samples(results)
    enrich_rule_failure_samples(samples, output_dir, results)
    return {"exists": True, "samples": samples}


@app.get("/api/review-summary/{dataset}")
def get_review_summary(dataset: str, format: str | None = Query(None), run_id: str | None = Query(None)):
    """Return combined review summary (validation + Gemini + fluctuation + rule failures). If run_id is set and GCS configured, use GCS."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    data = None
    if run_id:
        raw_vo = gcs_reports.get_report_from_gcs(run_id, dataset, "validation_output.json")
        raw_llm = gcs_reports.get_report_from_gcs(run_id, dataset, "schema_review.json")
        raw_report = gcs_reports.get_report_from_gcs(run_id, dataset, "report.json")
        if raw_vo is not None:
            try:
                results = json.loads(raw_vo.decode("utf-8"))
                if not isinstance(results, list):
                    results = []
                llm_issues = []
                if raw_llm is not None:
                    try:
                        llm_issues = json.loads(raw_llm.decode("utf-8"))
                        if not isinstance(llm_issues, list):
                            llm_issues = [llm_issues] if isinstance(llm_issues, dict) else []
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                report = None
                if raw_report is not None:
                    try:
                        report = json.loads(raw_report.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                data = _build_review_summary_from_data(dataset, results, llm_issues, report)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
    if data is None:
        output_dir = DATASET_OUTPUT_MAP[dataset]
        data = _build_review_summary(dataset, output_dir)
    if data is None:
        raise HTTPException(status_code=404, detail="No validation result. Run validation first.")
    if format and format.lower() == "md":
        md = _review_summary_to_markdown(data)
        return Response(
            content=md,
            media_type="text/markdown",
            headers={"Content-Disposition": f'attachment; filename="review_summary_{dataset}.md"'},
        )
    return data


@app.get("/report/{dataset}/{run_id}", response_class=HTMLResponse)
def serve_report_by_run_id(dataset: str, run_id: str):
    """Serve validation report from GCS or local per-run (so Open Report works for local runs that completed Step 3)."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if "/" in run_id or ".." in run_id:
        raise HTTPException(status_code=400, detail="Invalid run_id")
    content = gcs_reports.get_report_from_gcs(run_id, dataset, "validation_report.html")
    if content is None:
        # Local per-run fallback (GCS not set or not uploaded yet)
        per_run_path = OUTPUT_DIR / dataset / run_id / "validation_report.html"
        if per_run_path.exists():
            content = per_run_path.read_bytes()
        else:
            raise HTTPException(
                status_code=404,
                detail="Report not found. It may not have been uploaded to GCS yet, or GCS is not configured.",
            )
    # Rewrite "View full import tool report" link to be run-specific so it works from any instance.
    # generate_html_report.py only injects this link when summary_report.html exists, so we only
    # replace the exact href format below (no accidental match of other URLs).
    # Escape dataset/run_id for HTML so URL path params cannot break the attribute (XSS).
    safe_dataset = html.escape(dataset, quote=True)
    safe_run_id = html.escape(run_id, quote=True)
    old_link = b'href="/summary-report/' + dataset.encode("utf-8") + b'"'
    new_link = ('href="/summary-report/' + safe_dataset + "/" + safe_run_id + '"').encode("utf-8")
    if old_link in content:
        content = content.replace(old_link, new_link, 1)
    return HTMLResponse(
        content=content,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
    )


@app.get("/report/{dataset}")
def serve_report(dataset: str):
    """Serve validation report from local disk (latest run for this dataset)."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "validation_report.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="No report yet. Run validation first.")
    return FileResponse(
        path,
        media_type="text/html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
    )


@app.get("/summary-report/{dataset}/{run_id}", response_class=HTMLResponse)
def serve_summary_report_by_run_id(dataset: str, run_id: str):
    """Serve the import tool's summary_report.html from GCS or local per-run (counters, sample places, charts)."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    content = gcs_reports.get_report_from_gcs(run_id, dataset, "summary_report.html")
    if content is None:
        # Local per-run fallback (e.g. run without GCS; report exists in output/dataset/run_id/)
        per_run_path = OUTPUT_DIR / dataset / run_id / "summary_report.html"
        if per_run_path.exists():
            content = per_run_path.read_bytes()
        else:
            raise HTTPException(
                status_code=404,
                detail="Summary report not found. It may not have been uploaded to GCS yet.",
            )
    return HTMLResponse(
        content=content,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
    )


@app.get("/summary-report/{dataset}")
def serve_summary_report(dataset: str):
    """Serve the import tool's summary_report.html from local disk (latest run)."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "summary_report.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="No JAR summary report. Run validation first.")
    return FileResponse(
        path,
        media_type="text/html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
    )


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html")
