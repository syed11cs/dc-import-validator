#!/usr/bin/env python3
"""Web UI server for DC Import Validator.

Run with: uvicorn ui.server:app --reload --host 0.0.0.0 --port 8000

Logging: session ID + request_id; on Cloud Run logs go to stdout (captured by Cloud Logging);
locally to logs/dc_import_validator.log and console. See ui/app_logging.py.
"""

import asyncio
import html
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

# Project root for importing shared scripts and services
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))
_SCRIPTS_DIR = APP_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile, Body
from fastapi.responses import FileResponse, JSONResponse, Response, HTMLResponse
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
    _load_differ_stats,
)
from ui import gcs_reports
from ui.gcs_reports import GCSAccessError, is_gcs_configured
from ui import gcs_uploads as _gcs_uploads
import gcs_baselines as _gcs_baselines

CUSTOM_UPLOAD_DIR = APP_ROOT / "output" / "custom_upload"
MAX_UPLOAD_MB = 10 * 1024  # 10 GB per file
SCRIPT_DIR = APP_ROOT
OUTPUT_DIR = APP_ROOT / "output"
CONFIG_DIR = APP_ROOT / "validation_configs"

DATASET_OUTPUT_MAP = {
    "child_birth": OUTPUT_DIR / "child_birth_genmcf",
    "statistics_poland": OUTPUT_DIR / "statistics_poland_genmcf",
    "finland_census": OUTPUT_DIR / "finland_census_genmcf",
    "uae_population": OUTPUT_DIR / "uae_population_genmcf",
    "custom": OUTPUT_DIR / "custom_input",
}

DATASET_CONFIG_MAP = {
    "child_birth": "new_import_config.json",
    "statistics_poland": "new_import_config.json",
    "finland_census": "new_import_config.json",
    "uae_population": "new_import_config.json",
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
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
        return Path(path)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise


def _llm_review_enabled(llm_review: str | None) -> bool:
    """Whether to run LLM review this run.
    If the client did not specify llm_review (None), default to enabled only when an API key exists.
    If the client explicitly passed a value, respect it (truthy = enable; explicit 'false'/'no'/etc. = disable).
    """
    key = (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or "").strip()
    if llm_review is None:
        return bool(key)
    if isinstance(llm_review, str):
        s = llm_review.strip().lower()
        if s in ("false", "0", "no", "off", ""):
            return False
        if s in ("true", "1", "on", "yes"):
            return True
    return bool(llm_review)


# Allowlist of accepted Gemini model IDs. Values outside this set are rejected so
# unvalidated user input never reaches the external API as a model identifier.
_ALLOWED_LLM_MODELS: frozenset[str] = frozenset({
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
    "gemini-3.1-pro-preview",
})


def _existence_checks_env(existence_checks: str | None) -> dict[str, str]:
    """Return extra_env dict for IMPORT_EXISTENCE_CHECKS based on the UI toggle value.

    Default is OFF so large local datasets run quickly without DC API calls.
    Pass 'true'/'1'/'yes'/'on' to enable; anything else (including None) disables.
    """
    enabled = (
        existence_checks is not None
        and existence_checks.strip().lower() in ("1", "true", "yes", "on")
    )
    return {"IMPORT_EXISTENCE_CHECKS": "true" if enabled else "false"}


async def _stream_upload_to_file(
    upload: UploadFile, dest: Path, max_bytes: int, error_detail: str
) -> None:
    """Stream-copy an UploadFile to dest in 8 MB chunks, enforcing max_bytes.

    Uses asyncio.to_thread so the event loop is not blocked during disk I/O.
    Raises HTTPException(400) mid-stream if the file exceeds max_bytes.
    """
    CHUNK = 8 * 1024 * 1024  # 8 MB per read

    def _copy() -> None:
        total = 0
        upload.file.seek(0)
        with dest.open("wb") as out:
            while True:
                data = upload.file.read(CHUNK)
                if not data:
                    break
                total += len(data)
                if total > max_bytes:
                    raise HTTPException(status_code=400, detail=error_detail)
                out.write(data)

    await asyncio.to_thread(_copy)


def _validated_llm_model(model: str | None) -> str | None:
    """Return model if it is in the allowlist, else None (caller falls back to the script default).
    Prevents unvalidated user input from reaching the Gemini API as a model identifier.
    """
    if not model:
        return None
    stripped = model.strip()
    if stripped in _ALLOWED_LLM_MODELS:
        return stripped
    logger.warning("llm_model not in allowlist, ignoring: %r", model)
    return None


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


@app.exception_handler(GCSAccessError)
def gcs_access_error_handler(request: Request, exc: GCSAccessError):
    """Return 503 with clear message when GCS bucket is not accessible (do not swallow)."""
    return JSONResponse(
        status_code=503,
        content={"detail": f"GCS reports bucket not accessible: {exc!s}"},
    )


@app.get("/api/llm-status")
def llm_status():
    """Check if GEMINI_API_KEY or GOOGLE_API_KEY is set (for Gemini Review)."""
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    return {"key_set": bool(key and key.strip())}


@app.get("/api/upload-config")
def upload_config():
    """Return upload capability flags so the UI can choose between direct and GCS-backed upload.

    When gcs_uploads_configured is true the UI should use the two-phase GCS upload flow
    (POST /api/prepare-upload → browser PUT to GCS → POST /api/run/custom/stream with session_id)
    to bypass Cloud Run's 32 MB HTTP request limit.
    """
    return {"gcs_uploads_configured": _gcs_uploads.is_gcs_uploads_configured()}


class _PrepareUploadRequest(BaseModel):
    files: list[dict]


@app.post("/api/prepare-upload")
async def prepare_upload(body: _PrepareUploadRequest):
    """Create a GCS upload session and return signed PUT URLs for direct browser-to-GCS upload.

    Request body:
        {
            "files": [
                {"name": "data.tmcf", "size": 1234, "role": "tmcf"},
                {"name": "data.csv",  "size": 5000000, "role": "csv"},
                {"name": "stat_vars.mcf", "size": 2000, "role": "stat_vars_mcf"}
            ]
        }

    Response:
        {
            "session_id": "abc123...",
            "upload_urls": [
                {"filename": "data.tmcf", "url": "https://storage.googleapis.com/...", "content_type": "text/plain", "role": "tmcf"},
                ...
            ]
        }

    Returns 503 if GCS is not configured, 400 for invalid input.
    """
    if not _gcs_uploads.is_gcs_uploads_configured():
        raise HTTPException(
            status_code=503,
            detail="GCS uploads are not configured (GCS_REPORTS_BUCKET not set). Use direct file upload instead.",
        )
    try:
        result = await asyncio.to_thread(_gcs_uploads.create_upload_session, body.files)
        logger.info("upload_session_created session_id=%s files=%d", result["session_id"], len(body.files))
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to create upload session")
        raise HTTPException(status_code=500, detail=f"Failed to create upload session: {exc}")


@app.get("/api/datasets")
def list_datasets():
    return {
        "datasets": [
            {"id": "child_birth", "label": "Child Birth", "description": "Sample dataset (sample_data/child_birth/: TMCF, CSV, stat_vars.mcf). Expect PASS."},
            {"id": "statistics_poland", "label": "Statistics Poland", "description": "Sample dataset from data repo statvar_imports/statistics_poland/test/ (TMCF, CSV, stat_vars, stat_vars_schema)."},
            {"id": "finland_census", "label": "Finland Census", "description": "Sample dataset from data repo statvar_imports/finland_census/test_data/ (TMCF, CSV, stat_vars, stat_vars_schema)."},
            {"id": "uae_population", "label": "UAE Population", "description": "Sample dataset from data repo uae_bayanat/uae_population/test_data/ (TMCF, CSV)."},
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


def _sanitize_dataset_name(name: str) -> str:
    """Validate a user-provided dataset name against the required format.

    Returns the name unchanged if valid, or an empty string if invalid.
    Valid format: ^[a-z0-9-]{3,48}$ (matches the UI enforcement rule).
    """
    name = name.strip()
    return name if re.fullmatch(r"[a-z0-9-]{3,48}", name) else ""


async def _delete_gcs_session_bg(session_id: str) -> None:
    """Best-effort background task to delete a GCS upload session after validation."""
    try:
        deleted = await asyncio.to_thread(_gcs_uploads.delete_session, session_id)
        logger.info("gcs_session_deleted session_id=%s blobs=%d", session_id, deleted)
    except Exception:
        logger.warning("gcs_session_delete_failed session_id=%s", session_id)


async def _run_custom_validation_impl(
    request: Request,
    tmcf: UploadFile | None,
    csv: list[UploadFile] | None,
    stat_vars_mcf: UploadFile | None,
    stat_vars_schema_mcf: UploadFile | None,
    rules: str | None,
    llm_review: str | None,
    llm_model: str | None,
    stream: bool,
    dataset_name: str | None = None,
    existence_checks: str | None = None,
    session_id: str | None = None,
):
    """Shared implementation for /api/run/custom and /api/run/custom/stream.

    Supports two file-delivery modes:
    - Direct upload: tmcf and csv are UploadFile objects (existing behaviour).
    - GCS session:   session_id is set; files are downloaded from GCS (Cloud Run
                     large-file path — bypasses the 32 MB HTTP body limit).

    Either session_id OR (tmcf + csv) must be supplied; never both.
    """
    script = SCRIPT_DIR / "run_e2e_test.sh"
    if not script.exists():
        raise HTTPException(status_code=500, detail="run_e2e_test.sh not found")

    # Use per-run upload directory to prevent concurrent requests from overwriting each other's files.
    # request_id is set by LoggingMiddleware before this function is called.
    request_id = getattr(request.state, "request_id", "") or uuid.uuid4().hex[:12]
    run_upload_dir = CUSTOM_UPLOAD_DIR / request_id
    run_upload_dir.mkdir(parents=True, exist_ok=True)
    tmcf_path = run_upload_dir / "input.tmcf"
    csvs_dir = run_upload_dir / "csvs"
    csvs_dir.mkdir(parents=True, exist_ok=True)
    stat_vars_mcf_path = run_upload_dir / "input_stat_vars.mcf"
    stat_vars_schema_mcf_path = run_upload_dir / "input_stat_vars_schema.mcf"

    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    _size_display = f"{MAX_UPLOAD_MB // 1024} GB" if MAX_UPLOAD_MB >= 1024 else f"{MAX_UPLOAD_MB} MB"

    try:
        if session_id:
            # ── GCS session path: download files from GCS to local disk ──────────────
            logger.info("gcs_session_download session_id=%s request_id=%s", session_id, request_id)
            try:
                downloaded = await _gcs_uploads.download_session_to_dir(session_id, run_upload_dir)
            except FileNotFoundError as exc:
                raise HTTPException(status_code=400, detail=str(exc))
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))
            except Exception as exc:
                logger.exception("GCS session download failed session_id=%s", session_id)
                raise HTTPException(status_code=500, detail=f"Failed to download upload session: {exc}")

            tmcf_path = downloaded.get("tmcf")
            csv_paths: list[Path] = downloaded.get("csvs") or []

            if not tmcf_path or not tmcf_path.exists():
                raise HTTPException(status_code=400, detail="TMCF file missing from upload session")
            if not csv_paths:
                raise HTTPException(status_code=400, detail="No CSV files found in upload session")

            # Kick off background GCS session cleanup (non-blocking, best-effort).
            # The files have been copied to local disk so the session is no longer needed.
            asyncio.create_task(_delete_gcs_session_bg(session_id))

        else:
            # ── Direct upload path: stream UploadFile objects to disk ────────────────
            if not tmcf:
                raise HTTPException(status_code=400, detail="TMCF file is required")
            if not csv:
                raise HTTPException(status_code=400, detail="At least one CSV file is required")

            # Early reject: Content-Length of the whole multipart body exceeds the per-file limit.
            # This is a fast-path guard; the per-file streaming check is the authoritative enforcement.
            _cl_header = request.headers.get("content-length")
            if _cl_header and _cl_header.isdigit() and int(_cl_header) > max_bytes:
                raise HTTPException(
                    status_code=400,
                    detail=f"Upload too large. Maximum CSV size is {_size_display} per file.",
                )

            # TMCF files are always small — load into memory as before.
            tmcf_content = await tmcf.read()
            if len(tmcf_content) > max_bytes:
                raise HTTPException(status_code=400, detail=f"TMCF file exceeds {_size_display} limit")
            tmcf_path.write_bytes(tmcf_content)

            csv_paths = []
            for i, csv_file in enumerate(csv):
                # Preserve the original filename (sanitized) so the TMCF table reference C:name->col
                # matches the saved file (dc-import derives the table name from the filename stem).
                # Uniqueness is guaranteed by the per-run upload directory, not by filename prefixing.
                orig_name = Path(csv_file.filename).name if csv_file.filename else ""
                safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", orig_name) if orig_name else ""
                csv_save_name = safe_name if safe_name.lower().endswith(".csv") else f"input_{i:02d}.csv"
                csv_save_path = csvs_dir / csv_save_name
                # Stream-copy in chunks to avoid loading multi-GB files into RAM.
                await _stream_upload_to_file(
                    csv_file, csv_save_path, max_bytes,
                    f"CSV file '{csv_file.filename or csv_save_name}' exceeds {_size_display} limit",
                )
                csv_paths.append(csv_save_path)

            if stat_vars_mcf and stat_vars_mcf.filename:
                content = await stat_vars_mcf.read()
                if len(content) > max_bytes:
                    raise HTTPException(status_code=400, detail="Stat vars MCF file exceeds size limit")
                stat_vars_mcf_path.write_bytes(content)

            if stat_vars_schema_mcf and stat_vars_schema_mcf.filename:
                content = await stat_vars_schema_mcf.read()
                if len(content) > max_bytes:
                    raise HTTPException(status_code=400, detail="Stat vars schema MCF file exceeds size limit")
                stat_vars_schema_mcf_path.write_bytes(content)

    except HTTPException:
        # Clean up the upload dir so rejected requests do not accumulate files on disk.
        shutil.rmtree(run_upload_dir, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(run_upload_dir, ignore_errors=True)
        logger.exception("Error saving custom uploads or preparing run")
        raise HTTPException(status_code=500, detail=str(e))

    if not dataset_name or not _sanitize_dataset_name(dataset_name):
        raise HTTPException(
            status_code=400,
            detail="dataset_name is required and must be 3–48 characters using only lowercase letters, numbers, and hyphens (e.g. canada-population).",
        )
    baseline_name = f"custom_{dataset_name.strip()}"
    logger.info("custom run dataset_name=%r baseline_name=%s request_id=%s", dataset_name, baseline_name, request_id)

    rule_ids = [x.strip() for x in (rules or "").split(",") if x.strip()] if rules else []
    config_path = _create_filtered_config("custom", rule_ids)

    extra_env = _existence_checks_env(existence_checks)
    logger.info(
        "custom run existence_checks=%s request_id=%s",
        extra_env["IMPORT_EXISTENCE_CHECKS"],
        request_id,
    )

    try:
        args = ["bash", str(script), "custom", f"--tmcf={tmcf_path}"]
        for _csv_path in csv_paths:
            args.append(f"--csv={_csv_path}")
        args.append(f"--baseline-name={baseline_name}")
        if stat_vars_mcf_path.exists():
            args.append(f"--stat-vars-mcf={stat_vars_mcf_path}")
        if stat_vars_schema_mcf_path.exists():
            args.append(f"--stat-vars-schema-mcf={stat_vars_schema_mcf_path}")
        if config_path:
            args.extend([f"--config={config_path}"])
        llm_enabled = _llm_review_enabled(llm_review)
        if llm_enabled:
            args.append("--llm-review")
            validated_model = _validated_llm_model(llm_model)
            if validated_model:
                args.append(f"--model={validated_model}")
        else:
            args.append("--no-llm-review")
            logger.info("LLM review disabled for this run")
        # request_id was already extracted for the upload dir above; reuse it here.
        output_dir = (OUTPUT_DIR / "custom" / request_id) if request_id else DATASET_OUTPUT_MAP["custom"]
        canonical_output_dir = DATASET_OUTPUT_MAP["custom"]
        return await _run_validation_process(
            args, request, config_path, stream=stream, app_root=APP_ROOT,
            output_dir=output_dir, dataset="custom", canonical_output_dir=canonical_output_dir,
            # Pass run_upload_dir so the runner cleans it after the subprocess exits.
            # For streaming runs this happens in the generator's finally; for non-streaming
            # runs in the impl's finally. Both paths run after the subprocess has exited.
            extra_cleanup_dirs=[run_upload_dir],
            # baseline_id lets the UI call /api/accept-baseline/custom with the right dataset_id.
            extra_done_fields={"baseline_id": baseline_name},
            extra_env=extra_env,
        )
    except HTTPException:
        # Runner raised before or during startup (e.g. 429). Clean up locally since the
        # runner's cleanup callbacks were never registered or never reached.
        if config_path and config_path.exists():
            config_path.unlink(missing_ok=True)
        shutil.rmtree(run_upload_dir, ignore_errors=True)
        raise
    except Exception as e:
        if config_path and config_path.exists():
            config_path.unlink(missing_ok=True)
        shutil.rmtree(run_upload_dir, ignore_errors=True)
        logger.exception("Error running custom validation%s", " (stream)" if stream else "")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run/custom/stream")
async def run_custom_validation_stream(
    request: Request,
    session_id: str | None = Form(None),
    tmcf: UploadFile | None = File(None),
    csv: list[UploadFile] | None = File(None),
    stat_vars_mcf: UploadFile | None = File(None),
    stat_vars_schema_mcf: UploadFile | None = File(None),
    rules: str | None = Form(None),
    llm_review: str | None = Form(None),
    llm_model: str | None = Form(None),
    dataset_name: str | None = Form(None),
    existence_checks: str | None = Form(None),
):
    """Run validation with streaming output.

    Supports two file-delivery modes:
    - Direct upload: include tmcf and csv as multipart file fields (default).
    - GCS session:   include session_id (from /api/prepare-upload) without file fields;
                     the server downloads the files from GCS before running validation.
    """
    return await _run_custom_validation_impl(
        request, tmcf, csv, stat_vars_mcf, stat_vars_schema_mcf, rules, llm_review, llm_model, stream=True,
        dataset_name=dataset_name, existence_checks=existence_checks, session_id=session_id,
    )


@app.post("/api/run/custom")
async def run_custom_validation(
    request: Request,
    session_id: str | None = Form(None),
    tmcf: UploadFile | None = File(None),
    csv: list[UploadFile] | None = File(None),
    stat_vars_mcf: UploadFile | None = File(None),
    stat_vars_schema_mcf: UploadFile | None = File(None),
    rules: str | None = Form(None),
    llm_review: str | None = Form(None),
    llm_model: str | None = Form(None),
    dataset_name: str | None = Form(None),
    existence_checks: str | None = Form(None),
):
    """Run validation (non-streaming).

    Supports two file-delivery modes:
    - Direct upload: include tmcf and csv as multipart file fields (default).
    - GCS session:   include session_id (from /api/prepare-upload) without file fields.
    """
    return await _run_custom_validation_impl(
        request, tmcf, csv, stat_vars_mcf, stat_vars_schema_mcf, rules, llm_review, llm_model, stream=False,
        dataset_name=dataset_name, existence_checks=existence_checks, session_id=session_id,
    )


@app.post("/api/run/{dataset}")
async def run_validation(
    dataset: str,
    request: Request,
    rules: str | None = Query(None),
    stream: bool = Query(False),
    llm_review: str | None = Query(None),
    llm_model: str | None = Query(None),
    existence_checks: str | None = Query(None),
):
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    script = SCRIPT_DIR / "run_e2e_test.sh"
    if not script.exists():
        raise HTTPException(status_code=500, detail="run_e2e_test.sh not found")
    rule_ids = [x.strip() for x in (rules or "").split(",") if x.strip()] if rules else []
    config_path = _create_filtered_config(dataset, rule_ids)
    extra_env = _existence_checks_env(existence_checks)
    logger.info(
        "run existence_checks=%s dataset=%s request_id=%s",
        extra_env["IMPORT_EXISTENCE_CHECKS"],
        dataset,
        getattr(request.state, "request_id", ""),
    )
    try:
        args = ["bash", str(script), dataset]
        if config_path:
            args.extend([f"--config={config_path}"])
        llm_enabled = _llm_review_enabled(llm_review)
        if llm_enabled:
            args.append("--llm-review")
            validated_model = _validated_llm_model(llm_model)
            if validated_model:
                args.append(f"--model={validated_model}")
        else:
            args.append("--no-llm-review")
            logger.info("LLM review disabled for this run")
        request_id = getattr(request.state, "request_id", "")
        output_dir = (OUTPUT_DIR / dataset / request_id) if request_id else DATASET_OUTPUT_MAP[dataset]
        canonical_output_dir = DATASET_OUTPUT_MAP[dataset]
        return await _run_validation_process(
            args, request, config_path, stream, app_root=APP_ROOT,
            output_dir=output_dir, dataset=dataset, canonical_output_dir=canonical_output_dir,
            extra_env=extra_env,
        )
    except HTTPException:
        # Clean up temp config on 429 or other HTTP errors raised before the runner
        # could register its own cleanup (runner's finally is not reached on fast raises).
        if config_path and config_path.exists():
            config_path.unlink(missing_ok=True)
        raise
    except Exception as e:
        if config_path and config_path.exists():
            config_path.unlink(missing_ok=True)
        logger.exception("Error running validation for dataset %s", dataset)
        raise HTTPException(status_code=500, detail=str(e))


def _run_id_safe(run_id: str) -> bool:
    """Reject run_id that could escape OUTPUT_DIR (path traversal)."""
    return run_id is not None and "/" not in run_id and ".." not in run_id and "\x00" not in run_id


def _load_differ_stats_from_gcs(run_id: str, dataset: str) -> dict | None:
    """Download differ_output from GCS into a temp dir and return parsed differ_stats.

    Returns None when the files are absent (run predates this feature, differ was
    skipped, or GCS is not configured).  The caller is responsible for merging
    baseline provenance fields from the GCS manifest on top of the returned dict.
    """
    raw_summary = gcs_reports.get_report_from_gcs(
        run_id, dataset, "differ_output/differ_summary.json"
    )
    raw_csv = gcs_reports.get_report_from_gcs(
        run_id, dataset, "differ_output/obs_diff_summary.csv"
    )
    if raw_summary is None and raw_csv is None:
        return None
    with tempfile.TemporaryDirectory(prefix="gcs_differ_") as tmp:
        tmp_path = Path(tmp)
        differ_dir = tmp_path / "differ_output"
        differ_dir.mkdir()
        if raw_summary is not None:
            (differ_dir / "differ_summary.json").write_bytes(raw_summary)
        if raw_csv is not None:
            (differ_dir / "obs_diff_summary.csv").write_bytes(raw_csv)
        return _load_differ_stats(tmp_path)


def _resolve_artifact(dataset: str, run_id: str | None, filename: str) -> bytes | None:
    """Resolve artifact bytes via: GCS (when configured) → local per-run dir → canonical output dir.

    Returns raw bytes if found, None if not found at any level.
    When run_id is None, returns None immediately — canonical artifacts are not served
    without an explicit run_id to prevent stale data from appearing before any run.
    When GCS is configured and a run_id is present, GCS is the authoritative source so
    any instance (e.g. Cloud Run replica) can serve results; local is not consulted.
    """
    if not run_id or not _run_id_safe(run_id):
        return None
    raw = gcs_reports.get_report_from_gcs(run_id, dataset, filename)
    if raw is not None:
        return raw
    if is_gcs_configured():
        return None  # GCS is source of truth; do not fall through to local
    per_run_path = OUTPUT_DIR / dataset / run_id / filename
    if per_run_path.exists():
        try:
            return per_run_path.read_bytes()
        except OSError:
            return None
    # Per-run dir was cleaned up — fall back to canonical (latest) output
    output_dir = DATASET_OUTPUT_MAP.get(dataset)
    if not output_dir:
        return None
    path = output_dir / filename
    if not path.exists():
        return None
    try:
        return path.read_bytes()
    except OSError:
        return None


@app.get("/api/validation-result/{dataset}")
def get_validation_result(dataset: str, run_id: str | None = Query(None)):
    """Return per-rule validation results from validation_output.json. When GCS is configured and run_id is set, read from GCS only (no local fallback) so any instance can serve."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    raw = _resolve_artifact(dataset, run_id, "validation_output.json")
    if raw is None:
        return {"exists": False, "results": []}
    try:
        results = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"exists": True, "results": []}
    if not isinstance(results, list):
        results = []
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
        ai_advisory_count = len(issues) - len(blockers)
        return {"exists": True, "issues": issues, "passed": len(blockers) == 0, "ai_advisory_count": ai_advisory_count}

    raw = _resolve_artifact(dataset, run_id, "schema_review.json")
    if raw is None:
        return {"exists": False, "issues": [], "passed": True, "ai_advisory_count": 0}
    try:
        issues = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"exists": True, "issues": [], "passed": False, "ai_advisory_count": 0}
    return _issues_to_response(issues)


@app.get("/api/report-info/{dataset}")
def report_info(dataset: str, run_id: str | None = Query(None)):
    """Return report metadata including mtime. When GCS is configured and run_id is set, use GCS only (validation_report.html or schema_review.json mtime) so any instance can serve."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if not run_id or not _run_id_safe(run_id):
        return {"exists": False}
    if is_gcs_configured():
        mtime = gcs_reports.get_report_updated_from_gcs(run_id, dataset, "validation_report.html")
        if mtime is None:
            mtime = gcs_reports.get_report_updated_from_gcs(run_id, dataset, "schema_review.json")
        if mtime is not None:
            return {"exists": True, "mtime": mtime}
        return {"exists": False}
    mtime = gcs_reports.get_report_updated_from_gcs(run_id, dataset, "validation_report.html")
    if mtime is not None:
        return {"exists": True, "mtime": mtime}
    per_run = OUTPUT_DIR / dataset / run_id
    for name in ("validation_report.html", "schema_review.json"):
        p = per_run / name
        if p.exists():
            return {"exists": True, "mtime": p.stat().st_mtime}
    # Per-run dir was cleaned up — fall back to canonical
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / "validation_report.html"
    if not path.exists():
        return {"exists": False}
    return {"exists": True, "mtime": path.stat().st_mtime}


def _get_fluctuation_samples_internal(dataset: str, run_id: str | None) -> tuple[bool, list]:
    """Return (exists, samples) for the given dataset and optional run_id."""
    if dataset not in DATASET_OUTPUT_MAP:
        return False, []
    raw = _resolve_artifact(dataset, run_id, "report.json")
    if raw is None:
        return False, []
    try:
        report = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return True, []
    return True, _extract_fluctuation_samples(report)


def _get_lint_warnings_internal(dataset: str, run_id: str | None) -> tuple[bool, list[dict]]:
    """Return (exists, warnings) by aggregating report["entries"] where level is WARNING; exclude Existence_FailedDcCall_* (resolution diagnostics). Group by counterKey, count, sort descending."""
    if dataset not in DATASET_OUTPUT_MAP:
        return False, []
    raw = _resolve_artifact(dataset, run_id, "report.json")
    if raw is None:
        return False, []
    try:
        report = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return True, []
    if not report:
        return False, []
    entries = report.get("entries", [])
    if not isinstance(entries, list):
        return True, []
    # Aggregate from entries where level is WARNING; skip Existence_FailedDcCall_* (resolution diagnostics); group by counterKey, count occurrences
    LINT_RESOLUTION_PREFIX = "Existence_FailedDcCall_"
    count_by_key: dict[str, int] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        level = (e.get("level") or e.get("levelSummary") or "").strip()
        if level not in ("LEVEL_WARNING", "WARNING"):
            continue
        key = e.get("counterKey") or e.get("counter") or ""
        if not key or key.startswith(LINT_RESOLUTION_PREFIX):
            continue
        count_by_key[key] = count_by_key.get(key, 0) + 1
    warnings = [{"key": k, "count": c} for k, c in count_by_key.items()]
    warnings.sort(key=lambda x: (-x["count"], x["key"]))
    return True, warnings


@app.get("/api/lint-warnings/{dataset}")
def get_lint_warnings(dataset: str, run_id: str | None = Query(None)):
    """Return import tool LEVEL_WARNING counters from report.json (advisory only). Same report resolution as fluctuation-samples."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    exists, warnings = _get_lint_warnings_internal(dataset, run_id)
    return {"exists": exists, "warnings": warnings}


def _get_lint_errors_internal(dataset: str, run_id: str | None) -> tuple[bool, list[dict]]:
    """Return (exists, errors) from report.levelSummary.LEVEL_ERROR.counters,
    excluding Existence_FailedDcCall_* keys. Returns [{key, count}] sorted descending."""
    if dataset not in DATASET_OUTPUT_MAP:
        return False, []
    raw = _resolve_artifact(dataset, run_id, "report.json")
    if raw is None:
        return False, []
    try:
        report = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return True, []
    if not report:
        return False, []
    counters = (report.get("levelSummary") or {}).get("LEVEL_ERROR", {}).get("counters") or {}
    EXCLUDE_PREFIX = "Existence_FailedDcCall_"
    errors = [
        {"key": k, "count": int(v)}
        for k, v in counters.items()
        if not k.startswith(EXCLUDE_PREFIX)
    ]
    errors.sort(key=lambda x: (-x["count"], x["key"]))
    return True, errors


@app.get("/api/lint-errors/{dataset}")
def get_lint_errors(dataset: str, run_id: str | None = Query(None)):
    """Return structural LEVEL_ERROR counters from report.json (excludes Existence_FailedDcCall_*).
    Used by the UI to show a top-errors summary when check_structural_lint_error_count fails."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    exists, errors = _get_lint_errors_internal(dataset, run_id)
    return {"exists": exists, "errors": errors}


@app.get("/api/fluctuation-samples/{dataset}")
def get_fluctuation_samples(dataset: str, run_id: str | None = Query(None)):
    """Return structured fluctuation samples from report.json. If run_id is set and GCS is configured, use GCS; else local per-run then canonical."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    exists, samples = _get_fluctuation_samples_internal(dataset, run_id)
    return {"exists": exists, "samples": samples}


# Limit concurrent calls to the fluctuation-interpretation endpoint. Each call blocks a
# thread (sync endpoint) and makes a Gemini API request. Without a cap, rapid UI clicks
# or scripted calls could exhaust the API key quota.
_FLUCTUATION_INTERP_SEMAPHORE = threading.Semaphore(5)


class FluctuationInterpretationRequest(BaseModel):
    """Request body for optional AI interpretation of a fluctuation sample (advisory only)."""
    statVar: str | None = None
    location: str | None = None
    period: str | None = None
    percent_change: float | None = None
    technical_signals: dict | None = None
    observation_period: str | None = None
    period_gap_years: float | None = None
    series_length: int | None = None


@app.post("/api/fluctuation-interpretation")
def post_fluctuation_interpretation(body: FluctuationInterpretationRequest = Body(...)):
    """Optional, UI-triggered AI interpretation of a fluctuation sample. Advisory only; never affects validation."""
    if not _FLUCTUATION_INTERP_SEMAPHORE.acquire(blocking=False):
        raise HTTPException(
            status_code=429,
            detail="Too many concurrent interpretation requests. Try again shortly.",
            headers={"Retry-After": "5"},
        )
    try:
        stat_var = body.statVar or ""
        location = body.location or ""
        period = body.period or ""
        percent_change = body.percent_change
        technical_signals = body.technical_signals or {}
        observation_period = body.observation_period or ""
        period_gap_years = body.period_gap_years
        series_length = body.series_length
        interpretation = _interpret_fluctuation(
            stat_var, location, period, percent_change, technical_signals, observation_period, period_gap_years, series_length
        )
        if interpretation is None and not _get_gemini_api_key():
            return {"ai_interpretation": None, "error": "GEMINI_API_KEY or GOOGLE_API_KEY not set"}
        return {"ai_interpretation": interpretation}
    finally:
        _FLUCTUATION_INTERP_SEMAPHORE.release()


@app.get("/api/rule-failure-samples/{dataset}")
def get_rule_failure_samples(dataset: str, run_id: str | None = Query(None)):
    """Return structured rule failure samples from validation_output.json. If run_id is set and GCS is
    configured, use GCS; else local per-run then canonical (with enrichment when report.json/input.csv
    are present). Artifact resolution uses _resolve_artifact; enrichment source is determined separately
    because it requires a directory or raw GCS bytes depending on the storage tier."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")

    # --- Step 1: resolve validation_output.json via the shared helper ---
    raw_vo = _resolve_artifact(dataset, run_id, "validation_output.json")
    if raw_vo is None:
        return {"exists": False, "samples": []}
    try:
        results = json.loads(raw_vo.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"exists": True, "samples": []}
    if not isinstance(results, list):
        results = []
    samples = extract_rule_failure_samples(results)

    # --- Step 2: enrich samples with per-row detail from report.json + input.csv ---
    # Enrichment is best-effort: failures are silently ignored and samples are returned
    # without enrichment. The enrichment source mirrors the storage tier that _resolve_artifact
    # used: GCS when configured with a valid run_id, local per-run dir otherwise, or canonical.
    if run_id and _run_id_safe(run_id):
        raw_report = gcs_reports.get_report_from_gcs(run_id, dataset, "report.json")
        raw_csv = gcs_reports.get_report_from_gcs(run_id, dataset, "input.csv")
        if raw_report is not None and raw_csv is not None:
            # GCS path: write CSV to a temp dir so enrich_rule_failure_samples can read it
            try:
                report = json.loads(raw_report.decode("utf-8"))
                with tempfile.TemporaryDirectory(prefix="gcs_rule_failure_") as tmp:
                    tmp_path = Path(tmp)
                    (tmp_path / "input.csv").write_bytes(raw_csv)
                    report["commandArgs"] = report.get("commandArgs") or {}
                    report["commandArgs"]["inputFiles"] = [str(tmp_path / "input.csv")]
                    (tmp_path / "report.json").write_text(json.dumps(report), encoding="utf-8")
                    enrich_rule_failure_samples(samples, tmp_path, results)
            except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                pass
        elif not is_gcs_configured():
            # Local path: enrich directly from the per-run output directory if it exists
            per_run_dir = OUTPUT_DIR / dataset / run_id
            if per_run_dir.is_dir():
                enrich_rule_failure_samples(samples, per_run_dir, results)
            else:
                # Per-run dir absent (e.g. _resolve_artifact fell back to canonical).
                # Enrich from canonical so callers always get the best available data.
                enrich_rule_failure_samples(samples, DATASET_OUTPUT_MAP[dataset], results)
    else:
        enrich_rule_failure_samples(samples, DATASET_OUTPUT_MAP[dataset], results)

    return {"exists": True, "samples": samples}


@app.get("/api/review-summary/{dataset}")
def get_review_summary(dataset: str, format: str | None = Query(None), run_id: str | None = Query(None), baseline_id: str | None = Query(None)):
    """Return combined review summary (validation + Gemini + fluctuation + rule failures). If run_id is set and GCS configured, use GCS."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    data = None
    if run_id and _run_id_safe(run_id):
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
                # differ_stats / current_baseline_run_id — GCS path.
                # GCS is the sole authoritative source for differ_stats on Cloud Run.
                # Canonical differ_output is not used here — it lives on the local filesystem
                # and is not reliable across Cloud Run replicas or after container restarts.
                # differ_output files are uploaded to GCS with each run so any instance can
                # load the full diff counts. Provenance fields are always merged from the GCS
                # baseline manifest, which is the authoritative source for version/date/approver.
                if data is not None:
                    _gcs_versions: list = []
                    try:
                        # Use the caller-supplied baseline_id for custom datasets
                        # (e.g. "custom_fbi-gov-crime"); fall back to dataset name
                        # for built-in datasets where they are identical.
                        _resolved_baseline_id = baseline_id or dataset
                        _gcs_versions = _gcs_baselines.list_baseline_versions(_resolved_baseline_id)
                    except Exception:
                        pass
                    if _gcs_versions:
                        latest = _gcs_versions[0]
                        # Load full differ_stats (counts + changed StatVars) from GCS.
                        # Falls back to an empty dict for runs that predate this feature
                        # or where the differ was skipped — provenance is always filled in.
                        differ_stats = _load_differ_stats_from_gcs(run_id, dataset) or {}
                        # Provenance comes from the GCS manifest (authoritative on Cloud Run).
                        # Local manifest files are not readable from Cloud Run instances.
                        differ_stats["baseline_run_id"] = latest.get("run_id")
                        differ_stats["baseline_version"] = latest.get("version")
                        differ_stats["baseline_updated_at"] = latest.get("updated_at")
                        differ_stats["baseline_accepted_by"] = latest.get("accepted_by")
                        data["differ_stats"] = differ_stats
                        data["current_baseline_run_id"] = latest.get("run_id")
                    # else: no GCS baselines — differ_stats and current_baseline_run_id stay None
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        if data is None and run_id and is_gcs_configured():
            raise HTTPException(status_code=404, detail="No validation result in GCS for this run. Run validation or try again shortly.")
    if data is None:
        if run_id and _run_id_safe(run_id):
            output_dir = OUTPUT_DIR / dataset / run_id
            data = _build_review_summary(dataset, output_dir)
            # Per-run dir may have been cleaned up — fall back to canonical
            if data is None:
                canonical_dir = DATASET_OUTPUT_MAP.get(dataset)
                if canonical_dir:
                    data = _build_review_summary(dataset, canonical_dir)
        # When run_id is None, do not read canonical artifacts — caller has no run yet
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
    """Serve validation report from GCS when configured (so any instance can serve); otherwise local per-run."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if not _run_id_safe(run_id):
        raise HTTPException(status_code=400, detail="Invalid run_id")
    content = gcs_reports.get_report_from_gcs(run_id, dataset, "validation_report.html")
    if content is None:
        if is_gcs_configured():
            raise HTTPException(
                status_code=404,
                detail="Report not found. It may not have been uploaded to GCS yet.",
            )
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


_CSV_FILENAME = "validation_warnings_and_advisories.csv"


def _csv_download_filename(dataset: str, run_id: str | None = None) -> str:
    """Filename for CSV download: validator_findings_<dataset>_<run_id|latest>.csv
    Characters that could break a quoted Content-Disposition filename value are stripped.
    """
    raw = (run_id or "latest").replace(":", "-")
    for ch in ('"', "'", "\r", "\n", "\\", "\x00", ";"):
        raw = raw.replace(ch, "")
    if not raw:
        raw = "unknown"
    return f"validator_findings_{dataset}_{raw}.csv"


@app.get("/report/{dataset}/{run_id}/validation_warnings_and_advisories.csv")
def serve_warnings_csv_by_run_id(dataset: str, run_id: str):
    """Serve warnings/advisories CSV from GCS when configured; otherwise local per-run."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if not _run_id_safe(run_id):
        raise HTTPException(status_code=400, detail="Invalid run_id")
    content = gcs_reports.get_report_from_gcs(run_id, dataset, _CSV_FILENAME)
    if content is None:
        if is_gcs_configured():
            raise HTTPException(
                status_code=404,
                detail="Warnings CSV not found. It may not have been uploaded to GCS yet.",
            )
        per_run_path = OUTPUT_DIR / dataset / run_id / _CSV_FILENAME
        if not per_run_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Warnings CSV not found. It may not have been uploaded yet, or GCS is not configured.",
            )
        content = per_run_path.read_bytes()
    filename = _csv_download_filename(dataset, run_id)
    return Response(
        content=content,
        media_type="text/csv",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@app.get("/report/{dataset}/validation_warnings_and_advisories.csv")
def serve_warnings_csv(dataset: str):
    """Serve warnings/advisories CSV from local disk (latest run for this dataset)."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    output_dir = DATASET_OUTPUT_MAP[dataset]
    path = output_dir / _CSV_FILENAME
    if not path.exists():
        raise HTTPException(status_code=404, detail="Warnings CSV not found. Run validation first.")
    filename = _csv_download_filename(dataset, None)
    return FileResponse(
        path,
        media_type="text/csv",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@app.get("/summary-report/{dataset}/{run_id}", response_class=HTMLResponse)
def serve_summary_report_by_run_id(dataset: str, run_id: str):
    """Serve summary_report.html from GCS when configured (any instance); otherwise local per-run."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    content = gcs_reports.get_report_from_gcs(run_id, dataset, "summary_report.html")
    if content is None:
        if is_gcs_configured():
            raise HTTPException(
                status_code=404,
                detail="Summary report not found. It may not have been uploaded to GCS yet.",
            )
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


@app.post("/api/accept-baseline/{dataset}")
async def accept_baseline(dataset: str, body: dict = Body(default={})):
    """Promote the MCF output of a completed run to become the new versioned baseline.

    Body fields:
      run_id      (optional) – used to locate the per-run output directory.
      baseline_id (optional) – the baseline dataset_id to write to. Defaults to
                                dataset for named datasets. Required for custom.
      accepted_by (optional) – display name of the approver for the manifest.
    """
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")

    run_status = body.get("run_status") or None
    if run_status != "success":
        raise HTTPException(
            status_code=400,
            detail="Cannot accept baseline: the run did not succeed (run_status must be 'success').",
        )

    run_id = body.get("run_id") or None
    if run_id and not _run_id_safe(run_id):
        raise HTTPException(status_code=400, detail="Invalid run_id")

    baseline_id: str = body.get("baseline_id") or (dataset if dataset != "custom" else "")
    if not baseline_id:
        raise HTTPException(status_code=400, detail="baseline_id is required for custom datasets")

    accepted_by: str | None = body.get("accepted_by") or None

    # Idempotency guard: reject if this run_id is already the current baseline.
    if run_id:
        try:
            versions = _gcs_baselines.list_baseline_versions(baseline_id)
            if versions and versions[0].get("run_id") == run_id:
                raise HTTPException(
                    status_code=409,
                    detail="This run has already been accepted as the current baseline.",
                )
        except HTTPException:
            raise
        except Exception:
            pass  # Non-fatal: if the check fails, allow the update to proceed

    # Locate MCF files: per-run dir first (if run_id set and dir not yet cleaned up),
    # then the canonical output dir (which always receives MCF copies after each run).
    genmcf_dir: Path | None = None
    if run_id:
        candidate = OUTPUT_DIR / dataset / run_id
        if candidate.is_dir() and list(candidate.glob("*.mcf")):
            genmcf_dir = candidate
    if genmcf_dir is None:
        canonical = DATASET_OUTPUT_MAP[dataset]
        if canonical.is_dir() and list(canonical.glob("*.mcf")):
            genmcf_dir = canonical
    if genmcf_dir is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "MCF output not found. The run output may have been cleaned up. "
                "Re-run validation before accepting a new baseline."
            ),
        )

    cmd = [
        sys.executable,
        str(_SCRIPTS_DIR / "run_differ.py"),
        "--update_baseline",
        f"--current_mcf_dir={genmcf_dir}",
        f"--dataset_id={baseline_id}",
    ]
    if run_id:
        cmd.append(f"--run_id={run_id}")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(APP_ROOT),
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode(errors="replace").strip() or "Baseline update failed"
            logger.error(
                "accept_baseline failed dataset=%s baseline_id=%s run_id=%s: %s",
                dataset, baseline_id, run_id, err,
            )
            raise HTTPException(status_code=500, detail=err)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("accept_baseline error dataset=%s baseline_id=%s", dataset, baseline_id)
        raise HTTPException(status_code=500, detail=str(e))

    # Extract version from subprocess stdout first (fast path — no storage round-trip).
    # run_differ.py emits {"baseline_version": "vN"} on success.
    # Falls back to list_baseline_versions() which works for both local and GCS.
    version: str | None = None
    try:
        for line in stdout.decode(errors="replace").splitlines():
            line = line.strip()
            if line.startswith("{"):
                parsed = json.loads(line)
                version = parsed.get("baseline_version") or None
                if version:
                    break
    except Exception:
        pass
    if not version:
        try:
            versions = _gcs_baselines.list_baseline_versions(baseline_id)
            if versions:
                version = versions[0].get("version")
        except Exception:
            pass

    # If accepted_by was provided but not yet in manifest (run_differ doesn't pass it through),
    # patch it via the public storage API so it works for both local and GCS.
    if accepted_by and version:
        try:
            _gcs_baselines.patch_manifest_field(baseline_id, version, "accepted_by", accepted_by)
        except Exception:
            pass  # non-fatal

    logger.info(
        "accept_baseline ok dataset=%s baseline_id=%s run_id=%s version=%s accepted_by=%s",
        dataset, baseline_id, run_id, version, accepted_by,
    )
    return {"ok": True, "dataset": dataset, "baseline_id": baseline_id, "version": version, "run_id": run_id}


@app.get("/api/baseline-versions/{dataset}")
def get_baseline_versions(dataset: str, baseline_id: str | None = Query(None)):
    """Return baseline version history for a dataset (newest first).

    For named datasets the baseline_id equals the dataset name.
    For custom datasets pass the baseline_id (custom_{hash}) as a query param.
    Designed so a future history UI can call this without any other changes.
    """
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    bid = baseline_id or (dataset if dataset != "custom" else None)
    if not bid:
        return {"versions": []}
    try:
        return {"versions": _gcs_baselines.list_baseline_versions(bid)}
    except Exception as e:
        logger.warning("baseline-versions error dataset=%s baseline_id=%s: %s", dataset, bid, e)
        return {"versions": []}


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.get("/healthz")
def healthz():
    """Health check endpoint for infrastructure probes (Cloud Run, load balancers)."""
    return {"status": "ok"}


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "index.html")
