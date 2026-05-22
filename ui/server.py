#!/usr/bin/env python3
"""Web UI server for DC Import Validator.

Run with: uvicorn ui.server:app --reload --host 0.0.0.0 --port 8000

Logging: session ID + request_id; on Cloud Run logs go to stdout (captured by Cloud Logging);
locally to logs/dc_import_validator.log and console. See ui/app_logging.py.
"""

import asyncio
import datetime
from dataclasses import dataclass as _dataclass
import html
import ipaddress
import json
import os
import re
import secrets
import shutil
import socket
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
from ui.services import batch_runner as _batch_runner
from ui.services.job_status import get_job_status as _get_job_status
from ui.orchestration.executors.batch import BatchExecutor
from ui.orchestration.policy import BATCH, PolicyBlockedError, resolve_executor
from ui.orchestration.spec import BUILTIN_DATASETS
from ui.orchestration.runs import (
    build_run_created_response,
    effective_rules_filter,
    fetch_run_status,
    job_request_to_run_spec,
    normalize_run_status,
    pipeline_registry_payload,
    run_spec_with_batch_overrides,
    subprocess_legacy_hint,
)

CUSTOM_UPLOAD_DIR = APP_ROOT / "output" / "custom_upload"
MAX_UPLOAD_BYTES = 100 * 1024**3  # 100 GB per file

# Version info — set APP_VERSION and optionally COMMIT_SHA via env at deploy time.
# Example: APP_VERSION=v0.3.0 COMMIT_SHA=abc1234 uvicorn ...
_APP_VERSION = os.environ.get("APP_VERSION", "dev-local").strip() or "dev-local"
_COMMIT_SHA = os.environ.get("COMMIT_SHA", "").strip()[:7]  # short SHA only
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

def _validate_custom_rules(custom_rules: list) -> str | None:
    """Validate custom rules list. Returns error message if invalid, else None.

    Performs structural validation (query/condition present) plus a lightweight
    DuckDB EXPLAIN to catch obvious SQL syntax errors before the job is submitted.
    Dummy stats and differ tables (matching the real schemas) are injected so that
    column-name errors in those tables are caught here too.
    """
    # Dummy CTEs that mirror the real runtime table schemas exactly.
    # stats  → summary_report.csv produced by genmcf
    # differ → obs_diff_summary.csv produced by import_differ
    #
    # IMPORTANT: keep this in sync with the actual CSV headers:
    #   StatVar,NumPlaces,NumObservations,MinValue,MaxValue,NumObservationsDates,
    #   MinDate,MaxDate,MeasurementMethods,Units,ScalingFactors,observationPeriods
    # MinDate / MaxDate are TEXT (format "YYYY-MM"), NOT timestamps.
    _DUMMY_STATS = (
        "SELECT CAST(NULL AS TEXT) AS StatVar,"
        " CAST(NULL AS BIGINT) AS NumPlaces,"
        " CAST(NULL AS BIGINT) AS NumObservations,"
        " CAST(NULL AS DOUBLE) AS MinValue,"
        " CAST(NULL AS DOUBLE) AS MaxValue,"
        " CAST(NULL AS BIGINT) AS NumObservationsDates,"
        " CAST(NULL AS TEXT) AS MinDate,"
        " CAST(NULL AS TEXT) AS MaxDate,"
        " CAST(NULL AS TEXT) AS MeasurementMethods,"
        " CAST(NULL AS TEXT) AS Units,"
        " CAST(NULL AS TEXT) AS ScalingFactors,"
        " CAST(NULL AS TEXT) AS observationPeriods LIMIT 0"
    )
    # IMPORTANT: obs_diff_summary.csv is written with 'variableMeasured' by the DC differ
    # tool, but run_differ.py immediately renames it to 'StatVar' before the file is saved.
    # The column name at SQL runtime is therefore 'StatVar', NOT 'variableMeasured'.
    _DUMMY_DIFFER = (
        "SELECT CAST(NULL AS TEXT) AS StatVar,"
        " CAST(NULL AS BIGINT) AS ADDED,"
        " CAST(NULL AS BIGINT) AS DELETED,"
        " CAST(NULL AS BIGINT) AS MODIFIED LIMIT 0"
    )
    for i, rule in enumerate(custom_rules):
        if not isinstance(rule, dict):
            return f"custom_rules[{i}] must be an object"
        rule_id = rule.get("rule_id")
        if not isinstance(rule_id, str) or not rule_id.strip():
            return f"custom_rules[{i}].rule_id is required and must be a non-empty string"
        params = rule.get("params") or {}
        if not isinstance(params.get("query"), str) or not params["query"].strip():
            return f"custom_rules[{i}].params.query is required and must be a non-empty string"
        if not isinstance(params.get("condition"), str) or not params["condition"].strip():
            return f"custom_rules[{i}].params.condition is required and must be a non-empty string"
        # Pre-check SQL syntax via DuckDB EXPLAIN against real-schema dummy tables.
        try:
            import duckdb
            q = params["query"].strip().rstrip(";")
            c = params["condition"].strip()
            duckdb.execute(
                f"EXPLAIN WITH stats AS ({_DUMMY_STATS}), differ AS ({_DUMMY_DIFFER}),"
                f" _data AS ({q}) SELECT * FROM _data WHERE NOT ({c}) LIMIT 1"
            )
        except Exception as exc:
            rule_id = (rule.get("rule_id") or f"rule {i}")
            return f"SQL syntax error in {rule_id}: {exc}"
    return None


def _validate_config_bytes(content: bytes, source: str = "") -> None:
    """Parse and structurally validate a validation config JSON blob.

    Raises HTTPException 400 on invalid JSON or missing 'rules' array.
    Raises HTTPException 422 if any SQL_VALIDATOR rule fails the DuckDB EXPLAIN pre-check.
    """
    try:
        cfg = json.loads(content)
    except json.JSONDecodeError as exc:
        detail = f"Validation config is not valid JSON: {exc}"
        if source:
            detail += f" (source: {source})"
        raise HTTPException(status_code=400, detail=detail)
    if not isinstance(cfg.get("rules"), list):
        raise HTTPException(status_code=400, detail="Validation config must have a 'rules' array")
    sql_rules = [r for r in cfg["rules"] if isinstance(r, dict) and r.get("validator") == "SQL_VALIDATOR"]
    if sql_rules:
        err = _validate_custom_rules(sql_rules)
        if err:
            raise HTTPException(status_code=422, detail=f"SQL syntax error in config: {err}")


async def _fetch_and_validate_config(url: str) -> bytes:
    """Fetch a validation config from a gs:// or https:// URL and validate its structure.

    Supported schemes:
    - gs://bucket/path/config.json  — downloaded via GCS SDK using Cloud Run ADC.
    - https://...                   — fetched via httpx with SSRF protections.

    Architecture invariant: this function is the *only* place that touches the user-supplied
    URI.  The caller always re-uploads the returned bytes to an internal controlled GCS path
    before any Batch VM sees them.  Batch VMs never receive the original gs:// or https://
    URI — they only consume the server-generated internal artifact.

    SSRF protections (https path):
    - Rejects http:// and any non-https/gs scheme.
    - Resolves the hostname and blocks RFC-1918, link-local (169.254.x.x), loopback,
      reserved, and multicast addresses before opening any connection.
    - Does NOT follow redirects — caller must supply the final URL.

    Raises HTTPException 400 on unsupported scheme, download/fetch failure, redirect
    response, disallowed destination address, invalid JSON, or missing rules array.
    Raises HTTPException 422 on SQL syntax errors inside SQL_VALIDATOR rules.
    """
    url = url.strip()

    # ── gs:// path ────────────────────────────────────────────────────────────
    if url.startswith("gs://"):
        err = _validate_gcs_uri(url)
        if err:
            raise HTTPException(status_code=400, detail=err)
        logger.info('[OVERRIDE_TRACE] %s', json.dumps({
            "component": "_fetch_and_validate_config", "event": "gs_fetch_start",
            "uri": url,
        }))
        fd, _tmp_path = tempfile.mkstemp(suffix=".json", prefix="validation_config_gs_")
        os.close(fd)
        try:
            await _download_gcs_uri(url, Path(_tmp_path))
            content = Path(_tmp_path).read_bytes()
        finally:
            try:
                os.unlink(_tmp_path)
            except OSError:
                pass
        logger.info('[OVERRIDE_TRACE] %s', json.dumps({
            "component": "_fetch_and_validate_config", "event": "gs_fetch_done",
            "uri": url, "size_bytes": len(content),
        }))
        _validate_config_bytes(content, source=url)
        return content

    # ── https:// path ─────────────────────────────────────────────────────────
    if not url.startswith("https://"):
        raise HTTPException(
            status_code=400,
            detail="validation_config_url must start with https:// or gs://",
        )

    # Auto-convert GitHub blob viewer URLs to raw content URLs so users can
    # paste the browser URL directly without finding the "Raw" button.
    _m = re.match(r"^https://github\.com/([^/]+/[^/]+)/blob/(.+)$", url)
    if _m:
        url = f"https://raw.githubusercontent.com/{_m.group(1)}/{_m.group(2)}"

    # SSRF guard: resolve ALL addresses for the hostname (both IPv4 and IPv6) and
    # reject any that are private/link-local/loopback/reserved/multicast.
    # Using getaddrinfo(AF_UNSPEC) instead of gethostbyname() is intentional:
    #   - gethostbyname() returns only one IPv4 address. If a host has a public A
    #     record but a private AAAA record, gethostbyname() would pass the check
    #     while httpx (which does its own dual-stack resolution) could connect via
    #     the private IPv6 address — a silent bypass.
    #   - getaddrinfo(AF_UNSPEC) returns every A and AAAA record. We block if ANY
    #     resolved address is disallowed, making the guard correct by construction.
    # Primary targets blocked: GCE metadata (169.254.169.254, link-local /16),
    # RFC-1918 private ranges, loopback (127.x.x.x / ::1), and multicast.
    # Residual risk: DNS rebinding (TOCTOU between this check and httpx's own
    # resolution). Accepted: requires attacker to control DNS AND have a reachable
    # internal IPv6 service on Cloud Run — not the realistic threat model here.
    from urllib.parse import urlparse as _urlparse
    _hostname = (_urlparse(url).hostname or "").lower()
    if not _hostname:
        raise HTTPException(status_code=400, detail="Invalid URL: missing hostname")
    try:
        _addr_infos = socket.getaddrinfo(_hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
        for _fam, _typ, _proto, _canon, _sockaddr in _addr_infos:
            try:
                _resolved = ipaddress.ip_address(_sockaddr[0])
            except ValueError:
                continue
            if (_resolved.is_private or _resolved.is_link_local or
                    _resolved.is_loopback or _resolved.is_reserved or _resolved.is_multicast):
                raise HTTPException(
                    status_code=400,
                    detail="validation_config_url resolves to a disallowed address",
                )
    except HTTPException:
        raise
    except OSError:
        raise HTTPException(status_code=400, detail=f"Failed to resolve hostname: {_hostname}")

    try:
        import httpx
        # follow_redirects=False: we do not chase redirects.  A redirect response
        # is surfaced as an explicit 400 so the user can supply the final URL.
        # This eliminates the redirect-to-internal-host attack vector entirely.
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=False) as client:
            resp = await client.get(url)
        if resp.is_redirect:
            raise HTTPException(
                status_code=400,
                detail="validation_config_url returned a redirect — provide the final URL directly",
            )
        resp.raise_for_status()
        content = resp.content
    except HTTPException:
        raise
    except __import__("httpx").HTTPStatusError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to fetch config URL (HTTP {exc.response.status_code}): {url}",
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to fetch config URL: {exc}")
    _validate_config_bytes(content, source=url)
    return content


async def _resolve_validation_config(
    upload: "UploadFile | None",
    url: str | None,
    dest_dir: Path,
) -> Path | None:
    """Resolve a user-supplied validation config to a file inside dest_dir.

    Priority: uploaded file > URL. Returns None if neither is provided.
    The resulting file is named validation_config_upload.json and is cleaned up
    with dest_dir (the run upload dir) after the run completes.
    """
    content: bytes | None = None
    if upload and getattr(upload, "filename", None):
        try:
            content = await upload.read()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Failed to read config file: {exc}")
        _validate_config_bytes(content, source=upload.filename)
    elif url and url.strip():
        content = await _fetch_and_validate_config(url)
    if content is None:
        return None
    dest = dest_dir / "validation_config_upload.json"
    dest.write_bytes(content)
    logger.info("resolved_validation_config bytes=%d", len(content))
    return dest


def _normalize_custom_rule(rule: dict) -> dict:
    """Return a copy of a custom rule with all fields required by validate_config_template.py.

    The template validator requires: rule_id, description, validator, scope, params.
    Custom rules submitted from the UI include rule_id/validator/scope/params but omit
    description, so we add a default here rather than requiring every caller to supply it.
    Only the five required fields (plus optional 'enabled') are allowed by the validator,
    so this function explicitly builds the output dict to avoid passing unknown keys through.
    """
    rule_id = rule["rule_id"]  # guaranteed non-empty by _validate_custom_rules
    # Make a copy of params so we can strip UI-only keys (nl_prompt) before the
    # rule reaches the DC framework runner, which only expects query and condition.
    params = dict(rule.get("params") or {})
    # NL-generated rules carry the original prompt in params.nl_prompt; use it as the
    # human-readable description so validation output identifies the rule by its intent,
    # then remove it so it is not forwarded to the DC runner as an unknown param.
    nl_prompt = params.pop("nl_prompt", "") or ""
    description = rule.get("description") or nl_prompt or f"Custom SQL rule: {rule_id}"
    normalized: dict = {
        "rule_id": rule_id,
        "description": description,
        "validator": rule.get("validator") or "SQL_VALIDATOR",
        "scope": rule.get("scope") or {"data_source": "stats"},
        "params": params,
    }
    if "enabled" in rule:
        normalized["enabled"] = rule["enabled"]
    return normalized


def _create_merged_config(dataset: str, rule_ids: list[str], custom_rules: list[dict]) -> Path | None:
    """Create temp config with filtered built-in rules plus appended custom rules.

    Returns path to a temp JSON file, or None when no modifications are needed
    (i.e. all built-in rules selected and no custom rules — use the default config).
    """
    if not rule_ids and not custom_rules:
        return None
    config_name = DATASET_CONFIG_MAP.get(dataset)
    if not config_name:
        return None
    config_path = CONFIG_DIR / config_name
    if not config_path.exists():
        return None
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)
    base_rules = config.get("rules", [])

    if rule_ids:
        rule_id_set = set(rule_ids)
        filtered_base = [r for r in base_rules if r.get("rule_id") in rule_id_set]
        selected_custom = [_normalize_custom_rule(r) for r in custom_rules]
    else:
        filtered_base = list(base_rules)
        selected_custom = [_normalize_custom_rule(r) for r in custom_rules]

    # Shortcut: all built-in rules kept and no custom rules — use the default config unchanged.
    if not custom_rules and len(filtered_base) == len(base_rules):
        return None

    all_rules = filtered_base + selected_custom
    if not all_rules:
        return None

    config["rules"] = all_rules
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


def _create_filtered_config(dataset: str, rule_ids: list[str]) -> Path | None:
    """Backwards-compat wrapper: create temp config with only the selected built-in rules."""
    return _create_merged_config(dataset, rule_ids, [])


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


# Allowlist of accepted Gemini model IDs for user-selectable schema review (Step 1).
# Values outside this set are rejected so unvalidated user input never reaches the API.
# Add entries only after verifying the model ID is live against the Gemini API.
_ALLOWED_LLM_MODELS: frozenset[str] = frozenset({
    "gemini-2.5-flash",
    "gemini-2.5-pro",
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


def _validate_gcs_uri(uri: str) -> str | None:
    """Return an error message if uri is not a valid gs:// URI, else None."""
    if not uri.startswith("gs://"):
        return f"GCS path must start with gs://: {uri!r}"
    without_scheme = uri[5:]
    bucket, _, blob = without_scheme.partition("/")
    if not bucket:
        return f"GCS path is missing bucket name: {uri!r}"
    if not blob:
        return f"GCS path is missing object path: {uri!r}"
    return None


async def _download_gcs_uri(uri: str, dest: Path) -> None:
    """Download a GCS object to dest. Raises HTTPException on permission or not-found errors."""
    logger.info("gcs_download uri=%s", uri)

    def _do() -> None:
        try:
            from google.cloud import storage
        except ImportError as exc:
            raise RuntimeError("google-cloud-storage is not installed") from exc
        bucket_name, _, blob_path = uri[5:].partition("/")
        dest.parent.mkdir(parents=True, exist_ok=True)
        storage.Client().bucket(bucket_name).blob(blob_path).download_to_filename(str(dest))

    try:
        await asyncio.to_thread(_do)
    except Exception as exc:
        code = getattr(exc, "code", None)
        name = type(exc).__name__
        if name == "NotFound" or code == 404:
            raise HTTPException(status_code=400, detail=f"GCS file not found: {uri}")
        if name in ("Forbidden", "PermissionDenied") or code == 403:
            raise HTTPException(status_code=400, detail=f"Permission denied accessing GCS file: {uri}")
        raise HTTPException(status_code=500, detail=f"Failed to download {uri}: {exc}")


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
    # Emit build metadata on every startup — visible in Cloud Run logs and lets
    # us confirm which image/SHA is actually serving (mirrors entrypoint.sh for Batch).
    import socket as _socket
    log.info(
        "[BUILD_INFO] sha=%s build=%s image=%s host=%s",
        os.environ.get("GIT_SHA", "unknown"),
        os.environ.get("BUILD_DATE", "unknown"),
        os.environ.get("BATCH_IMAGE_URI", "unknown"),
        _socket.gethostname(),
    )
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


_POLLING_PATH_PREFIXES = (
    "/api/review-summary/",
    "/api/validation-result/",
    "/api/lint-errors/",
    "/api/llm-status",
    "/api/upload-config",
    "/healthz",
)

class LoggingMiddleware(BaseHTTPMiddleware):
    """Assign request_id per request and log run_started for /api/run/*.

    Frequent polling endpoints are logged at DEBUG to keep Cloud Run logs
    focused on meaningful events (run_started, upload_session_created, etc.).
    """

    async def dispatch(self, request: Request, call_next):
        rid = uuid.uuid4().hex[:12]
        set_request_id(rid)
        request.state.request_id = rid
        try:
            path = request.url.path
            if any(path.startswith(p) for p in _POLLING_PATH_PREFIXES):
                logger.debug("request_started method=%s path=%s request_id=%s", request.method, path, rid)
            else:
                logger.info("request_started method=%s path=%s request_id=%s", request.method, path, rid)
            if path.startswith("/api/run/"):
                # Middleware runs before route match, so path_params is not set; parse path instead
                suffix = path.split("/api/run/", 1)[-1].lstrip("/")
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


@app.get("/api/version")
def app_version():
    """Return app version and optional commit SHA for display in the UI."""
    payload: dict = {"version": _APP_VERSION}
    if _COMMIT_SHA:
        payload["commit"] = _COMMIT_SHA
    return payload


_DEFAULT_SQL_SUGGESTIONS = [
    "every StatVar should have at least one observation",
    "no StatVar should have negative minimum values",
    "every StatVar should have non-empty units",
    "observation dates should be after 2010",
]


def _build_suggestions(ctx: "_DatasetContext | None") -> list[str]:
    """Build a deterministic, data-aware suggestion list from _DatasetContext.

    Falls back to _DEFAULT_SQL_SUGGESTIONS when no context is available.
    All rules are purely conditional — no LLM involved.
    """
    if ctx is None:
        return list(_DEFAULT_SQL_SUGGESTIONS)

    result = []

    # Date-range suggestion: concrete year threshold so the vague-term guard never fires.
    # "recent" is blocked by the guard — always use an explicit year instead.
    # If the context has an actual date (from summary_report.csv or csv_preview_stats),
    # anchor to one year before the earliest observed date so the rule is meaningful for
    # the dataset.  Otherwise use 2010 as a safe, concrete fallback.
    if ctx.date_range and ctx.date_range[0]:
        raw = ctx.date_range[0]
        year = None
        if raw not in ("present", ""):
            import re as _re_date
            m = _re_date.match(r"(\d{4})", str(raw))
            if m:
                year = int(m.group(1))
        threshold = str((year - 1) if year else 2010)
        result.append(f"observation dates should be after {threshold}")

    # Unit presence check: per-row, always executable (Units != '[]').
    # "consistent across StatVars" requires an aggregate query the LLM generates
    # inconsistently — replaced with the per-row equivalent that always works.
    # Unit consistency is already enforced by the built-in check_unit_consistency rule.
    # Guard is > 0 (not > 1) because the check is per-row and valid for any dataset size.
    if ctx.num_statvars > 0:
        result.append("every StatVar should have non-empty units")

    # Mixed-scale note: injected into LLM context via _format_dataset_context already.
    # Not added as a chip — it is advisory text, not a valid rule description, and
    # would fail if submitted directly to the SQL generator.

    # Observation count is always a sensible baseline check.
    if ctx.num_statvars > 0:
        first_concept = ctx.concepts[0] if ctx.concepts else "every StatVar"
        result.append(f"{first_concept} should have at least one observation")
    else:
        result.append("every StatVar should have at least one observation")

    # Negative-value suggestion: appended last to match its position in _DEFAULT_SQL_SUGGESTIONS.
    # _DatasetContext has no min_value field so there is never an explicit contradiction —
    # always include as a safe default.
    result.append("no StatVar should have negative values")

    # De-duplicate while preserving order (ctx may overlap with defaults).
    seen: set[str] = set()
    deduped = []
    for s in result:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


class _SuggestionsRequest(BaseModel):
    dataset: str | None = None
    run_id: str | None = None
    # Optional pre-run stats sampled client-side (first ~50 KB of uploaded CSV).
    # Keys: num_rows (int), min_value (float|None), max_value (float|None),
    #       dates (list[str]), statvar_names (list[str])
    csv_preview_stats: dict | None = None


@app.post("/api/sql-rule-suggestions")
def sql_rule_suggestions(body: _SuggestionsRequest):
    """Return SQL rule suggestion strings.

    Without any fields, returns the static defaults.
    With dataset/run_id, builds context from post-run artifacts (report.json,
    validation_output.json, or summary_report.csv).
    With csv_preview_stats, builds context from pre-run CSV sample — allows
    data-aware suggestions immediately after upload, before validation runs.
    """
    ctx = _compute_dataset_context(body.run_id, body.dataset, body.csv_preview_stats)
    suggestions = _build_suggestions(ctx)
    return {"suggestions": suggestions}


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
    except Exception as exc:
        logger.warning("gcs_session_delete_failed session_id=%s: %s", session_id, exc)


async def _safe_delete_session(session_id: str) -> None:
    """Outer safety wrapper around _delete_gcs_session_bg for asyncio.create_task."""
    try:
        await _delete_gcs_session_bg(session_id)
    except Exception as exc:
        logger.warning("background session delete failed session_id=%s: %s", session_id, exc)


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
    custom_rules: list[dict] | None = None,
    tmcf_gcs_path: str | None = None,
    csv_gcs_paths: str | None = None,
    stat_vars_mcf_gcs_path: str | None = None,
    stat_vars_schema_mcf_gcs_path: str | None = None,
    validation_config: "UploadFile | None" = None,
    validation_config_url: str | None = None,
):
    """Shared implementation for /api/run/custom and /api/run/custom/stream.

    Supports three file-delivery modes (mutually exclusive):
    - Direct upload:  tmcf and csv are UploadFile objects (default).
    - GCS session:    session_id is set; files downloaded from GCS signed-URL session.
    - GCS input paths: tmcf_gcs_path / csv_gcs_paths provided; files downloaded
                      directly from GCS using the service account (no upload needed).
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

    max_bytes = MAX_UPLOAD_BYTES
    _size_display = "100 GB"
    # For GCS input path mode (streaming only): generator + lazy args builder defined below.
    _gcs_setup_gen = None
    _gcs_args_fn = None

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
            asyncio.create_task(_safe_delete_session(session_id))

        elif tmcf_gcs_path or csv_gcs_paths:
            # ── GCS input path mode: download files from caller-supplied gs:// URIs ──
            if tmcf or (csv and any(f.filename for f in csv)):
                raise HTTPException(
                    status_code=400,
                    detail="Cannot mix file uploads and GCS input paths — use one mode only",
                )

            tmcf_gcs_path = (tmcf_gcs_path or "").strip()
            csv_gcs_paths = (csv_gcs_paths or "").strip()
            stat_vars_mcf_gcs_path = (stat_vars_mcf_gcs_path or "").strip() or None
            stat_vars_schema_mcf_gcs_path = (stat_vars_schema_mcf_gcs_path or "").strip() or None

            if not tmcf_gcs_path:
                raise HTTPException(status_code=400, detail="tmcf_gcs_path is required")

            # Split and reject empty entries (e.g. accidental double commas).
            csv_uri_list = [u.strip() for u in csv_gcs_paths.split(",")]
            empty_count = csv_uri_list.count("")
            csv_uri_list = [u for u in csv_uri_list if u]
            if not csv_uri_list:
                raise HTTPException(status_code=400, detail="csv_gcs_paths is required (comma-separated gs:// URIs)")
            if empty_count:
                raise HTTPException(
                    status_code=400,
                    detail=f"csv_gcs_paths contains {empty_count} empty entr{'y' if empty_count == 1 else 'ies'} — check for accidental commas",
                )

            # Validate all URIs up front before any download attempt.
            all_uris: list[str] = [tmcf_gcs_path] + csv_uri_list
            if stat_vars_mcf_gcs_path:
                all_uris.append(stat_vars_mcf_gcs_path)
            if stat_vars_schema_mcf_gcs_path:
                all_uris.append(stat_vars_schema_mcf_gcs_path)
            for uri in all_uris:
                err = _validate_gcs_uri(uri)
                if err:
                    raise HTTPException(status_code=400, detail=err)

            if stream:
                # Streaming: defer downloads into the generator so progress lines are emitted
                # in real-time before the subprocess starts.
                csv_paths: list[Path] = []

                async def _gcs_download_gen():
                    yield json.dumps({"t": "line", "v": "Downloading files from GCS..."}) + "\n"
                    logger.info("gcs_input_download tmcf=%s request_id=%s", tmcf_gcs_path, request_id)
                    yield json.dumps({"t": "line", "v": f"  {Path(tmcf_gcs_path).name}"}) + "\n"
                    await _download_gcs_uri(tmcf_gcs_path, tmcf_path)
                    for _i, _csv_uri in enumerate(csv_uri_list):
                        _raw = Path(_csv_uri).name
                        _safe = re.sub(r"[^A-Za-z0-9._-]", "_", _raw) if _raw else ""
                        _dest = csvs_dir / (_safe if _safe.lower().endswith(".csv") else f"input_{_i:02d}.csv")
                        logger.info("gcs_input_download csv[%d]=%s request_id=%s", _i, _csv_uri, request_id)
                        yield json.dumps({"t": "line", "v": f"  {Path(_csv_uri).name}"}) + "\n"
                        await _download_gcs_uri(_csv_uri, _dest)
                        csv_paths.append(_dest)
                    if stat_vars_mcf_gcs_path:
                        logger.info("gcs_input_download stat_vars_mcf=%s request_id=%s", stat_vars_mcf_gcs_path, request_id)
                        yield json.dumps({"t": "line", "v": f"  {Path(stat_vars_mcf_gcs_path).name} (StatVars MCF)"}) + "\n"
                        await _download_gcs_uri(stat_vars_mcf_gcs_path, stat_vars_mcf_path)
                    if stat_vars_schema_mcf_gcs_path:
                        logger.info("gcs_input_download stat_vars_schema_mcf=%s request_id=%s", stat_vars_schema_mcf_gcs_path, request_id)
                        yield json.dumps({"t": "line", "v": f"  {Path(stat_vars_schema_mcf_gcs_path).name} (StatVars Schema MCF)"}) + "\n"
                        await _download_gcs_uri(stat_vars_schema_mcf_gcs_path, stat_vars_schema_mcf_path)
                    yield json.dumps({"t": "line", "v": "Download complete. Starting validation..."}) + "\n"

                _gcs_setup_gen = _gcs_download_gen
            else:
                # Non-streaming: download synchronously now (no progress feedback needed).
                logger.info("gcs_input_download tmcf=%s request_id=%s", tmcf_gcs_path, request_id)
                await _download_gcs_uri(tmcf_gcs_path, tmcf_path)
                csv_paths = []
                for i, csv_uri in enumerate(csv_uri_list):
                    raw_name = Path(csv_uri).name
                    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", raw_name) if raw_name else ""
                    csv_save_path = csvs_dir / (safe_name if safe_name.lower().endswith(".csv") else f"input_{i:02d}.csv")
                    logger.info("gcs_input_download csv[%d]=%s request_id=%s", i, csv_uri, request_id)
                    await _download_gcs_uri(csv_uri, csv_save_path)
                    csv_paths.append(csv_save_path)
                if stat_vars_mcf_gcs_path:
                    logger.info("gcs_input_download stat_vars_mcf=%s request_id=%s", stat_vars_mcf_gcs_path, request_id)
                    await _download_gcs_uri(stat_vars_mcf_gcs_path, stat_vars_mcf_path)
                if stat_vars_schema_mcf_gcs_path:
                    logger.info("gcs_input_download stat_vars_schema_mcf=%s request_id=%s", stat_vars_schema_mcf_gcs_path, request_id)
                    await _download_gcs_uri(stat_vars_schema_mcf_gcs_path, stat_vars_schema_mcf_path)

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

    # User-supplied config file or URL takes full precedence over rule selection / custom SQL rules.
    config_path = await _resolve_validation_config(validation_config, validation_config_url, run_upload_dir)
    if config_path is None:
        rule_ids = [x.strip() for x in (rules or "").split(",") if x.strip()] if rules else []
        config_path = _create_merged_config("custom", rule_ids, list(custom_rules or []))

    extra_env = _existence_checks_env(existence_checks)
    logger.info(
        "custom run existence_checks=%s request_id=%s",
        extra_env["IMPORT_EXISTENCE_CHECKS"],
        request_id,
    )

    try:
        llm_enabled = _llm_review_enabled(llm_review)
        validated_model = _validated_llm_model(llm_model) if llm_enabled else None
        if not llm_enabled:
            logger.info("LLM review disabled for this run")

        def _build_args() -> list[str]:
            _a = ["bash", str(script), "custom", f"--tmcf={tmcf_path}"]
            for _csv_path in csv_paths:
                _a.append(f"--csv={_csv_path}")
            _a.append(f"--baseline-name={baseline_name}")
            if stat_vars_mcf_path.exists():
                _a.append(f"--stat-vars-mcf={stat_vars_mcf_path}")
            if stat_vars_schema_mcf_path.exists():
                _a.append(f"--stat-vars-schema-mcf={stat_vars_schema_mcf_path}")
            if config_path:
                _a.extend([f"--config={config_path}"])
            if llm_enabled:
                _a.append("--llm-review")
                if validated_model:
                    _a.append(f"--model={validated_model}")
            else:
                _a.append("--no-llm-review")
            return _a

        # For GCS streaming, args are built lazily after downloads complete inside the generator.
        # For all other cases, build args immediately (csv_paths is already populated).
        if _gcs_setup_gen is not None:
            _gcs_args_fn = _build_args

        # request_id was already extracted for the upload dir above; reuse it here.
        output_dir = (OUTPUT_DIR / "custom" / request_id) if request_id else DATASET_OUTPUT_MAP["custom"]
        canonical_output_dir = DATASET_OUTPUT_MAP["custom"]
        return await _run_validation_process(
            [] if _gcs_args_fn is not None else _build_args(),
            request, config_path, stream=stream, app_root=APP_ROOT,
            output_dir=output_dir, dataset="custom", canonical_output_dir=canonical_output_dir,
            # Pass run_upload_dir so the runner cleans it after the subprocess exits.
            # For streaming runs this happens in the generator's finally; for non-streaming
            # runs in the impl's finally. Both paths run after the subprocess has exited.
            extra_cleanup_dirs=[run_upload_dir],
            # baseline_id lets the UI call /api/accept-baseline/custom with the right dataset_id.
            extra_done_fields={"baseline_id": baseline_name},
            extra_env=extra_env,
            setup_gen=_gcs_setup_gen,
            args_fn=_gcs_args_fn,
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
    custom_rules_json: str | None = Form(None),
    tmcf_gcs_path: str | None = Form(None),
    csv_gcs_paths: str | None = Form(None),
    stat_vars_mcf_gcs_path: str | None = Form(None),
    stat_vars_schema_mcf_gcs_path: str | None = Form(None),
    validation_config: UploadFile | None = File(None),
    validation_config_url: str | None = Form(None),
):
    """Run validation with streaming output.

    Supports three file-delivery modes (mutually exclusive):
    - Direct upload:    tmcf and csv as multipart file fields (default).
    - GCS session:      session_id from /api/prepare-upload.
    - GCS input paths:  tmcf_gcs_path + csv_gcs_paths (comma-separated gs:// URIs).

    Optional: validation_config (file) or validation_config_url (https:// or gs:// URI) to
    override the default validation rules. File takes priority over URL. When provided, all
    rule selection and custom SQL rules are ignored for this run.
    """
    custom_rules: list[dict] = []
    if custom_rules_json:
        try:
            parsed = json.loads(custom_rules_json)
            if not isinstance(parsed, list):
                raise HTTPException(status_code=400, detail="custom_rules_json must be a JSON array")
            err = _validate_custom_rules(parsed)
            if err:
                raise HTTPException(status_code=400, detail=err)
            custom_rules = parsed
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="custom_rules_json is not valid JSON")
    return await _run_custom_validation_impl(
        request, tmcf, csv, stat_vars_mcf, stat_vars_schema_mcf, rules, llm_review, llm_model, stream=True,
        dataset_name=dataset_name, existence_checks=existence_checks, session_id=session_id,
        custom_rules=custom_rules,
        tmcf_gcs_path=tmcf_gcs_path, csv_gcs_paths=csv_gcs_paths,
        stat_vars_mcf_gcs_path=stat_vars_mcf_gcs_path,
        stat_vars_schema_mcf_gcs_path=stat_vars_schema_mcf_gcs_path,
        validation_config=validation_config,
        validation_config_url=validation_config_url,
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
    custom_rules_json: str | None = Form(None),
    tmcf_gcs_path: str | None = Form(None),
    csv_gcs_paths: str | None = Form(None),
    stat_vars_mcf_gcs_path: str | None = Form(None),
    stat_vars_schema_mcf_gcs_path: str | None = Form(None),
    validation_config: UploadFile | None = File(None),
    validation_config_url: str | None = Form(None),
):
    """Run validation (non-streaming).

    Supports three file-delivery modes (mutually exclusive):
    - Direct upload:    tmcf and csv as multipart file fields (default).
    - GCS session:      session_id from /api/prepare-upload.
    - GCS input paths:  tmcf_gcs_path + csv_gcs_paths (comma-separated gs:// URIs).

    Optional: validation_config (file) or validation_config_url (https:// or gs:// URI) to
    override the default validation rules. File takes priority over URL.
    """
    custom_rules: list[dict] = []
    if custom_rules_json:
        try:
            parsed = json.loads(custom_rules_json)
            if not isinstance(parsed, list):
                raise HTTPException(status_code=400, detail="custom_rules_json must be a JSON array")
            err = _validate_custom_rules(parsed)
            if err:
                raise HTTPException(status_code=400, detail=err)
            custom_rules = parsed
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="custom_rules_json is not valid JSON")
    return await _run_custom_validation_impl(
        request, tmcf, csv, stat_vars_mcf, stat_vars_schema_mcf, rules, llm_review, llm_model, stream=False,
        dataset_name=dataset_name, existence_checks=existence_checks, session_id=session_id,
        custom_rules=custom_rules,
        tmcf_gcs_path=tmcf_gcs_path, csv_gcs_paths=csv_gcs_paths,
        stat_vars_mcf_gcs_path=stat_vars_mcf_gcs_path,
        stat_vars_schema_mcf_gcs_path=stat_vars_schema_mcf_gcs_path,
        validation_config=validation_config,
        validation_config_url=validation_config_url,
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
    validation_config_url: str | None = Query(None),
):
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    script = SCRIPT_DIR / "run_e2e_test.sh"
    if not script.exists():
        raise HTTPException(status_code=500, detail="run_e2e_test.sh not found")
    # Config override takes full priority — ignore rule selection when set.
    if validation_config_url and validation_config_url.strip():
        run_upload_dir = OUTPUT_DIR / dataset / getattr(request.state, "request_id", "override")
        run_upload_dir.mkdir(parents=True, exist_ok=True)
        config_path = await _resolve_validation_config(None, validation_config_url.strip(), run_upload_dir)
    else:
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
    try:
        raw = gcs_reports.get_report_from_gcs(run_id, dataset, filename)
    except GCSAccessError as exc:
        logger.warning("_resolve_artifact gcs_unavailable dataset=%s filename=%s: %s", dataset, filename, exc)
        return None
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

# Limit concurrent NL → SQL generation calls (each makes a Gemini API request).
# Capacity 2: sized for Pro primary (~5-8s/call). At capacity 3 under Pro latency,
# a queued 4th caller waits up to 24s before the API is even reached.
_GENERATE_SQL_RULE_SEMAPHORE = asyncio.Semaphore(2)

# NL → SQL generation model tier.
# Primary: Gemini Pro for semantic correctness (SQL is stored in persistent configs).
# Fallback: Gemini Flash on quota/availability errors (transient degradation only).
_SQL_PRIMARY_MODEL = "gemini-2.5-pro"
_SQL_FALLBACK_MODEL = "gemini-2.5-flash"
# Per-call timeout for SQL generation (primary + fallback combined).
# Pro p95 ≈ 8s; Flash fallback p95 ≈ 4s; 20s gives headroom without allowing
# indefinite hangs. Applied via asyncio.wait_for — the asyncio Task is cancelled
# and the semaphore released immediately; the underlying thread completes in the
# background when the OS socket eventually closes.
_SQL_LLM_TIMEOUT_SEC = 20.0


def _is_sql_llm_availability_error(exc: Exception) -> bool:
    """True for transient Gemini errors that warrant a Flash fallback.

    Retryable: ResourceExhausted (429), ServiceUnavailable (503),
               DeadlineExceeded, InternalServerError.
    Not retryable: InvalidArgument, PermissionDenied, Unauthenticated —
                   these indicate configuration problems, not transient state.
    """
    try:
        from google.api_core import exceptions as _gax
        return isinstance(exc, (
            _gax.ResourceExhausted,
            _gax.ServiceUnavailable,
            _gax.DeadlineExceeded,
            _gax.InternalServerError,
        ))
    except ImportError:
        msg = str(exc).lower()
        return any(t in msg for t in (
            "resourceexhausted", "serviceunavailable",
            "deadlineexceeded", "internalservererror", "429", "503",
        ))


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


@app.get("/api/config-errors/{dataset}")
def get_config_errors(dataset: str, run_id: str | None = Query(None)):
    """Return CONFIG_ERROR entries from validation_output.json for the given run.

    CONFIG_ERROR is emitted by SQL_VALIDATOR when the query is malformed (syntax
    error, invalid column reference, etc.).  These are surfaced separately in the UI
    so users see a clear error message instead of a silent pass.
    """
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    raw = _resolve_artifact(dataset, run_id, "validation_output.json")
    if raw is None:
        return {"exists": False, "errors": []}
    try:
        results = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {"exists": True, "errors": []}
    if not isinstance(results, list):
        results = []
    errors = [
        {
            "rule_id": r.get("validation_name") or "",
            "message": r.get("message") or "",
        }
        for r in results
        if r.get("status") == "CONFIG_ERROR"
    ]
    return {"exists": True, "errors": errors}


class _GenerateSqlRuleRequest(BaseModel):
    prompt: str
    dataset: str | None = None
    tmcf_schema: str | None = None
    csv_columns: list[str] | None = None
    run_id: str | None = None
    clarification_round: int = 0
    # Optional lightweight stats derived from uploaded CSV (no validation run needed).
    # Keys: num_rows (int), min_value (float|None), max_value (float|None),
    #       dates (list[str]), statvar_names (list[str])
    csv_preview_stats: dict | None = None


# ---------------------------------------------------------------------------
# DatasetContext — precomputed from stats_summary CSV for prompt injection
# ---------------------------------------------------------------------------

@_dataclass
class _DatasetContext:
    num_statvars: int
    concepts: list[str]
    has_mixed_scales: bool
    date_range: tuple[str, str]
    # True when context was derived from a client-side CSV sample (pre-run).
    # False for all post-run sources: summary_report.csv, report.json, validation_output.json.
    from_csv_preview: bool = False


_CONCEPT_KEYWORDS = ["birth", "death", "population", "fertility", "mortality", "gdp"]


def _extract_concepts(names: list[str]) -> list[str]:
    text = " ".join(names).lower()
    return sorted(set(k for k in _CONCEPT_KEYWORDS if k in text))


def _find_stats_summary(run_id: str | None, dataset: str | None) -> Path | None:
    """Return path to the stats/summary CSV if available for the given run/dataset.

    The pipeline writes summary_report.csv; an older convention used stats_summary.csv.
    Both filenames are checked so custom-dataset preview works regardless of which name
    the pipeline produced.  Checked in priority order:
      1. output/<dataset>/<run_id>/summary_report.csv   ← actual pipeline output (UI runs)
      2. output/<dataset>/<run_id>/stats_summary.csv    ← legacy name
      3. output/<dataset>_genmcf/summary_report.csv     ← named datasets, canonical dir
      4. output/<dataset>_genmcf/stats_summary.csv      ← legacy name, canonical dir
    """
    _FILENAMES = ("summary_report.csv", "stats_summary.csv")
    if run_id and dataset:
        base = APP_ROOT / "output" / dataset / run_id
        for name in _FILENAMES:
            p = base / name
            if p.exists():
                return p
    if dataset:
        base = APP_ROOT / "output" / f"{dataset}_genmcf"
        for name in _FILENAMES:
            p = base / name
            if p.exists():
                return p
    return None


def _context_from_artifacts(
    report_json_bytes: bytes | None,
    validation_output_bytes: bytes | None,
) -> "_DatasetContext | None":
    """Build a DatasetContext by combining report.json and validation_output.json.

    report.json  — statsCheckSummary provides the *complete* StatVar list regardless of
                   pass/fail status (each entry is one StatVar × location pair processed
                   by the DC import tool).  Non-empty summary also signals dates exist.
    validation_output.json — adds unit-consistency and date-rule signals, plus StatVar
                   names from failing rows (covers cases where report.json is absent).

    Returns None when neither source yields any useful signal.
    """
    # --- report.json: complete StatVar list from statsCheckSummary ---
    statvars: set[str] = set()
    if report_json_bytes:
        try:
            rj = json.loads(report_json_bytes.decode("utf-8"))
            for item in (rj.get("statsCheckSummary") or []):
                sv = (item.get("statVarDcid") or "").strip()
                if sv:
                    statvars.add(sv)
        except Exception:
            pass

    # --- validation_output.json: unit-consistency, date signals, failing-row StatVars ---
    has_mixed_scales = False
    # statsCheckSummary non-empty → DC tool processed observations → dates exist in data.
    has_dates = bool(statvars)
    vo_statvars: set[str] = set()
    if validation_output_bytes:
        try:
            vo_results = json.loads(validation_output_bytes.decode("utf-8"))
            _DATE_RULES = frozenset({"check_max_date_latest", "check_max_date_consistent"})
            if isinstance(vo_results, list):
                for r in vo_results:
                    if not isinstance(r, dict):
                        continue
                    rule_id = r.get("validation_name") or ""
                    status = r.get("status") or ""
                    details = r.get("details") or {}
                    if rule_id == "check_unit_consistency" and status == "FAILED":
                        has_mixed_scales = True
                    if rule_id in _DATE_RULES:
                        has_dates = True
                    # Collect failing-row StatVars as a fallback when report.json absent.
                    for row_key in ("failed_rows", "failing_rows"):
                        for row in (details.get(row_key) or []):
                            if not isinstance(row, dict):
                                continue
                            sv = (row.get("StatVar") or row.get("stat_var") or "").strip()
                            if sv:
                                vo_statvars.add(sv)
        except Exception:
            pass

    all_statvars = statvars or vo_statvars
    if not all_statvars and not has_dates and not has_mixed_scales:
        return None

    concepts = _extract_concepts(list(all_statvars))
    return _DatasetContext(
        num_statvars=len(all_statvars),
        concepts=concepts,
        has_mixed_scales=has_mixed_scales,
        date_range=("present" if has_dates else "", ""),
    )


def _compute_dataset_context(
    run_id: str | None,
    dataset: str | None,
    csv_preview_stats: dict | None = None,
) -> "_DatasetContext | None":
    """Compute a minimal DatasetContext for SQL rule suggestion generation.

    Resolution order:
    1. summary_report.csv / stats_summary.csv — richest source (local pipeline runs).
    2. report.json + validation_output.json — available on GCS; report.json provides the
       complete StatVar list via statsCheckSummary (works even for clean/all-PASSED runs);
       validation_output.json adds unit-consistency and date signals.
    3. csv_preview_stats — lightweight client-side stats from the raw uploaded CSV.
    Returns None if no source is available.
    """
    csv_path = _find_stats_summary(run_id, dataset)
    if csv_path:
        try:
            import duckdb as _duckdb
            con = _duckdb.connect()
            con.execute(f"CREATE VIEW stats AS SELECT * FROM read_csv_auto('{csv_path}')")
            row = con.execute(
                "SELECT COUNT(*), MIN(MinDate), MAX(MaxDate), MIN(MinValue), MAX(MaxValue) FROM stats"
            ).fetchone()
            if row is None:
                con.close()
                return None
            num, min_date, max_date, min_val, max_val = row
            has_mixed = (max_val or 0) > 1000 and abs(min_val or 0) < 1
            names_rows = con.execute("SELECT StatVar FROM stats").fetchall()
            con.close()
            names = [r[0] for r in names_rows if r[0]]
            concepts = _extract_concepts(names)
            return _DatasetContext(
                num_statvars=int(num or 0),
                concepts=concepts,
                has_mixed_scales=bool(has_mixed),
                date_range=(str(min_date or ""), str(max_date or "")),
            )
        except Exception as _exc:
            logger.debug("dataset_context csv failed run_id=%s dataset=%s: %s", run_id, dataset, _exc)

    # Fallback: GCS/local JSON artifacts.
    # report.json provides the complete StatVar list (statsCheckSummary).
    # validation_output.json adds unit-consistency and date signals.
    # Both are fetched once and merged by _context_from_artifacts.
    if run_id or dataset:
        try:
            raw_report = _resolve_artifact(dataset, run_id, "report.json")
            raw_vo = _resolve_artifact(dataset, run_id, "validation_output.json")
            ctx = _context_from_artifacts(raw_report, raw_vo)
            if ctx is not None:
                return ctx
        except Exception as _exc:
            logger.debug("dataset_context artifacts failed run_id=%s dataset=%s: %s", run_id, dataset, _exc)

    # Fallback: lightweight stats from the client-side CSV preview.
    if csv_preview_stats and isinstance(csv_preview_stats, dict):
        try:
            num = int(csv_preview_stats.get("num_rows") or 0)
            min_val = csv_preview_stats.get("min_value")
            max_val = csv_preview_stats.get("max_value")
            dates = csv_preview_stats.get("dates") or []
            statvar_names = csv_preview_stats.get("statvar_names") or []
            has_mixed = (
                max_val is not None and min_val is not None
                and float(max_val) > 1000 and abs(float(min_val)) < 1
            )
            concepts = _extract_concepts([str(n) for n in statvar_names])
            min_date = str(dates[0]) if dates else ""
            max_date = str(dates[-1]) if dates else ""
            return _DatasetContext(
                num_statvars=num,
                concepts=concepts,
                has_mixed_scales=bool(has_mixed),
                date_range=(min_date, max_date),
                from_csv_preview=True,
            )
        except Exception as _exc:
            logger.debug("csv_preview_stats context failed: %s", _exc)

    return None


def _format_dataset_context(ctx: "_DatasetContext | None") -> str:
    if ctx is None:
        return (
            "## Dataset context\n"
            "Not available — no validation run has been completed yet.\n"
            "If dataset context is NOT available:\n"
            "  - Do NOT assume specific concept names (birth, death, population, etc.) exist in the data.\n"
            "  - Prefer generic rules that apply to any dataset (MinValue, MaxValue, NumObservations, Units).\n"
            "  - If dataset context is not available, ILIKE-based concept matching may be unreliable.\n"
            "    Use it cautiously. Prefer generic rules when possible.\n\n"
        )
    concepts_str = ", ".join(ctx.concepts) if ctx.concepts else "none detected"
    mixed_scales_line = "Values span multiple scales (counts and small ratios mixed).\n" if ctx.has_mixed_scales else ""
    return (
        f"## Dataset context\n"
        f"{ctx.num_statvars} StatVars. "
        f"Date range: {ctx.date_range[0]}–{ctx.date_range[1]}.\n"
        f"Concepts present: {concepts_str}.\n"
        f"{mixed_scales_line}"
        "Use this to improve suggestion specificity. "
        "Prefer these concepts when relevant, but do not assume this list is exhaustive.\n\n"
    )


def _fallback_error(ctx: "_DatasetContext | None") -> dict:
    """Generic error returned when clarification round limit is hit or options are invalid."""
    if ctx and ctx.concepts:
        first = ctx.concepts[0]
        suggestions = [
            f"{first} values should have at least one observation",
            *_DEFAULT_SQL_SUGGESTIONS[1:],
        ]
    else:
        suggestions = list(_DEFAULT_SQL_SUGGESTIONS)
    return {
        "error": "Rule is too vague to generate SQL — no column or threshold can be inferred.",
        "suggestions": suggestions,
    }


_SQL_RULE_COLUMNS = {
    "StatVar", "NumPlaces", "NumObservations", "MinValue", "MaxValue",
    "NumObservationsDates", "MinDate", "MaxDate",
    "MeasurementMethods", "Units", "ScalingFactors", "observationPeriods",
    "ADDED", "DELETED", "MODIFIED",
}
_COL_PATTERN = "(?:" + "|".join(re.escape(c) for c in _SQL_RULE_COLUMNS) + ")"


def _select_columns(query: str) -> set[str] | None:
    """Return the set of identifiers exposed by the SELECT clause.

    Includes known table column names (from _SQL_RULE_COLUMNS) and aliases
    declared with AS.  Returns None when the clause cannot be parsed or when
    SELECT * is used (meaning all columns are available, so the alignment
    check should be skipped).
    """
    m = re.search(r"\bSELECT\s+(.*?)\s+FROM\b", query, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    clause = m.group(1).strip()
    if clause == "*":
        return None  # SELECT * — all columns available

    cols: set[str] = set()
    for item in clause.split(","):
        item = item.strip()
        # Alias: anything after AS is available in the condition.
        alias = re.search(r"\bAS\s+(\w+)\s*$", item, re.IGNORECASE)
        if alias:
            cols.add(alias.group(1))
        # Known table columns appearing in this item (handles bare names and
        # table-qualified names like stats.MinValue).
        for col in _SQL_RULE_COLUMNS:
            if re.search(r"\b" + re.escape(col) + r"\b", item, re.IGNORECASE):
                cols.add(col)
    return cols


def _condition_columns(condition: str) -> set[str]:
    """Return known table column names referenced in the condition expression."""
    return {
        col for col in _SQL_RULE_COLUMNS
        if re.search(r"\b" + re.escape(col) + r"\b", condition, re.IGNORECASE)
    }


def _explain_sql_condition(condition: str) -> str | None:
    """Return a short plain-English explanation for conditions that are counter-intuitive.

    Focuses on MinValue / MaxValue semantics, which are the most common source of
    confusion: users expect to check "the value" but the table exposes the min/max
    of the distribution.  Returns None when no targeted explanation applies.
    """
    c = condition.strip()

    if re.search(r"\bMinValue\s*>\s*0\b", c, re.IGNORECASE):
        return "Ensures every StatVar has strictly positive values."
    if re.search(r"\bMinValue\s*>=\s*0\b", c, re.IGNORECASE):
        return "Ensures no StatVar contains negative values."
    if re.search(r"\bMaxValue\s*<\s*0\b", c, re.IGNORECASE):
        return "Ensures every StatVar has exclusively negative values."
    if re.search(r"\bMaxValue\s*<=\s*0\b", c, re.IGNORECASE):
        return "Ensures no StatVar contains positive values."
    if re.search(r"\bMinValue\s*<=\s*MaxValue\b", c, re.IGNORECASE):
        return "Ensures minimum and maximum values are internally consistent."
    if re.search(r"\bNumObservations\s*>=\s*1\b", c, re.IGNORECASE):
        return "Ensures every StatVar has at least one observation."
    if re.search(r"\bUnits\s*!=\s*'\[\]'", c, re.IGNORECASE):
        return "Ensures every StatVar has a unit specified."

    m = re.search(r"\bMaxValue\s*<=\s*(\d+(?:\.\d+)?)\b", c, re.IGNORECASE)
    if m:
        return f"Ensures no StatVar has maximum values above {m.group(1)}."
    m = re.search(r"\bMinValue\s*>=\s*(\d+(?:\.\d+)?)\b", c, re.IGNORECASE)
    if m:
        return f"Ensures every StatVar has values of at least {m.group(1)}."
    m = re.search(r"\bMinDate\s*>=\s*'(\d{4})", c, re.IGNORECASE)
    if m:
        return f"Ensures all observations are from {m.group(1)} or later."
    m = re.search(r"\bMaxDate\s*>=\s*'(\d{4})", c, re.IGNORECASE)
    if m:
        return f"Ensures all data has observations up to {m.group(1)} or later."

    return None


def _post_validate_sql_rule(description: str, query: str, condition: str) -> str | None:
    """Deterministic post-generation checks on LLM-produced SQL rule.

    Returns a human-readable error string on failure, None on success.
    Checks (in order):
      1. Trivially-true rules (no duplicate StatVars, etc.)
      2. MinValue vs MaxValue semantic mistake for "all/only negative"
      3. MaxValue vs MinValue semantic mistake for "all/only positive"
      4. Condition direction: upper-bound language with > operator
      5. Condition direction: lower-bound language with < operator
      6. Upper-bound phrasing using MinValue instead of MaxValue
      7. Lower-bound phrasing using MaxValue instead of MinValue
      8. Column alignment: condition references only columns in SELECT list
    """
    desc_lower = description.lower()

    # 1. Trivial rules that are always true by schema construction.
    if re.search(r"\bduplicat\w*\b", desc_lower) and re.search(r"\bstatvars?\b", desc_lower):
        return (
            "Unsupported rule: the stats table has exactly one row per StatVar by "
            "construction, so a duplicate-StatVar check is trivially true. Please "
            "describe a meaningful data quality constraint instead."
        )

    # 2. Semantic: "only/all negative" must check MaxValue < 0, not MinValue < 0.
    #    MinValue < 0 only guarantees the minimum is negative; MaxValue could still be
    #    positive (mixed positive/negative values).  MaxValue < 0 ensures every value
    #    in the distribution is negative.
    if re.search(
        r"\b(only|all|must be|should be|are)\s+(negative|below\s+zero|less\s+than\s+zero)\b",
        desc_lower,
    ):
        has_minval = re.search(r"\bMinValue\s*<\s*0\b", condition, re.IGNORECASE)
        has_maxval = re.search(r"\bMaxValue\s*<\s*0\b", condition, re.IGNORECASE)
        if has_minval and not has_maxval:
            return (
                "Semantic error: for 'all/only values are negative', use MaxValue < 0 "
                "(not MinValue < 0). MinValue < 0 only confirms the minimum is negative "
                "while MaxValue could still be positive. MaxValue < 0 guarantees every "
                "value in the distribution is negative."
            )

    # 3. Semantic: "only/all positive" must check MinValue > 0, not MaxValue > 0.
    #    MaxValue > 0 only guarantees the maximum is positive; MinValue could still be
    #    negative.  MinValue > 0 ensures every value in the distribution is positive.
    if re.search(
        r"\b(only|all|must be|should be|are)\s+(positive|above\s+zero|greater\s+than\s+zero)\b",
        desc_lower,
    ):
        has_maxval = re.search(r"\bMaxValue\s*>\s*0\b", condition, re.IGNORECASE)
        has_minval = re.search(r"\bMinValue\s*>\s*0\b", condition, re.IGNORECASE)
        if has_maxval and not has_minval:
            return (
                "Semantic error: for 'all/only values are positive', use MinValue > 0 "
                "(not MaxValue > 0). MaxValue > 0 only confirms the maximum is positive "
                "while MinValue could still be negative. MinValue > 0 guarantees every "
                "value in the distribution is positive."
            )

    # 4. Upper-bound language in description but > operator in condition (direction inversion).
    #    Only checked for direct column references (row-level rules), not aggregate aliases.
    _upper_bound = (
        r"\b(not\s+exceed|at\s+most|no\s+more\s+than"
        r"|must\s+not\s+be\s+(more|greater)"
        r"|should\s+not\s+(be\s+more|be\s+greater|exceed))\b"
    )
    _col_gt = _COL_PATTERN + r"\s*>\s*[\d'\"]"
    if re.search(_upper_bound, desc_lower) and re.search(_col_gt, condition, re.IGNORECASE):
        return (
            "Condition direction error: description implies an upper bound (≤) but "
            "condition uses > on a column. The condition is a PASS predicate — "
            "'should not exceed 100' → condition: MaxValue <= 100."
        )

    # 4. Lower-bound language in description but < operator in condition (direction inversion).
    _lower_bound = (
        r"\b(at\s+least|no\s+less\s+than"
        r"|must\s+not\s+be\s+(less|below)"
        r"|should\s+not\s+(be\s+less|be\s+below)"
        r"|minimum\s+of)\b"
    )
    _col_lt = _COL_PATTERN + r"\s*<\s*[\d'\"]"
    if re.search(_lower_bound, desc_lower) and re.search(_col_lt, condition, re.IGNORECASE):
        return (
            "Condition direction error: description implies a lower bound (≥) but "
            "condition uses < on a column. The condition is a PASS predicate — "
            "'at least 1' → condition: NumObservations >= 1."
        )

    # 6. Upper-bound phrasing but condition uses MinValue instead of MaxValue.
    #    MinValue <= X does not prevent values from exceeding X — only MaxValue <= X does.
    if re.search(r"\b(not\s+exceed|at\s+most|no\s+more\s+than)\b", desc_lower):
        if re.search(r"\bMinValue\s*<=\s*\d+(?:\.\d+)?", condition, re.IGNORECASE):
            return (
                "Semantic error: upper bound constraints must use MaxValue, not MinValue. "
                "MinValue <= X does not prevent values from exceeding X — it only checks "
                "the minimum observed value. Use MaxValue <= X to enforce an upper bound."
            )

    # 7. Lower-bound phrasing but condition uses MaxValue instead of MinValue.
    #    MaxValue >= X does not guarantee all values meet the lower bound.
    if re.search(r"\b(at\s+least|no\s+less\s+than)\b", desc_lower):
        if re.search(r"\bMaxValue\s*>=\s*\d+(?:\.\d+)?", condition, re.IGNORECASE):
            return (
                "Semantic error: lower bound constraints must use MinValue, not MaxValue. "
                "MaxValue >= X only confirms the maximum meets the bound — it does not "
                "guarantee all values do. Use MinValue >= X to enforce a lower bound."
            )

    # 8. Column alignment: every known table column in the condition must appear
    #    in the SELECT list.  Aggregate aliases (e.g. "n") are not in
    #    _SQL_RULE_COLUMNS so they are not checked here; the DuckDB EXPLAIN
    #    pre-check catches unknown identifiers at the SQL level.
    sel_cols = _select_columns(query)
    if sel_cols is not None:  # None → SELECT * or unparseable → skip
        cond_cols = _condition_columns(condition)
        missing = cond_cols - sel_cols
        if missing:
            missing_list = ", ".join(sorted(missing))
            return (
                f"Column alignment error: condition references {missing_list} "
                f"but {'that column is' if len(missing) == 1 else 'those columns are'} "
                f"not in the SELECT list. Add the missing column(s) to the query."
            )

    return None


@app.post("/api/generate-sql-rule")
async def generate_sql_rule(body: _GenerateSqlRuleRequest):
    """Generate a SQL validation rule (query + condition) from a natural language description.

    Requires GEMINI_API_KEY or GOOGLE_API_KEY.  Calls Gemini, parses the JSON response,
    runs the existing DuckDB EXPLAIN pre-check, and returns {query, condition, rule_id}.
    """
    api_key = _get_gemini_api_key()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="LLM not configured — set GEMINI_API_KEY or GOOGLE_API_KEY",
        )
    prompt_text = (body.prompt or "").strip()
    if not prompt_text:
        raise HTTPException(status_code=400, detail="prompt is required")
    if len(prompt_text) > 2000:
        raise HTTPException(status_code=400, detail="prompt too long (max 2000 characters)")

    _VAGUE_TERMS = {
        "good": "please specify what makes a value good (e.g. greater than 0, within a known range)",
        "recent": "please specify a date (e.g. after 2020)",
        "valid": "please specify what makes a value valid (e.g. between 0 and 100)",
        "correct": "please specify what correct means (e.g. matches a known set of values)",
        "reasonable": "please specify a numeric range (e.g. between 0 and 1000)",
        "large": "please specify a threshold (e.g. greater than 1000)",
        "small": "please specify a threshold (e.g. less than 10)",
    }
    import re as _re_vague
    _HAS_NUMERIC_SIGNAL = bool(
        _re_vague.search(r"\d", prompt_text)
        or _re_vague.search(
            r"\b(after|before|above|below|greater than|less than|at least|at most)\b|\bbetween\s+[-+]?\d",
            prompt_text, _re_vague.IGNORECASE,
        )
        or _re_vague.search(r"[<>]=?", prompt_text)
    )
    # Short phrases that express a clear binary/negation constraint need no numeric signal.
    # e.g. "no negative values", "no empty units", "non-empty units"
    _HAS_CLEAR_NEGATION = bool(_re_vague.search(
        r"\bno\s+\w+|non[- ]empty|not\s+(empty|null|blank)|must\s+(exist|have|be\s+non)",
        prompt_text, _re_vague.IGNORECASE,
    ))
    _word_count = len(prompt_text.split())
    if not _HAS_NUMERIC_SIGNAL and not _HAS_CLEAR_NEGATION:
        if _word_count < 4:
            raise HTTPException(
                status_code=400,
                detail="Description is too vague — please be more specific (e.g. 'values must be greater than 0', 'dates after 2020').",
            )
        for term, suggestion in _VAGUE_TERMS.items():
            if _re_vague.search(rf"\b{term}\b", prompt_text, _re_vague.IGNORECASE):
                raise HTTPException(
                    status_code=400,
                    detail=f"The term '{term}' is ambiguous — {suggestion}.",
                )

    try:
        from google import genai
    except ImportError:
        raise HTTPException(status_code=503, detail="google-genai package not installed")

    columns_hint = ""
    if body.csv_columns:
        safe_cols = ", ".join(body.csv_columns[:50])  # cap to avoid prompt bloat
        columns_hint = f"\nActual columns in dataset: {safe_cols}"

    current_year = datetime.date.today().year

    system_prompt = (
        "You are generating SQL validation rules for a Data Commons validation system.\n\n"
        "## Execution model\n"
        "Your output is executed as:\n"
        "  WITH _data AS ({query}) SELECT * FROM _data WHERE NOT ({condition})\n"
        "Rows where condition is FALSE are returned → those rows are FAILURES.\n"
        "condition must be TRUE for a row to PASS.\n\n"
        "## Condition direction (CRITICAL — read carefully)\n"
        "The condition expresses what SHOULD BE TRUE, not what is wrong.\n\n"
        "Natural language → correct condition:\n"
        "  'should not exceed 100'          → condition: MaxValue <= 100\n"
        "  'should be at most 100'          → condition: MaxValue <= 100\n"
        "  'should be at least 0'           → condition: MinValue >= 0\n"
        "  'must be greater than 0'         → condition: NumObservations > 0\n"
        "  'must be less than X'            → condition: col < X\n"
        "  'MinValue should be <= MaxValue' → condition: MinValue <= MaxValue\n"
        "  'should not be empty'            → condition: Units != '[]'\n\n"
        "WRONG: 'percent values should not exceed 100' → condition: MaxValue > 100\n"
        "  ↑ This would mark rows where MaxValue > 100 as FAILING, but the NOT wrapper\n"
        "    means rows where MaxValue <= 100 would ALSO be checked — this is inverted.\n"
        "RIGHT: condition: MaxValue <= 100\n\n"
        "WRONG: 'MinValue should be <= MaxValue' → condition: MinValue > MaxValue\n"
        "RIGHT: condition: MinValue <= MaxValue\n\n"
        "Mnemonic: condition = the property that MUST HOLD. Violating it = failure.\n\n"
        "## Available tables\n\n"
        "Table `stats` — one row per StatVar (from summary_report.csv):\n"
        "  StatVar              TEXT\n"
        "  NumPlaces            INTEGER\n"
        "  NumObservations      INTEGER\n"
        "  MinValue             DOUBLE\n"
        "  MaxValue             DOUBLE\n"
        "  NumObservationsDates INTEGER\n"
        "  MinDate              TEXT    ← 'YYYY-MM' string, e.g. '2020-01'\n"
        "  MaxDate              TEXT    ← 'YYYY-MM' string, e.g. '2024-06'\n"
        "  MeasurementMethods   TEXT    ← serialized Python list, e.g. '[]' or '[dcs:Mean]'\n"
        "  Units                TEXT    ← serialized Python list, e.g. '[]' or '[MilePerHour]'\n"
        "  ScalingFactors       TEXT    ← serialized Python list, e.g. '[]' or '[100]'\n"
        "  observationPeriods   TEXT    ← serialized Python list, e.g. '[P1Y, P1M]'\n\n"
        "Table `differ` — one row per StatVar, may be empty (from obs_diff_summary.csv):\n"
        "  StatVar TEXT, ADDED INTEGER, DELETED INTEGER, MODIFIED INTEGER\n\n"
        "DO NOT reference any column not listed above. DO NOT reference any other table.\n\n"
        "## Units / ScalingFactors / MeasurementMethods semantics (CRITICAL)\n"
        "These columns store the ENTIRE Python list as a single text string.\n"
        "  Empty:      '[]'\n"
        "  One item:   '[MilePerHour]'\n"
        "  Two items:  '[MilePerHour, Kilometer]'\n\n"
        "Rules for querying these columns:\n"
        "  Empty check:     condition: Units != '[]'\n"
        "  Non-null check:  condition: Units IS NOT NULL   (they are never NULL in practice)\n"
        "  Single item:     no comma in string → condition: Units NOT LIKE '%,%'\n\n"
        "NEVER filter by content you cannot verify (e.g. do NOT assume '%' or 'Percent' is\n"
        "inside Units — you do not know which unit strings are present in the dataset).\n"
        "  WRONG: WHERE REGEXP_MATCHES(Units, '\"%%\"')   ← assumes '%' exists\n"
        "  WRONG: WHERE Units LIKE '%Percent%'           ← assumes 'Percent' exists\n"
        "  RIGHT: WHERE Units != '[]'                    ← safe: check non-empty only\n\n"
        "## Date columns (CRITICAL)\n"
        "MinDate and MaxDate may be TEXT ('YYYY', 'YYYY-MM', 'YYYY-MM-DD') or TIMESTAMP\n"
        "depending on the dataset. Write queries that are safe for both.\n\n"
        "Preferred: range-based comparisons (safe for TEXT and TIMESTAMP).\n"
        "Prefer full ISO date strings 'YYYY-MM-DD' as boundaries to ensure compatibility\n"
        "with TIMESTAMP types — bare 'YYYY' and 'YYYY-MM' are not valid TIMESTAMPs in DuckDB\n"
        "and will fail when MaxDate is a TIMESTAMP.\n\n"
        "Match the boundary format to the granularity implied by the rule description:\n\n"
        "  Year-level rule  → 'YYYY-01-01' boundaries (default when ambiguous):\n"
        "    MaxDate >= '2023-01-01' AND MaxDate < '2024-01-01'   ← data must be in 2023\n"
        "    MaxDate >= '2020-01-01' AND MaxDate < '2025-01-01'   ← data must fall in 2020–2024\n\n"
        "  Month-level rule → first-of-month boundaries:\n"
        "    MaxDate >= '2023-06-01' AND MaxDate < '2023-07-01'   ← data must be in June 2023\n\n"
        "Do NOT use bare 'YYYY' or 'YYYY-MM' as comparison values.\n"
        "Do NOT mix granularities in the same condition.\n\n"
        "Fallback if a string pattern is needed:\n"
        "  CAST(MaxDate AS VARCHAR) LIKE '2023%'\n\n"
        "NEVER use:\n"
        "  EXTRACT(YEAR FROM MaxDate)      ← fails if MaxDate is TEXT\n"
        "  SUBSTRING(MaxDate, 1, 4)        ← fragile if MaxDate is not a fixed-format string\n\n"
        "## Relative time references\n"
        f"The current year is {current_year}. Compute boundaries from this value — do NOT hardcode years.\n\n"
        "Supported phrases — year-based only:\n"
        "  'last N years' / 'past N years' / 'within the past N years'\n"
        "  'not older than N years' / 'no older than N years'\n"
        f"  → subtract N from {current_year} to get the boundary year, then use 'YYYY-01-01'.\n\n"
        "Examples:\n"
        f"  N=1  → MinDate >= '{current_year - 1}-01-01'\n"
        f"  N=3  → MinDate >= '{current_year - 3}-01-01'\n"
        f"  N=5  → MinDate >= '{current_year - 5}-01-01'\n"
        f"  N=10 → MinDate >= '{current_year - 10}-01-01'\n\n"
        "For month-based phrases (e.g. 'last 6 months', 'last 12 months'):\n"
        "  → Convert to the nearest whole year (round up) and use year-level boundaries.\n"
        f"  'last 6 months'  → MinDate >= '{current_year - 1}-01-01'\n"
        f"  'last 12 months' → MinDate >= '{current_year - 1}-01-01'\n"
        f"  'last 18 months' → MinDate >= '{current_year - 2}-01-01'\n\n"
        "Always use full ISO date format 'YYYY-MM-DD' for all boundaries.\n\n"
        "## StatVar scope inference\n\n"
        "Most rules apply to ALL StatVars. Default to no WHERE on StatVar unless the\n"
        "description clearly implies a semantic subset identifiable by naming pattern.\n\n"
        "When a subset is implied, add a WHERE clause using ILIKE (case-insensitive).\n"
        "Use multiple patterns for the same concept — StatVar names vary across datasets.\n\n"
        "Common patterns (singular and plural both apply):\n"
        "  'percent/percentage/percentages' → WHERE StatVar ILIKE '%Percent%' OR StatVar ILIKE '%Pct%'\n"
        "  'rate/rates'                     → WHERE StatVar ILIKE '%Rate%'\n"
        "  'ratio/ratios'                   → WHERE StatVar ILIKE '%Ratio%' OR StatVar ILIKE '%Fraction%' OR StatVar ILIKE '%Share%'\n"
        "  'count/counts'                   → WHERE StatVar ILIKE '%Count%'\n"
        "  'index/indices/indexes'          → WHERE StatVar ILIKE '%Index%'\n"
        "  'median'                         → WHERE StatVar ILIKE '%Median%'\n"
        "  'mean/average/avg'               → WHERE StatVar ILIKE '%Mean%' OR StatVar ILIKE '%Average%'\n\n"
        "Rules:\n"
        "  1. NEVER use = 'exact_name' — you do not know the exact StatVar names.\n"
        "  2. NEVER use case-sensitive LIKE for StatVar names — always ILIKE.\n"
        "  3. When in doubt, apply globally (no WHERE on StatVar).\n"
        "  4. Do NOT add a StatVar scope filter just because the description mentions\n"
        "     a unit (e.g. 'percent') in a value context ('values should not exceed 100'\n"
        "     without specifying which StatVars) — in that case apply globally.\n\n"
        "Scoped example:\n"
        "  'percent StatVars should not exceed 100'\n"
        "  → query: SELECT StatVar, MaxValue FROM stats\n"
        "            WHERE Units = '[Percent]'\n"
        "               OR Units LIKE '%Percent%'\n"
        "               OR StatVar ILIKE '%Percent%' OR StatVar ILIKE '%Pct%'\n"
        "  → condition: MaxValue <= 100\n\n"
        "Global example (description is general — no StatVar filter):\n"
        "  'all maximum values should not exceed 100'\n"
        "  → query: SELECT StatVar, MaxValue FROM stats\n"
        "  → condition: MaxValue <= 100\n\n"
        "## Row-level vs aggregate\n\n"
        "ROW-LEVEL (default): condition checked per StatVar row.\n"
        "  'minimum values must be non-negative'\n"
        "  → query: SELECT StatVar, MinValue FROM stats\n"
        "  → condition: MinValue >= 0\n\n"
        "  'units should not be empty'\n"
        "  → query: SELECT StatVar, Units FROM stats\n"
        "  → condition: Units != '[]'\n\n"
        "  'MinValue should always be <= MaxValue'\n"
        "  → query: SELECT StatVar, MinValue, MaxValue FROM stats\n"
        "  → condition: MinValue <= MaxValue\n\n"
        "AGGREGATE: use when description implies a global count.\n"
        "Trigger words: 'zero', 'no rows', 'count', 'any', 'at least N', 'at most N', 'total'.\n"
        "Aggregate functions MUST be aliased in the query; reference by alias in condition.\n"
        "  'there should be zero StatVars with negative minimum values'\n"
        "  → query: SELECT COUNT(*) AS n FROM stats WHERE MinValue < 0\n"
        "  → condition: n = 0\n\n"
        "## Schema limitations (CRITICAL)\n"
        "The stats table has exactly ONE ROW PER StatVar, aggregated across all CSV rows.\n\n"
        "Cross-StatVar comparisons are NOT possible:\n"
        "  There is no join key to compare values between different StatVars\n"
        "  (e.g. you cannot check 'Births > Deaths' — they are separate rows with no link).\n"
        "  If a rule requires comparing two StatVars, return {\"error\": ...} explaining the\n"
        "  limitation, or fall back to a single-StatVar approximation if clearly reasonable.\n\n"
        "observationPeriod is NOT available in the stats table:\n"
        "  Monthly vs yearly frequency is NOT encoded in StatVar names and cannot be inferred\n"
        "  from any available column. Do NOT generate rules that:\n"
        "    - Filter by observationPeriod (column does not exist in stats)\n"
        "    - Infer 'monthly' or 'yearly' from StatVar name patterns\n"
        "    - Compare values between monthly and yearly StatVars\n"
        "  If such a rule is requested, return {\"error\": ...}.\n\n"
        "Preferred single-StatVar rule patterns (always safe):\n"
        "  MinValue >= 0\n"
        "  MaxValue <= threshold\n"
        "  MinDate >= 'YYYY-MM-DD'\n"
        "  NumObservations >= 1\n\n"
        "## Rules that must NOT be generated\n"
        "These are always trivially true due to schema guarantees — do not generate them:\n"
        "  - 'no duplicate StatVars': stats has exactly one row per StatVar by construction.\n"
        "  - Any rule whose condition is always true regardless of data (e.g. condition: 1 = 1).\n\n"
        "If asked for such a rule, return the closest meaningful non-trivial rule instead.\n\n"
        "## Natural language → StatVar mapping\n"
        "When a description names a demographic or statistical concept, map it to ILIKE patterns.\n"
        "Common mappings (use OR to cover naming variants):\n"
        "  'births'        → StatVar ILIKE '%Birth%'\n"
        "  'deaths'        → StatVar ILIKE '%Death%'\n"
        "  'infant deaths' → StatVar ILIKE '%Death%Upto1Year%' OR StatVar ILIKE '%Death%Infant%'\n"
        "  'population'    → StatVar ILIKE '%Population%'\n"
        "  'fertility'     → StatVar ILIKE '%Fertility%'\n"
        "  'mortality'     → StatVar ILIKE '%Mortality%' OR StatVar ILIKE '%Death%'\n"
        "  'migration'     → StatVar ILIKE '%Migration%'\n"
        "Always use ILIKE (case-insensitive). Never use exact name matches.\n\n"
        "## StatVar group inference\n"
        "A dataset typically contains StatVars of MULTIPLE types simultaneously (e.g. counts +\n"
        "rates + ratios). Do NOT classify the whole dataset as a single type.\n"
        "Instead, infer which group the relevant StatVars belong to and apply rules per group.\n\n"
        "Primary signal: the rule description text.\n"
        "Secondary (weak) hint: csv_columns or StatVar name patterns.\n\n"
        "Groups and detection patterns (primary: rule description text; secondary: StatVar names):\n"
        "  PERCENT      — description mentions 'percent', 'percentage', '%'\n"
        "                 Filter priority (to avoid zero-row silent passes):\n"
        "                   1st: Units = '[Percent]'         ← exact match; Units stores '[Percent]'\n"
        "                   2nd: Units LIKE '%Percent%'      ← fallback for multi-item or variant formats\n"
        "                   3rd: StatVar ILIKE '%Percent%' OR StatVar ILIKE '%Pct%'  ← naming varies\n"
        "                 Default: combine all with OR so neither naming convention is missed:\n"
        "                   WHERE Units = '[Percent]'\n"
        "                      OR Units LIKE '%Percent%'\n"
        "                      OR StatVar ILIKE '%Percent%' OR StatVar ILIKE '%Pct%'\n"
        "                 → values are bounded 0–100\n"
        "  RATIO        — description mentions 'ratio', 'fraction', 'share'\n"
        "                 StatVar names contain Ratio, Fraction, Share (secondary)\n"
        "                 → values are bounded 0–1\n"
        "  RATE         — description mentions 'rate'\n"
        "                 StatVar names contain Rate (secondary)\n"
        "                 → ambiguous; do NOT assume any bounds (e.g. fertility rate and\n"
        "                   growth rate are not bounded 0–100). Apply constraints only if\n"
        "                   the rule description explicitly specifies a range or threshold.\n"
        "  COUNT        — description mentions 'count', 'number', 'total', or named entities\n"
        "                 (births, deaths, population, etc.)\n"
        "                 StatVar names contain Count, Number, Total, Birth, Death, Population (secondary)\n"
        "                 → non-negative integers; scale varies widely across StatVars\n"
        "  INDEX        — description mentions 'index', 'score'\n"
        "                 StatVar names contain Index, Score (secondary)\n"
        "                 → bounded but range varies by index type; no universal upper bound\n"
        "  DISTRIBUTION — PRIMARY signal: description mentions 'distribution', 'breakdown',\n"
        "                 'sum to 100', 'adds up to', 'exhaustive categories'\n"
        "                 Secondary: StatVar names share a common stem with varying suffixes\n"
        "                 → individual values are 0–100 shares; sum-to-100 NOT enforceable\n"
        "                   (one row per StatVar, no grouping key). See handling below.\n\n"
        "## Decision order (apply in sequence)\n"
        "When evaluating a rule, follow this order:\n\n"
        "  1. SCOPED — description names a specific StatVar subset (births, infant deaths, etc.)\n"
        "     → Apply ILIKE filter; evaluate threshold only for that group.\n"
        "     → Do NOT apply globally.\n\n"
        "  2. MIXED-SCALE — reject ONLY when ALL three conditions hold:\n"
        "     a) The rule specifies a numeric threshold, AND\n"
        "     b) The threshold is not universally safe (i.e. not MinValue >= 0 or\n"
        "        NumObservations >= 1), AND\n"
        "     c) The rule is global (no StatVar scope) AND multiple semantic groups\n"
        "        are present (e.g. counts, percentages, and rates in the same dataset)\n"
        "     Do NOT rely on inferred value magnitudes — use group membership as the signal.\n"
        "     → Reject with {\"error\"} and suggestions. Threshold is ambiguous.\n\n"
        "  3. TYPE-SPECIFIC — rule clearly targets a single known group\n"
        "     → Apply group-specific bounds:\n"
        "       PERCENT:      MaxValue <= 100, MinValue >= 0\n"
        "       RATIO:        MaxValue <= 1,   MinValue >= 0\n"
        "       RATE:         no assumed bounds — apply only if description explicitly states range\n"
        "       COUNT:        MinValue >= 0   (no upper bound — scale varies per StatVar)\n"
        "       INDEX:        no universal bound — apply only if description specifies range\n"
        "       DISTRIBUTION: explain schema limitation, suggest per-StatVar bounds (see below)\n\n"
        "  4. UNIVERSALLY SAFE — rule uses a constraint valid regardless of StatVar type or scale\n"
        "     → MinValue >= 0, NumObservations >= 1, Units != '[]'\n"
        "     → Apply globally with NO StatVar filter. Do NOT add scoping — it can only\n"
        "       narrow matches and cause silent false passes if StatVar names differ.\n"
        "     NOTE: MaxValue <= 100 is NOT universally safe — it applies only to PERCENT group.\n\n"
        "## Silent false pass prevention\n"
        "A rule that matches zero rows always PASSES silently — even if data violates it.\n"
        "This happens when WHERE clauses are too narrow (e.g. StatVar name pattern that no\n"
        "row matches). To prevent this:\n"
        "  - Prefer Units-based filters over StatVar name patterns for type detection.\n"
        "  - When using StatVar name patterns, combine multiple variants with OR.\n"
        "  - For percent-type rules, always use the combined filter:\n"
        "      WHERE Units = '[Percent]' OR Units LIKE '%Percent%'\n"
        "         OR StatVar ILIKE '%Percent%' OR StatVar ILIKE '%Pct%'\n"
        "  - If a threshold is universally valid (e.g. MinValue >= 0), apply globally —\n"
        "    adding a WHERE clause introduces zero-row risk with no benefit.\n\n"
        "## Distribution dataset handling\n"
        "If a rule implies sum-to-100 validation (e.g. 'age groups should add up to 100%'):\n"
        "  The stats table cannot support this — it has one row per StatVar with no group key.\n"
        "  Do NOT return a bare {\"error\"} — instead explain the limitation and suggest\n"
        "  alternative validations that ARE expressible:\n"
        "  {\"error\": \"Sum-to-100 validation is not possible in the current schema: each\n"
        "    StatVar is stored as a single aggregated row with no entity-level grouping.\n"
        "    Alternative rules you can apply instead:\\n\"\n"
        "    \"  - Each share StatVar is between 0 and 100: MaxValue <= 100 AND MinValue >= 0\\n\"\n"
        "    \"  - No share StatVar is negative: MinValue >= 0\\n\"\n"
        "    \"  - All share StatVars have observations: NumObservations >= 1\",\n"
        "   \"suggestions\": [\n"
        "     \"each share value should be between 0 and 100\",\n"
        "     \"no share value should be negative\"\n"
        "   ]}\n\n"
        "## Numeric threshold assessment\n"
        "When a rule specifies a numeric threshold, follow the decision order above first.\n"
        "If reaching step 2 (mixed-scale), the threshold is ambiguous — reject with:\n"
        "  {\"error\": \"This rule applies a numeric threshold across StatVars with very\n"
        "    different scales. Identify which StatVar group the rule targets and what\n"
        "    approximate range those StatVars have, then explain why the threshold does\n"
        "    not apply uniformly. Suggest 1–2 scoped alternatives.\",\n"
        "   \"suggestions\": [\"<scoped version 1>\", \"<scoped version 2>\"]}\n\n"
        "  Example:\n"
        "    User: 'minimum value should be less than 500'\n"
        "    → {\"error\": \"This rule applies a numeric threshold across StatVars with very\n"
        "        different scales:\\n\"\n"
        "        \"  - Birth counts (~300K–3.6M)\\n\"\n"
        "        \"  - Death counts (~240K–3.2M)\\n\"\n"
        "        \"  - Infant death counts (~1K–20K)\\n\\n\"\n"
        "        \"A threshold of 500 is not meaningful across all of them.\",\n"
        "       \"suggestions\": [\n"
        "         \"minimum infant deaths should be less than 500\",\n"
        "         \"minimum birth count should be less than 500000\"\n"
        "       ]}\n\n"
        "  UNIVERSALLY SAFE thresholds (always allowed globally, any StatVar type):\n"
        "    MinValue >= 0, NumObservations >= 1\n"
        "  TYPE-SPECIFIC thresholds (only for the named group):\n"
        "    PERCENT → MaxValue <= 100\n"
        "    RATIO   → MaxValue <= 1\n\n"
        "## Condition correctness rules\n"
        "1. Condition = PASS test. It must be the positive form of the constraint.\n"
        "2. Minimal: use the tightest single condition. No redundant AND clauses.\n"
        "   WRONG: MinValue >= 0 AND MaxValue >= 0\n"
        "   RIGHT: MinValue >= 0  (already the tightest lower bound)\n"
        "3. Condition must reference ONLY columns in the SELECT list.\n"
        "4. Aggregate functions forbidden in condition — alias in query, reference by alias.\n\n"
        "## SQL constraints\n"
        "- Only SELECT. No INSERT, UPDATE, DELETE, DDL. No semicolons.\n"
        "- Valid DuckDB SQL.\n"
        "- Regex: REGEXP_MATCHES(col, 'pattern') or col ~ 'pattern'.\n"
        "  Do NOT use REGEXP_FULL_MATCH, REGEXP_LIKE, RLIKE.\n\n"
        "## When to return {\"error\"}\n"
        "Return {\"error\": \"...\"} in these cases — do NOT generate misleading SQL.\n"
        "ALWAYS include \"suggestions\": at least 2–3 concrete prompts the user can try next.\n"
        "Keep the error message to 1–2 lines. Prioritise actionable output over explanation.\n\n"
        "## How to write context-aware suggestions (IMPORTANT)\n"
        "Suggestions are directly usable rule descriptions — concrete, dataset-ready prompts\n"
        "that the user can submit immediately to generate valid SQL.\n"
        "They are NOT intent summaries. Write them as complete, actionable rule statements.\n"
        "Example: \"every StatVar should have at least one observation\" (not \"Check observations\")\n\n"
        "Suggestions must reflect the user's original intent — not generic fallbacks.\n\n"
        "Rules (apply all):\n"
        "  1. Extract named concepts from the description (e.g. 'Female', 'Births', 'GDP', 'Total').\n"
        "     Use those terms directly in suggestion text — do NOT use placeholder strings.\n"
        "     NEVER output literal text like '<named concept>' or '<first named concept>'.\n"
        "  2. If no meaningful concept can be extracted, use general but specific phrasing.\n"
        "     Do NOT fabricate or guess concept names — use what the user wrote.\n"
        "  3. Do NOT invent StatVar identifiers or DCIDs. Use descriptive phrases:\n"
        "     WRONG: 'Count_Person_Female should have observations'\n"
        "     RIGHT: 'female population values should have observations'\n"
        "  4. Each suggestion must be distinct — do not repeat the same idea in different words.\n"
        "  5. Vary suggestion types across: value checks (non-negative, bounded),\n"
        "     completeness (has observations, units not empty), structural (consistent place count).\n"
        "     Cover key concepts from the input, but prioritise quality and diversity over rigid coverage.\n"
        "  6. Suggestions must be plain JSON strings — no comments, no placeholders, no markup.\n"
        "  7. Every suggestion MUST be expressible as a valid single-StatVar SQL rule.\n"
        "     Do NOT suggest rules that compare different StatVars to each other\n"
        "     (e.g. 'births should equal deaths', 'A should be greater than B').\n"
        "     Do NOT suggest rules that require joins or alignment across StatVars.\n"
        "     If a constraint cannot be expressed within a single StatVar row, it must NOT be suggested.\n"
        "     A suggestion that would itself produce a CROSS_STATVAR or structural error is invalid\n"
        "     and must not be included.\n"
        "     This rule overrides all ordering rules. If a suggestion violates this constraint,\n"
        "     it must be excluded even if it would otherwise be first.\n\n"
        "Ordering (apply strictly in this sequence):\n"
        "  a) STRUCTURAL first — but ONLY if it can be expressed as a valid single-StatVar rule.\n"
        "     If the original intent is a cross-StatVar comparison (A vs B, A > B, A matches B):\n"
        "       - DO NOT attempt to approximate the relationship directly.\n"
        "       - Instead, fall back to single-StatVar rules derived from each concept independently.\n"
        "     A structural suggestion that references alignment, co-presence, or comparability\n"
        "     between two different StatVars (e.g. 'birth and death values should exist for the\n"
        "     same places and dates') is itself a cross-StatVar rule and MUST NOT be suggested.\n"
        "     Example:\n"
        "       Input: 'births should be greater than deaths'\n"
        "       INVALID: 'birth and death values should exist for the same places and dates'\n"
        "       VALID:   'birth count values should have at least one observation'\n"
        "                'death count values should be non-negative'\n"
        "     For non-comparison rules, structural suggestions (e.g. 'all dates should be\n"
        "     consistent across StatVars') are fine — they do not compare StatVar rows.\n"
        "  b) COMPLETENESS second — use one of the extracted concept terms, not both.\n"
        "     Do NOT output two near-identical completeness suggestions\n"
        "     (e.g. 'birth count has observations' AND 'death count has observations' — pick one,\n"
        "     or vary by concept: one for births, one for a different attribute like units or dates).\n"
        "  c) VALUE CHECK last — non-negative, bounded.\n"
        "     'non-negative' is a last-resort fallback. Do NOT use it as suggestion 1 or 2\n"
        "     when structural or completeness alternatives exist for the named concepts.\n\n"
        "  Example — input: 'Births should be greater than Deaths'\n"
        "  BAD:  [\n"
        "    \"birth and death values should exist for the same places and dates\",\n"
        "    \"birth count values should be non-negative\",\n"
        "    \"death count values should be non-negative\"\n"
        "  ]\n"
        "  (First entry is cross-StatVar — invalid even as a structural suggestion.)\n"
        "  GOOD: [\n"
        "    \"birth count values should have at least one observation\",\n"
        "    \"death count values should be non-negative\",\n"
        "    \"birth count values should have non-empty units\"\n"
        "  ]\n\n"
        "  Example — input: 'Female + Male should equal Total'\n"
        "  BAD:  [\"all related StatVars should have at least one observation\",\n"
        "         \"<first named concept> values should be non-negative\"]\n"
        "  GOOD: [\n"
        "    \"female, male, and total population values should exist for the same places and dates\",\n"
        "    \"female population values should have at least one observation\",\n"
        "    \"total population values should be non-negative\"\n"
        "  ]\n\n"
        "  Example — input: 'GDP and population growth rate should move together'\n"
        "  GOOD: [\n"
        "    \"GDP and population growth rate values should be measured for the same time periods\",\n"
        "    \"GDP values should have at least one observation\",\n"
        "    \"population growth rate values should be non-negative\"\n"
        "  ]\n\n"
        "  Example — input: 'data should be consistent' (no extractable concept)\n"
        "  Pick 2–3 from the pool below; vary — do NOT output the same list each time:\n"
        "    structural:   \"observation dates should be consistent across StatVars\"\n"
        "                  \"the number of places covered should be consistent across StatVars\"\n"
        "    completeness: \"every StatVar should have at least one observation\"\n"
        "                  \"every StatVar should have non-empty units\"\n"
        "    value check:  \"no StatVar should have negative values\"\n"
        "                  \"observation dates should be recent\"\n\n"
        "If any reasonable single-StatVar interpretation exists, generate SQL instead of erroring:\n"
        "  'values should not be negative'     → MinValue >= 0\n"
        "  'dates should be recent'            → MaxDate >= '2020-01-01'\n"
        "  'there should be observations'      → NumObservations >= 1\n\n"
        "  1. CROSS-STATVAR — rule compares or combines values from different StatVars:\n"
        "       'Births should be greater than Deaths'\n"
        "       'Female + Male should equal Total'\n"
        "     → Extract named concepts (e.g. Female, Male, Total, Births, Deaths).\n"
        "     → First suggestion: structural alignment adapted to context (spatial+temporal, temporal\n"
        "       only, or general comparability — whichever the rule implies). Do not hardcode\n"
        "       'places and dates' unless both dimensions are clearly relevant.\n"
        "     → Then one completeness check (pick the most relevant concept — do not repeat for each).\n"
        "     → Value check last, only if no better option.\n"
        "     → The stats table has one row per StatVar — no join key exists between rows.\n"
        "     → {\"error\": \"Cross-StatVar comparison is not possible: each StatVar is a separate row with no join key.\",\n"
        "        \"suggestions\": [\n"
        "          \"birth count values should have at least one observation\",\n"
        "          \"death count values should be non-negative\",\n"
        "          \"birth count values should have non-empty units\"\n"
        "        ]}\n\n"
        "  2. OBSERVATION PERIOD — rule depends on observationPeriod or temporal frequency:\n"
        "       'monthly values should be less than yearly values'\n"
        "       'observations with P1M should have more rows'\n"
        "     → First suggestion: structural date consistency. Then completeness. Then recency.\n"
        "     → {\"error\": \"observationPeriod is a serialized string and cannot be queried as structured data.\",\n"
        "        \"suggestions\": [\n"
        "          \"observation dates should be consistent across StatVars\",\n"
        "          \"every StatVar should have at least one observation\",\n"
        "          \"observation dates should be recent\"\n"
        "        ]}\n\n"
        "  3. DUPLICATE / TRIVIALLY TRUE — rule checks something the schema guarantees:\n"
        "       'check for duplicate rows', 'no StatVar should appear twice'\n"
        "     → The stats table has exactly one row per StatVar by construction.\n"
        "     → First suggestion: uniqueness within observations (not at StatVar level — already guaranteed).\n"
        "     → Then completeness. Then value check.\n"
        "     → {\"error\": \"Duplicate detection is not meaningful: the stats table has exactly one row per StatVar.\",\n"
        "        \"suggestions\": [\n"
        "          \"each StatVar should have a unique set of observations by place and date\",\n"
        "          \"every StatVar should have at least one observation\",\n"
        "          \"no StatVar should have negative values\"\n"
        "        ]}\n\n"
        "  4. MIXED SCALE — numeric threshold globally across StatVars with different scales:\n"
        "       'min value should be less than 20000'  ← ambiguous across all StatVars\n"
        "     → Already handled in ## Numeric threshold assessment (with suggestions).\n\n"
        "When returning {\"error\"}, include ONLY \"error\" and \"suggestions\" — no \"query\" or \"condition\".\n\n"
        "## Clarification output\n"
        "Clarification MUST be used for vague or underspecified inputs where multiple valid\n"
        "interpretations exist. Do NOT return {\"error\"} for vague inputs — use {\"clarify\"} instead.\n\n"
        "DO NOT emit {\"clarify\": ...} for structural errors. These ALWAYS use {\"error\": ..., \"suggestions\": [...]}:\n"
        "  CROSS_STATVAR, OBSERVATION_PERIOD, MIXED_SCALE, DUPLICATE / TRIVIALLY TRUE\n\n"
        "PURPOSE DISTINCTION — clarification options vs suggestions:\n"
        "  Options (in {\"clarify\"}): intent-oriented. Help the user choose an interpretation.\n"
        "    Slightly higher-level — describe what the rule is checking, not the exact SQL constraint.\n"
        "    Example: \"Values should be non-negative\" (intent label, not a full rule description)\n"
        "  Suggestions (in {\"error\"}): action-oriented. Directly usable rule descriptions.\n"
        "    Concrete and dataset-ready — the user can submit them as-is.\n"
        "    Example: \"every StatVar should have at least one observation\"\n"
        "  Options and suggestions MUST NOT be identical lists. They may cover similar ground\n"
        "  but must differ in style: options are interpretations, suggestions are prompts.\n\n"
        "Shape:\n"
        "  {\"clarify\": \"What should this rule check?\", \"options\": [{\"label\": \"...\", \"refined\": \"...\"}, ...]}\n\n"
        "Option rules:\n"
        "  1. Always generate 3–4 options. Each must represent a distinct intent direction —\n"
        "     not the same rule rephrased.\n"
        "  2. Each \"refined\" MUST be a complete NL prompt that produces valid SQL when re-submitted.\n"
        "     Do NOT produce refined prompts that would trigger clarification, cross-StatVar errors,\n"
        "     or mixed-scale errors.\n"
        "     Before emitting an option, verify:\n"
        "       a) The \"refined\" text maps to a supported column: MinValue, MaxValue,\n"
        "          NumObservations, Units (non-empty check only), or MaxDate.\n"
        "       b) The rule does NOT require parsing serialized list contents, counting list\n"
        "          elements, or referencing non-existent columns.\n"
        "       c) The rule does NOT trigger CROSS_STATVAR, MIXED_SCALE, or unsupported logic.\n"
        "       d) The label and refined text are semantically aligned.\n"
        "     If any check fails, discard the option and generate another.\n"
        "     INVALID examples (discard these):\n"
        "       \"each StatVar has only one unit\"  ← requires parsing list contents\n"
        "       \"observation date recorded\"        ← column does not exist\n"
        "  3. If dataset context lists concepts:\n"
        "     - The first option MUST use a detected concept.\n"
        "     - At least one additional option SHOULD also use a detected concept when relevant.\n"
        "     - Do not force concept usage into every option — generic options are fine for the rest.\n"
        "  4. Labels are intent-oriented: describe what the rule is checking.\n"
        "     Write labels as short intent phrases, NOT full rule statements.\n"
        "     WRONG label: \"every StatVar should have at least one observation\" (too literal)\n"
        "     RIGHT label: \"Check that all StatVars have at least one observation\"\n"
        "  5. Labels MUST preserve the exact semantic meaning of the refined rule.\n"
        "     Do NOT drop key constraints such as \"at least one\", \"non-empty\", or \"across StatVars\".\n"
        "     Do NOT oversimplify or introduce concepts not present in the schema.\n"
        "     WRONG label: \"Check that all StatVars have observations\"  ← drops \"at least one\"\n"
        "     RIGHT label: \"Check that all StatVars have at least one observation\"\n"
        "     WRONG label: \"Check that dates are consistent\"            ← drops \"across StatVars\"\n"
        "     RIGHT label: \"Check that observation dates are consistent across StatVars\"\n"
        "     WRONG label: \"Check that units are valid\"                 ← introduces vague concept\n"
        "     RIGHT label: \"Check that every StatVar has non-empty units\"\n"
        "  6. Controlled variation: do NOT always return the same fixed set of options.\n"
        "     Ensure at least one option varies across generations by rotating coverage.\n"
        "     Valid variation pool — each entry maps to a supported column and valid SQL:\n"
        "       - Date recency:     MaxDate → observation dates should be recent\n"
        "       - Date consistency: MaxDate → observation dates should be consistent across StatVars\n"
        "       - Completeness:     NumObservations → every StatVar should have at least one observation\n"
        "       - Units non-empty:  Units → every StatVar should have non-empty units\n"
        "       - Value lower bound: MinValue → no StatVar should have negative values\n"
        "       - Value upper bound: MaxValue → percent StatVars should not exceed 100\n"
        "     Choose the most relevant 3–4 dimensions for the input. Do NOT always pick the\n"
        "     same combination — vary which dimensions are included each time.\n\n"
        "Example — input: \"make sure data is fine\", concepts: births, deaths\n"
        "  {\"clarify\": \"What should this rule check?\",\n"
        "   \"options\": [\n"
        "     {\"label\": \"Check that birth values are non-negative\",\n"
        "      \"refined\": \"birth count values should be non-negative\"},\n"
        "     {\"label\": \"Check that all StatVars have at least one observation\",\n"
        "      \"refined\": \"every StatVar should have at least one observation\"},\n"
        "     {\"label\": \"Check that observation dates are recent\",\n"
        "      \"refined\": \"observation dates should be recent\"},\n"
        "     {\"label\": \"Check that death values have non-empty units\",\n"
        "      \"refined\": \"death count values should have non-empty units\"}\n"
        "   ]}\n\n"
        "Example — input: \"validate\", no concepts available\n"
        "  {\"clarify\": \"What should this rule check?\",\n"
        "   \"options\": [\n"
        "     {\"label\": \"Check that no StatVar has negative values\",\n"
        "      \"refined\": \"no StatVar should have negative values\"},\n"
        "     {\"label\": \"Check that all StatVars have at least one observation\",\n"
        "      \"refined\": \"every StatVar should have at least one observation\"},\n"
        "     {\"label\": \"Check that observation dates are consistent across StatVars\",\n"
        "      \"refined\": \"observation dates should be consistent across StatVars\"},\n"
        "     {\"label\": \"Check that every StatVar has non-empty units\",\n"
        "      \"refined\": \"every StatVar should have non-empty units\"}\n"
        "   ]}\n\n"
        "## Vague term detection (check BEFORE generating SQL)\n"
        "If the input contains terms that are inherently ambiguous and cannot be mapped to a column\n"
        "or threshold without guessing, return a clarification or error — do NOT silently infer.\n\n"
        "Ambiguous terms that require clarification:\n"
        "  - 'recent', 'up to date', 'current' — date threshold unknown; ask for a specific year/date.\n"
        "  - 'good', 'valid', 'correct', 'ok', 'fine', 'proper' — no measurable column implied.\n"
        "  - 'reasonable', 'sensible', 'appropriate' — threshold undefined.\n"
        "  - 'large', 'small', 'high', 'low' without a number — relative without a reference point.\n\n"
        "When a vague term is the primary constraint (not just a modifier on an explicit value):\n"
        "  - Use {\"clarify\"} to ask for the missing specifics.\n"
        "  - Include 3–4 options that show what 'specific' looks like.\n"
        "  Example: input 'dates should be recent'\n"
        "    → {\"clarify\": \"What cutoff date should 'recent' mean?\", \"options\": [\n"
        "         {\"label\": \"After 2020\", \"refined\": \"observation dates should be after 2020\"},\n"
        "         {\"label\": \"After 2022\", \"refined\": \"observation dates should be after 2022\"},\n"
        "         {\"label\": \"Within the last 5 years\", \"refined\": \"observation dates should be after 2019\"},\n"
        "         {\"label\": \"Check date consistency instead\", \"refined\": \"observation dates should be consistent across StatVars\"}\n"
        "       ]}\n\n"
        "## Explanation rules\n"
        "Every SQL response MUST include \"explanation\": one sentence in plain English.\n"
        "Write for a non-technical audience. Rules:\n"
        "  - Open with \"Ensures\".\n"
        "  - No SQL syntax, no internal column names (MinValue, MaxDate, etc.), no 'passes when'.\n"
        "  - Describe the data-quality constraint, not the implementation.\n"
        "  - If a threshold or scope was inferred, append a brief parenthetical note.\n\n"
        "Examples (study the phrasing — no column names, no SQL):\n"
        "  MinValue >= 0          → \"Ensures no StatVar contains negative values.\"\n"
        "  MaxValue <= 100        → \"Ensures no StatVar has maximum values above 100.\"\n"
        "  NumObservations >= 1   → \"Ensures every StatVar has at least one observation.\"\n"
        "  Units != '[]'          → \"Ensures every StatVar has a unit specified.\"\n"
        "  MinDate >= '2010-01-01' → \"Ensures all observations are from 2010 or later.\"\n"
        "  MinValue <= MaxValue   → \"Ensures minimum and maximum values are internally consistent.\"\n"
        "  MaxDate >= '2020-01-01' (inferred) → \"Ensures all data is recent (observations after 2020).\"\n"
        "  MaxValue <= 100 WHERE Units LIKE '%Percent%' → \"Ensures percent-type StatVars do not exceed 100.\"\n\n"
        "## Output format (STRICT)\n"
        "Output ONLY a JSON object. Exactly one of these three shapes:\n"
        "  SQL:           {\"query\": \"...\", \"condition\": \"...\", \"explanation\": \"...\"}\n"
        "  Error:         {\"error\": \"...\", \"suggestions\": [\"...\", ...]}\n"
        "  Clarification: {\"clarify\": \"...\", \"options\": [{\"label\": \"...\", \"refined\": \"...\"}, ...]}\n\n"
        "Rules:\n"
        "  - SQL responses MUST include \"explanation\". Never omit it.\n"
        "  - Error responses MUST include \"suggestions\". Never return a bare {\"error\": \"...\"}.\n"
        "  - Clarification is for vague/underspecified inputs. Structural errors (CROSS_STATVAR,\n"
        "    OBSERVATION_PERIOD, MIXED_SCALE, DUPLICATE) ALWAYS use the Error shape.\n"
        "  - Never mix keys across shapes. No markdown, no explanation outside the JSON, no trailing text.\n\n"
        "## Self-verification (do this before generating output)\n"
        "Before emitting your response, work through these checks in order:\n"
        "  1. Is the rule scoped to a named StatVar subset?\n"
        "     Yes → apply ILIKE filter; skip steps 2–3.\n"
        "  2. Does the rule apply a numeric threshold globally across mixed-scale StatVars?\n"
        "     Yes → return {\"error\"} with scale explanation and suggestions.\n"
        "  3. Is the target StatVar group identifiable (percent/ratio/count/distribution)?\n"
        "     Yes → apply group-specific bounds; reject if threshold violates them.\n"
        "     Distribution → explain schema limitation and suggest per-StatVar alternatives.\n"
        "  4. Is the condition a PASS test (not failure logic)?\n"
        "     'should not exceed 100' → MaxValue <= 100  (not MaxValue > 100).\n"
        "  5. Does the input contain a vague term ('recent', 'valid', 'good', 'correct', 'reasonable')?\n"
        "     Yes → return {\"clarify\"} with specific options. Do NOT silently infer a threshold.\n"
        "  6. Is the input vague or underspecified (no clear column, threshold, or constraint)?\n"
        "     Yes → return {\"clarify\"} with 3–4 options. Do NOT return {\"error\"} for vague inputs.\n"
        "  7. Does the condition reference only columns in the SELECT list?\n"
        "  8. Is there any redundant filter or clause that can be removed?\n"
        "  9. Is the rule cross-StatVar, trivially true, or uses observationPeriod? → {\"error\"} + suggestions.\n"
        "     Every {\"error\"} response MUST include \"suggestions\" with 1–3 usable prompt alternatives.\n"
        "  10. Does the SQL response include \"explanation\" that names the column, condition, and any assumption?\n\n"
        "Worked examples (study the condition direction in each):\n"
        "{\"query\": \"SELECT StatVar, MinValue FROM stats\", \"condition\": \"MinValue >= 0\"}\n"
        "{\"query\": \"SELECT StatVar, MaxValue FROM stats\", \"condition\": \"MaxValue <= 100\"}\n"
        "{\"query\": \"SELECT StatVar, MinValue, MaxValue FROM stats\", \"condition\": \"MinValue <= MaxValue\"}\n"
        "{\"query\": \"SELECT StatVar, Units FROM stats\", \"condition\": \"Units != '[]'\"}\n"
        "{\"query\": \"SELECT StatVar, NumObservations FROM stats\", \"condition\": \"NumObservations >= 1\"}\n"
        "{\"query\": \"SELECT StatVar, MaxDate FROM stats\", \"condition\": \"MaxDate >= '2020-01-01' AND MaxDate < '2030-01-01'\"}\n"
        "{\"query\": \"SELECT COUNT(*) AS n FROM stats WHERE MinValue < 0\", \"condition\": \"n = 0\"}\n"
        "{\"query\": \"SELECT StatVar, MaxValue FROM stats WHERE Units = '[Percent]' OR Units LIKE '%Percent%' OR StatVar ILIKE '%Percent%' OR StatVar ILIKE '%Pct%'\", \"condition\": \"MaxValue <= 100\"}"
    )
    ctx = _compute_dataset_context(body.run_id, body.dataset, body.csv_preview_stats)
    context_section = _format_dataset_context(ctx)

    user_prompt = f"Rule description: {prompt_text}{columns_hint}"
    full_prompt = system_prompt + "\n\n" + context_section + "\n" + user_prompt

    # Build generation config: temperature=0 for deterministic, reproducible SQL.
    # SQL is stored in persistent validation configs and must not vary across calls.
    try:
        from google.genai import types as _genai_types
        _sql_gen_config = _genai_types.GenerateContentConfig(temperature=0, max_output_tokens=1024)
    except (ImportError, AttributeError) as _cfg_exc:
        logger.warning(
            "generate_sql_rule: GenerateContentConfig unavailable (%s) — "
            "temperature=0 not applied; SQL output will be non-deterministic.",
            _cfg_exc,
        )
        _sql_gen_config = None

    def _call_sql_llm(prompt: str) -> str:
        """Call Pro primary; fall back to Flash on quota/availability errors."""
        _client = genai.Client(api_key=api_key)

        def _generate(model_id: str) -> str:
            kw: dict = {"model": model_id, "contents": prompt}
            if _sql_gen_config is not None:
                kw["config"] = _sql_gen_config
            resp = _client.models.generate_content(**kw)
            extracted = resp.text if resp.text else ""
            if not extracted and getattr(resp, "candidates", None):
                for c in resp.candidates:
                    if getattr(c, "content", None) and getattr(c.content, "parts", None):
                        for p in c.content.parts:
                            if getattr(p, "text", None):
                                extracted = p.text.strip()
                                break
                    if extracted:
                        break
            return extracted

        try:
            return _generate(_SQL_PRIMARY_MODEL)
        except Exception as _exc:
            if not _is_sql_llm_availability_error(_exc):
                raise
            logger.warning(
                "[gemini] feature=sql_rule_generation model_used=%s fallback=true reason=%s",
                _SQL_FALLBACK_MODEL, _exc,
            )
            return _generate(_SQL_FALLBACK_MODEL)

    try:
        async with _GENERATE_SQL_RULE_SEMAPHORE:
            text = await asyncio.wait_for(
                asyncio.to_thread(_call_sql_llm, full_prompt),
                timeout=_SQL_LLM_TIMEOUT_SEC,
            )
        text = text.strip()
    except asyncio.TimeoutError:
        logger.warning(
            "[gemini] feature=sql_rule_generation timeout=true timeout_sec=%.1f",
            _SQL_LLM_TIMEOUT_SEC,
        )
        raise HTTPException(status_code=503, detail="Rule generation timed out — try again.")
    except Exception as exc:
        if _is_sql_llm_availability_error(exc):
            logger.warning(
                "[gemini] feature=sql_rule_generation both_models_failed=true error=%s", exc,
            )
            raise HTTPException(status_code=503, detail="LLM service temporarily unavailable — try again.")
        logger.warning("generate_sql_rule llm_error: %s", exc)
        raise HTTPException(status_code=502, detail=f"LLM request failed: {exc}")

    # Strip markdown code fences if the model added them despite instructions.
    if text.startswith("```"):
        lines = [ln for ln in text.splitlines() if not ln.startswith("```")]
        text = "\n".join(lines).strip()

    # Three-tier JSON recovery:
    #   1. Direct parse (fast path).
    #   2. Bracket extraction — strips leading/trailing prose the model occasionally adds.
    #   3. Single LLM retry with a nudge prompt (covers rare markdown-wrapped responses).
    result: dict | None = None
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        _start = text.find("{")
        _end = text.rfind("}")
        if _start != -1 and _end > _start:
            try:
                result = json.loads(text[_start : _end + 1])
            except json.JSONDecodeError:
                pass

    if result is None:
        logger.warning("generate_sql_rule invalid_json attempt=1 response=%s", text[:300])
        try:
            # Retry with the original full prompt so the model has its constraints
            # and dataset context intact. The correction is appended at the end so
            # it takes precedence without losing any prior grounding.
            _nudge = (
                full_prompt
                + "\n\n[CORRECTION REQUIRED] Your previous response was not valid JSON. "
                "Return ONLY the JSON object — no markdown, no prose, no code fences. "
                "Output must start with '{' and end with '}'."
            )
            async with _GENERATE_SQL_RULE_SEMAPHORE:
                _retry_text = await asyncio.wait_for(
                    asyncio.to_thread(_call_sql_llm, _nudge),
                    timeout=_SQL_LLM_TIMEOUT_SEC,
                )
            _retry_text = _retry_text.strip()
            if _retry_text.startswith("```"):
                _retry_text = "\n".join(
                    ln for ln in _retry_text.splitlines() if not ln.startswith("```")
                ).strip()
            result = json.loads(_retry_text)
            logger.info("generate_sql_rule invalid_json recovered via retry")
        except asyncio.TimeoutError:
            logger.warning(
                "[gemini] feature=sql_rule_generation_retry timeout=true timeout_sec=%.1f",
                _SQL_LLM_TIMEOUT_SEC,
            )
            raise HTTPException(status_code=503, detail="Rule generation timed out — try again.")
        except Exception as _retry_exc:
            logger.warning("generate_sql_rule invalid_json attempt=2: %s", _retry_exc)
            raise HTTPException(
                status_code=400,
                detail="LLM returned invalid JSON after retry. Try rephrasing your description.",
            )

    # Route by response shape — return HTTP 200 for all structured responses.
    if "clarify" in result and "query" not in result and "error" not in result:
        if body.clarification_round >= 1:
            return _fallback_error(ctx)
        options = result.get("options")
        if not isinstance(options, list):
            return _fallback_error(ctx)
        valid_opts = [
            o for o in options
            if isinstance(o, dict) and o.get("label") and o.get("refined")
        ]
        if not valid_opts:
            return _fallback_error(ctx)
        return {
            "clarify": result.get("clarify", "What should this rule check?"),
            "options": valid_opts[:4],
        }

    if "error" in result:
        suggestions = result.get("suggestions")
        if not isinstance(suggestions, list) or not suggestions:
            suggestions = list(_DEFAULT_SQL_SUGGESTIONS)
        return {"error": result["error"], "suggestions": suggestions}

    query = (result.get("query") or "").strip().rstrip(";")
    condition = (result.get("condition") or "").strip()
    if not query or not condition:
        raise HTTPException(
            status_code=400,
            detail="LLM response is missing 'query' or 'condition'",
        )

    # Guard: catch bare aggregate calls in the condition (e.g. "COUNT(*) = 0").
    # The condition is evaluated in a WHERE clause, so aggregate functions there
    # are invalid — columns must be named in the SELECT list of the query instead.
    if re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', condition, re.IGNORECASE):
        raise HTTPException(
            status_code=400,
            detail=(
                "Generated condition uses an aggregate function directly "
                "(e.g. COUNT(*) = 0). Aggregates must be aliased in the query "
                "and referenced by alias in the condition "
                "(e.g. query: 'SELECT COUNT(*) AS n FROM stats', condition: 'n = 0'). "
                "Try rephrasing your description."
            ),
        )

    # Post-generation semantic/direction validator.
    desc = getattr(body, "description", None) or getattr(body, "prompt", None) or ""
    post_err = _post_validate_sql_rule(desc, query, condition)
    if post_err:
        raise HTTPException(status_code=400, detail=post_err)

    # Reuse existing DuckDB EXPLAIN pre-check (validates syntax + column names
    # against real-schema dummy tables; no dataset loaded at this point).
    sql_err = _validate_custom_rules([{
        "rule_id": "tmp",
        "params": {"query": query, "condition": condition},
    }])
    if sql_err:
        raise HTTPException(
            status_code=400,
            detail=f"Generated SQL is invalid — try rephrasing your description. Detail: {sql_err}",
        )

    rule_id = "custom_sql_" + secrets.token_hex(4)
    explanation = result.get("explanation") or _explain_sql_condition(condition)
    logger.info(
        "generate_sql_rule rule_id=%s prompt_len=%d",
        rule_id, len(prompt_text),
    )
    return {
        "query": query,
        "condition": condition,
        "has_dataset_context": ctx is not None,
        "context_source": (
            "csv_preview" if (ctx is not None and ctx.from_csv_preview)
            else ("run" if ctx is not None else "none")
        ),
        "rule_id": rule_id,
        "explanation": explanation,
    }


class _PreviewSqlRuleRequest(BaseModel):
    query: str
    condition: str
    dataset: str | None = None
    run_id: str | None = None


@app.post("/api/preview-sql-rule")
async def preview_sql_rule(body: _PreviewSqlRuleRequest):
    """Run a SQL rule against the actual stats CSV for the dataset/run and return pass/fail + violations."""
    query = (body.query or "").strip().rstrip(";")
    condition = (body.condition or "").strip()
    if not query or not condition:
        raise HTTPException(status_code=400, detail="query and condition are required")

    csv_path = _find_stats_summary(body.run_id, body.dataset)
    if csv_path is None:
        raise HTTPException(
            status_code=404,
            detail="No stats data found — run a validation first to enable rule preview.",
        )

    try:
        import duckdb as _duckdb
        con = _duckdb.connect()
        con.execute(f"CREATE VIEW stats AS SELECT * FROM read_csv_auto('{csv_path}')")

        # Validate condition against query output by attempting a LIMIT 1 execution.
        validate_sql = (
            f"WITH _data AS ({query}) "
            f"SELECT * FROM _data WHERE NOT ({condition}) LIMIT 1"
        )
        try:
            con.execute(validate_sql)
        except Exception:
            con.close()
            return {"error": "Invalid SQL condition or column reference."}

        # Execute the user query as a CTE, then filter for violations (rows where condition is NOT met).
        violations_sql = (
            f"WITH _data AS ({query}) "
            f"SELECT * FROM _data WHERE NOT ({condition}) LIMIT 10"
        )
        try:
            cur = con.execute(violations_sql)
            violation_rows = cur.fetchall()
            col_names = [d[0] for d in cur.description]

            # Count total violations (without LIMIT).
            count_sql = (
                f"WITH _data AS ({query}) "
                f"SELECT COUNT(*) FROM _data WHERE NOT ({condition})"
            )
            total_violations = con.execute(count_sql).fetchone()[0]
        except Exception:
            con.close()
            return {"error": "SQL execution failed. Please verify the query and condition."}
        con.close()
    except Exception:
        return {"error": "SQL execution failed. Please verify the query and condition."}

    sample_rows = [dict(zip(col_names, row)) for row in violation_rows]
    return {
        "passed": total_violations == 0,
        "violations": int(total_violations),
        "sample_rows": sample_rows,
    }


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


@app.get("/report/{dataset}/{run_id}/report.json")
def serve_report_json_by_run_id(dataset: str, run_id: str):
    """Serve report.json (DC import tool lint output) as a downloadable artifact."""
    if dataset not in DATASET_OUTPUT_MAP:
        raise HTTPException(status_code=404, detail="Unknown dataset")
    if not _run_id_safe(run_id):
        raise HTTPException(status_code=400, detail="Invalid run_id")
    content = _resolve_artifact(dataset, run_id, "report.json")
    if content is None:
        raise HTTPException(status_code=404, detail="report.json not found for this run.")
    return Response(
        content=content,
        media_type="application/json",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Content-Disposition": f'attachment; filename="report_{dataset}_{run_id}.json"',
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

    # Locate MCF files.  Search order:
    #   1. Per-run local dir (local runs, or Batch if KEEP_RUN_DIR=1).
    #   2. Canonical local output dir (local runs after cleanup).
    #   3. GCS (Batch runs: MCF files are uploaded there; VM is gone by accept time).
    genmcf_dir: Path | None = None
    _mcf_tmp_dir: str | None = None  # temp dir created for GCS fallback; cleaned up below

    if run_id:
        candidate = OUTPUT_DIR / dataset / run_id
        if candidate.is_dir() and list(candidate.glob("*.mcf")):
            genmcf_dir = candidate
    if genmcf_dir is None:
        canonical = DATASET_OUTPUT_MAP[dataset]
        if canonical.is_dir() and list(canonical.glob("*.mcf")):
            genmcf_dir = canonical
    if genmcf_dir is None and run_id and is_gcs_configured():
        # Batch path: MCF files were uploaded to GCS by upload_reports_to_gcs()
        # but the Batch VM's local filesystem is gone.  Download to a temp dir.
        tmp = tempfile.mkdtemp(prefix="baseline_mcf_")
        _mcf_tmp_dir = tmp
        try:
            count = gcs_reports.download_mcf_files_from_gcs(run_id, dataset, Path(tmp))
        except GCSAccessError as exc:
            logger.warning("accept_baseline gcs_unavailable run_id=%s: %s", run_id, exc)
            count = 0
        if count > 0:
            logger.info("accept_baseline gcs_mcf_download run_id=%s files=%d", run_id, count)
            genmcf_dir = Path(tmp)

    if genmcf_dir is None:
        if _mcf_tmp_dir:
            shutil.rmtree(_mcf_tmp_dir, ignore_errors=True)
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
    finally:
        if _mcf_tmp_dir:
            shutil.rmtree(_mcf_tmp_dir, ignore_errors=True)

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


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline registry + unified run APIs (/api/runs, /api/pipeline/registry)
# ──────────────────────────────────────────────────────────────────────────────


@app.get("/api/pipeline/registry")
async def get_pipeline_registry():
    """Canonical pipeline step registry (labels, indices, legacy marker map)."""
    try:
        return pipeline_registry_payload(APP_ROOT)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        logger.exception("get_pipeline_registry failed")
        raise HTTPException(status_code=500, detail=f"Failed to load pipeline registry: {exc}")


async def _read_run_status(run_id: str) -> dict:
    """Unified status read (GCS status.json + Batch API); normalizes v1 + legacy fields."""
    try:
        status = await asyncio.to_thread(fetch_run_status, run_id.strip())
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        logger.exception("_read_run_status error run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=f"Failed to read run status: {exc}")
    if status is None:
        raise HTTPException(status_code=404, detail="Run status not found")
    return normalize_run_status(status)


@app.get("/api/runs/{run_id}")
async def get_run_status(run_id: str):
    """Unified run status for Batch (and GCS-backed) runs."""
    return await _read_run_status(run_id)


# ──────────────────────────────────────────────────────────────────────────────
# Cloud Batch job orchestration endpoints
#
# Canonical run API: POST/GET /api/runs, POST /api/runs/{run_id}/cancel.
# Legacy aliases (same behavior): POST /api/jobs, GET /api/jobs/{id}/status|report,
# POST /api/jobs/{id}/cancel. Local subprocess runs stay on /api/run/* stream endpoints.
#
# Typical Batch flow:
#   1. POST /api/prepare-upload   → signed GCS upload URLs
#   2. Browser uploads files to GCS
#   3. POST /api/runs (or /api/jobs) → submit Batch job
#   4. GET  /api/runs/{run_id}    → poll status (jobs/.../status is equivalent)
#   5. GET  /api/runs/{run_id}/report (or jobs/.../report) → HTML report from GCS
#   6. POST /api/runs/{run_id}/cancel (or jobs/.../cancel)
# ──────────────────────────────────────────────────────────────────────────────

def _validate_gcs_uri(uri: str, field: str) -> str | None:
    """Return an error string if uri is not a valid gs:// URI, else None."""
    if not uri.startswith("gs://"):
        return f"{field} must start with gs://, got: {uri!r}"
    parts = uri[5:].split("/", 1)
    if not parts[0]:
        return f"{field} has no bucket name: {uri!r}"
    if len(parts) < 2 or not parts[1].strip():
        return f"{field} has no object path (bucket-only URI): {uri!r}"
    return None


class _SubmitJobRequest(BaseModel):
    """Request body for POST /api/jobs."""
    run_id: str
    dataset: str
    # GCS upload session prefix (e.g. "sessions/abc123") — required for custom dataset.
    session_id: str = ""
    # File metadata (required for custom dataset).
    tmcf_filename: str = ""
    csv_filenames: list[str] = []
    stat_vars_mcf_filename: str = ""
    stat_vars_schema_mcf_filename: str = ""
    # Total compressed CSV bytes — used for machine tier selection.
    csv_total_bytes: int = 0
    # GCS path mode: full gs:// URIs (mutually exclusive with session_id + tmcf_filename).
    tmcf_gcs_path: str = ""
    csv_gcs_paths: list[str] = []
    stat_vars_mcf_gcs_path: str = ""
    stat_vars_schema_mcf_gcs_path: str = ""
    # Pipeline options.
    llm_review: bool = False
    rules: str = ""
    skip_rules: str = ""
    baseline_name: str = ""
    import_resolution_mode: str = "LOCAL"
    existence_checks: str = "false"
    # Per-run custom SQL rules (not persisted; merged into the config for this run only).
    custom_rules: list[dict] = []
    # Optional: https:// or gs:// URI to a validation config JSON that overrides rule selection and custom_rules.
    validation_config_url: str = ""
    # Performance tuning (Batch only).
    # machine_type_override: one of n2-highmem-16 / n2-highmem-32 / n2-highmem-64.
    # Omit to let the server auto-select based on csv_total_bytes.
    machine_type_override: str = ""
    # processing_mode: auto | conservative | aggressive | custom.
    processing_mode: str = "auto"
    # java_threads: explicit thread count; required when processing_mode == "custom".
    java_threads: int = 0


@app.post("/api/runs")
async def create_run(body: _SubmitJobRequest):
    """Canonical run submission — dispatches via orchestration policy (Batch in production)."""
    run_id = body.run_id.strip()
    if not run_id:
        raise HTTPException(status_code=400, detail="run_id is required")
    try:
        spec = job_request_to_run_spec(body)
        resolution = resolve_executor(spec)
    except PolicyBlockedError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if resolution.executor == "subprocess":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "USE_LEGACY_STREAM_ENDPOINT",
                "message": (
                    "Subprocess runs use existing upload/stream endpoints; "
                    "POST /api/runs submits Cloud Batch jobs when policy selects batch."
                ),
                "executor": resolution.executor,
                "profile": resolution.profile,
                "reason": resolution.reason,
                **subprocess_legacy_hint(spec),
            },
        )

    batch_result = await _execute_batch_job_submission(body)
    return build_run_created_response(
        run_id=run_id,
        resolution=resolution,
        batch_result=batch_result,
    )


async def _execute_batch_job_submission(body: _SubmitJobRequest) -> dict:
    """Submit a Cloud Batch validation job (shared by POST /api/jobs and POST /api/runs).

    Supports two file-delivery modes for custom datasets (mutually exclusive):
    - Upload session: files uploaded via POST /api/prepare-upload + browser PUT.
      Pass session_id + tmcf_filename + csv_filenames.
    - GCS path mode: files already in GCS. Pass tmcf_gcs_path + csv_gcs_paths.
      The Batch VM downloads them directly using BATCH_SERVICE_ACCOUNT, which
      must have read access to the target buckets.

    For built-in datasets (child_birth, etc.) all file fields may be omitted —
    the container already has those files baked in.

    Returns { run_id, job_name } on success.
    """
    run_id = body.run_id.strip()
    dataset = body.dataset.strip()

    # Shared orchestration policy (POST /api/jobs and POST /api/runs after runs pre-check).
    try:
        _policy_spec = job_request_to_run_spec(body)
        _policy_resolution = resolve_executor(_policy_spec)
    except PolicyBlockedError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if _policy_resolution.executor != BATCH:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "USE_LEGACY_STREAM_ENDPOINT",
                "message": (
                    "Subprocess runs use existing upload/stream endpoints; "
                    "Batch submission requires production profile with GCS + Batch configured."
                ),
                "executor": _policy_resolution.executor,
                "profile": _policy_resolution.profile,
                "reason": _policy_resolution.reason,
                **subprocess_legacy_hint(_policy_spec),
            },
        )

    if not run_id:
        raise HTTPException(status_code=400, detail="run_id is required")
    if not dataset:
        raise HTTPException(status_code=400, detail="dataset is required")
    if dataset not in BUILTIN_DATASETS and dataset != "custom":
        raise HTTPException(status_code=400, detail=f"Unknown dataset: {dataset!r}")
    if dataset == "custom":
        has_filenames = bool(body.tmcf_filename and body.csv_filenames)
        has_gcs_paths = bool(body.tmcf_gcs_path and body.csv_gcs_paths)
        if not has_filenames and not has_gcs_paths:
            raise HTTPException(
                status_code=400,
                detail="Provide either tmcf_filename+csv_filenames (upload session) "
                       "or tmcf_gcs_path+csv_gcs_paths (GCS path mode)",
            )
        if has_gcs_paths:
            body.tmcf_gcs_path = body.tmcf_gcs_path.strip()
            body.csv_gcs_paths = [u.strip() for u in body.csv_gcs_paths if u.strip()]
            if not body.csv_gcs_paths:
                raise HTTPException(status_code=400, detail="csv_gcs_paths is empty after stripping whitespace")
            if len(body.csv_gcs_paths) > 100:
                raise HTTPException(status_code=400, detail=f"csv_gcs_paths exceeds maximum of 100 entries (got {len(body.csv_gcs_paths)})")
            body.stat_vars_mcf_gcs_path = body.stat_vars_mcf_gcs_path.strip()
            body.stat_vars_schema_mcf_gcs_path = body.stat_vars_schema_mcf_gcs_path.strip()
            if err := _validate_gcs_uri(body.tmcf_gcs_path, "tmcf_gcs_path"):
                raise HTTPException(status_code=400, detail=err)
            for i, uri in enumerate(body.csv_gcs_paths):
                if err := _validate_gcs_uri(uri, f"csv_gcs_paths[{i}]"):
                    raise HTTPException(status_code=400, detail=err)
            if body.stat_vars_mcf_gcs_path:
                if err := _validate_gcs_uri(body.stat_vars_mcf_gcs_path, "stat_vars_mcf_gcs_path"):
                    raise HTTPException(status_code=400, detail=err)
            if body.stat_vars_schema_mcf_gcs_path:
                if err := _validate_gcs_uri(body.stat_vars_schema_mcf_gcs_path, "stat_vars_schema_mcf_gcs_path"):
                    raise HTTPException(status_code=400, detail=err)

    # Performance tuning validation.
    _ALLOWED_MACHINE_TYPES = set(_batch_runner._VCPUS_BY_MACHINE)
    _ALLOWED_PROCESSING_MODES = {"auto", "conservative", "aggressive", "custom"}
    if body.machine_type_override and body.machine_type_override not in _ALLOWED_MACHINE_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid machine_type_override: {body.machine_type_override!r}. "
                   f"Allowed: {sorted(_ALLOWED_MACHINE_TYPES)}",
        )
    if body.processing_mode not in _ALLOWED_PROCESSING_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid processing_mode: {body.processing_mode!r}. "
                   f"Allowed: {sorted(_ALLOWED_PROCESSING_MODES)}",
        )
    # Warn when aggressive mode is requested (via API/direct calls). Benchmark evidence
    # shows aggressive (75% vCPUs) degrades vs auto (50% vCPUs) at typical shard counts.
    if body.processing_mode == "aggressive":
        logger.warning(
            "submit_batch_job: processing_mode=aggressive requested — benchmark evidence "
            "shows this degrades vs auto at typical shard counts [run_id=%s]",
            run_id,
        )
    if body.processing_mode == "custom":
        if body.java_threads <= 0:
            raise HTTPException(
                status_code=400,
                detail="java_threads must be > 0 when processing_mode is 'custom'",
            )
        # Upper-bound check: if machine_type_override is set, cap at that machine's vCPUs.
        # Without an override, auto-tier selection runs at submission time — we don't know
        # the actual machine here. Use the global maximum (largest supported machine) as the
        # hard cap; _build_env_vars enforces the tighter per-machine cap at runtime.
        if body.machine_type_override:
            max_vcpus = _batch_runner._VCPUS_BY_MACHINE[body.machine_type_override]
            if body.java_threads > max_vcpus:
                raise HTTPException(
                    status_code=400,
                    detail=f"java_threads ({body.java_threads}) exceeds vCPU count for "
                           f"{body.machine_type_override} ({max_vcpus})",
                )
        else:
            global_max = max(_batch_runner._VCPUS_BY_MACHINE.values())  # 64
            if body.java_threads > global_max:
                raise HTTPException(
                    status_code=400,
                    detail=f"java_threads ({body.java_threads}) exceeds the maximum supported "
                           f"vCPU count ({global_max})",
                )

    if body.custom_rules:
        err = _validate_custom_rules(body.custom_rules)
        if err:
            raise HTTPException(status_code=400, detail=err)

    # For the Batch path, create the merged validation config on the server and upload
    # it to GCS so the Batch VM can download it via MERGED_CONFIG_GCS_PATH.
    # Priority: validation_config_url (user-supplied config) > custom_rules/rules (merged config).
    # The Batch VM never merges — it only downloads and passes --config= to the pipeline.
    merged_config_gcs_path = ""
    logger.info('[OVERRIDE_TRACE] %s', json.dumps({
        "component": "submit_batch_job", "event": "request_received",
        "run_id": run_id,
        "validation_config_url": body.validation_config_url or "",
        "rules": body.rules or "",
        "custom_rules_count": len(body.custom_rules or []),
    }))
    if body.validation_config_url:
        # Fetch the user-supplied config, validate it, write to temp, upload to GCS.
        _url_content = await _fetch_and_validate_config(body.validation_config_url)
        fd, _url_tmp = tempfile.mkstemp(suffix=".json", prefix="validation_config_")
        try:
            with os.fdopen(fd, "wb") as _f:
                _f.write(_url_content)
            logger.info(
                "submit_batch_job: uploading url config to GCS run_id=%s url=%s",
                run_id, body.validation_config_url,
            )
            merged_config_gcs_path = await asyncio.to_thread(
                gcs_reports.upload_merged_config_to_gcs, run_id, Path(_url_tmp)
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to upload config to GCS: {exc}")
        finally:
            try:
                os.unlink(_url_tmp)
            except OSError:
                pass
        # Detailed GCS artifact log: bucket / object path / full URI separately.
        if merged_config_gcs_path:
            _bucket_name = merged_config_gcs_path.split("/")[2] if merged_config_gcs_path.startswith("gs://") else "?"
            _obj_path = "/".join(merged_config_gcs_path.split("/")[3:]) if merged_config_gcs_path.startswith("gs://") else merged_config_gcs_path
            logger.info('[OVERRIDE_TRACE] %s', json.dumps({
                "component": "submit_batch_job", "event": "config_uploaded",
                "run_id": run_id,
                "bucket": _bucket_name,
                "object_path": _obj_path,
                "gcs_uri": merged_config_gcs_path,
            }))
        # Hard assertion: URL was set but upload returned empty — fail immediately.
        if not merged_config_gcs_path:
            logger.error('[OVERRIDE_TRACE] %s', json.dumps({
                "component": "submit_batch_job", "event": "upload_returned_empty",
                "run_id": run_id,
                "validation_config_url": body.validation_config_url,
                "hint": "GCS_REPORTS_BUCKET may not be set or upload_merged_config_to_gcs returned empty string",
            }))
            raise HTTPException(
                status_code=500,
                detail="GCS_REPORTS_BUCKET is required to use a custom validation config with Batch jobs.",
            )
    elif body.custom_rules or body.rules:
        dataset_key = dataset if dataset in DATASET_CONFIG_MAP else "custom"
        rule_ids = [x.strip() for x in body.rules.split(",") if x.strip()] if body.rules else []
        _merged_tmp = _create_merged_config(dataset_key, rule_ids, body.custom_rules)
        _merged_tmp_path = _merged_tmp  # may be None
        if _merged_tmp:
            logger.info(
                "submit_batch_job: uploading merged config to GCS run_id=%s custom_rule_count=%d",
                run_id, len(body.custom_rules),
            )
            try:
                merged_config_gcs_path = await asyncio.to_thread(
                    gcs_reports.upload_merged_config_to_gcs, run_id, _merged_tmp
                )
            except Exception as exc:
                logger.error("upload_merged_config_to_gcs failed run_id=%s: %s", run_id, exc)
                raise HTTPException(status_code=500, detail=f"Failed to upload merged config to GCS: {exc}")
            finally:
                if _merged_tmp_path and _merged_tmp_path.exists():
                    _merged_tmp_path.unlink()

            # Fail fast if GCS is not configured — Batch VMs cannot download the config
            # without a GCS path, so custom rules would be silently skipped.
            if not merged_config_gcs_path:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "GCS_REPORTS_BUCKET is required to use custom SQL rules or rule filters "
                        "with Batch jobs — the merged config must be uploaded to GCS before job "
                        "submission so the Batch VM can download it."
                    ),
                )
            logger.info("submit_batch_job: merged config uploaded run_id=%s path=%s", run_id, merged_config_gcs_path)

    # GCS path mode: csv_total_bytes is 0 because the client has no File objects to
    # measure. Fetch actual object sizes from GCS so tier selection is accurate.
    # Falls back to the largest tier on any error (permission denied, missing object,
    # GCS unavailable) so the job always proceeds.
    csv_total_bytes = body.csv_total_bytes
    if csv_total_bytes == 0 and body.csv_gcs_paths:
        _LARGEST_TIER_BYTES = 50 * 1024 ** 3 + 1  # forces n2-highmem-64 / 12 h in _select_tier
        try:
            from google.cloud import storage as _gcs
            _client = _gcs.Client()
            _total = 0
            _fallback = False
            _bucket_cache: dict = {}
            for _uri in body.csv_gcs_paths:
                # Parse gs://bucket/object-path
                _without_scheme = _uri[len("gs://"):]
                _bucket_name, _, _blob_path = _without_scheme.partition("/")
                if not _bucket_name or not _blob_path:
                    logger.warning(
                        "submit_batch_job: malformed GCS URI %r — falling back to largest tier",
                        _uri,
                    )
                    _fallback = True
                    break
                if _bucket_name not in _bucket_cache:
                    _bucket_cache[_bucket_name] = _client.bucket(_bucket_name)
                _blob = _bucket_cache[_bucket_name].get_blob(_blob_path)
                if _blob is None or _blob.size is None:
                    logger.warning(
                        "submit_batch_job: cannot get size for %s — falling back to largest tier",
                        _uri,
                    )
                    _fallback = True
                    break
                _total += _blob.size
            csv_total_bytes = _LARGEST_TIER_BYTES if _fallback else _total
            logger.info(
                "submit_batch_job: GCS path mode csv_total_bytes=%d fallback=%s run_id=%s",
                csv_total_bytes, _fallback, run_id,
            )
        except Exception as _exc:
            logger.warning(
                "submit_batch_job: failed to fetch GCS object sizes (%s) — falling back to largest tier",
                _exc,
            )
            csv_total_bytes = _LARGEST_TIER_BYTES

    # Auto-upgrade undersized machine overrides.
    # If the user explicitly picked a machine tier that is too small for the actual
    # file size, bump it to the correct tier rather than hard-failing. This protects
    # against accidental misconfiguration (e.g. selecting Small for a 15 GB file)
    # without breaking the API contract. The upgrade is logged as a warning so
    # operators can see when it fires.
    effective_machine_override = body.machine_type_override or ""
    if effective_machine_override and csv_total_bytes > 0:
        _tier_max_bytes: dict[str, int] = {
            "n2-highmem-16": 5  * 1024**3,
            "n2-highmem-32": 20 * 1024**3,
            "n2-highmem-64": 0,  # largest tier — no upper bound
        }
        _override_max = _tier_max_bytes.get(effective_machine_override, 0)
        # >= mirrors _select_tier's strict-less-than boundary: _select_tier picks
        # n2-highmem-16 only when csv_total_bytes < 5 GiB, so we upgrade on equality
        # too (a 5 GiB file is not valid for n2-highmem-16 in either path).
        if _override_max > 0 and csv_total_bytes >= _override_max:
            _recommended, _ = _batch_runner._select_tier(csv_total_bytes)
            logger.warning(
                "submit_batch_job: machine_type_override=%s is undersized for "
                "csv_total_bytes=%d (%.1f GB); auto-upgrading to %s [run_id=%s]",
                effective_machine_override,
                csv_total_bytes,
                csv_total_bytes / 1024**3,
                _recommended,
                run_id,
            )
            effective_machine_override = _recommended

    _base_spec = job_request_to_run_spec(body, merged_config_gcs_path=merged_config_gcs_path)
    _rules_filter_val = effective_rules_filter(_base_spec)
    logger.info('[OVERRIDE_TRACE] %s', json.dumps({
        "component": "submit_batch_job", "event": "building_batch_plan",
        "run_id": run_id,
        "merged_config_gcs_path": merged_config_gcs_path,
        "rules_filter": _rules_filter_val,
        "skip_rules_filter": body.skip_rules or "",
    }))
    batch_spec = run_spec_with_batch_overrides(
        _base_spec,
        csv_total_bytes=csv_total_bytes,
        rules_filter=_rules_filter_val,
        machine_type_override=effective_machine_override,
    )
    batch_plan = BatchExecutor().plan_submit(batch_spec)
    logger.info('[OVERRIDE_TRACE] %s', json.dumps({
        "component": "submit_batch_job", "event": "batch_plan_created",
        "run_id": run_id,
        "merged_config_gcs_path": batch_plan.input_files.merged_config_gcs_path,
        "rules_filter": batch_plan.input_files.rules_filter,
    }))

    try:
        batch_result = await asyncio.to_thread(BatchExecutor().submit, batch_plan)
        job_name = batch_result.job_name
    except KeyError as exc:
        # A required env var (BATCH_PROJECT_ID etc.) is not set.
        logger.error("submit_batch_job missing env var: %s", exc)
        raise HTTPException(
            status_code=503,
            detail=f"Batch not configured — missing environment variable: {exc}",
        )
    except Exception as exc:
        logger.exception("submit_batch_job failed run_id=%s dataset=%s", run_id, dataset)
        raise HTTPException(status_code=500, detail=f"Failed to submit Batch job: {exc}")

    custom_rule_ids = [r.get("rule_id") for r in body.custom_rules] if body.custom_rules else []
    logger.info(
        "batch_job_submitted run_id=%s dataset=%s job_name=%s custom_rules=%s",
        run_id, dataset, job_name, custom_rule_ids,
    )
    return {"run_id": run_id, "job_name": job_name}


@app.post("/api/jobs")
async def submit_batch_job(body: _SubmitJobRequest):
    """Legacy alias for POST /api/runs (same policy gate and Batch submission path)."""
    return await _execute_batch_job_submission(body)


@app.get("/api/jobs/{run_id}/status")
async def get_batch_job_status(run_id: str):
    """Same status path as GET /api/runs/{run_id}."""
    try:
        return await _read_run_status(run_id)
    except HTTPException as exc:
        if exc.status_code == 404:
            raise HTTPException(status_code=404, detail="Job status not found")
        raise


async def _batch_run_html_report(run_id: str) -> HTMLResponse:
    """Fetch validation_report.html for a Batch run (dataset from status.json)."""
    if not is_gcs_configured():
        raise HTTPException(
            status_code=503,
            detail="GCS reports bucket is not configured (GCS_REPORTS_BUCKET not set)",
        )

    try:
        status = await asyncio.to_thread(_get_job_status, run_id)
    except Exception as exc:
        logger.exception("batch_run_html_report: status read error run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=f"Failed to read run status: {exc}")

    if status is None:
        raise HTTPException(status_code=404, detail="Run not found")

    dataset = status.get("dataset", "")
    if not dataset:
        raise HTTPException(status_code=500, detail="Dataset not recorded in run status")

    try:
        content = await asyncio.to_thread(
            gcs_reports.get_report_from_gcs, run_id, dataset, "validation_report.html"
        )
    except GCSAccessError as exc:
        raise HTTPException(status_code=503, detail=f"GCS not accessible: {exc}")
    except Exception as exc:
        logger.exception(
            "batch_run_html_report: GCS read error run_id=%s dataset=%s", run_id, dataset
        )
        raise HTTPException(status_code=500, detail=f"Failed to read report: {exc}")

    if content is None:
        raise HTTPException(
            status_code=404,
            detail="Report not available yet — the run may still be in progress",
        )

    return HTMLResponse(content=content.decode("utf-8", errors="replace"))


@app.get("/api/runs/{run_id}/report", response_class=HTMLResponse)
async def get_run_report(run_id: str):
    """Canonical HTML report for a completed Batch run."""
    return await _batch_run_html_report(run_id)


@app.get("/api/jobs/{run_id}/report", response_class=HTMLResponse)
async def get_batch_job_report(run_id: str):
    """Legacy alias for GET /api/runs/{run_id}/report."""
    return await _batch_run_html_report(run_id)


async def _cancel_run_impl(run_id: str) -> dict:
    """Cancel a running Batch job (shared by /api/runs and /api/jobs cancel routes).

    Reads batch_job_name from status.json when available; falls back to
    compute_job_name(run_id) during provisioning before status.json exists.
    """
    try:
        status = await asyncio.to_thread(_get_job_status, run_id)
    except Exception as exc:
        logger.exception("cancel_batch_job: status read error run_id=%s", run_id)
        raise HTTPException(status_code=500, detail=f"Failed to read job status: {exc}")

    if status is None:
        # status.json not yet written (job still provisioning) — derive job_name
        # deterministically from env vars so we can still cancel.
        try:
            job_name = await asyncio.to_thread(_batch_runner.compute_job_name, run_id)
            logger.info(
                "cancel_batch_job: no status.json yet; derived job_name=%s run_id=%s",
                job_name, run_id,
            )
        except Exception as exc:
            logger.warning("cancel_batch_job: cannot derive job_name run_id=%s: %s", run_id, exc)
            raise HTTPException(status_code=404, detail="Job not found")
    else:
        job_name = status.get("batch_job_name", "")
        if not job_name:
            raise HTTPException(
                status_code=409,
                detail="batch_job_name not recorded in status — cannot cancel",
            )
        current_status = status.get("status", "")
        if current_status in ("succeeded", "failed"):
            logger.info("cancel_batch_job: job already terminal run_id=%s status=%s", run_id, current_status)
            return {"ok": True, "run_id": run_id, "message": f"Job already {current_status}"}

    try:
        await asyncio.to_thread(_batch_runner.cancel_job, job_name)
    except Exception as exc:
        logger.exception("cancel_batch_job: cancel error run_id=%s job_name=%s", run_id, job_name)
        raise HTTPException(status_code=500, detail=f"Failed to cancel job: {exc}")

    logger.info("batch_job_cancelled run_id=%s job_name=%s", run_id, job_name)
    return {"ok": True, "run_id": run_id, "job_name": job_name}


@app.post("/api/jobs/{run_id}/cancel")
async def cancel_batch_job(run_id: str):
    """Legacy cancel alias — prefer POST /api/runs/{run_id}/cancel."""
    return await _cancel_run_impl(run_id)


@app.post("/api/runs/{run_id}/cancel")
async def cancel_run(run_id: str):
    """Cancel a running Batch validation run."""
    return await _cancel_run_impl(run_id)


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
