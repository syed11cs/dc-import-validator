"""Run submission and status helpers for /api/runs."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Optional

from pipeline.registry import (
    default_registry_path,
    load_registry,
    registry_as_dict,
    step_by_id,
)
from pipeline.status_v1 import resolve_step_id_from_legacy_token
from ui.orchestration.policy import (
    BATCH,
    SUBPROCESS,
    ExecutorResolution,
    PolicyBlockedError,
    resolve_executor,
)
from ui.orchestration.spec import (
    BUILTIN,
    BUILTIN_DATASETS,
    CUSTOM,
    RunSpec,
    run_spec_from_mapping,
)

STATUS_SCHEMA_VERSION = "1.0"

_REGISTRY = None


def _registry():
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = load_registry()
    return _REGISTRY


def legacy_step_token_to_index(step_token: str) -> Optional[int]:
    """Map legacy status.step token (e.g. '2', '2.4') to canonical step_index."""
    token = str(step_token).strip()
    if not token:
        return None
    reg = _registry()
    step_id = resolve_step_id_from_legacy_token(reg, token)
    if step_id:
        step = step_by_id(reg, step_id)
        if step is not None:
            return step.index
    if token.isdigit():
        return int(token)
    return None


def pipeline_registry_payload(app_root: Path | str | None = None) -> dict[str, Any]:
    """API payload for GET /api/pipeline/registry."""
    if app_root is None:
        path = default_registry_path()
    else:
        path = Path(app_root) / "pipeline" / "registry.yaml"
    return registry_as_dict(load_registry(path))


def effective_rules_filter(spec: RunSpec) -> str:
    """Rules filter passed to Batch InputFiles (empty when a merged/URL config owns rules)."""
    if spec.rules.validation_config_url:
        return ""
    return spec.rules.rules_filter


def run_spec_with_batch_overrides(
    spec: RunSpec,
    *,
    merged_config_gcs_path: str | None = None,
    csv_total_bytes: int | None = None,
    rules_filter: str | None = None,
    machine_type_override: str | None = None,
) -> RunSpec:
    """Return a RunSpec copy with server-computed Batch submission fields applied."""
    inputs = spec.inputs
    rules = spec.rules
    options = spec.options
    if merged_config_gcs_path is not None:
        rules = replace(rules, merged_config_gcs_path=merged_config_gcs_path)
    if rules_filter is not None:
        rules = replace(rules, rules_filter=rules_filter)
    if csv_total_bytes is not None:
        inputs = replace(inputs, csv_total_bytes=csv_total_bytes)
    if machine_type_override is not None:
        options = replace(options, machine_type_override=machine_type_override)
    if inputs is not spec.inputs or rules is not spec.rules or options is not spec.options:
        return replace(spec, inputs=inputs, rules=rules, options=options)
    return spec


def job_request_to_run_spec(
    body: Any,
    *,
    merged_config_gcs_path: str = "",
) -> RunSpec:
    """Map POST /api/jobs | POST /api/runs body (_SubmitJobRequest) to RunSpec."""
    dataset = (getattr(body, "dataset", None) or "").strip()
    mode = BUILTIN if dataset in BUILTIN_DATASETS else CUSTOM
    dataset_id = dataset if mode == BUILTIN else CUSTOM

    csv_paths = list(getattr(body, "csv_gcs_paths", None) or [])
    csv_filenames = list(getattr(body, "csv_filenames", None) or [])

    return run_spec_from_mapping(
        {
            "run_id": getattr(body, "run_id", ""),
            "mode": mode,
            "dataset_id": dataset_id,
            "inputs": {
                "session_id": getattr(body, "session_id", "") or "",
                "tmcf_filename": getattr(body, "tmcf_filename", "") or "",
                "csv_filenames": csv_filenames,
                "stat_vars_mcf_filename": getattr(body, "stat_vars_mcf_filename", "") or "",
                "stat_vars_schema_mcf_filename": (
                    getattr(body, "stat_vars_schema_mcf_filename", "") or ""
                ),
                "csv_total_bytes": int(getattr(body, "csv_total_bytes", 0) or 0),
                "tmcf_gcs_path": getattr(body, "tmcf_gcs_path", "") or "",
                "csv_gcs_paths": csv_paths,
                "stat_vars_mcf_gcs_path": getattr(body, "stat_vars_mcf_gcs_path", "") or "",
                "stat_vars_schema_mcf_gcs_path": (
                    getattr(body, "stat_vars_schema_mcf_gcs_path", "") or ""
                ),
                "baseline_name": getattr(body, "baseline_name", "") or "",
            },
            "rules": {
                "rules_filter": getattr(body, "rules", "") or "",
                "skip_rules_filter": getattr(body, "skip_rules", "") or "",
                "custom_rules": tuple(getattr(body, "custom_rules", None) or ()),
                "validation_config_url": getattr(body, "validation_config_url", "") or "",
                "merged_config_gcs_path": merged_config_gcs_path,
            },
            "options": {
                "llm_review": bool(getattr(body, "llm_review", False)),
                "import_resolution_mode": getattr(body, "import_resolution_mode", "LOCAL") or "LOCAL",
                "existence_checks": getattr(body, "existence_checks", "false") or "false",
                "processing_mode": getattr(body, "processing_mode", "auto") or "auto",
                "java_threads": int(getattr(body, "java_threads", 0) or 0),
                "machine_type_override": getattr(body, "machine_type_override", "") or "",
            },
        }
    )


def normalize_run_status(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Ensure status.json v1 fields while preserving legacy poll keys."""
    out = dict(raw)
    out["schema_version"] = out.get("schema_version") or STATUS_SCHEMA_VERSION

    if out.get("step_index") is None and out.get("step_id"):
        step = step_by_id(_registry(), str(out["step_id"]))
        if step is not None:
            out["step_index"] = step.index
    if out.get("step_index") is None and out.get("step") is not None:
        idx = legacy_step_token_to_index(str(out["step"]))
        if idx is not None:
            out["step_index"] = idx

    if out.get("batch_job_name"):
        out.setdefault("executor", BATCH)
    elif not out.get("executor"):
        out.setdefault("executor", SUBPROCESS)

    return out


def fetch_run_status(
    run_id: str,
    *,
    job_name: Optional[str] = None,
    bucket_name: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Read run status from GCS (+ Batch API fallbacks). Returns normalized dict or None."""
    from ui.services.job_status import get_job_status

    if job_name is None:
        try:
            from ui.services.batch_runner import compute_job_name

            job_name = compute_job_name(run_id)
        except KeyError:
            job_name = None

    bucket = bucket_name or os.environ.get("GCS_REPORTS_BUCKET", "")
    if not bucket:
        return None

    raw = get_job_status(run_id, bucket, job_name=job_name)
    if raw is None:
        return None
    return normalize_run_status(raw)


def build_run_created_response(
    *,
    run_id: str,
    resolution: ExecutorResolution,
    batch_result: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Canonical POST /api/runs response (superset of legacy POST /api/jobs)."""
    out: dict[str, Any] = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "run_id": run_id,
        "executor": resolution.executor,
        "profile": resolution.profile,
        "status": "submitted",
    }
    job_name = (batch_result or {}).get("job_name") if batch_result else None
    if job_name:
        out["job_name"] = job_name
        out["batch_job_name"] = job_name
    return out


def subprocess_legacy_hint(spec: RunSpec) -> dict[str, str]:
    """Legacy endpoints that preserve upload/stream semantics for subprocess runs."""
    if spec.is_builtin:
        return {
            "legacy_endpoint": f"/api/run/{spec.dataset_id}",
            "legacy_stream_endpoint": f"/api/run/{spec.dataset_id}?stream=true",
        }
    return {"legacy_endpoint": "/api/run/custom/stream"}
