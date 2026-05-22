"""Canonical status.json v1 projection helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from pipeline.registry import Registry, Step, load_registry, step_by_id

STATUS_SCHEMA_VERSION = "1.0"

# Run lifecycle values written to status.json.
RunStatus = str  # starting | running | succeeded | failed | cancelled


@dataclass
class ProjectorState:
    """Internal canonical run projection state (not persisted verbatim)."""

    run_id: str = ""
    dataset: str = ""
    status: str = "starting"
    step_id: str | None = None
    step_index: int | None = None
    step_label: str | None = None
    substep_id: str | None = None
    substep_label: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None
    failure_details: dict[str, Any] | None = None
    failure_limit: int | None = None
    failure_step_id: str | None = None
    failure_step_index: int | None = None
    exit_code: int | None = None
    started_at: str = ""
    updated_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def touch(self) -> None:
        now = _utc_now_iso()
        if not self.started_at:
            self.started_at = now
        self.updated_at = now


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_legacy_step_map(registry: Registry) -> dict[str, str]:
    """Map canonical step_id -> legacy status.step token (e.g. '2.4')."""
    out: dict[str, str] = {}
    for marker, step_id in registry.legacy_markers.items():
        token = _legacy_token_from_marker(marker)
        if token is not None:
            out[step_id] = token
    return out


def build_legacy_numeric_step_map(registry: Registry) -> dict[str, str]:
    """Map legacy numeric step token -> canonical step_id (for legacy failure events)."""
    step_map = build_legacy_step_map(registry)
    return {token: step_id for step_id, token in step_map.items()}


def _legacy_token_from_marker(marker: str) -> str | None:
    prefix = "::STEP::"
    if not marker.startswith(prefix):
        return None
    rest = marker[len(prefix) :]
    token, sep, _ = rest.partition(":")
    if not sep:
        return None
    return token.strip()


def legacy_step_for_step_id(registry: Registry, step_id: str) -> str | None:
    return build_legacy_step_map(registry).get(step_id)


def resolve_step_id_from_legacy_token(
    registry: Registry, token: str | float | int
) -> str | None:
    """Resolve legacy failure/step token to canonical step_id."""
    key = str(token).strip()
    return build_legacy_numeric_step_map(registry).get(key)


def apply_step(
    state: ProjectorState,
    step: Step,
    *,
    substep_id: str | None = None,
    substep_label: str | None = None,
) -> None:
    if state.step_index is not None and step.index < state.step_index:
        return
    state.step_id = step.id
    state.step_index = step.index
    state.step_label = step.label
    state.substep_id = substep_id
    state.substep_label = substep_label
    if state.status == "starting":
        state.status = "running"
    state.touch()


def apply_failure(
    state: ProjectorState,
    *,
    failure_code: str,
    message: str,
    step: Step | None = None,
    limit: int | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    state.failure_code = failure_code
    state.failure_message = message
    state.failure_limit = limit
    state.failure_details = details
    if step is not None:
        state.failure_step_id = step.id
        state.failure_step_index = step.index
    # Pipeline may still be running until run_finished / process exit.
    if state.status in ("starting", "running"):
        state.status = "running"
    state.touch()


def apply_run_finished(
    state: ProjectorState,
    *,
    status: str,
    exit_code: int,
    failure_code: str | None = None,
    message: str | None = None,
    step: Step | None = None,
) -> None:
    state.status = status
    state.exit_code = exit_code
    if failure_code:
        state.failure_code = failure_code
    if message is not None:
        state.failure_message = message
    if step is not None:
        state.failure_step_id = step.id
        state.failure_step_index = step.index
        if state.step_id is None:
            apply_step(state, step)
    state.touch()


def build_status_projection(
    state: ProjectorState,
    registry: Registry,
) -> dict[str, Any]:
    """Build status.json v1 dict from internal state."""
    legacy_step = (
        legacy_step_for_step_id(registry, state.step_id)
        if state.step_id
        else None
    )

    projection: dict[str, Any] = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "run_id": state.run_id,
        "dataset": state.dataset,
        "status": state.status,
        "step_id": state.step_id,
        "step_index": state.step_index,
        "step_label": state.step_label,
        "substep_id": state.substep_id,
        "substep_label": state.substep_label,
        "step": legacy_step,
        "started_at": state.started_at or _utc_now_iso(),
        "updated_at": state.updated_at or _utc_now_iso(),
        "exit_code": state.exit_code,
        "failure_code": state.failure_code,
        "failure_message": state.failure_message,
        "failure_details": state.failure_details,
        "failure_limit": state.failure_limit,
        "failure_step_id": state.failure_step_id,
        "failure_step_index": state.failure_step_index,
    }

    for key, value in state.metadata.items():
        if key not in projection:
            projection[key] = value

    return projection


def validate_status_projection(
    data: Mapping[str, Any],
    *,
    registry: Registry | None = None,
) -> None:
    """Validate a status.json v1 projection (raises ValueError)."""
    if data.get("schema_version") != STATUS_SCHEMA_VERSION:
        raise ValueError(
            f"schema_version must be {STATUS_SCHEMA_VERSION!r}, "
            f"got {data.get('schema_version')!r}"
        )

    status = data.get("status")
    if status not in ("starting", "running", "succeeded", "failed", "cancelled"):
        raise ValueError(f"invalid status: {status!r}")

    step_id = data.get("step_id")
    if step_id is not None:
        reg = registry or load_registry()
        if step_by_id(reg, step_id) is None:
            raise ValueError(f"unknown step_id in projection: {step_id!r}")
        label = data.get("step_label")
        if not isinstance(label, str) or not label.strip():
            raise ValueError("step_label required when step_id is set")
        idx = data.get("step_index")
        if not isinstance(idx, int) or idx < 0:
            raise ValueError("step_index must be a non-negative int when step_id is set")

    legacy = data.get("step")
    if legacy is not None and not isinstance(legacy, str):
        raise ValueError("legacy step field must be a string when set")
