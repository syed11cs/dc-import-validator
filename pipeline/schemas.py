"""Canonical v1 pipeline event schemas.

These events are emitted on stdout (one JSON object per line) for ProgressProjector
and related consumers. Registry labels are authoritative; legacy ::STEP:: text is not parsed here.
"""

from __future__ import annotations

from typing import Any, Literal, Mapping

EVENT_VERSION = 1

EventType = Literal["progress", "failure", "run_finished"]
RunStatus = Literal["succeeded", "failed", "cancelled"]

REQUIRED_PROGRESS_KEYS = frozenset(
    {"v", "t", "step_id", "step_index", "step_label"}
)
REQUIRED_FAILURE_KEYS = frozenset(
    {"v", "t", "failure_code", "step_id", "step_index", "step_label", "message"}
)
REQUIRED_RUN_FINISHED_KEYS = frozenset({"v", "t", "status", "exit_code"})


class EventValidationError(ValueError):
    """Raised when an event dict does not match the v1 schema."""


def validate_event(event: Mapping[str, Any]) -> None:
    """Validate a v1 pipeline event dict. Raises EventValidationError on failure."""
    if not isinstance(event, Mapping):
        raise EventValidationError(f"event must be a mapping, got {type(event).__name__}")

    version = event.get("v")
    if version != EVENT_VERSION:
        raise EventValidationError(f"v must be {EVENT_VERSION}, got {version!r}")

    event_type = event.get("t")
    if event_type == "progress":
        _validate_progress(event)
    elif event_type == "failure":
        _validate_failure(event)
    elif event_type == "run_finished":
        _validate_run_finished(event)
    else:
        raise EventValidationError(
            f"t must be 'progress', 'failure', or 'run_finished', got {event_type!r}"
        )


def _validate_progress(event: Mapping[str, Any]) -> None:
    missing = REQUIRED_PROGRESS_KEYS - event.keys()
    if missing:
        raise EventValidationError(f"progress event missing keys: {sorted(missing)}")

    _require_str(event, "step_id")
    _require_str(event, "step_label")
    _require_non_negative_int(event, "step_index")

    substep_id = event.get("substep_id")
    if substep_id is not None:
        if not isinstance(substep_id, str) or not substep_id.strip():
            raise EventValidationError("substep_id must be a non-empty string when set")
        substep_label = event.get("substep_label")
        if substep_label is not None and (
            not isinstance(substep_label, str) or not substep_label.strip()
        ):
            raise EventValidationError("substep_label must be a non-empty string when set")

    _reject_unknown_keys(
        event,
        REQUIRED_PROGRESS_KEYS | {"substep_id", "substep_label"},
        "progress",
    )


def _validate_failure(event: Mapping[str, Any]) -> None:
    missing = REQUIRED_FAILURE_KEYS - event.keys()
    if missing:
        raise EventValidationError(f"failure event missing keys: {sorted(missing)}")

    _require_str(event, "failure_code")
    _require_str(event, "step_id")
    _require_str(event, "step_label")
    _require_str(event, "message")
    _require_non_negative_int(event, "step_index")

    limit = event.get("limit")
    if limit is not None and not isinstance(limit, int):
        raise EventValidationError("limit must be an integer when set")

    details = event.get("details")
    if details is not None and not isinstance(details, dict):
        raise EventValidationError("details must be an object when set")

    substep_id = event.get("substep_id")
    if substep_id is not None:
        if not isinstance(substep_id, str) or not substep_id.strip():
            raise EventValidationError("substep_id must be a non-empty string when set")
        substep_label = event.get("substep_label")
        if substep_label is not None and (
            not isinstance(substep_label, str) or not substep_label.strip()
        ):
            raise EventValidationError("substep_label must be a non-empty string when set")

    _reject_unknown_keys(
        event,
        REQUIRED_FAILURE_KEYS
        | {"limit", "details", "substep_id", "substep_label"},
        "failure",
    )


def _validate_run_finished(event: Mapping[str, Any]) -> None:
    missing = REQUIRED_RUN_FINISHED_KEYS - event.keys()
    if missing:
        raise EventValidationError(f"run_finished event missing keys: {sorted(missing)}")

    status = event.get("status")
    if status not in ("succeeded", "failed", "cancelled"):
        raise EventValidationError(
            f"status must be 'succeeded', 'failed', or 'cancelled', got {status!r}"
        )

    exit_code = event.get("exit_code")
    if not isinstance(exit_code, int):
        raise EventValidationError("exit_code must be an integer")

    failure_code = event.get("failure_code")
    if failure_code is not None and (
        not isinstance(failure_code, str) or not failure_code.strip()
    ):
        raise EventValidationError("failure_code must be a non-empty string when set")

    message = event.get("message")
    if message is not None and not isinstance(message, str):
        raise EventValidationError("message must be a string when set")

    step_id = event.get("step_id")
    if step_id is not None and (not isinstance(step_id, str) or not step_id.strip()):
        raise EventValidationError("step_id must be a non-empty string when set")

    step_index = event.get("step_index")
    if step_index is not None and (
        not isinstance(step_index, int) or step_index < 0
    ):
        raise EventValidationError("step_index must be a non-negative integer when set")

    _reject_unknown_keys(
        event,
        REQUIRED_RUN_FINISHED_KEYS
        | {"failure_code", "message", "step_id", "step_index", "step_label"},
        "run_finished",
    )


def _require_str(event: Mapping[str, Any], key: str) -> None:
    value = event.get(key)
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(f"{key} must be a non-empty string")


def _require_non_negative_int(event: Mapping[str, Any], key: str) -> None:
    value = event.get(key)
    if not isinstance(value, int) or value < 0:
        raise EventValidationError(f"{key} must be a non-negative integer")


def _reject_unknown_keys(
    event: Mapping[str, Any],
    allowed: frozenset[str] | set[str],
    event_type: str,
) -> None:
    extra = set(event.keys()) - set(allowed)
    if extra:
        raise EventValidationError(
            f"{event_type} event has unknown keys: {sorted(extra)}"
        )
