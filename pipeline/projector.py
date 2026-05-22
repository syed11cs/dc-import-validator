"""ProgressProjector — canonical semantic projection for pipeline stdout.

Parses v1 structured events and legacy ::STEP:: markers into a single internal state model
and produces status.json v1 projections. Registry labels are authoritative; legacy marker
text after ::STEP:: is never used for labels.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pipeline.registry import Registry, Step, load_registry, resolve_legacy_marker, step_by_id
from pipeline.schemas import EVENT_VERSION, EventValidationError, validate_event
from pipeline.status_v1 import (
    ProjectorState,
    apply_failure,
    apply_run_finished,
    apply_step,
    build_status_projection,
    resolve_step_id_from_legacy_token,
)

FeedKind = Literal[
    "v1_progress",
    "v1_failure",
    "v1_run_finished",
    "legacy_step",
    "legacy_failure",
    "ignored",
]


@dataclass(frozen=True)
class FeedResult:
    handled: bool
    kind: FeedKind = "ignored"


class ProgressProjector:
    """Stateful projector: feed stdout lines, read canonical status.json v1."""

    def __init__(
        self,
        *,
        registry: Registry | None = None,
        registry_path: Path | str | None = None,
        run_id: str = "",
        dataset: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._registry = registry if registry is not None else load_registry(registry_path)
        self._state = ProjectorState(run_id=run_id, dataset=dataset)
        if metadata:
            self._state.metadata.update(metadata)
        self._state.touch()

    @property
    def registry(self) -> Registry:
        return self._registry

    @property
    def state(self) -> ProjectorState:
        return self._state

    def set_run_context(
        self,
        *,
        run_id: str | None = None,
        dataset: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if run_id is not None:
            self._state.run_id = run_id
        if dataset is not None:
            self._state.dataset = dataset
        if metadata:
            self._state.metadata.update(metadata)
        self._state.touch()

    def mark_starting(self, *, step_label: str | None = None) -> None:
        """Batch/bootstrap: job provisioning before pipeline markers."""
        self._state.status = "starting"
        self._state.step_id = None
        self._state.step_index = None
        self._state.step_label = step_label
        self._state.substep_id = None
        self._state.substep_label = None
        self._state.touch()

    def feed_line(self, line: str) -> FeedResult:
        """Ingest one stdout line. Returns whether the line was recognized."""
        stripped = line.strip()
        if not stripped:
            return FeedResult(handled=False)

        if stripped.startswith("{"):
            parsed = _try_parse_json(stripped)
            if parsed is not None:
                return self._feed_json(parsed)

        step = resolve_legacy_marker(self._registry, stripped)
        if step is not None:
            apply_step(self._state, step)
            return FeedResult(handled=True, kind="legacy_step")

        return FeedResult(handled=False)

    def _feed_json(self, obj: dict[str, Any]) -> FeedResult:
        version = obj.get("v")
        event_type = obj.get("t")

        if version == EVENT_VERSION:
            return self._feed_v1_event(obj, event_type)

        if event_type == "failure" and version is None:
            return self._feed_legacy_failure(obj)

        return FeedResult(handled=False)

    def _feed_v1_event(self, obj: dict[str, Any], event_type: Any) -> FeedResult:
        try:
            validate_event(obj)
        except EventValidationError:
            return FeedResult(handled=False)

        if event_type == "progress":
            step = step_by_id(self._registry, obj["step_id"])
            assert step is not None
            apply_step(
                self._state,
                step,
                substep_id=obj.get("substep_id"),
                substep_label=obj.get("substep_label"),
            )
            return FeedResult(handled=True, kind="v1_progress")

        if event_type == "failure":
            step = step_by_id(self._registry, obj["step_id"])
            assert step is not None
            apply_failure(
                self._state,
                failure_code=obj["failure_code"],
                message=obj["message"],
                step=step,
                limit=obj.get("limit"),
                details=obj.get("details"),
            )
            return FeedResult(handled=True, kind="v1_failure")

        if event_type == "run_finished":
            step: Step | None = None
            step_id = obj.get("step_id")
            if step_id:
                step = step_by_id(self._registry, step_id)
            apply_run_finished(
                self._state,
                status=obj["status"],
                exit_code=obj["exit_code"],
                failure_code=obj.get("failure_code"),
                message=obj.get("message"),
                step=step,
            )
            return FeedResult(handled=True, kind="v1_run_finished")

        return FeedResult(handled=False)

    def _feed_legacy_failure(self, obj: dict[str, Any]) -> FeedResult:
        code = obj.get("code") or obj.get("failure_code")
        message = obj.get("message")
        if not code or not isinstance(message, str):
            return FeedResult(handled=False)

        step: Step | None = None
        code_str = str(code).strip()
        if code_str == "CSV_SPLIT_FAILED":
            step = step_by_id(self._registry, "pre_import")
        legacy_step = obj.get("step")
        if step is None and legacy_step is not None:
            step_id = resolve_step_id_from_legacy_token(self._registry, legacy_step)
            if step_id:
                step = step_by_id(self._registry, step_id)

        apply_failure(
            self._state,
            failure_code=str(code),
            message=message,
            step=step,
            limit=_coerce_int(obj.get("limit")),
            details=obj.get("details") if isinstance(obj.get("details"), dict) else None,
        )
        return FeedResult(handled=True, kind="legacy_failure")

    def to_status_dict(self) -> dict[str, Any]:
        """Canonical status.json v1 projection."""
        return build_status_projection(self._state, self._registry)

    def to_status_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_status_dict(), indent=indent, ensure_ascii=False)


def _try_parse_json(line: str) -> dict[str, Any] | None:
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None
