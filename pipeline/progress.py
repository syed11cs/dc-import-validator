"""Canonical v1 progress/failure/run_finished emitters.

CLI usage (stdout, one JSON line per invocation):
    python -m pipeline.progress progress --step-id pre_import
    python -m pipeline.progress progress --step-id pre_import --substep-id csv_split
    python -m pipeline.progress failure --step-id pre_import --failure-code PREFLIGHT_FAILED \\
        --message "Preflight failed" [--details-file errors.json]
    python -m pipeline.progress run_finished --status succeeded --exit-code 0
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from pipeline.registry import Registry, Substep, Step, load_registry, step_by_id
from pipeline.schemas import EVENT_VERSION, EventValidationError, validate_event


def _registry(path: Path | str | None = None) -> Registry:
    return load_registry(path)


def _resolve_step(
    registry: Registry,
    step_id: str,
    substep_id: str | None = None,
) -> tuple[Step, Substep | None]:
    step = step_by_id(registry, step_id)
    if step is None:
        known = ", ".join(s.id for s in registry.steps)
        raise ValueError(f"unknown step_id {step_id!r}; known steps: {known}")

    substep: Substep | None = None
    if substep_id is not None:
        for candidate in step.substeps:
            if candidate.id == substep_id:
                substep = candidate
                break
        if substep is None:
            known_subs = ", ".join(s.id for s in step.substeps) or "(none)"
            raise ValueError(
                f"unknown substep_id {substep_id!r} for step {step_id!r}; "
                f"known substeps: {known_subs}"
            )
    return step, substep


def build_progress_event(
    step_id: str,
    *,
    substep_id: str | None = None,
    registry: Registry | None = None,
    registry_path: Path | str | None = None,
) -> dict[str, Any]:
    """Build a validated v1 progress event (labels from registry only)."""
    reg = registry if registry is not None else _registry(registry_path)
    step, substep = _resolve_step(reg, step_id, substep_id)
    event: dict[str, Any] = {
        "v": EVENT_VERSION,
        "t": "progress",
        "step_id": step.id,
        "step_index": step.index,
        "step_label": step.label,
    }
    if substep is not None:
        event["substep_id"] = substep.id
        event["substep_label"] = substep.label
    validate_event(event)
    return event


def build_failure_event(
    step_id: str,
    failure_code: str,
    message: str,
    *,
    substep_id: str | None = None,
    limit: int | None = None,
    details: dict[str, Any] | None = None,
    registry: Registry | None = None,
    registry_path: Path | str | None = None,
) -> dict[str, Any]:
    """Build a validated v1 failure event (labels from registry only)."""
    if not failure_code or not failure_code.strip():
        raise ValueError("failure_code must be a non-empty string")
    if not isinstance(message, str):
        raise ValueError("message must be a string")

    reg = registry if registry is not None else _registry(registry_path)
    step, substep = _resolve_step(reg, step_id, substep_id)

    event: dict[str, Any] = {
        "v": EVENT_VERSION,
        "t": "failure",
        "failure_code": failure_code.strip(),
        "step_id": step.id,
        "step_index": step.index,
        "step_label": step.label,
        "message": message,
    }
    if substep is not None:
        event["substep_id"] = substep.id
        event["substep_label"] = substep.label
    if limit is not None:
        if not isinstance(limit, int):
            raise ValueError("limit must be an integer")
        event["limit"] = limit
    if details is not None:
        if not isinstance(details, dict):
            raise ValueError("details must be a dict")
        event["details"] = details
    validate_event(event)
    return event


def build_run_finished_event(
    status: str,
    exit_code: int,
    *,
    failure_code: str | None = None,
    message: str | None = None,
    step_id: str | None = None,
    registry: Registry | None = None,
    registry_path: Path | str | None = None,
) -> dict[str, Any]:
    """Build a validated v1 run_finished event."""
    if status not in ("succeeded", "failed", "cancelled"):
        raise ValueError(
            f"status must be 'succeeded', 'failed', or 'cancelled', got {status!r}"
        )
    if not isinstance(exit_code, int):
        raise ValueError("exit_code must be an integer")

    event: dict[str, Any] = {
        "v": EVENT_VERSION,
        "t": "run_finished",
        "status": status,
        "exit_code": exit_code,
    }

    if step_id is not None:
        reg = registry if registry is not None else _registry(registry_path)
        step, _ = _resolve_step(reg, step_id)
        event["step_id"] = step.id
        event["step_index"] = step.index
        event["step_label"] = step.label

    if failure_code is not None:
        if not failure_code.strip():
            raise ValueError("failure_code must be a non-empty string when set")
        event["failure_code"] = failure_code.strip()
    if message is not None:
        event["message"] = message

    validate_event(event)
    return event


def format_event(event: dict[str, Any]) -> str:
    """Serialize a v1 event to a single JSON line (no trailing newline)."""
    validate_event(event)
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False)


def emit_line(event: dict[str, Any], *, file=None) -> str:
    """Write one JSON event line to file (default stdout). Returns the line."""
    line = format_event(event)
    out = sys.stdout if file is None else file
    out.write(line + "\n")
    out.flush()
    return line


def _load_details_file(path: str) -> dict[str, Any]:
    details_path = Path(path)
    if not details_path.is_file():
        raise ValueError(f"details file not found: {details_path}")
    with details_path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("details file must contain a JSON object")
    return data


def _cmd_progress(args: argparse.Namespace) -> int:
    event = build_progress_event(
        args.step_id,
        substep_id=args.substep_id,
        registry_path=args.registry_path,
    )
    emit_line(event)
    return 0


def _cmd_failure(args: argparse.Namespace) -> int:
    details = None
    if args.details_file:
        try:
            details = _load_details_file(args.details_file)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"error: invalid details file: {e}", file=sys.stderr)
            return 2
    try:
        event = build_failure_event(
            args.step_id,
            args.failure_code,
            args.message,
            substep_id=args.substep_id,
            limit=args.limit,
            details=details,
            registry_path=args.registry_path,
        )
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    emit_line(event)
    return 0


def _cmd_run_finished(args: argparse.Namespace) -> int:
    try:
        event = build_run_finished_event(
            args.status,
            args.exit_code,
            failure_code=args.failure_code,
            message=args.message,
            step_id=args.step_id,
            registry_path=args.registry_path,
        )
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    emit_line(event)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Emit canonical v1 pipeline progress events (stdout, one JSON line)."
    )
    parser.add_argument(
        "--registry-path",
        type=Path,
        default=None,
        help="Path to registry.yaml (default: pipeline/registry.yaml)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_progress = subparsers.add_parser("progress", help="Emit a progress event")
    p_progress.add_argument("--step-id", required=True, help="Canonical step id")
    p_progress.add_argument("--substep-id", default=None, help="Canonical substep id")
    p_progress.set_defaults(func=_cmd_progress)

    p_failure = subparsers.add_parser("failure", help="Emit a failure event")
    p_failure.add_argument("--step-id", required=True)
    p_failure.add_argument("--substep-id", default=None)
    p_failure.add_argument("--failure-code", required=True)
    p_failure.add_argument("--message", required=True)
    p_failure.add_argument("--limit", type=int, default=None)
    p_failure.add_argument(
        "--details-file",
        default=None,
        help="Path to JSON object embedded as failure details",
    )
    p_failure.set_defaults(func=_cmd_failure)

    p_done = subparsers.add_parser(
        "run_finished", help="Emit a terminal run_finished event"
    )
    p_done.add_argument(
        "--status",
        required=True,
        choices=("succeeded", "failed", "cancelled"),
    )
    p_done.add_argument("--exit-code", type=int, required=True)
    p_done.add_argument("--failure-code", default=None)
    p_done.add_argument("--message", default=None)
    p_done.add_argument(
        "--step-id",
        default=None,
        help="Optional step context (labels from registry)",
    )
    p_done.set_defaults(func=_cmd_run_finished)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except EventValidationError as e:
        print(f"error: event validation failed: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
