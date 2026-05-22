"""GCS status.json writer driven by ProgressProjector.

Used by batch/entrypoint.sh. Feeds pipeline stdout through the projector and uploads
status.json v1 projections to GCS.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from pipeline.projector import ProgressProjector
from pipeline.registry import load_registry, step_by_id
from pipeline.status_v1 import (
    ProjectorState,
    _utc_now_iso,
    apply_failure,
    apply_run_finished,
    apply_step,
    build_status_projection,
    resolve_step_id_from_legacy_token,
)


def _app_root() -> Path:
    return Path(os.environ.get("DC_VALIDATOR_ROOT", "/app/dc-import-validator"))


def _upload_status(data: dict[str, Any]) -> None:
    from google.cloud import storage

    bucket_name = os.environ["GCS_REPORTS_BUCKET"]
    run_id = os.environ["RUN_ID"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"jobs/{run_id}/status.json")
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json",
    )
    step = data.get("step")
    status = data.get("status")
    label = data.get("step_label")
    ar = data.get("artifacts_ready")
    extra = f" artifacts_ready={ar}" if "artifacts_ready" in data else ""
    print(
        f"[status] step={step} status={status} label={label}{extra}",
        flush=True,
    )


def _state_from_dict(raw: dict[str, Any]) -> ProjectorState:
    meta = raw.get("metadata")
    if not isinstance(meta, dict):
        meta = {}
    return ProjectorState(
        run_id=str(raw.get("run_id") or ""),
        dataset=str(raw.get("dataset") or ""),
        status=str(raw.get("status") or "starting"),
        step_id=raw.get("step_id"),
        step_index=raw.get("step_index"),
        step_label=raw.get("step_label"),
        substep_id=raw.get("substep_id"),
        substep_label=raw.get("substep_label"),
        failure_code=raw.get("failure_code"),
        failure_message=raw.get("failure_message"),
        failure_details=raw.get("failure_details"),
        failure_limit=raw.get("failure_limit"),
        failure_step_id=raw.get("failure_step_id"),
        failure_step_index=raw.get("failure_step_index"),
        exit_code=raw.get("exit_code"),
        started_at=str(raw.get("started_at") or ""),
        updated_at=str(raw.get("updated_at") or ""),
        metadata=meta,
    )


class ProjectorBatchBridge:
    """Persisted projector state for entrypoint feed-line calls."""

    def __init__(self, state_file: Path, app_root: Path) -> None:
        self.state_file = state_file
        self.app_root = app_root
        self.registry = load_registry(app_root / "pipeline" / "registry.yaml")
        self.last_emitted_step_index: int | None = None
        self.projector = ProgressProjector(
            registry=self.registry,
            run_id=os.environ.get("RUN_ID", ""),
            dataset=os.environ.get("DATASET", ""),
            metadata={
                "batch_job_name": os.environ.get("BATCH_JOB_NAME", ""),
                "vm_type": os.environ.get("VM_TYPE", ""),
            },
        )
        if state_file.is_file():
            self._load()

    def _load(self) -> None:
        with self.state_file.open(encoding="utf-8") as f:
            raw = json.load(f)
        self.last_emitted_step_index = raw.get("last_emitted_step_index")
        ps = raw.get("projector_state")
        if isinstance(ps, dict):
            self.projector._state = _state_from_dict(ps)

    def save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_emitted_step_index": self.last_emitted_step_index,
            "projector_state": asdict(self.projector.state),
        }
        with self.state_file.open("w", encoding="utf-8") as f:
            json.dump(payload, f)

    def _step_from_legacy_token(self, legacy_step: str | None):
        if not legacy_step:
            return None
        step_id = resolve_step_id_from_legacy_token(self.registry, legacy_step)
        if not step_id:
            return None
        return step_by_id(self.registry, step_id)

    def _build_gcs_payload(
        self,
        *,
        status: str | None = None,
        failure_code: str | None = None,
        failure_message: str | None = None,
        failure_details: Any = None,
        artifacts_ready: bool | None = None,
        legacy_step: str | None = None,
        legacy_step_label: str | None = None,
    ) -> dict[str, Any]:
        state = self.projector.state
        step = self._step_from_legacy_token(legacy_step)

        if status == "starting":
            self.projector.mark_starting(
                step_label=legacy_step_label or state.step_label or "Starting"
            )
        elif status in ("succeeded", "failed", "cancelled"):
            if failure_code or failure_message or failure_details is not None:
                apply_failure(
                    state,
                    failure_code=failure_code or state.failure_code or "RUN_FAILED",
                    message=failure_message or state.failure_message or "",
                    step=step,
                    details=failure_details if failure_details is not None else state.failure_details,
                )
            exit_code = state.exit_code if state.exit_code is not None else (
                0 if status == "succeeded" else 1
            )
            apply_run_finished(
                state,
                status=status,
                exit_code=exit_code,
                failure_code=failure_code or state.failure_code,
                message=failure_message if failure_message is not None else state.failure_message,
                step=step,
            )
        elif failure_code is not None or failure_message is not None:
            apply_failure(
                state,
                failure_code=failure_code or "",
                message=failure_message or "",
                step=step,
                details=failure_details,
            )

        data = build_status_projection(state, self.registry)
        data["attempt"] = int(os.environ.get("BATCH_TASK_ATTEMPT", "0") or "0")

        if legacy_step is not None:
            data["step"] = legacy_step
        if legacy_step_label is not None:
            data["step_label"] = legacy_step_label

        if artifacts_ready is not None:
            data["artifacts_ready"] = artifacts_ready

        return data

    def upload(
        self,
        *,
        status: str | None = None,
        failure_code: str | None = None,
        failure_message: str | None = None,
        failure_details: Any = None,
        artifacts_ready: bool | None = None,
        legacy_step: str | None = None,
        legacy_step_label: str | None = None,
    ) -> None:
        data = self._build_gcs_payload(
            status=status,
            failure_code=failure_code,
            failure_message=failure_message,
            failure_details=failure_details,
            artifacts_ready=artifacts_ready,
            legacy_step=legacy_step,
            legacy_step_label=legacy_step_label,
        )
        _upload_status(data)
        self.save()

    def mark_starting(self, label: str) -> None:
        self.projector.mark_starting(step_label=label)
        if not self.projector.state.started_at:
            started = os.environ.get("STARTED_AT") or _utc_now_iso()
            self.projector.state.started_at = started
        self.upload(status="starting", legacy_step="0", legacy_step_label=label)

    def feed_line(self, line: str) -> None:
        stripped = line.strip()
        if not stripped:
            return
        self.projector.feed_line(stripped)
        idx = self.projector.state.step_index
        if idx is not None and idx != self.last_emitted_step_index:
            self.upload(status="running")
            self.last_emitted_step_index = idx

    def write_explicit(
        self,
        *,
        legacy_step: str,
        step_label: str,
        status: str,
        failure_code: str = "",
        failure_message: str = "",
        failure_details: Any = None,
        artifacts_ready: bool | None = None,
    ) -> None:
        step_id = resolve_step_id_from_legacy_token(self.registry, legacy_step)
        if step_id:
            step = step_by_id(self.registry, step_id)
            if step is not None:
                apply_step(self.projector.state, step)
        details = failure_details
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except json.JSONDecodeError:
                details = None
        self.upload(
            status=status,
            failure_code=failure_code or None,
            failure_message=failure_message or None,
            failure_details=details,
            artifacts_ready=artifacts_ready,
            legacy_step=legacy_step,
            legacy_step_label=step_label,
        )

    def failure_snapshot(self) -> dict[str, Any]:
        s = self.projector.state
        out: dict[str, Any] = {}
        if s.failure_code:
            out["code"] = s.failure_code
            out["message"] = s.failure_message or ""
            out["step"] = s.failure_step_index
            if s.failure_details is not None:
                out["details"] = s.failure_details
            if s.failure_step_id:
                out["step_id"] = s.failure_step_id
        return out


def _parse_failure_details(raw: str | None) -> Any:
    if not raw or raw == "null":
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def cmd_init(args: argparse.Namespace) -> int:
    bridge = ProjectorBatchBridge(Path(args.state_file), Path(args.app_root))
    if args.started_at:
        bridge.projector.state.started_at = args.started_at
    bridge.save()
    return 0


def cmd_write_starting(args: argparse.Namespace) -> int:
    bridge = ProjectorBatchBridge(Path(args.state_file), Path(args.app_root))
    bridge.mark_starting(args.label)
    return 0


def cmd_feed_line(args: argparse.Namespace) -> int:
    bridge = ProjectorBatchBridge(Path(args.state_file), Path(args.app_root))
    bridge.feed_line(args.line)
    return 0


def cmd_read_failure(args: argparse.Namespace) -> int:
    """Print code, message, details JSON (tab-separated) for entrypoint.sh."""
    bridge = ProjectorBatchBridge(Path(args.state_file), Path(args.app_root))
    snap = bridge.failure_snapshot()
    code = snap.get("code", "")
    message = snap.get("message", "")
    details = snap.get("details")
    details_s = json.dumps(details) if details is not None else "null"
    print(f"{code}\t{message}\t{details_s}")
    return 0


def cmd_write_explicit(args: argparse.Namespace) -> int:
    bridge = ProjectorBatchBridge(Path(args.state_file), Path(args.app_root))
    ar: bool | None = None
    if args.artifacts_ready == "true":
        ar = True
    elif args.artifacts_ready == "false":
        ar = False
    bridge.write_explicit(
        legacy_step=args.step,
        step_label=args.step_label,
        status=args.status,
        failure_code=args.failure_code or "",
        failure_message=args.failure_message or "",
        failure_details=_parse_failure_details(args.failure_details_json),
        artifacts_ready=ar,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Batch GCS status.json via ProgressProjector")
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--app-root", default=str(_app_root()))
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init")
    p_init.add_argument("--started-at", default="")
    p_init.set_defaults(func=cmd_init)

    p_start = sub.add_parser("write-starting")
    p_start.add_argument("--label", required=True)
    p_start.set_defaults(func=cmd_write_starting)

    p_feed = sub.add_parser("feed-line")
    p_feed.add_argument("--line", required=True)
    p_feed.set_defaults(func=cmd_feed_line)

    p_write = sub.add_parser("write-explicit")
    p_write.add_argument("--step", required=True)
    p_write.add_argument("--step-label", required=True)
    p_write.add_argument("--status", required=True)
    p_write.add_argument("--failure-code", default="")
    p_write.add_argument("--failure-message", default="")
    p_write.add_argument("--failure-details-json", default="null")
    p_write.add_argument("--artifacts-ready", default="", choices=["", "true", "false"])
    p_write.set_defaults(func=cmd_write_explicit)

    p_fail = sub.add_parser("read-failure")
    p_fail.set_defaults(func=cmd_read_failure)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:
        print(f"[projector_status] error: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
