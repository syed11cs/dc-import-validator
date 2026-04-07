"""Unified job status reader for Cloud Batch validation runs.

Primary source: GCS status.json written by batch/entrypoint.sh.
Fallback: Cloud Batch API (via batch_runner.get_batch_state) used when the
GCS file shows "running" but has not been updated in over 5 minutes — which
indicates the VM died or was preempted without writing a final status update.

Status JSON schema (written by batch/entrypoint.sh write_status()):
    {
        "run_id":          str,
        "batch_job_name":  str,
        "dataset":         str,
        "vm_type":         str,
        "step":            str,       # "0", "1", "2", "2.4", "3", "4"
        "step_label":      str,
        "status":          str,       # "running" | "succeeded" | "failed"
        "started_at":      str,       # ISO-8601 UTC
        "updated_at":      str,       # ISO-8601 UTC
        "failure_code":    str | null,
        "failure_message": str | null
    }

This module has no FastAPI dependency so it can be imported from tests.
"""

import json
import os
from datetime import datetime, timezone
from typing import Optional

from ui.app_logging import get_logger

logger = get_logger(__name__)

# How long a "running" status may go without an update before we probe the
# Batch API to detect a silent terminal state.
_STALE_THRESHOLD_SECONDS = 5 * 60  # 5 minutes

# Batch API states that map to a terminal "succeeded" status.
_BATCH_SUCCEEDED_STATES = {"SUCCEEDED"}

# Batch API states that map to a terminal "failed" status.
_BATCH_FAILED_STATES = {"FAILED", "DELETION_IN_PROGRESS", "UNKNOWN"}


def _gcs_client():
    """Return a google.cloud.storage.Client instance."""
    from google.cloud import storage  # lazy import — not needed for unit tests
    return storage.Client()


def _status_blob_path(run_id: str) -> str:
    return f"jobs/{run_id}/status.json"


def _parse_utc(ts: str) -> Optional[datetime]:
    """Parse an ISO-8601 UTC timestamp string into an aware datetime."""
    if not ts:
        return None
    try:
        # entrypoint.sh writes strftime('%Y-%m-%dT%H:%M:%SZ')
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            # Fallback: fromisoformat handles '+00:00' suffix
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            logger.warning("Cannot parse timestamp: %r", ts)
            return None


def _seconds_since(ts: Optional[datetime]) -> float:
    """Return seconds elapsed since ts. Returns infinity if ts is None."""
    if ts is None:
        return float("inf")
    return (datetime.now(timezone.utc) - ts).total_seconds()


def _fetch_status_json(bucket_name: str, run_id: str) -> Optional[dict]:
    """Download and parse status.json from GCS.

    Returns None if the blob does not exist. Propagates other GCS errors.
    """
    from google.cloud.exceptions import NotFound

    client = _gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(_status_blob_path(run_id))
    try:
        raw = blob.download_as_text()
    except NotFound:
        return None
    return json.loads(raw)


def _apply_batch_fallback(status: dict, batch_state: str) -> dict:
    """Return an updated copy of status with the Batch-derived terminal state applied."""
    updated = dict(status)
    if batch_state in _BATCH_SUCCEEDED_STATES:
        logger.info(
            "run_id=%s: GCS status stale; Batch reports SUCCEEDED — marking succeeded",
            status.get("run_id"),
        )
        updated["status"] = "succeeded"
        updated["step_label"] = "Completed"
    elif batch_state in _BATCH_FAILED_STATES:
        logger.info(
            "run_id=%s: GCS status stale; Batch reports %s — marking failed",
            status.get("run_id"), batch_state,
        )
        updated["status"] = "failed"
        if not updated.get("failure_code"):
            updated["failure_code"] = "BATCH_JOB_LOST"
        if not updated.get("failure_message"):
            updated["failure_message"] = (
                f"The Batch VM stopped reporting progress (Batch state: {batch_state}). "
                "The VM may have been preempted or terminated unexpectedly."
            )
    # If Batch still reports QUEUED/RUNNING/SCHEDULED we leave the GCS status
    # as-is — the VM is alive but just slow to write status updates.
    return updated


def get_job_status(
    run_id: str,
    bucket_name: Optional[str] = None,
    *,
    stale_threshold_seconds: float = _STALE_THRESHOLD_SECONDS,
) -> Optional[dict]:
    """Return the status dict for a validation run, or None if not found yet.

    Args:
        run_id:                   The run identifier.
        bucket_name:              GCS bucket. Defaults to GCS_REPORTS_BUCKET env var.
        stale_threshold_seconds:  Override the staleness threshold (useful in tests).

    Returns:
        The status dict (keys: run_id, status, step, step_label, started_at,
        updated_at, failure_code, failure_message, batch_job_name, vm_type,
        dataset), or None if the GCS file does not exist yet.

    Raises:
        ValueError: if bucket_name is not provided and GCS_REPORTS_BUCKET is unset.
        google.cloud.exceptions.*: on unexpected GCS errors.
    """
    if bucket_name is None:
        bucket_name = os.environ.get("GCS_REPORTS_BUCKET", "")
    if not bucket_name:
        raise ValueError("bucket_name or GCS_REPORTS_BUCKET env var is required")

    status = _fetch_status_json(bucket_name, run_id)
    if status is None:
        logger.debug("run_id=%s: status.json not found in GCS yet", run_id)
        return None

    # Fast path: terminal states need no further checks.
    if status.get("status") in ("succeeded", "failed"):
        return status

    # The status is "running" — check whether it has gone stale.
    updated_at = _parse_utc(status.get("updated_at", ""))
    age = _seconds_since(updated_at)

    if age < stale_threshold_seconds:
        # Recent heartbeat — trust GCS.
        return status

    # Stale running status: probe Cloud Batch API to detect silent VM death.
    job_name = status.get("batch_job_name", "")
    if not job_name:
        logger.warning(
            "run_id=%s: status is stale (%.0fs) but batch_job_name is empty — cannot probe Batch API",
            run_id, age,
        )
        return status

    logger.info(
        "run_id=%s: status.json is stale (%.0fs > %.0fs threshold); probing Batch API for job %s",
        run_id, age, stale_threshold_seconds, job_name,
    )
    try:
        from ui.services.batch_runner import get_batch_state
        batch_state = get_batch_state(job_name)
    except Exception as exc:
        logger.warning("run_id=%s: Batch API probe failed: %s", run_id, exc)
        return status

    return _apply_batch_fallback(status, batch_state)
