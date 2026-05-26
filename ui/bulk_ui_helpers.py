"""Small helpers for bulk GCS UI validation (mirrors client-side rules in index.html)."""

from __future__ import annotations


def validate_bulk_gcs_root(value: str) -> str | None:
    """Return an error message if invalid, else None."""
    path = (value or "").strip()
    if not path:
        return "Root GCS folder is required."
    if not path.startswith("gs://"):
        return "Path must start with gs://."
    if len(path) <= len("gs://"):
        return "Enter a bucket and folder (e.g. gs://my-bucket/imports/)."
    return None


def bulk_folder_display_name(folder_prefix: str) -> str:
    """Last path segment of a GCS folder prefix for display."""
    p = (folder_prefix or "").rstrip("/")
    if not p:
        return folder_prefix or ""
    return p.split("/")[-1] or p


def bulk_outcome_title(outcome_code: str) -> str:
    """Short heading for bulk dashboard empty / failure states."""
    titles = {
        "gcs_not_found": "GCS path not found",
        "gcs_access_denied": "Access denied",
        "gcs_invalid_path": "Invalid GCS path",
        "gcs_error": "Discovery failed",
        "empty_root": "No dataset folders found",
        "no_runnable": "No runnable datasets",
        "submit_failed": "Submission failed",
    }
    return titles.get(outcome_code, "Bulk discovery issue")


def bulk_response_outcome(data: dict) -> dict[str, str] | None:
    """Classify a POST /api/bulk-runs JSON body when no jobs were submitted.

    Returns None when the dashboard should stay in normal operational mode.
    """
    submitted = int(data.get("submitted") or 0)
    if submitted > 0:
        return None
    code = (data.get("outcome") or "").strip()
    message = (data.get("outcome_message") or "").strip()
    if code and message:
        return {
            "code": code,
            "title": bulk_outcome_title(code),
            "message": message,
            "severity": "warning" if code in ("no_runnable", "submit_failed") else "error",
        }
    discovered = int(data.get("datasets_found") or 0)
    skipped = len(data.get("skipped_folders") or [])
    runs = data.get("runs") or []
    if discovered == 0 and skipped == 0:
        return {
            "code": "empty_root",
            "title": bulk_outcome_title("empty_root"),
            "message": "No dataset folders found under the provided GCS path.",
            "severity": "error",
        }
    if not runs and skipped > 0:
        return {
            "code": "no_runnable",
            "title": bulk_outcome_title("no_runnable"),
            "message": "No runnable dataset folders discovered. Every folder was skipped.",
            "severity": "warning",
        }
    if runs:
        return {
            "code": "submit_failed",
            "title": bulk_outcome_title("submit_failed"),
            "message": "No jobs were submitted. Review per-dataset errors below.",
            "severity": "warning",
        }
    return {
        "code": "no_runnable",
        "title": bulk_outcome_title("no_runnable"),
        "message": "No runnable dataset folders discovered.",
        "severity": "warning",
    }


def bulk_run_stats_summary(data: dict) -> dict[str, int]:
    """Extract discovered / valid / skipped counts from POST /api/bulk-runs response."""
    discovered = int(data.get("datasets_found") or 0)
    submitted = int(data.get("submitted") or 0)
    skipped = int(data.get("skipped") or len(data.get("skipped_folders") or []))
    return {
        "discovered": discovered,
        "valid_runs": submitted,
        "runnable": submitted,
        "skipped": skipped,
    }
