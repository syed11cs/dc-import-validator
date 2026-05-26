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


# Discovery succeeded but nothing to run — not a submission/system failure.
BULK_INFORMATIONAL_OUTCOMES = frozenset({"empty_root", "no_runnable"})


def bulk_outcome_severity(outcome_code: str) -> str:
    """UI severity: error (red), warning (amber), info (neutral empty discovery)."""
    if outcome_code in BULK_INFORMATIONAL_OUTCOMES:
        return "info"
    if outcome_code == "submit_failed":
        return "warning"
    return "error"


def bulk_outcome_title(outcome_code: str) -> str:
    """Short heading for bulk dashboard empty / failure states."""
    titles = {
        "gcs_not_found": "GCS path not found",
        "gcs_access_denied": "Access denied",
        "gcs_invalid_path": "Invalid GCS path",
        "gcs_error": "Discovery failed",
        "empty_root": "Bulk discovery completed",
        "no_runnable": "Bulk discovery completed",
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
            "severity": bulk_outcome_severity(code),
        }
    discovered = int(data.get("datasets_found") or 0)
    skipped = len(data.get("skipped_folders") or [])
    runs = data.get("runs") or []
    if discovered == 0 and skipped == 0:
        return {
            "code": "empty_root",
            "title": bulk_outcome_title("empty_root"),
            "message": "No dataset folders found under this path.",
            "severity": bulk_outcome_severity("empty_root"),
        }
    if not runs and skipped > 0:
        return {
            "code": "no_runnable",
            "title": bulk_outcome_title("no_runnable"),
            "message": "No runnable dataset folders. Every folder was skipped — see list below.",
            "severity": bulk_outcome_severity("no_runnable"),
        }
    if runs:
        return {
            "code": "submit_failed",
            "title": bulk_outcome_title("submit_failed"),
            "message": "No jobs were submitted. Review per-dataset errors below.",
            "severity": bulk_outcome_severity("submit_failed"),
        }
    return {
        "code": "no_runnable",
        "title": bulk_outcome_title("no_runnable"),
        "message": "No runnable dataset folders under this path.",
        "severity": bulk_outcome_severity("no_runnable"),
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
