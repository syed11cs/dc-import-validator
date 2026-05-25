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
