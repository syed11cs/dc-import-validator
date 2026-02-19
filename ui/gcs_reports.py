"""Upload and serve validation reports from Google Cloud Storage.

When GCS_REPORTS_BUCKET is set, reports are uploaded after a successful run so any
Cloud Run instance can serve them. Local files are
still written by the pipeline; GCS is an additional copy.

Paths in GCS: gs://bucket/reports/{run_id}/{dataset}/
  - validation_report.html, summary_report.html (served)
  - validation_output.json, report.json, schema_review.json (stored for debug/audit/re-render)
  - input.csv (when present, for rule-failure enrichment when serving from GCS)
"""

import json
import os
from pathlib import Path


def _get_bucket():
    """Return the GCS bucket name from env, or None if not configured."""
    name = (os.environ.get("GCS_REPORTS_BUCKET") or "").strip()
    return name if name else None


def upload_reports_to_gcs(
    output_dir: Path,
    run_id: str,
    dataset: str,
) -> bool:
    """Upload report HTML and JSON artifacts to GCS. Returns True if upload ran (and at least one file uploaded)."""
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return False
    try:
        from google.cloud import storage
    except ImportError:
        return False

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"reports/{run_id}/{dataset}"
    uploaded = 0

    # HTML reports (served to users)
    for filename in ("validation_report.html", "summary_report.html"):
        path = output_dir / filename
        if not path.exists():
            continue
        blob = bucket.blob(f"{prefix}/{filename}")
        blob.upload_from_filename(str(path), content_type="text/html")
        uploaded += 1

    # JSON artifacts (for debug, audit, re-render, future API)
    for filename in ("validation_output.json", "report.json", "schema_review.json"):
        path = output_dir / filename
        if not path.exists():
            continue
        blob = bucket.blob(f"{prefix}/{filename}")
        blob.upload_from_filename(str(path), content_type="application/json")
        uploaded += 1

    # Input CSV (for rule-failure enrichment when serving from GCS: Location, Date, Source row)
    try:
        report_path = output_dir / "report.json"
        if report_path.exists():
            with open(report_path, encoding="utf-8") as f:
                report = json.load(f)
            for p in (report.get("commandArgs") or {}).get("inputFiles") or []:
                if str(p).lower().endswith(".csv"):
                    csv_path = Path(p)
                    if csv_path.exists():
                        blob = bucket.blob(f"{prefix}/input.csv")
                        blob.upload_from_filename(str(csv_path), content_type="text/csv")
                        uploaded += 1
                        break
    except (json.JSONDecodeError, OSError, TypeError):
        pass

    return uploaded > 0


def get_report_from_gcs(run_id: str, dataset: str, filename: str) -> bytes | None:
    """Read a report file from GCS. Returns content or None if not found or GCS not configured."""
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return None
    try:
        from google.cloud import storage
    except ImportError:
        return None

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"reports/{run_id}/{dataset}/{filename}")
    if not blob.exists():
        return None
    return blob.download_as_bytes()


def get_report_updated_from_gcs(run_id: str, dataset: str, filename: str) -> float | None:
    """Return last-modified timestamp (Unix) for a report file in GCS, or None."""
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return None
    try:
        from google.cloud import storage
    except ImportError:
        return None
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(f"reports/{run_id}/{dataset}/{filename}")
    if not blob or not blob.updated:
        return None
    return blob.updated.timestamp()
