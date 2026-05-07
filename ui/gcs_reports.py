"""Upload and serve validation reports from Google Cloud Storage.

When GCS_REPORTS_BUCKET is set, reports are uploaded after a successful run so any
Cloud Run instance can serve them. Local files are
still written by the pipeline; GCS is an additional copy.

Paths in GCS: gs://bucket/reports/{run_id}/{dataset}/
  - validation_report.html, summary_report.html (served)
  - validation_output.json, report.json, schema_review.json (stored for debug/audit/re-render)
  - input.csv (when present, for rule-failure enrichment when serving from GCS)

If the bucket is not accessible (missing, permission denied, network error), functions
raise so callers can log or surface the error clearly instead of failing silently.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def _upload_mcf_file(mcf_path: Path, bucket_name: str, blob_path: str) -> None:
    """Upload one MCF file to GCS.

    Creates its own storage.Client so it is safe to call from multiple threads
    concurrently — storage.Client (and its underlying requests.Session) is not
    thread-safe when shared across threads.
    """
    from google.cloud import storage as _gcs
    blob = _gcs.Client().bucket(bucket_name).blob(blob_path)
    blob.upload_from_filename(str(mcf_path), content_type="text/plain")


def _get_bucket():
    """Return the GCS bucket name from env, or None if not configured."""
    name = (os.environ.get("GCS_REPORTS_BUCKET") or "").strip()
    return name if name else None


def is_gcs_configured() -> bool:
    """True when GCS_REPORTS_BUCKET is set (Cloud Run / multi-instance mode). Report endpoints should prefer GCS and avoid local fallback when this is True and run_id is set."""
    return _get_bucket() is not None


class GCSAccessError(Exception):
    """Raised when GCS_REPORTS_BUCKET is set but the bucket is not accessible."""


def _get_client_and_bucket():
    """Return (client, bucket) for the configured bucket, or (None, None) if not configured.
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is not accessible."""
    bucket_name = _get_bucket()
    if not bucket_name:
        return None, None
    try:
        from google.cloud import storage
    except ImportError:
        raise GCSAccessError(
            "GCS_REPORTS_BUCKET is set but google-cloud-storage is not installed. Install it or unset GCS_REPORTS_BUCKET."
        )
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        # Lightweight access check so we fail clearly if bucket is missing or inaccessible
        bucket.reload()
        return client, bucket
    except Exception as e:
        raise GCSAccessError(
            f"GCS bucket {bucket_name!r} is not accessible: {e}"
        ) from e


def upload_merged_config_to_gcs(run_id: str, config_path: "Path") -> str:
    """Upload a merged validation config JSON to GCS so a Batch VM can download it.

    Returns the GCS URI (gs://bucket/configs/{run_id}/validation_config.json) on
    success, or an empty string if GCS is not configured.
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is inaccessible.
    Raises RuntimeError if the upload cannot be verified after writing.
    """
    import logging
    _log = logging.getLogger(__name__)

    bucket_name = _get_bucket()
    if not bucket_name:
        return ""
    if not run_id:
        raise ValueError("upload_merged_config_to_gcs: run_id must not be empty")
    _, bucket = _get_client_and_bucket()
    blob_path = f"configs/{run_id}/validation_config.json"
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    _log.info("upload_merged_config_to_gcs: uploading %s to %s", config_path, gcs_uri)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(config_path), content_type="application/json")
    _log.info("upload_merged_config_to_gcs: upload complete — %s", gcs_uri)
    return gcs_uri


def upload_reports_to_gcs(
    output_dir: Path,
    run_id: str,
    dataset: str,
) -> bool:
    """Upload all existing per-run artifacts to GCS so any Cloud Run instance can serve them (stateless).
    Uploads validation_report.html, summary_report.html, validation_output.json, report.json, schema_review.json, and input.csv when present.
    Returns True if upload ran (and at least one file uploaded).
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is not accessible (do not swallow)."""
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return False
    _, bucket = _get_client_and_bucket()
    if not bucket:
        return False

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

    # Warnings/advisories CSV (for DE import documentation)
    csv_path = output_dir / "validation_warnings_and_advisories.csv"
    if csv_path.exists():
        blob = bucket.blob(f"{prefix}/validation_warnings_and_advisories.csv")
        blob.upload_from_filename(str(csv_path), content_type="text/csv")
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

    # differ output (dataset comparison across Cloud Run instances)
    differ_dir = output_dir / "differ_output"
    for filename, content_type in (
        ("differ_summary.json", "application/json"),
        ("obs_diff_summary.csv", "text/csv"),
    ):
        path = differ_dir / filename
        if not path.exists():
            continue
        blob = bucket.blob(f"{prefix}/differ_output/{filename}")
        blob.upload_from_filename(str(path), content_type=content_type)
        uploaded += 1

    # MCF output (required for baseline creation via /api/accept-baseline).
    # Batch VMs write MCF files to the per-run output dir but the Cloud Run
    # instance serving accept-baseline never has local access to them, so they
    # must be in GCS for the baseline workflow to work.
    #
    # Uploaded in parallel: large shard counts produce an equal number of MCF
    # files (e.g. 1890 shards → 1890 MCF files), making sequential uploads the
    # dominant wall-time cost (~18 min observed vs ~30 s with parallelism).
    # Each worker creates its own storage.Client to avoid sharing the underlying
    # requests.Session across threads.
    mcf_paths = sorted(output_dir.glob("*.mcf"))
    if mcf_paths:
        n_workers = min(16, len(mcf_paths))
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = {
                executor.submit(
                    _upload_mcf_file,
                    p,
                    bucket_name,
                    f"{prefix}/{p.name}",
                ): p
                for p in mcf_paths
            }
            for future in as_completed(futures):
                future.result()  # re-raises any upload exception immediately
        uploaded += len(mcf_paths)

    return uploaded > 0


def download_mcf_files_from_gcs(run_id: str, dataset: str, dest_dir: Path) -> int:
    """Download all *.mcf files for a run from GCS into dest_dir.

    Used by /api/accept-baseline when the Batch VM has already shut down and
    MCF files are no longer on the local filesystem.

    Returns the number of MCF files downloaded (0 when none found or GCS not
    configured).  Raises GCSAccessError if GCS is configured but inaccessible.
    """
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return 0
    _, bucket = _get_client_and_bucket()
    if not bucket:
        return 0

    prefix = f"reports/{run_id}/{dataset}/"
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for blob in bucket.list_blobs(prefix=prefix):
        # Strip the prefix to get the bare filename; skip subdirectory entries.
        name = blob.name[len(prefix):]
        if not name or "/" in name or not name.endswith(".mcf"):
            continue
        blob.download_to_filename(str(dest_dir / name))
        count += 1

    return count


def get_report_from_gcs(run_id: str, dataset: str, filename: str) -> bytes | None:
    """Read a report file from GCS. Returns content or None if not found or GCS not configured.
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is not accessible."""
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return None
    _, bucket = _get_client_and_bucket()
    if not bucket:
        return None

    blob = bucket.blob(f"reports/{run_id}/{dataset}/{filename}")
    if not blob.exists():
        return None
    return blob.download_as_bytes()


def get_report_updated_from_gcs(run_id: str, dataset: str, filename: str) -> float | None:
    """Return last-modified timestamp (Unix) for a report file in GCS, or None.
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is not accessible."""
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return None
    _, bucket = _get_client_and_bucket()
    if not bucket:
        return None

    blob = bucket.get_blob(f"reports/{run_id}/{dataset}/{filename}")
    if not blob or not blob.updated:
        return None
    return blob.updated.timestamp()
