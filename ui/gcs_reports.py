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
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_log = logging.getLogger(__name__)


def _upload_mcf_file(mcf_path: Path, bucket_name: str, blob_path: str) -> None:
    """Upload one MCF file to GCS.

    Creates its own storage.Client so it is safe to call from multiple threads
    concurrently — storage.Client (and its underlying requests.Session) is not
    thread-safe when shared across threads.
    """
    from google.cloud import storage as _gcs
    blob = _gcs.Client().bucket(bucket_name).blob(blob_path)
    blob.upload_from_filename(str(mcf_path), content_type="text/plain")


def _upload_one_file(
    src_path: Path,
    bucket_name: str,
    blob_path: str,
    content_type: str,
) -> None:
    """Upload a single file to GCS with its own storage.Client (thread-safe)."""
    from google.cloud import storage as _gcs
    blob = _gcs.Client().bucket(bucket_name).blob(blob_path)
    blob.upload_from_filename(str(src_path), content_type=content_type)


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


def upload_critical_reports_to_gcs(
    output_dir: Path,
    run_id: str,
    dataset: str,
) -> int:
    """Upload UI-critical artifacts to GCS in parallel.

    Covers files the user needs immediately after the pipeline finishes:
      - validation_report.html, summary_report.html (served directly)
      - validation_output.json, report.json, schema_review.json (API / re-render)
      - differ_output/differ_summary.json, differ_output/obs_diff_summary.csv

    Returns the number of files uploaded (0 if GCS not configured or nothing found).
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is not accessible.
    """
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return 0
    # Lightweight bucket access check (raises GCSAccessError on failure).
    _get_client_and_bucket()

    prefix = f"reports/{run_id}/{dataset}"

    candidates: list[tuple[Path, str, str]] = []  # (src_path, blob_path, content_type)

    for filename in ("validation_report.html", "summary_report.html"):
        path = output_dir / filename
        if path.exists():
            candidates.append((path, f"{prefix}/{filename}", "text/html"))

    for filename in ("validation_output.json", "report.json", "schema_review.json"):
        path = output_dir / filename
        if path.exists():
            candidates.append((path, f"{prefix}/{filename}", "application/json"))

    differ_dir = output_dir / "differ_output"
    for filename, ct in (
        ("differ_summary.json", "application/json"),
        ("obs_diff_summary.csv", "text/csv"),
    ):
        path = differ_dir / filename
        if path.exists():
            candidates.append((path, f"{prefix}/differ_output/{filename}", ct))

    if not candidates:
        _log.info("upload_critical_reports: no files found in %s [run_id=%s]", output_dir, run_id)
        return 0

    t0 = time.monotonic()
    n_workers = min(8, len(candidates))
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = [
            executor.submit(_upload_one_file, src, bucket_name, blob_path, ct)
            for src, blob_path, ct in candidates
        ]
        for future in as_completed(futures):
            future.result()  # re-raises any upload exception immediately

    elapsed = time.monotonic() - t0
    _log.info(
        "[upload] Phase 1 (critical): %d files in %.1fs (%.1f files/s) [run_id=%s]",
        len(candidates), elapsed, len(candidates) / max(elapsed, 0.001), run_id,
    )
    return len(candidates)


def upload_deferred_artifacts_to_gcs(
    output_dir: Path,
    run_id: str,
    dataset: str,
) -> int:
    """Upload deferred (non-UI-critical) artifacts to GCS.

    Covers artifacts needed for the baseline workflow and post-run debugging,
    but not required for the user to see their validation results:
      - validation_warnings_and_advisories.csv
      - input.csv (from report.json commandArgs, for rule-failure enrichment)
      - genmcf_profile.jfr and gc.log (JFR/GC profiling artifacts, if present)
      - *.mcf files (required for /api/accept-baseline)

    MCF files are uploaded in parallel (1890+ files possible for large shard counts).
    Returns the number of files uploaded.
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is not accessible.
    """
    bucket_name = _get_bucket()
    if not bucket_name or not run_id or not dataset:
        return 0
    _get_client_and_bucket()

    prefix = f"reports/{run_id}/{dataset}"
    uploaded = 0

    t0 = time.monotonic()

    # Warnings/advisories CSV
    csv_path = output_dir / "validation_warnings_and_advisories.csv"
    if csv_path.exists():
        _upload_one_file(csv_path, bucket_name, f"{prefix}/validation_warnings_and_advisories.csv", "text/csv")
        uploaded += 1

    # Input CSV (for rule-failure enrichment when serving from GCS)
    try:
        report_path = output_dir / "report.json"
        if report_path.exists():
            with open(report_path, encoding="utf-8") as f:
                report = json.load(f)
            for p in (report.get("commandArgs") or {}).get("inputFiles") or []:
                if str(p).lower().endswith(".csv"):
                    input_csv = Path(p)
                    if input_csv.exists():
                        _upload_one_file(input_csv, bucket_name, f"{prefix}/input.csv", "text/csv")
                        uploaded += 1
                        break
    except (json.JSONDecodeError, OSError, TypeError):
        pass

    # JFR profiling artifacts (genmcf_profile.jfr, gc.log) — may be in a genmcf subdir.
    for jfr_filename in ("genmcf_profile.jfr", "gc.log"):
        # genmcf writes into a subdirectory (e.g. output_dir/genmcf_output/) — search one level.
        for candidate in list(output_dir.glob(f"*/{jfr_filename}")) + [output_dir / jfr_filename]:
            if candidate.exists():
                ct = "application/octet-stream" if jfr_filename.endswith(".jfr") else "text/plain"
                _upload_one_file(candidate, bucket_name, f"{prefix}/{jfr_filename}", ct)
                uploaded += 1
                break  # upload only the first match

    # MCF output (required for baseline creation via /api/accept-baseline).
    # Uploaded in parallel: large shard counts produce an equal number of MCF
    # files (e.g. 1890 shards → 1890 MCF files), making sequential uploads the
    # dominant wall-time cost (~18 min observed vs ~30 s with parallelism).
    mcf_t0 = time.monotonic()
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
        mcf_elapsed = time.monotonic() - mcf_t0
        _log.info(
            "[upload] MCF: %d files in %.1fs (%.1f files/s, workers=%d) [run_id=%s]",
            len(mcf_paths), mcf_elapsed, len(mcf_paths) / max(mcf_elapsed, 0.001),
            n_workers, run_id,
        )
        uploaded += len(mcf_paths)

    total_elapsed = time.monotonic() - t0
    _log.info(
        "[upload] Phase 2 (deferred): %d files in %.1fs [run_id=%s]",
        uploaded, total_elapsed, run_id,
    )
    return uploaded


def upload_reports_to_gcs(
    output_dir: Path,
    run_id: str,
    dataset: str,
) -> bool:
    """Upload all per-run artifacts to GCS (backward-compatible wrapper).

    Calls upload_critical_reports_to_gcs followed by upload_deferred_artifacts_to_gcs.
    Use the individual functions directly when two-phase upload ordering matters
    (e.g. write_status between phases in entrypoint.sh).

    Returns True if at least one file was uploaded.
    Raises GCSAccessError if GCS_REPORTS_BUCKET is set but the bucket is not accessible.
    """
    critical = upload_critical_reports_to_gcs(output_dir, run_id, dataset)
    deferred = upload_deferred_artifacts_to_gcs(output_dir, run_id, dataset)
    return (critical + deferred) > 0


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
