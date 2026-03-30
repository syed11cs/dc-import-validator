"""GCS-backed upload sessions for large files on Cloud Run.

When GCS_REPORTS_BUCKET is set, the browser can upload files directly to GCS
using V4 signed PUT URLs, bypassing Cloud Run's 32 MB HTTP request limit.

Session structure in GCS:
    sessions/{session_id}/{filename}       — uploaded files
    sessions/{session_id}/_manifest.json  — lists files + their roles

Typical flow:
    1. POST /api/prepare-upload  → create_upload_session() → signed PUT URLs
    2. Browser PUTs each file directly to GCS (bypasses Cloud Run entirely)
    3. POST /api/run/custom/stream { session_id: ... }
    4. Server calls download_session_to_dir() to fetch files to local disk
    5. Existing pipeline runs against the local copies
    6. delete_session() removes session files from GCS (called in finally block)

CORS requirement:
    The GCS bucket must allow PUT from the UI origin.  Configure once with:
        gsutil cors set cors.json gs://<bucket>
    where cors.json:
        [{"origin":["*"],"method":["PUT"],"responseHeader":["Content-Type"],"maxAgeSeconds":3600}]
    or use the GCP Console → Bucket → Permissions → CORS.

IAM requirement on Cloud Run:
    The Cloud Run service account needs the following IAM role on the bucket:
        roles/storage.objectAdmin  (to write, read, and delete objects)
    For signed URL generation it also needs:
        roles/iam.serviceAccountTokenCreator  (granted to itself)
    or equivalently:
        iam.serviceAccounts.signBlob  permission on the project.
"""

import datetime
import json
import logging
import os
import uuid
from pathlib import Path

_log = logging.getLogger(__name__)

_SESSION_PREFIX = "sessions"
_MANIFEST_FILE = "_manifest.json"
_SIGNED_URL_EXPIRY_MINUTES = 60

# Maximum total size (in bytes) across all files in a single upload session.
MAX_SESSION_SIZE = 50 * 1024 * 1024 * 1024  # 50 GB

# Valid roles for files in an upload session.
_VALID_ROLES = frozenset({"tmcf", "csv", "stat_vars_mcf", "stat_vars_schema_mcf"})


def is_gcs_uploads_configured() -> bool:
    """True when GCS_REPORTS_BUCKET is set (same bucket used for reports and upload sessions)."""
    return bool((os.environ.get("GCS_REPORTS_BUCKET") or "").strip())


def _get_client_and_bucket():
    """Return (client, bucket) or raise GCSAccessError. Delegates to gcs_reports."""
    from ui.gcs_reports import _get_client_and_bucket as _reports_get
    return _reports_get()


def _make_signed_url(blob, expiry: datetime.timedelta, content_type: str) -> str:
    """Generate a V4 signed PUT URL that works both locally and on Cloud Run.

    - With a service account key file (GOOGLE_APPLICATION_CREDENTIALS set to a JSON
      key): the google-cloud-storage library signs directly.
    - On Cloud Run / Compute Engine: default credentials lack sign_bytes, so we use
      the IAM signBlob API by passing service_account_email + access_token.
    """
    import google.auth
    import google.auth.exceptions
    import google.auth.transport.requests

    try:
        credentials, _ = google.auth.default()
    except google.auth.exceptions.DefaultCredentialsError as exc:
        raise RuntimeError(f"No GCP credentials found: {exc}") from exc

    # Compute Engine / Cloud Run credentials do not implement sign_bytes.
    # Detect this and fall back to token-based URL signing via IAM signBlob.
    if not hasattr(credentials, "sign_bytes"):
        req = google.auth.transport.requests.Request()
        credentials.refresh(req)
        return blob.generate_signed_url(
            version="v4",
            expiration=expiry,
            method="PUT",
            content_type=content_type,
            service_account_email=credentials.service_account_email,
            access_token=credentials.token,
        )

    # Service account key file: the library signs the URL directly.
    return blob.generate_signed_url(
        version="v4",
        expiration=expiry,
        method="PUT",
        content_type=content_type,
        credentials=credentials,
    )


def _guess_content_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return {".csv": "text/csv", ".tmcf": "text/plain", ".mcf": "text/plain"}.get(
        ext, "application/octet-stream"
    )


def create_upload_session(files: list[dict]) -> dict:
    """Create a GCS upload session and return signed PUT URLs for each file.

    Args:
        files: list of dicts, each with:
            "name"         — filename (no path separators)
            "size"         — byte size (informational; stored in manifest)
            "role"         — one of: tmcf, csv, stat_vars_mcf, stat_vars_schema_mcf
            "content_type" — optional; guessed from extension if absent

    Returns:
        {
            "session_id": str,
            "upload_urls": [{"filename": str, "url": str, "content_type": str, "role": str}]
        }

    Raises:
        ValueError if files is empty or contains invalid filenames / roles.
        GCSAccessError if the GCS bucket is not accessible.
        RuntimeError if GCS is not configured or no GCP credentials found.
    """
    if not files:
        raise ValueError("files list must not be empty")

    total_size = sum(int(f.get("size") or 0) for f in files)
    if total_size > MAX_SESSION_SIZE:
        limit_gb = MAX_SESSION_SIZE // (1024 ** 3)
        raise ValueError(f"Total upload size exceeds {limit_gb} GB limit.")

    _, bucket = _get_client_and_bucket()
    if bucket is None:
        raise RuntimeError("GCS is not configured (GCS_REPORTS_BUCKET not set)")

    session_id = uuid.uuid4().hex
    expiry = datetime.timedelta(minutes=_SIGNED_URL_EXPIRY_MINUTES)

    total_size_mb = total_size // (1024 * 1024)
    _log.info(
        "gcs_upload_start session=%s files=%d total_size_mb=%d",
        session_id,
        len(files),
        total_size_mb,
    )

    upload_urls = []
    manifest_entries = []

    for file_info in files:
        filename = (file_info.get("name") or "").strip()
        if not filename or "/" in filename or "\\" in filename or filename.startswith("."):
            raise ValueError(f"Invalid filename: {filename!r}")

        role = (file_info.get("role") or "").strip()
        if role not in _VALID_ROLES:
            raise ValueError(f"Invalid role {role!r}; must be one of {sorted(_VALID_ROLES)}")

        content_type = (file_info.get("content_type") or "").strip() or _guess_content_type(filename)
        gcs_path = f"{_SESSION_PREFIX}/{session_id}/{filename}"
        blob = bucket.blob(gcs_path)

        signed_url = _make_signed_url(blob, expiry, content_type)
        upload_urls.append({"filename": filename, "url": signed_url, "content_type": content_type, "role": role})
        manifest_entries.append(
            {
                "filename": filename,
                "role": role,
                "content_type": content_type,
                "size": file_info.get("size", 0),
            }
        )

    # Write manifest so any Cloud Run instance can resolve the session's files.
    manifest_blob = bucket.blob(f"{_SESSION_PREFIX}/{session_id}/{_MANIFEST_FILE}")
    manifest_blob.upload_from_string(
        json.dumps({"session_id": session_id, "files": manifest_entries}),
        content_type="application/json",
    )

    return {"session_id": session_id, "upload_urls": upload_urls}


async def download_session_to_dir(session_id: str, dest_dir: Path) -> dict:
    """Download all session files from GCS into the standard upload directory layout.

    Files are placed at canonical paths mirroring what _run_custom_validation_impl
    expects so that the rest of the validation pipeline is unchanged:
        dest_dir/input.tmcf
        dest_dir/csvs/<original-filename>.csv
        dest_dir/input_stat_vars.mcf          (when present)
        dest_dir/input_stat_vars_schema.mcf   (when present)

    All file downloads run concurrently via asyncio.gather + asyncio.to_thread so
    multiple large CSV files transfer in parallel without blocking the event loop.
    The manifest fetch (always tiny) runs first in its own to_thread call.

    Returns:
        {
            "tmcf": Path,
            "csvs": list[Path],          # order preserved from manifest
            "stat_vars_mcf": Path | None,
            "stat_vars_schema_mcf": Path | None,
        }

    Raises:
        ValueError if session_id is malformed.
        FileNotFoundError if the session manifest does not exist in GCS.
        GCSAccessError / RuntimeError if GCS is not accessible.
    """
    import asyncio as _asyncio

    if (
        not session_id
        or len(session_id) != 32
        or not all(c in "0123456789abcdef" for c in session_id)
    ):
        raise ValueError(f"Invalid session_id: {session_id!r}")

    # Create local directories (fast, non-blocking).
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    csvs_dir = dest_dir / "csvs"
    csvs_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: fetch manifest ────────────────────────────────────────────────
    # bucket.reload() and manifest download are both blocking network calls;
    # run them together in one thread so the event loop stays free.
    def _load_manifest():
        _, bucket = _get_client_and_bucket()
        if bucket is None:
            raise RuntimeError("GCS is not configured (GCS_REPORTS_BUCKET not set)")
        blob = bucket.blob(f"{_SESSION_PREFIX}/{session_id}/{_MANIFEST_FILE}")
        if not blob.exists():
            raise FileNotFoundError(
                f"Upload session {session_id!r} not found (manifest missing — session may have expired)"
            )
        return json.loads(blob.download_as_bytes()), bucket

    manifest, bucket = await _asyncio.to_thread(_load_manifest)

    # ── Step 2: resolve destination paths (pure computation, no I/O) ─────────
    # Collect as a list to preserve manifest order for the "csvs" result key.
    file_tasks: list[tuple[str, Path, str]] = []  # (role, dest_path, gcs_path)
    for entry in manifest.get("files", []):
        filename = entry.get("filename", "")
        role = entry.get("role", "")
        if not filename or role not in _VALID_ROLES:
            continue
        gcs_path = f"{_SESSION_PREFIX}/{session_id}/{filename}"
        if role == "tmcf":
            dest_path = dest_dir / "input.tmcf"
        elif role == "csv":
            dest_path = csvs_dir / filename
        elif role == "stat_vars_mcf":
            dest_path = dest_dir / "input_stat_vars.mcf"
        elif role == "stat_vars_schema_mcf":
            dest_path = dest_dir / "input_stat_vars_schema.mcf"
        else:
            continue
        file_tasks.append((role, dest_path, gcs_path))

    # ── Step 3: download all files concurrently (bounded concurrency) ─────────
    # Each download runs in its own thread-pool slot.  A semaphore caps active
    # downloads at _MAX_PARALLEL_DOWNLOADS so a session with many CSVs does not
    # exhaust the thread pool.  Errors propagate immediately (gather default).
    _MAX_PARALLEL_DOWNLOADS = 4
    semaphore = _asyncio.Semaphore(_MAX_PARALLEL_DOWNLOADS)

    def _download(gcs_path: str, dest_path: Path) -> None:
        bucket.blob(gcs_path).download_to_filename(str(dest_path))

    async def _download_task(gcs_path: str, dest_path: Path) -> None:
        async with semaphore:
            await _asyncio.to_thread(_download, gcs_path, dest_path)

    await _asyncio.gather(*[
        _download_task(gcs_path, dest_path)
        for _, dest_path, gcs_path in file_tasks
    ])

    # ── Step 4: assemble result dict (manifest order preserved) ──────────────
    result: dict = {"tmcf": None, "csvs": [], "stat_vars_mcf": None, "stat_vars_schema_mcf": None}
    for role, dest_path, _ in file_tasks:
        if role == "tmcf":
            result["tmcf"] = dest_path
        elif role == "csv":
            result["csvs"].append(dest_path)
        elif role == "stat_vars_mcf":
            result["stat_vars_mcf"] = dest_path
        elif role == "stat_vars_schema_mcf":
            result["stat_vars_schema_mcf"] = dest_path

    return result


def delete_session(session_id: str) -> int:
    """Delete all blobs for a session from GCS.

    Returns the number of blobs deleted. Non-raising — errors are silently
    ignored so this is safe to call in finally blocks and background cleanups.
    """
    if (
        not session_id
        or len(session_id) != 32
        or not all(c in "0123456789abcdef" for c in session_id)
    ):
        return 0
    try:
        _, bucket = _get_client_and_bucket()
        if bucket is None:
            return 0
        prefix = f"{_SESSION_PREFIX}/{session_id}/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            try:
                blob.delete()
            except Exception:
                pass
        return len(blobs)
    except Exception:
        return 0
