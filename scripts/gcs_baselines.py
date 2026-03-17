"""Baseline storage for differ-based validation.

Baselines are stored under:
  GCS:   gs://{GCS_REPORTS_BUCKET}/baselines/{dataset_id}/latest/
  Local: {project_root}/output/baselines/{dataset_id}/latest/

Each accepted baseline is also archived as a versioned copy (v1/, v2/, ...),
with latest/ always pointing to the most recently accepted version.

A manifest.json sentinel is written alongside the MCF files so existence
checks are a single O(1) blob lookup rather than a list-blobs call.
"""

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _local_baseline_dir(dataset_id: str) -> Path:
    return _PROJECT_ROOT / "output" / "baselines" / dataset_id / "latest"


def _local_version_dir(dataset_id: str, version: str) -> Path:
    return _PROJECT_ROOT / "output" / "baselines" / dataset_id / version


def _get_bucket_name() -> str | None:
    name = (os.environ.get("GCS_REPORTS_BUCKET") or "").strip()
    return name if name else None


def _get_bucket():
    """Return GCS bucket object, or None if GCS not configured.
    Raises RuntimeError if GCS_REPORTS_BUCKET is set but the bucket is inaccessible."""
    bucket_name = _get_bucket_name()
    if not bucket_name:
        return None
    try:
        from google.cloud import storage  # type: ignore
    except ImportError:
        raise RuntimeError(
            "GCS_REPORTS_BUCKET is set but google-cloud-storage is not installed."
        )
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        bucket.reload()
        return bucket
    except Exception as e:
        raise RuntimeError(f"GCS bucket {bucket_name!r} not accessible: {e}") from e


def _next_version(dataset_id: str) -> str:
    """Return the next version string (e.g. 'v2') by reading version from latest/manifest.json.
    Returns 'v1' when no baseline exists yet or the version field is absent (migration case).
    """
    try:
        bucket = _get_bucket()
        if bucket is not None:
            blob = bucket.get_blob(f"baselines/{dataset_id}/latest/manifest.json")
            if blob:
                manifest = json.loads(blob.download_as_text())
                n = int(manifest.get("version", "v0")[1:]) + 1
                return f"v{n}"
        else:
            m = _local_baseline_dir(dataset_id) / "manifest.json"
            if m.exists():
                manifest = json.loads(m.read_text(encoding="utf-8"))
                n = int(manifest.get("version", "v0")[1:]) + 1
                return f"v{n}"
    except Exception:
        pass
    return "v1"


def baseline_exists(dataset_id: str) -> bool:
    """Return True if a baseline exists for dataset_id. Fail-open: returns False on error."""
    try:
        bucket = _get_bucket()
        if bucket is not None:
            blob = bucket.get_blob(f"baselines/{dataset_id}/latest/manifest.json")
            return blob is not None
        else:
            return (_local_baseline_dir(dataset_id) / "manifest.json").exists()
    except Exception:
        return False


def download_baseline(dataset_id: str, local_dir: Path) -> bool:
    """Download baseline MCF files to local_dir. Returns True on success, False if not found."""
    local_dir.mkdir(parents=True, exist_ok=True)
    try:
        bucket = _get_bucket()
        if bucket is not None:
            prefix = f"baselines/{dataset_id}/latest/"
            blobs = list(bucket.list_blobs(prefix=prefix))
            if not blobs:
                return False
            for blob in blobs:
                filename = blob.name[len(prefix):]
                if not filename:
                    continue
                dest = local_dir / filename
                dest.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(str(dest))
            return True
        else:
            src = _local_baseline_dir(dataset_id)
            if not (src / "manifest.json").exists():
                return False
            for f in src.iterdir():
                shutil.copy2(f, local_dir / f.name)
            return True
    except Exception as e:
        print(f"Warning: baseline download failed for '{dataset_id}': {e}", flush=True)
        return False


def upload_baseline(
    dataset_id: str,
    mcf_dir: Path,
    run_id: str | None = None,
    accepted_by: str | None = None,
) -> tuple[bool, str]:
    """Upload *.mcf files from mcf_dir as the new baseline.

    Writes to both latest/ (backward-compatible) and a new versioned directory v{N}/.
    Returns (success, version_string) — e.g. (True, 'v3').
    Existing callers that used the old bool return value should unpack the tuple.
    """
    mcf_files = list(mcf_dir.glob("*.mcf"))
    if not mcf_files:
        print(
            f"Warning: no .mcf files found in {mcf_dir} — baseline not updated",
            flush=True,
        )
        return False, ""
    version = _next_version(dataset_id)
    manifest: dict = {
        "dataset_id": dataset_id,
        "version": version,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "file_count": len(mcf_files),
    }
    if run_id:
        manifest["run_id"] = run_id
    if accepted_by:
        manifest["accepted_by"] = accepted_by
    manifest_text = json.dumps(manifest, indent=2)
    try:
        bucket = _get_bucket()
        if bucket is not None:
            for prefix in (
                f"baselines/{dataset_id}/latest/",
                f"baselines/{dataset_id}/{version}/",
            ):
                for mcf_file in mcf_files:
                    bucket.blob(f"{prefix}{mcf_file.name}").upload_from_filename(
                        str(mcf_file), content_type="text/plain"
                    )
                bucket.blob(f"{prefix}manifest.json").upload_from_string(
                    manifest_text, content_type="application/json"
                )
        else:
            for dest in (
                _local_baseline_dir(dataset_id),        # latest/
                _local_version_dir(dataset_id, version), # v{N}/
            ):
                dest.mkdir(parents=True, exist_ok=True)
                for mcf_file in mcf_files:
                    shutil.copy2(mcf_file, dest / mcf_file.name)
                (dest / "manifest.json").write_text(manifest_text, encoding="utf-8")
        print(f"Baseline updated for dataset_id='{dataset_id}' → {version}", flush=True)
        return True, version
    except Exception as e:
        print(f"Warning: baseline upload failed for '{dataset_id}': {e}", flush=True)
        return False, ""


def list_baseline_versions(dataset_id: str) -> list[dict]:
    """Return version manifests sorted newest-first. Empty list if none or on error.

    Only versioned directories (v1/, v2/, ...) are included — latest/ is excluded
    because it is always a copy of the most recent version.
    """
    results: list[dict] = []
    try:
        bucket = _get_bucket()
        if bucket is not None:
            prefix = f"baselines/{dataset_id}/"
            seen: set[str] = set()
            for blob in bucket.list_blobs(prefix=prefix):
                # Match baselines/{dataset}/v{N}/manifest.json
                rest = blob.name[len(prefix):]
                parts = rest.split("/")
                if len(parts) == 2 and parts[0].startswith("v") and parts[1] == "manifest.json":
                    ver_key = parts[0]
                    if ver_key not in seen:
                        seen.add(ver_key)
                        try:
                            results.append(json.loads(blob.download_as_text()))
                        except Exception:
                            pass
        else:
            base = _local_baseline_dir(dataset_id).parent
            for manifest_path in base.glob("v*/manifest.json"):
                try:
                    results.append(json.loads(manifest_path.read_text(encoding="utf-8")))
                except Exception:
                    pass
    except Exception:
        pass
    results.sort(key=lambda m: int(m.get("version", "v0")[1:]), reverse=True)
    return results
