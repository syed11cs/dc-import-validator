#!/usr/bin/env python3
"""
Download one file or all files under a prefix from GCS.

Usage:
    gcs_download.py <gcs_uri> <local_dest>

Modes (determined by whether gcs_uri ends with /):

    Single-file:  gcs_uri does NOT end with /
        Downloads one object to local_dest (treated as a file path).

    Prefix:  gcs_uri ends with /
        Lists all objects under the prefix and downloads each file to
        local_dest/<filename>.  Exits 1 when no objects are found.

All output goes to stdout (no stderr) so Cloud Logging captures it in a
single stream.  Exit code is 0 on success, 1 on any error.
"""

import sys
from pathlib import Path

from google.cloud import storage


def _parse_uri(uri: str) -> tuple[str, str]:
    """Return (bucket_name, blob_path) from a gs:// URI."""
    if not uri.startswith("gs://"):
        print(f"ERROR: GCS URI must start with gs://: {uri}", flush=True)
        sys.exit(1)
    without_scheme = uri[len("gs://"):]
    bucket_name, _, blob_path = without_scheme.partition("/")
    if not bucket_name:
        print(f"ERROR: Cannot parse bucket from URI: {uri}", flush=True)
        sys.exit(1)
    return bucket_name, blob_path


def download_file(client: storage.Client, uri: str, dest: str) -> None:
    """Download a single GCS object to a local file path."""
    bucket_name, blob_path = _parse_uri(uri)
    if not blob_path:
        print(f"ERROR: No blob path in URI: {uri}", flush=True)
        sys.exit(1)
    blob = client.bucket(bucket_name).blob(blob_path)
    Path(dest).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(dest)
    print(f"[download] {uri} -> {dest}", flush=True)


def download_prefix(client: storage.Client, uri: str, dest_dir: str) -> None:
    """Download all objects under a GCS prefix into a local directory."""
    bucket_name, prefix = _parse_uri(uri)
    prefix = prefix.rstrip("/") + "/"
    bucket = client.bucket(bucket_name)
    blobs = [b for b in bucket.list_blobs(prefix=prefix) if not b.name.endswith("/")]
    if not blobs:
        print(f"ERROR: No files found at gs://{bucket_name}/{prefix}", flush=True)
        sys.exit(1)
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    for blob in blobs:
        # Preserve relative path from the prefix so subdirectories (e.g. goldens/) are kept.
        rel_path = blob.name[len(prefix):]
        if not rel_path:
            continue
        local_path = dest / rel_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local_path))
        size_mb = (blob.size or 0) / (1024 * 1024)
        print(f"[download] {blob.name} -> {local_path} ({size_mb:.1f} MB)", flush=True)
    print(f"[download] Complete: {len(blobs)} file(s) downloaded", flush=True)


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <gcs_uri> <local_dest>", flush=True)
        sys.exit(1)

    gcs_uri, local_dest = sys.argv[1], sys.argv[2]

    try:
        client = storage.Client()
        if gcs_uri.endswith("/"):
            download_prefix(client, gcs_uri, local_dest)
        else:
            download_file(client, gcs_uri, local_dest)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"ERROR: GCS download failed: {exc}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
