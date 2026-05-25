"""Discover per-dataset folders under a GCS root for bulk Batch validation.

Only immediate child prefixes of root_gcs_path are considered (no recursion).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Callable

_DATASET_ID_RE = re.compile(r"^[a-z0-9-]{3,48}$")
_MAX_DATASET_ID_LEN = 48
_MIN_DATASET_ID_LEN = 3
DEFAULT_MAX_BULK_PARALLELISM = 5

# Known optional MCF basenames (case-insensitive).
_STAT_VARS_MCF_NAMES = frozenset({"stat_vars.mcf"})
_SCHEMA_MCF_NAMES = frozenset({"stat_vars_schema.mcf", "schema.mcf"})
_VALIDATION_CONFIG_NAME = "validation_config.json"


@dataclass(frozen=True)
class DiscoveredDataset:
    """Files resolved for one immediate child folder."""

    folder_prefix: str  # e.g. imports/dataset_a/ (no gs://)
    dataset_id: str
    csv_gcs_path: str
    tmcf_gcs_path: str
    stat_vars_mcf_gcs_path: str = ""
    stat_vars_schema_mcf_gcs_path: str = ""
    validation_config_gcs_path: str = ""
    csv_total_bytes: int = 0


@dataclass(frozen=True)
class SkippedFolder:
    folder_prefix: str
    reason: str


def parse_gs_root(uri: str) -> tuple[str, str]:
    """Return (bucket_name, object_prefix) for a gs:// root folder.

    Prefix always ends with '/' when non-empty.
  """
    uri = (uri or "").strip()
    if not uri.startswith("gs://"):
        raise ValueError("root_gcs_path must start with gs://")
    without = uri[5:]
    bucket, _, blob = without.partition("/")
    if not bucket:
        raise ValueError("root_gcs_path is missing bucket name")
    prefix = blob.strip("/")
    if prefix:
        prefix += "/"
    return bucket, prefix


def gs_uri(bucket: str, object_name: str) -> str:
    return f"gs://{bucket}/{object_name.lstrip('/')}"


def folder_name_from_prefix(prefix: str, root_prefix: str) -> str:
    """Immediate child folder name under root_prefix."""
    rel = prefix
    if root_prefix and rel.startswith(root_prefix):
        rel = rel[len(root_prefix) :]
    rel = rel.strip("/")
    if "/" in rel:
        return rel.split("/")[0]
    return rel


def clamp_bulk_parallelism(value: int, *, maximum: int = DEFAULT_MAX_BULK_PARALLELISM) -> int:
    """Bound bulk submit concurrency (default 1, max ``maximum``)."""
    if value <= 0:
        return 1
    return min(int(value), maximum)


def normalize_dataset_id(folder_name: str) -> str | None:
    """Map a folder display name to a custom dataset id ([a-z0-9-]{3,48}).

    Examples: ``dataset_a`` -> ``dataset-a``, ``Birth Dataset 2024`` -> ``birth-dataset-2024``.
    Very short names get a stable ``ds-<hash>`` fallback instead of being skipped.
    """
    raw = (folder_name or "").strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", raw.lower()).strip("-")
    cleaned = re.sub(r"-+", "-", cleaned)
    if len(cleaned) > _MAX_DATASET_ID_LEN:
        cleaned = cleaned[:_MAX_DATASET_ID_LEN].rstrip("-")
    if len(cleaned) < _MIN_DATASET_ID_LEN:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
        cleaned = f"ds-{digest}"
    if not _DATASET_ID_RE.fullmatch(cleaned):
        return None
    return cleaned


def sanitize_dataset_id(folder_name: str) -> str | None:
    """Alias for :func:`normalize_dataset_id`."""
    return normalize_dataset_id(folder_name)


def classify_folder_objects(
    bucket: str,
    folder_prefix: str,
    object_names: list[str],
    *,
    root_prefix: str = "",
) -> tuple[DiscoveredDataset | None, str | None]:
    """Classify blob object paths in one folder prefix.

    object_names are full object paths (e.g. imports/dataset_a/data.csv).
    Returns (dataset, None) on success or (None, reason) when invalid.
    """
    if folder_prefix and not folder_prefix.endswith("/"):
        folder_prefix += "/"

    basenames: dict[str, str] = {}
    for name in object_names:
        if not name.startswith(folder_prefix):
            continue
        if name.endswith("/"):
            continue
        base = name[len(folder_prefix) :]
        if "/" in base:
            continue
        basenames[base.lower()] = name

    csv_paths: list[str] = []
    tmcf_paths: list[str] = []
    stat_vars_path = ""
    schema_path = ""
    validation_config_path = ""

    for base_lower, full_name in basenames.items():
        if base_lower == _VALIDATION_CONFIG_NAME:
            validation_config_path = gs_uri(bucket, full_name)
            continue
        if base_lower.endswith(".csv"):
            csv_paths.append(gs_uri(bucket, full_name))
            continue
        if base_lower.endswith(".tmcf"):
            tmcf_paths.append(gs_uri(bucket, full_name))
            continue
        if base_lower in _STAT_VARS_MCF_NAMES:
            if stat_vars_path:
                return None, "multiple stat_vars.mcf files"
            stat_vars_path = gs_uri(bucket, full_name)
            continue
        if base_lower in _SCHEMA_MCF_NAMES:
            if schema_path:
                return None, "multiple schema MCF files"
            schema_path = gs_uri(bucket, full_name)
            continue

    if len(csv_paths) == 0:
        return None, "missing CSV (exactly one .csv required)"
    if len(csv_paths) > 1:
        return None, f"multiple CSV files ({len(csv_paths)})"
    if len(tmcf_paths) == 0:
        return None, "missing TMCF (exactly one .tmcf required)"
    if len(tmcf_paths) > 1:
        return None, f"multiple TMCF files ({len(tmcf_paths)})"

    folder_name = folder_name_from_prefix(folder_prefix, root_prefix)
    dataset_id = normalize_dataset_id(folder_name)
    if not dataset_id:
        return None, f"could not normalize dataset folder name {folder_name!r}"

    return (
        DiscoveredDataset(
            folder_prefix=folder_prefix,
            dataset_id=dataset_id,
            csv_gcs_path=csv_paths[0],
            tmcf_gcs_path=tmcf_paths[0],
            stat_vars_mcf_gcs_path=stat_vars_path,
            stat_vars_schema_mcf_gcs_path=schema_path,
            validation_config_gcs_path=validation_config_path,
        ),
        None,
    )


def list_immediate_child_prefixes(
    bucket_name: str,
    root_prefix: str,
    *,
    list_blobs_fn: Callable[..., object] | None = None,
) -> list[str]:
    """List immediate child folder prefixes under root_prefix using GCS delimiter."""
    if list_blobs_fn is None:
        from google.cloud import storage

        def list_blobs_fn(**kwargs):
            return storage.Client().bucket(bucket_name).list_blobs(**kwargs)

    if root_prefix and not root_prefix.endswith("/"):
        root_prefix += "/"

    # delimiter="/" returns only immediate child prefixes (non-recursive).
    iterator = list_blobs_fn(prefix=root_prefix, delimiter="/")
    # Consume blob listings so GCS populates iterator.prefixes.
    for _ in iterator:
        pass
    prefixes = sorted(getattr(iterator, "prefixes", []) or [])
    return prefixes


def list_objects_under_prefix(
    bucket_name: str,
    prefix: str,
    *,
    list_blobs_fn: Callable[..., object] | None = None,
) -> list[tuple[str, int]]:
    """Return (object_name, size_bytes) for all blobs under prefix."""
    if list_blobs_fn is None:
        from google.cloud import storage

        def list_blobs_fn(**kwargs):
            return storage.Client().bucket(bucket_name).list_blobs(**kwargs)

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    out: list[tuple[str, int]] = []
    for blob in list_blobs_fn(prefix=prefix):
        name = getattr(blob, "name", None) or ""
        if not name or name.endswith("/"):
            continue
        size = int(getattr(blob, "size", None) or 0)
        out.append((name, size))
    return out


def discover_datasets_under_root(
    root_gcs_path: str,
    *,
    list_child_prefixes_fn: Callable[[str, str], list[str]] | None = None,
    list_objects_fn: Callable[[str, str], list[tuple[str, int]]] | None = None,
) -> tuple[list[DiscoveredDataset], list[SkippedFolder]]:
    """Enumerate and classify all immediate child folders under root_gcs_path."""
    bucket, root_prefix = parse_gs_root(root_gcs_path)
    list_child = list_child_prefixes_fn or list_immediate_child_prefixes
    list_objs = list_objects_fn or list_objects_under_prefix

    child_prefixes = list_child(bucket, root_prefix)
    discovered: list[DiscoveredDataset] = []
    skipped: list[SkippedFolder] = []

    for folder_prefix in child_prefixes:
        objects = list_objs(bucket, folder_prefix)
        names = [n for n, _ in objects]
        ds, err = classify_folder_objects(
            bucket, folder_prefix, names, root_prefix=root_prefix
        )
        if ds is None:
            skipped.append(SkippedFolder(folder_prefix=folder_prefix, reason=err or "unknown"))
            continue
        csv_total = sum(
            size for name, size in objects if name.lower().endswith(".csv")
        )
        discovered.append(
            DiscoveredDataset(
                folder_prefix=ds.folder_prefix,
                dataset_id=ds.dataset_id,
                csv_gcs_path=ds.csv_gcs_path,
                tmcf_gcs_path=ds.tmcf_gcs_path,
                stat_vars_mcf_gcs_path=ds.stat_vars_mcf_gcs_path,
                stat_vars_schema_mcf_gcs_path=ds.stat_vars_schema_mcf_gcs_path,
                validation_config_gcs_path=ds.validation_config_gcs_path,
                csv_total_bytes=csv_total,
            )
        )

    return discovered, skipped


def resolve_validation_config_url(
    folder_config: str,
    request_config: str,
) -> str:
    """Precedence: per-folder validation_config.json > request-level URL > sidebar merge."""
    if folder_config:
        return folder_config
    return (request_config or "").strip()


def bulk_rules_for_submit(
    folder_config: str,
    request_config: str,
    rules: str,
    custom_rules: list,
) -> tuple[str, str, list]:
    """Return (validation_config_url, rules, custom_rules) for one bulk dataset submit.

    When any external config URL applies, rules/custom_rules are cleared so the Batch
    path uses only --config (not sidebar merge).
    """
    config_url = resolve_validation_config_url(folder_config, request_config)
    if config_url:
        return config_url, "", []
    return "", rules or "", list(custom_rules or [])
