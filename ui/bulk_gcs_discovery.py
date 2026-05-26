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

_VALIDATION_CONFIG_NAME = "validation_config.json"
_STAT_VARS_MCF_SUFFIX = "_stat_vars.mcf"
_STAT_VARS_SCHEMA_MCF_SUFFIX = "_stat_vars_schema.mcf"
_OUTPUT_TMCF_SUFFIX = "_output.tmcf"
_OUTPUT_CSV_SUFFIX = "_output.csv"


def _is_output_tmcf_basename(basename_lower: str) -> bool:
    return basename_lower.endswith(_OUTPUT_TMCF_SUFFIX)


def _is_output_csv_basename(basename_lower: str) -> bool:
    return basename_lower.endswith(_OUTPUT_CSV_SUFFIX)


def _output_tmcf_stem(basename_lower: str) -> str:
    """Stem for ``sample_output.tmcf`` -> ``sample_output``."""
    return basename_lower[: -len(".tmcf")]


def _is_aux_stat_vars_mcf_basename(basename_lower: str) -> bool:
    """e.g. ``StatisticsPoland_output_stat_vars.mcf`` (also bare ``stat_vars.mcf``)."""
    return (
        basename_lower.endswith(_STAT_VARS_MCF_SUFFIX)
        or basename_lower == "stat_vars.mcf"
    )


def _is_aux_stat_vars_schema_mcf_basename(basename_lower: str) -> bool:
    """e.g. ``*_stat_vars_schema.mcf`` (also bare ``stat_vars_schema.mcf``)."""
    return (
        basename_lower.endswith(_STAT_VARS_SCHEMA_MCF_SUFFIX)
        or basename_lower == "stat_vars_schema.mcf"
    )


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

    output_tmcf_entries: list[tuple[str, str]] = []  # (basename_lower, gs_uri)
    output_csv_basenames: list[str] = []
    stat_vars_mcf_basenames: list[str] = []
    stat_vars_schema_mcf_basenames: list[str] = []
    validation_config_path = ""

    for base_lower, full_name in basenames.items():
        if base_lower == _VALIDATION_CONFIG_NAME:
            validation_config_path = gs_uri(bucket, full_name)
            continue
        if _is_aux_stat_vars_schema_mcf_basename(base_lower):
            stat_vars_schema_mcf_basenames.append(base_lower)
            continue
        if _is_aux_stat_vars_mcf_basename(base_lower):
            stat_vars_mcf_basenames.append(base_lower)
            continue
        if _is_output_tmcf_basename(base_lower):
            output_tmcf_entries.append((base_lower, gs_uri(bucket, full_name)))
            continue
        if _is_output_csv_basename(base_lower):
            output_csv_basenames.append(base_lower)
            continue
        # Ignore unrelated CSVs, TMCFs, and other files.

    if len(stat_vars_mcf_basenames) > 1:
        names = ", ".join(sorted(stat_vars_mcf_basenames))
        return None, f"multiple *_stat_vars.mcf files ({names})"
    if len(stat_vars_schema_mcf_basenames) > 1:
        names = ", ".join(sorted(stat_vars_schema_mcf_basenames))
        return None, f"multiple *_stat_vars_schema.mcf files ({names})"

    stat_vars_path = ""
    if len(stat_vars_mcf_basenames) == 1:
        stat_vars_path = gs_uri(bucket, basenames[stat_vars_mcf_basenames[0]])
    schema_path = ""
    if len(stat_vars_schema_mcf_basenames) == 1:
        schema_path = gs_uri(bucket, basenames[stat_vars_schema_mcf_basenames[0]])

    if len(output_tmcf_entries) == 0:
        return None, "missing *_output.tmcf (exactly one required)"
    if len(output_tmcf_entries) > 1:
        names = ", ".join(e[0] for e in sorted(output_tmcf_entries))
        return None, f"multiple *_output.tmcf files ({names})"

    tmcf_basename_lower, tmcf_path = output_tmcf_entries[0]
    tmcf_stem_lower = _output_tmcf_stem(tmcf_basename_lower)
    expected_csv_lower = f"{tmcf_stem_lower}.csv"
    csv_blob_name = basenames.get(expected_csv_lower)
    csv_path = gs_uri(bucket, csv_blob_name) if csv_blob_name else ""

    if len(output_csv_basenames) > 1:
        names = ", ".join(sorted(output_csv_basenames))
        return None, f"multiple *_output.csv files ({names})"
    if not csv_path:
        if output_csv_basenames:
            only = output_csv_basenames[0]
            return None, (
                f"no *_output.csv matching {tmcf_basename_lower} "
                f"(found {only} instead)"
            )
        return None, f"no matching *_output.csv for {tmcf_basename_lower}"

    folder_name = folder_name_from_prefix(folder_prefix, root_prefix)
    dataset_id = normalize_dataset_id(folder_name)
    if not dataset_id:
        return None, f"could not normalize dataset folder name {folder_name!r}"

    return (
        DiscoveredDataset(
            folder_prefix=folder_prefix,
            dataset_id=dataset_id,
            csv_gcs_path=csv_path,
            tmcf_gcs_path=tmcf_path,
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


def classify_gcs_discovery_error(exc: BaseException) -> tuple[int, dict[str, str]]:
    """Map a GCS listing failure to HTTP status and a structured error payload."""
    raw = str(exc).strip()
    code = "gcs_error"
    message = "Failed to access the GCS path. Check the URL and try again."
    status = 500

    try:
        from google.api_core import exceptions as gexc

        if isinstance(exc, gexc.NotFound):
            return 404, {
                "code": "gcs_not_found",
                "message": "GCS path not found. Check the bucket and folder path.",
                "detail": raw,
            }
        if isinstance(exc, gexc.Forbidden):
            return 403, {
                "code": "gcs_access_denied",
                "message": "Access denied to GCS path. Verify bucket IAM for the service account.",
                "detail": raw,
            }
        if isinstance(exc, gexc.Unauthorized):
            return 403, {
                "code": "gcs_access_denied",
                "message": "Access denied to GCS path. Verify credentials and bucket permissions.",
                "detail": raw,
            }
    except ImportError:
        pass

    lower = raw.lower()
    if "404" in lower or "not found" in lower or "nosuchbucket" in lower or "no such object" in lower:
        status, code = 404, "gcs_not_found"
        message = "GCS path not found. Check the bucket and folder path."
    elif (
        "403" in lower
        or "forbidden" in lower
        or "access denied" in lower
        or "permission" in lower
        or "caller does not have" in lower
    ):
        status, code = 403, "gcs_access_denied"
        message = "Access denied to GCS path. Verify bucket IAM for the service account."
    elif "invalid" in lower and "bucket" in lower:
        status, code = 400, "gcs_invalid_path"
        message = "Invalid GCS path. Check the bucket name and folder prefix."

    return status, {"code": code, "message": message, "detail": raw}


def bulk_discovery_outcome(
    *,
    datasets_found: int,
    submitted: int,
    discovered_count: int,
    skipped_count: int,
    run_count: int,
) -> tuple[str, str]:
    """Return (outcome_code, user_message) for a completed bulk discovery response."""
    if datasets_found == 0:
        return (
            "empty_root",
            "No dataset folders found under this path.",
        )
    if submitted > 0:
        return "ok", ""
    if discovered_count == 0 and skipped_count > 0:
        return (
            "no_runnable",
            "No runnable dataset folders discovered. Every folder was skipped — see reasons below.",
        )
    if run_count > 0:
        return (
            "submit_failed",
            "No jobs were submitted. Review per-dataset errors in the table below.",
        )
    return (
        "no_runnable",
        "No runnable dataset folders discovered.",
    )


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
            size
            for name, size in objects
            if gs_uri(bucket, name) == ds.csv_gcs_path
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
