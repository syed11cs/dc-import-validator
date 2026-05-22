"""Canonical run output and GCS path conventions."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RunPaths:
    """Resolved filesystem and GCS locations for a single run."""

    app_root: Path
    run_id: str
    dataset_id: str
    output_dir: Path
    canonical_output_dir: Path
    custom_upload_dir: Path | None = None

    @property
    def gcs_reports_bucket(self) -> str:
        return (os.environ.get("GCS_REPORTS_BUCKET") or "").strip()

    @property
    def status_json_uri(self) -> str | None:
        bucket = self.gcs_reports_bucket
        if not bucket or not self.run_id:
            return None
        return f"gs://{bucket}/jobs/{self.run_id}/status.json"

    @property
    def reports_prefix_uri(self) -> str | None:
        bucket = self.gcs_reports_bucket
        if not bucket or not self.run_id:
            return None
        return f"gs://{bucket}/reports/{self.run_id}/{self.dataset_id}/"


def default_app_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def canonical_dataset_output_dir(app_root: Path, dataset_id: str) -> Path:
    """Latest/canonical artifact dir (e.g. output/child_birth_genmcf)."""
    mapping = {
        "child_birth": "child_birth_genmcf",
        "statistics_poland": "statistics_poland_genmcf",
        "finland_census": "finland_census_genmcf",
        "uae_population": "uae_population_genmcf",
        "custom": "custom_input",
    }
    suffix = mapping.get(dataset_id, f"{dataset_id}_genmcf")
    return app_root / "output" / suffix


def per_run_output_dir(app_root: Path, dataset_id: str, run_id: str) -> Path:
    """Isolated per-run dir when run_id is set (output/<dataset>/<run_id>/)."""
    if not run_id:
        return canonical_dataset_output_dir(app_root, dataset_id)
    return app_root / "output" / dataset_id / run_id


def custom_upload_dir(app_root: Path, run_id: str) -> Path:
    return app_root / "output" / "custom_upload" / run_id


def resolve_run_paths(
    dataset_id: str,
    run_id: str,
    *,
    app_root: Path | None = None,
) -> RunPaths:
    root = app_root or default_app_root()
    upload = custom_upload_dir(root, run_id) if dataset_id == "custom" and run_id else None
    return RunPaths(
        app_root=root,
        run_id=run_id,
        dataset_id=dataset_id,
        output_dir=per_run_output_dir(root, dataset_id, run_id),
        canonical_output_dir=canonical_dataset_output_dir(root, dataset_id),
        custom_upload_dir=upload,
    )
