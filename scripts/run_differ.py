#!/usr/bin/env python3
"""Run the DC import differ to produce differ output for validation.

Compares the current genmcf output against a stored baseline using the official
DC import_differ.py tool, then normalizes output for the validation runner.

Exit codes (diff mode):
  0  differ ran successfully; --output_dir is populated with obs_diff_summary.csv
     and differ_summary.json
  1  no baseline exists (first run); skipping is expected and non-fatal
  2  error; pipeline continues without differ output

Usage:
  # Diff mode (Step 2.4 — before import_validation):
  python run_differ.py \\
    --current_mcf_dir=DIR --dataset_id=ID --output_dir=DIR

  # Baseline update (after successful validation):
  python run_differ.py \\
    --update_baseline --current_mcf_dir=DIR --dataset_id=ID
"""

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import gcs_baselines  # noqa: E402

# Column header matching sample_data/empty_differ.csv
_EMPTY_DIFFER_HEADER = "StatVar,DELETED,MODIFIED,ADDED\n"


def _find_mcf_files(directory: str) -> list[str]:
    return glob.glob(os.path.join(directory, "*.mcf"))


def _invoke_import_differ(
    current_data: str,
    previous_data: str,
    output_location: str,
    data_repo: str,
) -> bool:
    """Run import_differ as a module (same pattern as run_validation.py invokes the runner).

    Equivalent to:
      cd DATA_REPO && python -m tools.import_differ.import_differ --runner_mode=local ...

    Returns True on exit 0, False otherwise.
    """
    cmd = [
        sys.executable,
        "-m", "tools.import_differ.import_differ",
        f"--current_data={current_data}",
        f"--previous_data={previous_data}",
        f"--output_location={output_location}",
        "--file_format=mcf",
        "--runner_mode=local",
    ]
    result = subprocess.run(cmd, cwd=data_repo, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, flush=True)
    if result.stderr:
        print(result.stderr, file=sys.stderr, flush=True)
    return result.returncode == 0


def _normalize_diff_output(output_dir: str) -> None:
    """Rename variableMeasured → StatVar in obs_diff_summary.csv.

    The DC differ tool writes 'variableMeasured'; the DC validation runner
    expects 'StatVar' for scope filtering. Normalizes the file in-place.
    If the file is absent (differ produced no observations), writes an empty
    CSV with the correct schema matching sample_data/empty_differ.csv.
    """
    import pandas as pd

    path = os.path.join(output_dir, "obs_diff_summary.csv")
    if not os.path.isfile(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(_EMPTY_DIFFER_HEADER)
        return
    df = pd.read_csv(path)
    if "variableMeasured" in df.columns and "StatVar" not in df.columns:
        df.rename(columns={"variableMeasured": "StatVar"}, inplace=True)
        # Reorder to match empty_differ.csv column order
        cols = ["StatVar", "DELETED", "MODIFIED", "ADDED"]
        existing = [c for c in cols if c in df.columns]
        remaining = [c for c in df.columns if c not in cols]
        df = df[existing + remaining]
        df.to_csv(path, index=False)


def _count_mcf_observations(mcf_dir: str) -> int:
    """Count StatVarObservation nodes in MCF files by scanning for typeOf lines."""
    count = 0
    for mcf_file in glob.glob(os.path.join(mcf_dir, "*.mcf")):
        try:
            with open(mcf_file, encoding="utf-8", errors="replace") as f:
                for line in f:
                    if "typeOf" in line and "StatVarObservation" in line:
                        count += 1
        except OSError:
            pass
    return count


def _ensure_differ_summary(output_dir: str, previous_mcf_dir: str) -> None:
    """Ensure differ_summary.json contains previous_obs_size.

    The DC validation runner's DELETED_RECORDS_PERCENT validator requires
    this key. If import_differ.py did not write it, compute it from the
    baseline MCF files.
    """
    summary_path = os.path.join(output_dir, "differ_summary.json")
    data: dict = {}
    if os.path.isfile(summary_path):
        try:
            with open(summary_path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = {}
    if "previous_obs_size" not in data:
        data["previous_obs_size"] = _count_mcf_observations(previous_mcf_dir)
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(data, f)


def run_diff(args: argparse.Namespace) -> int:
    """Compare current MCF against stored baseline. Returns exit code."""
    data_repo = os.environ.get("DATA_REPO")
    if not data_repo or not os.path.isdir(data_repo):
        print(
            "Error: DATA_REPO must be set and point to datacommonsorg/data",
            file=sys.stderr,
        )
        return 2

    mcf_files = _find_mcf_files(args.current_mcf_dir)
    if not mcf_files:
        print(
            f"Warning: no .mcf files in {args.current_mcf_dir} — differ skipped",
            file=sys.stderr,
        )
        return 2

    if not gcs_baselines.baseline_exists(args.dataset_id):
        print(
            f"No baseline found for dataset_id='{args.dataset_id}' — "
            "skipping differ (first run)"
        )
        return 1

    baseline_dir = tempfile.mkdtemp(prefix="dc_baseline_")
    try:
        if not gcs_baselines.download_baseline(args.dataset_id, Path(baseline_dir)):
            print(
                f"Warning: baseline download failed for '{args.dataset_id}'",
                file=sys.stderr,
            )
            return 2

        if not _find_mcf_files(baseline_dir):
            print(
                f"Warning: baseline for '{args.dataset_id}' contains no .mcf files",
                file=sys.stderr,
            )
            return 2

        os.makedirs(args.output_dir, exist_ok=True)

        # import_differ.py accepts glob patterns such as *.mcf and handles expansion internally.
        ok = _invoke_import_differ(
            current_data=os.path.join(args.current_mcf_dir, "*.mcf"),
            previous_data=os.path.join(baseline_dir, "*.mcf"),
            output_location=args.output_dir,
            data_repo=data_repo,
        )
        if not ok:
            print(
                "Warning: import_differ failed — differ output unavailable",
                file=sys.stderr,
            )
            return 2

        _normalize_diff_output(args.output_dir)
        _ensure_differ_summary(args.output_dir, baseline_dir)
        # Persist dataset_id so review_summary.py can locate the correct baseline manifest
        # (critical for custom datasets where baseline_id is "custom_{hash}", not "custom").
        _summary_path = os.path.join(args.output_dir, "differ_summary.json")
        try:
            with open(_summary_path, encoding="utf-8") as _f:
                _summary_data = json.load(_f)
            _summary_data["dataset_id"] = args.dataset_id
            with open(_summary_path, "w", encoding="utf-8") as _f:
                json.dump(_summary_data, _f)
        except Exception:
            pass
        return 0
    finally:
        shutil.rmtree(baseline_dir, ignore_errors=True)


def update_baseline(args: argparse.Namespace) -> int:
    """Upload current MCF files as new baseline. Returns exit code."""
    run_id = getattr(args, "run_id", None) or None
    ok, version = gcs_baselines.upload_baseline(
        args.dataset_id, Path(args.current_mcf_dir), run_id=run_id
    )
    if ok:
        return 0
    return 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--current_mcf_dir",
        required=True,
        help="Directory containing current *.mcf files (genmcf output)",
    )
    parser.add_argument(
        "--dataset_id",
        required=True,
        help="Stable baseline identifier (e.g. 'child_birth', 'population_france')",
    )
    parser.add_argument(
        "--output_dir",
        default="",
        help="Where to write differ output (required in diff mode)",
    )
    parser.add_argument(
        "--update_baseline",
        action="store_true",
        help="Upload current MCF as new baseline instead of running diff",
    )
    parser.add_argument(
        "--run_id",
        default="",
        help="Run ID to record in the baseline manifest (optional, baseline mode only)",
    )
    args = parser.parse_args()

    if args.update_baseline:
        return update_baseline(args)

    if not args.output_dir:
        print("Error: --output_dir is required in diff mode", file=sys.stderr)
        return 2

    return run_diff(args)


if __name__ == "__main__":
    sys.exit(main())
