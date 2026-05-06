#!/usr/bin/env python3
"""
CSV splitter for genmcf parallelism.

Splits a single large CSV into fixed-size shards, preserving the header row in
every shard. Designed so genmcf can process shards concurrently via
--num-threads (one thread per file).

Algorithm: single streaming pass — rows are written to shards as they are
read. Row count is tracked during the write; if the final count is below
--threshold-rows the shards are deleted and status "skipped" is written to
the manifest. This minimizes I/O for the common case (file IS above threshold)
while still protecting against accidental enabling on small files.

Usage:
  python split_csv_for_genmcf.py \\
      --input FILE --output-dir DIR \\
      [--rows-per-shard N] [--threshold-rows N] [--manifest PATH]

Exits 0 on success (includes "skipped" when below threshold).
Exits 1 on any hard failure (unreadable input, write error, no shards).

On exit 0, writes a manifest JSON to --manifest (default:
<output-dir>/manifest.json) with shape:
  {
    "status":          "done" | "skipped",
    "original_csv":    "<abs-path>",
    "shard_paths":     ["<abs-path>", ...],  // empty when skipped
    "total_rows":      <int>,
    "shard_count":     <int>,
    "rows_per_shard":  <int>,
    "threshold_rows":  <int>,
    "elapsed_seconds": <float>
  }
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time


_DEFAULT_ROWS_PER_SHARD = 1_000_000
_DEFAULT_THRESHOLD_ROWS = 5_000_000


def _log(msg: str) -> None:
    print(f"[SPLIT] {msg}", flush=True)


def split_csv(
    input_path: str,
    output_dir: str,
    rows_per_shard: int,
    threshold_rows: int,
    manifest_path: str,
) -> int:
    """
    Core split logic. Returns exit code (0 = ok, 1 = hard failure).

    Single-pass: reads input once, writes shards incrementally.
    If final row count < threshold_rows the shards are deleted and the
    manifest records status="skipped".
    """
    t_start = time.monotonic()
    abs_input = os.path.abspath(input_path)
    basename = os.path.splitext(os.path.basename(input_path))[0]

    _log(f"Input:          {abs_input}")
    _log(f"Output dir:     {output_dir}")
    _log(f"Rows per shard: {rows_per_shard:,}")
    _log(f"Threshold:      {threshold_rows:,} rows")

    os.makedirs(output_dir, exist_ok=True)

    shard_paths: list[str] = []
    shard_index = 0
    total_rows = 0
    header: list[str] = []

    current_shard_file = None
    current_shard_writer = None
    rows_in_current_shard = 0

    def _open_shard() -> tuple:
        nonlocal shard_index
        path = os.path.join(output_dir, f"{basename}_shard_{shard_index:04d}.csv")
        fh = open(path, "w", newline="", encoding="utf-8")
        writer = csv.writer(fh)
        writer.writerow(header)
        shard_index += 1
        return path, fh, writer

    def _cleanup_shards() -> None:
        for p in shard_paths:
            try:
                os.remove(p)
            except OSError:
                pass

    try:
        with open(abs_input, newline="", encoding="utf-8") as fh_in:
            reader = csv.reader(fh_in)

            # Read header row.
            try:
                header = next(reader)
            except StopIteration:
                _log("ERROR: input CSV is empty (no header row)")
                return 1

            if not header:
                _log("ERROR: CSV header is empty")
                return 1

            # Open first shard and begin writing.
            current_shard_path, current_shard_file, current_shard_writer = _open_shard()
            shard_paths.append(current_shard_path)
            rows_in_current_shard = 0

            for row in reader:
                # Roll over to a new shard when current one is full.
                # Check BEFORE writing so we never open a shard with 0 data rows.
                if rows_in_current_shard >= rows_per_shard:
                    current_shard_file.close()
                    current_shard_file = None
                    current_shard_path, current_shard_file, current_shard_writer = _open_shard()
                    shard_paths.append(current_shard_path)
                    rows_in_current_shard = 0

                current_shard_writer.writerow(row)
                total_rows += 1
                rows_in_current_shard += 1

        if current_shard_file is not None:
            current_shard_file.close()
            current_shard_file = None

    except Exception as exc:
        if current_shard_file is not None:
            try:
                current_shard_file.close()
            except Exception:
                pass
        _log(f"ERROR: split failed: {exc}")
        _cleanup_shards()
        return 1

    elapsed = time.monotonic() - t_start

    # Below-threshold guard: clean up and record "skipped".
    if total_rows < threshold_rows:
        _log(
            f"Skipped: {total_rows:,} rows < threshold {threshold_rows:,}; "
            "deleting partial shards"
        )
        _cleanup_shards()
        _write_manifest(
            manifest_path,
            status="skipped",
            original_csv=abs_input,
            shard_paths=[],
            total_rows=total_rows,
            shard_count=0,
            rows_per_shard=rows_per_shard,
            threshold_rows=threshold_rows,
            elapsed=elapsed,
        )
        return 0

    # Validate at least one shard was produced with data.
    if not shard_paths:
        _log("ERROR: split produced no shards (unexpected; original may be empty)")
        return 1

    actual_shard_count = len(shard_paths)
    _log(f"Split complete: {total_rows:,} rows -> {actual_shard_count} shards")
    _log(f"Elapsed: {elapsed:.1f}s ({total_rows / max(elapsed, 0.001) / 1_000_000:.2f}M rows/s)")

    _write_manifest(
        manifest_path,
        status="done",
        original_csv=abs_input,
        shard_paths=[os.path.abspath(p) for p in shard_paths],
        total_rows=total_rows,
        shard_count=actual_shard_count,
        rows_per_shard=rows_per_shard,
        threshold_rows=threshold_rows,
        elapsed=elapsed,
    )
    return 0


def _write_manifest(
    path: str,
    *,
    status: str,
    original_csv: str,
    shard_paths: list[str],
    total_rows: int,
    shard_count: int,
    rows_per_shard: int,
    threshold_rows: int,
    elapsed: float,
) -> None:
    data = {
        "status": status,
        "original_csv": original_csv,
        "shard_paths": shard_paths,
        "total_rows": total_rows,
        "shard_count": shard_count,
        "rows_per_shard": rows_per_shard,
        "threshold_rows": threshold_rows,
        "elapsed_seconds": round(elapsed, 2),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Split a large CSV into fixed-size shards for genmcf parallelism."
    )
    parser.add_argument("--input", required=True, help="Path to source CSV file")
    parser.add_argument("--output-dir", required=True, help="Directory for shard files")
    parser.add_argument(
        "--rows-per-shard",
        type=int,
        default=_DEFAULT_ROWS_PER_SHARD,
        help=f"Data rows per shard (default: {_DEFAULT_ROWS_PER_SHARD:,})",
    )
    parser.add_argument(
        "--threshold-rows",
        type=int,
        default=_DEFAULT_THRESHOLD_ROWS,
        help=(
            f"Minimum row count to trigger splitting (default: {_DEFAULT_THRESHOLD_ROWS:,}). "
            "If source CSV has fewer rows, shards are deleted and status=skipped is written."
        ),
    )
    parser.add_argument(
        "--manifest",
        default="",
        help="Path for manifest JSON output (default: <output-dir>/manifest.json)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        _log(f"ERROR: input file not found: {args.input}")
        return 1

    if args.rows_per_shard <= 0:
        _log(f"ERROR: --rows-per-shard must be > 0 (got {args.rows_per_shard})")
        return 1

    manifest_path = args.manifest or os.path.join(args.output_dir, "manifest.json")
    os.makedirs(args.output_dir, exist_ok=True)

    return split_csv(
        input_path=args.input,
        output_dir=args.output_dir,
        rows_per_shard=args.rows_per_shard,
        threshold_rows=args.threshold_rows,
        manifest_path=manifest_path,
    )


if __name__ == "__main__":
    sys.exit(main())
