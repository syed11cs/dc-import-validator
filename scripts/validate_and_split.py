#!/usr/bin/env python3
"""
Combined CSV quality validation + shard splitting in a single streaming pass.

Replaces the sequential validate_csv_quality.py → split_csv_for_genmcf.py
pipeline with one disk-read, performing both operations concurrently on each row.

Quality checks (same as validate_csv_quality.py):
  - Duplicate column names in header
  - Columns that are entirely empty (all cells empty/whitespace)
  - Duplicate rows (blake2b-128 fingerprint; fail-fast after first duplicate)
  - Non-numeric values in the value column

Shard splitting (same as split_csv_for_genmcf.py):
  - Splits input into fixed-size CSV shards (rows_per_shard rows each)
  - Every shard includes the header row
  - Shards are deleted when total rows < threshold_rows (below-threshold guard)

Output files (schemas unchanged from the individual scripts):
  --output-details PATH   Quality check details JSON
                          {empty_columns, duplicate_columns, duplicate_rows, non_numeric_rows}
  --manifest PATH         Split manifest JSON
                          {status, original_csv, shard_paths, total_rows, shard_count,
                           rows_per_shard, threshold_rows, elapsed_seconds}

Exit codes:
  0  Quality checks passed; split done (or status=skipped when below threshold)
  1  Quality check failed (details written to --output-details) or hard I/O error

Pass --no-split to validate only (no shards produced, no manifest written).

Usage:
  # Validate + split in one pass
  python validate_and_split.py --input FILE --output-dir DIR \\
      [--rows-per-shard N] [--threshold-rows N] [--manifest PATH] \\
      [--output-details PATH] [--value-column COL] [--allow-empty-columns]
      [--no-dup-check]

  # Validate only (split disabled)
  python validate_and_split.py --input FILE --no-split \\
      [--output-details PATH] [--value-column COL] [--allow-empty-columns]
      [--no-dup-check]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
from pathlib import Path


_DEFAULT_ROWS_PER_SHARD = 1_000_000
_DEFAULT_THRESHOLD_ROWS = 5_000_000


def _log(msg: str) -> None:
    print(f"[VALIDATE+SPLIT] {msg}", flush=True)


def _is_empty(s: str) -> bool:
    return not s or not s.strip()


def _is_numeric(s: str) -> bool:
    if _is_empty(s):
        return True
    try:
        float(s.strip())
        return True
    except ValueError:
        return False


def _write_details(
    path: str,
    *,
    empty_columns: list[str],
    duplicate_columns: list[str],
    duplicate_rows: list[int],
    non_numeric_rows: list[int],
) -> None:
    data = {
        "empty_columns": empty_columns,
        "duplicate_columns": duplicate_columns,
        "duplicate_rows": duplicate_rows,
        "non_numeric_rows": non_numeric_rows,
    }
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


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
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _cleanup_shards(shard_paths: list[str]) -> None:
    for p in shard_paths:
        try:
            os.remove(p)
        except OSError:
            pass


def run(
    input_path: str,
    output_dir: str | None,
    rows_per_shard: int,
    threshold_rows: int,
    manifest_path: str | None,
    output_details_path: str | None,
    value_column: str | None,
    allow_empty_columns: bool,
    no_split: bool,
    no_dup_check: bool = False,
) -> int:
    """Combined validate + split. Returns 0 on success, 1 on failure."""
    t_start = time.monotonic()
    abs_input = os.path.abspath(input_path)
    basename = os.path.splitext(os.path.basename(input_path))[0]

    splitting = not no_split and output_dir is not None

    if no_dup_check:
        _log("Duplicate row check: disabled (--no-dup-check)")

    if splitting:
        os.makedirs(output_dir, exist_ok=True)
        _log(f"Input:          {abs_input}")
        _log(f"Output dir:     {output_dir}")
        _log(f"Rows per shard: {rows_per_shard:,}")
        _log(f"Threshold:      {threshold_rows:,} rows")
    else:
        _log(f"Validating: {abs_input} (split disabled)")

    # ── Validation state ──────────────────────────────────────────────────────
    seen_nonempty: set[str] = set()
    seen_row_hashes: set[bytes] = set()
    first_dup_row: int | None = None
    bad_rows: list[int] = []
    duplicate_columns: list[str] = []

    # ── Splitting state ───────────────────────────────────────────────────────
    shard_paths: list[str] = []
    shard_index = 0
    total_rows = 0
    rows_in_current_shard = 0
    current_shard_file = None
    current_shard_writer = None

    def _open_shard(hdr: list[str]) -> tuple:
        nonlocal shard_index
        path = os.path.join(output_dir, f"{basename}_shard_{shard_index:04d}.csv")
        fh = open(path, "w", newline="", encoding="utf-8")
        writer = csv.writer(fh)
        writer.writerow(hdr)
        shard_index += 1
        return path, fh, writer

    try:
        with open(abs_input, newline="", encoding="utf-8") as fh_in:
            reader = csv.reader(fh_in)

            # ── Header ────────────────────────────────────────────────────────
            try:
                header = next(reader)
            except StopIteration:
                _log("ERROR: input CSV is empty (no header row)")
                return 1

            if not header:
                _log("ERROR: CSV header is empty")
                return 1

            # ── Duplicate column check (header only) ─────────────────────────
            seen_names: dict[str, list[int]] = {}
            for i, col in enumerate(header):
                name = (col or "").strip()
                seen_names.setdefault(name, []).append(i)
            duplicate_columns = [n for n, idxs in seen_names.items() if len(idxs) > 1]

            # ── Pre-compute column indices for the hot path ───────────────────
            # Using list indices (not dict) avoids per-row dict allocation.
            key_col_pairs: list[tuple[str, int]] = [
                (col, i) for i, col in enumerate(header) if col
            ]
            all_key_col_names: list[str] = [col for col, _ in key_col_pairs]
            check_value_col = bool(value_column and value_column in header)
            value_col_idx = header.index(value_column) if check_value_col else -1

            # ── Open first shard ──────────────────────────────────────────────
            if splitting:
                shard_path, current_shard_file, current_shard_writer = _open_shard(header)
                shard_paths.append(shard_path)

            # ── Single streaming pass ─────────────────────────────────────────
            for i, row in enumerate(reader):
                row_num = i + 2  # 1-based; header = row 1
                total_rows += 1

                # 1. Empty column tracking — short-circuits once all seen
                if len(seen_nonempty) < len(key_col_pairs):
                    for col, idx in key_col_pairs:
                        if col not in seen_nonempty:
                            val = row[idx] if idx < len(row) else ""
                            if val and val.strip():
                                seen_nonempty.add(col)

                # 2. Duplicate row detection (fail-fast; hash set stops growing
                #    after the first duplicate is found)
                if not no_dup_check and first_dup_row is None:
                    key = tuple(
                        row[idx].strip() if idx < len(row) else ""
                        for _, idx in key_col_pairs
                    )
                    h = hashlib.blake2b(repr(key).encode(), digest_size=16).digest()
                    if h in seen_row_hashes:
                        first_dup_row = row_num
                    else:
                        seen_row_hashes.add(h)

                # 3. Non-numeric value column
                if check_value_col and value_col_idx >= 0:
                    val = row[value_col_idx] if value_col_idx < len(row) else ""
                    if not _is_numeric(val):
                        bad_rows.append(row_num)

                # 4. Shard writer — roll over when current shard is full
                if splitting:
                    if rows_in_current_shard >= rows_per_shard:
                        current_shard_file.close()
                        current_shard_file = None
                        shard_path, current_shard_file, current_shard_writer = _open_shard(header)
                        shard_paths.append(shard_path)
                        rows_in_current_shard = 0
                    current_shard_writer.writerow(row)
                    rows_in_current_shard += 1

        if current_shard_file is not None:
            current_shard_file.close()
            current_shard_file = None

    except (OSError, csv.Error, UnicodeDecodeError) as exc:
        if current_shard_file is not None:
            try:
                current_shard_file.close()
            except Exception:
                pass
        _cleanup_shards(shard_paths)
        _log(f"ERROR: {exc}")
        return 1

    elapsed = time.monotonic() - t_start

    # ── Assemble validation results ───────────────────────────────────────────
    errors: list[str] = []

    if duplicate_columns:
        errors.append(
            f"Duplicate column name(s): {', '.join(repr(d) for d in duplicate_columns)}"
        )

    empty_columns = [col for col in all_key_col_names if col not in seen_nonempty]
    if empty_columns and not allow_empty_columns:
        for col in empty_columns:
            errors.append(f"Column is entirely empty: {col!r}")

    if first_dup_row is not None:
        errors.append(
            f"Duplicate row at 1-based row {first_dup_row} (header is row 1)"
        )

    if bad_rows:
        sample = bad_rows[:5]
        more = f" (and {len(bad_rows) - 5} more)" if len(bad_rows) > 5 else ""
        errors.append(
            f"Non-numeric value(s) in column {value_column!r} at row(s) {sample}{more}".strip()
        )

    # ── Write validation details (same schema as validate_csv_quality.py) ─────
    details_written = bool(errors) or (allow_empty_columns and empty_columns)
    if output_details_path and details_written:
        try:
            _write_details(
                output_details_path,
                empty_columns=empty_columns,
                duplicate_columns=duplicate_columns,
                duplicate_rows=[first_dup_row] if first_dup_row is not None else [],
                non_numeric_rows=bad_rows,
            )
        except OSError as exc:
            _log(f"WARNING: could not write details file: {exc}")

    # ── Validation failed ─────────────────────────────────────────────────────
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        _cleanup_shards(shard_paths)
        return 1

    # ── Split: below-threshold guard ──────────────────────────────────────────
    if splitting:
        if total_rows < threshold_rows:
            _log(
                f"Skipped: {total_rows:,} rows < threshold {threshold_rows:,}; "
                "deleting partial shards"
            )
            _cleanup_shards(shard_paths)
            if manifest_path:
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
        else:
            actual_shard_count = len(shard_paths)
            _log(
                f"Split complete: {total_rows:,} rows -> {actual_shard_count} shards "
                f"in {elapsed:.1f}s "
                f"({total_rows / max(elapsed, 0.001) / 1_000_000:.2f}M rows/s)"
            )
            if manifest_path:
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

    dup_check_state = "disabled" if no_dup_check else "enabled"
    _log(f"Validation passed: {total_rows:,} rows in {elapsed:.1f}s (dup_check={dup_check_state})")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Combined CSV quality validation + shard splitting in one streaming pass. "
            "Produces the same output files as validate_csv_quality.py (--output-details) "
            "and split_csv_for_genmcf.py (--manifest) but reads the input only once."
        )
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument(
        "--output-dir",
        default="",
        help="Directory for shard files (required unless --no-split)",
    )
    parser.add_argument(
        "--no-split",
        action="store_true",
        help="Validate only; do not produce shard files or a manifest",
    )
    parser.add_argument(
        "--rows-per-shard",
        type=int,
        default=_DEFAULT_ROWS_PER_SHARD,
        help=f"Data rows per shard (default: {_DEFAULT_ROWS_PER_SHARD:,}). Ignored with --no-split.",
    )
    parser.add_argument(
        "--threshold-rows",
        type=int,
        default=_DEFAULT_THRESHOLD_ROWS,
        help=(
            f"Minimum rows to trigger splitting (default: {_DEFAULT_THRESHOLD_ROWS:,}). "
            "Below this, shards are deleted and manifest records status=skipped. "
            "Ignored with --no-split."
        ),
    )
    parser.add_argument(
        "--manifest",
        default="",
        help=(
            "Path for manifest JSON (default: <output-dir>/manifest.json). "
            "Ignored with --no-split."
        ),
    )
    parser.add_argument(
        "--output-details",
        default="",
        help="Path to write quality check details JSON when checks fail (or when "
             "--allow-empty-columns and empty columns are found).",
    )
    parser.add_argument(
        "--value-column",
        default="value",
        help="Column name for numeric check (default: value). Pass empty string to skip.",
    )
    parser.add_argument(
        "--allow-empty-columns",
        action="store_true",
        help="Treat entirely empty columns as non-fatal: record in details but do not fail.",
    )
    parser.add_argument(
        "--no-dup-check",
        action="store_true",
        help=(
            "Skip the duplicate row check. Saves ~165s on very large CSVs (38M+ rows) "
            "at the cost of not detecting duplicate rows. Set CSV_DUP_CHECK=false to "
            "enable this via the shell wrapper."
        ),
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        _log(f"ERROR: input file not found: {args.input}")
        return 1

    if not args.no_split and not args.output_dir:
        _log("ERROR: --output-dir is required unless --no-split is set")
        return 1

    if args.rows_per_shard <= 0:
        _log(f"ERROR: --rows-per-shard must be > 0 (got {args.rows_per_shard})")
        return 1

    output_dir = args.output_dir if not args.no_split else None
    manifest_path: str | None = None
    if not args.no_split and args.output_dir:
        manifest_path = args.manifest or os.path.join(args.output_dir, "manifest.json")

    value_col = (args.value_column or "").strip() or None

    return run(
        input_path=args.input,
        output_dir=output_dir,
        rows_per_shard=args.rows_per_shard,
        threshold_rows=args.threshold_rows,
        manifest_path=manifest_path,
        output_details_path=(args.output_details or "").strip() or None,
        value_column=value_col,
        allow_empty_columns=args.allow_empty_columns,
        no_split=args.no_split,
        no_dup_check=args.no_dup_check,
    )


if __name__ == "__main__":
    sys.exit(main())
