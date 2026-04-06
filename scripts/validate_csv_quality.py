#!/usr/bin/env python3
"""CSV data quality checks: duplicate columns, empty columns, duplicate rows, non-numeric value column.

Runs before genmcf to fail fast on obvious CSV issues. Does not replace dc-import
or import_validation; those remain authoritative for schema and business rules.

Checks:
- Duplicate column names in header
- Columns that are entirely empty (all cells empty/whitespace)
- Duplicate rows (identical values in all columns)
- Non-numeric values in the value column (optional; column name configurable)

Exits 0 if all checks pass, 1 otherwise (errors to stderr).
Optional --output-details PATH writes structured details (empty_columns, duplicate_columns,
duplicate_rows, non_numeric_rows) as JSON when checks fail, for UI consumption.

Usage:
  python validate_csv_quality.py --csv path/to/file.csv
  python validate_csv_quality.py --csv file.csv [--value-column value] [--output-details PATH]
  python validate_csv_quality.py --csv file.csv --allow-empty-columns  # treat empty columns as non-fatal (e.g. custom dataset)
"""

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path


def _is_empty(s: str) -> bool:
    return s is None or (isinstance(s, str) and not s.strip())


def _is_numeric(s: str) -> bool:
    if _is_empty(s):
        return True
    s = str(s).strip()
    if not s:
        return True
    try:
        float(s)
        return True
    except ValueError:
        return False


def validate_csv(
    csv_path: str,
    value_column: str | None,
    *,
    allow_empty_columns: bool = False,
) -> tuple[list[str], dict]:
    """Run quality checks. Returns (errors, details).
    errors: list of human-readable error messages (empty if valid).
    details: dict with empty_columns, duplicate_columns, duplicate_rows, non_numeric_rows (lists).
    When allow_empty_columns is True, entirely empty columns are recorded in details but not added to errors.
    """
    errors: list[str] = []
    details: dict = {
        "empty_columns": [],
        "duplicate_columns": [],
        "duplicate_rows": [],
        "non_numeric_rows": [],
    }
    path = Path(csv_path)
    if not path.exists():
        return ([f"CSV file not found: {csv_path}"], details)

    # Single file open for all checks. csv.DictReader reads the header row the
    # first time .fieldnames is accessed (before any data rows are iterated), so
    # the duplicate-column check and the streaming data pass share one open().
    # No row is held in memory beyond the current loop iteration — the original
    # list(csv.DictReader(...)) pattern loaded the entire CSV as a list of dicts,
    # which cost ~7–8× the raw CSV size in Python object overhead on large files.
    #
    # Memory profile of the streaming approach:
    #   Duplicate columns: O(columns) — header dict only
    #   Empty columns:     O(columns) — seen_nonempty set, never grows with row count
    #   Duplicate rows:    O(N) ints — one 8-byte hash per unique row (vs O(N×cols×val_len)
    #                      for full tuple storage); stops growing after first duplicate found
    #   Non-numeric:       O(bad_rows) — only offending row numbers; empty for valid data
    key_cols: list[str] = []
    seen_nonempty: set[str] = set()     # columns that have received at least one non-empty value
    seen_row_hashes: set[bytes] = set() # row fingerprints: blake2b-128 of repr(key)
    first_dup_row: int | None = None
    bad_rows: list[int] = []
    row_count = 0
    header: list[str] = []

    try:
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)

            # Accessing .fieldnames reads the header row from the file. All
            # subsequent iterations start from the first data row automatically.
            raw_fieldnames = reader.fieldnames
            if not raw_fieldnames:
                return (errors + ["CSV has no header row"], details)
            header = list(raw_fieldnames)

            # 1. Duplicate column names — inspects header only, O(columns)
            seen: dict[str, list[int]] = {}
            for i, col in enumerate(header):
                name = (col or "").strip()
                if name not in seen:
                    seen[name] = []
                seen[name].append(i)
            dupes = [name for name, indices in seen.items() if len(indices) > 1]
            if dupes:
                details["duplicate_columns"] = list(dupes)
                errors.append(f"Duplicate column name(s): {', '.join(repr(d) for d in dupes)}")

            key_cols = [c for c in header if c]
            check_value_col = bool(value_column and value_column in header)

            # Streaming pass: checks 2, 3, and 4 run concurrently in one loop.
            for i, row in enumerate(reader):
                row_count += 1
                row_num = i + 2  # 1-based; header is row 1

                # 2. Empty column tracking: add a column to seen_nonempty the
                #    first time a non-empty value is encountered. The inner loop
                #    short-circuits once all columns are accounted for.
                if len(seen_nonempty) < len(key_cols):
                    for col in key_cols:
                        if col not in seen_nonempty and not _is_empty(row.get(col, "")):
                            seen_nonempty.add(col)

                # 3. Duplicate row detection: store a 128-bit blake2b digest of
                #    repr(key) instead of the full tuple.
                #
                #    Why repr(key) rather than "|".join(key): Python's tuple repr
                #    quotes and escapes each element individually, making the encoding
                #    unambiguous — ("a|b", "c") and ("a", "|b|c") produce different
                #    reprs regardless of separator choice.
                #
                #    Why blake2b digest_size=16 rather than hash(): Python's hash()
                #    space is 2^61 (sys.hash_info.modulus is a Mersenne prime). The
                #    birthday-paradox collision probability at 30M rows is ~1 in 5,000
                #    — non-trivial for a validator that must not produce false positives.
                #    A 128-bit digest reduces that probability to ~10^-24.
                #
                #    Memory: 16 bytes per unique row seen. Stops accumulating after
                #    the first duplicate is found (fail-fast, same behaviour as before).
                if first_dup_row is None:
                    key = tuple(str(row.get(c, "")).strip() for c in key_cols)
                    h = hashlib.blake2b(repr(key).encode(), digest_size=16).digest()
                    if h in seen_row_hashes:
                        first_dup_row = row_num
                    else:
                        seen_row_hashes.add(h)

                # 4. Non-numeric value column: accumulate all offending row
                #    numbers; only bad rows consume memory.
                if check_value_col:
                    if not _is_numeric(row.get(value_column, "")):
                        bad_rows.append(row_num)

    except (OSError, csv.Error, UnicodeDecodeError) as e:
        return (errors + [f"Error reading CSV rows: {e}"], details)

    if row_count == 0:
        return (errors, details)

    # 2. Empty columns — assemble results after the streaming pass
    for col in key_cols:
        if col not in seen_nonempty:
            details["empty_columns"].append(col)
            if not allow_empty_columns:
                errors.append(f"Column is entirely empty: {col!r}")

    # 3. Duplicate rows
    if first_dup_row is not None:
        details["duplicate_rows"].append(first_dup_row)
        errors.append(f"Duplicate row at 1-based row {first_dup_row} (header is row 1)")

    # 4. Non-numeric values in value column
    if bad_rows:
        details["non_numeric_rows"] = bad_rows
        sample = bad_rows[:5]
        more = f" (and {len(bad_rows) - 5} more)" if len(bad_rows) > 5 else ""
        errors.append(
            f"Non-numeric value(s) in column {value_column!r} at row(s) {sample}{more}".strip()
        )

    return (errors, details)


def main():
    parser = argparse.ArgumentParser(
        description="CSV data quality checks (duplicate columns, empty columns, duplicate rows, non-numeric value column)"
    )
    parser.add_argument("--csv", required=True, action="append", help="Path to CSV file (repeatable)")
    parser.add_argument(
        "--value-column",
        default="value",
        help="Column name for numeric check (default: value). Pass empty to skip.",
    )
    parser.add_argument(
        "--output-details",
        default="",
        help="When checks fail, write structured details (empty_columns, duplicate_columns, duplicate_rows, non_numeric_rows) to this JSON file. Also written on success when --allow-empty-columns is set and empty_columns is non-empty.",
    )
    parser.add_argument(
        "--allow-empty-columns",
        action="store_true",
        help="Treat entirely empty columns as non-fatal: record in details but do not fail. Use for custom datasets.",
    )
    args = parser.parse_args()

    value_col = (args.value_column or "").strip() or None
    all_errors: list[str] = []
    merged_details: dict = {
        "empty_columns": [],
        "duplicate_columns": [],
        "duplicate_rows": [],
        "non_numeric_rows": [],
    }
    for csv_arg in args.csv:
        errors, details = validate_csv(
            csv_arg, value_col, allow_empty_columns=args.allow_empty_columns
        )
        all_errors.extend(errors)
        for key in merged_details:
            merged_details[key].extend(details.get(key, []))

    out_path = (args.output_details or "").strip()
    if out_path:
        out_path = Path(out_path)
        write_details = bool(all_errors) or (
            args.allow_empty_columns and merged_details.get("empty_columns")
        )
        if write_details:
            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(merged_details, f, indent=2)
            except OSError:
                pass

    if all_errors:
        for err in all_errors:
            print(err, file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
