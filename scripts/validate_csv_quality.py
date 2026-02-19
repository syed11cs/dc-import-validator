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

Usage:
  python validate_csv_quality.py --csv path/to/file.csv
  python validate_csv_quality.py --csv file.csv [--value-column value]
"""

import argparse
import csv
import re
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


def validate_csv(csv_path: str, value_column: str | None) -> list[str]:
    """Run quality checks. Returns list of error messages (empty if valid)."""
    errors = []
    path = Path(csv_path)
    if not path.exists():
        return [f"CSV file not found: {csv_path}"]

    try:
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        return [f"Error reading CSV: {e}"]

    if not header:
        return ["CSV has no header row"]

    # 1. Duplicate column names
    seen: dict[str, list[int]] = {}
    for i, col in enumerate(header):
        name = (col or "").strip()
        if name not in seen:
            seen[name] = []
        seen[name].append(i)
    dupes = [name for name, indices in seen.items() if len(indices) > 1]
    if dupes:
        errors.append(f"Duplicate column name(s): {', '.join(repr(d) for d in dupes)}")

    # Read all rows for remaining checks
    try:
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return errors + ["CSV has no header"]
            rows = list(reader)
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        return errors + [f"Error reading CSV rows: {e}"]

    if not rows:
        return errors  # no rows to check for empty cols / duplicates / numeric

    # 2. Empty columns
    for col in (reader.fieldnames or header):
        if not col:
            continue
        if all(_is_empty(row.get(col, "")) for row in rows):
            errors.append(f"Column is entirely empty: {col!r}")

    # 3. Duplicate rows (normalize: tuple of stripped values in header order)
    key_cols = [c for c in (reader.fieldnames or header) if c]
    if key_cols:
        seen_rows: set[tuple[str, ...]] = set()
        for i, row in enumerate(rows):
            key = tuple(str(row.get(c, "")).strip() for c in key_cols)
            if key in seen_rows:
                errors.append(f"Duplicate row at 1-based row {i + 2} (header is row 1)")
                break
            seen_rows.add(key)

    # 4. Non-numeric values in value column (optional)
    if value_column and value_column in (reader.fieldnames or header):
        bad_rows = []
        for i, row in enumerate(rows):
            val = row.get(value_column, "")
            if not _is_numeric(val):
                bad_rows.append(i + 2)  # 1-based, +1 for header
        if bad_rows:
            sample = bad_rows[:5]
            more = f" (and {len(bad_rows) - 5} more)" if len(bad_rows) > 5 else ""
            errors.append(
                f"Non-numeric value(s) in column {value_column!r} at row(s) {sample}{more}".strip()
            )

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="CSV data quality checks (duplicate columns, empty columns, duplicate rows, non-numeric value column)"
    )
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument(
        "--value-column",
        default="value",
        help="Column name for numeric check (default: value). Pass empty to skip.",
    )
    args = parser.parse_args()

    value_col = (args.value_column or "").strip() or None
    errors = validate_csv(args.csv, value_col)

    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
