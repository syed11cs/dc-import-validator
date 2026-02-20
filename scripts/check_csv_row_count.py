#!/usr/bin/env python3
"""CSV row count must not exceed threshold (pre-import constraint).

Two modes:

1) Pre-Import (--output PATH): Count rows, write one result entry to PATH as JSON.
   Used in Step 0 (Pre-Import Checks). Caller gates on FAILED + not warn-only.
   Does not require validation_output.json.

2) Append (--validation_output PATH): Read validation_output.json, append one
   result entry, write back. Used when merging a pre-computed result after
   import_validation (legacy or when result was written in Step 0).

Exits 0 in both modes; pass/fail is in the result (apply_warn_only downgrades
FAILED to WARNING for warn_only rules).

Usage:
  python check_csv_row_count.py --csv=PATH --output=PATH [--threshold=1000]
  python check_csv_row_count.py --csv=PATH --validation_output=PATH [--threshold=1000]
"""

import argparse
import json
import sys
from pathlib import Path


def count_data_rows(csv_path: str) -> int:
    """Count lines in CSV minus header. Returns 0 if file missing or empty."""
    p = Path(csv_path)
    if not p.exists() or not p.is_file():
        return 0
    with open(p, encoding="utf-8", errors="replace") as f:
        lines = [line for line in f if line.strip()]
    if not lines:
        return 0
    return max(0, len(lines) - 1)


def build_entry(n: int, threshold: int) -> dict:
    if n > threshold:
        return {
            "validation_name": "check_csv_row_count",
            "status": "FAILED",
            "message": f"Input CSV has {n} data rows, which exceeds the sample limit of {threshold}.",
            "details": {"row_count": n, "threshold": threshold},
            "validation_params": {"threshold": threshold},
        }
    return {
        "validation_name": "check_csv_row_count",
        "status": "PASSED",
        "message": "",
        "details": {"row_count": n, "threshold": threshold},
        "validation_params": {"threshold": threshold},
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CSV row-count check: pre-import or append to validation_output.json"
    )
    parser.add_argument("--csv", required=True, help="Path to input CSV")
    parser.add_argument(
        "--output",
        help="Write single result entry to this JSON file (Pre-Import mode; array of one object)",
    )
    parser.add_argument(
        "--validation_output",
        help="Path to validation_output.json (append mode; read and update in place)",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=1000,
        help="Max allowed data rows for sample imports (default 1000)",
    )
    args = parser.parse_args()

    if bool(args.output) == bool(args.validation_output):
        print("Exactly one of --output or --validation_output is required", file=sys.stderr)
        sys.exit(2)

    n = count_data_rows(args.csv)
    entry = build_entry(n, args.threshold)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump([entry], f, indent=2, default=str)
        sys.exit(0)

    # Append mode
    out_path = Path(args.validation_output)
    if not out_path.exists():
        print(f"[WARN] {out_path} not found; skipping row count check", file=sys.stderr)
        sys.exit(0)
    try:
        with open(out_path, encoding="utf-8") as f:
            results = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] Could not read {out_path}: {e}", file=sys.stderr)
        sys.exit(0)
    if not isinstance(results, list):
        print(f"[WARN] validation_output is not a list; skipping row count check", file=sys.stderr)
        sys.exit(0)
    results.append(entry)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    sys.exit(0)


if __name__ == "__main__":
    main()
