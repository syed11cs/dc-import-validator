#!/usr/bin/env python3
"""Preflight check: required import files exist and follow expected naming.

Validates:
- TMCF file exists and has .tmcf extension
- CSV file exists and has .csv extension
- Optional stat_vars.mcf / stat_vars_schema.mcf exist and have .mcf extension

Exits 0 if all provided paths are valid, 1 otherwise (prints errors to stderr).
Optional --output-errors PATH writes {"errors": ["...", ...]} as JSON when validation fails.

Usage:
  python validate_import_files.py --tmcf path/to/file.tmcf --csv path/to/file.csv
  python validate_import_files.py --tmcf file.tmcf --csv file.csv [--stat-vars-mcf x.mcf] [--output-errors PATH]
"""

import argparse
import json
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Validate import files exist and have expected extensions"
    )
    parser.add_argument("--tmcf", required=True, help="Path to TMCF file")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--stat-vars-mcf", default="", help="Optional stat_vars.mcf path")
    parser.add_argument("--stat-vars-schema-mcf", default="", help="Optional stat_vars_schema.mcf path")
    parser.add_argument(
        "--output-errors",
        default="",
        help="When validation fails, write {\"errors\": [...]} to this JSON file.",
    )
    args = parser.parse_args()

    errors = []

    tmcf = Path(args.tmcf)
    if not tmcf.exists():
        errors.append(f"TMCF file not found: {args.tmcf}")
    elif tmcf.suffix.lower() not in (".tmcf", ".mcf"):
        errors.append(f"TMCF file must have .tmcf or .mcf extension: {args.tmcf}")

    csv_path = Path(args.csv)
    if not csv_path.exists():
        errors.append(f"CSV file not found: {args.csv}")
    elif csv_path.suffix.lower() != ".csv":
        errors.append(f"CSV file must have .csv extension: {args.csv}")

    for label, path_arg in (
        ("stat_vars.mcf", args.stat_vars_mcf),
        ("stat_vars_schema.mcf", args.stat_vars_schema_mcf),
    ):
        if not path_arg or not path_arg.strip():
            continue
        p = Path(path_arg.strip())
        if not p.exists():
            errors.append(f"{label} not found: {path_arg}")
        elif p.suffix.lower() != ".mcf":
            errors.append(f"{label} must have .mcf extension: {path_arg}")

    if errors:
        if (args.output_errors or "").strip():
            out_path = Path(args.output_errors.strip())
            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump({"errors": errors}, f, indent=2)
            except OSError:
                pass
        for err in errors:
            print(err, file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
