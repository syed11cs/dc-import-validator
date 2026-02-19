#!/usr/bin/env python3
"""Post-process validation_output.json: convert FAILED to WARNING for warn_only rules.

Only Errors (FAILED) block; Warnings do not. This script applies warn_only
overrides without modifying the Data Commons validation runner.
"""

import argparse
import json
import sys


def apply_warn_only(
    validation_output_path: str,
    warn_only_rules_path: str,
    dataset: str,
) -> tuple[bool, int]:
    """Convert FAILED to WARNING for rules in warn_only list. Modifies file in place.
    Returns (success, converted_count)."""
    try:
        with open(validation_output_path, encoding="utf-8") as f:
            results = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading {validation_output_path}: {e}", file=sys.stderr)
        return False, 0

    try:
        with open(warn_only_rules_path, encoding="utf-8") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading {warn_only_rules_path}: {e}", file=sys.stderr)
        return False, 0

    raw = config.get(dataset)
    if not isinstance(raw, list):
        raw = []
    warn_only = {str(x).strip().lower() for x in raw if x is not None and str(x).strip()}
    if not warn_only:
        return True, 0

    converted = 0
    for r in results:
        if not isinstance(r, dict):
            continue
        if r.get("status") == "FAILED":
            name = r.get("validation_name")
            if name is not None and str(name).strip().lower() in warn_only:
                r["status"] = "WARNING"
                converted += 1

    if converted:
        with open(validation_output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)

    return True, converted


def has_blockers(validation_output_path: str) -> bool:
    """Return True if any result is FAILED (blocker)."""
    try:
        with open(validation_output_path, encoding="utf-8") as f:
            results = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return True  # Assume fail on error
    if not isinstance(results, list):
        return True
    return any(r.get("status") == "FAILED" for r in results if isinstance(r, dict))


def main():
    parser = argparse.ArgumentParser(
        description="Apply warn_only overrides to validation_output.json"
    )
    parser.add_argument("validation_output", help="Path to validation_output.json")
    parser.add_argument(
        "--warn_only_rules",
        required=True,
        help="Path to warn_only_rules.json (dataset -> [rule_ids])",
    )
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument(
        "--check_blockers",
        action="store_true",
        help="Exit 1 if any FAILED remain (for pass/fail determination)",
    )
    args = parser.parse_args()

    success, converted = apply_warn_only(
        args.validation_output, args.warn_only_rules, args.dataset
    )
    if not success:
        sys.exit(1)

    if converted:
        msg = f"1 failure(s) converted to warning(s)" if converted == 1 else f"{converted} failure(s) converted to warning(s)"
        print(f"[INFO] {msg}", file=sys.stderr)

    if args.check_blockers and has_blockers(args.validation_output):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
