#!/usr/bin/env python3
"""Min-value validation rule with StatVar exemptions for negative-allowed series.

Checks that MinValue for each StatVar is not below a defined minimum. StatVars
whose names contain any of NEGATIVE_ALLOWED_KEYWORDS (Incremental, GrowthRate,
Net, Change) are skipped so that negative values are allowed for those series.
Returns a result dict matching the validation_output.json schema.

Used by run_validation.py; does not modify upstream DC validator.
"""

import argparse
import csv
import json
import sys
from pathlib import Path

# StatVar names containing any of these are exempt from min-value check (allow negatives).
NEGATIVE_ALLOWED_KEYWORDS = frozenset({
    "Incremental", "GrowthRate", "Net", "Change",
})


def _parse_min_value(val: str) -> float | None:
    """Parse MinValue cell to float. Returns None if empty or invalid."""
    if val is None or (isinstance(val, str) and not val.strip()):
        return None
    try:
        return float(str(val).strip())
    except ValueError:
        return None


def run(
    stats_summary_path: str | None,
    params: dict,
    rule_id: str = "check_min_value",
) -> dict:
    """Run the min-value check with NEGATIVE_ALLOWED_KEYWORDS exemption.

    Args:
        stats_summary_path: Path to summary_report.csv (or None if missing).
        params: Dict with 'minimum' (int or float).
        rule_id: validation_name to use in the result.

    Returns:
        One result entry in validation_output schema:
        validation_name, status, message, details, validation_params.
    """
    if "minimum" not in params:
        return {
            "validation_name": rule_id,
            "status": "CONFIG_ERROR",
            "message": "Configuration error: 'minimum' key not specified.",
            "details": {},
            "validation_params": params,
        }

    min_val = params["minimum"]
    try:
        min_val = float(min_val)
    except (TypeError, ValueError):
        min_val = 0

    if not stats_summary_path or not Path(stats_summary_path).is_file():
        return {
            "validation_name": rule_id,
            "status": "PASSED",
            "message": "",
            "details": {"rows_processed": 0, "rows_succeeded": 0, "rows_failed": 0},
            "validation_params": params,
        }

    rows_processed = 0
    rows_failed = 0
    failed_rows_details = []
    header = None

    with open(stats_summary_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        if "MinValue" not in header:
            return {
                "validation_name": rule_id,
                "status": "DATA_ERROR",
                "message": "Input data is missing required column: 'MinValue'.",
                "details": {},
                "validation_params": params,
            }

        for row in reader:
            stat_var = row.get("StatVar", "Unknown")
            stat_var_str = str(stat_var)
            if any(kw in stat_var_str for kw in NEGATIVE_ALLOWED_KEYWORDS):
                continue
            rows_processed += 1
            min_value = _parse_min_value(row.get("MinValue", ""))
            if min_value is None:
                continue
            if min_value < min_val:
                rows_failed += 1
                failed_rows_details.append({
                    "stat_var": stat_var,
                    "actual_min_value": min_value,
                    "minimum": min_val,
                })

    rows_succeeded = rows_processed - rows_failed

    if rows_failed > 0:
        return {
            "validation_name": rule_id,
            "status": "WARNING",
            "message": f"{rows_failed} out of {rows_processed} StatVars failed the minimum value check.",
            "details": {
                "failed_rows": failed_rows_details,
                "rows_processed": rows_processed,
                "rows_succeeded": rows_succeeded,
                "rows_failed": rows_failed,
            },
            "validation_params": params,
        }

    return {
        "validation_name": rule_id,
        "status": "PASSED",
        "message": "",
        "details": {
            "rows_processed": rows_processed,
            "rows_succeeded": rows_succeeded,
            "rows_failed": rows_failed,
        },
        "validation_params": params,
    }


def main() -> None:
    """CLI: read stats summary CSV, run rule, print result as JSON line."""
    parser = argparse.ArgumentParser(
        description="Min-value check with NEGATIVE_ALLOWED_KEYWORDS exemption"
    )
    parser.add_argument("--stats_summary", required=True, help="Path to summary_report.csv")
    parser.add_argument("--params", default="{}", help="JSON params (e.g. {\"minimum\": 0})")
    parser.add_argument("--rule_id", default="check_min_value", help="Rule ID for validation_name")
    args = parser.parse_args()

    params = json.loads(args.params) if args.params else {}
    result = run(args.stats_summary, params, rule_id=args.rule_id)
    print(json.dumps(result), file=sys.stdout)


if __name__ == "__main__":
    main()
