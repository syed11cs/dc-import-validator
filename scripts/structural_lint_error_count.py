#!/usr/bin/env python3
"""Structural lint error count validation rule.

Counts LEVEL_ERROR counters in the lint report EXCLUDING any counter whose key
starts with Existence_FailedDcCall_ (resolution diagnostics). Applies the same
threshold logic as LINT_ERROR_COUNT. Returns a result dict matching the
validation_output.json schema for integration with the HTML report and pipeline.

Used by run_validation.py; can be run standalone for testing.
"""

import json
import sys
from pathlib import Path

# Counter names with this prefix are resolution diagnostics (DC API dependent);
# they are excluded so existence resolution failures do not block validation.
EXCLUDE_PREFIX = "Existence_FailedDcCall_"


def compute_structural_lint_count(report: dict) -> int:
    """Sum LEVEL_ERROR counters excluding keys starting with EXCLUDE_PREFIX."""
    if not report:
        return 0
    counters = (
        report.get("levelSummary", {})
        .get("LEVEL_ERROR", {})
        .get("counters", {})
    )
    if not counters:
        return 0
    return sum(
        int(value)
        for key, value in counters.items()
        if not key.startswith(EXCLUDE_PREFIX)
    )


def run(
    report: dict,
    params: dict,
    rule_id: str = "check_structural_lint_error_count",
) -> dict:
    """Run the structural lint error count check.

    Args:
        report: Lint report JSON (as dict). Can be None/empty.
        params: Dict with optional 'threshold' (int, default 0).
        rule_id: validation_name to use in the result (default same as rule_id in config).

    Returns:
        One result entry in validation_output schema:
        validation_name, status, message, details, validation_params.
    """
    threshold = params.get("threshold", 0)
    lint_error_count = compute_structural_lint_count(report)

    if lint_error_count > threshold:
        return {
            "validation_name": rule_id,
            "status": "FAILED",
            "message": (
                f"Found {lint_error_count} structural schema/MCF lint errors "
                f"(non-resolution), which exceeds the threshold of {threshold}."
            ),
            "details": {"lint_error_count": lint_error_count},
            "validation_params": params,
        }
    return {
        "validation_name": rule_id,
        "status": "PASSED",
        "message": "",
        "details": {"lint_error_count": lint_error_count},
        "validation_params": params,
    }


def main() -> None:
    """CLI: read lint report from file, run rule, print result as JSON line."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Structural lint error count validation (excludes Existence_FailedDcCall_*)"
    )
    parser.add_argument(
        "--lint_report",
        required=True,
        help="Path to lint report JSON file",
    )
    parser.add_argument(
        "--params",
        default="{}",
        help="JSON params object (default: {\"threshold\": 0})",
    )
    parser.add_argument(
        "--rule_id",
        default="check_structural_lint_error_count",
        help="Rule ID for validation_name",
    )
    args = parser.parse_args()

    path = Path(args.lint_report)
    if not path.exists():
        print(json.dumps({
            "validation_name": args.rule_id,
            "status": "PASSED",
            "message": "",
            "details": {"lint_error_count": 0},
            "validation_params": {},
        }), file=sys.stdout)
        sys.exit(0)

    with open(path, encoding="utf-8") as f:
        report = json.load(f)
    params = json.loads(args.params) if args.params else {}
    result = run(report, params, rule_id=args.rule_id)
    print(json.dumps(result), file=sys.stdout)


if __name__ == "__main__":
    main()
