#!/usr/bin/env python3
"""Check that StatVars/NumObservations in summary match counters in report.json.

Ensures sum(NumObservations) in stats == NumNodeSuccesses in report LEVEL_INFO.
Both inputs must come from the same run (e.g. genmcf output): use report.json
from the same directory as summary_report.csv. Do not mix with lint-phase report
or resolution differences will cause false mismatches.
"""

import argparse
import json
import sys
import pandas as pd


def check_counters_match(stats_summary_path: str, report_path: str) -> tuple[bool, str]:
    """Returns (success, message)."""
    try:
        stats_df = pd.read_csv(stats_summary_path)
    except Exception as e:
        return False, f"Failed to read stats: {e}"

    try:
        with open(report_path, encoding="utf-8") as f:
            report = json.load(f)
    except Exception as e:
        return False, f"Failed to read report: {e}"

    if "NumObservations" not in stats_df.columns:
        return False, "Stats missing NumObservations column"

    level_info = report.get("levelSummary", {}).get("LEVEL_INFO", {})
    counters = level_info.get("counters", {})
    num_node_successes = counters.get("NumNodeSuccesses")
    if num_node_successes is None:
        return True, "NumNodeSuccesses not in report (skip check)"

    try:
        expected = int(num_node_successes)
    except (ValueError, TypeError):
        return False, f"Invalid NumNodeSuccesses: {num_node_successes}"

    try:
        col = pd.to_numeric(stats_df["NumObservations"], errors="coerce")
        total = col.sum()
        if pd.isna(total):
            return False, "NumObservations sum is NaN (non-numeric or missing values)"
        actual = int(total)
    except (ValueError, TypeError, KeyError) as e:
        return False, f"Invalid or missing NumObservations: {e}"
    if actual != expected:
        return False, (
            f"NumObservations sum ({actual}) != NumNodeSuccesses ({expected})"
        )
    return True, f"Match: {actual} observations"


def main():
    parser = argparse.ArgumentParser(description="Check counters match")
    parser.add_argument("--stats_summary", required=True, help="Path to summary_report.csv")
    parser.add_argument("--report", required=True, help="Path to report.json")
    args = parser.parse_args()

    ok, msg = check_counters_match(args.stats_summary, args.report)
    if ok:
        print(f"[INFO] {msg}")
        sys.exit(0)
    else:
        print(f"[ERROR] {msg}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
