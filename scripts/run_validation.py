#!/usr/bin/env python3
"""Orchestrate validation: run DC framework rules + our custom rules, write validation_output.json once.

This script is the single entry point for "Step 3" validation. It:
  - Loads the full validation config.
  - Runs the DC runner with a filtered config (only rules the DC framework knows;
    LINT_ERROR_COUNT (legacy; not in config) and STRUCTURAL_LINT_ERROR_COUNT are excluded).
  - Runs custom validators (e.g. STRUCTURAL_LINT_ERROR_COUNT) in this repo.
  - Merges results and writes validation_output.json once (no post-processing).

Accepts the same flags as the DC runner. Requires DATA_REPO in the environment
when there are DC rules to run.

Usage:
  DATA_REPO=/path/to/datacommonsorg/data python run_validation.py \\
    --validation_config=config.json --validation_output=out.json [--lint_report=...] [--stats_summary=...] [--differ_output=...]
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Custom validator names that we run in this repo (DC runner does not know them).
CUSTOM_VALIDATORS = frozenset({"STRUCTURAL_LINT_ERROR_COUNT"})

# Validators we do not pass to the DC runner (we replace or run them ourselves).
DC_EXCLUDE_VALIDATORS = frozenset({"LINT_ERROR_COUNT", "STRUCTURAL_LINT_ERROR_COUNT"})


def _load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _split_rules(config: dict) -> tuple[list, list]:
    """Split rules into DC rules and custom rules (only enabled)."""
    rules = config.get("rules", [])
    if not isinstance(rules, list):
        return [], []

    dc_rules = []
    custom_rules = []
    for r in rules:
        if not isinstance(r, dict):
            continue
        if not r.get("enabled", True):
            continue
        validator = r.get("validator") or ""
        if validator in CUSTOM_VALIDATORS:
            custom_rules.append(r)
        elif validator not in DC_EXCLUDE_VALIDATORS:
            dc_rules.append(r)

    return dc_rules, custom_rules


def _run_dc_runner(
    data_repo: str,
    config_path: str,
    output_path: str,
    stats_summary: str | None,
    lint_report: str | None,
    differ_output: str | None,
    python: str,
) -> list:
    """Run the DC runner; return list of result dicts."""
    cmd = [
        python, "-m", "tools.import_validation.runner",
        "--validation_config", config_path,
        "--validation_output", output_path,
    ]
    if stats_summary and os.path.isfile(stats_summary):
        cmd.extend(["--stats_summary", stats_summary])
    if lint_report and os.path.isfile(lint_report):
        cmd.extend(["--lint_report", lint_report])
    if differ_output is not None:
        cmd.extend(["--differ_output", differ_output or ""])

    result = subprocess.run(
        cmd,
        cwd=data_repo,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"DC runner exited with code {result.returncode}", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

    if not os.path.isfile(output_path):
        return []
    try:
        with open(output_path, encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            return []
        data = json.loads(raw)
    except (json.JSONDecodeError, OSError):
        return []
    return data if isinstance(data, list) else []


def _run_custom_validators(
    custom_rules: list[dict],
    lint_report_path: str | None,
) -> list[dict]:
    """Run custom validators and return result dicts (validation_output schema)."""
    from structural_lint_error_count import run as run_structural_lint  # noqa: I001

    results = []
    report = None
    if lint_report_path and os.path.isfile(lint_report_path):
        with open(lint_report_path, encoding="utf-8") as f:
            report = json.load(f)

    for rule in custom_rules:
        validator = rule.get("validator")
        rule_id = rule.get("rule_id", "")
        params = rule.get("params", {})

        if validator == "STRUCTURAL_LINT_ERROR_COUNT":
            result = run_structural_lint(report or {}, params, rule_id=rule_id)
            results.append(result)
        else:
            # Unknown custom validator; skip or could add more here
            pass

    return results


def main() -> int:
    import argparse

    script_dir = Path(__file__).resolve().parent
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))
    parser = argparse.ArgumentParser(
        description="Run validation (DC framework + custom rules), write validation_output.json once"
    )
    parser.add_argument("--validation_config", required=True, help="Path to validation config JSON")
    parser.add_argument("--validation_output", required=True, help="Path to write validation_output.json")
    parser.add_argument("--stats_summary", default=None, help="Path to stats summary (for DC rules)")
    parser.add_argument("--lint_report", default=None, help="Path to lint report JSON (for DC and custom rules)")
    parser.add_argument("--differ_output", default=None, help="Path to differ output (optional)")
    args = parser.parse_args()

    config_path = args.validation_config
    output_path = args.validation_output
    if not os.path.isfile(config_path):
        print(f"Error: validation config not found: {config_path}", file=sys.stderr)
        return 2

    config = _load_config(config_path)
    dc_rules, custom_rules = _split_rules(config)

    dc_results = []
    if dc_rules:
        data_repo = os.environ.get("DATA_REPO")
        if not data_repo or not os.path.isdir(data_repo):
            print("Error: DATA_REPO must be set and point to datacommonsorg/data when DC rules are present", file=sys.stderr)
            return 2
        python = os.environ.get("PYTHON", sys.executable)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", prefix="dc_config_", delete=False
        ) as f_config:
            json.dump({"schema_version": config.get("schema_version", "1.0"), "rules": dc_rules}, f_config)
            dc_config_path = f_config.name
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", prefix="dc_output_", delete=False
        ) as f_out:
            dc_output_path = f_out.name
        try:
            dc_results = _run_dc_runner(
                data_repo=data_repo,
                config_path=dc_config_path,
                output_path=dc_output_path,
                stats_summary=args.stats_summary,
                lint_report=args.lint_report,
                differ_output=args.differ_output,
                python=python,
            )
            if len(dc_results) < len(dc_rules):
                # Runner failed or wrote partial output; treat as failure
                print(
                    "Warning: DC runner produced fewer results than rules; some checks may have failed to run.",
                    file=sys.stderr,
                )
        finally:
            try:
                os.unlink(dc_config_path)
            except OSError:
                pass
            try:
                os.unlink(dc_output_path)
            except OSError:
                pass

    custom_results = _run_custom_validators(custom_rules, args.lint_report)
    combined = dc_results + custom_results

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, default=str)

    all_passed = all(r.get("status") == "PASSED" for r in combined)
    dc_run_ok = not dc_rules or len(dc_results) >= len(dc_rules)
    if not dc_run_ok:
        return 1  # DC runner failed or produced partial output
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
