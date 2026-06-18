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
CUSTOM_VALIDATORS = frozenset({"STRUCTURAL_LINT_ERROR_COUNT", "OBSERVATION_DATE_GRANULARITY"})

# Validators implemented in this repository (not handled by the DC runner).
DC_EXCLUDE_VALIDATORS = frozenset({"LINT_ERROR_COUNT", "STRUCTURAL_LINT_ERROR_COUNT"})


def _load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


import re as _re
_SQL_ROWS_FAILED_RE = _re.compile(r"^(\d+) rows failed the SQL validation\.$")


def _rewrite_sql_validator_messages(results: list[dict]) -> None:
    """Replace the DC framework's "N rows failed the SQL validation." message with
    StatVar-centric wording.

    The stats table has one row per StatVar (aggregated over all CSV rows), so
    "rows" in the DC framework message refers to StatVars, not raw CSV rows.
    Detection is by message pattern — the exact string is only ever emitted by
    SQL_VALIDATOR, so no rule_id allowlist is needed.
    Operates in-place.
    """
    for r in results:
        msg = r.get("message") or ""
        m = _SQL_ROWS_FAILED_RE.match(msg)
        if not m:
            continue
        n = int(m.group(1))
        label = "StatVar" if n == 1 else "StatVars"
        r["message"] = f"Rule failed for {n} {label}."
        r.setdefault("details", {})["sql_context"] = (
            "Rules are evaluated per StatVar (aggregated across all CSV rows)"
        )


_GOLDENS_MISSING_RE = _re.compile(r"^Found (\d+) missing golden records\.$")


def _rewrite_goldens_check_messages(results: list[dict]) -> None:
    """Fix singular/plural in GOLDENS_CHECK messages and surface which records are missing."""
    for r in results:
        msg = r.get("message") or ""
        m = _GOLDENS_MISSING_RE.match(msg)
        if not m:
            continue
        n = int(m.group(1))
        label = "record" if n == 1 else "records"
        r["message"] = f"Found {n} missing golden {label}."


_DIFFER_NO_BASELINE_RE = _re.compile(
    r"Differ summary is missing required field", _re.IGNORECASE
)


def _rewrite_differ_no_baseline_messages(results: list[dict]) -> None:
    """Rewrite DATA_ERROR differ messages that indicate a missing baseline into
    user-friendly WARNINGs.

    When no baseline exists (e.g. first run), the DC runner emits DATA_ERROR with
    "Differ summary is missing required field: 'previous_obs_size'." for differ
    rules. This is expected — not a genuine data error. Promote to WARNING with a
    clear explanation so it appears in the Warnings section, not "Other".

    Genuine differ failures (malformed JSON, unexpected fields) produce different
    error messages and are left untouched.
    Operates in-place.
    """
    _DIFFER_RULE_IDS = frozenset({
        "check_deleted_records_count",
        "check_deleted_records_percent",
        "check_modified_records_count",
        "check_added_records_count",
    })
    for r in results:
        if r.get("validation_name") not in _DIFFER_RULE_IDS:
            continue
        if r.get("status") != "DATA_ERROR":
            continue
        msg = r.get("message") or ""
        if not _DIFFER_NO_BASELINE_RE.search(msg):
            continue
        r["status"] = "WARNING"
        r["message"] = "No baseline found — skipping change detection (first run)."
        r.setdefault("details", {})["differ_context"] = (
            "This is expected on the first run or when no baseline has been accepted. "
            "Use 'Accept Baseline' after a successful run to enable differ checks."
        )


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
        if result.returncode == 1:
            print("Validation rules reported failures (exit code 1)", file=sys.stderr)
        else:
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
    stats_summary_path: str | None,
    tmcf_path: str | None = None,
    csv_path: str | None = None,
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
        elif validator == "OBSERVATION_DATE_GRANULARITY":
            from observation_date_granularity import run as run_observation_date_granularity  # noqa: I001
            result = run_observation_date_granularity(
                tmcf_path, csv_path, params, rule_id=rule_id
            )
            results.append(result)
        else:
            # Unknown custom validator: surface as WARNING so config mistakes are visible
            results.append({
                "validation_name": rule_id,
                "status": "WARNING",
                "message": f"Unknown custom validator: {validator}",
                "details": {"validator": validator},
                "validation_params": params,
            })

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
    parser.add_argument("--tmcf", default=None, help="Path to TMCF (for OBSERVATION_DATE_GRANULARITY)")
    parser.add_argument("--csv", default=None, action="append", help="Path to CSV (repeatable; first CSV used for OBSERVATION_DATE_GRANULARITY)")
    args = parser.parse_args()

    config_path = args.validation_config
    output_path = args.validation_output
    _cfg_exists = os.path.isfile(config_path)
    _cfg_size = os.path.getsize(config_path) if _cfg_exists else -1
    print('[OVERRIDE_TRACE] ' + json.dumps({
        "component": "run_validation", "event": "startup",
        "config_path": config_path,
        "exists": _cfg_exists,
        "size_bytes": _cfg_size,
    }), flush=True)
    if not _cfg_exists:
        print(f"Error: validation config not found: {config_path}", file=sys.stderr)
        return 2

    config = _load_config(config_path)
    dc_rules, custom_rules = _split_rules(config)
    print('[OVERRIDE_TRACE] ' + json.dumps({
        "component": "run_validation", "event": "config_loaded",
        "config_path": config_path,
        "total_rules": len(config.get("rules", [])),
        "dc_rules": len(dc_rules),
        "custom_rules": len(custom_rules),
    }), flush=True)

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
            # Always check by rule_id, not just by count. A length-only check can
            # miss cases where the runner emits the right number of results but with
            # duplicate validation_names, leaving some rules with no result.
            returned_ids = {r.get("validation_name") for r in dc_results}
            missing_rules = [rule for rule in dc_rules if rule.get("rule_id", "") not in returned_ids]
            if missing_rules:
                # DC runner crashed or wrote partial output. Inject a synthetic FAILED
                # result for every rule whose result is missing so that:
                #   1. validation_output.json is self-describing (no silent gaps).
                #   2. apply_warn_only.py sees FAILED entries and exits 1.
                #   3. The final HTML report shows which rules did not execute.
                for rule in missing_rules:
                    dc_results.append({
                        "validation_name": rule.get("rule_id", ""),
                        "status": "FAILED",
                        "message": (
                            "Rule did not execute — DC runner exited before producing "
                            "a result. Check runner stderr for details (e.g. import "
                            "errors or crashes)."
                        ),
                        "details": {},
                        "validation_params": rule.get("params", {}),
                    })
                print(
                    f"DC runner produced fewer results than expected; "
                    f"{len(missing_rules)} rule(s) marked FAILED.",
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

    custom_results = _run_custom_validators(
        custom_rules,
        args.lint_report,
        args.stats_summary,
        tmcf_path=args.tmcf,
        csv_path=(args.csv[0] if args.csv else None),
    )
    combined = dc_results + custom_results
    _rewrite_sql_validator_messages(combined)
    _rewrite_differ_no_baseline_messages(combined)
    _rewrite_goldens_check_messages(combined)

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, default=str)

    # PASSED and WARNING are non-blocking; FAILED and CONFIG_ERROR cause exit 1.
    # CONFIG_ERROR is returned by SQL_VALIDATOR when the query is malformed (syntax
    # error, invalid column reference, etc.).  It is treated as a hard failure so
    # bad custom SQL rules are never silently ignored.
    # Partial DC runner output is handled above by injecting synthetic FAILED entries,
    # so combined always reflects the true state of every rule.
    _PASSING_STATUSES = frozenset({"PASSED", "WARNING"})
    all_passed = all(r.get("status") in _PASSING_STATUSES for r in combined)
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
