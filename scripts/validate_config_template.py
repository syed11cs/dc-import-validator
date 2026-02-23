#!/usr/bin/env python3
"""Validate validation_config.json (or sample_config.json) against expected template.

Ensures config has required structure: schema_version (optional), rules array,
and each rule has rule_id, description, validator, scope, params.
Exits 0 if valid, 1 if invalid (prints errors to stderr).

Usage:
  python validate_config_template.py validation_config.json
  python validate_config_template.py --path path/to/config.json
"""

import argparse
import json
import re
import sys
from pathlib import Path


REQUIRED_TOP_LEVEL = ("rules",)
OPTIONAL_TOP_LEVEL = ("schema_version",)
REQUIRED_RULE_KEYS = ("rule_id", "description", "validator", "scope", "params")
OPTIONAL_RULE_KEYS = ("enabled",)  # enabled: false to disable a rule without removing it
VALID_DATA_SOURCES = ("stats", "lint", "differ")
RULE_ID_PATTERN = r"^[a-z][a-z0-9_]*$"  # snake_case, starts with letter


def _validate_config(config: dict, path: str) -> list[str]:
    """Return list of error messages. Empty if valid."""
    errors = []

    if not isinstance(config, dict):
        errors.append(f"{path}: root must be a JSON object")
        return errors

    for key in REQUIRED_TOP_LEVEL:
        if key not in config:
            errors.append(f"{path}: missing required key '{key}'")
        elif not isinstance(config[key], list):
            errors.append(f"{path}: '{key}' must be an array")

    for key in config:
        if key not in REQUIRED_TOP_LEVEL and key not in OPTIONAL_TOP_LEVEL:
            errors.append(f"{path}: unknown top-level key '{key}' (allowed: {list(REQUIRED_TOP_LEVEL) + list(OPTIONAL_TOP_LEVEL)})")

    if "rules" not in config or not isinstance(config["rules"], list):
        return errors

    seen_rule_ids: set[str] = set()
    for i, rule in enumerate(config["rules"]):
        if not isinstance(rule, dict):
            errors.append(f"{path}: rules[{i}] must be an object")
            continue
        prefix = f"{path}: rules[{i}]"
        for rk in REQUIRED_RULE_KEYS:
            if rk not in rule:
                errors.append(f"{prefix}: missing required key '{rk}'")
        for rk, rv in rule.items():
            if rk == "rule_id":
                if not isinstance(rv, str) or not rv.strip():
                    errors.append(f"{prefix}: rule_id must be a non-empty string")
                elif not re.match(RULE_ID_PATTERN, rv):
                    errors.append(f"{prefix}: rule_id should be snake_case (e.g. check_min_value)")
                elif rv in seen_rule_ids:
                    errors.append(f"{prefix}: duplicate rule_id {rv!r} (rule_id must be unique)")
                else:
                    seen_rule_ids.add(rv)
            elif rk == "description":
                if not isinstance(rv, str):
                    errors.append(f"{prefix}: description must be a string")
            elif rk == "validator":
                if not isinstance(rv, str) or not rv.strip():
                    errors.append(f"{prefix}: validator must be a non-empty string")
            elif rk == "scope":
                if not isinstance(rv, dict):
                    errors.append(f"{prefix}: scope must be an object")
                elif "data_source" in rv and rv["data_source"] not in VALID_DATA_SOURCES:
                    errors.append(f"{prefix}: scope.data_source must be one of {VALID_DATA_SOURCES}")
            elif rk == "params":
                if not isinstance(rv, dict):
                    errors.append(f"{prefix}: params must be an object")
            elif rk == "enabled":
                if not isinstance(rv, bool):
                    errors.append(f"{prefix}: enabled must be a boolean")
            elif rk not in REQUIRED_RULE_KEYS and rk not in OPTIONAL_RULE_KEYS:
                errors.append(f"{prefix}: unknown rule key '{rk}' (allowed: {list(REQUIRED_RULE_KEYS) + list(OPTIONAL_RULE_KEYS)})")

    if "schema_version" in config and not isinstance(config["schema_version"], str):
        errors.append(f"{path}: schema_version must be a string")

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Validate validation_config.json against expected template"
    )
    parser.add_argument(
        "config_path",
        nargs="?",
        default=None,
        help="Path to validation config JSON",
    )
    parser.add_argument(
        "--path",
        "-p",
        help="Path to config (alternative to positional)",
    )
    args = parser.parse_args()
    path = args.path or args.config_path
    if not path:
        parser.error("Provide config_path or --path")
    path = str(path)
    if not Path(path).exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)
    try:
        with open(path, encoding="utf-8") as f:
            config = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
        sys.exit(1)
    errors = _validate_config(config, path)
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
