#!/usr/bin/env python3
"""Create a filtered validation config with only selected rules.

Usage:
  filter_validation_config.py --config=BASE.json --rules=id1,id2 [--output=PATH]
  filter_validation_config.py --config=BASE.json --skip-rules=id1,id2 [--output=PATH]

Outputs the path to the filtered config file (temp file if --output not given).
"""

import argparse
import json
import sys
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Filter validation config by rules")
    parser.add_argument("--config", required=True, help="Base config path")
    parser.add_argument("--rules", help="Comma-separated rule IDs to include")
    parser.add_argument("--skip-rules", help="Comma-separated rule IDs to exclude")
    parser.add_argument("--output", help="Output path (default: temp file)")
    args = parser.parse_args()

    if not args.rules and not args.skip_rules:
        print("Error: provide --rules or --skip-rules", file=sys.stderr)
        sys.exit(1)
    if args.rules and args.skip_rules:
        print("Error: use --rules OR --skip-rules, not both", file=sys.stderr)
        sys.exit(1)

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: config not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"Error reading config: {e}", file=sys.stderr)
        sys.exit(1)

    rules = config.get("rules", [])
    if not isinstance(rules, list):
        print("Error: config 'rules' must be an array", file=sys.stderr)
        sys.exit(1)
    all_ids = {r.get("rule_id") for r in rules if isinstance(r, dict) and r.get("rule_id")}

    if args.rules:
        include = {x.strip() for x in args.rules.split(",") if x.strip()}
        unknown = include - all_ids
        if unknown:
            print(f"Error: unknown rule ID(s): {', '.join(sorted(unknown))}. Valid IDs: {', '.join(sorted(all_ids)) or '(none)'}", file=sys.stderr)
            sys.exit(1)
        filtered = [r for r in rules if r.get("rule_id") in include]
    else:
        exclude = {x.strip() for x in args.skip_rules.split(",") if x.strip()}
        filtered = [r for r in rules if r.get("rule_id") not in exclude]

    if not filtered:
        print("Error: no rules left after filter (zero rules)", file=sys.stderr)
        sys.exit(1)

    config["rules"] = filtered

    if args.output:
        out_path = Path(args.output)
    else:
        fd, tmp = tempfile.mkstemp(suffix=".json", prefix="validation_config_")
        out_path = Path(tmp)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

    print(str(out_path.resolve()))


if __name__ == "__main__":
    main()
