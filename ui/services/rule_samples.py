"""Rule failure sample extraction and enrichment from validation_output.json + CSV.

Rules with special enrichment (check_min_value uses validation_params.minimum and
CSV value columns): if you add/rename rules in config, update enrich_rule_failure_samples
for consistent sample display.
"""

import csv
import json
from pathlib import Path


def get_csv_path(output_dir: Path) -> Path | None:
    """Get input CSV path from report.json commandArgs.inputFiles. Returns None if not found."""
    try:
        path = output_dir / "report.json"
        if not path.exists():
            return None
        with open(path, encoding="utf-8") as f:
            report = json.load(f)
        args = report.get("commandArgs") or {}
        for p in args.get("inputFiles") or []:
            if str(p).lower().endswith(".csv"):
                fp = Path(p)
                if fp.exists():
                    return fp
        return None
    except (json.JSONDecodeError, OSError, TypeError):
        return None


def load_csv_rows(csv_path: Path) -> list[dict] | None:
    """Load CSV as list of dicts (first row = headers). Returns None on failure."""
    try:
        with open(csv_path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except (OSError, csv.Error):
        return None


def _stat_var_matches(csv_val: str, stat_var: str) -> bool:
    """Match CSV variableMeasured/StatVar to validation stat_var (e.g. dcid:Count_X vs Count_X)."""
    if not stat_var or csv_val is None:
        return False
    csv_val = str(csv_val).strip()
    stat_var = str(stat_var).strip()
    return csv_val == stat_var or csv_val.endswith(":" + stat_var) or csv_val.endswith("/" + stat_var)


def _row_val_float(row: dict, key_candidates: list[str]) -> float | None:
    """Get first present key's value as float."""
    for k in key_candidates:
        if k in row and row[k] is not None and str(row[k]).strip() != "":
            try:
                return float(str(row[k]).strip())
            except ValueError:
                pass
    return None


def enrich_rule_failure_samples(samples: list[dict], output_dir: Path, results: list) -> None:
    """Enrich samples with location, date, sourceRow from CSV when possible. Mutates samples in place."""
    csv_path = get_csv_path(output_dir)
    if not csv_path:
        return
    rows = load_csv_rows(csv_path)
    if not rows:
        return
    csv_basename = csv_path.name
    stat_var_cols = ["variableMeasured", "StatVar", "stat_var", "Variable"]
    loc_cols = ["observationAbout", "observation_about", "place", "Place"]
    date_cols = ["observationDate", "observation_date", "date", "Date"]
    value_cols = ["value", "Value"]

    def get_val(row: dict, candidates: list[str]) -> str:
        for c in candidates:
            if c in row and row[c] is not None:
                return str(row[c]).strip()
        return ""

    min_value_threshold = None
    for r in results or []:
        if r.get("status") == "FAILED" and r.get("validation_name") == "check_min_value":
            min_value_threshold = (r.get("validation_params") or {}).get("minimum")
            if min_value_threshold is not None:
                break
    if min_value_threshold is None:
        min_value_threshold = 0

    for s in samples:
        rule = s.get("rule") or ""
        stat_var = s.get("statVar") or ""

        if rule == "check_min_value" and stat_var:
            for i, row in enumerate(rows):
                csv_sv = get_val(row, stat_var_cols)
                if not _stat_var_matches(csv_sv, stat_var):
                    continue
                val = _row_val_float(row, value_cols)
                if val is not None and val < min_value_threshold:
                    s["location"] = get_val(row, loc_cols) or None
                    s["date"] = get_val(row, date_cols) or None
                    s["sourceRow"] = f"{csv_basename}:{i + 2}"
                    break
        elif rule == "check_scaling_factor_consistency" and stat_var:
            for i, row in enumerate(rows):
                csv_sv = get_val(row, stat_var_cols)
                if not _stat_var_matches(csv_sv, stat_var):
                    continue
                s["location"] = get_val(row, loc_cols) or None
                s["date"] = get_val(row, date_cols) or None
                s["sourceRow"] = f"{csv_basename}:{i + 2}"
                break


def extract_rule_failure_samples(results: list) -> list[dict]:
    """Parse validation_output.json results and return structured failure samples for failed rules."""
    samples = []
    for r in results or []:
        if r.get("status") != "FAILED":
            continue
        rule = r.get("validation_name") or ""
        message = r.get("message") or ""
        details = r.get("details") or {}
        params = r.get("validation_params") or {}

        if rule == "check_min_value":
            failed_rows = details.get("failed_rows") or []
            minimum = params.get("minimum")
            expected = f">= {minimum}" if minimum is not None else ">= 0"
            for row in failed_rows:
                stat_var = row.get("stat_var") or row.get("StatVar") or ""
                value = row.get("actual_min_value")
                if value is None and "value" in row:
                    value = row["value"]
                samples.append({
                    "statVar": stat_var,
                    "location": None,
                    "date": None,
                    "value": value if value is not None else "—",
                    "rule": rule,
                    "expected": expected,
                    "sourceRow": None,
                    "message": message,
                })
        elif rule == "check_unit_consistency":
            units_seen = details.get("units") or details.get("unit_values") or details.get("units_seen")
            if units_seen is not None and not isinstance(units_seen, str):
                units_seen = str(units_seen)
            samples.append({
                "statVar": None,
                "location": None,
                "date": None,
                "value": units_seen if units_seen else None,
                "rule": rule,
                "expected": "consistent units (one unit per StatVar)",
                "sourceRow": None,
                "message": message,
            })
        elif rule == "check_scaling_factor_consistency":
            failing_rows = details.get("failing_rows") or []
            raw_expected = params.get("condition") or "consistent scaling factor"
            if "ScalingFactors = (SELECT ScalingFactors FROM stats LIMIT 1)" in (raw_expected or ""):
                expected = "one scaling factor per StatVar (all rows same)"
            else:
                expected = raw_expected
            for row in failing_rows:
                stat_var = row.get("StatVar") or row.get("stat_var") or ""
                value = row.get("ScalingFactors") or row.get("scaling_factors")
                if value is not None and not isinstance(value, str):
                    value = str(value)
                samples.append({
                    "statVar": stat_var,
                    "location": None,
                    "date": None,
                    "value": value if value is not None else "—",
                    "rule": rule,
                    "expected": expected,
                    "sourceRow": None,
                    "message": message,
                })
        else:
            samples.append({
                "statVar": None,
                "location": None,
                "date": None,
                "value": None,
                "rule": rule,
                "expected": "",
                "sourceRow": None,
                "message": message,
            })
    return samples
