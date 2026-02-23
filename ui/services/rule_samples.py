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


def _format_unit_display(raw: str) -> str:
    """Format unit value for display: avoid raw [] or [, x]. Empty/missing → (missing), one value → as-is, multiple → comma-joined."""
    if raw is None or not isinstance(raw, str):
        return "(missing)"
    s = raw.strip()
    if not s or s in ("—", "[]"):
        return "(missing)"
    if s.startswith("["):
        # Parse list-like string: strip brackets, split by comma, clean, join
        inner = s[1:].rstrip("]").strip()
        parts = [p.strip() for p in inner.split(",") if p.strip()]
        if not parts:
            return "(missing)"
        return ", ".join(parts)
    return s


def _load_summary_statvar_units(output_dir: Path) -> list[tuple[str, str]]:
    """Load (StatVar, Units) from summary_report.csv. Returns [] if not found or no Units column."""
    path = output_dir / "summary_report.csv"
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except (OSError, csv.Error):
        return []
    if not rows:
        return []
    sv_col = "StatVar" if "StatVar" in rows[0] else "stat_var"
    unit_col = "Units" if "Units" in rows[0] else "units"
    if sv_col not in rows[0] or unit_col not in rows[0]:
        return []
    out = []
    for row in rows:
        sv = (row.get(sv_col) or "").strip()
        unit = (row.get(unit_col) or "").strip()
        if sv:
            out.append((sv, unit or "—"))
    return out


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

    # Expand check_unit_consistency into one sample per StatVar with its unit (when summary_report.csv exists)
    expanded = []
    for s in samples:
        if (s.get("rule") or "") == "check_unit_consistency":
            statvar_units = _load_summary_statvar_units(output_dir)
            if statvar_units:
                msg = s.get("message") or ""
                expected = s.get("expected") or "consistent units (one unit per StatVar)"
                for stat_var, unit in statvar_units:
                    expanded.append({
                        "statVar": stat_var,
                        "location": None,
                        "date": None,
                        "value": _format_unit_display(unit),
                        "rule": s["rule"],
                        "expected": expected,
                        "sourceRow": None,
                        "message": msg,
                    })
            else:
                expanded.append(s)
        else:
            expanded.append(s)
    samples.clear()
    samples.extend(expanded)


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
                "value": _format_unit_display(units_seen or ""),
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
        elif rule == "check_structural_lint_error_count":
            samples.append({
                "statVar": None,
                "location": None,
                "date": None,
                "value": None,
                "rule": rule,
                "expected": "0 structural lint errors",
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
