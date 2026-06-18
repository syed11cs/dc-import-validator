"""Rule failure sample extraction and enrichment from validation_output.json + CSV.

Rules with special extraction logic:
  - check_min_value: uses validation_params.minimum and CSV value columns for enrichment.
  - check_unit_consistency: expanded per-StatVar from summary_report.csv.
  - check_structural_lint_error_count: no row-level data.

All other rules — including any SQL_VALIDATOR rule — are handled generically:
  if details contains failing_rows, one sample is emitted per row with dynamic
  key-value rendering; otherwise a single summary sample is emitted.
Adding a new SQL_VALIDATOR rule to new_import_config.json requires no changes here.
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


def _format_sql_row_value(row: dict) -> str:
    """Format a SQL failing_row dict as a display string, skipping the StatVar column.

    Single non-StatVar column → 'key: value'.
    Multiple columns → 'key: value, key: value, ...'.
    Empty → '—'.
    """
    _sv_keys = {"StatVar", "stat_var", "statVar"}
    parts = []
    for k, v in row.items():
        if k in _sv_keys:
            continue
        parts.append(f"{k}: {v!s}" if v is not None else f"{k}: —")
    return ", ".join(parts) if parts else "—"


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
    """Enrich samples with location, date, sourceRow from CSV when possible. Mutates samples in place.

    Streams the CSV row-by-row (O(1) memory per row) instead of loading the entire file.
    A single pass over the file resolves all pending samples; iteration stops as soon as
    every sample that needs enrichment has been matched or EOF is reached.
    """
    csv_path = get_csv_path(output_dir)
    if not csv_path:
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

    # Collect samples that need per-row CSV enrichment, keyed by list index for
    # direct in-place mutation.  Any sample with a statVar is eligible; rules that
    # never produce CSV-matchable rows (unit_consistency, structural_lint) have no
    # statVar and are naturally excluded by the guard below.
    _no_csv_rules = {"check_unit_consistency", "check_structural_lint_error_count"}
    pending: dict[int, dict] = {
        idx: s
        for idx, s in enumerate(samples)
        if (s.get("statVar") or "")
        and (s.get("rule") or "") not in _no_csv_rules
    }

    # Single streaming pass: one row at a time, O(1) memory.
    # csv_has_rows mirrors the original early-return guard: the unit-consistency
    # expansion below is skipped when the CSV is absent, empty, or unreadable —
    # exactly as in the previous list(DictReader) approach.
    csv_has_rows = False
    try:
        with open(csv_path, encoding="utf-8", newline="") as f:
            for i, row in enumerate(csv.DictReader(f)):
                csv_has_rows = True
                if not pending:
                    break  # all samples resolved — stop reading the file
                csv_sv = get_val(row, stat_var_cols)
                for idx in list(pending):
                    s = pending[idx]
                    rule = s.get("rule") or ""
                    stat_var = s.get("statVar") or ""
                    if not _stat_var_matches(csv_sv, stat_var):
                        continue
                    if rule == "check_min_value":
                        val = _row_val_float(row, value_cols)
                        if val is None or val >= min_value_threshold:
                            continue  # value condition not met; keep looking for this sample
                    s["location"] = get_val(row, loc_cols) or None
                    s["date"] = get_val(row, date_cols) or None
                    s["sourceRow"] = f"{csv_basename}:{i + 2}"
                    del pending[idx]
    except (OSError, csv.Error):
        return  # mirrors original: load failure → skip enrichment + expansion

    if not csv_has_rows:
        return  # mirrors original: empty CSV → skip enrichment + expansion

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
        elif details.get("missing_goldens") is not None:
            # GOLDENS_CHECK: one sample per missing golden record.
            for node in (details["missing_goldens"] or []):
                stat_var = (
                    node.get("StatVar") or node.get("stat_var")
                    or node.get("variableMeasured") or None
                )
                extra = {k: v for k, v in node.items()
                         if k not in ("StatVar", "stat_var", "variableMeasured") and v}
                value_str = ", ".join(f"{k}: {v}" for k, v in extra.items()) if extra else "—"
                samples.append({
                    "statVar": stat_var,
                    "location": None,
                    "date": None,
                    "value": value_str,
                    "rule": rule,
                    "expected": "present in import output",
                    "sourceRow": None,
                    "message": message,
                })
        elif details.get("failing_rows"):
            # Generic handler for any rule that returns row-level data (e.g. SQL_VALIDATOR).
            # One sample per failing row; StatVar extracted if present, remaining columns
            # rendered as key=value pairs.
            for row in details["failing_rows"]:
                stat_var = row.get("StatVar") or row.get("stat_var") or None
                samples.append({
                    "statVar": stat_var,
                    "location": None,
                    "date": None,
                    "value": _format_sql_row_value(row),
                    "rule": rule,
                    "expected": params.get("condition") or "",
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
