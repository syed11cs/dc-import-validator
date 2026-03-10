#!/usr/bin/env python3
"""Observation date granularity consistency rule.

Deterministic check: detect mixed observationDate formats (YYYY, YYYY-MM, YYYY-MM-DD)
in TMCF literals and/or in the CSV column mapped by observationDate. Emits a WARNING
(not blocker) when mixed resolutions are found so the pipeline still passes.

Used by run_validation.py when --tmcf and --csv are provided. Can be run standalone.
"""

import csv as csv_module
import re
from pathlib import Path

# ISO 8601–style date formats we treat as distinct granularities.
_DATE_LITERAL_RE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")
# observationDate: literal (no C:)
_OBS_DATE_LINE_RE = re.compile(
    r"observationDate\s*:\s*([^\s#].*?)$", re.IGNORECASE
)
# observationDate: C:Table->Column (column name can contain non-word chars)
_OBS_DATE_COLUMN_RE = re.compile(
    r"observationDate\s*:\s*C:([A-Za-z0-9_\-]+)->([^\s#]+)",
    re.IGNORECASE,
)


def _granularity(value: str) -> str | None:
    """Return granularity label for a date string, or None if not a recognized format."""
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if not s or not _DATE_LITERAL_RE.match(s):
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return "YYYY-MM-DD"
    if re.match(r"^\d{4}-\d{2}$", s):
        return "YYYY-MM"
    if re.match(r"^\d{4}$", s):
        return "YYYY"
    return None


def _collect_literal_formats(tmcf_content: str) -> set[str]:
    """Collect observationDate literal granularities from TMCF (exclude column mappings)."""
    formats: set[str] = set()
    for line in tmcf_content.splitlines():
        m = _OBS_DATE_LINE_RE.search(line.split("#", 1)[0])
        if not m:
            continue
        val = m.group(1).strip()
        if val.startswith("C:") or "->" in val:
            continue
        g = _granularity(val)
        if g:
            formats.add(g)
    return formats


def _find_observation_date_column(tmcf_content: str) -> tuple[str | None, str | None]:
    """Return (table_name, column_id) for observationDate column mapping, or (None, None)."""
    for line in tmcf_content.splitlines():
        m = _OBS_DATE_COLUMN_RE.search(line.split("#", 1)[0])
        if m:
            return m.group(1).strip(), m.group(2).strip()
    return None, None


def _sample_csv_date_formats(
    csv_path: Path, column_id: str, max_rows: int = 10_000
) -> set[str]:
    """Sample CSV column values and return set of date granularities (YYYY, YYYY-MM, YYYY-MM-DD)."""
    formats: set[str] = set()
    try:
        with open(csv_path, encoding="utf-8", errors="replace", newline="") as f:
            reader = csv_module.DictReader(f)
            if column_id not in (reader.fieldnames or []):
                return formats
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                val = row.get(column_id, "")
                g = _granularity(val)
                if g:
                    formats.add(g)
    except (OSError, KeyError, csv_module.Error):
        pass
    return formats


def _resolve_csv_for_table(tmcf_path: Path, table_name: str) -> Path | None:
    """Resolve CSV path: same dir as TMCF, prefer TableName.csv then first non-metadata CSV."""
    parent = tmcf_path.parent
    name = table_name if table_name.lower().endswith(".csv") else f"{table_name}.csv"
    candidate = parent / name
    if candidate.exists():
        return candidate
    for p in parent.glob("*.csv"):
        if "metadata" in p.name.lower() or "pvmap" in p.name.lower() or "pv_map" in p.name.lower():
            continue
        return p
    return None


def run(
    tmcf_path: str | Path | None,
    csv_path: str | Path | None,
    params: dict,
    rule_id: str = "check_observation_date_granularity",
) -> dict:
    """Run the observation date granularity check.

    Args:
        tmcf_path: Path to TMCF file. If None, returns PASSED (skipped).
        csv_path: Optional path to CSV (used when observationDate is column-mapped).
                  If not provided, CSV is resolved from TMCF dir and table name.
        params: Optional dict (e.g. max_rows for CSV sample). Unused for now.
        rule_id: validation_name in the result.

    Returns:
        One result dict: validation_name, status ("PASSED" | "WARNING"), message, details, validation_params.
        WARNING when mixed granularities are detected (never FAILED).
    """
    if not tmcf_path:
        return {
            "validation_name": rule_id,
            "status": "PASSED",
            "message": "Skipped (no TMCF provided).",
            "details": {},
            "validation_params": params,
        }
    path = Path(tmcf_path)
    if not path.exists():
        return {
            "validation_name": rule_id,
            "status": "PASSED",
            "message": f"Skipped (TMCF not found: {path}).",
            "details": {},
            "validation_params": params,
        }
    try:
        tmcf_content = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {
            "validation_name": rule_id,
            "status": "PASSED",
            "message": f"Skipped (could not read TMCF: {path}).",
            "details": {},
            "validation_params": params,
        }

    all_formats: set[str] = set()
    literal_formats: set[str] = set()
    column_formats: set[str] = set()
    column_id: str | None = None

    # 1) Literals in TMCF
    literal_formats = _collect_literal_formats(tmcf_content)
    all_formats.update(literal_formats)

    # 2) Column-mapped observationDate: need CSV
    table_name, col_id = _find_observation_date_column(tmcf_content)
    if table_name and col_id:
        column_id = col_id
        csv_file = Path(csv_path) if csv_path and Path(csv_path).exists() else None
        if not csv_file:
            csv_file = _resolve_csv_for_table(path, table_name)
        if csv_file:
            column_formats = _sample_csv_date_formats(csv_file, col_id)
            all_formats.update(column_formats)

    if len(all_formats) <= 1:
        return {
            "validation_name": rule_id,
            "status": "PASSED",
            "message": "No mixed observationDate granularities detected.",
            "details": {"formats_found": sorted(all_formats) if all_formats else []},
            "validation_params": params,
        }

    # Mixed granularity → emit WARNING (not blocker)
    formats_list = sorted(all_formats)

    # Structured sources for UI: where the dates came from and what formats were found there
    sources: list[dict] = []
    if literal_formats:
        sources.append({
            "source": "tmcf_literal",
            "formats_found": sorted(literal_formats),
        })
    if column_formats and column_id is not None:
        sources.append({
            "source": "csv_column",
            "column": column_id,
            "formats_found": sorted(column_formats),
        })

    # Clear, actionable message
    if sources:
        if len(sources) == 1:
            s = sources[0]
            if s["source"] == "csv_column":
                message = (
                    f"Mixed observationDate granularities detected in column \"{s['column']}\". "
                    f"Formats found: {', '.join(formats_list)}."
                )
            else:
                message = (
                    "Mixed observationDate granularities detected in TMCF literal dates. "
                    f"Formats found: {', '.join(formats_list)}."
                )
        else:
            parts = []
            for s in sources:
                if s["source"] == "csv_column":
                    parts.append(f"column \"{s['column']}\"")
                else:
                    parts.append("TMCF literal dates")
            message = (
                f"Mixed observationDate granularities detected in {' and in '.join(parts)}. "
                f"Formats found: {', '.join(formats_list)}."
            )
    else:
        message = (
            f"Mixed observationDate granularities detected. "
            f"Formats found: {', '.join(formats_list)}."
        )

    details: dict = {
        "formats_found": formats_list,
        "tmcf_path": str(path),
        "sources": sources,
    }
    # Single source: expose column or source at top level for simple UI
    if len(sources) == 1:
        if sources[0]["source"] == "csv_column":
            details["column"] = sources[0]["column"]
            details["source"] = "csv_column"
        else:
            details["source"] = "tmcf_literal"

    return {
        "validation_name": rule_id,
        "status": "WARNING",
        "message": message,
        "details": details,
        "validation_params": params,
    }


def main() -> None:
    """CLI: run rule with --tmcf and optional --csv, print result as JSON line."""
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Observation date granularity check (warn on mixed YYYY / YYYY-MM / YYYY-MM-DD)"
    )
    parser.add_argument("--tmcf", required=True, help="Path to TMCF file")
    parser.add_argument("--csv", default=None, help="Path to CSV file (optional; resolved from TMCF if missing)")
    parser.add_argument("--params", default="{}", help="JSON params object")
    parser.add_argument(
        "--rule_id",
        default="check_observation_date_granularity",
        help="Rule ID for validation_name",
    )
    args = parser.parse_args()

    params = json.loads(args.params) if args.params else {}
    result = run(
        args.tmcf,
        args.csv,
        params,
        rule_id=args.rule_id,
    )
    print(json.dumps(result), flush=True)
