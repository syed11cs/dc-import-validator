#!/usr/bin/env python3
"""LLM-based schema and typo review for TMCF files.

Reviews TMCF content for typos, property naming issues, and schema consistency.
Requires GEMINI_API_KEY or GOOGLE_API_KEY environment variable.

When stat_vars MCF is provided: compares StatVars used in TMCF/CSV to
definitions in stat_vars MCF and flags any used StatVar that has no Node
(missing_statvar_definition); also flags percent/rate StatVars in stat_vars
MCF that lack measurementDenominator (missing_measurement_denominator).

Usage:
  python llm_schema_review.py --tmcf=path/to/file.tmcf [--output=report.json]
  python llm_schema_review.py --tmcf=path/to/file.tmcf --csv=path/to/file.csv [--stat-vars-mcf=...] [--stat-vars-schema-mcf=...]
  python llm_schema_review.py --tmcf=path/to/file.tmcf --output=-

Output:
  - Writes JSON report to --output path (default: stdout)
  - Each issue may include "severity": "blocker" | "warning" (default: blocker)
  - If no issues found, returns empty list
  - If API key missing, exits with non-zero (deterministic issues still written).
  - If Gemini call fails (timeout, network, invalid key): pipeline continues; deterministic
    issues are returned plus one warning issue; exit code is 0 unless those issues have blockers.
"""

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path

# Short TMCF/schema reference for the prompt (Data Commons).
TMCF_SPEC = """
TMCF (Table MCF) basics:
- Each node has Node: <column_id> and typeOf: <SchemaType> (e.g. dcs:StatVarObservation).
- Column references use exact column IDs (e.g. C:table->C1). Typos break the mapping.
- StatVar DCIDs: UpperCamelCase segments separated by underscores (e.g. Count_Person, Median_Income_Household). No spaces or hyphens. Schema types and schema-valued properties use the dcs: prefix.
- Schema: https://github.com/datacommonsorg/schema | MCF format: https://github.com/datacommonsorg/data/blob/master/docs/mcf_format.md
"""

EXAMPLE_GOOD = """
# Good: required props present, correct names, dcs: prefix, column refs consistent
Node: E:table->E0
typeOf: dcs:StatVarObservation
variableMeasured: dcs:Count_Person
observationAbout: C:table->C1
observationDate: C:table->C2
value: C:table->C3
"""

EXAMPLE_BAD_TYPO_OR_COLUMN = """
# Bad: wrong column ref (C99 not used elsewhere), typo risk
Node: E:table->E0
typeOf: dcs:StatVarObservation
variableMeasured: dcs:Count_Person
observationAbout: C:table->C1
observationDate: C:table->C99
value: C:table->C3
"""

EXAMPLE_BAD_MISSING_REQUIRED = """
# Bad: missing required properties for StatVarObservation (observationAbout, observationDate)
Node: E:table->E0
typeOf: dcs:StatVarObservation
variableMeasured: dcs:Count_Person
value: C:table->C3
"""

EXAMPLE_BAD_NAMESPACE_OR_DUPLICATE = """
# Bad: schema value without dcs: prefix in TMCF (StatVarObservation -> dcs:StatVarObservation)
# Bad: duplicate single-value property (observationDate appears twice)
Node: E:table->E0
typeOf: dcs:StatVarObservation
variableMeasured: Count_Person
observationAbout: C:table->C1
observationDate: C:table->C2
observationDate: C:table->C3
value: C:table->C4
"""


# Column reference in TMCF: C:tableName->columnId (captures until newline so column IDs with spaces e.g. "GDP (USD)" work)
_COLUMN_REF_RE = re.compile(r"C:[^>]+->([^\n\r]+)")

# variableMeasured in TMCF: literal (dcs:X or X) or column ref (C:table->col); value on same line only
_VARIABLE_MEASURED_RE = re.compile(r"variableMeasured\s*:\s*([^\n\r#]+)", re.IGNORECASE)

# observationDate literal: ISO 8601 YYYY, YYYY-MM, or YYYY-MM-DD only
_OBSERVATION_DATE_ISO8601_RE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")

# StatVar DCID: UpperCamelCase segments separated by underscores (e.g. Count_Person)
_STATVAR_DCID_FORMAT_RE = re.compile(r"^[A-Z0-9][A-Za-z0-9]*(?:_[A-Z0-9][A-Za-z0-9]*)*$")


def _get_api_key() -> str | None:
    """Get API key from environment."""
    return os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")


def _normalize_statvar_dcid(raw: str) -> str:
    """Normalize StatVar DCID: strip dcid:, dcs:, and surrounding whitespace."""
    if not raw or not isinstance(raw, str):
        return ""
    s = raw.strip()
    for prefix in ("dcid:", "dcs:"):
        if s.lower().startswith(prefix.lower()):
            s = s[len(prefix) :].strip()
            break
    return s


def _extract_generated_statvar_dcids(tmcf_content: str, csv_path: str | Path | None) -> set[str]:
    """
    Extract StatVar DCIDs that are "generated" by the TMCF/CSV (used in variableMeasured).
    - From TMCF: literal variableMeasured values (e.g. dcs:Count_Person).
    - From CSV: when variableMeasured maps to a column (C:table->columnId), unique values in those columns.
    Reads the CSV exactly once and extracts values for all referenced columns in a single pass.
    Returns set of normalized DCIDs (no dcid:/dcs: prefix).
    """
    dcids: set[str] = set()
    # 1) Literals from TMCF (non–C: variableMeasured values)
    for m in _VARIABLE_MEASURED_RE.finditer(tmcf_content):
        val = m.group(1).strip().split("\n")[0].strip()
        if not val or val.startswith("C:"):
            continue
        dcid = _normalize_statvar_dcid(val)
        if dcid:
            dcids.add(dcid)
    # 2) Collect ALL column IDs referenced in variableMeasured C:table->columnId
    column_ids: set[str] = set()
    for m in _VARIABLE_MEASURED_RE.finditer(tmcf_content):
        val = m.group(1).strip().split("\n")[0].strip()
        if not val.startswith("C:") or "->" not in val:
            continue
        col_ref = re.search(r"->\s*([^\s#]+)", val)
        if not col_ref:
            continue
        col_id = col_ref.group(1).strip()
        if col_id:
            column_ids.add(col_id)
    # 3) Read CSV exactly once; extract unique values for all referenced columns in a single pass
    if not column_ids or not csv_path or not Path(csv_path).exists():
        return dcids
    try:
        with open(csv_path, encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            cols_to_read = [c for c in column_ids if c in fieldnames]
            if not cols_to_read:
                return dcids
            for row in reader:
                for col_id in cols_to_read:
                    v = (row.get(col_id) or "").strip()
                    if v:
                        dcid = _normalize_statvar_dcid(v)
                        if dcid:
                            dcids.add(dcid)
    except (OSError, csv.Error):
        pass
    return dcids


def _extract_statvar_dcids_from_mcf(content: str) -> set[str]:
    """
    Extract StatVar Node DCIDs from stat_vars MCF (Node: dcid:X or Node: X).
    Returns set of normalized DCIDs.
    """
    dcids: set[str] = set()
    for line in content.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line.startswith("Node:") or ":" not in line:
            continue
        _, _, rest = line.partition(":")
        val = rest.strip()
        if not val:
            continue
        dcid = _normalize_statvar_dcid(val)
        if dcid:
            dcids.add(dcid)
    return dcids


def _compare_generated_to_stat_vars_mcf(
    generated_dcids: set[str],
    stat_vars_content: str,
    stat_vars_mcf_path: str,
) -> list[dict]:
    """
    Compare generated StatVar DCIDs to stat_vars MCF. Flag each generated DCID
    that has no definition in stat_vars MCF.
    """
    defined = _extract_statvar_dcids_from_mcf(stat_vars_content)
    file_name = Path(stat_vars_mcf_path).name
    missing = sorted(generated_dcids - defined)
    if not missing:
        return []
    issues = []
    for dcid in missing[:20]:
        issues.append({
            "line": None,
            "type": "missing_statvar_definition",
            "message": f"StatVar '{dcid}' is used in TMCF/CSV but has no definition in stat_vars MCF.",
            "suggestion": f"Add a Node for '{dcid}' in stat_vars MCF with name, description, and optionally alternateName.",
            "severity": "warning",
            "file": file_name,
        })
    if len(missing) > 20:
        issues.append({
            "line": None,
            "type": "missing_statvar_definition",
            "message": f"… and {len(missing) - 20} more StatVars used in TMCF/CSV but not defined in stat_vars MCF.",
            "suggestion": "Add Node entries in stat_vars MCF for all StatVars used in the import.",
            "severity": "warning",
            "file": file_name,
        })
    return issues


def _extract_column_refs_from_tmcf(tmcf_content: str) -> list[str]:
    """
    Extract column IDs referenced in TMCF (part after -> in C:table->columnId), stripped.
    Returns list in order of first appearance (for stable issue ordering).
    Inline comments (# ...) are stripped before extraction so they don't pollute the column ID.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for line in tmcf_content.splitlines():
        line = line.split("#", 1)[0]  # remove inline comment
        for m in _COLUMN_REF_RE.finditer(line):
            ref = m.group(1).strip()
            if ref and ref not in seen:
                seen.add(ref)
                ordered.append(ref)
    return ordered


def _validate_column_refs_against_header(
    tmcf_content: str,
    header_columns: list[str],
) -> list[dict]:
    """
    Deterministic check: column IDs referenced in TMCF must exist in header (case-sensitive).
    Returns list of schema blocker issues for missing columns (order of first appearance in TMCF).
    """
    refs_ordered = _extract_column_refs_from_tmcf(tmcf_content)
    header_set = {c.strip() for c in header_columns}
    missing = [r for r in refs_ordered if r not in header_set]
    if not missing:
        return []
    issues = []
    for col in missing:
        issues.append({
            "line": None,
            "type": "schema",
            "message": f"Column reference '{col}' does not exist in CSV header (case-sensitive).",
            "suggestion": f"Use a column ID from the header or fix the typo.",
            "severity": "blocker",
        })
    return issues


def _find_unused_csv_columns(
    tmcf_content: str,
    header_columns: list[str],
) -> list[dict]:
    """
    Deterministic check: CSV columns that are never referenced in TMCF (may indicate mapping mistake).
    Returns list of warning issues (one per unused column, up to 10).
    """
    refs_set = set(_extract_column_refs_from_tmcf(tmcf_content))
    unused = [c for c in header_columns if c and c not in refs_set]
    if not unused:
        return []
    issues = []
    for col in unused[:10]:  # cap to avoid noise
        issues.append({
            "line": None,
            "type": "unused_column",
            "message": f"CSV column '{col}' is not referenced in the TMCF (unused column).",
            "suggestion": "Remove the column from the CSV or add a mapping in the TMCF if it is needed.",
            "severity": "warning",
        })
    if len(unused) > 10:
        issues.append({
            "line": None,
            "type": "unused_column",
            "message": f"… and {len(unused) - 10} more unused CSV columns.",
            "suggestion": "Ensure all CSV columns are mapped in the TMCF or remove unused columns.",
            "severity": "warning",
        })
    return issues


def _check_value_column_mapped(tmcf_content: str) -> list[dict]:
    """
    Deterministic check: StatVarObservation nodes must map a value column (value: C:table->columnId).
    Returns one blocker if StatVarObservation appears but no value mapping is found.
    """
    if not re.search(r"typeOf\s*:\s*dcs:StatVarObservation", tmcf_content):
        return []
    # Look for a line that maps value to a column (value: C:...->...)
    for line in tmcf_content.splitlines():
        s = line.split("#", 1)[0].strip()
        if s.startswith("value:") and "C:" in s and "->" in s:
            return []
    return [{
        "line": None,
        "type": "schema",
        "message": "StatVarObservation node(s) found but no 'value' property mapping to a CSV column (e.g. value: C:table->value).",
        "suggestion": "Add a line mapping value to a numeric CSV column.",
        "severity": "blocker",
    }]


def _validate_observation_date_literals(tmcf_content: str) -> list[dict]:
    """
    Deterministic check: observationDate literal values must match ISO 8601 (YYYY, YYYY-MM, or YYYY-MM-DD).
    Column mappings (C:table->columnId) are skipped. Invalid formats are flagged as format (warning).
    """
    issues: list[dict] = []
    obs_re = re.compile(r"observationDate\s*:\s*([^\n\r#]+)", re.IGNORECASE)
    for line_no, line in enumerate(tmcf_content.splitlines(), start=1):
        m = obs_re.search(line.split("#", 1)[0])
        if not m:
            continue
        val = m.group(1).strip()
        if val.startswith("C:"):
            continue
        if not _OBSERVATION_DATE_ISO8601_RE.match(val):
            issues.append({
                "line": line_no,
                "type": "format",
                "message": f"observationDate literal '{val}' does not match ISO 8601 (use YYYY, YYYY-MM, or YYYY-MM-DD).",
                "suggestion": "Use a date in YYYY, YYYY-MM, or YYYY-MM-DD format.",
                "severity": "warning",
            })
    return issues


def _validate_statvar_dcid_format(
    tmcf_content: str,
    csv_path: str | Path | None,
    generated_dcids: set[str] | None = None,
) -> list[dict]:
    """
    Deterministic check: StatVar DCIDs (from variableMeasured literals and CSV column values) must match
    UpperCamelCase segments separated by underscores. Invalid formats are flagged as format (warning).
    If generated_dcids is provided, it is used (avoids re-reading CSV); otherwise extracted from tmcf_content/csv_path.
    """
    issues: list[dict] = []
    dcids = generated_dcids if generated_dcids is not None else _extract_generated_statvar_dcids(tmcf_content, csv_path)
    for dcid in sorted(dcids):
        if not _STATVAR_DCID_FORMAT_RE.match(dcid):
            issues.append({
                "line": None,
                "type": "format",
                "message": f"StatVar DCID '{dcid}' does not match required format (UpperCamelCase segments separated by underscores).",
                "suggestion": "Use only letters, numbers, and underscores; each segment should start with an uppercase letter (e.g. Count_Person).",
                "severity": "warning",
            })
    return issues


def _validate_stat_vars_mcf(content: str, path: str) -> list[dict]:
    """
    Deterministic validation of stat_vars MCF: name (required), description (required),
    alternateName (optional but recommended). Advisory only (severity: warning).
    Single-line parsing for v1. Returns issues with file and line when possible.
    """
    issues: list[dict] = []
    file_name = Path(path).name
    lines = content.splitlines()
    node_start_line: int | None = None
    props: dict[str, str] = {}

    def flush_node() -> None:
        nonlocal node_start_line, props
        if node_start_line is None:
            return
        line_no = node_start_line
        name_val = (props.get("name") or "").strip()
        desc_val = (props.get("description") or "").strip()
        alt_val = (props.get("alternateName") or "").strip()

        if not name_val:
            issues.append({
                "line": line_no,
                "type": "mcf_required",
                "message": "Missing or empty 'name' for StatVar node.",
                "suggestion": "Add a non-empty name: property.",
                "severity": "warning",
                "file": file_name,
            })
        if not desc_val:
            issues.append({
                "line": line_no,
                "type": "mcf_required",
                "message": "Missing or empty 'description' for StatVar node.",
                "suggestion": "Add a non-empty description: property.",
                "severity": "warning",
                "file": file_name,
            })
        if not alt_val:
            issues.append({
                "line": line_no,
                "type": "mcf_recommended",
                "message": "Consider adding 'alternateName' for StatVar node.",
                "suggestion": "Add alternateName: with a short label.",
                "severity": "warning",
                "file": file_name,
            })
        # Require populationType and measuredProperty only for StatVar (StatisticalVariable) nodes
        type_of = (props.get("typeOf") or "").strip().lower()
        if "statisticalvariable" in type_of:
            population_type = (props.get("populationType") or "").strip()
            if not population_type:
                issues.append({
                    "line": line_no,
                    "type": "mcf_required",
                    "message": "Missing or empty 'populationType' for StatVar node.",
                    "suggestion": "Add a non-empty populationType: (e.g. dcs:Person).",
                    "severity": "warning",
                    "file": file_name,
                })
            measured_property = (props.get("measuredProperty") or "").strip()
            if not measured_property:
                issues.append({
                    "line": line_no,
                    "type": "mcf_required",
                    "message": "Missing or empty 'measuredProperty' for StatVar node.",
                    "suggestion": "Add a non-empty measuredProperty: (e.g. dcs:count).",
                    "severity": "warning",
                    "file": file_name,
                })
        node_start_line = None
        props = {}

    for i, raw_line in enumerate(lines):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("Node:"):
            flush_node()
            node_start_line = i + 1
            props = {}
        elif ":" in line and node_start_line is not None:
            key, _, rest = line.partition(":")
            key = key.strip()
            value = rest.strip()
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1].replace('\\"', '"').strip()
            props[key] = value

    flush_node()
    return issues


def _check_percent_statvar_denominator(content: str, path: str) -> list[dict]:
    """
    For percent/rate StatVars in stat_vars MCF, flag missing measurementDenominator.
    Percent/rate: unit contains Percent, or statType contains rate, or measuredProperty contains Rate.
    When stat_vars MCF is provided (Custom), we can run this check.
    """
    issues: list[dict] = []
    file_name = Path(path).name
    lines = content.splitlines()
    node_start_line: int | None = None
    props: dict[str, str] = {}

    def flush_node() -> None:
        nonlocal node_start_line, props
        if node_start_line is None:
            return
        line_no = node_start_line
        unit_val = (props.get("unit") or "").strip().lower()
        stat_type_val = (props.get("statType") or "").strip().lower()
        measured_prop_val = (props.get("measuredProperty") or "").strip().lower()
        denominator = (props.get("measurementDenominator") or "").strip()
        node_val = (props.get("Node") or "").strip()
        dcid = _normalize_statvar_dcid(node_val) if node_val else f"line {line_no}"

        is_percent_or_rate = (
            "percent" in unit_val
            or "rate" in stat_type_val
            or "rate" in measured_prop_val
        )
        if not is_percent_or_rate:
            node_start_line = None
            props = {}
            return

        if not denominator:
            issues.append({
                "line": line_no,
                "type": "missing_measurement_denominator",
                "message": f"Percent/rate StatVar '{dcid}' should have measurementDenominator in stat_vars MCF.",
                "suggestion": "Add measurementDenominator: (e.g. dcs:Count_Person) for rate/percent StatVars.",
                "severity": "warning",
                "file": file_name,
            })
        node_start_line = None
        props = {}

    for i, raw_line in enumerate(lines):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("Node:"):
            flush_node()
            node_start_line = i + 1
            props = {}
            _, _, rest = line.partition(":")
            props["Node"] = rest.strip()
        elif ":" in line and node_start_line is not None:
            key, _, rest = line.partition(":")
            key = key.strip()
            value = rest.strip()
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1].replace('\\"', '"').strip()
            props[key] = value

    flush_node()
    return issues


def _read_csv_header(csv_path: str | Path) -> list[str] | None:
    """Read first row of CSV using csv module (handles quoted commas). Returns None if file missing or empty."""
    path = Path(csv_path)
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
        if not header:
            return None
        return [c.strip() for c in header]
    except (OSError, UnicodeDecodeError, csv.Error):
        return None


def _call_gemini(
    tmcf_content: str,
    api_key: str,
    model_id: str = "gemini-2.5-flash",
    stat_vars_content: str | None = None,
    stat_vars_schema_content: str | None = None,
    csv_header_columns: list[str] | None = None,
) -> str:
    """Call Gemini API for TMCF review (uses google-genai SDK)."""
    try:
        from google import genai
    except ImportError:
        print(
            "Error: google-genai not installed. Run: pip install google-genai",
            file=sys.stderr,
        )
        sys.exit(1)

    client = genai.Client(api_key=api_key)
    prompt = _build_prompt(
        tmcf_content,
        stat_vars_content=stat_vars_content,
        stat_vars_schema_content=stat_vars_schema_content,
        csv_header_columns=csv_header_columns,
    )
    # temperature=0 for deterministic schema linting (reproducible findings)
    try:
        from google.genai import types
        config = types.GenerateContentConfig(temperature=0)
    except (ImportError, AttributeError):
        config = None
    if config is not None:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=config,
        )
    else:
        response = client.models.generate_content(model=model_id, contents=prompt)
    return response.text if response.text else "[]"


def _build_prompt(
    tmcf_content: str,
    stat_vars_content: str | None = None,
    stat_vars_schema_content: str | None = None,
    csv_header_columns: list[str] | None = None,
) -> str:
    """Build the prompt for the LLM."""
    spec = TMCF_SPEC.strip()
    good = EXAMPLE_GOOD.strip()
    bad_typo = EXAMPLE_BAD_TYPO_OR_COLUMN.strip()
    bad_required = EXAMPLE_BAD_MISSING_REQUIRED.strip()
    bad_ns_dup = EXAMPLE_BAD_NAMESPACE_OR_DUPLICATE.strip()

    # When header is provided, column-vs-header is validated in a separate deterministic step; LLM focuses on other checks
    header_section = ""
    check5_and_9 = ""
    if csv_header_columns:
        header_list = "\n".join(f"- {c}" for c in csv_header_columns)
        header_section = (
            "\n\nTable header (column IDs from CSV; matching is case-sensitive). "
            "Column references are validated against this list in a separate step; do not flag column-not-in-header here. "
            "CSV columns not referenced in the TMCF (unused columns) are also reported in a separate step; do not flag them here.\n"
            + header_list
            + "\n"
        )
        check5_and_9 = """
5. Column references (internal consistency only): flag obvious inconsistencies within the TMCF (e.g. same node using C1 in one place and C01 in another). Do not flag column-not-in-header—that is validated separately.
9. Skip column-existence check (handled separately)."""
    else:
        check5_and_9 = """
5. Column references (e.g. C:table->C1): must be consistent within the TMCF; flag references that look like typos (e.g. C99 when only C1–C5 appear elsewhere).
9. If no table header list was provided, skip column-existence check."""

    extra = "\n\nOptional reference — known StatVars/schema (use to check if TMCF StatVar DCIDs or types exist):\n"
    extra += "Standard Data Commons schema types (treat as known; do not flag as unknown_statvar): dcs:StatisticalVariable, dcs:Percent, dcs:StatVarObservation.\n"
    if stat_vars_content or stat_vars_schema_content:
        if stat_vars_content:
            extra += "--- stat_vars.mcf (excerpt) ---\n" + (stat_vars_content[:12000] or "") + "\n"
        if stat_vars_schema_content:
            extra += "--- stat_vars_schema.mcf (excerpt) ---\n" + (stat_vars_schema_content[:8000] or "") + "\n"

    return f"""You are reviewing a Data Commons TMCF (Table MCF) file for quality issues. This is early linting only; the import tool is the authoritative validator. Only flag issues clearly supported by the rules below; do not guess schema. If unsure, do not report.

{spec}

Required validation checks (apply in order; flag each violation). Use the "type" value that best fits:
1. Required properties for StatVarObservation only: for each node with typeOf: dcs:StatVarObservation (or typeOf: StatVarObservation), the node must have exactly one of each: typeOf, variableMeasured, observationAbout, observationDate, value. Flag if any is missing (type: required). (Check 2 covers typeOf; here only flag missing variableMeasured, observationAbout, observationDate, or value.) Do not apply to other node types.
2. typeOf: exactly one typeOf per node; must be present and a valid schema type (e.g. dcs:StatVarObservation). Flag if missing or duplicated (type: schema).
3. No duplicate single-value properties: each property key must appear at most once per node. Flag duplicate keys (type: duplicate).
4. Property names must match Data Commons schema (e.g. variableMeasured, observationAbout). Flag typos in property names (type: typo).{check5_and_9}
6. DCID format: StatVar DCIDs should use UpperCamelCase segments separated by underscores (e.g. Count_Person, Median_Income_Household). No spaces or hyphens. Flag invalid format (type: format).
7. Namespace prefix:
- In TMCF, schema types such as StatVarObservation must use the dcs: prefix (e.g. typeOf: dcs:StatVarObservation).
- For StatVar DCIDs (e.g. Count_Person), dcid: is allowed.
- Do NOT require dcs:StatisticalVariable in instance MCF files; dcid:StatisticalVariable is valid.
Flag missing dcs: only when clearly required for schema types in TMCF (type: namespace).
8. If StatVar/schema reference is provided below: flag StatVar DCIDs or types that do not appear in the reference (type: unknown_statvar).
10. Unexpected properties: for nodes with typeOf: dcs:StatVarObservation, standard properties include typeOf, variableMeasured, observationAbout, observationDate, value, unit, measurementMethod, observationPeriod, scalingFactor. Flag any other property names that look non-standard or like typos (type: naming). Do not flag the standard ones.
11. Suspicious combinations: flag as warning if StatVarObservation has unit or scalingFactor but no measurementMethod when it would typically be expected (e.g. when unit is present and non-empty), or other clearly inconsistent combinations. Do not guess; only flag when clearly wrong.{header_section}
{extra}

Examples — correct:
{good}

Examples — issues to flag:
{bad_typo}

{bad_required}

{bad_ns_dup}

TMCF content to review:
---
{tmcf_content}
---

Respond with a JSON array only.
- Each item: "line" (integer line number in the TMCF content above, or null), "type", "message", "suggestion", "severity".
- Line number: Include the line number when the issue is on a specific line (e.g. duplicate property → line of the duplicate; typo → line with the wrong name; namespace → line with missing dcs:; naming → line with the unexpected property). Use null only for node-level issues where no single line is wrong (e.g. "missing required property" for a node — the node is incomplete, so null is fine).
- Suggestion: Always provide a short fix when possible. For missing required property X: "Add X with a column mapping (e.g. X: C:table->columnId)." For duplicate: "Remove the duplicate line." For namespace: "Add dcs: prefix (e.g. dcs:Value)." For typo: give the correct spelling.
- Use type: duplicate, required, namespace, format, typo, schema, naming, unknown_statvar, unused_column as defined in the checks.
- Severity: typo, schema, unknown_statvar, duplicate, required, namespace -> "blocker". Naming, unused_column, format -> "warning" unless it breaks schema (then "blocker").
- If no issues found, respond with: []
- Return only valid JSON, no markdown or extra text.
- Report at most 25 issues; if more exist, list the first 25 and omit the rest."""


def _parse_llm_response(text: str) -> list[dict]:
    """Parse LLM response into structured list. Normalize severity (blocker/warning)."""
    text = text.strip()
    # Remove markdown code blocks if present
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    try:
        result = json.loads(text)
        if isinstance(result, list):
            issues = result
        else:
            issues = [result] if isinstance(result, dict) else []
    except json.JSONDecodeError:
        return [{"type": "parse_error", "message": "Could not parse LLM response", "raw": text[:200], "severity": "blocker"}]

    # Cap issue count to avoid flooding UI/reports
    MAX_ISSUES = 25
    if len(issues) > MAX_ISSUES:
        issues = issues[:MAX_ISSUES]

    # Normalize severity: typo/schema/unknown_statvar/duplicate/required/namespace -> blocker; naming/unused_column/format -> warning unless LLM set blocker
    for i in issues:
        t = i.get("type")
        if t in ("typo", "schema", "unknown_statvar", "duplicate", "required", "namespace"):
            i["severity"] = "blocker"
        elif t in ("naming", "unused_column", "format"):
            # Respect prompt: naming/unused_column/format -> warning unless it breaks schema; let LLM escalate to blocker when needed
            i["severity"] = i.get("severity") if i.get("severity") in ("blocker", "warning") else "warning"
        elif i.get("severity") not in ("blocker", "warning"):
            i["severity"] = i.get("severity", "warning")
    return issues


def review_tmcf(
    tmcf_path: str,
    model_id: str = "gemini-2.5-flash",
    stat_vars_mcf_path: str | None = None,
    stat_vars_schema_mcf_path: str | None = None,
    csv_path: str | None = None,
    use_llm: bool = True,
) -> tuple[list[dict], bool, str | None]:
    """
    Review TMCF: always run deterministic checks (column refs, unused columns, value mapped).
    If use_llm=True and API key set, also call LLM and merge issues.
    Returns (issues_list, success, skip_reason).
    skip_reason "api_key_missing" when use_llm=True but no key (output still has deterministic issues).
    """
    path = Path(tmcf_path)
    if not path.exists():
        return [{"type": "error", "message": f"File not found: {tmcf_path}", "severity": "blocker"}], False, None

    content = path.read_text(encoding="utf-8", errors="replace")
    if not content.strip():
        return [{"type": "info", "message": "Empty TMCF file", "severity": "warning"}], True, None

    stat_vars_content: str | None = None
    stat_vars_schema_content: str | None = None
    if stat_vars_mcf_path and Path(stat_vars_mcf_path).exists():
        stat_vars_content = Path(stat_vars_mcf_path).read_text(encoding="utf-8", errors="replace")
    if stat_vars_schema_mcf_path and Path(stat_vars_schema_mcf_path).exists():
        stat_vars_schema_content = Path(stat_vars_schema_mcf_path).read_text(encoding="utf-8", errors="replace")

    csv_header_columns = _read_csv_header(csv_path) if csv_path else None

    # Deterministic checks (no LLM) — always run
    python_issues: list[dict] = []
    if csv_header_columns:
        python_issues.extend(_validate_column_refs_against_header(content, csv_header_columns))
        python_issues.extend(_find_unused_csv_columns(content, csv_header_columns))
    python_issues.extend(_check_value_column_mapped(content))
    python_issues.extend(_validate_observation_date_literals(content))
    # Reuse generated_dcids for format check and (below) stat_vars comparison so CSV is read at most once
    generated_dcids = _extract_generated_statvar_dcids(content, csv_path)
    python_issues.extend(_validate_statvar_dcid_format(content, csv_path, generated_dcids=generated_dcids))

    if stat_vars_content and stat_vars_mcf_path:
        python_issues.extend(_validate_stat_vars_mcf(stat_vars_content, stat_vars_mcf_path))
        # Compare generated StatVars (from TMCF/CSV) to stat_vars MCF; flag missing definitions
        if generated_dcids:
            python_issues.extend(
                _compare_generated_to_stat_vars_mcf(
                    generated_dcids, stat_vars_content, stat_vars_mcf_path
                )
            )
        # Percent/rate StatVars in stat_vars MCF should have measurementDenominator
        python_issues.extend(
            _check_percent_statvar_denominator(stat_vars_content, stat_vars_mcf_path)
        )
    if stat_vars_schema_content and stat_vars_schema_mcf_path:
        python_issues.extend(_validate_stat_vars_mcf(stat_vars_schema_content, stat_vars_schema_mcf_path))

    n = len(python_issues)
    if n == 0:
        print("✔ Deterministic checks passed (0 issues)", file=sys.stderr, flush=True)
    else:
        print(f"✖ Deterministic checks found {n} issue(s)", file=sys.stderr, flush=True)

    if not use_llm:
        print("LLM review disabled", file=sys.stderr, flush=True)
        return python_issues, True, None

    api_key = _get_api_key()
    if not api_key:
        print("LLM review skipped (no API key)", file=sys.stderr, flush=True)
        return python_issues, True, "api_key_missing"

    try:
        response_text = _call_gemini(
            content,
            api_key,
            model_id,
            stat_vars_content=stat_vars_content,
            stat_vars_schema_content=stat_vars_schema_content,
            csv_header_columns=csv_header_columns,
        )
        llm_issues = _parse_llm_response(response_text)
        # Prepend deterministic issues; cap total so LLM issues don't flood
        MAX_ISSUES = 25
        remaining = max(0, MAX_ISSUES - len(python_issues))
        combined = python_issues + llm_issues[:remaining]
        # Deduplicate by (type, normalized message) so whitespace variance doesn't create duplicates
        def _norm_msg(s):
            return " ".join((s or "").split())

        seen = set()
        issues = []
        for i in combined:
            key = (i.get("type"), _norm_msg(i.get("message")), i.get("file"))
            if key in seen:
                continue
            seen.add(key)
            issues.append(i)
        blocker_count = sum(1 for i in issues if (i.get("severity") or "").lower() == "blocker")
        advisory_count = len(issues) - blocker_count
        if blocker_count and advisory_count:
            print(f"✔ LLM review completed ({len(issues)} issues: {blocker_count} blocking, {advisory_count} advisory)", file=sys.stderr, flush=True)
        elif blocker_count:
            print(f"✔ LLM review completed ({blocker_count} blocking issue{'s' if blocker_count != 1 else ''})", file=sys.stderr, flush=True)
        else:
            print(f"✔ LLM review completed ({advisory_count} advisory finding{'s' if advisory_count != 1 else ''})", file=sys.stderr, flush=True)
        return issues, True, None
    except Exception as e:
        # LLM failure (timeout, network, API error) must NOT fail the pipeline.
        # Return deterministic issues only; add a warning so the report shows the failure.
        print(f"Gemini review failed (pipeline continues): {e}", file=sys.stderr, flush=True)
        return (
            python_issues + [{"type": "error", "message": str(e), "severity": "warning"}],
            True,
            None,
        )


def main():
    parser = argparse.ArgumentParser(description="Gemini Review (schema/typo) for TMCF")
    parser.add_argument("--tmcf", required=True, help="Path to TMCF file")
    parser.add_argument("--output", "-o", default="-", help="Output path (default: stdout, use - for stdout)")
    parser.add_argument(
        "--model",
        default="gemini-2.5-flash",
        help="Gemini model ID (default: gemini-2.5-flash)",
    )
    parser.add_argument(
        "--stat-vars-mcf",
        default="",
        help="Optional path to stat_vars.mcf for StatVar existence check",
    )
    parser.add_argument(
        "--stat-vars-schema-mcf",
        default="",
        help="Optional path to stat_vars_schema.mcf for schema reference",
    )
    parser.add_argument(
        "--csv",
        default="",
        help="Optional path to CSV file; first line (header) is used to validate column references in TMCF",
    )
    parser.add_argument(
        "--llm-review",
        action="store_true",
        help="Also run LLM review (requires GEMINI_API_KEY or GOOGLE_API_KEY). If omitted, only deterministic schema checks run.",
    )
    args = parser.parse_args()

    stat_vars = args.stat_vars_mcf.strip() or None
    stat_vars_schema = args.stat_vars_schema_mcf.strip() or None
    csv_path = args.csv.strip() or None

    issues, success, skip_reason = review_tmcf(
        args.tmcf,
        model_id=args.model,
        stat_vars_mcf_path=stat_vars,
        stat_vars_schema_mcf_path=stat_vars_schema,
        csv_path=csv_path,
        use_llm=args.llm_review,
    )

    source_file = Path(args.tmcf).name
    for i in issues:
        i.setdefault("file", source_file)

    if skip_reason == "api_key_missing":
        print("Skip: GEMINI_API_KEY or GOOGLE_API_KEY not set. Set it to enable Gemini review.", file=sys.stderr)
        # Still write deterministic issues (already in issues)
        output_json = json.dumps(issues, indent=2)
        if args.output != "-":
            Path(args.output).write_text(output_json, encoding="utf-8")
        else:
            print(output_json)
        has_blockers = any(
            i.get("severity") == "blocker"
            and i.get("type") not in ("parse_error", "error", "info")
            for i in issues
        )
        sys.exit(1 if has_blockers else 0)

    if not success:
        print("Error: Gemini review failed.", file=sys.stderr)
        output_json = json.dumps(issues, indent=2)
        if args.output == "-":
            print(output_json)
        else:
            Path(args.output).write_text(output_json, encoding="utf-8")
        sys.exit(1)

    output_json = json.dumps(issues, indent=2)
    if args.output == "-":
        print(output_json)
    else:
        Path(args.output).write_text(output_json, encoding="utf-8")

    # Exit 1 if any blocker (align with validation report: only blockers block)
    has_blockers = any(
        i.get("severity") == "blocker"
        and i.get("type") not in ("parse_error", "error", "info")
        for i in issues
    )
    sys.exit(1 if has_blockers else 0)


if __name__ == "__main__":
    main()
