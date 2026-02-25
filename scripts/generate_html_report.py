#!/usr/bin/env python3
"""Generate HTML report from validation_output.json.

Produces a Go/No-Go report with blocking validation failures (FAILED), warnings,
and passed rules. Also includes import run info, key counters, StatVar summary,
lint summary, and top lint issues from the import tool outputs.
Only FAILED rules block; WARNING rules do not.

Rules with special formatting in this script (if you add/rename rules in config,
update these for consistent report display): check_min_value, check_unit_consistency,
check_scaling_factor_consistency.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

# Shared fluctuation extraction (same package)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fluctuation_utils import extract_fluctuation_samples

# Counter names with this prefix are resolution diagnostics (DC API dependent); grouped separately in report.
LINT_RESOLUTION_DIAGNOSTICS_PREFIX = "Existence_FailedDcCall_"

# Rule IDs treated as system-level checks (e.g. pre-import safeguards). Excluded from "Validation" counts; shown under "System Checks".
SYSTEM_CHECK_IDS = frozenset()


def _rule_id(r: dict) -> str:
    """Return the rule/validation identifier for grouping (validation_name or validation_id)."""
    return (r.get("validation_name") or r.get("validation_id") or "").strip()


def _is_system_check(r: dict) -> bool:
    """True if this result is a system-level check (excluded from Validation section)."""
    return _rule_id(r) in SYSTEM_CHECK_IDS


def _escape_html(s):
    """Escape HTML special characters."""
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _render_details(details):
    """Render details dict as HTML."""
    if not details:
        return ""
    lines = []
    for k, v in details.items():
        if isinstance(v, (list, dict)):
            v = json.dumps(v, indent=2)
        lines.append(f"<tr><td>{_escape_html(k)}</td><td>{_escape_html(str(v))}</td></tr>")
    return f"<table class='details'>{''.join(lines)}</table>"


def _load_report_json(output_dir: str):
    """Load report.json from output dir. Returns None if not found."""
    path = os.path.join(output_dir, "report.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _load_summary_csv(output_dir: str):
    """Load summary_report.csv from output dir. Returns None if not found."""
    path = os.path.join(output_dir, "summary_report.csv")
    if not os.path.isfile(path):
        return None
    try:
        return pd.read_csv(path)
    except (pd.errors.EmptyDataError, OSError):
        return None


def _format_unit_display(raw: str) -> str:
    """Format unit value for display: avoid raw [] or [, x]. Empty/missing → (missing), one value → as-is, multiple → comma-joined."""
    if raw is None or not isinstance(raw, str):
        return "(missing)"
    s = raw.strip()
    if not s or s in ("—", "[]"):
        return "(missing)"
    if s.startswith("["):
        inner = s[1:].rstrip("]").strip()
        parts = [p.strip() for p in inner.split(",") if p.strip()]
        if not parts:
            return "(missing)"
        return ", ".join(parts)
    return s


def _load_summary_statvar_units(output_dir: str) -> list[tuple[str, str]]:
    """Load (StatVar, Units) from summary_report.csv. Returns [] if not found or no Units column."""
    df = _load_summary_csv(output_dir)
    if df is None or df.empty:
        return []
    sv_col = "StatVar" if "StatVar" in df.columns else "stat_var"
    unit_col = "Units" if "Units" in df.columns else "units"
    if sv_col not in df.columns or unit_col not in df.columns:
        return []
    out = []
    for _, row in df.iterrows():
        sv = str(row.get(sv_col, "") or "").strip()
        unit = str(row.get(unit_col, "") or "").strip()
        if sv:
            out.append((sv, unit or "—"))
    return out


def _load_llm_review(output_dir: str):
    """Load schema_review.json from output dir (schema + optional Gemini review issues). Returns None if not found or invalid."""
    path = os.path.join(output_dir, "schema_review.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return None


def _load_pipeline_failure(output_dir: str) -> dict | None:
    """Load pipeline_failure.json from output dir. Returns None if not found or invalid."""
    path = os.path.join(output_dir, "pipeline_failure.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and data.get("stage") and data.get("reason"):
            return data
        return None
    except (json.JSONDecodeError, OSError):
        return None


def _extract_rule_failure_samples(results: list) -> list[dict]:
    """Parse validation results and return structured rule failure samples."""
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
                    "rule": rule,
                    "expected": expected,
                    "value": value if value is not None else "—",
                    "message": message,
                })
        elif rule == "check_unit_consistency":
            units_seen = details.get("units") or details.get("unit_values") or details.get("units_seen")
            if units_seen is not None and not isinstance(units_seen, str):
                units_seen = str(units_seen)
            samples.append({
                "statVar": None,
                "rule": rule,
                "expected": "consistent units (one unit per StatVar)",
                "value": _format_unit_display(units_seen or ""),
                "message": message,
            })
        elif rule == "check_scaling_factor_consistency":
            failing_rows = details.get("failing_rows") or []
            expected = params.get("condition") or "consistent scaling factor"
            for row in failing_rows:
                stat_var = row.get("StatVar") or row.get("stat_var") or ""
                value = row.get("ScalingFactors") or row.get("scaling_factors")
                if value is not None and not isinstance(value, str):
                    value = str(value)
                samples.append({
                    "statVar": stat_var,
                    "rule": rule,
                    "expected": expected,
                    "value": value if value is not None else "—",
                    "message": message,
                })
        elif rule == "check_structural_lint_error_count":
            samples.append({
                "statVar": None,
                "rule": rule,
                "expected": "0 structural lint errors",
                "value": None,
                "message": message,
            })
        else:
            # Generic fallback for rules without custom handling.
            samples.append({
                "statVar": None,
                "rule": rule,
                "expected": "",
                "value": None,
                "message": message,
            })
    return samples


def _is_llm_blocker(i: dict) -> bool:
    """True if issue is a blocker. Severity is normalized in llm_schema_review; missing severity is treated as warning."""
    return i.get("severity") == "blocker"


def _render_llm_section(output_dir: str, gemini_review_enabled: bool = False) -> str:
    """Render AI Advisory Findings section. When AI was not enabled, show 'Not enabled for this run'."""
    if not gemini_review_enabled:
        return """
    <section class="report-section" id="ai-review">
      <h2>AI Advisory Findings (Non-Blocking)</h2>
      <p class="empty">Not enabled for this run.</p>
    </section>
"""
    issues = _load_llm_review(output_dir)
    if issues is None:
        return """
    <section class="report-section" id="ai-review">
      <h2>AI Advisory Findings (Non-Blocking)</h2>
      <p class="empty">No review data.</p>
    </section>
"""
    blockers = [i for i in issues if _is_llm_blocker(i)]
    advisories = [i for i in issues if not _is_llm_blocker(i)]
    if not blockers and not advisories:
        return """
    <section class="report-section" id="ai-review">
      <h2>AI Advisory Findings (Non-Blocking)</h2>
      <p class="empty">Passed — no issues found.</p>
    </section>
"""
    html = """
    <section class="report-section" id="ai-review">
      <h2>AI Advisory Findings (Non-Blocking)</h2>
"""
    if blockers:
        html += "      <p class='advisory-note'>Advisory: these issues do not affect Overall pass/fail; that is based on validation rules only.</p>\n"
        html += "      <p><strong>Blocking (AI)</strong></p>\n      <table class='details lint-table'><thead><tr><th>File</th><th>Line</th><th>Type</th><th>Message</th><th>Suggestion</th></tr></thead><tbody>\n"
        for i in blockers:
            file_str = _escape_html(i.get("file") or "—")
            line = i.get("line")
            line_str = str(line) if line is not None else "—"
            html += f"        <tr><td>{file_str}</td><td>{_escape_html(line_str)}</td><td>{_escape_html(i.get('type', ''))}</td><td>{_escape_html(i.get('message', ''))}</td><td>{_escape_html(i.get('suggestion', ''))}</td></tr>\n"
        html += "      </tbody></table>\n"
    if advisories:
        html += "      <p><strong>Advisories</strong></p>\n      <table class='details lint-table'><thead><tr><th>File</th><th>Line</th><th>Type</th><th>Message</th><th>Suggestion</th></tr></thead><tbody>\n"
        for i in advisories:
            file_str = _escape_html(i.get("file") or "—")
            line = i.get("line")
            line_str = str(line) if line is not None else "—"
            html += f"        <tr><td>{file_str}</td><td>{_escape_html(line_str)}</td><td>{_escape_html(i.get('type', ''))}</td><td>{_escape_html(i.get('message', ''))}</td><td>{_escape_html(i.get('suggestion', ''))}</td></tr>\n"
        html += "      </tbody></table>\n"
    html += "    </section>\n"
    return html


def _render_compact_summary_block(
    overall: str,
    overall_class: str,
    n_val_blockers: int,
    n_val_warnings: int,
    n_val_passed: int,
    n_sys_blockers: int,
    n_sys_warnings: int,
    n_sys_passed: int,
    gemini_review_enabled: bool,
    gemini_review_count: int,
    fluctuation_count: int,
    rule_failure_count: int,
) -> str:
    """Render summary stat strip at top of report. Validation row = config-defined rules only; System Checks = system-level safeguards."""
    if gemini_review_enabled:
        ai_val = f"{gemini_review_count} AI advisor{'ies' if gemini_review_count != 1 else 'y'}"
    else:
        ai_val = "Not enabled for this run"
    val_str = f"{n_val_blockers} failed · {n_val_warnings} warning{'s' if n_val_warnings != 1 else ''} · {n_val_passed} passed"
    sys_str = f"{n_sys_blockers} failed · {n_sys_warnings} warning{'s' if n_sys_warnings != 1 else ''} · {n_sys_passed} passed"
    return (
        "<div class='summary-strip'>"
        f"<div class='summary-stat'><span class='summary-label'>Validation</span><span class='summary-val'>{val_str}</span></div>"
        f"<div class='summary-stat'><span class='summary-label'>System Checks</span><span class='summary-val'>{sys_str}</span></div>"
        f"<div class='summary-stat'><span class='summary-label'>AI Advisories</span><span class='summary-val'>{ai_val}</span></div>"
        f"<div class='summary-stat'><span class='summary-label'>Fluctuation</span><span class='summary-val'>{fluctuation_count}</span></div>"
        f"<div class='summary-stat'><span class='summary-label'>Rule failures</span><span class='summary-val'>{rule_failure_count}</span></div>"
        "</div>"
    )


def _format_change_pct(pct: float | None) -> str:
    """Format percent change for display; match UI formatChangePct (K%, M% for large values)."""
    if pct is None:
        return "—"
    sign = "+" if pct >= 0 else "-"
    abs_pct = abs(pct)
    if abs_pct >= 1e6:
        return f"{sign}{(abs_pct / 1e6):.1f}M%"
    if abs_pct >= 1e3:
        return f"{sign}{(abs_pct / 1e3):.1f}K%"
    return f"{sign}{int(round(abs_pct)):,}%"


def _format_technical_signals_row(ts: dict) -> str:
    """Format technical_signals dict as HTML for report (screenshot-friendly). All values escaped for XSS safety."""
    if not ts:
        return ""
    def fmt_num(n):
        if n is None:
            return "—"
        if isinstance(n, float):
            return f"{n:.4g}" if n != int(n) else str(int(n))
        return str(n)
    def fmt_pct(n):
        return _format_change_pct(n)
    def yes_no(b):
        if b is None:
            return "Unknown"
        return "Yes" if b else "No"
    if ts.get("zero_baseline"):
        change_str = _escape_html(ts.get("change_message") or "Increase from zero baseline (percentage undefined)")
        if ts.get("absolute_change") is not None:
            change_str += f" (absolute change: {_escape_html(fmt_num(ts.get('absolute_change')))})"
    else:
        change_str = _escape_html(fmt_pct(ts.get("percent_change")))
    lines = [
        f"<strong>Previous value:</strong> {_escape_html(fmt_num(ts.get('previous_value')))}",
        f"<strong>Current value:</strong> {_escape_html(fmt_num(ts.get('current_value')))}",
        f"<strong>Change:</strong> {change_str}",
        f"<strong>Previous near zero:</strong> {_escape_html(yes_no(ts.get('previous_near_zero')))}",
        f"<strong>Scaling factor changed:</strong> {_escape_html(yes_no(ts.get('scaling_changed')))}",
        f"<strong>Unit changed:</strong> {_escape_html(yes_no(ts.get('unit_changed')))}",
        f"<strong>Missing intermediate periods:</strong> {_escape_html(yes_no(ts.get('missing_intermediate_periods')))}",
        f"<strong>First valid after placeholder:</strong> {_escape_html(yes_no(ts.get('first_valid_after_placeholder')))}",
    ]
    return "<br>\n        ".join(lines)


def _render_fluctuation_section(report: dict, gemini_review_enabled: bool = False) -> tuple[str, list[dict]]:
    """Render Data Fluctuation Analysis section. Returns (html, list of payloads for Explain when gemini_review_enabled)."""
    samples = extract_fluctuation_samples(report) if report else []
    explain_payloads: list[dict] = []
    html = """
    <section class="report-section" id="fluctuation">
      <h2>Data Fluctuation Analysis</h2>
"""
    if not samples:
        html += "      <p class='empty'>No significant fluctuations (>100%) detected.</p>\n"
    else:
        has_100 = any(s.get("counterKey") == "StatsCheck_MaxPercentFluctuationGreaterThan100" for s in samples)
        has_500 = any(s.get("counterKey") == "StatsCheck_MaxPercentFluctuationGreaterThan500" for s in samples)
        if has_100 and has_500:
            threshold_label = "100% / 500%"
        elif has_500:
            threshold_label = "500%"
        else:
            threshold_label = "100%"
        html += f"      <p>{len(samples)} fluctuation(s) above {threshold_label} threshold.</p>\n"
        html += "      <table class='details lint-table'><thead><tr><th scope='col'>StatVar</th><th scope='col'>Location</th><th scope='col'>Change</th><th scope='col'>Period</th></tr></thead><tbody>\n"
        for idx, s in enumerate(samples):
            pct = s.get("percentDifference")
            change_display = (
                s.get("change_message")
                if s.get("technical_signals", {}).get("zero_baseline")
                else _format_change_pct(pct)
            )
            points = s.get("problemPoints") or []
            period = ""
            if len(points) >= 2:
                period = f"{points[-2].get('date', '')} → {points[-1].get('date', '')}"
            html += f"        <tr><td>{_escape_html(s.get('statVar') or '—')}</td><td>{_escape_html(s.get('location') or '—')}</td><td>{_escape_html(change_display)}</td><td>{_escape_html(period)}</td></tr>\n"
            ts = s.get("technical_signals")
            if ts:
                html += "        <tr><td colspan='4' class='technical-signals-cell'><div class='technical-signals-title'>Technical Signals</div><div class='technical-signals'>" + _format_technical_signals_row(ts) + "</div>"
                if gemini_review_enabled:
                    html += f"<div class='fluctuation-explain-wrap'><button type='button' class='explain-fluctuation-btn' data-fluctuation-index='{idx}'>Explain with AI</button><div class='ai-interpretation-cell' id='ai-interpretation-{idx}' aria-live='polite'></div></div>"
                    explain_payloads.append({
                        "statVar": s.get("statVar") or "",
                        "location": s.get("location") or "",
                        "period": period,
                        "percent_change": pct if not (ts and ts.get("zero_baseline")) else None,
                        "technical_signals": ts,
                    })
                html += "</td></tr>\n"
        html += "      </tbody></table>\n"
    html += "    </section>\n"
    return html, explain_payloads


def _get_csv_path_from_report(output_dir: str) -> str | None:
    """Get input CSV path from report.json commandArgs.inputFiles. Returns None if not found."""
    report = _load_report_json(output_dir)
    if not report:
        return None
    for p in (report.get("commandArgs") or {}).get("inputFiles") or []:
        path = str(p).strip()
        if not path.lower().endswith(".csv"):
            continue
        if os.path.isfile(path):
            return path
        if not os.path.isabs(path):
            candidate = os.path.join(output_dir, path)
            if os.path.isfile(candidate):
                return candidate
    return None


def _load_csv_rows(csv_path: str) -> list[dict] | None:
    """Load CSV as list of dicts (first row = headers). Returns None on failure."""
    import csv as csv_module
    try:
        with open(csv_path, encoding="utf-8", newline="") as f:
            reader = csv_module.DictReader(f)
            return list(reader)
    except (OSError, csv_module.Error):
        return None


def _stat_var_matches(csv_val: str | None, stat_var: str) -> bool:
    """Match CSV variableMeasured/StatVar to validation stat_var."""
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


def _enrich_rule_failure_samples(samples: list[dict], output_dir: str, results: list) -> None:
    """Enrich samples with location, date, sourceRow from CSV when possible. Mutates samples in place."""
    csv_path = _get_csv_path_from_report(output_dir)
    if not csv_path:
        return
    rows = _load_csv_rows(csv_path)
    if not rows:
        return
    csv_basename = os.path.basename(csv_path)
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
                        "rule": s["rule"],
                        "expected": expected,
                        "value": _format_unit_display(unit),
                        "message": msg,
                    })
            else:
                expanded.append(s)
        else:
            expanded.append(s)
    samples.clear()
    samples.extend(expanded)


def _render_rule_failure_section(results: list, output_dir: str) -> str:
    """Render Rule failure samples section from validation results (enriched with CSV when available)."""
    samples = _extract_rule_failure_samples(results) if results else []
    _enrich_rule_failure_samples(samples, output_dir, results or [])
    html = """
    <section class="report-section" id="rule-failures">
      <h2>Rule failure samples</h2>
"""
    if not samples:
        html += "      <p class='empty'>No validation rule failures detected.</p>\n"
    else:
        rule_count = len({s.get("rule") for s in samples if s.get("rule")})
        if rule_count == 1 and len(samples) == 1:
            html += "      <p>1 blocking rule failed.</p>\n"
        elif len(samples) != rule_count:
            html += f"      <p>{rule_count} blocking rules failed ({len(samples)} affected entries).</p>\n"
        else:
            html += f"      <p>{rule_count} blocking rules failed.</p>\n"
        html += "      <table class='details lint-table'><thead><tr><th>Rule</th><th>StatVar</th><th>Expected</th><th>Value</th><th>Location</th><th>Source row</th><th>Message</th></tr></thead><tbody>\n"
        for s in samples:
            loc = s.get("location") or "—"
            src = s.get("sourceRow") or "—"
            val = s.get("value") or "—"
            if (s.get("rule") or "") == "check_unit_consistency":
                val_cell = "Unit: " + _escape_html(str(val))
            else:
                val_cell = _escape_html(str(val))
            html += f"        <tr><td>{_escape_html(s.get('rule') or '—')}</td><td>{_escape_html(str(s.get('statVar') or '—'))}</td><td>{_escape_html(str(s.get('expected') or '—'))}</td><td>{val_cell}</td><td>{_escape_html(str(loc))}</td><td>{_escape_html(str(src))}</td><td>{_escape_html(str(s.get('message') or '')[:80])}</td></tr>\n"
        html += "      </tbody></table>\n"
    html += "    </section>\n"
    return html


def _render_jar_report_link_section(dataset_name: str, output_dir: str) -> str:
    """Render link to JAR summary report if it exists. Link is dataset-only; when served by run_id the server rewrites it to /summary-report/{dataset}/{run_id}."""
    if not dataset_name:
        return ""
    path = os.path.join(output_dir, "summary_report.html")
    if not os.path.isfile(path):
        return ""
    return f"""
    <section class="report-section" id="see-also">
      <h2>See also</h2>
      <p><a href="/summary-report/{_escape_html(dataset_name)}" target="_blank" rel="noopener">View full import tool report</a> — detailed counters, sample places, time series charts</p>
    </section>
"""


def _render_import_run_section(report: dict) -> str:
    """Render Import Run Info section."""
    content = _render_import_run_info(report)
    if not content:
        return ""
    return """
    <section class="report-section" id="import-run">
      <h2>Import Run Info</h2>
      """ + content + """
    </section>
"""


def _render_import_run_info(report: dict) -> str:
    """Render import run info from report.json commandArgs and runtimeMetadata."""
    if not report:
        return ""
    parts = []
    cmd = report.get("commandArgs", {})
    if cmd.get("inputFiles"):
        files = cmd["inputFiles"]
        if isinstance(files, list):
            for f in files:
                name = os.path.basename(str(f))
                lower = name.lower()
                if lower.endswith(".csv"):
                    label = "CSV"
                elif lower.endswith((".tmcf", ".mcf")):
                    if "stat_var" in lower or "statvar" in lower or "schema" in lower:
                        label = "StatVar MCF"
                    else:
                        label = "Node TMCF"
                else:
                    label = "Input"
                parts.append(f"<tr><td>{_escape_html(label)}</td><td>{_escape_html(name)}</td></tr>")
        else:
            parts.append(f"<tr><td>Input</td><td>{_escape_html(str(files))}</td></tr>")
    meta = report.get("runtimeMetadata", {})
    if meta.get("startTime") and meta.get("endTime"):
        try:
            start = datetime.fromisoformat(meta["startTime"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(meta["endTime"].replace("Z", "+00:00"))
            dur = (end - start).total_seconds()
            parts.append(f"<tr><td>Generation duration</td><td>{int(dur)} seconds</td></tr>")
        except (ValueError, TypeError):
            pass
    if meta.get("toolVersion"):
        parts.append(f"<tr><td>Tool version</td><td>{_escape_html(str(meta['toolVersion']))}</td></tr>")
    if cmd.get("existenceChecks") is not None:
        parts.append(f"<tr><td>Existence checks</td><td>{'yes' if cmd['existenceChecks'] else 'no'}</td></tr>")
    if cmd.get("resolution"):
        parts.append(f"<tr><td>Resolution mode</td><td>{_escape_html(str(cmd['resolution']))}</td></tr>")
    if not parts:
        return ""
    return f"<table class='details'>{''.join(parts)}</table>"


def _render_key_counters_section(report: dict, stats_df) -> str:
    """Render Key Counters section."""
    content = _render_key_counters(report, stats_df)
    if not content:
        return ""
    return """
    <section class="report-section" id="key-counters">
      <h2>Key Counters</h2>
      """ + content + """
    </section>
"""


def _render_key_counters(report: dict, stats_df) -> str:
    """Render key counters: total observations first, then NumRowSuccesses, etc."""
    parts = []
    if stats_df is not None and "NumObservations" in stats_df.columns:
        total = int(stats_df["NumObservations"].sum())
        parts.append(f"<tr><td>Total observations</td><td>{total}</td></tr>")
    if report:
        info = report.get("levelSummary", {}).get("LEVEL_INFO", {}).get("counters", {})
        for key in ("NumRowSuccesses", "NumNodeSuccesses", "NumPVSuccesses"):
            if key in info:
                parts.append(f"<tr><td>{_escape_html(key)}</td><td>{_escape_html(str(info[key]))}</td></tr>")
    if not parts:
        return ""
    return f"<table class='details'>{''.join(parts)}</table>"


def _render_statvar_section(stats_df) -> str:
    """Render StatVar Summary section."""
    content = _render_statvar_table(stats_df)
    if not content:
        return ""
    return """
    <section class="report-section" id="statvar">
      <h2>StatVar Summary</h2>
      """ + content + """
    </section>
"""


def _format_cell(val):
    """Format cell value for display; empty lists show as dash."""
    if pd.isna(val):
        return "—"
    s = str(val).strip()
    if s in ("[]", "nan", ""):
        return "—"
    return s


def _render_statvar_table(stats_df) -> str:
    """Render StatVar summary table from summary_report.csv."""
    if stats_df is None or stats_df.empty:
        return ""
    cols = ["StatVar", "NumPlaces", "NumObservations", "MinValue", "MaxValue", "MinDate", "MaxDate", "Units"]
    available = [c for c in cols if c in stats_df.columns]
    if not available:
        return ""
    subset = stats_df[available].head(50)  # limit to 50 rows
    rows = []
    for _, row in subset.iterrows():
        cells = "".join(f"<td>{_escape_html(_format_cell(row[c]))}</td>" for c in available)
        rows.append(f"<tr>{cells}</tr>")
    headers = "".join(f"<th>{_escape_html(c)}</th>" for c in available)
    return f"<table class='details statvar-table'><thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def _render_lint_summary_section(report: dict) -> str:
    """Render Lint Summary section."""
    content = _render_lint_summary(report)
    if not content:
        return ""
    return """
    <section class="report-section" id="lint-summary">
      <h2>Lint Summary</h2>
      <p class="section-desc">Totals are sums of counter values per level (counter aggregation). The entry list in Top Lint may have a different count.</p>
      """ + content + """
    </section>
"""


def _get_lint_summary_totals(report: dict) -> dict[str, int]:
    """Return per-level totals from levelSummary (same source as Lint Summary table). Keys: ERROR, WARNING, INFO."""
    totals = {"ERROR": 0, "WARNING": 0, "INFO": 0}
    if not report:
        return totals
    level_summary = report.get("levelSummary", {})
    for level in ("LEVEL_INFO", "LEVEL_WARNING", "LEVEL_ERROR"):
        counters = level_summary.get(level, {}).get("counters", {})
        total = sum(int(v) for v in counters.values() if str(v).isdigit())
        key = level.replace("LEVEL_", "")
        totals[key] = total
    return totals


def _render_lint_summary(report: dict) -> str:
    """Render lint summary: counts of INFO, WARNING, ERROR."""
    if not report:
        return ""
    totals = _get_lint_summary_totals(report)
    parts = []
    for level in ("ERROR", "WARNING", "INFO"):
        total = totals.get(level, 0)
        if total > 0:
            parts.append(f"<tr><td>{_escape_html(level)}</td><td>{total}</td></tr>")
    if not parts:
        return ""
    return f"<table class='details'>{''.join(parts)}</table>"


def _render_counter_breakdown_section(report: dict) -> str:
    """Render all counters with counts, collapsible (details/summary).
    For LEVEL_ERROR, counters are grouped into Structural Errors (blocking) vs
    Resolution Diagnostics (Existence_FailedDcCall_*).
    """
    if not report:
        return ""
    level_summary = report.get("levelSummary", {})
    levels_order = ("LEVEL_FATAL", "LEVEL_ERROR", "LEVEL_WARNING", "LEVEL_INFO")
    parts = []
    for level in levels_order:
        counters = level_summary.get(level, {}).get("counters", {})
        if not counters:
            continue
        label = level.replace("LEVEL_", "")
        total = sum(int(v) for v in counters.values() if str(v).isdigit())
        sorted_items = sorted(
            counters.items(),
            key=lambda x: (-int(x[1]) if str(x[1]).isdigit() else 0, x[0]),
        )
        if level == "LEVEL_ERROR":
            structural = [(n, c) for n, c in sorted_items if not n.startswith(LINT_RESOLUTION_DIAGNOSTICS_PREFIX)]
            resolution = [(n, c) for n, c in sorted_items if n.startswith(LINT_RESOLUTION_DIAGNOSTICS_PREFIX)]
            rows = []
            if structural:
                rows.append(
                    f"<tr class='counter-level-header'><td colspan='2'><strong>{_escape_html(label)}</strong> (total {total})</td></tr>"
                )
                rows.append(
                    f"<tr class='counter-group-header'><td colspan='2'>Structural errors (blocking)</td></tr>"
                )
                for name, count in structural:
                    rows.append(f"<tr><td>{_escape_html(name)}</td><td>{_escape_html(str(count))}</td></tr>")
            if resolution:
                if not rows:
                    rows.append(
                        f"<tr class='counter-level-header'><td colspan='2'><strong>{_escape_html(label)}</strong> (total {total})</td></tr>"
                    )
                rows.append(
                    f"<tr class='counter-group-header'><td colspan='2'>Resolution diagnostics (Existence_FailedDcCall_*)</td></tr>"
                )
                for name, count in resolution:
                    rows.append(f"<tr><td>{_escape_html(name)}</td><td>{_escape_html(str(count))}</td></tr>")
        else:
            rows = []
            rows.append(
                f"<tr class='counter-level-header'><td colspan='2'><strong>{_escape_html(label)}</strong> (total {total})</td></tr>"
            )
            for name, count in sorted_items:
                rows.append(f"<tr><td>{_escape_html(name)}</td><td>{_escape_html(str(count))}</td></tr>")
        if rows:
            parts.append("".join(rows))
    if not parts:
        return ""
    table = f"<table class='details counter-breakdown-table'><tbody>{''.join(parts)}</tbody></table>"
    return f"""
    <details class="counter-breakdown-details">
      <summary>Show all counters with counts</summary>
      <div class="counter-breakdown-content">
{table}
      </div>
    </details>
"""


def _render_counter_breakdown_section_wrapped(report: dict) -> str:
    """Wrap counter breakdown in a section with id for nav."""
    content = _render_counter_breakdown_section(report)
    if not content.strip():
        return ""
    return """
    <section class="report-section" id="counter-breakdown">
      <h2>Counter breakdown</h2>
      <p class="section-desc">All counters from the import tool report (same as in the JAR summary).</p>
      """ + content + """
    </section>
"""


def _render_data_holes_section(report: dict) -> str:
    """Render Data Holes (StatsCheck_Data_Holes) from statsCheckSummary. Advisory only."""
    if not report:
        return ""
    stats_check = report.get("statsCheckSummary") or []
    holes = []
    for item in stats_check:
        for vc in item.get("validationCounters") or []:
            if vc.get("counterKey") == "StatsCheck_Data_Holes":
                holes.append({
                    "place": item.get("placeDcid", "—"),
                    "statVar": item.get("statVarDcid", "—"),
                    "details": vc.get("additionalDetails", ""),
                    "observationPeriod": item.get("observationPeriod", ""),
                    "scalingFactor": item.get("scalingFactor", ""),
                })
                break
    if not holes:
        return """
    <section class="report-section" id="data-holes">
      <h2>Data holes</h2>
      <p class="section-desc">Advisory: gaps in time-series dates (StatsCheck_Data_Holes). Does not affect pass/fail.</p>
      <p class="empty">No data holes reported.</p>
    </section>
"""
    rows = []
    for h in holes:
        details = _escape_html(h["details"])
        period = f"<br><span class='data-hole-meta'>Period: {_escape_html(h['observationPeriod'])}</span>" if h["observationPeriod"] else ""
        rows.append(
            f"<tr><td>{_escape_html(h['place'])}</td><td>{_escape_html(h['statVar'])}</td>"
            f"<td>{details}{period}</td></tr>"
        )
    table = (
        "<table class='details lint-table'><thead><tr>"
        "<th>Place</th><th>StatVar</th><th>Details</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )
    return f"""
    <section class="report-section" id="data-holes">
      <h2>Data holes</h2>
      <p class="section-desc">Advisory: gaps in time-series dates (StatsCheck_Data_Holes). Does not affect pass/fail.</p>
      {table}
    </section>
"""


def _lint_entries_to_rows(entries: list, msg_max_len: int = 120) -> list[str]:
    """Build table rows for lint entries (File, Line, Level, Counter, Message)."""
    rows = []
    for e in entries:
        loc = e.get("location", {})
        file_name = loc.get("file", "?")
        line = loc.get("lineNumber", "?")
        level = e.get("level", "").replace("LEVEL_", "")
        counter_key = e.get("counterKey", "")
        msg = (e.get("userMessage") or "")[:msg_max_len]
        if len(e.get("userMessage") or "") > msg_max_len:
            msg += "..."
        rows.append(
            f"<tr><td>{_escape_html(file_name)}</td><td>{_escape_html(str(line))}</td>"
            f"<td>{_escape_html(level)}</td><td>{_escape_html(counter_key)}</td><td>{_escape_html(msg)}</td></tr>"
        )
    return rows


def _lint_line_number(loc: dict) -> int:
    """Normalize location.lineNumber to int for stable sorting."""
    n = (loc or {}).get("lineNumber", 0)
    if isinstance(n, int):
        return n
    return int(n) if isinstance(n, str) and n.isdigit() else 0


def _render_top_lint_section(report: dict) -> str:
    """Render Top Lint Issues: first 10, then collapsible full list grouped into Structural vs Resolution."""
    if not report:
        return ""
    entries = report.get("entries", [])
    if not entries:
        return """
    <section class="report-section" id="top-lint">
      <h2>Top Lint Issues</h2>
      <p class="empty">No lint entries in report.</p>
    </section>
"""
    by_level = {"LEVEL_ERROR": 0, "LEVEL_WARNING": 1, "LEVEL_INFO": 2}
    sorted_entries = sorted(
        entries,
        key=lambda e: (
            by_level.get(e.get("level", ""), 3),
            (e.get("location") or {}).get("file", ""),
            _lint_line_number(e.get("location") or {}),
        ),
    )
    structural_entries = [
        e for e in sorted_entries
        if not (e.get("counterKey") or "").startswith(LINT_RESOLUTION_DIAGNOSTICS_PREFIX)
    ]
    resolution_entries = [
        e for e in sorted_entries
        if (e.get("counterKey") or "").startswith(LINT_RESOLUTION_DIAGNOSTICS_PREFIX)
    ]
    total_entries = len(sorted_entries)
    summary_totals = _get_lint_summary_totals(report)
    summary_sum = summary_totals["ERROR"] + summary_totals["WARNING"] + summary_totals["INFO"]
    if summary_sum != total_entries and summary_sum > 0:
        summary_note = f" Lint Summary totals by level: {summary_totals['ERROR']} ERROR, {summary_totals['WARNING']} WARNING, {summary_totals['INFO']} INFO (counter aggregation; entry list may differ)."
    else:
        summary_note = ""
    # Preview: up to 5 structural first, then fill up to 10 total with resolution (more representative when both exist).
    top_structural = structural_entries[:5]
    remaining_slots = 10 - len(top_structural)
    top_resolution = resolution_entries[:remaining_slots]
    top_entries = top_structural + top_resolution
    top_rows = _lint_entries_to_rows(top_entries)
    table_header = (
        "<table class='details lint-table'><thead><tr>"
        "<th>File</th><th>Line</th><th>Level</th><th>Counter</th><th>Message</th></tr></thead>"
    )
    top_table = table_header + f"<tbody>{''.join(top_rows)}</tbody></table>"
    structural_rows = _lint_entries_to_rows(structural_entries)
    resolution_rows = _lint_entries_to_rows(resolution_entries)
    structural_table = ""
    if structural_entries:
        structural_table = (
            "<h3 class='top-lint-group-heading'>Structural errors (blocking)</h3>"
            + table_header
            + f"<tbody>{''.join(structural_rows)}</tbody></table>"
        )
    resolution_table = ""
    if resolution_entries:
        resolution_table = (
            "<h3 class='top-lint-group-heading'>Resolution diagnostics (Existence_FailedDcCall_*)</h3>"
            + table_header
            + f"<tbody>{''.join(resolution_rows)}</tbody></table>"
        )
    full_tables_grouped = structural_table + resolution_table
    show_all_label = f"Show all lint issues ({total_entries})"
    hide_label = "Hide full lint list"
    return f"""
    <section class="report-section" id="top-lint">
      <h2>Top Lint Issues</h2>
      <p class="top-lint-summary">Showing first 10 of {total_entries} entries.{_escape_html(summary_note)}</p>
      {top_table}
      <details class="full-lint-details" id="full-lint-details">
        <summary id="full-lint-summary">{_escape_html(show_all_label)}</summary>
        <div class="full-lint-table-wrap">{full_tables_grouped}</div>
      </details>
      <script>
        (function() {{
          var details = document.getElementById('full-lint-details');
          var summary = document.getElementById('full-lint-summary');
          if (details && summary) {{
            var showAll = {json.dumps(show_all_label)};
            var hideLabel = {json.dumps(hide_label)};
            details.addEventListener('toggle', function() {{
              summary.textContent = details.open ? hideLabel : showAll;
            }});
          }}
        }})();
      </script>
    </section>
"""


def generate_html(
    validation_output_path: str,
    html_output_path: str,
    dataset_name: str = "",
    overall_override: str | None = None,
    gemini_review_enabled: bool = False,
) -> bool:
    """Generate HTML report from validation_output.json.

    overall_override: If "fail", report shows Overall: FAIL regardless of validation_output.json
    (e.g. when run failed due to counters check or genmcf failure). If "pass", use rules result.
    gemini_review_enabled: If True, show Explain button per fluctuation anomaly (lazy AI interpretation).
    """
    try:
        with open(validation_output_path, encoding="utf-8") as f:
            results = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading {validation_output_path}: {e}", file=sys.stderr)
        return False

    # Split into validation rules (config-defined) vs system checks for display.
    validation_results = [r for r in results if not _is_system_check(r)]
    system_check_results = [r for r in results if _is_system_check(r)]

    blockers = [r for r in results if r.get("status") == "FAILED"]
    warnings = [r for r in results if r.get("status") == "WARNING"]
    passed = [r for r in results if r.get("status") == "PASSED"]
    other = [r for r in results if r.get("status") not in ("FAILED", "PASSED", "WARNING")]

    # Validation-section: only config-defined rules (exclude system checks).
    val_blockers = [r for r in validation_results if r.get("status") == "FAILED"]
    val_warnings = [r for r in validation_results if r.get("status") == "WARNING"]
    val_passed = [r for r in validation_results if r.get("status") == "PASSED"]
    n_val_blockers = len(val_blockers)
    n_val_warnings = len(val_warnings)
    n_val_passed = len(val_passed)

    # System checks: separate counts for summary and section.
    sys_blockers = [r for r in system_check_results if r.get("status") == "FAILED"]
    sys_warnings = [r for r in system_check_results if r.get("status") == "WARNING"]
    sys_passed = [r for r in system_check_results if r.get("status") == "PASSED"]
    n_sys_blockers = len(sys_blockers)
    n_sys_warnings = len(sys_warnings)
    n_sys_passed = len(sys_passed)

    # Overall pass/fail still uses all results (validation logic unchanged).
    n_blockers = len(blockers)
    n_warnings = len(warnings)
    n_passed = len(passed)

    if overall_override == "fail":
        overall = "FAIL"
        overall_class = "fail"
        overall_warning_badge = None
    elif n_blockers > 0:
        overall = "FAIL"
        overall_class = "fail"
        overall_warning_badge = None
    elif n_warnings > 0:
        overall = "PASS"
        overall_class = "pass"
        overall_warning_badge = f"{n_warnings} warning{'s' if n_warnings != 1 else ''}"
    else:
        overall = "PASS"
        overall_class = "pass"
        overall_warning_badge = None

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if overall_warning_badge:
        outcome_html = f'<span class="outcome {overall_class}">{_escape_html(overall)}</span><span class="outcome warn">{_escape_html(overall_warning_badge)}</span>'
    else:
        outcome_html = f'<span class="outcome {overall_class}">{_escape_html(overall)}</span>'

    # Load JAR tool outputs from same directory
    output_dir = os.path.dirname(validation_output_path)
    report = _load_report_json(output_dir)
    stats_df = _load_summary_csv(output_dir)
    pipeline_failure = _load_pipeline_failure(output_dir)
    pipeline_failure_banner_html = ""
    if pipeline_failure:
        stage = _escape_html(str(pipeline_failure.get("stage", "")))
        reason = _escape_html(str(pipeline_failure.get("reason", "")))
        pipeline_failure_banner_html = (
            f'<div class="pipeline-failure-banner" role="alert">'
            f'<strong>Pipeline failed at:</strong> <span class="stage">{stage}</span><br>'
            f'<strong>Reason:</strong> {reason}'
            f"</div>"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Validation Report — {_escape_html(dataset_name or "Import")}</title>
  <style>
    :root {{
      --bg: #f0f2f5;
      --surface: #ffffff;
      --text: #1a1d21;
      --text-muted: #5c6370;
      --border: #e4e6eb;
      --pass: #0f7b4d;
      --pass-bg: #e6f4ee;
      --fail: #c41e3a;
      --fail-bg: #fce8ec;
      --warn: #b8860b;
      --warn-bg: #fef9e7;
      --radius: 10px;
      --shadow: 0 1px 3px rgba(0,0,0,0.06);
      --shadow-md: 0 4px 12px rgba(0,0,0,0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      margin: 0; padding: 32px 24px;
      background: var(--bg); color: var(--text);
      font-size: 15px; line-height: 1.5;
    }}
    .container {{
      max-width: min(1200px, 95vw); margin: 0 auto;
      background: var(--surface); border-radius: var(--radius);
      box-shadow: var(--shadow-md); overflow: hidden;
    }}
    .hero {{
      padding: 28px 32px 24px;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(180deg, #fafbfc 0%, var(--surface) 100%);
    }}
    .hero h1 {{
      margin: 0 0 6px 0; font-size: 1.5rem; font-weight: 600; letter-spacing: -0.02em;
    }}
    .hero-meta {{
      display: flex; flex-wrap: wrap; align-items: center; gap: 12px 20px;
      color: var(--text-muted); font-size: 0.875rem; margin-top: 12px;
    }}
    .outcome {{
      display: inline-flex; align-items: center;
      padding: 6px 14px; border-radius: 20px; font-weight: 600; font-size: 0.9rem;
    }}
    .outcome.pass {{ background: var(--pass-bg); color: var(--pass); }}
    .outcome.fail {{ background: var(--fail-bg); color: var(--fail); }}
    .outcome.warn {{ background: var(--warn-bg); color: var(--warn); }}
    .summary-strip {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 16px; padding: 20px 32px;
      background: var(--surface); border-bottom: 1px solid var(--border);
    }}
    .summary-stat {{ display: flex; flex-direction: column; gap: 2px; }}
    .summary-label {{ font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-muted); }}
    .summary-val {{ font-size: 0.9375rem; font-weight: 500; color: var(--text); }}
    .main {{ padding: 24px 32px 32px; }}
    .report-section {{
      margin-bottom: 32px; padding: 20px 0;
      border-bottom: 1px solid var(--border);
    }}
    .report-section:last-of-type {{ border-bottom: none; }}
    .report-section h2 {{
      margin: 0 0 4px 0; font-size: 1.1rem; font-weight: 600; color: var(--text);
    }}
    .section-desc {{ margin: 0 0 16px 0; font-size: 0.8125rem; color: var(--text-muted); }}
    section {{ margin: 28px 0; }}
    section h2 {{ font-size: 1.1rem; font-weight: 600; margin: 0 0 12px 0; color: var(--text); }}
    .card {{
      border: 1px solid var(--border); border-radius: 8px; padding: 16px 18px;
      margin-bottom: 12px; background: var(--surface);
    }}
    .card.blocker {{ border-left: 4px solid var(--fail); background: var(--fail-bg); }}
    .card.passed {{ border-left: 4px solid var(--pass); background: var(--pass-bg); }}
    .card.warn {{ border-left: 4px solid var(--warn); background: var(--warn-bg); }}
    .card h3 {{ margin: 0 0 6px 0; font-size: 0.9375rem; font-weight: 600; }}
    .card .message {{ color: var(--text-muted); font-size: 0.875rem; margin: 8px 0 0; }}
    .details {{ width: 100%; font-size: 0.8125rem; border-collapse: collapse; margin-top: 10px; border-radius: 6px; overflow: hidden; border: 1px solid var(--border); }}
    .details th, .details td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--border); }}
    .details th {{ background: #f6f7f9; font-weight: 600; color: var(--text-muted); }}
    .details tr:last-child td {{ border-bottom: none; }}
    .details td:first-child {{ font-weight: 500; width: 140px; color: var(--text-muted); }}
    .statvar-table .details td:first-child, .lint-table td:first-child {{ font-weight: 500; }}
    .counter-breakdown-table .counter-group-header td {{ font-size: 0.75rem; color: var(--text-muted); padding-left: 24px; font-style: italic; }}
    .empty {{ color: var(--text-muted); font-style: italic; font-size: 0.875rem; }}
    .advisory-note {{ font-size: 0.8125rem; color: var(--text-muted); font-style: italic; margin-bottom: 12px; }}
    .hero-note {{ font-size: 0.8125rem; color: var(--text-muted); margin-top: 10px; }}
    .pipeline-failure-banner {{
      margin: 0; padding: 16px 32px;
      border-bottom: 1px solid var(--border);
      background: var(--fail-bg); border-left: 4px solid var(--fail);
      font-size: 0.9375rem;
    }}
    .pipeline-failure-banner strong {{ color: var(--fail); }}
    .pipeline-failure-banner .stage {{ font-weight: 600; color: var(--text); }}
    .contents {{ margin-bottom: 28px; }}
    .contents-title {{ font-size: 0.75rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted); margin-bottom: 10px; }}
    .nav-links {{ display: flex; flex-wrap: wrap; gap: 6px 12px; font-size: 0.8125rem; }}
    .nav-links a {{ color: var(--text-muted); text-decoration: none; padding: 4px 8px; border-radius: 4px; }}
    .nav-links a:hover {{ color: #0969da; background: var(--bg); }}
    .empty-positive {{ color: var(--pass); font-size: 0.875rem; font-weight: 500; }}
    .technical-signals-cell {{ background: #f8f9fa; padding: 12px 16px; vertical-align: top; }}
    .technical-signals-title {{ font-weight: 600; font-size: 0.8125rem; margin-bottom: 6px; color: var(--text); }}
    .technical-signals {{ font-size: 0.8125rem; color: var(--text-muted); line-height: 1.5; }}
    .fluctuation-explain-wrap {{ margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--border); }}
    .explain-fluctuation-btn {{
      display: inline-flex; align-items: center; gap: 6px;
      padding: 5px 12px; border-radius: 16px; font-size: 13px; font-weight: 500;
      border: 1px solid #0969da; background-color: #eaf3ff; color: #0969da;
      cursor: pointer; transition: all 0.2s ease;
    }}
    .explain-fluctuation-btn:hover {{ background-color: #0969da; color: white; box-shadow: 0 2px 6px rgba(9, 105, 218, 0.2); }}
    .explain-fluctuation-btn:disabled {{ opacity: 0.7; cursor: default; background: #f0f2f5; border-color: var(--border); color: var(--text-muted); box-shadow: none; }}
    .ai-interpretation-cell {{ margin-top: 10px; font-size: 0.8125rem; color: var(--text-muted); }}
    .ai-interpretation-loading {{ font-style: italic; }}
    .ai-interpretation-advisory {{ font-weight: 600; color: var(--text); margin: 0 0 4px 0; }}
    .ai-interpretation-note {{ font-size: 0.75rem; font-style: italic; margin: 0 0 8px 0; color: var(--text-muted); }}
    .ai-interpretation-text {{ margin: 0; line-height: 1.4; }}
    .ai-interpretation-unavailable {{ margin: 0; font-style: italic; color: var(--text-muted); }}
    .top-lint-summary {{ margin: 0 0 10px 0; font-size: 0.875rem; color: var(--text-muted); }}
    .top-lint-group-heading {{ font-size: 0.9375rem; font-weight: 600; margin: 20px 0 8px 0; color: var(--text); }}
    .top-lint-group-heading:first-child {{ margin-top: 0; }}
    .full-lint-details {{ margin-top: 12px; }}
    .full-lint-details summary {{ cursor: pointer; font-size: 0.875rem; color: #0969da; }}
    .full-lint-details summary:hover {{ text-decoration: underline; }}
    .full-lint-table-wrap {{ margin-top: 10px; }}
    a {{ color: #0969da; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <header class="hero">
      <h1>Import Validation Report</h1>
      <div class="hero-meta">
        {f"<span><strong>Dataset</strong> {_escape_html(dataset_name)}</span>" if dataset_name else ""}
        {outcome_html}
        <span>{timestamp}</span>
      </div>
      <p class="hero-note">Overall is based on validation rules only; Gemini Review is advisory and does not affect pass/fail.</p>
    </header>
    {pipeline_failure_banner_html}
"""
    # Compact summary block
    llm_issues = _load_llm_review(output_dir) or []
    ai_blockers = [i for i in llm_issues if _is_llm_blocker(i)]
    ai_warnings = [i for i in llm_issues if not _is_llm_blocker(i)]
    gemini_review_count = len(ai_blockers) + len(ai_warnings)
    fluctuation_samples = extract_fluctuation_samples(report) if report else []
    rule_failure_samples = _extract_rule_failure_samples(results)
    html += _render_compact_summary_block(
        overall, overall_class,
        n_val_blockers, n_val_warnings, n_val_passed,
        n_sys_blockers, n_sys_warnings, n_sys_passed,
        gemini_review_enabled, gemini_review_count, len(fluctuation_samples), len(rule_failure_samples),
    )
    html += """
    <div class="main">
    <div class="contents">
      <p class="contents-title">Contents</p>
      <nav class="nav-links" aria-label="Report sections">
        <a href="#blocking-failures">Blocking Validation Failures</a>
        <a href="#warnings">Warnings</a>
        <a href="#passed">Passed</a>
        <a href="#system-checks">System Checks</a>
        <a href="#ai-review">AI Advisory Findings</a>
        <a href="#fluctuation">Fluctuation</a>
        <a href="#rule-failures">Rule failures</a>
        <a href="#import-run">Import run</a>
        <a href="#key-counters">Key counters</a>
        <a href="#statvar">StatVar summary</a>
        <a href="#lint-summary">Lint</a>
        <a href="#counter-breakdown">Counter breakdown</a>
        <a href="#data-holes">Data holes</a>
        <a href="#top-lint">Top lint</a>
        <a href="#see-also">See also</a>
      </nav>
    </div>
"""
    # Gemini Review section (if schema_review.json exists)
    html += _render_llm_section(output_dir, gemini_review_enabled)

    # Fluctuation Analysis and Blocking Validation Failures
    fluctuation_html, explain_payloads = _render_fluctuation_section(report, gemini_review_enabled)
    html += fluctuation_html
    html += _render_rule_failure_section(results, output_dir)

    html += """
    <section class="report-section" id="blocking-failures">
      <h2>Blocking Validation Failures</h2>
      <p class="section-desc">Validation rules that failed (must be fixed to pass).</p>
"""

    if val_blockers:
        for r in val_blockers:
            html += f"""
      <div class="card blocker">
        <h3>{_escape_html(r.get("validation_name", "?"))}</h3>
        <div class="message">{_escape_html(r.get("message", ""))}</div>
        {_render_details(r.get("details", {}))}
      </div>
"""
    else:
        html += '      <p class="empty-positive">No blocking failures.</p>\n'

    html += """
    </section>

    <section class="report-section" id="warnings">
      <h2>Warnings</h2>
      <p class="section-desc">Non-blocking; import can still pass.</p>
"""

    if val_warnings:
        for r in val_warnings:
            html += f"""
      <div class="card warn">
        <h3>{_escape_html(r.get("validation_name", "?"))}</h3>
        <div class="message">{_escape_html(r.get("message", ""))}</div>
        {_render_details(r.get("details", {}))}
      </div>
"""
    else:
        html += '      <p class="empty">None</p>\n'

    html += """
    </section>

    <section class="report-section" id="passed">
      <h2>Passed</h2>
      <p class="section-desc">Validation rules that passed.</p>
"""

    if val_passed:
        for r in val_passed:
            html += f"""
      <div class="card passed">
        <h3>{_escape_html(r.get("validation_name", "?"))}</h3>
        <div class="message">{_escape_html(r.get("message", "") or "OK")}</div>
        {_render_details(r.get("details", {}))}
      </div>
"""
    else:
        html += '      <p class="empty">None</p>\n'

    html += "    </section>\n"

    # System Checks — separate from config-defined validation rules
    html += """
    <section class="report-section" id="system-checks">
      <h2>System Checks</h2>
      <p class="section-desc">Pre-import and system-level safeguards.</p>
"""
    if sys_blockers:
        for r in sys_blockers:
            html += f"""
      <div class="card blocker">
        <h3>{_escape_html(r.get("validation_name", "?"))}</h3>
        <div class="message">{_escape_html(r.get("message", ""))}</div>
        {_render_details(r.get("details", {}))}
      </div>
"""
    if sys_warnings:
        for r in sys_warnings:
            html += f"""
      <div class="card warn">
        <h3>{_escape_html(r.get("validation_name", "?"))}</h3>
        <div class="message">{_escape_html(r.get("message", ""))}</div>
        {_render_details(r.get("details", {}))}
      </div>
"""
    if sys_passed:
        for r in sys_passed:
            html += f"""
      <div class="card passed">
        <h3>{_escape_html(r.get("validation_name", "?"))}</h3>
        <div class="message">{_escape_html(r.get("message", "") or "OK")}</div>
        {_render_details(r.get("details", {}))}
      </div>
"""
    if not sys_blockers and not sys_warnings and not sys_passed:
        html += '      <p class="empty">None run for this import.</p>\n'
    html += "    </section>\n"

    # Import tool details (after validation results)
    html += _render_import_run_section(report)
    html += _render_key_counters_section(report, stats_df)
    html += _render_statvar_section(stats_df)
    html += _render_lint_summary_section(report)
    html += _render_counter_breakdown_section_wrapped(report)
    html += _render_data_holes_section(report)
    html += _render_top_lint_section(report)
    html += _render_jar_report_link_section(dataset_name, output_dir)

    if other:
        html += """
    <section class="report-section" id="other">
      <h2>Other</h2>
"""
        for r in other:
            html += f"""
      <div class="card warn">
        <h3>{_escape_html(r.get("validation_name", "?"))} <span class="badge badge-warn">{_escape_html(r.get("status", ""))}</span></h3>
        <div class="message">{_escape_html(r.get("message", ""))}</div>
        {_render_details(r.get("details", {}))}
      </div>
"""
        html += "    </section>\n"

    if explain_payloads:
        payloads_json = json.dumps(explain_payloads).replace("</", "<\\/")
        html += f"""
  <script>
  window.FLUCTUATION_EXPLAIN_PAYLOADS = {payloads_json};
  document.querySelectorAll('.explain-fluctuation-btn').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
      var self = this;
      var idx = parseInt(this.getAttribute('data-fluctuation-index'), 10);
      var cell = document.getElementById('ai-interpretation-' + idx);
      if (!cell || cell.dataset.loaded === '1') return;
      cell.innerHTML = '<span class="ai-interpretation-loading">Loading...</span>';
      cell.dataset.loaded = '1';
      fetch('/api/fluctuation-interpretation', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(window.FLUCTUATION_EXPLAIN_PAYLOADS[idx])
      }}).then(function(r) {{ return r.json(); }}).then(function(data) {{
        cell.innerHTML = '';
        if (data.error) {{
          cell.innerHTML = '<p class="ai-interpretation-unavailable">AI interpretation unavailable (API key not configured).</p>';
        }} else if (data.ai_interpretation) {{
          cell.innerHTML = '<p class="ai-interpretation-advisory">AI Interpretation (Advisory)</p><p class="ai-interpretation-note">This explanation is AI-generated and does not affect validation results.</p><p class="ai-interpretation-text"></p>';
          var p = cell.querySelector('.ai-interpretation-text');
          if (p) p.textContent = data.ai_interpretation;
        }} else {{
          cell.innerHTML = '<p class="ai-interpretation-unavailable">No interpretation returned.</p>';
        }}
        self.textContent = 'Explained ✓';
        self.disabled = true;
      }}).catch(function() {{
        cell.innerHTML = '<p class="ai-interpretation-unavailable">Request failed.</p>';
        self.textContent = 'Explained ✓';
        self.disabled = true;
      }});
    }});
  }});
  </script>
"""
    html += """
    </div>
  </div>
</body>
</html>
"""

    with open(html_output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return True


def main():
    parser = argparse.ArgumentParser(description="Generate HTML report from validation_output.json")
    parser.add_argument("input", help="Path to validation_output.json")
    parser.add_argument("output", help="Path for HTML output")
    parser.add_argument("--dataset", default="", help="Dataset name for report header")
    parser.add_argument(
        "--overall",
        choices=("pass", "fail"),
        default=None,
        help="Force Overall badge: fail when run failed (e.g. counters check); pass to use rules result",
    )
    parser.add_argument(
        "--ai-review-enabled",
        action="store_true",
        help="Show Explain button per fluctuation anomaly (only when Gemini Review was on for this run)",
    )
    args = parser.parse_args()

    if not generate_html(args.input, args.output, args.dataset, overall_override=args.overall, gemini_review_enabled=args.ai_review_enabled):
        sys.exit(1)


if __name__ == "__main__":
    main()
