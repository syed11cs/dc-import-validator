"""Combined review summary (validation + Gemini review + fluctuation + rule failures) and markdown export."""

import json
import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from scripts.fluctuation_utils import extract_fluctuation_samples as _extract_fluctuation_samples

from ui.services.rule_samples import enrich_rule_failure_samples, extract_rule_failure_samples


def build_review_summary_from_data(
    dataset: str,
    validation_results: list,
    ai_review_issues: list,
    report: dict | None,
) -> dict:
    """Build review summary from in-memory data (e.g. from GCS). No CSV enrichment for rule failures."""
    results = validation_results if isinstance(validation_results, list) else []
    blockers = [r for r in results if r.get("status") == "FAILED"]
    warnings = [r for r in results if r.get("status") == "WARNING"]
    passed = [r for r in results if r.get("status") == "PASSED"]
    n_blockers = len(blockers)
    n_warnings = len(warnings)
    n_passed = len(passed)
    if n_blockers > 0:
        overall = "FAIL"
    elif n_warnings > 0:
        overall = f"PASS (with {n_warnings} warning{'s' if n_warnings != 1 else ''})"
    else:
        overall = "PASS"
    llm_issues = ai_review_issues if isinstance(ai_review_issues, list) else []
    fluctuation_samples = _extract_fluctuation_samples(report) if report else []
    rule_failure_samples = extract_rule_failure_samples(results)
    return {
        "dataset": dataset,
        "overall": overall,
        "summary": {
            "blockers": n_blockers,
            "warnings": n_warnings,
            "passed": n_passed,
            "ai_review_issues": len(llm_issues),
            "fluctuation_anomalies": len(fluctuation_samples),
            "rule_failures": len(rule_failure_samples),
        },
        "validation_result": results,
        "ai_review": llm_issues,
        "fluctuation_samples": fluctuation_samples,
        "rule_failure_samples": rule_failure_samples,
    }


def build_review_summary(dataset: str, output_dir: Path) -> dict | None:
    """Build combined review summary from validation_output, llm_review, report.json. Returns None if no validation result."""
    path = output_dir / "validation_output.json"
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            results = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(results, list):
        results = []

    blockers = [r for r in results if r.get("status") == "FAILED"]
    warnings = [r for r in results if r.get("status") == "WARNING"]
    passed = [r for r in results if r.get("status") == "PASSED"]
    n_blockers = len(blockers)
    n_warnings = len(warnings)
    n_passed = len(passed)

    if n_blockers > 0:
        overall = "FAIL"
    elif n_warnings > 0:
        overall = f"PASS (with {n_warnings} warning{'s' if n_warnings != 1 else ''})"
    else:
        overall = "PASS"

    llm_issues = []
    llm_path = output_dir / "schema_review.json"
    if llm_path.exists():
        try:
            with open(llm_path, encoding="utf-8") as f:
                data = json.load(f)
            llm_issues = data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError):
            pass

    report = None
    report_path = output_dir / "report.json"
    if report_path.exists():
        try:
            with open(report_path, encoding="utf-8") as f:
                report = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

    fluctuation_samples = _extract_fluctuation_samples(report) if report else []
    rule_failure_samples = extract_rule_failure_samples(results)
    enrich_rule_failure_samples(rule_failure_samples, output_dir, results)

    return {
        "dataset": dataset,
        "overall": overall,
        "summary": {
            "blockers": n_blockers,
            "warnings": n_warnings,
            "passed": n_passed,
            "ai_review_issues": len(llm_issues),
            "fluctuation_anomalies": len(fluctuation_samples),
            "rule_failures": len(rule_failure_samples),
        },
        "validation_result": results,
        "ai_review": llm_issues,
        "fluctuation_samples": fluctuation_samples,
        "rule_failure_samples": rule_failure_samples,
    }


def review_summary_to_markdown(data: dict) -> str:
    """Convert review summary dict to markdown."""
    lines = [
        "# Review Summary",
        "",
        f"**Dataset:** {data.get('dataset', '')}",
        f"**Overall:** {data.get('overall', '')}",
        "",
        "## Summary",
        "",
    ]
    s = data.get("summary") or {}
    for k, v in s.items():
        lines.append(f"- **{k}:** {v}")
    lines.extend(["", "## Validation Result", ""])
    for r in data.get("validation_result") or []:
        status = r.get("status", "?")
        name = r.get("validation_name", "?")
        msg = (r.get("message") or "")[:80]
        lines.append(f"- [{status}] {name}: {msg}")
    lines.extend(["", "## Gemini Review", ""])
    for i in data.get("ai_review") or []:
        sev = i.get("severity", "?")
        typ = i.get("type", "?")
        msg = (i.get("message") or "")[:80]
        lines.append(f"- [{sev}] {typ}: {msg}")
    lines.extend(["", "## Fluctuation Analysis", ""])
    for f in data.get("fluctuation_samples") or []:
        lines.append(f"- {f.get('statVar', '—')} @ {f.get('location', '—')}: {f.get('percentDifference')}%")
    if not data.get("fluctuation_samples"):
        lines.append("- No significant fluctuations.")
    lines.extend(["", "## Blocking Rule Failures", ""])
    for r in data.get("rule_failure_samples") or []:
        lines.append(f"- {r.get('rule', '—')}: {r.get('message', '')[:60]}")
    if not data.get("rule_failure_samples"):
        lines.append("- No rule failures.")
    return "\n".join(lines)
