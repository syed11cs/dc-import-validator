"""Combined review summary (validation + Gemini review + fluctuation + rule failures) and markdown export."""

import json
import sys
from pathlib import Path

try:
    import pandas as _pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

_APP_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from scripts.fluctuation_utils import extract_fluctuation_samples as _extract_fluctuation_samples

from ui.services.rule_samples import enrich_rule_failure_samples, extract_rule_failure_samples


def _load_differ_stats(output_dir: Path, baseline_id: str | None = None) -> dict | None:
    """Load differ statistics from differ_output/ subdirectory. Returns None if no baseline was used."""
    differ_dir = output_dir / "differ_output"
    summary_path = differ_dir / "differ_summary.json"
    csv_path = differ_dir / "obs_diff_summary.csv"
    if not summary_path.exists() and not csv_path.exists():
        return None
    stats: dict = {}

    if summary_path.exists():
        try:
            with open(summary_path, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                for key in ("previous_obs_size", "current_obs_size", "obs_diff_size"):
                    if key in raw:
                        stats[key] = raw[key]
                # Use the recorded dataset_id (e.g. "custom_abc123") to locate the correct
                # baseline manifest — overrides the generic baseline_id param which may be
                # "custom" for custom datasets (wrong directory).
                if raw.get("dataset_id"):
                    baseline_id = raw["dataset_id"]
        except (json.JSONDecodeError, OSError):
            pass

    if csv_path.exists() and _HAS_PANDAS:
        try:
            df = _pd.read_csv(csv_path)
            stats["deleted"] = int(df["DELETED"].sum()) if "DELETED" in df.columns else 0
            stats["modified"] = int(df["MODIFIED"].sum()) if "MODIFIED" in df.columns else 0
            stats["added"] = int(df["ADDED"].sum()) if "ADDED" in df.columns else 0

            # Total unique StatVars present in the differ comparison.
            # summary_report.csv (loaded below) overrides this with the accurate full count
            # when available (local dev). On Cloud Run this is the best available value.
            if "StatVar" in df.columns:
                stats["total_stat_var_count"] = int(df["StatVar"].nunique())

            # Build per-StatVar change list (rows with any change, sorted by total desc, capped at 10)
            if "StatVar" in df.columns:
                change_cols = [c for c in ("DELETED", "MODIFIED", "ADDED") if c in df.columns]
                if change_cols:
                    df["_total"] = df[change_cols].fillna(0).sum(axis=1).astype(int)
                    changed = df[df["_total"] > 0].sort_values("_total", ascending=False).head(10)

                    def _int(val) -> int:
                        try:
                            return int(val) if val == val else 0  # NaN check
                        except (TypeError, ValueError):
                            return 0

                    stats["changed_statvars"] = [
                        {
                            "name": str(row["StatVar"]).removeprefix("dcid:"),
                            "deleted": _int(row.get("DELETED", 0)),
                            "modified": _int(row.get("MODIFIED", 0)),
                            "added": _int(row.get("ADDED", 0)),
                        }
                        for _, row in changed.iterrows()
                    ]
                else:
                    stats["changed_statvars"] = []
        except Exception:
            pass
    elif csv_path.exists():
        # Minimal fallback without pandas: signal that baseline exists
        stats.setdefault("deleted", None)

    # Total StatVar count from summary_report.csv (represents all StatVars, not just changed ones)
    summary_csv = output_dir / "summary_report.csv"
    if summary_csv.exists() and _HAS_PANDAS:
        try:
            sv_df = _pd.read_csv(summary_csv)
            stats["total_stat_var_count"] = len(sv_df)
        except Exception:
            pass

    # Modified percentage relative to baseline size
    prev = stats.get("previous_obs_size")
    mod = stats.get("modified")
    if prev and prev > 0 and mod is not None:
        stats["modified_pct"] = round(mod / prev * 100, 1)

    # Baseline provenance: read updated_at from manifest.json (standard datasets only)
    if baseline_id:
        manifest_path = _APP_ROOT / "output" / "baselines" / baseline_id / "latest" / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path, encoding="utf-8") as f:
                    manifest = json.load(f)
                if "updated_at" in manifest:
                    stats["baseline_updated_at"] = manifest["updated_at"]
                if "version" in manifest:
                    stats["baseline_version"] = manifest["version"]
                if "accepted_by" in manifest:
                    stats["baseline_accepted_by"] = manifest["accepted_by"]
                if "run_id" in manifest:
                    stats["baseline_run_id"] = manifest["run_id"]
            except (json.JSONDecodeError, OSError):
                pass

    return stats if stats else None


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
        "differ_stats": None,
        "current_baseline_run_id": None,
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
    differ_stats = _load_differ_stats(output_dir, baseline_id=dataset)

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
        "differ_stats": differ_stats,
        "current_baseline_run_id": differ_stats.get("baseline_run_id") if differ_stats else None,
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
