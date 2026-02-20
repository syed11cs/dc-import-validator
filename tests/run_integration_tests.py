#!/usr/bin/env python3
"""Tiny integration test harness for DC Import Validator.

Runs run_e2e_test.sh for key datasets and asserts exit codes and output.
Run from project root: python tests/run_integration_tests.py

Datasets in project (run_e2e_test.sh / server):
  - child_birth              : clean, expect PASS
  - child_birth_fail_min_value   : negative value → check_min_value FAIL
  - child_birth_fail_units       : mixed units → check_unit_consistency FAIL
  - child_birth_fail_scaling_factor : inconsistent scaling → check_scaling_factor_consistency FAIL
  - child_birth_ai_demo       : TMCF schema/typos → Gemini Review (or deterministic) finds issues
  - custom                    : requires --tmcf/--csv (not covered here)

Scenarios covered: PASS, FAIL (min_value, units, scaling_factor), AI demo (no LLM), no API key, step labels.
Requirements:
  - datacommonsorg/data repo at ../datacommonsorg/data (or PROJECTS_DIR) for the import_validation runner only; child_birth testdata is in this repo (sample_data/child_birth/).
  - ./run_e2e_test.sh and ./setup.sh already run once (venv, JAR, etc.)
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path


def project_root() -> Path:
    root = Path(__file__).resolve().parent.parent
    assert (root / "run_e2e_test.sh").exists(), f"run_e2e_test.sh not found under {root}"
    return root


def run_e2e(dataset: str, *extra_args: str, env: dict | None = None) -> tuple[int, str]:
    """Run run_e2e_test.sh with dataset and optional args. Returns (returncode, combined stdout+stderr)."""
    root = project_root()
    script = root / "run_e2e_test.sh"
    cmd = ["bash", str(script), dataset, *extra_args]
    env = env if env is not None else os.environ.copy()
    result = subprocess.run(
        cmd,
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    out = (result.stdout or "") + (result.stderr or "")
    return result.returncode, out


def test_child_birth_pass() -> None:
    """Dataset child_birth with --no-llm-review: expect PASS, exit 0, step markers."""
    code, out = run_e2e("child_birth", "--no-llm-review")
    assert code == 0, f"child_birth (no LLM) expected exit 0, got {code}\n{out[-2000:]}"
    assert "Validation PASSED" in out or "✓ Validation PASSED" in out, f"Expected PASSED in output\n{out[-1500:]}"
    assert "::STEP::" in out or "Step 1" in out, "Expected step markers or Step 1 in output"


def test_child_birth_fail_min_value() -> None:
    """Dataset child_birth_fail_min_value: expect FAIL, exit 1 (check_min_value)."""
    code, out = run_e2e("child_birth_fail_min_value", "--no-llm-review")
    assert code == 1, f"child_birth_fail_min_value expected exit 1, got {code}\n{out[-2000:]}"
    assert "Validation FAILED" in out or "✗ Validation FAILED" in out, f"Expected FAILED in output\n{out[-1500:]}"


def test_child_birth_fail_units() -> None:
    """Dataset child_birth_fail_units: expect FAIL, exit 1 (check_unit_consistency)."""
    code, out = run_e2e("child_birth_fail_units", "--no-llm-review")
    assert code == 1, f"child_birth_fail_units expected exit 1, got {code}\n{out[-2000:]}"
    assert "Validation FAILED" in out or "✗ Validation FAILED" in out, f"Expected FAILED in output\n{out[-1500:]}"


def test_child_birth_fail_scaling_factor() -> None:
    """Dataset child_birth_fail_scaling_factor: expect FAIL, exit 1 (check_scaling_factor_consistency)."""
    code, out = run_e2e("child_birth_fail_scaling_factor", "--no-llm-review")
    assert code == 1, f"child_birth_fail_scaling_factor expected exit 1, got {code}\n{out[-2000:]}"
    assert "Validation FAILED" in out or "✗ Validation FAILED" in out, f"Expected FAILED in output\n{out[-1500:]}"


def test_child_birth_ai_demo_no_llm() -> None:
    """Dataset child_birth_ai_demo with --no-llm-review: runs deterministic only; check steps and exit."""
    code, out = run_e2e("child_birth_ai_demo", "--no-llm-review")
    # Deterministic checks may pass or find issues; pipeline should complete (0 or 1)
    assert code in (0, 1), f"child_birth_ai_demo (no LLM) expected exit 0 or 1, got {code}\n{out[-2000:]}"
    assert "Step 1" in out or "::STEP::1" in out, "Expected Step 1 or ::STEP::1 in output"


def test_no_api_key_skips_llm() -> None:
    """With GEMINI_API_KEY unset and --llm-review: LLM is skipped; exit 0 or 1 from deterministic only."""
    env = os.environ.copy()
    env.pop("GEMINI_API_KEY", None)
    env.pop("GOOGLE_API_KEY", None)
    code, out = run_e2e("child_birth", "--llm-review", env=env)
    assert "LLM review skipped" in out or "no API key" in out, f"Expected LLM skip message in output\n{out[-1500:]}"
    # child_birth is clean so deterministic passes → exit 0
    assert code == 0, f"child_birth with no API key expected exit 0 (deterministic pass), got {code}\n{out[-1500:]}"


def test_step_protocol_labels() -> None:
    """Output should contain formal step labels (::STEP::N:Label) matching the pipeline."""
    code, out = run_e2e("child_birth", "--no-llm-review")
    assert code == 0, f"child_birth expected exit 0, got {code}"
    assert "::STEP::0:Pre-Import Checks" in out, "Expected ::STEP::0:Pre-Import Checks"
    assert "::STEP::1:Gemini Review" in out, "Expected ::STEP::1:Gemini Review"
    assert "::STEP::2:DC Import Tool" in out, "Expected ::STEP::2:DC Import Tool"
    assert "::STEP::3:DC Import Validation" in out, "Expected ::STEP::3:DC Import Validation"
    assert "::STEP::4:Results" in out, "Expected ::STEP::4:Results"


def test_deterministic_mode_no_existence_counters() -> None:
    """With LOCAL + existence-checks=false, run passes and report has no existence-related lint counters."""
    env = os.environ.copy()
    env["IMPORT_RESOLUTION_MODE"] = "LOCAL"
    env["IMPORT_EXISTENCE_CHECKS"] = "false"
    code, out = run_e2e("child_birth", "--no-llm-review", env=env)
    assert code == 0, f"deterministic mode expected exit 0, got {code}\n{out[-2000:]}"
    assert "Validation PASSED" in out or "✓ Validation PASSED" in out

    root = project_root()
    report_path = root / "output" / "child_birth_genmcf" / "report.json"
    assert report_path.exists(), f"report.json not found at {report_path}"

    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)
    cmd = report.get("commandArgs", {})
    assert cmd.get("existenceChecks") is False, "expected existenceChecks=false in report commandArgs"

    level_summary = report.get("levelSummary", {})
    existence_related = []
    for level, data in level_summary.items():
        for key in (data.get("counters") or {}).keys():
            if "existence" in key.lower():
                existence_related.append(f"{level}.{key}")
    assert not existence_related, (
        f"expected no existence-related lint counters when existence-checks=false, found: {existence_related}"
    )


def test_full_mode_smoke() -> None:
    """FULL resolution mode: pipeline runs to completion (smoke test; may require network)."""
    env = os.environ.copy()
    env["IMPORT_RESOLUTION_MODE"] = "FULL"
    code, out = run_e2e("child_birth", "--no-llm-review", env=env)
    assert code in (0, 1), f"FULL mode expected exit 0 or 1, got {code}\n{out[-2000:]}"
    assert "::STEP::2:DC Import Tool" in out, "Expected Step 2 in output"
    assert "::STEP::3:DC Import Validation" in out or "::STEP::4:Results" in out, (
        "Expected Step 3 or 4 (pipeline progressed)"
    )


def main() -> int:
    root = project_root()
    os.chdir(root)
    tests = [
        ("child_birth PASS", test_child_birth_pass),
        ("child_birth_fail_min_value FAIL", test_child_birth_fail_min_value),
        ("child_birth_fail_units FAIL", test_child_birth_fail_units),
        ("child_birth_fail_scaling_factor FAIL", test_child_birth_fail_scaling_factor),
        ("child_birth_ai_demo (no LLM)", test_child_birth_ai_demo_no_llm),
        ("no API key skips LLM", test_no_api_key_skips_llm),
        ("step protocol labels", test_step_protocol_labels),
        ("deterministic mode (LOCAL, no existence counters)", test_deterministic_mode_no_existence_counters),
        ("FULL mode smoke", test_full_mode_smoke),
    ]
    failed = []
    for name, fn in tests:
        print(f"  Running: {name}...", end=" ", flush=True)
        start = time.perf_counter()
        try:
            fn()
            elapsed = time.perf_counter() - start
            print(f"OK ({elapsed:.1f}s)")
        except AssertionError as e:
            elapsed = time.perf_counter() - start
            print(f"FAIL ({elapsed:.1f}s)")
            print(f"       {e}")
            failed.append((name, e))
    if failed:
        print(f"\n{len(failed)} test(s) failed")
        return 1
    print(f"\nAll {len(tests)} integration tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
