"""Static and smoke tests for legacy ::STEP:: + v1 progress emission in run_e2e_test.sh."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUN_E2E = ROOT / "run_e2e_test.sh"
PYTHON = ROOT / ".venv/bin/python" if (ROOT / ".venv/bin/python").is_file() else sys.executable


class TestRunE2eProgressEmitWiring(unittest.TestCase):
    def test_script_defines_progress_emit_helpers(self) -> None:
        text = RUN_E2E.read_text(encoding="utf-8")
        self.assertIn("emit_step_marker", text)
        self.assertIn("emit_v1_failure", text)
        self.assertIn("emit_run_finished", text)
        self.assertIn('CSV_SPLIT_FAILED) echo "pre_import csv_split"', text)
        self.assertIn('emit_step_marker "::STEP::2.4:Differ" baseline_diff', text)
        self.assertNotIn('echo "::STEP::0:Pre-Import Checks"', text)

    def test_emit_order_legacy_then_v1(self) -> None:
        """emit_step_marker echoes legacy ::STEP:: before v1 progress JSON."""
        text = RUN_E2E.read_text(encoding="utf-8")
        start = text.index("emit_step_marker() {")
        block = text[start : start + 200]
        self.assertLess(block.index("echo"), block.index("emit_v1_progress"))


class TestCsvSplitDualFailureAttribution(unittest.TestCase):
    def test_v1_failure_overrides_legacy_step_2_semantics(self) -> None:
        """Legacy failure step=2 preserved; v1 failure uses pre_import/csv_split."""
        from pipeline.progress import build_failure_event, format_event
        from pipeline.projector import ProgressProjector
        from pipeline.registry import load_registry

        registry = load_registry()
        p = ProgressProjector(registry=registry)
        p.feed_line(
            json.dumps(
                {
                    "t": "failure",
                    "code": "CSV_SPLIT_FAILED",
                    "step": 2,
                    "message": "CSV split produced no shards",
                }
            )
        )
        p.feed_line(
            format_event(
                build_failure_event(
                    "pre_import",
                    "CSV_SPLIT_FAILED",
                    "CSV split produced no shards",
                    substep_id="csv_split",
                    registry=registry,
                )
            )
        )
        status = p.to_status_dict()
        self.assertEqual(status["failure_step_id"], "pre_import")
        self.assertEqual(status["failure_step_index"], 0)
        self.assertEqual(status["failure_code"], "CSV_SPLIT_FAILED")


class TestProgressEmitSmoke(unittest.TestCase):
    def test_progress_cli_emits_v1_line(self) -> None:
        env = {**dict(__import__("os").environ), "PYTHONPATH": str(ROOT)}
        proc = subprocess.run(
            [str(PYTHON), "-m", "pipeline.progress", "progress", "--step-id", "baseline_diff"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        event = json.loads(proc.stdout.strip())
        self.assertEqual(event["v"], 1)
        self.assertEqual(event["t"], "progress")
        self.assertEqual(event["step_id"], "baseline_diff")
        self.assertEqual(event["step_index"], 3)


if __name__ == "__main__":
    unittest.main()
