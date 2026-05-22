"""Tests for pipeline/progress.py and pipeline/schemas.py.

Run with:
    python -m unittest tests.test_pipeline_progress
"""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.progress import (
    build_failure_event,
    build_progress_event,
    build_run_finished_event,
    emit_line,
    format_event,
    main,
)
from pipeline.registry import load_registry
from pipeline.schemas import EventValidationError, validate_event

PYTHON = sys.executable
PROGRESS_MODULE = "pipeline.progress"


class TestProgressEventBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_progress_step_labels_from_registry(self) -> None:
        event = build_progress_event("schema_review", registry=self.registry)
        self.assertEqual(event["v"], 1)
        self.assertEqual(event["t"], "progress")
        self.assertEqual(event["step_id"], "schema_review")
        self.assertEqual(event["step_index"], 1)
        self.assertEqual(event["step_label"], "Schema review")
        self.assertNotIn("substep_id", event)
        validate_event(event)

    def test_progress_substep_labels_from_registry(self) -> None:
        event = build_progress_event(
            "pre_import",
            substep_id="csv_split",
            registry=self.registry,
        )
        self.assertEqual(event["substep_id"], "csv_split")
        self.assertEqual(event["substep_label"], "Validate and split CSV")
        validate_event(event)

    def test_progress_unknown_step_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            build_progress_event("not_a_step", registry=self.registry)
        self.assertIn("unknown step_id", str(ctx.exception))

    def test_progress_unknown_substep_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            build_progress_event(
                "pre_import",
                substep_id="missing",
                registry=self.registry,
            )
        self.assertIn("unknown substep_id", str(ctx.exception))

    def test_format_event_single_line_json(self) -> None:
        event = build_progress_event("import_tool", registry=self.registry)
        line = format_event(event)
        self.assertNotIn("\n", line)
        parsed = json.loads(line)
        self.assertEqual(parsed, event)


class TestFailureEventBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_failure_uses_registry_not_legacy_labels(self) -> None:
        # Legacy ::STEP:: says "Gemini Review"; registry label is authoritative.
        event = build_failure_event(
            "schema_review",
            "GEMINI_BLOCKING",
            "Gemini review failed",
            registry=self.registry,
        )
        self.assertEqual(event["step_label"], "Schema review")
        self.assertEqual(event["failure_code"], "GEMINI_BLOCKING")
        validate_event(event)

    def test_failure_with_limit_and_details(self) -> None:
        event = build_failure_event(
            "pre_import",
            "ROW_COUNT_EXCEEDED",
            "Too many rows",
            limit=1_000_000,
            details={"rows": 2_000_000},
            substep_id="csv_quality",
            registry=self.registry,
        )
        self.assertEqual(event["substep_id"], "csv_quality")
        self.assertEqual(event["limit"], 1_000_000)
        self.assertEqual(event["details"], {"rows": 2_000_000})
        validate_event(event)

    def test_failure_empty_code_raises(self) -> None:
        with self.assertRaises(ValueError):
            build_failure_event(
                "pre_import",
                "  ",
                "msg",
                registry=self.registry,
            )


class TestRunFinishedEventBuilder(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_run_finished_succeeded_minimal(self) -> None:
        event = build_run_finished_event("succeeded", 0)
        self.assertEqual(event["status"], "succeeded")
        self.assertEqual(event["exit_code"], 0)
        validate_event(event)

    def test_run_finished_failed_with_step_context(self) -> None:
        event = build_run_finished_event(
            "failed",
            1,
            failure_code="DATA_PROCESSING_FAILED",
            message="genmcf failed",
            step_id="import_tool",
            registry=self.registry,
        )
        self.assertEqual(event["step_id"], "import_tool")
        self.assertEqual(event["step_label"], "DC import (genmcf)")
        validate_event(event)

    def test_run_finished_invalid_status_raises(self) -> None:
        with self.assertRaises(ValueError):
            build_run_finished_event("running", 0)


class TestSchemaValidation(unittest.TestCase):
    def test_rejects_wrong_version(self) -> None:
        with self.assertRaises(EventValidationError):
            validate_event({"v": 2, "t": "progress", "step_id": "x", "step_index": 0, "step_label": "X"})

    def test_rejects_unknown_progress_keys(self) -> None:
        event = build_progress_event("results", registry=load_registry())
        event["extra"] = True
        with self.assertRaises(EventValidationError):
            validate_event(event)


class TestProgressCli(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [PYTHON, "-m", PROGRESS_MODULE, *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_cli_progress_stdout(self) -> None:
        proc = self._run("progress", "--step-id", "baseline_diff")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        event = json.loads(proc.stdout.strip())
        self.assertEqual(event["step_id"], "baseline_diff")
        self.assertEqual(event["step_index"], 3)
        validate_event(event)

    def test_cli_failure_unknown_step_exit_2(self) -> None:
        proc = self._run(
            "failure",
            "--step-id",
            "bogus",
            "--failure-code",
            "X",
            "--message",
            "fail",
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("unknown step_id", proc.stderr)

    def test_cli_run_finished_cancelled(self) -> None:
        proc = self._run("run_finished", "--status", "cancelled", "--exit-code", "-1")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        event = json.loads(proc.stdout.strip())
        self.assertEqual(event["status"], "cancelled")
        self.assertEqual(event["exit_code"], -1)


class TestEmitLine(unittest.TestCase):
    def test_emit_line_writes_newline(self) -> None:
        buf = StringIO()
        event = build_progress_event("validation", registry=load_registry())
        line = emit_line(event, file=buf)
        self.assertTrue(buf.getvalue().endswith("\n"))
        self.assertEqual(line, format_event(event))


class TestMainEntrypoint(unittest.TestCase):
    def test_main_no_args_returns_2(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            main([])
        self.assertEqual(ctx.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
