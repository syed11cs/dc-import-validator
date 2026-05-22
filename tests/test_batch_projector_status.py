"""Tests for batch/projector_status.py (ProgressProjector GCS status writer)."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from batch.projector_status import ProjectorBatchBridge
from pipeline.progress import build_progress_event, format_event
from pipeline.registry import load_registry


class TestProjectorBatchBridge(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.state_file = Path(self._tmp.name) / "bridge.json"
        self.app_root = ROOT
        os.environ["RUN_ID"] = "test-run"
        os.environ["DATASET"] = "child_birth"
        os.environ["GCS_REPORTS_BUCKET"] = "test-bucket"
        os.environ["BATCH_JOB_NAME"] = "job/1"
        os.environ["VM_TYPE"] = "n2-highmem-16"
        os.environ["STARTED_AT"] = "2026-05-22T12:00:00Z"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _bridge(self) -> ProjectorBatchBridge:
        bridge = ProjectorBatchBridge(self.state_file, self.app_root)
        return bridge

    @patch("batch.projector_status._upload_status")
    def test_mark_starting_writes_starting_status(self, mock_upload: unittest.mock.MagicMock) -> None:
        bridge = self._bridge()
        bridge.mark_starting("Preparing validation environment")
        data = mock_upload.call_args[0][0]
        self.assertEqual(data["status"], "starting")
        self.assertEqual(data["step"], "0")
        self.assertEqual(data["step_label"], "Preparing validation environment")

    @patch("batch.projector_status._upload_status")
    def test_feed_legacy_marker_uses_registry_label(self, mock_upload: unittest.mock.MagicMock) -> None:
        bridge = self._bridge()
        bridge.feed_line("::STEP::1:Gemini Review")
        data = mock_upload.call_args[0][0]
        self.assertEqual(data["status"], "running")
        self.assertEqual(data["step_id"], "schema_review")
        self.assertEqual(data["step_label"], "Schema review")
        self.assertEqual(data["step"], "1")
        self.assertEqual(data["step_index"], 1)

    @patch("batch.projector_status._upload_status")
    def test_feed_baseline_diff_step_index_three(self, mock_upload: unittest.mock.MagicMock) -> None:
        bridge = self._bridge()
        bridge.feed_line("::STEP::2.4:Differ")
        data = mock_upload.call_args[0][0]
        self.assertEqual(data["step_id"], "baseline_diff")
        self.assertEqual(data["step_index"], 3)
        self.assertEqual(data["step"], "2.4")

    @patch("batch.projector_status._upload_status")
    def test_v1_progress_line(self, mock_upload: unittest.mock.MagicMock) -> None:
        registry = load_registry()
        bridge = self._bridge()
        bridge.feed_line(
            format_event(build_progress_event("validation", registry=registry))
        )
        data = mock_upload.call_args[0][0]
        self.assertEqual(data["step_id"], "validation")
        self.assertEqual(data["step"], "3")

    @patch("batch.projector_status._upload_status")
    def test_failure_snapshot_csv_split(self, mock_upload: unittest.mock.MagicMock) -> None:
        from pipeline.progress import build_failure_event

        registry = load_registry()
        bridge = self._bridge()
        bridge.feed_line(
            format_event(
                build_failure_event(
                    "pre_import",
                    "CSV_SPLIT_FAILED",
                    "no shards",
                    substep_id="csv_split",
                    registry=registry,
                )
            )
        )
        snap = bridge.failure_snapshot()
        self.assertEqual(snap["code"], "CSV_SPLIT_FAILED")
        self.assertEqual(snap["step"], 0)
        self.assertEqual(snap["step_id"], "pre_import")

    @patch("batch.projector_status._upload_status")
    def test_write_explicit_terminal_results(self, mock_upload: unittest.mock.MagicMock) -> None:
        bridge = self._bridge()
        bridge.write_explicit(
            legacy_step="4",
            step_label="Results",
            status="succeeded",
            artifacts_ready=False,
        )
        data = mock_upload.call_args[0][0]
        self.assertEqual(data["status"], "succeeded")
        self.assertEqual(data["step"], "4")
        self.assertEqual(data["step_id"], "results")
        self.assertEqual(data["artifacts_ready"], False)


class TestEntrypointWiring(unittest.TestCase):
    def test_entrypoint_uses_projector_feed_line(self) -> None:
        text = (ROOT / "batch" / "entrypoint.sh").read_text(encoding="utf-8")
        self.assertIn("feed-line", text)
        self.assertIn("projector_status.py", text)
        self.assertNotRegex(text, r'::STEP::\(\[0-9')


if __name__ == "__main__":
    unittest.main()
