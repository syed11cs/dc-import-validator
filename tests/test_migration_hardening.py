"""Tests for orchestration status normalization, projector monotonicity, and path resolution."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from batch.projector_status import ProjectorBatchBridge
from pipeline.projector import ProgressProjector
from pipeline.registry import load_registry
from pipeline.status_v1 import apply_step, step_by_id
from ui.orchestration.executors.subprocess import SubprocessExecutor, default_app_root
from ui.orchestration.policy import (
    PolicyBlockedError,
    batch_configured,
    resolve_executor,
)
from ui.orchestration.runs import legacy_step_token_to_index, normalize_run_status
from ui.orchestration.spec import run_spec_from_mapping


class TestNormalizeRunStatus(unittest.TestCase):
    def test_step_2_maps_to_import_tool(self) -> None:
        out = normalize_run_status({"step": "2", "status": "running"})
        self.assertEqual(out["step_index"], 2)

    def test_step_2_4_maps_to_baseline_diff(self) -> None:
        out = normalize_run_status({"step": "2.4", "status": "running"})
        self.assertEqual(out["step_index"], 3)
        self.assertEqual(out.get("step"), "2.4")

    def test_step_id_authoritative(self) -> None:
        out = normalize_run_status(
            {"step": "2", "step_id": "baseline_diff", "status": "running"}
        )
        self.assertEqual(out["step_index"], 3)

    def test_preserves_existing_step_index(self) -> None:
        out = normalize_run_status({"step": "2.4", "step_index": 3, "status": "running"})
        self.assertEqual(out["step_index"], 3)


class TestLegacyStepTokenToIndex(unittest.TestCase):
    def test_tokens(self) -> None:
        self.assertEqual(legacy_step_token_to_index("2.4"), 3)
        self.assertEqual(legacy_step_token_to_index("3"), 4)


class TestApplyStepMonotonic(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_forward_allowed(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line("::STEP::2:DC Import Tool")
        p.feed_line("::STEP::2.4:Differ")
        self.assertEqual(p.state.step_index, 3)

    def test_same_step_refresh_allowed(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line("::STEP::2:DC Import Tool")
        import_tool = step_by_id(self.registry, "import_tool")
        assert import_tool is not None
        apply_step(p.state, import_tool, substep_id="genmcf")
        self.assertEqual(p.state.step_index, 2)
        self.assertEqual(p.state.substep_id, "genmcf")

    def test_backward_ignored(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line("::STEP::2.4:Differ")
        p.feed_line("::STEP::2:DC Import Tool")
        self.assertEqual(p.state.step_index, 3)
        self.assertEqual(p.state.step_id, "baseline_diff")


class TestSubprocessExecutorAppRoot(unittest.TestCase):
    def test_default_app_root_is_repo_root(self) -> None:
        root = default_app_root()
        self.assertEqual(root, ROOT)
        self.assertTrue((root / "run_e2e_test.sh").is_file())

    def test_executor_e2e_script_path(self) -> None:
        ex = SubprocessExecutor()
        self.assertEqual(ex.app_root, ROOT)
        self.assertEqual(ex.e2e_script_path(), ROOT / "run_e2e_test.sh")


class TestPolicyEdgeCases(unittest.TestCase):
    def test_partial_batch_env_not_configured(self) -> None:
        env = {
            "GCS_REPORTS_BUCKET": "b",
            "BATCH_PROJECT_ID": "p",
            # missing BATCH_REGION, BATCH_SERVICE_ACCOUNT
        }
        self.assertFalse(batch_configured(env))

    def test_dev_custom_requires_allow_flag(self) -> None:
        spec = run_spec_from_mapping(
            {
                "run_id": "r1",
                "mode": "custom",
                "dataset_id": "custom",
                "inputs": {"session_id": "s", "tmcf_filename": "a.tmcf", "csv_filenames": ["b.csv"]},
                "rules": {},
                "options": {},
            }
        )
        with self.assertRaises(PolicyBlockedError):
            resolve_executor(spec, env={"DEPLOYMENT_PROFILE": "development"})
        res = resolve_executor(
            spec,
            env={"DEPLOYMENT_PROFILE": "development", "ALLOW_LOCAL_CUSTOM": "1"},
        )
        self.assertEqual(res.executor, "subprocess")


class TestBatchBridgeGcsPayload(unittest.TestCase):
    @patch("batch.projector_status._upload_status")
    def test_terminal_failed_uses_run_finished(self, mock_upload: unittest.mock.MagicMock) -> None:
        import os
        import tempfile

        tmp = tempfile.mkdtemp()
        state_file = Path(tmp) / "bridge.json"
        os.environ["RUN_ID"] = "r1"
        os.environ["DATASET"] = "child_birth"
        os.environ["GCS_REPORTS_BUCKET"] = "b"
        bridge = ProjectorBatchBridge(state_file, ROOT)
        bridge.write_explicit(
            legacy_step="2.4",
            step_label="Baseline comparison",
            status="failed",
            failure_code="DIFFER_FAILED",
            failure_message="diff error",
        )
        data = mock_upload.call_args[0][0]
        self.assertEqual(data["status"], "failed")
        self.assertEqual(data["failure_code"], "DIFFER_FAILED")
        self.assertEqual(data["step_id"], "baseline_diff")


if __name__ == "__main__":
    unittest.main()
