"""Tests for ui/orchestration (policy, spec, paths, executors)."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.orchestration import (
    BUILTIN,
    CUSTOM,
    BatchExecutor,
    PolicyBlockedError,
    SubprocessExecutor,
    deployment_profile,
    resolve_executor,
    resolve_run_paths,
    run_spec_from_mapping,
)
from ui.orchestration.policy import BATCH, DEVELOPMENT, PRODUCTION, SUBPROCESS
from ui.services.batch_runner import InputFiles


def _builtin_spec(**overrides) -> dict:
    base = {
        "run_id": "r-test-1",
        "mode": BUILTIN,
        "dataset_id": "child_birth",
        "inputs": {},
        "rules": {},
        "options": {},
    }
    base.update(overrides)
    return base


def _custom_spec(**overrides) -> dict:
    base = {
        "run_id": "r-custom-1",
        "mode": CUSTOM,
        "dataset_id": "custom",
        "inputs": {"session_id": "sess1", "tmcf_filename": "a.tmcf", "csv_filenames": ["b.csv"]},
        "rules": {},
        "options": {},
    }
    base.update(overrides)
    return base


class TestDeploymentProfile(unittest.TestCase):
    def test_explicit_development(self) -> None:
        self.assertEqual(
            deployment_profile({"DEPLOYMENT_PROFILE": "development"}),
            DEVELOPMENT,
        )

    def test_infer_production_when_gcs_and_batch(self) -> None:
        env = {
            "GCS_REPORTS_BUCKET": "bucket",
            "BATCH_PROJECT_ID": "proj",
        }
        self.assertEqual(deployment_profile(env), PRODUCTION)


class TestResolveExecutor(unittest.TestCase):
    def test_dev_builtin_subprocess(self) -> None:
        spec = run_spec_from_mapping(_builtin_spec())
        res = resolve_executor(spec, env={"DEPLOYMENT_PROFILE": "development"})
        self.assertEqual(res.executor, SUBPROCESS)
        self.assertEqual(res.profile, DEVELOPMENT)

    def test_dev_custom_blocked(self) -> None:
        spec = run_spec_from_mapping(_custom_spec())
        with self.assertRaises(PolicyBlockedError):
            resolve_executor(spec, env={"DEPLOYMENT_PROFILE": "development"})

    def test_dev_custom_allowed_with_flag(self) -> None:
        spec = run_spec_from_mapping(_custom_spec())
        res = resolve_executor(
            spec,
            env={"DEPLOYMENT_PROFILE": "development", "ALLOW_LOCAL_CUSTOM": "1"},
        )
        self.assertEqual(res.executor, SUBPROCESS)

    def test_prod_builtin_batch(self) -> None:
        spec = run_spec_from_mapping(_builtin_spec())
        env = {
            "DEPLOYMENT_PROFILE": "production",
            "GCS_REPORTS_BUCKET": "b",
            "BATCH_PROJECT_ID": "p",
            "BATCH_REGION": "us-central1",
            "BATCH_SERVICE_ACCOUNT": "sa@test",
        }
        res = resolve_executor(spec, env=env)
        self.assertEqual(res.executor, BATCH)
        self.assertTrue(res.requires_gcs)

    def test_prod_custom_requires_gcs(self) -> None:
        spec = run_spec_from_mapping(_custom_spec())
        env = {
            "DEPLOYMENT_PROFILE": "production",
            "BATCH_PROJECT_ID": "p",
            "BATCH_REGION": "us-central1",
            "BATCH_SERVICE_ACCOUNT": "sa@test",
        }
        with self.assertRaises(PolicyBlockedError):
            resolve_executor(spec, env=env)


class TestRunPaths(unittest.TestCase):
    def test_per_run_output_dir(self) -> None:
        paths = resolve_run_paths("child_birth", "run-abc", app_root=ROOT)
        self.assertEqual(paths.output_dir, ROOT / "output" / "child_birth" / "run-abc")
        self.assertEqual(
            paths.canonical_output_dir, ROOT / "output" / "child_birth_genmcf"
        )

    def test_status_uri(self) -> None:
        paths = resolve_run_paths("child_birth", "run-abc", app_root=ROOT)
        with patch.dict(os.environ, {"GCS_REPORTS_BUCKET": "my-bucket"}, clear=False):
            self.assertEqual(
                paths.status_json_uri,
                "gs://my-bucket/jobs/run-abc/status.json",
            )


class TestRunSpec(unittest.TestCase):
    def test_frozen_immutable(self) -> None:
        spec = run_spec_from_mapping(_builtin_spec())
        with self.assertRaises(Exception):
            spec.run_id = "other"  # type: ignore[misc]

    def test_custom_requires_custom_dataset_id(self) -> None:
        with self.assertRaises(ValueError):
            run_spec_from_mapping(
                {"run_id": "x", "mode": CUSTOM, "dataset_id": "child_birth"}
            )


class TestSubprocessExecutor(unittest.TestCase):
    def test_build_builtin_args(self) -> None:
        spec = run_spec_from_mapping(_builtin_spec())
        ex = SubprocessExecutor(ROOT)
        args = ex.build_args(spec)
        self.assertIn("child_birth", args)
        self.assertTrue(str(ex.e2e_script_path()) in args[0] or "bash" in args)

    def test_plan_run_paths(self) -> None:
        spec = run_spec_from_mapping(_builtin_spec())
        plan = SubprocessExecutor(ROOT).plan_run(spec, stream=True)
        self.assertEqual(plan.dataset, "child_birth")
        self.assertEqual(plan.output_dir.name, "r-test-1")


class TestBatchExecutor(unittest.TestCase):
    def test_spec_to_input_files_session_mode(self) -> None:
        spec = run_spec_from_mapping(_custom_spec())
        files = BatchExecutor().spec_to_input_files(spec)
        self.assertIsInstance(files, InputFiles)
        self.assertEqual(files.gcs_prefix, "sessions/sess1")
        self.assertEqual(files.tmcf_filename, "a.tmcf")

    @patch("ui.orchestration.executors.batch.submit_job", return_value="projects/p/locations/r/jobs/j1")
    def test_submit_delegates(self, mock_submit: unittest.mock.MagicMock) -> None:
        spec = run_spec_from_mapping(_custom_spec())
        result = BatchExecutor().submit_spec(spec)
        self.assertEqual(result.job_name, "projects/p/locations/r/jobs/j1")
        mock_submit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
