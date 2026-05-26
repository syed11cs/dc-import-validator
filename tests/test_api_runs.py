"""Tests for /api/runs and orchestration run helpers."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.orchestration.policy import BATCH, DEVELOPMENT, PRODUCTION, SUBPROCESS
from ui.orchestration.policy import ExecutorResolution, resolve_executor
from ui.orchestration.executors.batch import BatchExecutor
from ui.orchestration.runs import (
    build_run_created_response,
    effective_rules_filter,
    job_request_to_run_spec,
    normalize_run_status,
    pipeline_registry_payload,
    run_spec_with_batch_overrides,
    subprocess_legacy_hint,
)
from ui.services.batch_runner import InputFiles
from ui.orchestration.spec import run_spec_from_mapping


def _job_body(**kwargs) -> SimpleNamespace:
    defaults = {
        "run_id": "run-abc",
        "dataset": "child_birth",
        "session_id": "",
        "tmcf_filename": "",
        "csv_filenames": [],
        "stat_vars_mcf_filename": "",
        "stat_vars_schema_mcf_filename": "",
        "csv_total_bytes": 0,
        "tmcf_gcs_path": "",
        "csv_gcs_paths": [],
        "stat_vars_mcf_gcs_path": "",
        "stat_vars_schema_mcf_gcs_path": "",
        "llm_review": False,
        "rules": "",
        "skip_rules": "",
        "baseline_name": "",
        "import_resolution_mode": "LOCAL",
        "existence_checks": "true",
        "custom_rules": [],
        "validation_config_url": "",
        "machine_type_override": "",
        "processing_mode": "auto",
        "java_threads": 0,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestJobRequestToRunSpec(unittest.TestCase):
    def test_builtin_mapping(self) -> None:
        spec = job_request_to_run_spec(_job_body(dataset="child_birth"))
        self.assertEqual(spec.mode, "builtin")
        self.assertEqual(spec.dataset_id, "child_birth")
        self.assertEqual(spec.options.existence_checks, "true")

    def test_custom_mapping(self) -> None:
        spec = job_request_to_run_spec(
            _job_body(
                dataset="custom",
                session_id="s1",
                tmcf_filename="a.tmcf",
                csv_filenames=["b.csv"],
            )
        )
        self.assertEqual(spec.mode, "custom")
        self.assertEqual(spec.dataset_id, "custom")
        self.assertEqual(spec.inputs.session_id, "s1")


class TestNormalizeRunStatus(unittest.TestCase):
    def test_adds_schema_version_and_executor(self) -> None:
        out = normalize_run_status({"step": "2", "status": "running"})
        self.assertEqual(out["schema_version"], "1.0")
        self.assertEqual(out["executor"], SUBPROCESS)
        self.assertEqual(out["step_index"], 2)

    def test_preserves_batch_fields(self) -> None:
        raw = {
            "schema_version": "1.0",
            "step_id": "import_tool",
            "step_index": 2,
            "status": "running",
            "batch_job_name": "projects/p/jobs/j",
        }
        out = normalize_run_status(raw)
        self.assertEqual(out["executor"], BATCH)
        self.assertEqual(out["step_id"], "import_tool")


class TestBuildRunCreatedResponse(unittest.TestCase):
    def test_superset_of_legacy_jobs(self) -> None:
        res = ExecutorResolution(executor=BATCH, profile=PRODUCTION, reason="test")
        out = build_run_created_response(
            run_id="r1",
            resolution=res,
            batch_result={"run_id": "r1", "job_name": "projects/x/jobs/y"},
        )
        self.assertEqual(out["run_id"], "r1")
        self.assertEqual(out["job_name"], "projects/x/jobs/y")
        self.assertEqual(out["batch_job_name"], "projects/x/jobs/y")
        self.assertEqual(out["executor"], BATCH)


class TestPipelineRegistryPayload(unittest.TestCase):
    def test_loads_six_steps(self) -> None:
        payload = pipeline_registry_payload(ROOT)
        self.assertIn("schema_version", payload)
        self.assertEqual(len(payload["steps"]), 6)
        self.assertIn("legacy_markers", payload)


class TestBatchRunSpecHelpers(unittest.TestCase):
    def test_effective_rules_filter_empty_when_config_url(self) -> None:
        spec = run_spec_from_mapping(
            {
                "run_id": "r1",
                "mode": "builtin",
                "dataset_id": "child_birth",
                "inputs": {},
                "rules": {
                    "rules_filter": "rule_a",
                    "validation_config_url": "https://example.com/c.json",
                },
                "options": {},
            }
        )
        self.assertEqual(effective_rules_filter(spec), "")

    def test_run_spec_overrides_match_batch_executor_input_files(self) -> None:
        spec = run_spec_from_mapping(
            {
                "run_id": "r1",
                "mode": "custom",
                "dataset_id": "custom",
                "inputs": {
                    "session_id": "sess",
                    "tmcf_filename": "a.tmcf",
                    "csv_filenames": ["b.csv"],
                    "csv_total_bytes": 1000,
                },
                "rules": {"merged_config_gcs_path": "gs://b/o.json"},
                "options": {"machine_type_override": "n2-highmem-32"},
            }
        )
        final = run_spec_with_batch_overrides(
            spec,
            csv_total_bytes=2000,
            rules_filter="",
            machine_type_override="n2-highmem-64",
        )
        files = BatchExecutor().spec_to_input_files(final)
        self.assertIsInstance(files, InputFiles)
        self.assertEqual(files.csv_total_bytes, 2000)
        self.assertEqual(files.rules_filter, "")
        self.assertEqual(files.merged_config_gcs_path, "gs://b/o.json")
        self.assertEqual(final.options.machine_type_override, "n2-highmem-64")


class TestSubprocessLegacyHint(unittest.TestCase):
    def test_builtin_hint(self) -> None:
        spec = run_spec_from_mapping(
            {
                "run_id": "r1",
                "mode": "builtin",
                "dataset_id": "child_birth",
                "inputs": {},
                "rules": {},
                "options": {},
            }
        )
        hint = subprocess_legacy_hint(spec)
        self.assertIn("/api/run/child_birth", hint["legacy_endpoint"])


class TestCreateRunPolicy(unittest.TestCase):
    def test_dev_builtin_resolves_subprocess(self) -> None:
        spec = job_request_to_run_spec(_job_body())
        res = resolve_executor(spec, env={"DEPLOYMENT_PROFILE": "development"})
        self.assertEqual(res.executor, SUBPROCESS)


class TestServerRoutes(unittest.TestCase):
    def test_registry_route(self) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        client = TestClient(app)
        resp = client.get("/api/pipeline/registry")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data["steps"]), 6)

    @patch("ui.server._execute_batch_job_submission")
    def test_create_run_batch_profile(self, mock_submit) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        mock_submit.return_value = {"run_id": "run-1", "job_name": "projects/p/jobs/j1"}
        env = {
            "DEPLOYMENT_PROFILE": "production",
            "GCS_REPORTS_BUCKET": "b",
            "BATCH_PROJECT_ID": "p",
            "BATCH_REGION": "us-central1",
            "BATCH_SERVICE_ACCOUNT": "sa@test",
        }
        with patch.dict(os.environ, env, clear=False):
            client = TestClient(app)
            resp = client.post(
                "/api/runs",
                json={"run_id": "run-1", "dataset": "child_birth"},
            )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["job_name"], "projects/p/jobs/j1")
        self.assertEqual(data["executor"], BATCH)
        mock_submit.assert_called_once()

    def test_create_run_subprocess_profile_returns_legacy_hint(self) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        with patch.dict(os.environ, {"DEPLOYMENT_PROFILE": "development"}, clear=False):
            client = TestClient(app)
            resp = client.post(
                "/api/runs",
                json={"run_id": "run-1", "dataset": "child_birth"},
            )
        self.assertEqual(resp.status_code, 400)
        detail = resp.json()["detail"]
        self.assertEqual(detail["code"], "USE_LEGACY_STREAM_ENDPOINT")
        self.assertIn("legacy_endpoint", detail)

    @patch("ui.server.fetch_run_status", return_value={"status": "running", "step": "1"})
    def test_jobs_status_delegates_to_runs_api(self, mock_fetch) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        client = TestClient(app)
        resp = client.get("/api/jobs/run-xyz/status")
        self.assertEqual(resp.status_code, 200)
        mock_fetch.assert_called_once()
        self.assertEqual(resp.json()["schema_version"], "1.0")

    def test_jobs_submit_blocked_in_dev_same_as_runs(self) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        with patch.dict(os.environ, {"DEPLOYMENT_PROFILE": "development"}, clear=False):
            client = TestClient(app)
            resp = client.post(
                "/api/jobs",
                json={"run_id": "run-1", "dataset": "child_birth"},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["detail"]["code"], "USE_LEGACY_STREAM_ENDPOINT")

    @patch("ui.server._batch_runner.cancel_job")
    @patch("ui.server._get_job_status", return_value={"status": "running", "batch_job_name": "projects/p/jobs/j1"})
    def test_runs_cancel_alias(self, mock_status, mock_cancel) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        client = TestClient(app)
        resp = client.post("/api/runs/run-xyz/cancel")
        self.assertEqual(resp.status_code, 200)
        mock_cancel.assert_called_once_with("projects/p/jobs/j1")

    @patch("ui.server.is_gcs_configured", return_value=True)
    @patch("ui.server._get_job_status", return_value=None)
    def test_run_report_not_found_message(self, _status, _gcs) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        client = TestClient(app)
        resp = client.get("/api/runs/missing-run/report")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("Report not available yet", resp.json()["detail"])

    @patch("ui.server._batch_run_html_report")
    def test_runs_report_alias(self, mock_report) -> None:
        from fastapi.responses import HTMLResponse
        from fastapi.testclient import TestClient
        from ui.server import app

        mock_report.return_value = HTMLResponse("<html>ok</html>")
        client = TestClient(app)
        resp = client.get("/api/runs/run-xyz/report")
        self.assertEqual(resp.status_code, 200)
        mock_report.assert_called_once_with("run-xyz")
        resp_jobs = client.get("/api/jobs/run-xyz/report")
        self.assertEqual(resp_jobs.status_code, 200)
        self.assertEqual(mock_report.call_count, 2)

    @patch("ui.server.fetch_run_status", return_value={"status": "running", "step": "1"})
    def test_runs_status_unified(self, mock_fetch) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        client = TestClient(app)
        resp = client.get("/api/runs/run-xyz")
        self.assertEqual(resp.status_code, 200)
        mock_fetch.assert_called_once_with("run-xyz")


if __name__ == "__main__":
    unittest.main()
