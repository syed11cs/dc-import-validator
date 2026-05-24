"""Tests for Batch validation config file upload (POST /api/runs/{run_id}/validation-config)."""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.orchestration.policy import BATCH, PRODUCTION, ExecutorResolution

# Minimal config with two distinct rules (no SQL_VALIDATOR — avoids DuckDB pre-check).
TWO_RULE_CONFIG = json.dumps(
    {
        "schema_version": "1.0",
        "rules": [
            {
                "rule_id": "override_rule_a",
                "description": "override a",
                "validator": "MIN_VALUE_CHECK",
                "scope": {"data_source": "stats"},
                "params": {"minimum": 0},
            },
            {
                "rule_id": "override_rule_b",
                "description": "override b",
                "validator": "NUM_OBSERVATIONS_CHECK",
                "scope": {"data_source": "stats"},
                "params": {"minimum": 1},
            },
        ],
    }
).encode()

URL_ONLY_CONFIG = json.dumps(
    {
        "schema_version": "1.0",
        "rules": [
            {
                "rule_id": "from_url_only",
                "description": "url rule",
                "validator": "MIN_VALUE_CHECK",
                "scope": {"data_source": "stats"},
                "params": {"minimum": 0},
            },
        ],
    }
).encode()


def _job_body(**kwargs):
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


class TestUploadValidationConfigEndpoint(unittest.TestCase):
    def _client(self):
        from fastapi.testclient import TestClient
        from ui.server import app

        return TestClient(app)

    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server.gcs_reports.upload_merged_config_to_gcs")
    def test_upload_success_returns_gs_uri(self, mock_upload, _mock_gcs) -> None:
        gcs_uri = "gs://test-bucket/configs/run-abc/validation_config.json"
        mock_upload.return_value = gcs_uri
        client = self._client()
        resp = client.post(
            "/api/runs/run-abc/validation-config",
            files={"validation_config": ("override.json", TWO_RULE_CONFIG, "application/json")},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["validation_config_url"], gcs_uri)
        mock_upload.assert_called_once()
        _run_id, path = mock_upload.call_args[0]
        self.assertEqual(_run_id, "run-abc")
        self.assertIsInstance(path, Path)

    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    def test_upload_rejects_invalid_json(self, _mock_gcs) -> None:
        client = self._client()
        resp = client.post(
            "/api/runs/run-abc/validation-config",
            files={"validation_config": ("bad.json", b"{not json", "application/json")},
        )
        self.assertEqual(resp.status_code, 400)

    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=False)
    def test_upload_503_when_gcs_not_configured(self, _mock_gcs) -> None:
        client = self._client()
        resp = client.post(
            "/api/runs/run-abc/validation-config",
            files={"validation_config": ("c.json", TWO_RULE_CONFIG, "application/json")},
        )
        self.assertEqual(resp.status_code, 503)

    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server.gcs_reports.upload_merged_config_to_gcs", return_value="")
    def test_upload_fails_closed_when_gcs_upload_empty(self, mock_upload, _mock_gcs) -> None:
        client = self._client()
        resp = client.post(
            "/api/runs/run-abc/validation-config",
            files={"validation_config": ("c.json", TWO_RULE_CONFIG, "application/json")},
        )
        self.assertEqual(resp.status_code, 500)
        mock_upload.assert_called_once()


class TestBatchConfigPrecedence(unittest.TestCase):
    """Batch UI contract: file upload must not fall back to URL field on failure."""

    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server.gcs_reports.upload_merged_config_to_gcs", return_value="")
    def test_upload_failure_is_500_not_silent_success(self, _mock_upload, _mock_gcs) -> None:
        from fastapi.testclient import TestClient
        from ui.server import app

        client = TestClient(app)
        resp = client.post(
            "/api/runs/run-fail/validation-config",
            files={"validation_config": ("c.json", TWO_RULE_CONFIG, "application/json")},
        )
        self.assertEqual(resp.status_code, 500)
        self.assertNotIn("validation_config_url", resp.json())


class TestBatchConfigUrlOverride(unittest.IsolatedAsyncioTestCase):
    """Batch submission via validation_config_url (after file upload → gs:// URI)."""

    @patch("ui.server.BatchExecutor.submit", return_value=SimpleNamespace(job_name="projects/p/jobs/j1"))
    @patch("ui.server.resolve_executor")
    @patch("ui.server.gcs_reports.upload_merged_config_to_gcs")
    @patch("ui.server._fetch_and_validate_config", new_callable=AsyncMock)
    async def test_batch_submit_uses_url_config_not_sidebar_rules(
        self, mock_fetch, mock_upload_gcs, mock_resolve, mock_submit
    ) -> None:
        from ui.server import _execute_batch_job_submission

        mock_resolve.return_value = ExecutorResolution(
            executor=BATCH, profile=PRODUCTION, reason="test"
        )
        gcs_uri = "gs://test-bucket/configs/run-file/validation_config.json"
        mock_fetch.return_value = TWO_RULE_CONFIG
        mock_upload_gcs.return_value = gcs_uri

        body = _job_body(
            run_id="run-file",
            dataset="child_birth",
            rules="check_min_value,check_num_observations",
            validation_config_url=gcs_uri,
        )
        with patch.dict(
            os.environ,
            {
                "GCS_REPORTS_BUCKET": "test-bucket",
                "BATCH_PROJECT_ID": "p",
                "BATCH_REGION": "us-central1",
                "BATCH_SERVICE_ACCOUNT": "sa@test",
            },
            clear=False,
        ):
            result = await _execute_batch_job_submission(body)

        self.assertEqual(result["run_id"], "run-file")
        mock_fetch.assert_awaited_once_with(gcs_uri)
        mock_submit.assert_called_once()
        batch_plan = mock_submit.call_args[0][0]
        self.assertEqual(batch_plan.input_files.merged_config_gcs_path, gcs_uri)
        self.assertEqual(batch_plan.input_files.rules_filter, "")

    @patch("ui.server.BatchExecutor.submit", return_value=SimpleNamespace(job_name="projects/p/jobs/j1"))
    @patch("ui.server.resolve_executor")
    @patch("ui.server.gcs_reports.upload_merged_config_to_gcs")
    @patch("ui.server._fetch_and_validate_config", new_callable=AsyncMock)
    async def test_upload_then_batch_uses_file_config_bytes(
        self, mock_fetch, mock_upload_gcs, mock_resolve, mock_submit
    ) -> None:
        """Simulates UI: POST validation-config (file) → POST /api/runs with returned gs:// URI."""
        from fastapi.testclient import TestClient
        from ui.server import _execute_batch_job_submission, app

        gcs_uri = "gs://test-bucket/configs/run-chain/validation_config.json"
        mock_upload_gcs.return_value = gcs_uri
        mock_fetch.return_value = TWO_RULE_CONFIG
        mock_resolve.return_value = ExecutorResolution(
            executor=BATCH, profile=PRODUCTION, reason="test"
        )

        with patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True):
            client = TestClient(app)
            up = client.post(
                "/api/runs/run-chain/validation-config",
                files={"validation_config": ("override.json", TWO_RULE_CONFIG, "application/json")},
            )
        self.assertEqual(up.status_code, 200)
        returned_url = up.json()["validation_config_url"]
        self.assertEqual(returned_url, gcs_uri)

        body = _job_body(
            run_id="run-chain",
            dataset="child_birth",
            rules="check_min_value",
            validation_config_url=returned_url,
        )
        with patch.dict(
            os.environ,
            {
                "GCS_REPORTS_BUCKET": "test-bucket",
                "BATCH_PROJECT_ID": "p",
                "BATCH_REGION": "us-central1",
                "BATCH_SERVICE_ACCOUNT": "sa@test",
            },
            clear=False,
        ):
            await _execute_batch_job_submission(body)

        mock_fetch.assert_awaited_once_with(gcs_uri)
        batch_plan = mock_submit.call_args[0][0]
        self.assertEqual(batch_plan.input_files.merged_config_gcs_path, gcs_uri)
        self.assertEqual(batch_plan.input_files.rules_filter, "")

    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server.gcs_reports.upload_merged_config_to_gcs")
    def test_file_upload_content_differs_from_url_field(
        self, mock_upload, _mock_gcs
    ) -> None:
        """File upload stores TWO_RULE_CONFIG; URL field content is not used on upload endpoint."""
        gcs_uri = "gs://test-bucket/configs/run-prec/validation_config.json"
        captured: list[bytes] = []

        def _capture_upload(_run_id: str, path: Path) -> str:
            captured.append(path.read_bytes())
            return gcs_uri

        mock_upload.side_effect = _capture_upload

        from fastapi.testclient import TestClient
        from ui.server import app

        client = TestClient(app)
        resp = client.post(
            "/api/runs/run-prec/validation-config",
            files={"validation_config": ("file.json", TWO_RULE_CONFIG, "application/json")},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0], TWO_RULE_CONFIG)
        self.assertNotEqual(captured[0], URL_ONLY_CONFIG)


if __name__ == "__main__":
    unittest.main()
