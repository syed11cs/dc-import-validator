"""Tests for POST /api/bulk-runs."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.bulk_gcs_discovery import DiscoveredDataset, SkippedFolder
from ui.orchestration.policy import BATCH, PRODUCTION, ExecutorResolution


class TestBulkRunsEndpoint(unittest.TestCase):
    def _client(self):
        from fastapi.testclient import TestClient
        from ui.server import app

        return TestClient(app)

    def _env_patch(self):
        return patch.dict(
            os.environ,
            {
                "GCS_REPORTS_BUCKET": "b",
                "BATCH_PROJECT_ID": "p",
                "BATCH_REGION": "us-central1",
                "BATCH_SERVICE_ACCOUNT": "sa@test",
            },
            clear=False,
        )

    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=False)
    def test_503_without_gcs(self, _mock_gcs) -> None:
        client = self._client()
        resp = client.post(
            "/api/bulk-runs",
            json={"root_gcs_path": "gs://b/imports/"},
        )
        self.assertEqual(resp.status_code, 503)

    @patch("ui.server.resolve_executor")
    @patch("ui.server._execute_batch_job_submission", new_callable=AsyncMock)
    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server._bulk_gcs.discover_datasets_under_root")
    def test_one_batch_job_per_folder(
        self,
        mock_discover,
        _mock_gcs,
        mock_submit,
        mock_resolve,
    ) -> None:
        mock_resolve.return_value = ExecutorResolution(
            executor=BATCH, profile=PRODUCTION, reason="test"
        )
        mock_discover.return_value = (
            [
                DiscoveredDataset(
                    folder_prefix="imports/a/",
                    dataset_id="dataset-a",
                    csv_gcs_path="gs://b/imports/a/x.csv",
                    tmcf_gcs_path="gs://b/imports/a/x.tmcf",
                    csv_total_bytes=1000,
                ),
                DiscoveredDataset(
                    folder_prefix="imports/b/",
                    dataset_id="dataset-b",
                    csv_gcs_path="gs://b/imports/b/y.csv",
                    tmcf_gcs_path="gs://b/imports/b/y.tmcf",
                    validation_config_gcs_path="gs://b/imports/b/validation_config.json",
                    csv_total_bytes=2000,
                ),
            ],
            [],
        )
        mock_submit.return_value = {"run_id": "r1", "job_name": "projects/p/jobs/j"}

        client = self._client()
        with self._env_patch():
            resp = client.post(
                "/api/bulk-runs",
                json={
                    "root_gcs_path": "gs://b/imports/",
                    "parallelism": 1,
                    "llm_review": False,
                },
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["submitted"], 2)
        self.assertEqual(len(data["runs"]), 2)
        self.assertEqual(mock_submit.await_count, 2)

        second_call = mock_submit.await_args_list[1].args[0]
        self.assertEqual(
            second_call.validation_config_url,
            "gs://b/imports/b/validation_config.json",
        )
        self.assertEqual(second_call.rules, "")
        self.assertEqual(second_call.custom_rules, [])

    @patch("ui.server.resolve_executor")
    @patch("ui.server._execute_batch_job_submission", new_callable=AsyncMock)
    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server._bulk_gcs.discover_datasets_under_root")
    def test_one_failure_does_not_block_other(
        self,
        mock_discover,
        _mock_gcs,
        mock_submit,
        mock_resolve,
    ) -> None:
        from fastapi import HTTPException

        mock_resolve.return_value = ExecutorResolution(
            executor=BATCH, profile=PRODUCTION, reason="test"
        )
        mock_discover.return_value = (
            [
                DiscoveredDataset(
                    folder_prefix="imports/good/",
                    dataset_id="good",
                    csv_gcs_path="gs://b/imports/good/x.csv",
                    tmcf_gcs_path="gs://b/imports/good/x.tmcf",
                ),
                DiscoveredDataset(
                    folder_prefix="imports/bad/",
                    dataset_id="bad",
                    csv_gcs_path="gs://b/imports/bad/y.csv",
                    tmcf_gcs_path="gs://b/imports/bad/y.tmcf",
                ),
            ],
            [],
        )

        async def _side_effect(body):
            if body.baseline_name == "custom_bad":
                raise HTTPException(status_code=500, detail="config upload failed")
            return {"run_id": body.run_id, "job_name": "j"}

        mock_submit.side_effect = _side_effect

        client = self._client()
        with self._env_patch():
            resp = client.post(
                "/api/bulk-runs",
                json={"root_gcs_path": "gs://b/imports/"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["submitted"], 1)
        statuses = {r["dataset"]: r["status"] for r in data["runs"]}
        self.assertEqual(statuses["good"], "submitted")
        self.assertEqual(statuses["bad"], "failed")

    @patch("ui.server.asyncio.Semaphore")
    @patch("ui.server.resolve_executor")
    @patch("ui.server._execute_batch_job_submission", new_callable=AsyncMock)
    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server._bulk_gcs.discover_datasets_under_root")
    def test_parallelism_capped_on_server(
        self,
        mock_discover,
        _mock_gcs,
        mock_submit,
        mock_resolve,
        mock_semaphore,
    ) -> None:
        mock_resolve.return_value = ExecutorResolution(
            executor=BATCH, profile=PRODUCTION, reason="test"
        )
        mock_discover.return_value = (
            [
                DiscoveredDataset(
                    folder_prefix="imports/a/",
                    dataset_id="a",
                    csv_gcs_path="gs://b/imports/a/x.csv",
                    tmcf_gcs_path="gs://b/imports/a/x.tmcf",
                ),
            ],
            [],
        )
        mock_submit.return_value = {"run_id": "r1", "job_name": "j"}
        mock_semaphore.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_semaphore.return_value.__aexit__ = AsyncMock(return_value=None)

        client = self._client()
        with self._env_patch():
            resp = client.post(
                "/api/bulk-runs",
                json={"root_gcs_path": "gs://b/imports/", "parallelism": 99},
            )

        self.assertEqual(resp.status_code, 200)
        mock_semaphore.assert_called_once_with(5)

    @patch("ui.server.resolve_executor")
    @patch("ui.server._execute_batch_job_submission", new_callable=AsyncMock)
    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server._bulk_gcs.discover_datasets_under_root")
    def test_request_url_used_when_no_folder_config(
        self,
        mock_discover,
        _mock_gcs,
        mock_submit,
        mock_resolve,
    ) -> None:
        mock_resolve.return_value = ExecutorResolution(
            executor=BATCH, profile=PRODUCTION, reason="test"
        )
        mock_discover.return_value = (
            [
                DiscoveredDataset(
                    folder_prefix="imports/a/",
                    dataset_id="a",
                    csv_gcs_path="gs://b/imports/a/x.csv",
                    tmcf_gcs_path="gs://b/imports/a/x.tmcf",
                ),
            ],
            [],
        )
        mock_submit.return_value = {"run_id": "r1", "job_name": "j"}

        client = self._client()
        with self._env_patch():
            resp = client.post(
                "/api/bulk-runs",
                json={
                    "root_gcs_path": "gs://b/imports/",
                    "validation_config_url": "gs://b/global.json",
                    "rules": "check_min_value",
                },
            )

        self.assertEqual(resp.status_code, 200)
        body = mock_submit.await_args.args[0]
        self.assertEqual(body.validation_config_url, "gs://b/global.json")
        self.assertEqual(body.rules, "")

    @patch("ui.server.resolve_executor")
    @patch("ui.server._execute_batch_job_submission", new_callable=AsyncMock)
    @patch("ui.server._gcs_uploads.is_gcs_uploads_configured", return_value=True)
    @patch("ui.server._bulk_gcs.discover_datasets_under_root")
    def test_skipped_folders_in_response(
        self,
        mock_discover,
        _mock_gcs,
        mock_submit,
        mock_resolve,
    ) -> None:
        mock_resolve.return_value = ExecutorResolution(
            executor=BATCH, profile=PRODUCTION, reason="test"
        )
        mock_discover.return_value = (
            [
                DiscoveredDataset(
                    folder_prefix="imports/dataset-ok/",
                    dataset_id="dataset-ok",
                    csv_gcs_path="gs://b/imports/dataset-ok/x.csv",
                    tmcf_gcs_path="gs://b/imports/dataset-ok/x.tmcf",
                ),
            ],
            [SkippedFolder(folder_prefix="imports/bad/", reason="missing CSV")],
        )
        mock_submit.return_value = {"run_id": "r1", "job_name": "j"}

        client = self._client()
        with self._env_patch():
            resp = client.post("/api/bulk-runs", json={"root_gcs_path": "gs://b/imports/"})

        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["datasets_found"], 2)
        self.assertEqual(len(data["skipped_folders"]), 1)
        mock_submit.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
