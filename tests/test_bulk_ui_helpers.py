"""Tests for bulk UI helper logic and index.html contract."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.bulk_ui_helpers import (
    bulk_folder_display_name,
    bulk_run_stats_summary,
    validate_bulk_gcs_root,
)

INDEX_HTML = ROOT / "ui" / "index.html"


class TestValidateBulkGcsRoot(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(validate_bulk_gcs_root(""), "Root GCS folder is required.")

    def test_requires_gs_scheme(self) -> None:
        self.assertIn("gs://", validate_bulk_gcs_root("s3://b/x") or "")

    def test_valid(self) -> None:
        self.assertIsNone(validate_bulk_gcs_root("gs://my-bucket/imports/"))


class TestBulkFolderDisplayName(unittest.TestCase):
    def test_trailing_slash(self) -> None:
        self.assertEqual(
            bulk_folder_display_name("imports/dataset_a/"),
            "dataset_a",
        )


class TestBulkRunStatsSummary(unittest.TestCase):
    def test_counts(self) -> None:
        stats = bulk_run_stats_summary({
            "datasets_found": 12,
            "submitted": 9,
            "skipped": 3,
        })
        self.assertEqual(stats["discovered"], 12)
        self.assertEqual(stats["valid_runs"], 9)
        self.assertEqual(stats["runnable"], 9)
        self.assertEqual(stats["skipped"], 3)


class TestIndexHtmlBulkUiContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = INDEX_HTML.read_text(encoding="utf-8")

    def test_segmented_gcs_validation_toggle(self) -> None:
        self.assertIn('id="gcs-mode-single-btn"', self.html)
        self.assertIn('id="gcs-mode-bulk-btn"', self.html)
        self.assertNotIn('id="gcs-validation-mode"', self.html)

    def test_from_gcs_label(self) -> None:
        self.assertIn('id="mode-gcs-btn">From GCS</button>', self.html)

    def test_parallel_validations_label(self) -> None:
        self.assertIn("Parallel validations", self.html)
        self.assertNotIn("Max parallel jobs", self.html)

    def test_bulk_sidebar_note(self) -> None:
        self.assertIn("id=\"bulk-rules-sidebar-note\"", self.html)
        self.assertIn("validation_config.json", self.html)

    def test_bulk_stats_and_discovery_status(self) -> None:
        self.assertIn("id=\"bulk-runs-stats\"", self.html)
        self.assertIn("id=\"bulk-discovery-status\"", self.html)
        self.assertIn("Discovering datasets from GCS", self.html)
        self.assertIn("Runnable:", self.html)
        self.assertIn("id=\"bulk-stat-runnable\"", self.html)

    def test_bulk_placeholder_and_parallel_hint(self) -> None:
        self.assertIn("gs://dc-imports/2026/batch-run/", self.html)
        self.assertIn("Number of dataset folders processed at the same time", self.html)
        self.assertNotIn("bulk-helper-primary", self.html)
        self.assertNotIn("Higher parallelism increases Batch resource usage", self.html)

    def test_bulk_parallelism_input_constraints(self) -> None:
        self.assertIn('id="bulk-gcs-parallelism"', self.html)
        idx = self.html.find('id="bulk-gcs-parallelism"')
        snippet = self.html[idx:idx + 220]
        self.assertIn('min="1"', snippet)
        self.assertIn('max="5"', snippet)
        self.assertIn('step="1"', snippet)
        self.assertIn('inputmode="numeric"', snippet)
        self.assertIn("appearance: textfield", self.html)
        self.assertIn("inner-spin-button", self.html)

    def test_bulk_root_inline_error(self) -> None:
        self.assertIn("id=\"bulk-gcs-root-error\"", self.html)

    def test_expected_structure_under_root(self) -> None:
        self.assertIn("id=\"bulk-folder-structure-hint\"", self.html)
        idx = self.html.find("id=\"bulk-gcs-root\"")
        idx_hint = self.html.find("id=\"bulk-folder-structure-hint\"")
        self.assertGreater(idx_hint, idx)

    def test_bulk_dashboard_ux_polish_contract(self) -> None:
        self.assertIn("id=\"bulk-run-id-row\"", self.html)
        self.assertIn("Bulk run ID", self.html)
        self.assertIn("id=\"bulk-live-status-summary\"", self.html)
        self.assertIn("id=\"bulk-final-completion\"", self.html)
        self.assertIn("id=\"bulk-poll-refreshing\"", self.html)
        self.assertIn("id=\"bulk-submission-error\"", self.html)
        self.assertIn("id=\"bulk-runs-empty\"", self.html)
        self.assertIn("bulk-status-pill--submitted", self.html)
        self.assertIn("bulk-status-pill--succeeded", self.html)
        self.assertIn("bulk-report-pending", self.html)
        self.assertIn("Report preparing", self.html)
        self.assertNotIn("bulk-report-btn--pending", self.html)
        self.assertIn("Updating job statuses", self.html)
        self.assertIn("bulk-live-metric", self.html)
        self.assertIn("bulk-live-status-label", self.html)
        self.assertIn("Dataset validation in progress", self.html)
        self.assertIn("Dataset job status", self.html)
        self.assertIn("setBulkSubmissionMode", self.html)
        self.assertIn("Jobs submitted", self.html)
        self.assertIn("Discovery & submit complete", self.html)
        self.assertIn("Submitted ", self.html)
        self.assertIn("formatBulkRunLogMessage", self.html)
        self.assertIn("bulkReportEndpointReady", self.html)
        self.assertIn("bulk-runs-panel--failure", self.html)
        self.assertIn("classifyBulkResponseOutcome", self.html)
        self.assertIn("showBulkDiscoveryFailurePanel", self.html)
        self.assertIn("applyBulkOutcomePresentation", self.html)
        self.assertIn("bulk-runs-panel--empty", self.html)
        self.assertIn("bulk-runs-empty--info", self.html)
        self.assertIn("Discovery complete", self.html)
        self.assertIn("bulk-orchestration-banner", self.html)
        self.assertIn("bulk-poll-warning", self.html)
        self.assertIn("cancelBulkChildJobs", self.html)
        self.assertIn("bulkExecutionPhase", self.html)
        self.assertIn("lastBulkDiscoveryInformational", self.html)
        self.assertNotIn("Bulk jobs running", self.html)
        self.assertIn("bulk-submission-error-title", self.html)
        self.assertIn("bulkDatasetJobsActive", self.html)
        self.assertIn("syncBulkRunButtonState", self.html)
        self.assertIn("isBulkGcsRun", self.html)
        self.assertIn("!lastRunWasBulkSubmission", self.html)
        self.assertIn("applyBulkRunButtonMode", self.html)
        self.assertIn("refreshBulkOrchestrationBannerVisibility", self.html)
        self.assertIn("bulk-runs-panel--finished", self.html)
        self.assertIn("Discovering datasets", self.html)
        self.assertIn("Cancel active jobs", self.html)
        self.assertNotIn("Discovering & submitting", self.html)
        self.assertNotIn("Cancel dataset jobs", self.html)
        self.assertIn("btn-bulk-running", self.html)
        self.assertNotIn("Bulk run bulk", self.html)


if __name__ == "__main__":
    unittest.main()
