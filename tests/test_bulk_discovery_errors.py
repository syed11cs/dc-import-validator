"""Tests for bulk GCS discovery error classification and outcomes."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui import bulk_gcs_discovery as bgd
from ui.bulk_ui_helpers import bulk_outcome_severity, bulk_response_outcome


class TestClassifyGcsDiscoveryError(unittest.TestCase):
    def test_not_found_by_message(self) -> None:
        status, body = bgd.classify_gcs_discovery_error(Exception("404 Not Found: bucket"))
        self.assertEqual(status, 404)
        self.assertEqual(body["code"], "gcs_not_found")

    def test_access_denied_by_message(self) -> None:
        status, body = bgd.classify_gcs_discovery_error(Exception("403 Forbidden: access denied"))
        self.assertEqual(status, 403)
        self.assertEqual(body["code"], "gcs_access_denied")


class TestBulkDiscoveryOutcome(unittest.TestCase):
    def test_empty_root(self) -> None:
        code, msg = bgd.bulk_discovery_outcome(
            datasets_found=0,
            submitted=0,
            discovered_count=0,
            skipped_count=0,
            run_count=0,
        )
        self.assertEqual(code, "empty_root")
        self.assertIn("No dataset folders", msg)

    def test_no_runnable(self) -> None:
        code, msg = bgd.bulk_discovery_outcome(
            datasets_found=2,
            submitted=0,
            discovered_count=0,
            skipped_count=2,
            run_count=0,
        )
        self.assertEqual(code, "no_runnable")
        self.assertIn("runnable", msg.lower())

    def test_ok_when_submitted(self) -> None:
        code, msg = bgd.bulk_discovery_outcome(
            datasets_found=1,
            submitted=1,
            discovered_count=1,
            skipped_count=0,
            run_count=1,
        )
        self.assertEqual(code, "ok")
        self.assertEqual(msg, "")


class TestBulkResponseOutcome(unittest.TestCase):
    def test_uses_server_outcome_fields(self) -> None:
        out = bulk_response_outcome({
            "submitted": 0,
            "outcome": "empty_root",
            "outcome_message": "No dataset folders found under the provided GCS path.",
        })
        self.assertIsNotNone(out)
        assert out is not None
        self.assertEqual(out["code"], "empty_root")
        self.assertEqual(out["severity"], "info")
        self.assertEqual(out["title"], "Discovery complete")

    def test_empty_root_inferred_severity(self) -> None:
        out = bulk_response_outcome({"submitted": 0, "datasets_found": 0})
        self.assertIsNotNone(out)
        assert out is not None
        self.assertEqual(out["code"], "empty_root")
        self.assertEqual(out["severity"], "info")

    def test_none_when_jobs_submitted(self) -> None:
        self.assertIsNone(bulk_response_outcome({"submitted": 2, "datasets_found": 2}))


class TestBulkOutcomeSeverity(unittest.TestCase):
    def test_informational_codes(self) -> None:
        self.assertEqual(bulk_outcome_severity("empty_root"), "info")
        self.assertEqual(bulk_outcome_severity("no_runnable"), "info")

    def test_failure_codes(self) -> None:
        self.assertEqual(bulk_outcome_severity("gcs_not_found"), "error")
        self.assertEqual(bulk_outcome_severity("submit_failed"), "warning")


if __name__ == "__main__":
    unittest.main()
