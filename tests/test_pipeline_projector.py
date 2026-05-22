"""Tests for pipeline/projector.py and pipeline/status_v1.py.

Run with:
    python -m unittest tests.test_pipeline_projector
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.progress import (
    build_failure_event,
    build_progress_event,
    build_run_finished_event,
    format_event,
)
from pipeline.projector import FeedResult, ProgressProjector
from pipeline.registry import load_registry
from pipeline.status_v1 import (
    STATUS_SCHEMA_VERSION,
    build_legacy_step_map,
    legacy_step_for_step_id,
    validate_status_projection,
)

LEGACY_MARKERS_IN_ORDER = (
    "::STEP::0:Pre-Import Checks",
    "::STEP::1:Gemini Review",
    "::STEP::2:DC Import Tool",
    "::STEP::2.4:Differ",
    "::STEP::3:DC Import Validation",
    "::STEP::4:Results",
)


class TestLegacyStepMap(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()
        cls.legacy_map = build_legacy_step_map(cls.registry)

    def test_baseline_diff_legacy_token_is_2_4(self) -> None:
        self.assertEqual(self.legacy_map["baseline_diff"], "2.4")

    def test_gemini_marker_maps_to_schema_review_not_label(self) -> None:
        self.assertEqual(legacy_step_for_step_id(self.registry, "schema_review"), "1")


class TestProgressProjectorLegacyMarkers(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_legacy_schema_review_uses_registry_label(self) -> None:
        p = ProgressProjector(registry=self.registry, run_id="r1", dataset="child_birth")
        result = p.feed_line("::STEP::1:Gemini Review")
        self.assertTrue(result.handled)
        self.assertEqual(result.kind, "legacy_step")
        status = p.to_status_dict()
        self.assertEqual(status["step_id"], "schema_review")
        self.assertEqual(status["step_label"], "Schema review")
        self.assertNotEqual(status["step_label"], "Gemini Review")
        self.assertEqual(status["step_index"], 1)
        self.assertEqual(status["step"], "1")
        self.assertEqual(status["status"], "running")

    def test_legacy_baseline_diff_step_index_three(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line("::STEP::2.4:Differ")
        status = p.to_status_dict()
        self.assertEqual(status["step_id"], "baseline_diff")
        self.assertEqual(status["step_index"], 3)
        self.assertEqual(status["step"], "2.4")

    def test_legacy_marker_strips_whitespace(self) -> None:
        p = ProgressProjector(registry=self.registry)
        self.assertTrue(p.feed_line("  ::STEP::0:Pre-Import Checks\n").handled)
        self.assertEqual(p.to_status_dict()["step_id"], "pre_import")

    def test_unknown_line_not_handled(self) -> None:
        p = ProgressProjector(registry=self.registry)
        self.assertFalse(p.feed_line("[INFO] hello").handled)

    def test_full_legacy_marker_sequence(self) -> None:
        p = ProgressProjector(registry=self.registry, run_id="run-abc", dataset="custom")
        for marker in LEGACY_MARKERS_IN_ORDER:
            self.assertTrue(p.feed_line(marker).handled, marker)
        status = p.to_status_dict()
        self.assertEqual(status["step_id"], "results")
        self.assertEqual(status["step_index"], 5)
        validate_status_projection(status, registry=self.registry)


class TestProgressProjectorV1Events(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_v1_progress(self) -> None:
        p = ProgressProjector(registry=self.registry)
        line = format_event(
            build_progress_event("validation", registry=self.registry)
        )
        result = p.feed_line(line)
        self.assertEqual(result.kind, "v1_progress")
        status = p.to_status_dict()
        self.assertEqual(status["step_id"], "validation")
        self.assertEqual(status["step_label"], "Import validation")
        self.assertEqual(status["step"], "3")

    def test_v1_progress_with_substep(self) -> None:
        p = ProgressProjector(registry=self.registry)
        line = format_event(
            build_progress_event(
                "pre_import",
                substep_id="csv_split",
                registry=self.registry,
            )
        )
        p.feed_line(line)
        status = p.to_status_dict()
        self.assertEqual(status["substep_id"], "csv_split")
        self.assertEqual(status["substep_label"], "Validate and split CSV")

    def test_v1_failure(self) -> None:
        p = ProgressProjector(registry=self.registry)
        line = format_event(
            build_failure_event(
                "pre_import",
                "PREFLIGHT_FAILED",
                "Preflight failed",
                registry=self.registry,
            )
        )
        p.feed_line(line)
        status = p.to_status_dict()
        self.assertEqual(status["failure_code"], "PREFLIGHT_FAILED")
        self.assertEqual(status["failure_step_id"], "pre_import")
        self.assertEqual(status["failure_step_index"], 0)
        self.assertEqual(status["status"], "running")

    def test_v1_run_finished_succeeded(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line(
            format_event(build_run_finished_event("succeeded", 0))
        )
        status = p.to_status_dict()
        self.assertEqual(status["status"], "succeeded")
        self.assertEqual(status["exit_code"], 0)

    def test_v1_run_finished_failed(self) -> None:
        p = ProgressProjector(registry=self.registry)
        line = format_event(
            build_run_finished_event(
                "failed",
                1,
                failure_code="DATA_PROCESSING_FAILED",
                message="genmcf failed",
                step_id="import_tool",
                registry=self.registry,
            )
        )
        p.feed_line(line)
        status = p.to_status_dict()
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["failure_code"], "DATA_PROCESSING_FAILED")
        self.assertEqual(status["step_id"], "import_tool")


class TestLegacyFailureNormalization(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_legacy_csv_split_failure_uses_pre_import_not_step_2(self) -> None:
        p = ProgressProjector(registry=self.registry)
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
        status = p.to_status_dict()
        self.assertEqual(status["failure_step_id"], "pre_import")
        self.assertEqual(status["failure_step_index"], 0)

    def test_legacy_failure_code_and_numeric_step(self) -> None:
        p = ProgressProjector(registry=self.registry)
        line = json.dumps(
            {
                "t": "failure",
                "code": "DATA_PROCESSING_FAILED",
                "step": 2,
                "message": "Data processing failed",
            }
        )
        result = p.feed_line(line)
        self.assertEqual(result.kind, "legacy_failure")
        status = p.to_status_dict()
        self.assertEqual(status["failure_code"], "DATA_PROCESSING_FAILED")
        self.assertEqual(status["failure_step_id"], "import_tool")
        self.assertEqual(status["failure_step_index"], 2)

    def test_legacy_failure_maps_step_2_4_to_baseline_diff(self) -> None:
        p = ProgressProjector(registry=self.registry)
        # Hypothetical legacy emitter using float step (Batch regex allows it).
        line = json.dumps(
            {
                "t": "failure",
                "code": "DIFFER_FAILED",
                "step": 2.4,
                "message": "Differ failed",
            }
        )
        p.feed_line(line)
        status = p.to_status_dict()
        self.assertEqual(status["failure_step_id"], "baseline_diff")
        self.assertEqual(status["failure_step_index"], 3)


class TestV1PrecedenceOverLegacy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_v1_progress_after_legacy_updates_canonical_fields(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line("::STEP::1:Gemini Review")
        self.assertEqual(p.to_status_dict()["step_label"], "Schema review")
        line = format_event(
            build_progress_event(
                "schema_review",
                substep_id="llm_review",
                registry=self.registry,
            )
        )
        p.feed_line(line)
        status = p.to_status_dict()
        self.assertEqual(status["substep_id"], "llm_review")
        self.assertEqual(status["step_label"], "Schema review")

    def test_v1_progress_wins_over_legacy_label_semantics(self) -> None:
        """v1 is authoritative for step/substep; legacy cannot override label."""
        p = ProgressProjector(registry=self.registry)
        v1 = format_event(build_progress_event("import_tool", registry=self.registry))
        p.feed_line(v1)
        status = p.to_status_dict()
        self.assertEqual(status["step_label"], "DC import (genmcf)")
        self.assertNotEqual(status["step_label"], "DC Import Tool")


class TestStatusProjectionShape(unittest.TestCase):
    def test_schema_version_and_compat_step_field(self) -> None:
        registry = load_registry()
        p = ProgressProjector(
            registry=registry,
            run_id="r1",
            dataset="uae_population",
            metadata={"batch_job_name": "job/1", "vm_type": "n2-highmem-16"},
        )
        p.mark_starting(step_label="Preparing validation environment")
        p.feed_line("::STEP::0:Pre-Import Checks")
        status = p.to_status_dict()
        self.assertEqual(status["schema_version"], STATUS_SCHEMA_VERSION)
        self.assertEqual(status["run_id"], "r1")
        self.assertEqual(status["dataset"], "uae_population")
        self.assertEqual(status["batch_job_name"], "job/1")
        self.assertIn("started_at", status)
        self.assertIn("updated_at", status)
        validate_status_projection(status, registry=registry)

    def test_mark_starting_before_pipeline(self) -> None:
        p = ProgressProjector(registry=load_registry())
        p.mark_starting(step_label="Preparing validation environment")
        status = p.to_status_dict()
        self.assertEqual(status["status"], "starting")
        self.assertIsNone(status["step_id"])
        self.assertEqual(status["step_label"], "Preparing validation environment")

    def test_to_status_json_roundtrip(self) -> None:
        p = ProgressProjector(registry=load_registry())
        p.feed_line("::STEP::4:Results")
        parsed = json.loads(p.to_status_json(indent=None))
        self.assertEqual(parsed["step_id"], "results")


class TestLegacyAndV1ProgressEmit(unittest.TestCase):
    """Legacy ::STEP:: marker followed by v1 progress for the same step."""

    def test_legacy_then_v1_last_writer_for_substep(self) -> None:
        registry = load_registry()
        p = ProgressProjector(registry=registry)
        p.feed_line("::STEP::0:Pre-Import Checks")
        p.feed_line(
            format_event(
                build_progress_event(
                    "pre_import",
                    substep_id="csv_quality",
                    registry=registry,
                )
            )
        )
        status = p.to_status_dict()
        self.assertEqual(status["step_id"], "pre_import")
        self.assertEqual(status["substep_id"], "csv_quality")


if __name__ == "__main__":
    unittest.main()
