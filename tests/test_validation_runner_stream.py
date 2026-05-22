"""Unit tests for projector-driven streaming in validation_runner.py."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.progress import build_failure_event, build_progress_event, format_event
from pipeline.projector import ProgressProjector
from pipeline.registry import load_registry
from ui.services.validation_runner import (
    _failure_dict_from_state,
    _infer_step_fallback,
    _maybe_emit_step_ndjson,
    _new_projector,
    _normalize_failure_event,
    _step_payload_from_status,
    _strip_ansi,
)


class TestNormalizeFailureEvent(unittest.TestCase):
    def test_v1_failure(self) -> None:
        event = {
            "v": 1,
            "t": "failure",
            "failure_code": "CSV_SPLIT_FAILED",
            "step_id": "pre_import",
            "step_index": 0,
            "step_label": "Pre-import checks",
            "message": "no shards",
            "substep_id": "csv_split",
        }
        out = _normalize_failure_event(event)
        assert out is not None
        self.assertEqual(out["code"], "CSV_SPLIT_FAILED")
        self.assertEqual(out["step"], 0)
        self.assertEqual(out["step_id"], "pre_import")

    def test_legacy_failure(self) -> None:
        out = _normalize_failure_event(
            {"t": "failure", "code": "PREFLIGHT_FAILED", "step": 0, "message": "x"}
        )
        assert out is not None
        self.assertEqual(out["code"], "PREFLIGHT_FAILED")


class TestProjectorDrivenStepEmit(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()
        cls.projector = ProgressProjector(registry=cls.registry)

    def test_legacy_then_v1_single_step_advance(self) -> None:
        p = ProgressProjector(registry=self.registry)
        last = None
        lines = []
        p.feed_line("::STEP::1:Gemini Review")
        line, last = _maybe_emit_step_ndjson(p, last)
        if line:
            lines.append(json.loads(line))
        p.feed_line(
            format_event(build_progress_event("schema_review", registry=self.registry))
        )
        line, last = _maybe_emit_step_ndjson(p, last)
        self.assertIsNone(line)
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0]["step"], 1)
        self.assertEqual(lines[0]["label"], "Schema review")
        self.assertEqual(lines[0]["step_id"], "schema_review")

    def test_baseline_diff_emits_step_index_three(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line("::STEP::2.4:Differ")
        line, _ = _maybe_emit_step_ndjson(p, None)
        assert line is not None
        payload = json.loads(line)
        self.assertEqual(payload["step"], 3)
        self.assertEqual(payload["step_id"], "baseline_diff")

    def test_monotonic_emit_skips_backward_projector_state(self) -> None:
        p = ProgressProjector(registry=self.registry)
        last = None
        p.feed_line("::STEP::2.4:Differ")
        line, last = _maybe_emit_step_ndjson(p, last)
        assert line is not None
        self.assertEqual(last, 3)
        p.feed_line("::STEP::2:DC Import Tool")
        line2, last2 = _maybe_emit_step_ndjson(p, last)
        self.assertIsNone(line2)
        self.assertEqual(last2, 3)

    def test_v1_failure_updates_state_for_done(self) -> None:
        p = ProgressProjector(registry=self.registry)
        p.feed_line(
            format_event(
                build_failure_event(
                    "pre_import",
                    "CSV_SPLIT_FAILED",
                    "split failed",
                    substep_id="csv_split",
                    registry=self.registry,
                )
            )
        )
        failure = _failure_dict_from_state(p.state)
        assert failure is not None
        self.assertEqual(failure["code"], "CSV_SPLIT_FAILED")
        self.assertEqual(failure["step"], 0)
        self.assertEqual(failure["step_id"], "pre_import")


class TestInferStepFallback(unittest.TestCase):
    def test_no_gemini_blocking_heuristic(self) -> None:
        step, _ = _infer_step_fallback("Gemini review found issues in schema")
        self.assertIsNone(step)

    def test_genmcf_log_fallback(self) -> None:
        step, _ = _infer_step_fallback("Running dc-import genmcf...")
        self.assertEqual(step, 2)

    def test_step_24_log_does_not_match_step_2(self) -> None:
        step, _ = _infer_step_fallback("Step 2.4: Differ complete → /tmp/diff")
        self.assertEqual(step, 3)

    def test_step_2_completed_log_matches_import_tool(self) -> None:
        step, _ = _infer_step_fallback("Step 2 completed in 42s")
        self.assertEqual(step, 2)

class TestNewProjector(unittest.TestCase):
    def test_loads_registry_from_app_root(self) -> None:
        p = _new_projector(ROOT)
        self.assertEqual(p.registry.step_count, 6)


class TestStripAnsi(unittest.TestCase):
    def test_strips_color_codes(self) -> None:
        self.assertEqual(_strip_ansi("\x1b[32mhello\x1b[0m"), "hello")


if __name__ == "__main__":
    unittest.main()
