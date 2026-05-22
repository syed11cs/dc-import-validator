"""Tests for pipeline/registry.yaml and pipeline/registry.py.

Run with:
    python -m unittest tests.test_pipeline_registry
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.registry import (
    default_registry_path,
    load_registry,
    parse_registry,
    resolve_legacy_marker,
    step_by_id,
    step_by_index,
)

EXPECTED_STEP_IDS = (
    "pre_import",
    "schema_review",
    "import_tool",
    "baseline_diff",
    "validation",
    "results",
)

LEGACY_MARKERS = {
    "::STEP::0:Pre-Import Checks": "pre_import",
    "::STEP::1:Gemini Review": "schema_review",
    "::STEP::2:DC Import Tool": "import_tool",
    "::STEP::2.4:Differ": "baseline_diff",
    "::STEP::3:DC Import Validation": "validation",
    "::STEP::4:Results": "results",
}


class TestPipelineRegistry(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = load_registry()

    def test_default_path_exists(self) -> None:
        path = default_registry_path()
        self.assertTrue(path.is_file())
        self.assertEqual(path.name, "registry.yaml")

    def test_schema_version(self) -> None:
        self.assertEqual(self.registry.schema_version, "1.0")

    def test_six_steps_contiguous_indices(self) -> None:
        self.assertEqual(self.registry.step_count, 6)
        indices = [s.index for s in self.registry.steps]
        self.assertEqual(indices, list(range(6)))
        ids = [s.id for s in self.registry.steps]
        self.assertEqual(ids, list(EXPECTED_STEP_IDS))

    def test_blocking_semantics(self) -> None:
        blocking = {s.id: s.blocking for s in self.registry.steps}
        self.assertEqual(
            blocking,
            {
                "pre_import": True,
                "schema_review": False,
                "import_tool": True,
                "baseline_diff": False,
                "validation": True,
                "results": False,
            },
        )

    def test_legacy_markers_match_run_e2e(self) -> None:
        self.assertEqual(self.registry.legacy_markers, LEGACY_MARKERS)

    def test_resolve_legacy_marker_exact_lines(self) -> None:
        for marker, step_id in LEGACY_MARKERS.items():
            step = resolve_legacy_marker(self.registry, marker)
            self.assertIsNotNone(step, msg=f"marker {marker!r}")
            assert step is not None
            self.assertEqual(step.id, step_id)

    def test_resolve_legacy_marker_strips_whitespace(self) -> None:
        step = resolve_legacy_marker(self.registry, "  ::STEP::2:DC Import Tool  \n")
        self.assertIsNotNone(step)
        assert step is not None
        self.assertEqual(step.id, "import_tool")

    def test_resolve_unknown_marker_returns_none(self) -> None:
        self.assertIsNone(resolve_legacy_marker(self.registry, "::STEP::99:Unknown"))

    def test_step_by_id_and_index(self) -> None:
        step = step_by_id(self.registry, "baseline_diff")
        self.assertIsNotNone(step)
        assert step is not None
        self.assertEqual(step.index, 3)
        self.assertEqual(step_by_index(self.registry, 3), step)

    def test_substeps_unique_within_step(self) -> None:
        pre = step_by_id(self.registry, "pre_import")
        self.assertIsNotNone(pre)
        assert pre is not None
        sub_ids = [s.id for s in pre.substeps]
        self.assertEqual(len(sub_ids), len(set(sub_ids)))
        self.assertIn("csv_split", sub_ids)

    def test_parse_registry_rejects_duplicate_step_id(self) -> None:
        data = {
            "schema_version": "1.0",
            "steps": [
                {
                    "id": "a",
                    "index": 0,
                    "label": "A",
                    "blocking": True,
                    "substeps": [],
                },
                {
                    "id": "a",
                    "index": 1,
                    "label": "B",
                    "blocking": True,
                    "substeps": [],
                },
            ],
            "legacy_markers": {"::STEP::0:A": "a"},
        }
        with self.assertRaises(ValueError) as ctx:
            parse_registry(data)
        self.assertIn("duplicate step id", str(ctx.exception))

    def test_parse_registry_rejects_non_contiguous_indices(self) -> None:
        data = {
            "schema_version": "1.0",
            "steps": [
                {
                    "id": "a",
                    "index": 0,
                    "label": "A",
                    "blocking": True,
                    "substeps": [],
                },
                {
                    "id": "b",
                    "index": 2,
                    "label": "B",
                    "blocking": True,
                    "substeps": [],
                },
            ],
            "legacy_markers": {"::STEP::0:A": "a", "::STEP::2:B": "b"},
        }
        with self.assertRaises(ValueError) as ctx:
            parse_registry(data)
        self.assertIn("contiguous", str(ctx.exception))

    def test_parse_registry_rejects_invalid_legacy_target(self) -> None:
        data = {
            "schema_version": "1.0",
            "steps": [
                {
                    "id": "a",
                    "index": 0,
                    "label": "A",
                    "blocking": True,
                    "substeps": [],
                },
            ],
            "legacy_markers": {"::STEP::0:A": "missing"},
        }
        with self.assertRaises(ValueError) as ctx:
            parse_registry(data)
        self.assertIn("unknown step id", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
