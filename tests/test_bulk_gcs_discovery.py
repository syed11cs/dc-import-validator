"""Tests for GCS bulk folder discovery (ui/bulk_gcs_discovery.py)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.bulk_gcs_discovery import (
    bulk_rules_for_submit,
    classify_folder_objects,
    clamp_bulk_parallelism,
    discover_datasets_under_root,
    list_immediate_child_prefixes,
    normalize_dataset_id,
    parse_gs_root,
    resolve_validation_config_url,
    sanitize_dataset_id,
)


class TestParseGsRoot(unittest.TestCase):
    def test_trailing_slash_normalized(self) -> None:
        bucket, prefix = parse_gs_root("gs://my-bucket/imports/")
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(prefix, "imports/")

    def test_rejects_non_gs(self) -> None:
        with self.assertRaises(ValueError):
            parse_gs_root("https://example.com/x")


class TestNormalizeDatasetId(unittest.TestCase):
    def test_underscore_folder(self) -> None:
        self.assertEqual(normalize_dataset_id("dataset_a"), "dataset-a")

    def test_human_readable_folder(self) -> None:
        self.assertEqual(
            normalize_dataset_id("Birth Dataset 2024"),
            "birth-dataset-2024",
        )

    def test_short_name_gets_stable_fallback(self) -> None:
        out = normalize_dataset_id("ab")
        self.assertIsNotNone(out)
        assert out is not None
        self.assertTrue(out.startswith("ds-"))
        self.assertEqual(len(out), 11)

    def test_sanitize_alias(self) -> None:
        self.assertEqual(sanitize_dataset_id("dataset_a"), "dataset-a")


class TestClampParallelism(unittest.TestCase):
    def test_defaults_and_cap(self) -> None:
        self.assertEqual(clamp_bulk_parallelism(0), 1)
        self.assertEqual(clamp_bulk_parallelism(-3), 1)
        self.assertEqual(clamp_bulk_parallelism(3), 3)
        self.assertEqual(clamp_bulk_parallelism(99, maximum=5), 5)


class TestBulkRulesForSubmit(unittest.TestCase):
    def test_folder_config_clears_sidebar_rules(self) -> None:
        url, rules, custom = bulk_rules_for_submit(
            "gs://b/f/validation_config.json",
            "gs://b/global.json",
            "check_min_value",
            [{"rule_id": "x"}],
        )
        self.assertEqual(url, "gs://b/f/validation_config.json")
        self.assertEqual(rules, "")
        self.assertEqual(custom, [])

    def test_request_url_clears_sidebar_rules(self) -> None:
        url, rules, custom = bulk_rules_for_submit(
            "",
            "gs://b/global.json",
            "check_min_value",
            [{"rule_id": "x"}],
        )
        self.assertEqual(url, "gs://b/global.json")
        self.assertEqual(rules, "")
        self.assertEqual(custom, [])

    def test_sidebar_merge_when_no_override(self) -> None:
        url, rules, custom = bulk_rules_for_submit("", "", "a,b", [{"rule_id": "c"}])
        self.assertEqual(url, "")
        self.assertEqual(rules, "a,b")
        self.assertEqual(len(custom), 1)


class TestResolveValidationConfig(unittest.TestCase):
    def test_folder_config_wins(self) -> None:
        url = resolve_validation_config_url(
            "gs://b/imports/a/validation_config.json",
            "gs://b/global.json",
        )
        self.assertEqual(url, "gs://b/imports/a/validation_config.json")

    def test_request_fallback(self) -> None:
        url = resolve_validation_config_url("", "gs://b/global.json")
        self.assertEqual(url, "gs://b/global.json")


class TestClassifyFolder(unittest.TestCase):
    def test_valid_minimal_folder(self) -> None:
        prefix = "imports/dataset_a/"
        names = [
            "imports/dataset_a/data.csv",
            "imports/dataset_a/data.tmcf",
        ]
        ds, err = classify_folder_objects(
            "b", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(err)
        assert ds is not None
        self.assertEqual(ds.dataset_id, "dataset-a")

    def test_birth_dataset_folder_name(self) -> None:
        prefix = "imports/Birth Dataset 2024/"
        names = [
            "imports/Birth Dataset 2024/data.csv",
            "imports/Birth Dataset 2024/data.tmcf",
        ]
        ds, err = classify_folder_objects(
            "b", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(err)
        assert ds is not None
        self.assertEqual(ds.dataset_id, "birth-dataset-2024")

    def test_nested_files_not_counted(self) -> None:
        prefix = "imports/dataset_a/"
        names = [
            "imports/dataset_a/data.csv",
            "imports/dataset_a/data.tmcf",
            "imports/dataset_a/sub/extra.csv",
        ]
        ds, err = classify_folder_objects(
            "b", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(err)
        assert ds is not None

    def test_with_validation_config_and_mcf(self) -> None:
        prefix = "imports/dataset_b/"
        names = [
            "imports/dataset_b/out.csv",
            "imports/dataset_b/import.tmcf",
            "imports/dataset_b/validation_config.json",
            "imports/dataset_b/stat_vars.mcf",
            "imports/dataset_b/schema.mcf",
        ]
        ds, err = classify_folder_objects(
            "bucket", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(err)
        assert ds is not None
        self.assertTrue(ds.validation_config_gcs_path.endswith("validation_config.json"))

    def test_missing_csv_skipped(self) -> None:
        ds, err = classify_folder_objects(
            "b",
            "imports/bad/",
            ["imports/bad/only.tmcf"],
            root_prefix="imports/",
        )
        self.assertIsNone(ds)
        self.assertIn("missing CSV", err or "")


class TestListImmediateChildPrefixes(unittest.TestCase):
    def test_uses_delimiter_for_non_recursive_listing(self) -> None:
        captured: dict = {}

        def fake_list_blobs(**kwargs):
            captured.update(kwargs)
            it = MagicMock()
            it.prefixes = ["imports/a/", "imports/b/"]
            return it

        prefixes = list_immediate_child_prefixes(
            "bucket",
            "imports/",
            list_blobs_fn=fake_list_blobs,
        )
        self.assertEqual(captured.get("delimiter"), "/")
        self.assertEqual(captured.get("prefix"), "imports/")
        self.assertEqual(prefixes, ["imports/a/", "imports/b/"])


class TestDiscoverUnderRoot(unittest.TestCase):
    def test_mixed_valid_invalid_folders(self) -> None:
        def list_children(_bucket: str, _root: str) -> list[str]:
            return ["imports/dataset_ok/", "imports/no_csv/", "imports/Birth Dataset 2024/"]

        def list_objects(_bucket: str, prefix: str) -> list[tuple[str, int]]:
            if prefix == "imports/dataset_ok/":
                return [
                    ("imports/dataset_ok/data.csv", 100),
                    ("imports/dataset_ok/data.tmcf", 50),
                ]
            if prefix == "imports/Birth Dataset 2024/":
                return [
                    ("imports/Birth Dataset 2024/out.csv", 200),
                    ("imports/Birth Dataset 2024/out.tmcf", 80),
                ]
            return [("imports/no_csv/x.tmcf", 10)]

        found, skipped = discover_datasets_under_root(
            "gs://bucket/imports/",
            list_child_prefixes_fn=list_children,
            list_objects_fn=list_objects,
        )
        self.assertEqual(len(found), 2)
        ids = {d.dataset_id for d in found}
        self.assertIn("dataset-ok", ids)
        self.assertIn("birth-dataset-2024", ids)
        self.assertEqual(len(skipped), 1)
        self.assertIn("CSV", skipped[0].reason)


if __name__ == "__main__":
    unittest.main()
