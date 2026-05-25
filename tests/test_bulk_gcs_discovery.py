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
    def test_valid_output_pair_with_extra_csvs(self) -> None:
        """Extra non-output CSVs are ignored."""
        prefix = "imports/dataset_a/"
        names = [
            "imports/dataset_a/raw.csv",
            "imports/dataset_a/notes.csv",
            "imports/dataset_a/sample_output.csv",
            "imports/dataset_a/sample_output.tmcf",
        ]
        ds, err = classify_folder_objects(
            "b", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(err)
        assert ds is not None
        self.assertEqual(ds.dataset_id, "dataset-a")
        self.assertTrue(ds.csv_gcs_path.endswith("sample_output.csv"))
        self.assertTrue(ds.tmcf_gcs_path.endswith("sample_output.tmcf"))

    def test_birth_dataset_folder_name(self) -> None:
        prefix = "imports/Birth Dataset 2024/"
        names = [
            "imports/Birth Dataset 2024/birth_output.csv",
            "imports/Birth Dataset 2024/birth_output.tmcf",
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
            "imports/dataset_a/sample_output.csv",
            "imports/dataset_a/sample_output.tmcf",
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
            "imports/dataset_b/run_output.csv",
            "imports/dataset_b/run_output.tmcf",
            "imports/dataset_b/validation_config.json",
            "imports/dataset_b/run_output_stat_vars.mcf",
            "imports/dataset_b/run_output_stat_vars_schema.mcf",
            "imports/dataset_b/debug.csv",
        ]
        ds, err = classify_folder_objects(
            "bucket", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(err)
        assert ds is not None
        self.assertTrue(ds.validation_config_gcs_path.endswith("validation_config.json"))
        self.assertTrue(ds.stat_vars_mcf_gcs_path.endswith("run_output_stat_vars.mcf"))
        self.assertTrue(
            ds.stat_vars_schema_mcf_gcs_path.endswith("run_output_stat_vars_schema.mcf")
        )

    def test_statistics_poland_auxiliary_mcf(self) -> None:
        prefix = "imports/statistics_poland/"
        names = [
            "imports/statistics_poland/StatisticsPoland_output.csv",
            "imports/statistics_poland/StatisticsPoland_output.tmcf",
            "imports/statistics_poland/StatisticsPoland_output_stat_vars.mcf",
            "imports/statistics_poland/StatisticsPoland_output_stat_vars_schema.mcf",
        ]
        ds, err = classify_folder_objects(
            "bucket", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(err)
        assert ds is not None
        self.assertTrue(
            ds.stat_vars_mcf_gcs_path.endswith("StatisticsPoland_output_stat_vars.mcf")
        )
        self.assertTrue(
            ds.stat_vars_schema_mcf_gcs_path.endswith(
                "StatisticsPoland_output_stat_vars_schema.mcf"
            )
        )

    def test_literal_stat_vars_mcf_suffix(self) -> None:
        prefix = "imports/dataset_c/"
        names = [
            "imports/dataset_c/sample_output.csv",
            "imports/dataset_c/sample_output.tmcf",
            "imports/dataset_c/stat_vars.mcf",
            "imports/dataset_c/stat_vars_schema.mcf",
        ]
        ds, err = classify_folder_objects("b", prefix, names, root_prefix="imports/")
        self.assertIsNone(err)
        assert ds is not None
        self.assertTrue(ds.stat_vars_mcf_gcs_path.endswith("stat_vars.mcf"))
        self.assertTrue(ds.stat_vars_schema_mcf_gcs_path.endswith("stat_vars_schema.mcf"))

    def test_multiple_stat_vars_mcf_invalid(self) -> None:
        prefix = "imports/dataset_a/"
        names = [
            "imports/dataset_a/sample_output.csv",
            "imports/dataset_a/sample_output.tmcf",
            "imports/dataset_a/a_stat_vars.mcf",
            "imports/dataset_a/b_stat_vars.mcf",
        ]
        ds, err = classify_folder_objects("b", prefix, names, root_prefix="imports/")
        self.assertIsNone(ds)
        self.assertIn("multiple *_stat_vars.mcf", err or "")

    def test_unrelated_mcf_ignored(self) -> None:
        prefix = "imports/dataset_a/"
        names = [
            "imports/dataset_a/sample_output.csv",
            "imports/dataset_a/sample_output.tmcf",
            "imports/dataset_a/schema.mcf",
            "imports/dataset_a/import.tmcf",
        ]
        ds, err = classify_folder_objects("b", prefix, names, root_prefix="imports/")
        self.assertIsNone(err)
        assert ds is not None
        self.assertEqual(ds.stat_vars_mcf_gcs_path, "")
        self.assertEqual(ds.stat_vars_schema_mcf_gcs_path, "")

    def test_missing_output_tmcf(self) -> None:
        ds, err = classify_folder_objects(
            "b",
            "imports/bad/",
            ["imports/bad/legacy.tmcf", "imports/bad/data.csv"],
            root_prefix="imports/",
        )
        self.assertIsNone(ds)
        self.assertIn("*_output.tmcf", err or "")

    def test_missing_matching_output_csv(self) -> None:
        ds, err = classify_folder_objects(
            "b",
            "imports/bad/",
            ["imports/bad/sample_output.tmcf"],
            root_prefix="imports/",
        )
        self.assertIsNone(ds)
        self.assertIn("no matching *_output.csv", err or "")

    def test_multiple_output_tmcf_invalid(self) -> None:
        prefix = "imports/dataset_a/"
        names = [
            "imports/dataset_a/a_output.tmcf",
            "imports/dataset_a/b_output.tmcf",
            "imports/dataset_a/a_output.csv",
        ]
        ds, err = classify_folder_objects(
            "b", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(ds)
        self.assertIn("multiple *_output.tmcf", err or "")

    def test_multiple_output_csv_invalid(self) -> None:
        prefix = "imports/dataset_a/"
        names = [
            "imports/dataset_a/a_output.csv",
            "imports/dataset_a/b_output.csv",
            "imports/dataset_a/sample_output.tmcf",
        ]
        ds, err = classify_folder_objects(
            "b", prefix, names, root_prefix="imports/"
        )
        self.assertIsNone(ds)
        self.assertIn("multiple *_output.csv", err or "")

    def test_non_output_tmcf_ignored_requires_output_tmcf(self) -> None:
        ds, err = classify_folder_objects(
            "b",
            "imports/bad/",
            ["imports/bad/data.csv", "imports/bad/import.tmcf"],
            root_prefix="imports/",
        )
        self.assertIsNone(ds)
        self.assertIn("*_output.tmcf", err or "")


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
                    ("imports/dataset_ok/run_output.csv", 100),
                    ("imports/dataset_ok/run_output.tmcf", 50),
                    ("imports/dataset_ok/raw.csv", 999),
                ]
            if prefix == "imports/Birth Dataset 2024/":
                return [
                    ("imports/Birth Dataset 2024/birth_output.csv", 200),
                    ("imports/Birth Dataset 2024/birth_output.tmcf", 80),
                ]
            return [("imports/no_csv/legacy.tmcf", 10)]

        found, skipped = discover_datasets_under_root(
            "gs://bucket/imports/",
            list_child_prefixes_fn=list_children,
            list_objects_fn=list_objects,
        )
        self.assertEqual(len(found), 2)
        ids = {d.dataset_id for d in found}
        self.assertIn("dataset-ok", ids)
        self.assertIn("birth-dataset-2024", ids)
        ok_ds = next(d for d in found if d.dataset_id == "dataset-ok")
        self.assertEqual(ok_ds.csv_total_bytes, 100)
        self.assertEqual(len(skipped), 1)
        self.assertIn("*_output.tmcf", skipped[0].reason)


if __name__ == "__main__":
    unittest.main()
