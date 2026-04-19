"""Unit tests for validation orchestration correctness.

Covers:
  - _normalize_custom_rule: description defaulting, unknown key stripping, rule_id contract
  - _validate_custom_rules: rule_id required, early rejection
  - _create_merged_config: template-valid output, description defaults, built-in rules unchanged
  - run_validation._run_dc_runner partial results: synthetic FAILED injection for missing rules

Run with:
    python tests/test_validation_pipeline.py
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from scripts.validate_config_template import _validate_config
from ui.server import _create_merged_config, _normalize_custom_rule, _validate_custom_rules

# Minimal valid custom rule as sent by the browser (description deliberately absent).
# Uses the real runtime table (stats) and Option A style (query + condition).
_RULE_FROM_CLIENT = {
    "rule_id": "custom_sql_abc1",
    "validator": "SQL_VALIDATOR",
    "scope": {"data_source": "stats"},
    "params": {"query": "SELECT StatVar, MinValue FROM stats", "condition": "MinValue >= 0"},
}


# ─── _validate_custom_rules ───────────────────────────────────────────────────

class TestValidateCustomRules(unittest.TestCase):

    def _valid_rule(self, rule_id="custom_sql_abc1"):
        return {
            "rule_id": rule_id,
            "params": {"query": "SELECT StatVar, MinValue FROM stats", "condition": "MinValue >= 0"},
        }

    def test_missing_rule_id_is_rejected(self):
        rule = self._valid_rule()
        del rule["rule_id"]
        self.assertIsNotNone(_validate_custom_rules([rule]))

    def test_empty_rule_id_is_rejected(self):
        err = _validate_custom_rules([dict(self._valid_rule(), rule_id="")])
        self.assertIsNotNone(err)
        self.assertIn("rule_id", err)

    def test_whitespace_only_rule_id_is_rejected(self):
        self.assertIsNotNone(_validate_custom_rules([dict(self._valid_rule(), rule_id="   ")]))

    def test_valid_rule_passes(self):
        self.assertIsNone(_validate_custom_rules([self._valid_rule()]))

    def test_error_references_correct_index(self):
        rules = [self._valid_rule("custom_sql_ok"), {"rule_id": "", "params": {"query": "x", "condition": "y"}}]
        err = _validate_custom_rules(rules)
        self.assertIsNotNone(err)
        self.assertIn("[1]", err)


# ─── _normalize_custom_rule ───────────────────────────────────────────────────

class TestNormalizeCustomRule(unittest.TestCase):

    def test_adds_description_when_missing(self):
        self.assertEqual(
            _normalize_custom_rule(_RULE_FROM_CLIENT)["description"],
            "Custom SQL rule: custom_sql_abc1",
        )

    def test_preserves_existing_description(self):
        self.assertEqual(
            _normalize_custom_rule(dict(_RULE_FROM_CLIENT, description="My rule"))["description"],
            "My rule",
        )

    def test_defaults_validator_when_absent(self):
        rule = {k: v for k, v in _RULE_FROM_CLIENT.items() if k != "validator"}
        self.assertEqual(_normalize_custom_rule(rule)["validator"], "SQL_VALIDATOR")

    def test_defaults_scope_when_absent(self):
        rule = {k: v for k, v in _RULE_FROM_CLIENT.items() if k != "scope"}
        self.assertEqual(_normalize_custom_rule(rule)["scope"], {"data_source": "stats"})

    def test_preserves_enabled_flag(self):
        self.assertFalse(_normalize_custom_rule(dict(_RULE_FROM_CLIENT, enabled=False))["enabled"])

    def test_enabled_absent_when_not_in_source(self):
        self.assertNotIn("enabled", _normalize_custom_rule(_RULE_FROM_CLIENT))

    def test_no_unknown_keys_in_output(self):
        rule = dict(_RULE_FROM_CLIENT, some_extra="dropped")
        normalized = _normalize_custom_rule(rule)
        allowed = {"rule_id", "description", "validator", "scope", "params", "enabled"}
        self.assertTrue(set(normalized.keys()).issubset(allowed))

    def test_missing_rule_id_raises(self):
        # _normalize_custom_rule assumes rule_id was already validated; KeyError is
        # the signal that the caller skipped _validate_custom_rules.
        rule = {k: v for k, v in _RULE_FROM_CLIENT.items() if k != "rule_id"}
        with self.assertRaises(KeyError):
            _normalize_custom_rule(rule)


# ─── _create_merged_config ───────────────────────────────────────────────────

class TestCreateMergedConfig(unittest.TestCase):

    def _make_rule(self, rule_id="custom_sql_abc1"):
        return dict(_RULE_FROM_CLIENT, rule_id=rule_id)

    def test_merged_config_passes_template_validator(self):
        path = _create_merged_config("custom", [], [self._make_rule()])
        self.assertIsNotNone(path)
        try:
            with open(path) as f:
                errors = _validate_config(json.load(f), str(path))
            self.assertEqual(errors, [], "\n".join(errors))
        finally:
            path.unlink(missing_ok=True)

    def test_all_required_fields_present(self):
        required = {"rule_id", "description", "validator", "scope", "params"}
        rules = [self._make_rule(f"custom_sql_{i:04d}") for i in range(3)]
        path = _create_merged_config("custom", [], rules)
        self.assertIsNotNone(path)
        try:
            with open(path) as f:
                config = json.load(f)
            custom = [r for r in config["rules"] if r.get("rule_id", "").startswith("custom_sql_")]
            self.assertEqual(len(custom), 3)
            for r in custom:
                self.assertEqual(required - set(r.keys()), set(), r.get("rule_id"))
        finally:
            path.unlink(missing_ok=True)

    def test_description_defaults_to_rule_id_label(self):
        path = _create_merged_config("custom", [], [self._make_rule("custom_sql_abc1")])
        self.assertIsNotNone(path)
        try:
            with open(path) as f:
                rule = next(r for r in json.load(f)["rules"] if r.get("rule_id") == "custom_sql_abc1")
            self.assertEqual(rule["description"], "Custom SQL rule: custom_sql_abc1")
        finally:
            path.unlink(missing_ok=True)

    def test_builtin_rules_unchanged(self):
        path = _create_merged_config("custom", [], [self._make_rule()])
        self.assertIsNotNone(path)
        try:
            with open(path) as f:
                merged = json.load(f)
            builtin_path = ROOT / "validation_configs" / "new_import_config.json"
            with open(builtin_path) as f:
                original = json.load(f)
            for orig in original["rules"]:
                match = next((r for r in merged["rules"] if r.get("rule_id") == orig["rule_id"]), None)
                self.assertIsNotNone(match, orig["rule_id"])
                self.assertEqual(match, orig, orig["rule_id"])
        finally:
            path.unlink(missing_ok=True)

    def test_builtin_config_passes_template_validator(self):
        builtin_path = ROOT / "validation_configs" / "new_import_config.json"
        with open(builtin_path) as f:
            config = json.load(f)
        errors = _validate_config(config, str(builtin_path))
        self.assertEqual(errors, [], "\n".join(errors))

    def test_returns_none_when_no_modifications(self):
        self.assertIsNone(_create_merged_config("custom", [], []))


# ─── run_validation partial results ──────────────────────────────────────────

class TestPartialResultsInjection(unittest.TestCase):
    """Verify that when the DC runner produces fewer results than rules,
    synthetic FAILED entries are injected for every missing rule."""

    def _run_main_with_partial_dc(self, dc_rules, dc_results_returned):
        """
        Exercise the partial-results branch of run_validation.main() by patching
        _run_dc_runner to return fewer results than rules, then checking the
        written validation_output.json.
        """
        import scripts.run_validation as rv

        config = {
            "schema_version": "1.0",
            "rules": dc_rules,
        }

        with tempfile.TemporaryDirectory() as td:
            config_path = Path(td) / "config.json"
            output_path = Path(td) / "validation_output.json"
            config_path.write_text(json.dumps(config))

            with patch.object(rv, "_run_dc_runner", return_value=dc_results_returned), \
                 patch.object(rv, "_run_custom_validators", return_value=[]), \
                 patch.dict("os.environ", {"DATA_REPO": td}):
                # Patch os.path.isdir so DATA_REPO check passes
                with patch("os.path.isdir", return_value=True):
                    sys.argv = [
                        "run_validation.py",
                        f"--validation_config={config_path}",
                        f"--validation_output={output_path}",
                    ]
                    exit_code = rv.main()

            results = json.loads(output_path.read_text())
            return exit_code, results

    def _make_dc_rule(self, rule_id):
        return {
            "rule_id": rule_id,
            "description": f"Built-in rule {rule_id}",
            "validator": "MIN_VALUE_CHECK",
            "scope": {"data_source": "stats"},
            "params": {"minimum": 0},
        }

    def test_missing_rule_gets_failed_entry(self):
        """DC runner returns 0 of 1 expected results → 1 synthetic FAILED injected."""
        dc_rules = [self._make_dc_rule("check_min_value")]
        exit_code, results = self._run_main_with_partial_dc(dc_rules, dc_results_returned=[])

        self.assertEqual(exit_code, 1, "should fail when a rule has no result")
        failed = [r for r in results if r.get("status") == "FAILED"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["validation_name"], "check_min_value")
        self.assertIn("did not execute", failed[0]["message"])

    def test_partial_results_injects_only_missing(self):
        """DC runner returns 1 of 2 results → only the missing rule gets a FAILED entry."""
        dc_rules = [
            self._make_dc_rule("check_min_value"),
            self._make_dc_rule("check_unit_consistency"),
        ]
        partial = [{"validation_name": "check_min_value", "status": "PASSED"}]
        exit_code, results = self._run_main_with_partial_dc(dc_rules, dc_results_returned=partial)

        self.assertEqual(exit_code, 1)
        statuses = {r["validation_name"]: r["status"] for r in results}
        self.assertEqual(statuses["check_min_value"], "PASSED")
        self.assertEqual(statuses["check_unit_consistency"], "FAILED")

    def test_complete_results_not_affected(self):
        """When the DC runner returns all results (all PASSED), no synthetic entries added."""
        dc_rules = [self._make_dc_rule("check_min_value")]
        full = [{"validation_name": "check_min_value", "status": "PASSED"}]
        exit_code, results = self._run_main_with_partial_dc(dc_rules, dc_results_returned=full)

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "PASSED")

    def test_complete_results_with_failure_exits_one(self):
        """When the DC runner returns all results including a FAILED, exit code is 1."""
        dc_rules = [self._make_dc_rule("check_min_value")]
        full = [{"validation_name": "check_min_value", "status": "FAILED", "message": "too low"}]
        exit_code, results = self._run_main_with_partial_dc(dc_rules, dc_results_returned=full)

        self.assertEqual(exit_code, 1)
        self.assertEqual(results[0]["status"], "FAILED")

    def test_duplicate_validation_name_injects_missing(self):
        """Runner returns correct *count* but with a duplicate name — missing rule must still get FAILED.

        A length-only guard (len(results) < len(rules)) would pass here because counts match,
        but check_unit_consistency has no result and should be injected as FAILED.
        """
        dc_rules = [
            self._make_dc_rule("check_min_value"),
            self._make_dc_rule("check_unit_consistency"),
        ]
        # Two entries, but both for the same rule_id — counts match, IDs don't.
        duplicated = [
            {"validation_name": "check_min_value", "status": "PASSED"},
            {"validation_name": "check_min_value", "status": "PASSED"},
        ]
        exit_code, results = self._run_main_with_partial_dc(dc_rules, dc_results_returned=duplicated)

        self.assertEqual(exit_code, 1)
        statuses = {r["validation_name"]: r["status"] for r in results}
        self.assertEqual(statuses["check_min_value"], "PASSED")
        self.assertEqual(statuses["check_unit_consistency"], "FAILED")
        self.assertIn("did not execute", next(
            r["message"] for r in results if r["validation_name"] == "check_unit_consistency"
        ))


if __name__ == "__main__":
    unittest.main(verbosity=2)
