# Validation configs

- **new_import_config.json** — Default config used by the pipeline and Web UI for all datasets (child_birth, child_birth_fail_*, child_birth_ai_demo, custom). Defines which rules run (min value, num observations, unit consistency, etc.).
- **warn_only_rules.json** — Maps dataset IDs to rule IDs that should be treated as warnings (non-blocking) instead of blockers. Used by `run_e2e_test.sh` after import_validation. When adding a new dataset ID (e.g. in `run_e2e_test.sh` or the Web UI), add an entry to this file with the desired rule list so warn-only behavior applies.
