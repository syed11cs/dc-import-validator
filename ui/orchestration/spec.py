"""Immutable run intent for orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Tuple

RunMode = str  # "builtin" | "custom"
BUILTIN = "builtin"
CUSTOM = "custom"

BUILTIN_DATASETS = frozenset(
    {"child_birth", "statistics_poland", "finland_census", "uae_population"}
)


@dataclass(frozen=True)
class RunInputs:
    """Input file delivery for custom runs (ignored for builtin)."""

    session_id: str = ""
    tmcf_filename: str = ""
    csv_filenames: Tuple[str, ...] = ()
    stat_vars_mcf_filename: str = ""
    stat_vars_schema_mcf_filename: str = ""
    csv_total_bytes: int = 0
    tmcf_gcs_path: str = ""
    csv_gcs_paths: Tuple[str, ...] = ()
    stat_vars_mcf_gcs_path: str = ""
    stat_vars_schema_mcf_gcs_path: str = ""
    baseline_name: str = ""


@dataclass(frozen=True)
class RunRules:
    """Validation rule selection for this run."""

    rules_filter: str = ""
    skip_rules_filter: str = ""
    custom_rules: Tuple[dict[str, Any], ...] = ()
    validation_config_path: str = ""
    validation_config_url: str = ""
    merged_config_gcs_path: str = ""


@dataclass(frozen=True)
class RunOptions:
    """Pipeline/runtime options."""

    llm_review: bool = False
    llm_model: str = ""
    import_resolution_mode: str = "LOCAL"
    existence_checks: str = "false"
    processing_mode: str = "auto"
    java_threads: int = 0
    machine_type_override: str = ""


@dataclass(frozen=True)
class RunSpec:
    """Canonical immutable run intent for a validation run."""

    run_id: str
    mode: RunMode
    dataset_id: str
    inputs: RunInputs = field(default_factory=RunInputs)
    rules: RunRules = field(default_factory=RunRules)
    options: RunOptions = field(default_factory=RunOptions)

    def __post_init__(self) -> None:
        if not self.run_id or not self.run_id.strip():
            raise ValueError("run_id is required")
        if self.mode not in (BUILTIN, CUSTOM):
            raise ValueError(f"mode must be {BUILTIN!r} or {CUSTOM!r}, got {self.mode!r}")
        if not self.dataset_id or not self.dataset_id.strip():
            raise ValueError("dataset_id is required")
        if self.mode == BUILTIN and self.dataset_id not in BUILTIN_DATASETS:
            raise ValueError(f"unknown builtin dataset_id: {self.dataset_id!r}")
        if self.mode == CUSTOM and self.dataset_id != "custom":
            raise ValueError("custom mode requires dataset_id='custom'")

    @property
    def is_builtin(self) -> bool:
        return self.mode == BUILTIN

    @property
    def is_custom(self) -> bool:
        return self.mode == CUSTOM


def run_spec_from_mapping(data: Mapping[str, Any]) -> RunSpec:
    """Build RunSpec from a plain dict (tests and HTTP request mapping)."""
    inputs_raw = data.get("inputs") or {}
    rules_raw = data.get("rules") or {}
    options_raw = data.get("options") or {}

    def _tuple_strs(key: str, source: Mapping[str, Any]) -> tuple[str, ...]:
        val = source.get(key, ())
        if val is None:
            return ()
        if isinstance(val, str):
            return (val,)
        return tuple(str(x) for x in val)

    inputs = RunInputs(
        session_id=str(inputs_raw.get("session_id") or ""),
        tmcf_filename=str(inputs_raw.get("tmcf_filename") or ""),
        csv_filenames=_tuple_strs("csv_filenames", inputs_raw),
        stat_vars_mcf_filename=str(inputs_raw.get("stat_vars_mcf_filename") or ""),
        stat_vars_schema_mcf_filename=str(
            inputs_raw.get("stat_vars_schema_mcf_filename") or ""
        ),
        csv_total_bytes=int(inputs_raw.get("csv_total_bytes") or 0),
        tmcf_gcs_path=str(inputs_raw.get("tmcf_gcs_path") or ""),
        csv_gcs_paths=_tuple_strs("csv_gcs_paths", inputs_raw),
        stat_vars_mcf_gcs_path=str(inputs_raw.get("stat_vars_mcf_gcs_path") or ""),
        stat_vars_schema_mcf_gcs_path=str(
            inputs_raw.get("stat_vars_schema_mcf_gcs_path") or ""
        ),
        baseline_name=str(inputs_raw.get("baseline_name") or ""),
    )

    custom_rules_raw = rules_raw.get("custom_rules") or ()
    custom_rules = tuple(custom_rules_raw) if isinstance(custom_rules_raw, list) else ()

    rules = RunRules(
        rules_filter=str(rules_raw.get("rules_filter") or ""),
        skip_rules_filter=str(rules_raw.get("skip_rules_filter") or ""),
        custom_rules=custom_rules,
        validation_config_path=str(rules_raw.get("validation_config_path") or ""),
        validation_config_url=str(rules_raw.get("validation_config_url") or ""),
        merged_config_gcs_path=str(rules_raw.get("merged_config_gcs_path") or ""),
    )

    options = RunOptions(
        llm_review=bool(options_raw.get("llm_review", False)),
        llm_model=str(options_raw.get("llm_model") or ""),
        import_resolution_mode=str(options_raw.get("import_resolution_mode") or "LOCAL"),
        existence_checks=str(options_raw.get("existence_checks") or "false"),
        processing_mode=str(options_raw.get("processing_mode") or "auto"),
        java_threads=int(options_raw.get("java_threads") or 0),
        machine_type_override=str(options_raw.get("machine_type_override") or ""),
    )

    return RunSpec(
        run_id=str(data["run_id"]).strip(),
        mode=str(data["mode"]).strip(),
        dataset_id=str(data["dataset_id"]).strip(),
        inputs=inputs,
        rules=rules,
        options=options,
    )
