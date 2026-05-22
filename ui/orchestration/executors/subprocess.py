"""Thin wrapper around ui.services.validation_runner (local subprocess spine)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ui.orchestration.paths import RunPaths, resolve_run_paths
from ui.orchestration.spec import BUILTIN, RunSpec


def default_app_root() -> Path:
    """Repo root (ui/orchestration/executors/ → four parents)."""
    return Path(__file__).resolve().parent.parent.parent.parent


@dataclass(frozen=True)
class SubprocessRunPlan:
    """Arguments prepared for validation_runner.run_validation_process."""

    args: tuple[str, ...]
    app_root: Path
    output_dir: Path
    canonical_output_dir: Path
    dataset: str
    extra_env: dict[str, str]
    extra_cleanup_dirs: tuple[Path, ...]
    extra_done_fields: dict[str, str]
    stream: bool = True


class SubprocessExecutor:
    """Build subprocess invocation plans — does not execute (server remains caller)."""

    def __init__(self, app_root: Path | None = None) -> None:
        self.app_root = app_root or default_app_root()

    def e2e_script_path(self) -> Path:
        return self.app_root / "run_e2e_test.sh"

    def build_args(self, spec: RunSpec, *, config_path: str | None = None) -> list[str]:
        """Build run_e2e_test.sh argv from a RunSpec."""
        script = self.e2e_script_path()
        if spec.is_builtin:
            args: list[str] = ["bash", str(script), spec.dataset_id]
        else:
            args = ["bash", str(script), "custom"]
            if spec.inputs.tmcf_gcs_path:
                args.append(f"--tmcf={spec.inputs.tmcf_gcs_path}")
                for uri in spec.inputs.csv_gcs_paths:
                    args.append(f"--csv={uri}")
            else:
                paths = resolve_run_paths("custom", spec.run_id, app_root=self.app_root)
                base = paths.output_dir
                if spec.inputs.tmcf_filename:
                    args.append(f"--tmcf={base / spec.inputs.tmcf_filename}")
                for name in spec.inputs.csv_filenames:
                    args.append(f"--csv={base / name}")
                if spec.inputs.stat_vars_mcf_filename:
                    args.append(f"--stat-vars-mcf={base / spec.inputs.stat_vars_mcf_filename}")
                if spec.inputs.stat_vars_schema_mcf_filename:
                    args.append(
                        f"--stat-vars-schema-mcf={base / spec.inputs.stat_vars_schema_mcf_filename}"
                    )
            if spec.inputs.baseline_name:
                args.append(f"--baseline-name={spec.inputs.baseline_name}")

        if config_path:
            args.append(f"--config={config_path}")
        if spec.options.llm_review:
            args.append("--llm-review")
            if spec.options.llm_model:
                args.append(f"--model={spec.options.llm_model}")
        else:
            args.append("--no-llm-review")
        if spec.rules.rules_filter:
            args.append(f"--rules={spec.rules.rules_filter}")
        if spec.rules.skip_rules_filter:
            args.append(f"--skip-rules={spec.rules.skip_rules_filter}")
        return args

    def build_extra_env(self, spec: RunSpec) -> dict[str, str]:
        env: dict[str, str] = {
            "RUN_ID": spec.run_id,
            "BASELINE_AUTO_UPDATE": "false",
        }
        if spec.options.import_resolution_mode:
            env["IMPORT_RESOLUTION_MODE"] = spec.options.import_resolution_mode
        if spec.options.existence_checks:
            env["IMPORT_EXISTENCE_CHECKS"] = spec.options.existence_checks
        return env

    def plan_run(
        self,
        spec: RunSpec,
        *,
        config_path: str | None = None,
        stream: bool = True,
        extra_cleanup_dirs: list[Path] | None = None,
    ) -> SubprocessRunPlan:
        paths = resolve_run_paths(spec.dataset_id, spec.run_id, app_root=self.app_root)
        cleanup: list[Path] = list(extra_cleanup_dirs or [])
        if paths.custom_upload_dir and paths.custom_upload_dir not in cleanup:
            cleanup.append(paths.custom_upload_dir)
        done_fields: dict[str, str] = {}
        if spec.is_custom and spec.inputs.baseline_name:
            done_fields["baseline_id"] = spec.inputs.baseline_name
        return SubprocessRunPlan(
            args=tuple(self.build_args(spec, config_path=config_path)),
            app_root=self.app_root,
            output_dir=paths.output_dir,
            canonical_output_dir=paths.canonical_output_dir,
            dataset=spec.dataset_id if spec.mode == BUILTIN else "custom",
            extra_env=self.build_extra_env(spec),
            extra_cleanup_dirs=tuple(cleanup),
            extra_done_fields=done_fields,
            stream=stream,
        )

    async def run(
        self,
        plan: SubprocessRunPlan,
        request: Any,
        config_path: Path | None,
        **kwargs: Any,
    ) -> Any:
        """Delegate to validation_runner (existing implementation)."""
        from ui.services.validation_runner import run_validation_process

        return await run_validation_process(
            list(plan.args),
            request,
            config_path,
            stream=plan.stream,
            app_root=plan.app_root,
            output_dir=plan.output_dir,
            dataset=plan.dataset,
            canonical_output_dir=plan.canonical_output_dir,
            extra_cleanup_dirs=list(plan.extra_cleanup_dirs) or None,
            extra_done_fields=plan.extra_done_fields or None,
            extra_env=plan.extra_env,
            **kwargs,
        )
