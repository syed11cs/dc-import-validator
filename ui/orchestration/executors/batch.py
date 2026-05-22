"""Thin wrapper around ui.services.batch_runner (Cloud Batch job submission)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ui.orchestration.spec import RunSpec
from ui.services.batch_runner import InputFiles, submit_job


@dataclass(frozen=True)
class BatchSubmitPlan:
    """Prepared Batch submission — no side effects until submit()."""

    run_id: str
    dataset: str
    input_files: InputFiles
    machine_type_override: str = ""


@dataclass(frozen=True)
class BatchSubmitResult:
    job_name: str


class BatchExecutor:
    """Map RunSpec → InputFiles and submit_job (existing Batch client)."""

    def spec_to_input_files(self, spec: RunSpec) -> InputFiles:
        """Translate RunSpec into batch_runner.InputFiles (no new semantics)."""
        inp = spec.inputs
        opt = spec.options
        gcs_prefix = ""
        if inp.session_id and not inp.tmcf_gcs_path:
            gcs_prefix = f"sessions/{inp.session_id}"
        return InputFiles(
            gcs_prefix=gcs_prefix,
            tmcf_filename=inp.tmcf_filename,
            csv_filenames=list(inp.csv_filenames),
            stat_vars_mcf_filename=inp.stat_vars_mcf_filename or None,
            stat_vars_schema_mcf_filename=inp.stat_vars_schema_mcf_filename or None,
            csv_total_bytes=inp.csv_total_bytes,
            tmcf_gcs_path=inp.tmcf_gcs_path,
            csv_gcs_paths=list(inp.csv_gcs_paths),
            stat_vars_mcf_gcs_path=inp.stat_vars_mcf_gcs_path,
            stat_vars_schema_mcf_gcs_path=inp.stat_vars_schema_mcf_gcs_path,
            llm_review=opt.llm_review,
            rules_filter=spec.rules.rules_filter,
            skip_rules_filter=spec.rules.skip_rules_filter,
            baseline_name=inp.baseline_name,
            import_resolution_mode=opt.import_resolution_mode,
            import_existence_checks=opt.existence_checks,
            processing_mode=opt.processing_mode,
            java_threads=opt.java_threads,
            merged_config_gcs_path=spec.rules.merged_config_gcs_path,
        )

    def plan_submit(self, spec: RunSpec) -> BatchSubmitPlan:
        dataset = spec.dataset_id if spec.is_builtin else "custom"
        return BatchSubmitPlan(
            run_id=spec.run_id,
            dataset=dataset,
            input_files=self.spec_to_input_files(spec),
            machine_type_override=spec.options.machine_type_override,
        )

    def submit(self, plan: BatchSubmitPlan) -> BatchSubmitResult:
        """Submit Cloud Batch job via existing batch_runner.submit_job."""
        job_name = submit_job(
            plan.run_id,
            plan.dataset,
            plan.input_files,
            machine_type_override=plan.machine_type_override,
        )
        return BatchSubmitResult(job_name=job_name)

    def submit_spec(self, spec: RunSpec) -> BatchSubmitResult:
        return self.submit(self.plan_submit(spec))
