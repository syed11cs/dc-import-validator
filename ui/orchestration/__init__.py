"""Lightweight run orchestration (policy, spec, paths, executors).

HTTP routes: POST/GET /api/runs, GET /api/pipeline/registry (ui/server.py).
"""

from ui.orchestration.executors import (
    BatchExecutor,
    BatchSubmitResult,
    SubprocessExecutor,
    SubprocessRunPlan,
)
from ui.orchestration.paths import RunPaths, resolve_run_paths
from ui.orchestration.policy import (
    BATCH,
    DEVELOPMENT,
    ExecutorResolution,
    PolicyBlockedError,
    PRODUCTION,
    SUBPROCESS,
    batch_configured,
    deployment_profile,
    gcs_reports_configured,
    resolve_executor,
)
from ui.orchestration.spec import (
    BUILTIN,
    BUILTIN_DATASETS,
    CUSTOM,
    RunInputs,
    RunOptions,
    RunRules,
    RunSpec,
    run_spec_from_mapping,
)

__all__ = [
    "BATCH",
    "BUILTIN",
    "BUILTIN_DATASETS",
    "CUSTOM",
    "BatchExecutor",
    "BatchSubmitResult",
    "DEVELOPMENT",
    "ExecutorResolution",
    "PRODUCTION",
    "PolicyBlockedError",
    "RunInputs",
    "RunOptions",
    "RunPaths",
    "RunRules",
    "RunSpec",
    "SUBPROCESS",
    "SubprocessExecutor",
    "SubprocessRunPlan",
    "batch_configured",
    "deployment_profile",
    "gcs_reports_configured",
    "resolve_executor",
    "resolve_run_paths",
    "run_spec_from_mapping",
]
