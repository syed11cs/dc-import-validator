"""Executor and deployment profile resolution."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from ui.orchestration.spec import RunSpec

DeploymentProfile = str  # development | production
DEVELOPMENT = "development"
PRODUCTION = "production"

ExecutorKind = str  # subprocess | batch
SUBPROCESS = "subprocess"
BATCH = "batch"


class OrchestrationError(Exception):
    """Base orchestration error."""


class PolicyBlockedError(OrchestrationError):
    """Run cannot be started under current deployment policy."""


@dataclass(frozen=True)
class ExecutorResolution:
    """Result of policy.resolve_executor — no side effects."""

    executor: ExecutorKind
    profile: DeploymentProfile
    reason: str
    requires_gcs: bool = False
    requires_batch: bool = False


def _env(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return env if env is not None else os.environ


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def gcs_reports_configured(env: Mapping[str, str]) -> bool:
    return bool((env.get("GCS_REPORTS_BUCKET") or "").strip())


def batch_configured(env: Mapping[str, str]) -> bool:
    required = ("BATCH_PROJECT_ID", "BATCH_REGION", "BATCH_SERVICE_ACCOUNT", "GCS_REPORTS_BUCKET")
    return all((env.get(k) or "").strip() for k in required)


def deployment_profile(env: Mapping[str, str] | None = None) -> DeploymentProfile:
    """Resolve deployment profile from env (explicit or inferred)."""
    e = _env(env)
    explicit = (e.get("DEPLOYMENT_PROFILE") or e.get("DC_DEPLOYMENT_PROFILE") or "").strip().lower()
    if explicit in ("development", "dev", "local"):
        return DEVELOPMENT
    if explicit in ("production", "prod"):
        return PRODUCTION
    if gcs_reports_configured(e) and (e.get("BATCH_PROJECT_ID") or "").strip():
        return PRODUCTION
    return DEVELOPMENT


def resolve_executor(
    spec: RunSpec,
    env: Mapping[str, str] | None = None,
) -> ExecutorResolution:
    """Choose subprocess vs batch for a run spec. Raises PolicyBlockedError when disallowed."""
    e = _env(env)
    profile = deployment_profile(e)

    if spec.is_builtin:
        if profile == PRODUCTION:
            if not batch_configured(e):
                raise PolicyBlockedError(
                    "Built-in runs in production require Cloud Batch configuration "
                    "(BATCH_PROJECT_ID, BATCH_REGION, BATCH_SERVICE_ACCOUNT, GCS_REPORTS_BUCKET)."
                )
            return ExecutorResolution(
                executor=BATCH,
                profile=profile,
                reason="builtin dataset in production profile → Cloud Batch",
                requires_gcs=True,
                requires_batch=True,
            )
        return ExecutorResolution(
            executor=SUBPROCESS,
            profile=profile,
            reason="builtin dataset in development profile → local subprocess",
        )

    # custom
    if profile == DEVELOPMENT:
        if not _truthy(e.get("ALLOW_LOCAL_CUSTOM")):
            raise PolicyBlockedError(
                "Custom dataset runs are blocked in development profile. "
                "Set ALLOW_LOCAL_CUSTOM=1 to enable local subprocess execution, "
                "or use DEPLOYMENT_PROFILE=production with GCS + Batch."
            )
        return ExecutorResolution(
            executor=SUBPROCESS,
            profile=profile,
            reason="custom dataset in development with ALLOW_LOCAL_CUSTOM=1 → subprocess",
        )

    if not gcs_reports_configured(e):
        raise PolicyBlockedError(
            "Custom runs in production profile require GCS_REPORTS_BUCKET."
        )
    if not batch_configured(e):
        raise PolicyBlockedError(
            "Custom runs in production profile require Cloud Batch configuration."
        )
    return ExecutorResolution(
        executor=BATCH,
        profile=profile,
        reason="custom dataset in production profile → Cloud Batch + GCS",
        requires_gcs=True,
        requires_batch=True,
    )
