"""Cloud Batch submission client for DC Import Validator.

Submits validation jobs to Google Cloud Batch instead of running them
in Cloud Run. Cloud Run becomes a thin API/UI layer; all JVM and heavy
compute runs on Batch VMs.

Environment variables (all required at submit time):
    BATCH_PROJECT_ID          GCP project ID
    BATCH_REGION              Region for Batch jobs (e.g. us-central1)
    BATCH_SERVICE_ACCOUNT     Service account email for the Batch VM
    GCS_REPORTS_BUCKET        GCS bucket for status.json and reports

The container image for Batch jobs is read from BATCH_IMAGE_URI, which the
CI/CD deploy workflow sets automatically via --update-env-vars on every
deployment. This keeps Batch jobs in sync with Cloud Run without any manual
configuration step.

Optional environment variables:
    BATCH_IMAGE_URI           Container image URI. Set automatically by the
                              deploy workflow; must be set manually for local
                              development.
    GEMINI_API_KEY / GOOGLE_API_KEY / DC_API_KEY
    IMPORT_RESOLUTION_MODE / IMPORT_EXISTENCE_CHECKS
    JAVA_THREADS
    LOG_LEVEL
    BATCH_PROVISIONING_MODEL  SPOT (default) or STANDARD
"""

import os
import re
from dataclasses import dataclass, field
from typing import Optional

from google.cloud import batch_v1
from google.protobuf import duration_pb2

from ui.app_logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Machine tier table
# < 5 GB  → n2-highmem-16  (128 GB RAM), 2 h max
# 5–20 GB → n2-highmem-32  (256 GB RAM), 6 h max
# 20–50 GB → n2-highmem-64 (512 GB RAM), 12 h max
# ---------------------------------------------------------------------------
_TIERS = [
    # (max_bytes,        machine_type,      max_run_seconds)
    (5  * 1024**3,  "n2-highmem-16",   2  * 3600),
    (20 * 1024**3,  "n2-highmem-32",   6  * 3600),
    (50 * 1024**3,  "n2-highmem-64",   12 * 3600),
]
_DEFAULT_TIER = ("n2-highmem-16", 2 * 3600)  # fallback when size unknown

# ---------------------------------------------------------------------------
# Performance tuning tables
# ---------------------------------------------------------------------------
# vCPU counts per machine type — used to compute JAVA_THREADS.
_VCPUS_BY_MACHINE: dict[str, int] = {
    "n2-highmem-16": 16,
    "n2-highmem-32": 32,
    "n2-highmem-64": 64,
}

# JVM -Xmx per machine type: machine_RAM * 0.75, rounded to nearest GB.
_JVM_XMX_BY_MACHINE: dict[str, str] = {
    "n2-highmem-16": "96g",   # 128 GB * 0.75
    "n2-highmem-32": "192g",  # 256 GB * 0.75
    "n2-highmem-64": "384g",  # 512 GB * 0.75
}

# Thread fraction of total vCPUs per processing mode.
# Capped at 0.75 — G1GC background threads claim ~vCPUs/4 concurrently.
_THREAD_FRACTION_BY_MODE: dict[str, float] = {
    "auto":         0.50,
    "conservative": 0.30,
    "aggressive":   0.75,
}

# Max run seconds by machine type — used when a machine is explicitly requested.
_MAX_RUN_SECONDS_BY_MACHINE: dict[str, int] = {
    mt: s for _, mt, s in _TIERS
}


@dataclass
class InputFiles:
    """Describes the files for a custom dataset validation run.

    For built-in datasets (child_birth, etc.) pass an empty InputFiles()
    and set dataset to the built-in name.
    """
    gcs_prefix: str = ""                         # e.g. "sessions/abc123"
    tmcf_filename: str = ""
    csv_filenames: list = field(default_factory=list)
    stat_vars_mcf_filename: Optional[str] = None
    stat_vars_schema_mcf_filename: Optional[str] = None
    csv_total_bytes: int = 0                     # used for tier selection
    # GCS path mode: full gs:// URIs passed directly (alternative to gcs_prefix + filenames).
    # Used when the user provides existing GCS paths instead of uploading files.
    tmcf_gcs_path: str = ""
    csv_gcs_paths: list = field(default_factory=list)   # list of full gs:// URIs
    stat_vars_mcf_gcs_path: str = ""
    stat_vars_schema_mcf_gcs_path: str = ""
    # Pipeline options
    llm_review: bool = False
    rules_filter: str = ""
    skip_rules_filter: str = ""
    baseline_name: str = ""
    import_resolution_mode: str = "LOCAL"
    import_existence_checks: str = "false"
    # Performance tuning
    processing_mode: str = "auto"   # "auto" | "conservative" | "aggressive" | "custom"
    java_threads: int = 0           # 0 = compute from processing_mode; >0 = explicit override
    # GCS URI of a pre-merged validation config (gs://bucket/configs/{run_id}/...).
    # Set when custom SQL rules are present; the Batch VM downloads and passes --config=.
    merged_config_gcs_path: str = ""


def _select_tier(csv_total_bytes: int):
    """Return (machine_type, max_run_seconds) for the given CSV size."""
    if csv_total_bytes <= 0:
        return _DEFAULT_TIER
    for max_bytes, machine_type, max_seconds in _TIERS:
        if csv_total_bytes < max_bytes:
            return machine_type, max_seconds
    # Larger than 50 GB — use the biggest tier with the longest window
    _, machine_type, max_seconds = _TIERS[-1]
    return machine_type, max_seconds


def _sanitize_job_id(run_id: str) -> str:
    """Return a Cloud Batch-safe job ID derived from run_id.

    Batch job IDs: lowercase alphanumeric + hyphens, must start with letter,
    max 63 characters.
    """
    sanitized = re.sub(r"[^a-z0-9-]", "-", run_id.lower())
    sanitized = re.sub(r"-+", "-", sanitized).strip("-")
    if not sanitized or not sanitized[0].isalpha():
        sanitized = "j-" + sanitized
    # Prefix to namespace our jobs; leave room for prefix + 63 char limit
    job_id = ("dc-import-" + sanitized)[:63]
    return job_id.rstrip("-")


def _job_name(project: str, region: str, job_id: str) -> str:
    return f"projects/{project}/locations/{region}/jobs/{job_id}"


def compute_job_name(run_id: str) -> str:
    """Return the fully-qualified Batch job name for a given run_id.

    Reads BATCH_PROJECT_ID and BATCH_REGION from the environment.
    Useful for probing job state before status.json has been written.
    """
    project = os.environ["BATCH_PROJECT_ID"]
    region  = os.environ["BATCH_REGION"]
    job_id  = _sanitize_job_id(run_id)
    return _job_name(project, region, job_id)


def _batch_client(region: str) -> batch_v1.BatchServiceClient:
    return batch_v1.BatchServiceClient()


def _build_env_vars(run_id: str, dataset: str, input_files: InputFiles, machine_type: str = "") -> dict:
    """Build the env var dict to inject into the Batch container."""
    env = {
        # Required by entrypoint.sh
        "RUN_ID": run_id,
        "DATASET": dataset,
        "GCS_REPORTS_BUCKET": os.environ["GCS_REPORTS_BUCKET"],
        # Pipeline options
        "BASELINE_AUTO_UPDATE": "false",
        "IMPORT_RESOLUTION_MODE": input_files.import_resolution_mode or os.environ.get("IMPORT_RESOLUTION_MODE", "LOCAL"),
        "IMPORT_EXISTENCE_CHECKS": input_files.import_existence_checks or os.environ.get("IMPORT_EXISTENCE_CHECKS", "false"),
    }

    # Custom dataset — pass file locations
    if input_files.gcs_prefix:
        env["GCS_INPUT_PREFIX"] = input_files.gcs_prefix
    if input_files.tmcf_filename:
        env["TMCF_FILENAME"] = input_files.tmcf_filename
    if input_files.csv_filenames:
        env["CSV_FILENAMES"] = ",".join(input_files.csv_filenames)
    if input_files.stat_vars_mcf_filename:
        env["STAT_VARS_MCF_FILENAME"] = input_files.stat_vars_mcf_filename
    if input_files.stat_vars_schema_mcf_filename:
        env["STAT_VARS_SCHEMA_MCF_FILENAME"] = input_files.stat_vars_schema_mcf_filename
    # GCS path mode: full gs:// URIs for each input file. The Batch VM downloads them
    # directly using its attached service account (BATCH_SERVICE_ACCOUNT), which must
    # have read access to the target buckets (not necessarily GCS_REPORTS_BUCKET).
    # Paths are newline-separated — GCS object names cannot contain newlines.
    if input_files.tmcf_gcs_path:
        env["TMCF_GCS_PATH"] = input_files.tmcf_gcs_path
    if input_files.csv_gcs_paths:
        env["CSV_GCS_PATHS"] = "\n".join(input_files.csv_gcs_paths)
    if input_files.stat_vars_mcf_gcs_path:
        env["STAT_VARS_MCF_GCS_PATH"] = input_files.stat_vars_mcf_gcs_path
    if input_files.stat_vars_schema_mcf_gcs_path:
        env["STAT_VARS_SCHEMA_MCF_GCS_PATH"] = input_files.stat_vars_schema_mcf_gcs_path
    if input_files.baseline_name:
        env["BASELINE_NAME"] = input_files.baseline_name

    # LLM / rule filters
    if input_files.llm_review:
        env["LLM_REVIEW"] = "true"
    if input_files.rules_filter:
        env["RULES_FILTER"] = input_files.rules_filter
    if input_files.skip_rules_filter:
        env["SKIP_RULES_FILTER"] = input_files.skip_rules_filter
    if input_files.merged_config_gcs_path:
        env["MERGED_CONFIG_GCS_PATH"] = input_files.merged_config_gcs_path

    logger.info(
        '[OVERRIDE_TRACE] {"component":"_build_env_vars","run_id":"%s",'
        '"input_files.merged_config_gcs_path":%r,"input_files.rules_filter":%r,'
        '"MERGED_CONFIG_GCS_PATH_in_env":%r,"RULES_FILTER_in_env":%r}',
        run_id,
        input_files.merged_config_gcs_path,
        input_files.rules_filter,
        env.get("MERGED_CONFIG_GCS_PATH", "(not set)"),
        env.get("RULES_FILTER", "(not set)"),
    )

    # Java concurrency: derive JAVA_THREADS and JAVA_XMX.
    #
    # JAVA_XMX: always set from machine type when known so we never silently rely on the
    # JVM default (25% of RAM). Falls back to Cloud Run env, then leaves unset (shell
    # defaults to 96g — the n2-highmem-16 value).
    if machine_type in _JVM_XMX_BY_MACHINE:
        env["JAVA_XMX"] = _JVM_XMX_BY_MACHINE[machine_type]
    elif xmx_env := os.environ.get("JAVA_XMX", ""):
        env["JAVA_XMX"] = xmx_env
    # else: leave unset — shell script JAVA_XMX="${JAVA_XMX:-96g}" provides the default
    #
    # JAVA_THREADS priority (highest to lowest):
    #   1. JAVA_THREADS env var on Cloud Run (operator override — always wins)
    #   2. input_files.java_threads > 0 (user-selected "custom" mode)
    #   3. Computed from processing_mode fraction × machine vCPUs
    #   4. Unset — shell script defaults to 2
    _effective_mode = input_files.processing_mode or "auto"
    _thread_source = "default"
    if java_threads_env := os.environ.get("JAVA_THREADS", ""):
        env["JAVA_THREADS"] = java_threads_env
        _thread_source = "env_override"
        # HIGH-VISIBILITY: env var is silently overriding the user's UI selection.
        if input_files.java_threads > 0 and str(input_files.java_threads) != java_threads_env:
            logger.warning(
                "!!!! JAVA_THREADS ENV OVERRIDE !!!!\n"
                "  JAVA_THREADS env var (%s) is overriding the user-selected value (%d).\n"
                "  The job will run with %s threads, not %d.\n"
                "  To use the user-selected value, remove JAVA_THREADS from the Cloud Run service env.\n"
                "  [run_id=%s processing_mode=%s machine=%s]",
                java_threads_env, input_files.java_threads,
                java_threads_env, input_files.java_threads,
                run_id, _effective_mode, machine_type,
            )
        elif input_files.java_threads == 0 and _effective_mode != "auto":
            logger.warning(
                "JAVA_THREADS env override (%s) is overriding processing_mode=%s "
                "[run_id=%s machine=%s]",
                java_threads_env, _effective_mode, run_id, machine_type,
            )
        # Detect oversubscription — env override is not capped, but we warn when it
        # exceeds the machine's vCPU count so operators can spot misconfiguration.
        try:
            _env_threads_int = int(java_threads_env)
        except ValueError:
            _env_threads_int = 0
        if _env_threads_int > 0 and machine_type in _VCPUS_BY_MACHINE:
            _vcpus = _VCPUS_BY_MACHINE[machine_type]
            if _env_threads_int > _vcpus:
                logger.warning(
                    "JAVA_THREADS (%d) exceeds vCPU count (%d) for %s — "
                    "this may cause CPU oversubscription [run_id=%s processing_mode=%s]",
                    _env_threads_int, _vcpus, machine_type, run_id, _effective_mode,
                )
    elif input_files.java_threads > 0:
        env["JAVA_THREADS"] = str(input_files.java_threads)
        _thread_source = "ui_custom"
    elif machine_type in _VCPUS_BY_MACHINE:
        vcpus = _VCPUS_BY_MACHINE[machine_type]
        if _effective_mode == "custom":
            # processing_mode=custom but java_threads was 0 — fall back to auto.
            logger.warning(
                "processing_mode=custom but java_threads=0; falling back to auto mode "
                "[run_id=%s machine=%s]",
                run_id, machine_type,
            )
            _effective_mode = "auto"
        fraction = _THREAD_FRACTION_BY_MODE.get(_effective_mode, _THREAD_FRACTION_BY_MODE["auto"])
        threads = min(max(1, int(vcpus * fraction)), vcpus)
        env["JAVA_THREADS"] = str(threads)
        _thread_source = f"mode_auto({_effective_mode})"

    # Propagate thread source into the container so entrypoint.sh can include it
    # in the [PERF] log line without re-deriving the priority logic in bash.
    env["JAVA_THREADS_SOURCE"] = _thread_source

    # Canonical thread-config log line — one grep-able line with all resolved values.
    logger.info(
        "[THREAD_CONFIG] run_id=%s machine=%s processing_mode=%s "
        "requested_java_threads=%s effective_java_threads=%s thread_source=%s JAVA_XMX=%s",
        run_id,
        machine_type or "unknown",
        _effective_mode,
        input_files.java_threads if input_files.java_threads > 0 else "auto",
        env.get("JAVA_THREADS", "default(2)"),
        _thread_source,
        env.get("JAVA_XMX", "default(96g)"),
    )

    # Pass-through API keys (only if set on the server)
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "DC_API_KEY"):
        val = os.environ.get(key, "")
        if val:
            env[key] = val

    # CSV auto-split controls: pass through from Cloud Run env so operators can
    # enable splitting for benchmarking without code changes.
    # Default is off; set CSV_SPLIT_ENABLED=true on the Cloud Run service to enable.
    # CSV_DUP_CHECK: auto (default) | true | false. Controls duplicate row hashing in
    # validate_and_split.py. In auto mode the shell script disables checking for large
    # single-CSV split runs (>5M row threshold) to save ~165s on 38M-row datasets.
    for key in ("CSV_SPLIT_ENABLED", "CSV_SPLIT_ROWS", "CSV_SPLIT_TARGET_SHARDS_PER_THREAD", "CSV_SPLIT_THRESHOLD_ROWS", "CSV_SPLIT_CLEANUP", "CSV_DUP_CHECK"):
        val = os.environ.get(key, "")
        if val:
            env[key] = val

    # JFR/GC profiling: set GENMCF_PROFILE=true on the Cloud Run service to enable
    # for the next Batch job without a code change. Default is false (off).
    if os.environ.get("GENMCF_PROFILE", ""):
        env["GENMCF_PROFILE"] = os.environ["GENMCF_PROFILE"]

    # Logging
    log_level = os.environ.get("LOG_LEVEL", "")
    if log_level:
        env["LOG_LEVEL"] = log_level

    return env


def _resolve_image() -> str:
    """Return the container image URI to use for Batch jobs.

    BATCH_IMAGE_URI is set automatically by the CI/CD deploy workflow on every
    deployment (via --update-env-vars), so it always matches the image that is
    running in Cloud Run. For local development, set it manually.
    """
    image = os.environ.get("BATCH_IMAGE_URI", "")
    if not image:
        raise RuntimeError(
            "BATCH_IMAGE_URI is not set. In production it is injected automatically "
            "by the deploy workflow. For local development, set it manually."
        )
    return image


def submit_job(run_id: str, dataset: str, input_files: InputFiles, machine_type_override: str = "") -> str:
    """Submit a Cloud Batch job for a validation run.

    Args:
        run_id:                Unique run identifier (also used as job name seed).
        dataset:               Dataset name (built-in name or "custom").
        input_files:           InputFiles describing uploaded files and pipeline options.
        machine_type_override: If non-empty, bypass tier selection and use this machine type
                               directly. Must be a key in _VCPUS_BY_MACHINE.

    Returns:
        Fully-qualified Batch job name (can be used with cancel_job / get_batch_state).
    """
    project = os.environ["BATCH_PROJECT_ID"]
    region  = os.environ["BATCH_REGION"]
    image   = _resolve_image()
    sa      = os.environ["BATCH_SERVICE_ACCOUNT"]

    provisioning_model_name = os.environ.get("BATCH_PROVISIONING_MODEL", "STANDARD").upper()
    provisioning_model = (
        batch_v1.AllocationPolicy.ProvisioningModel.SPOT
        if provisioning_model_name == "SPOT"
        else batch_v1.AllocationPolicy.ProvisioningModel.STANDARD
    )

    if machine_type_override and machine_type_override in _VCPUS_BY_MACHINE:
        machine_type = machine_type_override
        max_run_seconds = _MAX_RUN_SECONDS_BY_MACHINE.get(machine_type, _DEFAULT_TIER[1])
    else:
        machine_type, max_run_seconds = _select_tier(input_files.csv_total_bytes)
    job_id   = _sanitize_job_id(run_id)
    job_name = _job_name(project, region, job_id)

    logger.info(
        "Submitting Batch job run_id=%s job_id=%s dataset=%s machine=%s "
        "csv_bytes=%d max_run_seconds=%d provisioning=%s",
        run_id, job_id, dataset, machine_type,
        input_files.csv_total_bytes, max_run_seconds, provisioning_model_name,
    )

    env_vars = _build_env_vars(run_id, dataset, input_files, machine_type)
    env_vars["BATCH_JOB_NAME"] = job_name
    env_vars["VM_TYPE"]        = machine_type

    import json as _json
    # Serialize full env payload — mask only GCS URIs to avoid credential leakage.
    _env_for_log = {
        k: ("[MASKED]" if any(s in k for s in ("KEY", "SECRET", "TOKEN", "PASSWORD")) else v)
        for k, v in env_vars.items()
    }
    logger.info(
        '[OVERRIDE_TRACE] {"component":"submit_job","event":"batch_env_payload",'
        '"run_id":"%s","env":%s}',
        run_id, _json.dumps(_env_for_log),
    )

    # ------------------------------------------------------------------
    # Runnable: our container with the Batch entrypoint
    # ------------------------------------------------------------------
    container = batch_v1.Runnable.Container(
        image_uri=image,
        entrypoint="/app/dc-import-validator/batch/entrypoint.sh",
        # No additional commands — entrypoint is self-contained
        commands=[],
    )
    runnable = batch_v1.Runnable(container=container)

    # ------------------------------------------------------------------
    # TaskSpec
    # ------------------------------------------------------------------
    task_spec = batch_v1.TaskSpec(
        runnables=[runnable],
        environment=batch_v1.Environment(variables=env_vars),
        max_run_duration=duration_pb2.Duration(seconds=max_run_seconds),
        max_retry_count=0,  # no retries during STANDARD stabilization; set to 1 + SPOT to re-enable
        # lifecycle_policies guards exit code 1 (validation failure) from retrying even if
        # max_retry_count is later raised — keeps the policy in place as defense-in-depth.
        lifecycle_policies=[
            batch_v1.LifecyclePolicy(
                action=batch_v1.LifecyclePolicy.Action.FAIL_TASK,
                action_condition=batch_v1.LifecyclePolicy.ActionCondition(
                    exit_codes=[1],
                ),
            )
        ],
    )

    task_group = batch_v1.TaskGroup(
        task_spec=task_spec,
        task_count=1,
        parallelism=1,
    )

    # ------------------------------------------------------------------
    # Allocation policy: machine type, boot disk, provisioning model
    # ------------------------------------------------------------------
    boot_disk = batch_v1.AllocationPolicy.Disk(
        type_="pd-ssd",
        size_gb=500,
    )
    instance_policy = batch_v1.AllocationPolicy.InstancePolicy(
        machine_type=machine_type,
        boot_disk=boot_disk,
        provisioning_model=provisioning_model,
    )
    instance_policy_or_template = batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
        policy=instance_policy,
    )
    service_account = batch_v1.ServiceAccount(email=sa)
    allocation_policy = batch_v1.AllocationPolicy(
        instances=[instance_policy_or_template],
        service_account=service_account,
    )

    # ------------------------------------------------------------------
    # Logs: Cloud Logging
    # ------------------------------------------------------------------
    logs_policy = batch_v1.LogsPolicy(
        destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING,
    )

    # ------------------------------------------------------------------
    # Assemble and submit
    # ------------------------------------------------------------------
    job = batch_v1.Job(
        task_groups=[task_group],
        allocation_policy=allocation_policy,
        logs_policy=logs_policy,
        labels={
            "dc-import-validator": "true",
            "dataset": re.sub(r"[^a-z0-9_-]", "-", dataset.lower())[:63],
        },
    )

    request = batch_v1.CreateJobRequest(
        parent=f"projects/{project}/locations/{region}",
        job_id=job_id,
        job=job,
    )

    client = _batch_client(region)
    created_job = client.create_job(request=request)
    logger.info("Batch job created: %s", created_job.name)
    return created_job.name


def cancel_job(job_name: str) -> None:
    """Cancel (delete) a running Batch job.

    Cloud Batch has no dedicated cancel API; deleting the job terminates it.

    Args:
        job_name: Fully-qualified job name returned by submit_job().
    """
    logger.info("Cancelling Batch job: %s", job_name)
    region = job_name.split("/")[3]
    client = _batch_client(region)
    try:
        op = client.delete_job(name=job_name)
        # Fire-and-forget: don't wait for the long-running operation
        logger.info("Delete operation started for %s: %s", job_name, op.operation.name)
    except Exception as exc:
        # Job may already be terminal — log and swallow so callers don't fail
        logger.warning("cancel_job(%s) raised: %s", job_name, exc)


def get_batch_state(job_name: str) -> str:
    """Return the current Batch job state as a string.

    Returns one of: QUEUED, SCHEDULED, RUNNING, SUCCEEDED, FAILED,
    DELETION_IN_PROGRESS, STATE_UNSPECIFIED, or UNKNOWN (on error).

    Args:
        job_name: Fully-qualified job name returned by submit_job().
    """
    region = job_name.split("/")[3]
    client = _batch_client(region)
    try:
        job = client.get_job(name=job_name)
        state_name = batch_v1.JobStatus.State(job.status.state).name
        logger.debug("Batch job %s state: %s", job_name, state_name)
        return state_name
    except Exception as exc:
        logger.warning("get_batch_state(%s) raised: %s", job_name, exc)
        return "UNKNOWN"
