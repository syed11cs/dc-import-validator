"""Cloud Batch submission client for DC Import Validator.

Submits validation jobs to Google Cloud Batch instead of running them
in Cloud Run. Cloud Run becomes a thin API/UI layer; all JVM and heavy
compute runs on Batch VMs.

Environment variables (all required at submit time):
    BATCH_PROJECT_ID          GCP project ID
    BATCH_REGION              Region for Batch jobs (e.g. us-central1)
    BATCH_IMAGE_URI           Docker image URI (same image as Cloud Run)
    BATCH_SERVICE_ACCOUNT     Service account email for the Batch VM
    GCS_REPORTS_BUCKET        GCS bucket for status.json and reports

Optional environment variables passed through to the Batch job container:
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
    # Pipeline options
    llm_review: bool = False
    rules_filter: str = ""
    skip_rules_filter: str = ""
    baseline_name: str = ""
    import_resolution_mode: str = "LOCAL"
    import_existence_checks: str = "false"


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


def _batch_client(region: str) -> batch_v1.BatchServiceClient:
    return batch_v1.BatchServiceClient()


def _build_env_vars(run_id: str, dataset: str, input_files: InputFiles) -> dict:
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
    if input_files.baseline_name:
        env["BASELINE_NAME"] = input_files.baseline_name

    # LLM / rule filters
    if input_files.llm_review:
        env["LLM_REVIEW"] = "true"
    if input_files.rules_filter:
        env["RULES_FILTER"] = input_files.rules_filter
    if input_files.skip_rules_filter:
        env["SKIP_RULES_FILTER"] = input_files.skip_rules_filter

    # Java concurrency
    java_threads = os.environ.get("JAVA_THREADS", "")
    if java_threads:
        env["JAVA_THREADS"] = java_threads

    # Pass-through API keys (only if set on the server)
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "DC_API_KEY"):
        val = os.environ.get(key, "")
        if val:
            env[key] = val

    # Logging
    log_level = os.environ.get("LOG_LEVEL", "")
    if log_level:
        env["LOG_LEVEL"] = log_level

    return env


def submit_job(run_id: str, dataset: str, input_files: InputFiles) -> str:
    """Submit a Cloud Batch job for a validation run.

    Args:
        run_id:      Unique run identifier (also used as job name seed).
        dataset:     Dataset name (built-in name or "custom").
        input_files: InputFiles describing uploaded files and pipeline options.

    Returns:
        Fully-qualified Batch job name (can be used with cancel_job / get_batch_state).
    """
    project = os.environ["BATCH_PROJECT_ID"]
    region  = os.environ["BATCH_REGION"]
    image   = os.environ["BATCH_IMAGE_URI"]
    sa      = os.environ["BATCH_SERVICE_ACCOUNT"]

    provisioning_model_name = os.environ.get("BATCH_PROVISIONING_MODEL", "SPOT").upper()
    provisioning_model = (
        batch_v1.AllocationPolicy.ProvisioningModel.SPOT
        if provisioning_model_name == "SPOT"
        else batch_v1.AllocationPolicy.ProvisioningModel.STANDARD
    )

    machine_type, max_run_seconds = _select_tier(input_files.csv_total_bytes)
    job_id   = _sanitize_job_id(run_id)
    job_name = _job_name(project, region, job_id)

    logger.info(
        "Submitting Batch job run_id=%s job_id=%s dataset=%s machine=%s "
        "csv_bytes=%d max_run_seconds=%d provisioning=%s",
        run_id, job_id, dataset, machine_type,
        input_files.csv_total_bytes, max_run_seconds, provisioning_model_name,
    )

    env_vars = _build_env_vars(run_id, dataset, input_files)
    env_vars["BATCH_JOB_NAME"] = job_name
    env_vars["VM_TYPE"]        = machine_type

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
        max_retry_count=1,  # one retry on SPOT preemption
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
