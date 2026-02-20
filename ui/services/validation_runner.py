"""Subprocess orchestration for run_e2e_test.sh: stream NDJSON or wait and return result.

When RUN_ID is set (UI), the script writes to output/{dataset}/{run_id}/. After upload
to GCS, we copy artifacts to the canonical dir (e.g. output/child_birth_genmcf) so
"latest" serving and APIs without run_id still work.
"""

import asyncio
import json
import os
import re
import shutil
import time
from pathlib import Path

from fastapi import Request
from fastapi.responses import StreamingResponse

from ui.app_logging import get_logger

logger = get_logger(__name__)

# Max run time (seconds). 0 or unset = no timeout. Set VALIDATION_RUN_TIMEOUT_SEC to enable (e.g. 3600).
def _run_timeout_sec() -> int:
    try:
        v = os.environ.get("VALIDATION_RUN_TIMEOUT_SEC", "").strip()
        return int(v) if v else 0
    except ValueError:
        return 0

# Artifacts to copy from per-run dir to canonical "latest" dir
_CANONICAL_ARTIFACTS = (
    "validation_report.html",
    "summary_report.html",
    "validation_output.json",
    "report.json",
    "schema_review.json",
    "summary_report.csv",
)
# Only copy to canonical when the full report exists (pipeline reached Step 3). Avoids
# overwriting canonical with partial state when Step 0/1/2 fails (e.g. Gemini blocks, genmcf fails).
_VALIDATION_REPORT_HTML = "validation_report.html"


def _copy_run_to_canonical(run_dir: Path, canonical_dir: Path) -> None:
    """Copy report artifacts from per-run dir to canonical dir so latest-run APIs work.
    Only runs when validation_report.html exists (full run completed); copies only existing
    files and never clears canonical first.
    """
    if not run_dir.is_dir() or not canonical_dir or run_dir.resolve() == canonical_dir.resolve():
        return
    if not (run_dir / _VALIDATION_REPORT_HTML).exists():
        return
    try:
        canonical_dir.mkdir(parents=True, exist_ok=True)
        for name in _CANONICAL_ARTIFACTS:
            src = run_dir / name
            if src.exists():
                shutil.copy2(src, canonical_dir / name)
    except OSError as e:
        logger.warning("copy run to canonical failed run_dir=%s canonical_dir=%s: %s", run_dir, canonical_dir, e)


def _upload_reports_sync(output_dir: Path, run_id: str, dataset: str) -> None:
    """Run GCS upload in a thread-friendly way (sync). Validation success must not depend on upload."""
    try:
        from ui.gcs_reports import upload_reports_to_gcs
        if upload_reports_to_gcs(output_dir, run_id, dataset):
            logger.info("uploaded reports to GCS run_id=%s dataset=%s", run_id, dataset)
    except Exception:
        logger.exception("GCS upload failed run_id=%s dataset=%s", run_id, dataset)

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _parse_failure(output: str) -> dict | None:
    """Parse run output into a structured failure event. Returns None if no known failure.
    Fallback only when no structured failure was emitted during stream. Order matters: first match wins.
    """
    if not output or not isinstance(output, str):
        return None
    # Observation count mismatch (validation step)
    m = re.search(r"NumObservations sum \((\d+)\)\s*!=\s*NumNodeSuccesses \((\d+)\)", output)
    if m:
        return {
            "code": "OBSERVATION_COUNT_MISMATCH",
            "step": 3,
            "message": f"Observation count ({m.group(1)}) doesn't match node count ({m.group(2)})",
        }
    if "CSV row count exceeds" in output:
        limit_m = re.search(r"exceeds (\d+)|limit of (\d+)", output)
        limit = int(limit_m.group(1) or limit_m.group(2)) if limit_m else None
        return {
            "code": "ROW_COUNT_EXCEEDED",
            "step": 0,
            "message": "CSV row count exceeds limit" + (f" ({limit} rows max)" if limit else ""),
            "limit": limit,
        }
    if "CSV quality check failed" in output:
        return {"code": "CSV_QUALITY_FAILED", "step": 0, "message": "CSV quality check failed"}
    if "Preflight failed" in output:
        return {"code": "PREFLIGHT_FAILED", "step": 0, "message": "Preflight failed"}
    if "dc-import genmcf failed" in output or "Aborting prematurely" in output:
        return {"code": "DATA_PROCESSING_FAILED", "step": 2, "message": "Data processing failed"}
    if "dc-import lint failed" in output:
        return {"code": "LINT_FAILED", "step": 2, "message": "lint failed"}
    if "Gemini review found issues" in output or "Step 0 found blocking issues" in output:
        return {"code": "GEMINI_BLOCKING", "step": 1, "message": "Gemini review found issues"}
    return None  # Order above defines priority when multiple phrases could match


def _normalize_failure_event(obj: dict) -> dict | None:
    """Validate and normalize a streamed failure event (t==='failure'). Returns dict with code, step, message, optional limit."""
    if not isinstance(obj, dict) or obj.get("t") != "failure":
        return None
    code = obj.get("code")
    step = obj.get("step")
    message = obj.get("message")
    if not code or not isinstance(message, str):
        return None
    out = {"code": str(code), "step": int(step) if isinstance(step, (int, float)) else None, "message": message}
    if obj.get("limit") is not None and isinstance(obj["limit"], (int, float)):
        out["limit"] = int(obj["limit"])
    return out


async def _stream_run_output(proc):
    """Stream process stdout line by line, yielding NDJSON. Detect steps and structured failure events."""
    output_lines = []
    cancelled = False
    last_failure = None  # First structured failure emitted by backend; used in done so we don't re-parse output
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace")
            text = _strip_ansi(text)
            output_lines.append(text)
            # Detect structured failure line (single-line JSON from backend)
            stripped = text.strip()
            if stripped.startswith("{") and '"t"' in stripped and '"failure"' in stripped:
                try:
                    obj = json.loads(stripped)
                    failure = _normalize_failure_event(obj)
                    if failure:
                        if last_failure is None:
                            last_failure = failure  # Keep first failure (root cause); ignore cascaded ones
                        yield json.dumps({"t": "failure", **failure}) + "\n"
                except (json.JSONDecodeError, TypeError):
                    pass
            step = None
            label = None
            _step_match = re.search(r"::STEP::(\d)(?::(.+))?", text)
            if _step_match:
                step = int(_step_match.group(1))
                if _step_match.group(2):
                    label = _step_match.group(2).strip()
            # Fallback only; ::STEP::N is authoritative. Backend log "Step N" = UI step N (0–4).
            elif "Pre-Import Checks" in text or "CSV quality" in text.lower():
                step = 0
            elif "Step 1" in text or ("Gemini review" in text and "model:" in text):
                step = 1
            elif "Step 2" in text or "Running dc-import genmcf" in text:
                step = 2
            elif "Step 3" in text or "Running import_validation" in text:
                step = 3
            elif "Step 4" in text or "HTML report:" in text or "Validation PASSED" in text or "Validation FAILED" in text:
                step = 4
            if step is not None:
                payload = {"t": "step", "step": step, "ts": time.time()}
                if label:
                    payload["label"] = label
                yield json.dumps(payload) + "\n"
            yield json.dumps({"t": "line", "v": text}) + "\n"
        await proc.wait()
    except asyncio.CancelledError:
        cancelled = True
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.wait()
        except ProcessLookupError:
            pass
        output_lines.append("[INFO] Validation cancelled by user.\n")
    finally:
        output = "".join(output_lines)
        done_payload = {
            "t": "done",
            "success": False if cancelled else (proc.returncode == 0),
            "exit_code": -1 if cancelled else (proc.returncode if proc.returncode is not None else -1),
            "output": output,
            "cancelled": cancelled,
            "ts_end": time.time(),
        }
        if not done_payload["success"] and not cancelled:
            failure = last_failure or _parse_failure(output)
            if failure:
                done_payload["failure_code"] = failure["code"]
                done_payload["failure_step"] = failure["step"]
                done_payload["failure_message"] = failure["message"]
                if failure.get("limit") is not None:
                    done_payload["failure_limit"] = failure["limit"]
        yield json.dumps(done_payload) + "\n"


async def run_validation_process(
    args: list,
    request: Request,
    config_path: Path | None,
    stream: bool,
    app_root: Path,
    output_dir: Path | None = None,
    dataset: str | None = None,
    canonical_output_dir: Path | None = None,
):
    """Run the validation script; stream NDJSON or wait and return result. Cleans up config_path on exit.
    If output_dir and dataset are set, uploads reports to GCS (when GCS_REPORTS_BUCKET is set), then
    copies artifacts to canonical_output_dir when it differs from output_dir (per-run isolation).
    """
    request_id = getattr(request.state, "request_id", "")
    env = {**os.environ, "RUN_ID": request_id} if request_id else None
    run_start_time = time.monotonic()
    timeout_sec = _run_timeout_sec()  # 0 = no timeout
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(app_root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
    )
    try:
        if stream:
            request_id = getattr(request.state, "request_id", "")
            run_timed_out = False

            async def gen():
                nonlocal proc, run_timed_out
                async def timeout_killer():
                    nonlocal run_timed_out
                    await asyncio.sleep(timeout_sec)
                    if proc.returncode is None:
                        run_timed_out = True
                        try:
                            proc.kill()
                        except ProcessLookupError:
                            pass
                        logger.warning("validation run timed out after %s sec request_id=%s", timeout_sec, request_id)

                timeout_task = asyncio.create_task(timeout_killer()) if timeout_sec > 0 else None
                done_yielded = False
                try:
                    async for chunk in _stream_run_output(proc):
                        try:
                            obj = json.loads(chunk) if chunk.strip() else {}
                            if obj.get("t") == "done":
                                done_yielded = True
                                if run_timed_out:
                                    obj["timeout"] = True
                                    obj["success"] = False
                                    obj["exit_code"] = -1
                                    obj["failure_code"] = "RUN_TIMEOUT"
                                    obj["failure_step"] = None
                                    obj["failure_message"] = "Validation run timed out."
                                duration_sec = round(time.monotonic() - run_start_time, 2)
                                logger.info(
                                    "run_finished request_id=%s success=%s cancelled=%s duration_sec=%s",
                                    request_id,
                                    obj.get("success"),
                                    obj.get("cancelled"),
                                    duration_sec,
                                )
                                # Add run_id so UI can fetch report from GCS
                                obj["run_id"] = request_id
                                # Upload to GCS whenever a report was produced (success or failure) so any instance can serve it
                                if output_dir and dataset:
                                    await asyncio.to_thread(
                                        _upload_reports_sync, output_dir, request_id, dataset
                                    )
                                    if canonical_output_dir and output_dir != canonical_output_dir:
                                        await asyncio.to_thread(
                                            _copy_run_to_canonical, output_dir, canonical_output_dir
                                        )
                                chunk = json.dumps(obj) + "\n"
                        except (json.JSONDecodeError, TypeError):
                            pass
                        yield chunk
                    # If we timed out and the subprocess never sent "done", emit a synthetic done so UI gets structured failure
                    if run_timed_out and not done_yielded:
                        duration_sec = round(time.monotonic() - run_start_time, 2)
                        logger.info(
                            "run_finished request_id=%s success=False timeout=True duration_sec=%s",
                            request_id,
                            duration_sec,
                        )
                        synthetic = {
                            "t": "done",
                            "success": False,
                            "exit_code": -1,
                            "timeout": True,
                            "run_id": request_id,
                            "failure_code": "RUN_TIMEOUT",
                            "failure_step": None,
                            "failure_message": "Validation run timed out.",
                        }
                        if output_dir and dataset:
                            await asyncio.to_thread(
                                _upload_reports_sync, output_dir, request_id, dataset
                            )
                            if canonical_output_dir and output_dir != canonical_output_dir:
                                await asyncio.to_thread(
                                    _copy_run_to_canonical, output_dir, canonical_output_dir
                                )
                        yield json.dumps(synthetic) + "\n"
                except asyncio.CancelledError:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
                    try:
                        await proc.wait()
                    except ProcessLookupError:
                        pass
                    duration_sec = round(time.monotonic() - run_start_time, 2)
                    logger.info(
                        "run_finished request_id=%s success=False cancelled=True duration_sec=%s",
                        request_id,
                        duration_sec,
                    )
                    yield json.dumps({
                        "t": "done",
                        "success": False,
                        "exit_code": -1,
                        "output": "[INFO] Validation cancelled by user.\n",
                        "cancelled": True,
                        "run_id": request_id,
                        "failure_code": "RUN_CANCELLED",
                        "failure_step": None,
                        "failure_message": "Validation cancelled by user.",
                    }) + "\n"
                finally:
                    if timeout_task is not None:
                        timeout_task.cancel()
                        try:
                            await timeout_task
                        except asyncio.CancelledError:
                            pass
                    if config_path and config_path.exists():
                        config_path.unlink(missing_ok=True)

            return StreamingResponse(
                gen(),
                media_type="application/x-ndjson",
                headers={"Cache-Control": "no-cache"},
            )

        async def wait_disconnect():
            while True:
                msg = await request.receive()
                if msg.get("type") == "http.disconnect":
                    return True

        proc_task = asyncio.create_task(proc.communicate())
        disconnect_task = asyncio.create_task(wait_disconnect())

        if timeout_sec > 0:
            try:
                done, pending = await asyncio.wait_for(
                    asyncio.wait(
                        [proc_task, disconnect_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    ),
                    timeout=timeout_sec,
                )
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await proc.wait()
                except ProcessLookupError:
                    pass
                for t in (proc_task, disconnect_task):
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
                request_id = getattr(request.state, "request_id", "")
                duration_sec = round(time.monotonic() - run_start_time, 2)
                logger.warning("validation run timed out after %s sec request_id=%s duration_sec=%s", timeout_sec, request_id, duration_sec)
                return {
                    "success": False,
                    "exit_code": -1,
                    "output": f"[ERROR] Validation run timed out after {timeout_sec} seconds.\n",
                    "run_id": request_id,
                    "timeout": True,
                    "failure_code": "RUN_TIMEOUT",
                    "failure_step": None,
                    "failure_message": "Validation run timed out.",
                }
        else:
            done, pending = await asyncio.wait(
                [proc_task, disconnect_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

        if disconnect_task in done:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await proc.wait()
            except ProcessLookupError:
                pass
            for t in pending:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            request_id = getattr(request.state, "request_id", "")
            duration_sec = round(time.monotonic() - run_start_time, 2)
            logger.info(
                "run_finished request_id=%s success=False cancelled=True duration_sec=%s",
                request_id,
                duration_sec,
            )
            return {
                "success": False,
                "exit_code": -1,
                "output": "[INFO] Validation cancelled by user.\n",
                "cancelled": True,
                "run_id": request_id,
                "failure_code": "RUN_CANCELLED",
                "failure_step": None,
                "failure_message": "Validation cancelled by user.",
            }

        stdout, _ = await proc_task
        disconnect_task.cancel()
        try:
            await disconnect_task
        except asyncio.CancelledError:
            pass

        output = stdout.decode("utf-8", errors="replace")
        output = _strip_ansi(output)
        exit_code = proc.returncode if proc.returncode is not None else -1
        success = proc.returncode == 0
        request_id = getattr(request.state, "request_id", "")
        duration_sec = round(time.monotonic() - run_start_time, 2)
        logger.info(
            "run_finished request_id=%s success=%s cancelled=False duration_sec=%s",
            request_id,
            success,
            duration_sec,
        )
        # Upload to GCS whenever a report was produced (success or failure) so any instance can serve it
        if output_dir and dataset:
            await asyncio.to_thread(
                _upload_reports_sync, output_dir, request_id, dataset
            )
            if canonical_output_dir and output_dir != canonical_output_dir:
                await asyncio.to_thread(
                    _copy_run_to_canonical, output_dir, canonical_output_dir
                )
        result = {
            "success": success,
            "exit_code": exit_code,
            "output": output,
            "run_id": request_id,
        }
        if not success:
            failure = _parse_failure(output)
            if failure:
                result["failure_code"] = failure["code"]
                result["failure_step"] = failure["step"]
                result["failure_message"] = failure["message"]
                if failure.get("limit") is not None:
                    result["failure_limit"] = failure["limit"]
        return result
    finally:
        if not stream and config_path and config_path.exists():
            config_path.unlink(missing_ok=True)
