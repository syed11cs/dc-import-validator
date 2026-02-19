"""Logging layer for DC Import Validator: session ID, request_id, and environment-aware handlers.

When the service starts we assign a random server session ID and configure handlers by environment:

- Cloud Run (K_SERVICE set): StreamHandler(sys.stdout) only. Cloud Run captures stdout and
  sends it to Cloud Logging; no file handler (ephemeral disk) and no CloudLoggingHandler.
- Local / VM: TimedRotatingFileHandler to logs/dc_import_validator.log (daily rotation,
  keep 30 days). Optional StreamHandler for console during development.

Log level is read from LOG_LEVEL (default INFO). Example: LOG_LEVEL=DEBUG.

Each request (and each validation run) gets a request_id so logs can be correlated.
"""

import logging
import logging.handlers
import os
import sys
import uuid
from contextvars import ContextVar
from pathlib import Path

_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def _get_log_level() -> int:
    """Log level from LOG_LEVEL env (default INFO)."""
    raw = (os.environ.get("LOG_LEVEL") or "INFO").strip().upper()
    return _LOG_LEVELS.get(raw, logging.INFO)

# Server session ID: set once at startup.
_server_session_id: str = ""

# Request/run ID: set per request by middleware, so all logs in that request carry it.
_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")


def _new_session_id() -> str:
    """Return a short random session/request ID (12 hex chars)."""
    return uuid.uuid4().hex[:12]


def get_server_session_id() -> str:
    """Return the server session ID (set after configure_logging)."""
    return _server_session_id


def set_request_id(rid: str) -> None:
    _request_id_ctx.set(rid)


def get_request_id() -> str:
    return _request_id_ctx.get() or ""


def clear_request_id() -> None:
    try:
        _request_id_ctx.set("")
    except LookupError:
        pass


class SessionFilter(logging.Filter):
    """Add server_session_id and request_id to every LogRecord."""

    def __init__(self, server_session_id: str) -> None:
        super().__init__()
        self._server_session_id = server_session_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.server_session_id = getattr(record, "server_session_id", self._server_session_id)
        record.request_id = getattr(record, "request_id", get_request_id())
        return True


def configure_logging(app_root: Path) -> str:
    """Configure app-wide logging: stdout on Cloud Run, file (+ optional console) locally. Returns server session ID."""
    global _server_session_id
    _server_session_id = _new_session_id()
    level = _get_log_level()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] session=%(server_session_id)s request_id=%(request_id)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    session_filter = SessionFilter(_server_session_id)

    root = logging.getLogger()
    root.setLevel(level)
    # Avoid duplicate handlers if this is called more than once (e.g. reload).
    for h in root.handlers[:]:
        if getattr(h, "dc_import_validator_marker", False):
            root.removeHandler(h)

    is_cloud_run = bool(os.environ.get("K_SERVICE"))

    if is_cloud_run:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(session_filter)
        stream_handler.dc_import_validator_marker = True  # type: ignore[attr-defined]
        root.addHandler(stream_handler)
    else:
        logs_dir = app_root / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / "dc_import_validator.log"
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(session_filter)
        file_handler.dc_import_validator_marker = True  # type: ignore[attr-defined]
        root.addHandler(file_handler)
        # Optional console so devs see logs in terminal
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(session_filter)
        stream_handler.dc_import_validator_marker = True  # type: ignore[attr-defined]
        root.addHandler(stream_handler)

    return _server_session_id


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module (use __name__)."""
    return logging.getLogger(name)
