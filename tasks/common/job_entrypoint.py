from __future__ import annotations

import os
import socket
import sys
import time
import traceback
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from asset_allocation_runtime_common.market_data import core as mdc
_LogFn = Callable[[str], None]
_RunFn = Callable[[], int | None]
_CallbackFn = Callable[[], Any]


def _default_info(message: str) -> None:
    mdc.write_line(message)


def _default_warning(message: str) -> None:
    mdc.write_warning(message)


def _default_error(message: str) -> None:
    mdc.write_error(message)


def _callback_name(callback: _CallbackFn) -> str:
    raw_name = getattr(callback, "__name__", "") or getattr(callback, "__qualname__", "")
    name = str(raw_name).strip()
    if name:
        return name
    return callback.__class__.__name__


def _safe_url_for_log(raw: object) -> str:
    value = str(raw or "").strip()
    if not value:
        return "-"
    normalized = value if "://" in value else f"http://{value}"
    try:
        parsed = urlparse(normalized)
    except Exception:
        return value
    if not parsed.scheme or not parsed.netloc:
        return value
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}" if path else f"{parsed.scheme}://{parsed.netloc}"


def _format_kv(**fields: object) -> str:
    parts: list[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    return " ".join(parts)


def _job_runtime_context(job_name: str, callback_count: int) -> str:
    return _format_kv(
        job=job_name,
        pid=os.getpid(),
        host=socket.gethostname(),
        python=sys.version.split()[0],
        api_base_url=_safe_url_for_log(os.environ.get("ASSET_ALLOCATION_API_BASE_URL")),
        execution_name=os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME"),
        replica=os.environ.get("CONTAINER_APP_REPLICA_NAME"),
        next_jobs=(os.environ.get("TRIGGER_NEXT_JOB_NAME") or "").replace(" ", ""),
        log_level=os.environ.get("LOG_LEVEL"),
        log_format=os.environ.get("LOG_FORMAT"),
        success_callbacks=callback_count,
    )


def run_logged_job(
    *,
    job_name: str,
    run: _RunFn,
    on_success: Sequence[_CallbackFn] = (),
    log_info: _LogFn | None = None,
    log_warning: _LogFn | None = None,
    log_error: _LogFn | None = None,
    log_exception: _LogFn | None = None,
) -> int:
    info = log_info or _default_info
    warning = log_warning or _default_warning
    error = log_error or _default_error
    callbacks = tuple(on_success)

    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    info(f"Job runtime context: {_job_runtime_context(job_name, len(callbacks))}")
    info(f"Job started: job={job_name} started_at={started_at.isoformat()}")
    info(f"Job body starting: job={job_name}")

    try:
        raw_exit_code = run()
        exit_code = 0 if raw_exit_code is None else int(raw_exit_code)
        finished_at = datetime.now(timezone.utc)
        elapsed = time.perf_counter() - started
        info(
            "Job body finished: "
            f"job={job_name} raw_exit_code={raw_exit_code!r} exit_code={exit_code} "
            f"finished_at={finished_at.isoformat()} elapsed_sec={elapsed:.2f}"
        )
        if exit_code == 0:
            if callbacks:
                info(f"Running success callbacks: job={job_name} callback_count={len(callbacks)}")
            for index, callback in enumerate(callbacks, start=1):
                callback_name = _callback_name(callback)
                callback_started = time.perf_counter()
                info(
                    "Success callback starting: "
                    f"job={job_name} callback={callback_name} index={index}/{len(callbacks)}"
                )
                callback()
                callback_elapsed = time.perf_counter() - callback_started
                info(
                    "Success callback completed: "
                    f"job={job_name} callback={callback_name} index={index}/{len(callbacks)} "
                    f"elapsed_sec={callback_elapsed:.2f}"
                )
            info(
                "Job completed successfully: "
                f"job={job_name} exit_code={exit_code} finished_at={finished_at.isoformat()} "
                f"elapsed_sec={elapsed:.2f}"
            )
            return exit_code

        if callbacks:
            warning(
                "Skipping success callbacks: "
                f"job={job_name} exit_code={exit_code} callback_count={len(callbacks)}"
            )
        warning(
            "Job completed with failures: "
            f"job={job_name} exit_code={exit_code} finished_at={finished_at.isoformat()} "
            f"elapsed_sec={elapsed:.2f}"
        )
        return exit_code
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        elapsed = time.perf_counter() - started
        message = (
            "Job failed with exception: "
            f"job={job_name} error={type(exc).__name__}: {exc} "
            f"finished_at={finished_at.isoformat()} elapsed_sec={elapsed:.2f}"
        )
        if log_exception is not None:
            log_exception(message)
        else:
            error(f"{message}\n{traceback.format_exc()}")
        raise
