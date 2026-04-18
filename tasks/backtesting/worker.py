from __future__ import annotations

import logging
import os
import time as monotonic_time

from core.backtest_repository import BacktestRepository
from core.backtest_runtime import execute_backtest_run
from core.control_plane_transport import ControlPlaneTransport
from core.postgres import connect

logger = logging.getLogger("asset-allocation.tasks.backtesting")


def _require_env(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


def _log_lifecycle(phase: str, **fields: object) -> None:
    parts = [f"phase={phase}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    logger.info("backtest_lifecycle_event %s", " ".join(parts))


def _probe_postgres(dsn: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()


def _probe_control_plane(transport: ControlPlaneTransport) -> None:
    transport.probe("/api/internal/backtests/ready")


def _log_stage_timing(phase: str, started_at: float, **fields: object) -> None:
    duration = monotonic_time.monotonic() - started_at
    _log_lifecycle(phase, duration_sec=f"{duration:.2f}", **fields)


def preflight_dependencies(*, dsn: str, execution_name: str | None, explicit_run_id: str | None) -> None:
    transport: ControlPlaneTransport | None = None
    preflight_started_at = monotonic_time.monotonic()
    try:
        _log_lifecycle(
            "preflight_start",
            execution_name=execution_name,
            run_id=explicit_run_id,
        )
        transport = ControlPlaneTransport.from_env()
        _probe_postgres(dsn)
        _log_stage_timing(
            "preflight_postgres_ok",
            preflight_started_at,
            execution_name=execution_name,
            run_id=explicit_run_id,
        )
        _probe_control_plane(transport)
        _log_stage_timing(
            "preflight_control_plane_ok",
            preflight_started_at,
            execution_name=execution_name,
            run_id=explicit_run_id,
        )
        _log_lifecycle("preflight_ok", execution_name=execution_name, run_id=explicit_run_id)
    except Exception as exc:
        logger.error(
            "backtest_preflight_failed phase=preflight execution_name=%s run_id=%s error=%s",
            execution_name or "-",
            explicit_run_id or "-",
            exc,
        )
        raise
    finally:
        if transport is not None:
            transport.close()


def main() -> int:
    dsn = _require_env("POSTGRES_DSN")
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip() or None
    explicit_run_id = str(os.environ.get("BACKTEST_RUN_ID") or "").strip() or None
    try:
        preflight_dependencies(dsn=dsn, execution_name=execution_name, explicit_run_id=explicit_run_id)
    except Exception:
        return 1

    repo = BacktestRepository(dsn)

    if explicit_run_id:
        run = repo.get_run(explicit_run_id)
        if not run:
            raise ValueError(f"Run '{explicit_run_id}' not found.")
        run_id = explicit_run_id
    else:
        claimed = repo.claim_next_run(execution_name=execution_name)
        if not claimed:
            logger.info("No queued backtest runs found.")
            return 0
        run_id = str(claimed["run_id"])
        _log_lifecycle(
            "claim",
            run_id=run_id,
            execution_name=execution_name,
            attempt_count=claimed.get("attempt_count"),
            status=claimed.get("status"),
        )

    try:
        result = execute_backtest_run(dsn, run_id=run_id, execution_name=execution_name)
        _log_lifecycle(
            "complete",
            run_id=run_id,
            execution_name=execution_name,
            trades=result.get("summary", {}).get("trades"),
            final_equity=result.get("summary", {}).get("final_equity"),
        )
        logger.info(
            "Backtest run completed: run_id=%s trades=%s final_equity=%s",
            run_id,
            result.get("summary", {}).get("trades"),
            result.get("summary", {}).get("final_equity"),
        )
        return 0
    except Exception as exc:
        logger.exception("Backtest run failed: run_id=%s", run_id)
        _log_lifecycle(
            "fail",
            run_id=run_id,
            execution_name=execution_name,
            failure_reason=str(exc),
        )
        try:
            repo.fail_run(run_id, error=str(exc))
        except Exception:
            logger.exception("Backtest failure reporting failed: run_id=%s", run_id)
        return 1


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    raise SystemExit(
        run_logged_job(
            job_name="backtesting-worker-job",
            run=main,
            log_info=logger.info,
            log_warning=logger.warning,
            log_error=logger.error,
            log_exception=logger.exception,
        )
    )
