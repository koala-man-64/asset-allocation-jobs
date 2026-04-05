from __future__ import annotations

import logging
import os

from core.backtest_repository import BacktestRepository
from core.backtest_runtime import execute_backtest_run

logger = logging.getLogger("asset-allocation.tasks.backtesting")


def _require_env(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


def main() -> int:
    dsn = _require_env("POSTGRES_DSN")
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip() or None
    explicit_run_id = str(os.environ.get("BACKTEST_RUN_ID") or "").strip() or None
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

    try:
        result = execute_backtest_run(dsn, run_id=run_id, execution_name=execution_name)
        logger.info(
            "Backtest run completed: run_id=%s trades=%s final_equity=%s",
            run_id,
            result.get("summary", {}).get("trades"),
            result.get("summary", {}).get("final_equity"),
        )
        return 0
    except Exception as exc:
        logger.exception("Backtest run failed: run_id=%s", run_id)
        repo.fail_run(run_id, error=str(exc))
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
