from __future__ import annotations

import logging
import os

from asset_allocation_runtime_common.backtest_repository import BacktestRepository

logger = logging.getLogger("asset-allocation.tasks.backtesting")


def main() -> int:
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip() or None
    result = BacktestRepository().reconcile_runs()
    logger.info(
        "backtest_lifecycle_event phase=reconcile execution_name=%s dispatched=%s dispatch_failed=%s "
        "failed_stale_running=%s skipped_active=%s no_action=%s",
        execution_name or "-",
        result.dispatchedCount,
        result.dispatchFailedCount,
        result.failedStaleRunningCount,
        result.skippedActiveCount,
        result.noActionCount,
    )
    return 0


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    raise SystemExit(
        run_logged_job(
            job_name="backtesting-reconcile-job",
            run=main,
            log_info=logger.info,
            log_warning=logger.warning,
            log_error=logger.error,
            log_exception=logger.exception,
        )
    )
