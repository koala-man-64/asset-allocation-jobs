from __future__ import annotations

import logging
import os

from asset_allocation_runtime_common.results_repository import ResultsRepository

logger = logging.getLogger("asset-allocation.tasks.results")


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


def main() -> int:
    dry_run = _env_flag("RESULTS_RECONCILE_DRY_RUN", default=False)
    result = ResultsRepository().reconcile(dry_run=dry_run)
    logger.info(
        "results_reconcile dry_run=%s ranking_dirty=%s ranking_noop=%s canonical_enqueued=%s canonical_up_to_date=%s canonical_skipped=%s publication_signals_processed=%s publication_signals_error=%s error_count=%s",
        dry_run,
        result.rankingDirtyCount,
        result.rankingNoopCount,
        result.canonicalEnqueuedCount,
        result.canonicalUpToDateCount,
        result.canonicalSkippedCount,
        result.publicationSignalsProcessedCount,
        result.publicationSignalsErrorCount,
        result.errorCount,
    )
    if result.errorCount > 0:
        logger.error("results_reconcile errors=%s", result.errors)
        return 1
    return 0


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    raise SystemExit(
        run_logged_job(
            job_name="results-reconcile-job",
            run=main,
            log_info=logger.info,
            log_warning=logger.warning,
            log_error=logger.error,
            log_exception=logger.exception,
        )
    )
