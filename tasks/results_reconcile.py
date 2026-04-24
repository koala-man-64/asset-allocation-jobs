from __future__ import annotations

import logging
import os

from asset_allocation_runtime_common.results_repository import ResultsRepository

logger = logging.getLogger("asset-allocation.tasks.results")


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "t", "yes", "y", "on"}


def main() -> int:
    dry_run = _env_flag("RESULTS_RECONCILE_DRY_RUN", default=False)
    result = ResultsRepository().reconcile(dry_run=dry_run)
    logger.info(
        "results_reconcile dry_run=%s ranking_dirty=%s ranking_noop=%s canonical_enqueued=%s canonical_up_to_date=%s canonical_skipped=%s publication_signals_processed=%s publication_signals_error=%s error_count=%s",
        dry_run,
        result.get("rankingDirtyCount"),
        result.get("rankingNoopCount"),
        result.get("canonicalEnqueuedCount"),
        result.get("canonicalUpToDateCount"),
        result.get("canonicalSkippedCount"),
        result.get("publicationSignalsProcessedCount"),
        result.get("publicationSignalsErrorCount"),
        result.get("errorCount"),
    )
    return 1 if int(result.get("errorCount") or 0) > 0 else 0


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
