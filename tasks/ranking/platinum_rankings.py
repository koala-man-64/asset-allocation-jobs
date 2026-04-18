from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from asset_allocation_runtime_common.foundation.logging_config import configure_logging
from asset_allocation_runtime_common.ranking_engine.service import materialize_strategy_rankings
from asset_allocation_runtime_common.ranking_repository import RankingRepository
from tasks.common.system_health_markers import write_system_health_marker

logger = logging.getLogger(__name__)


def _configure_job_logging() -> None:
    os.environ.setdefault("LOG_LEVEL", "INFO")
    os.environ.setdefault("LOG_FORMAT", "JSON")
    configure_logging()


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def main() -> int:
    _configure_job_logging()
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required for ranking materialization.")

    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip() or None
    repo = RankingRepository(dsn)
    failures: list[str] = []
    completed_count = 0

    while True:
        work = repo.claim_next_refresh(execution_name=execution_name)
        if not work:
            if completed_count == 0:
                logger.info("No pending ranking refresh work found.")
            break

        strategy_name = str(work.get("strategyName") or "").strip()
        claim_token = str(work.get("claimToken") or "").strip()
        if not strategy_name or not claim_token:
            failures.append(strategy_name or "unknown")
            logger.error("Ranking refresh claim returned an invalid payload: %s", work)
            continue

        start_date = _parse_date(work.get("startDate"))
        end_date = _parse_date(work.get("endDate"))
        if start_date is None or end_date is None:
            try:
                repo.fail_refresh(
                    strategy_name,
                    claim_token=claim_token,
                    error="Ranking refresh work item was missing a valid date window.",
                )
            except Exception:
                logger.exception("Ranking refresh failure reporting failed.", extra={"context": {"strategyName": strategy_name}})
            failures.append(strategy_name)
            continue

        try:
            result = materialize_strategy_rankings(
                dsn,
                strategy_name=strategy_name,
                start_date=start_date,
                end_date=end_date,
                triggered_by="job",
            )
            repo.complete_refresh(
                strategy_name,
                claim_token=claim_token,
                run_id=str(result.get("runId") or "").strip() or None,
                dependency_fingerprint=str(work.get("dependencyFingerprint") or "").strip() or None,
                dependency_state=work.get("dependencyState") if isinstance(work.get("dependencyState"), dict) else None,
            )
            completed_count += 1
        except Exception as exc:
            failures.append(strategy_name)
            logger.exception(
                "Ranking materialization failed.",
                extra={"context": {"strategyName": strategy_name, "startDate": start_date, "endDate": end_date}},
            )
            try:
                repo.fail_refresh(strategy_name, claim_token=claim_token, error=str(exc))
            except Exception:
                logger.exception(
                    "Ranking refresh failure reporting failed.",
                    extra={"context": {"strategyName": strategy_name}},
                )
            continue

        status = str(result.get("status") or "success")
        message = "Ranking materialization skipped." if status == "noop" else "Ranking materialization complete."
        logger.info(
            message,
            extra={
                "context": {
                    "strategyName": result["strategyName"],
                    "rankingSchemaName": result["rankingSchemaName"],
                    "outputTableName": result["outputTableName"],
                    "startDate": result.get("startDate"),
                    "endDate": result.get("endDate"),
                    "previousWatermark": result.get("previousWatermark"),
                    "currentWatermark": result.get("currentWatermark"),
                    "rowCount": result["rowCount"],
                    "dateCount": result["dateCount"],
                    "runId": result["runId"],
                    "status": status,
                    "reason": result.get("reason"),
                }
            },
        )

    if completed_count > 0:
        write_system_health_marker(
            layer="platinum",
            domain="rankings",
            job_name="platinum-rankings-job",
            metadata={"completedCount": completed_count},
        )

    if failures:
        logger.error(
            "Ranking materialization completed with failures.",
            extra={
                "context": {
                    "failedStrategyCount": len(failures),
                    "failedStrategies": ",".join(sorted(set(failures))),
                }
            },
        )
        return 1
    return 0


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    _configure_job_logging()
    raise SystemExit(
        run_logged_job(
            job_name="platinum-rankings-job",
            run=main,
            log_info=logger.info,
            log_warning=logger.warning,
            log_error=logger.error,
            log_exception=logger.exception,
        )
    )
