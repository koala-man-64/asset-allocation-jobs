from __future__ import annotations

import logging
import os
from typing import Any

from core.logging_config import configure_logging
from core.ranking_engine.service import materialize_strategy_rankings
from core.ranking_repository import RankingRepository
from core.strategy_repository import StrategyRepository

logger = logging.getLogger(__name__)


def _configure_job_logging() -> None:
    os.environ.setdefault("LOG_LEVEL", "INFO")
    os.environ.setdefault("LOG_FORMAT", "JSON")
    configure_logging()


def _resolve_strategy_candidates(dsn: str) -> list[dict[str, Any]]:
    strategy_repo = StrategyRepository(dsn)
    ranking_repo = RankingRepository(dsn)
    available_schema_names = {
        str(row.get("name") or "").strip()
        for row in ranking_repo.list_ranking_schemas()
        if str(row.get("name") or "").strip()
    }
    candidates: dict[str, dict[str, Any]] = {}
    for strategy in strategy_repo.list_strategies():
        strategy_name = str(strategy.get("name") or "").strip()
        if not strategy_name:
            continue
        detail = dict(strategy) if strategy.get("config") is not None else strategy_repo.get_strategy(strategy_name)
        config = (detail or {}).get("config") or {}
        schema_name = str(config.get("rankingSchemaName") or "").strip()
        if schema_name and schema_name in available_schema_names:
            candidates[strategy_name] = {"name": strategy_name, **dict(detail or {})}
    return [candidates[name] for name in sorted(candidates)]


def main() -> int:
    _configure_job_logging()
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required for ranking materialization.")

    strategy_candidates = _resolve_strategy_candidates(dsn)
    if not strategy_candidates:
        logger.info("No ranking-enabled strategies found to materialize.")
        return 0

    failures: list[str] = []
    for strategy in strategy_candidates:
        strategy_name = str(strategy.get("name") or "").strip()
        if not strategy_name:
            continue
        try:
            result = materialize_strategy_rankings(
                dsn,
                strategy_name=strategy_name,
                triggered_by="job",
                strategy_payload=strategy,
            )
        except Exception:
            failures.append(strategy_name)
            logger.exception(
                "Ranking materialization failed.",
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

    if failures:
        logger.error(
            "Ranking materialization completed with failures.",
            extra={
                "context": {
                    "failedStrategyCount": len(failures),
                    "failedStrategies": ",".join(sorted(failures)),
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
