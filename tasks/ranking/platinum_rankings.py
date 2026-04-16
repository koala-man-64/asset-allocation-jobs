from __future__ import annotations

import logging
import os

from core.logging_config import configure_logging
from core.ranking_engine.service import materialize_strategy_rankings
from core.ranking_repository import RankingRepository
from core.strategy_repository import StrategyRepository

logger = logging.getLogger(__name__)


def _configure_job_logging() -> None:
    os.environ.setdefault("LOG_LEVEL", "INFO")
    os.environ.setdefault("LOG_FORMAT", "JSON")
    configure_logging()


def _resolve_strategy_names(dsn: str) -> list[str]:
    strategy_repo = StrategyRepository(dsn)
    ranking_repo = RankingRepository(dsn)
    available_schema_names = {row["name"] for row in ranking_repo.list_ranking_schemas()}
    names: set[str] = set()
    for strategy in strategy_repo.list_strategies():
        strategy_name = str(strategy.get("name") or "").strip()
        if not strategy_name:
            continue
        detail = strategy_repo.get_strategy(strategy_name)
        config = (detail or {}).get("config") or {}
        schema_name = str(config.get("rankingSchemaName") or "").strip()
        if schema_name and schema_name in available_schema_names:
            names.add(strategy_name)
    return sorted(names)


def main() -> int:
    _configure_job_logging()
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required for ranking materialization.")

    strategy_names = _resolve_strategy_names(dsn)
    if not strategy_names:
        logger.info("No ranking-enabled strategies found to materialize.")
        return 0

    for name in strategy_names:
        result = materialize_strategy_rankings(
            dsn,
            strategy_name=name,
            triggered_by="job",
        )
        logger.info(
            "Ranking materialization complete.",
            extra={
                "context": {
                    "strategyName": result["strategyName"],
                    "rankingSchemaName": result["rankingSchemaName"],
                    "outputTableName": result["outputTableName"],
                    "rowCount": result["rowCount"],
                    "dateCount": result["dateCount"],
                    "runId": result["runId"],
                }
            },
        )
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
