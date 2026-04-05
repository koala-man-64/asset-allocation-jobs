from __future__ import annotations

import logging
import os
from datetime import date

from core.logging_config import configure_logging
from core.ranking_engine.service import materialize_strategy_rankings
from core.ranking_repository import RankingRepository
from core.strategy_repository import StrategyRepository

logger = logging.getLogger(__name__)


def _configure_job_logging() -> None:
    os.environ.setdefault("LOG_LEVEL", "INFO")
    os.environ.setdefault("LOG_FORMAT", "JSON")
    configure_logging()


def _parse_date(value: str | None) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return date.fromisoformat(raw)


def _resolve_strategies(dsn: str, explicit_name: str | None) -> list[str]:
    if explicit_name:
        return [explicit_name]

    strategy_repo = StrategyRepository(dsn)
    ranking_repo = RankingRepository(dsn)
    available_schema_names = {row["name"] for row in ranking_repo.list_ranking_schemas()}
    names: list[str] = []
    for strategy in strategy_repo.list_strategies():
        detail = strategy_repo.get_strategy(str(strategy["name"]))
        config = (detail or {}).get("config") or {}
        schema_name = str(config.get("rankingSchemaName") or "").strip()
        if schema_name and schema_name in available_schema_names:
            names.append(str(strategy["name"]))
    return names


def main() -> int:
    _configure_job_logging()
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required for ranking materialization.")

    strategy_name = str(os.environ.get("RANKING_STRATEGY_NAME") or "").strip() or None
    start_date = _parse_date(os.environ.get("RANKING_START_DATE"))
    end_date = _parse_date(os.environ.get("RANKING_END_DATE"))

    strategy_names = _resolve_strategies(dsn, strategy_name)
    if not strategy_names:
        logger.info("No ranking-enabled strategies found to materialize.")
        return 0

    for name in strategy_names:
        result = materialize_strategy_rankings(
            dsn,
            strategy_name=name,
            start_date=start_date,
            end_date=end_date,
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
