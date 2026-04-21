from __future__ import annotations

import os
from dataclasses import dataclass


def _strip(value: object) -> str:
    return str(value or "").strip()


def _env_text(name: str, default: str = "") -> str:
    value = _strip(os.environ.get(name))
    return value or default


def _env_int(name: str, default: int) -> int:
    raw = _strip(os.environ.get(name))
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be an integer.") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = _strip(os.environ.get(name))
    if not raw:
        return bool(default)
    return raw.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class QuiverDataConfig:
    bronze_container: str
    silver_container: str
    gold_container: str
    job_mode: str
    ticker_batch_size: int
    historical_batch_size: int
    symbol_limit: int
    page_size: int
    sec13f_today_only: bool
    postgres_dsn: str | None

    def symbol_batch_size(self) -> int:
        if self.job_mode == "historical_backfill":
            return self.historical_batch_size
        return self.ticker_batch_size

    @staticmethod
    def from_env() -> "QuiverDataConfig":
        job_mode = _env_text("QUIVER_DATA_JOB_MODE", "incremental").lower()
        if job_mode not in {"incremental", "historical_backfill"}:
            raise ValueError("QUIVER_DATA_JOB_MODE must be one of: incremental, historical_backfill.")

        return QuiverDataConfig(
            bronze_container=_env_text("AZURE_CONTAINER_BRONZE", "bronze"),
            silver_container=_env_text("AZURE_CONTAINER_SILVER", "silver"),
            gold_container=_env_text("AZURE_CONTAINER_GOLD", "gold"),
            job_mode=job_mode,
            ticker_batch_size=max(1, _env_int("QUIVER_DATA_TICKER_BATCH_SIZE", 50)),
            historical_batch_size=max(1, _env_int("QUIVER_DATA_HISTORICAL_BATCH_SIZE", 20)),
            symbol_limit=max(0, _env_int("QUIVER_DATA_SYMBOL_LIMIT", 500)),
            page_size=max(1, min(500, _env_int("QUIVER_DATA_PAGE_SIZE", 100))),
            sec13f_today_only=_env_bool("QUIVER_DATA_SEC13F_TODAY_ONLY", True),
            postgres_dsn=_env_text("POSTGRES_DSN") or None,
        )
