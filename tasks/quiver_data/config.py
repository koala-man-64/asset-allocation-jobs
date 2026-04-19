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
    except Exception:
        return int(default)


def _csv(raw: str) -> tuple[str, ...]:
    text = _strip(raw)
    if not text:
        return ()
    return tuple(dict.fromkeys(part.strip().upper() for part in text.split(",") if part.strip()))


@dataclass(frozen=True)
class QuiverDataConfig:
    bronze_container: str
    silver_container: str
    gold_container: str
    historical_tickers: tuple[str, ...]
    page_size: int
    sec13f_today_only: bool

    @staticmethod
    def from_env() -> "QuiverDataConfig":
        return QuiverDataConfig(
            bronze_container=_env_text("AZURE_CONTAINER_BRONZE", "bronze"),
            silver_container=_env_text("AZURE_CONTAINER_SILVER", "silver"),
            gold_container=_env_text("AZURE_CONTAINER_GOLD", "gold"),
            historical_tickers=_csv(os.environ.get("QUIVER_DATA_TICKERS", "")),
            page_size=max(1, min(500, _env_int("QUIVER_DATA_PAGE_SIZE", 100))),
            sec13f_today_only=_env_text("QUIVER_DATA_SEC13F_TODAY_ONLY", "false").lower() in {"1", "true", "yes", "on"},
        )
