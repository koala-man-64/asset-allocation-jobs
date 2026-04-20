from __future__ import annotations

import re
from typing import Iterable

from asset_allocation_runtime_common.foundation.postgres import connect

from tasks.quiver_data.config import QuiverDataConfig

_ACTIVE_STATUS_TOKENS = ("active", "listed", "trading", "tradable")
_COMMON_EQUITY_TOKENS = ("common stock", "common_equity", "common equity", "equity", "stock", "common", "cs")
_EXCLUDED_ASSET_TYPE_TOKENS = (
    "etf",
    "fund",
    "adr",
    "depositary",
    "preferred",
    "warrant",
    "unit",
)


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_text(value: object) -> str:
    return str(value or "").strip().lower()


def _is_supported_status(value: object) -> bool:
    status = _normalize_text(value)
    if not status:
        return False
    tokens = tuple(token for token in re.split(r"[^a-z]+", status) if token)
    return status in _ACTIVE_STATUS_TOKENS or any(token in _ACTIVE_STATUS_TOKENS for token in tokens)


def _is_common_equity_asset_type(value: object) -> bool:
    asset_type = _normalize_text(value)
    if not asset_type:
        return False
    if any(token in asset_type for token in _EXCLUDED_ASSET_TYPE_TOKENS):
        return False
    return any(token in asset_type for token in _COMMON_EQUITY_TOKENS)


def _filter_symbols(rows: Iterable[tuple[object, object, object]]) -> tuple[str, ...]:
    symbols: list[str] = []
    for raw_symbol, raw_status, raw_asset_type in rows:
        symbol = _normalize_symbol(raw_symbol)
        if not symbol or "." in symbol:
            continue
        if symbol != symbol.upper():
            continue
        if not _is_supported_status(raw_status):
            continue
        if not _is_common_equity_asset_type(raw_asset_type):
            continue
        symbols.append(symbol)
    return tuple(sorted(dict.fromkeys(symbols)))


def load_core_symbols(*, dsn: str, symbol_limit: int = 0) -> tuple[str, ...]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    symbol,
                    COALESCE(status, '') AS status,
                    COALESCE(asset_type, '') AS asset_type
                FROM core.symbols
                ORDER BY symbol ASC
                """
            )
            rows = cur.fetchall()

    filtered = _filter_symbols(rows)
    if symbol_limit > 0:
        return filtered[:symbol_limit]
    return filtered


def resolve_symbol_universe(config: QuiverDataConfig) -> tuple[str, ...]:
    if config.universe_source == "env_tickers":
        return config.configured_tickers

    dsn = str(config.postgres_dsn or "").strip()
    if not dsn:
        raise ValueError("POSTGRES_DSN is required when QUIVER_DATA_UNIVERSE_SOURCE=core_symbols.")
    return load_core_symbols(dsn=dsn, symbol_limit=config.symbol_limit)
