from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Iterable, Literal

from asset_allocation_runtime_common.market_data import layer_bucketing
from asset_allocation_runtime_common.shared_core.config import parse_debug_symbols

SCOPE_MODE_ENV = "MARKET_REFRESH_SCOPE_MODE"
SCOPE_SYMBOLS_ENV = "MARKET_REFRESH_SCOPE_SYMBOLS"
LEGACY_DEBUG_SYMBOLS_ENV = "DEBUG_SYMBOLS"

ScopeMode = Literal["scheduled", "intraday"]
ReconciliationMode = Literal["full_domain", "scoped"]


def normalize_scope_symbols(values: object) -> tuple[str, ...]:
    if values is None:
        return tuple()
    if isinstance(values, str):
        raw_values: Iterable[object] = parse_debug_symbols(values)
    else:
        raw_values = values if isinstance(values, Iterable) else (values,)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return tuple(normalized)


@dataclass(frozen=True)
class MarketRefreshScope:
    mode: ScopeMode
    symbols: tuple[str, ...] = tuple()

    @property
    def is_intraday(self) -> bool:
        return self.mode == "intraday"

    @property
    def is_scoped(self) -> bool:
        return self.is_intraday and bool(self.symbols)

    @property
    def buckets(self) -> frozenset[str]:
        return frozenset(layer_bucketing.bucket_letter(symbol) for symbol in self.symbols)

    @property
    def symbol_set(self) -> frozenset[str]:
        return frozenset(self.symbols)


@dataclass(frozen=True)
class ReconciliationScope:
    mode: ReconciliationMode
    touched_symbols: frozenset[str] = frozenset()
    touched_buckets: frozenset[str] = frozenset()
    protected_symbols: frozenset[str] = frozenset()

    @classmethod
    def full_domain(cls, *, protected_symbols: Iterable[str] = ()) -> "ReconciliationScope":
        return cls(
            mode="full_domain",
            protected_symbols=frozenset(normalize_scope_symbols(protected_symbols)),
        )

    @classmethod
    def from_market_refresh_scope(
        cls,
        scope: MarketRefreshScope,
        *,
        protected_symbols: Iterable[str] = (),
    ) -> "ReconciliationScope":
        if not scope.is_scoped:
            return cls.full_domain(protected_symbols=protected_symbols)
        return cls(
            mode="scoped",
            touched_symbols=scope.symbol_set,
            touched_buckets=scope.buckets,
            protected_symbols=frozenset(normalize_scope_symbols(protected_symbols)),
        )

    @property
    def is_scoped(self) -> bool:
        return self.mode == "scoped"


def current_market_refresh_scope() -> MarketRefreshScope:
    raw_mode = str(os.environ.get(SCOPE_MODE_ENV) or "scheduled").strip().lower()
    if raw_mode not in {"scheduled", "intraday"}:
        raise ValueError(f"{SCOPE_MODE_ENV} must be 'scheduled' or 'intraday', got {raw_mode!r}.")

    symbols = normalize_scope_symbols(os.environ.get(SCOPE_SYMBOLS_ENV))
    if not symbols:
        legacy_symbols = normalize_scope_symbols(os.environ.get(LEGACY_DEBUG_SYMBOLS_ENV))
        if legacy_symbols:
            symbols = legacy_symbols
            raw_mode = "intraday"

    if raw_mode == "intraday" and not symbols:
        raise ValueError(f"{SCOPE_MODE_ENV}=intraday requires {SCOPE_SYMBOLS_ENV}.")

    return MarketRefreshScope(mode=raw_mode, symbols=symbols)


def current_reconciliation_scope(*, protected_symbols: Iterable[str] = ()) -> ReconciliationScope:
    return ReconciliationScope.from_market_refresh_scope(
        current_market_refresh_scope(),
        protected_symbols=protected_symbols,
    )

