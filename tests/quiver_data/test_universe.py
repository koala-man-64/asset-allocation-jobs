from __future__ import annotations

import pytest

from tasks.quiver_data.config import QuiverDataConfig
from tasks.quiver_data.universe import load_core_symbols, resolve_symbol_universe


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchall(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, rows):
        self._cursor = _FakeCursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self):
        return self._cursor


def _config(**overrides) -> QuiverDataConfig:
    base = {
        "bronze_container": "bronze",
        "silver_container": "silver",
        "gold_container": "gold",
        "job_mode": "incremental",
        "ticker_batch_size": 50,
        "historical_batch_size": 20,
        "symbol_limit": 500,
        "page_size": 100,
        "sec13f_today_only": True,
        "postgres_dsn": "postgresql://example",
    }
    base.update(overrides)
    return QuiverDataConfig(**base)


def test_load_core_symbols_filters_to_supported_common_equities(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        ("aapl", "Active", "Common Stock"),
        ("TSLA", "Trading", "Equity"),
        ("SPY", "Active", "ETF"),
        ("BRK.B", "Active", "Common Stock"),
        ("PREF", "Active", "Preferred Stock"),
        ("ADR1", "Active", "ADR"),
        ("UNIT1", "Active", "Unit"),
        ("QQQ", "Listed", "Fund"),
        ("HALT", "Inactive", "Common Stock"),
        ("", "Active", "Common Stock"),
    ]
    monkeypatch.setattr("tasks.quiver_data.universe.connect", lambda dsn: _FakeConnection(rows))

    symbols = load_core_symbols(dsn="postgresql://example")

    assert symbols == ("AAPL", "TSLA")


def test_load_core_symbols_applies_symbol_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        ("MSFT", "Active", "Common Stock"),
        ("AAPL", "Active", "Common Stock"),
        ("NVDA", "Active", "Common Stock"),
    ]
    monkeypatch.setattr("tasks.quiver_data.universe.connect", lambda dsn: _FakeConnection(rows))

    symbols = load_core_symbols(dsn="postgresql://example", symbol_limit=2)

    assert symbols == ("AAPL", "MSFT")


def test_resolve_symbol_universe_requires_postgres_for_core_symbols() -> None:
    with pytest.raises(ValueError, match="POSTGRES_DSN"):
        resolve_symbol_universe(_config(postgres_dsn=None))
