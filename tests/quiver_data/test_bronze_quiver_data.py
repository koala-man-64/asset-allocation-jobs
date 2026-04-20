from __future__ import annotations

from tasks.quiver_data.bronze_quiver_data import _build_requests, plan_symbol_batch
from tasks.quiver_data.config import QuiverDataConfig


class _FakeClient:
    def get_live_congress_trading(self):
        return []

    def get_live_senate_trading(self):
        return []

    def get_live_house_trading(self):
        return []

    def get_live_gov_contracts(self):
        return []

    def get_live_gov_contracts_all(self, **_kwargs):
        return []

    def get_live_lobbying(self, **_kwargs):
        return []

    def get_live_congress_holdings(self):
        return []

    def get_historical_congress_trading(self, *, ticker):
        return [{"Ticker": ticker}]

    def get_historical_senate_trading(self, *, ticker):
        return [{"Ticker": ticker}]

    def get_historical_house_trading(self, *, ticker):
        return [{"Ticker": ticker}]

    def get_historical_gov_contracts(self, *, ticker):
        return [{"Ticker": ticker}]

    def get_historical_gov_contracts_all(self, *, ticker):
        return [{"Ticker": ticker}]

    def get_live_insiders(self, **_kwargs):
        return []

    def get_live_sec13f(self, **_kwargs):
        return []

    def get_live_sec13f_changes(self, **_kwargs):
        return []

    def get_historical_lobbying(self, **_kwargs):
        return []

    def get_live_etf_holdings(self, **_kwargs):
        return []


def _config(**overrides) -> QuiverDataConfig:
    base = {
        "bronze_container": "bronze",
        "silver_container": "silver",
        "gold_container": "gold",
        "universe_source": "env_tickers",
        "job_mode": "incremental",
        "configured_tickers": ("AAPL", "MSFT"),
        "ticker_batch_size": 2,
        "historical_batch_size": 1,
        "symbol_limit": 500,
        "page_size": 100,
        "sec13f_today_only": True,
        "postgres_dsn": None,
    }
    base.update(overrides)
    return QuiverDataConfig(**base)


def test_build_requests_incremental_includes_global_and_rotating_ticker_feeds() -> None:
    requests = _build_requests(_FakeClient(), _config(), selected_symbols=("AAPL", "MSFT"))

    ids = [(dataset, ticker) for dataset, _family, ticker, _callback in requests]
    assert ("congress_trading_live", None) in ids
    assert ("government_contracts_live", None) in ids
    assert ("congress_holdings_live", None) in ids
    assert ("insiders_live", "AAPL") in ids
    assert ("sec13fchanges_live", "MSFT") in ids
    assert ("etf_holdings_live", "MSFT") in ids
    assert ("congress_trading_historical", "AAPL") not in ids


def test_build_requests_historical_backfill_only_includes_historical_ticker_feeds() -> None:
    requests = _build_requests(
        _FakeClient(),
        _config(job_mode="historical_backfill", historical_batch_size=2),
        selected_symbols=("AAPL", "MSFT"),
    )

    ids = [(dataset, ticker) for dataset, _family, ticker, _callback in requests]
    assert ("congress_trading_historical", "AAPL") in ids
    assert ("government_contracts_all_historical", "MSFT") in ids
    assert ("lobbying_historical", "MSFT") in ids
    assert ("congress_trading_live", None) not in ids
    assert ("insiders_live", "AAPL") not in ids


def test_plan_symbol_batch_rotates_and_wraps_from_saved_cursor() -> None:
    plan = plan_symbol_batch(
        _config(ticker_batch_size=3),
        universe_symbols=("AAPL", "AMZN", "GOOG", "MSFT"),
        cursor_next=2,
    )

    assert plan.selected_symbols == ("GOOG", "MSFT", "AAPL")
    assert plan.cursor_start == 2
    assert plan.cursor_end == 0
    assert plan.cursor_next == 1


def test_plan_symbol_batch_uses_historical_batch_size_for_backfill_mode() -> None:
    plan = plan_symbol_batch(
        _config(job_mode="historical_backfill", ticker_batch_size=5, historical_batch_size=2),
        universe_symbols=("AAPL", "AMZN", "GOOG"),
        cursor_next=0,
    )

    assert plan.batch_size == 2
    assert plan.selected_symbols == ("AAPL", "AMZN")
