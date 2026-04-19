from __future__ import annotations

from tasks.quiver_data.bronze_quiver_data import _build_requests
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


def test_build_requests_includes_live_and_ticker_scoped_feeds() -> None:
    config = QuiverDataConfig(
        bronze_container="bronze",
        silver_container="silver",
        gold_container="gold",
        historical_tickers=("AAPL", "MSFT"),
        page_size=100,
        sec13f_today_only=False,
    )

    requests = _build_requests(_FakeClient(), config)

    ids = [(dataset, ticker) for dataset, _family, ticker, _callback in requests]
    assert ("congress_trading_live", None) in ids
    assert ("insiders_live", "AAPL") in ids
    assert ("etf_holdings_live", "MSFT") in ids
