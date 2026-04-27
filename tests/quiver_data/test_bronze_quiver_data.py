from __future__ import annotations

import pytest

from tasks.quiver_data import bronze_quiver_data as bronze
from tasks.quiver_data import constants
from tasks.quiver_data.bronze_quiver_data import PaginationLimitExceeded, QuiverSourceRequest, _build_requests, plan_symbol_batch
from tasks.quiver_data.config import QuiverDataConfig


class _FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.pages: dict[str, dict[int, list[dict[str, object]]]] = {}
        self.single_payloads: dict[str, list[dict[str, object]]] = {}

    def _single(self, name: str, **kwargs: object) -> list[dict[str, object]]:
        self.calls.append((name, dict(kwargs)))
        return list(self.single_payloads.get(name, []))

    def _paged(self, name: str, **kwargs: object) -> list[dict[str, object]]:
        self.calls.append((name, dict(kwargs)))
        page = int(kwargs.get("page") or 1)
        return list(self.pages.get(name, {}).get(page, []))

    def get_live_congress_trading(self):
        return self._single("get_live_congress_trading")

    def get_live_senate_trading(self):
        return self._single("get_live_senate_trading")

    def get_live_house_trading(self):
        return self._single("get_live_house_trading")

    def get_live_gov_contracts(self):
        return self._single("get_live_gov_contracts")

    def get_live_gov_contracts_all(self, **kwargs):
        return self._paged("get_live_gov_contracts_all", **kwargs)

    def get_live_lobbying(self, **kwargs):
        return self._paged("get_live_lobbying", **kwargs)

    def get_live_congress_holdings(self):
        return self._single("get_live_congress_holdings")

    def get_live_wall_street_bets(self, **kwargs):
        return self._single("get_live_wall_street_bets", **kwargs)

    def get_live_patents(self):
        return self._single("get_live_patents")

    def get_historical_congress_trading(self, *, ticker):
        return self._single("get_historical_congress_trading", ticker=ticker) or [{"Ticker": ticker}]

    def get_historical_senate_trading(self, *, ticker):
        return self._single("get_historical_senate_trading", ticker=ticker) or [{"Ticker": ticker}]

    def get_historical_house_trading(self, *, ticker):
        return self._single("get_historical_house_trading", ticker=ticker) or [{"Ticker": ticker}]

    def get_historical_gov_contracts(self, *, ticker):
        return self._single("get_historical_gov_contracts", ticker=ticker) or [{"Ticker": ticker}]

    def get_historical_gov_contracts_all(self, *, ticker):
        return self._single("get_historical_gov_contracts_all", ticker=ticker) or [{"Ticker": ticker}]

    def get_live_insiders(self, **kwargs):
        return self._paged("get_live_insiders", **kwargs)

    def get_live_sec13f(self, **kwargs):
        return self._paged("get_live_sec13f", **kwargs)

    def get_live_sec13f_changes(self, **kwargs):
        return self._paged("get_live_sec13f_changes", **kwargs)

    def get_historical_lobbying(self, **kwargs):
        return self._paged("get_historical_lobbying", **kwargs)

    def get_live_etf_holdings(self, **kwargs):
        return self._single("get_live_etf_holdings", **kwargs)

    def get_historical_wall_street_bets(self, *, ticker):
        return self._single("get_historical_wall_street_bets", ticker=ticker) or [{"Ticker": ticker}]

    def get_historical_patents(self, *, ticker):
        return self._single("get_historical_patents", ticker=ticker) or [{"Ticker": ticker}]


def _config(**overrides) -> QuiverDataConfig:
    base = {
        "bronze_container": "bronze",
        "silver_container": "silver",
        "gold_container": "gold",
        "enabled": True,
        "job_mode": "incremental",
        "ticker_batch_size": 2,
        "historical_batch_size": 1,
        "symbol_limit": 500,
        "page_size": 100,
        "max_pages_per_request": 0,
        "sec13f_today_only": True,
        "postgres_dsn": None,
    }
    base.update(overrides)
    return QuiverDataConfig(**base)


def _request_by_id(requests: list[QuiverSourceRequest], source_dataset: str, ticker: str | None) -> QuiverSourceRequest:
    for request in requests:
        if request.source_dataset == source_dataset and request.requested_symbol == ticker:
            return request
    raise AssertionError(f"missing request {source_dataset}:{ticker}")


def test_build_requests_incremental_includes_global_and_rotating_ticker_feeds() -> None:
    requests = _build_requests(_FakeClient(), _config(), selected_symbols=("AAPL", "MSFT"))

    ids = [(request.source_dataset, request.requested_symbol) for request in requests]
    assert ("congress_trading_live", None) in ids
    assert ("government_contracts_live", None) in ids
    assert ("congress_holdings_live", None) in ids
    assert ("insiders_live_all", None) in ids
    assert ("wall_street_bets_live", None) in ids
    assert ("patents_live", None) in ids
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

    ids = [(request.source_dataset, request.requested_symbol) for request in requests]
    assert ("wall_street_bets_historical_all", None) in ids
    assert ("congress_trading_historical", "AAPL") in ids
    assert ("government_contracts_all_historical", "MSFT") in ids
    assert ("lobbying_historical", "MSFT") in ids
    assert ("wall_street_bets_historical", "AAPL") in ids
    assert ("patents_historical", "MSFT") in ids
    assert ("congress_trading_live", None) not in ids
    assert ("insiders_live", "AAPL") not in ids


def test_paginated_request_fetches_until_short_page() -> None:
    client = _FakeClient()
    client.pages["get_live_insiders"] = {
        1: [{"Ticker": "AAPL", "id": 1}, {"Ticker": "AAPL", "id": 2}],
        2: [{"Ticker": "AAPL", "id": 3}],
    }
    request = _request_by_id(_build_requests(client, _config(page_size=2), selected_symbols=("AAPL",)), "insiders_live", "AAPL")

    result = request.fetch()

    assert [row["id"] for row in result.rows] == [1, 2, 3]
    assert [call[1]["page"] for call in client.calls if call[0] == "get_live_insiders" and call[1].get("ticker") == "AAPL"] == [1, 2]
    assert result.metadata["pagesFetched"] == 2
    assert result.metadata["rowsFetched"] == 3
    assert result.metadata["stopReason"] == "short_page"
    assert result.metadata["capHit"] is False


def test_paginated_request_stops_on_empty_first_page() -> None:
    client = _FakeClient()
    request = _request_by_id(_build_requests(client, _config(page_size=2), selected_symbols=()), "government_contracts_all_live", None)

    result = request.fetch()

    assert result.rows == []
    assert [call[1]["page"] for call in client.calls if call[0] == "get_live_gov_contracts_all"] == [1]
    assert result.metadata["pagesFetched"] == 1
    assert result.metadata["rowsFetched"] == 0
    assert result.metadata["stopReason"] == "empty_page"


def test_non_paginated_request_is_called_once() -> None:
    client = _FakeClient()
    client.single_payloads["get_live_congress_trading"] = [{"Ticker": "AAPL"}]
    request = _request_by_id(_build_requests(client, _config(), selected_symbols=()), "congress_trading_live", None)

    result = request.fetch()

    assert result.rows == [{"Ticker": "AAPL"}]
    assert [call[0] for call in client.calls].count("get_live_congress_trading") == 1
    assert result.metadata["paginated"] is False
    assert result.metadata["pagesFetched"] == 1
    assert result.metadata["stopReason"] == "single_request"


def test_paginated_request_fails_when_page_cap_is_full() -> None:
    client = _FakeClient()
    client.pages["get_live_sec13f"] = {
        1: [{"Ticker": "AAPL", "id": 1}],
        2: [{"Ticker": "AAPL", "id": 2}],
    }
    request = _request_by_id(
        _build_requests(client, _config(page_size=1, max_pages_per_request=2), selected_symbols=("AAPL",)),
        "sec13f_live",
        "AAPL",
    )

    with pytest.raises(PaginationLimitExceeded) as exc_info:
        request.fetch()

    assert [call[1]["page"] for call in client.calls if call[0] == "get_live_sec13f"] == [1, 2]
    assert exc_info.value.metadata["pagesFetched"] == 2
    assert exc_info.value.metadata["rowsFetched"] == 2
    assert exc_info.value.metadata["stopReason"] == "max_pages_reached"
    assert exc_info.value.metadata["capHit"] is True


def test_wall_street_bets_global_and_ticker_sources_use_distinct_raw_paths() -> None:
    global_path = constants.bronze_raw_path("run-1", "wall_street_bets_historical_all", "A")
    ticker_path = constants.bronze_raw_path("run-1", "wall_street_bets_historical", "A")

    assert global_path != ticker_path


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


def test_main_disabled_quiver_exits_before_client_or_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config(enabled=False)

    monkeypatch.setattr(bronze.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(bronze.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(
        bronze.mdc,
        "get_storage_client",
        lambda _container: (_ for _ in ()).throw(AssertionError("storage client should not be created")),
    )
    monkeypatch.setattr(
        bronze,
        "QuiverGatewayClient",
        type(
            "ForbiddenClient",
            (),
            {"from_env": staticmethod(lambda: (_ for _ in ()).throw(AssertionError("client should not be created")))},
        ),
    )
    monkeypatch.setattr(
        bronze,
        "write_domain_artifact",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("domain artifact should not be written")),
    )

    assert bronze.main(config) == 0
