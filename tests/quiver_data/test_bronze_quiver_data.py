from __future__ import annotations

import pytest

from tasks.quiver_data import bronze_quiver_data as bronze
from tasks.quiver_data import constants
from tasks.quiver_data.bronze_quiver_data import (
    PaginationLimitExceeded,
    QuiverRequestFetchError,
    QuiverSourceRequest,
    RequestFetchResult,
    _build_requests,
    plan_symbol_batch,
)
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


def test_bronze_job_name_is_unified_for_all_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONTAINER_APP_JOB_NAME", raising=False)

    assert constants.BRONZE_JOB_NAME == "bronze-quiver-job"
    assert bronze._runtime_job_name(constants.BRONZE_JOB_NAME) == "bronze-quiver-job"


def test_bronze_watermark_keys_remain_mode_specific() -> None:
    assert bronze._last_success_key("incremental") == "bronze_quiver_data_incremental"
    assert bronze._last_success_key("historical_backfill") == "bronze_quiver_data_historical_backfill"
    assert bronze._cursor_watermark_key("incremental") == "quiver_bronze_cursor_incremental"
    assert bronze._cursor_watermark_key("historical_backfill") == "quiver_bronze_cursor_historical_backfill"


def test_main_fails_when_quiver_gateway_client_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bronze.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(bronze, "QuiverGatewayClient", None)

    with pytest.raises(RuntimeError, match="QuiverGatewayClient is unavailable"):
        bronze.main(_config())


class _FakeGateway:
    @classmethod
    def from_env(cls):
        return cls()

    def close(self) -> None:
        return None


def _request_metadata(dataset: str, symbol: str | None, *, failed: bool = False) -> dict[str, object]:
    return {
        "sourceDataset": dataset,
        "requestedSymbol": symbol,
        "pagesFetched": 1,
        "rowsFetched": 0 if failed else 1,
        "stopReason": "failed" if failed else "single_request",
    }


def _success_request(dataset: str, symbol: str | None = None) -> QuiverSourceRequest:
    return QuiverSourceRequest(
        source_dataset=dataset,
        dataset_family="test_family",
        requested_symbol=symbol,
        paginated=False,
        fetch=lambda: RequestFetchResult(
            rows=[{"Ticker": symbol or "AAPL", "value": 1}],
            metadata=_request_metadata(dataset, symbol),
        ),
    )


def _failure_request(dataset: str, symbol: str | None = None) -> QuiverSourceRequest:
    def fetch() -> RequestFetchResult:
        raise QuiverRequestFetchError("provider failed", metadata=_request_metadata(dataset, symbol, failed=True))

    return QuiverSourceRequest(
        source_dataset=dataset,
        dataset_family="test_family",
        requested_symbol=symbol,
        paginated=False,
        fetch=fetch,
    )


def _run_quiver_main_with_requests(
    monkeypatch: pytest.MonkeyPatch,
    requests: list[QuiverSourceRequest],
    *,
    domain_artifact_error: BaseException | None = None,
) -> dict[str, object]:
    saved_json: list[tuple[object, str]] = []
    saved_success: list[dict[str, object]] = []
    saved_watermarks: list[tuple[str, dict[str, object]]] = []
    warnings: list[str] = []

    plan = bronze.SymbolBatchPlan(
        universe_symbols=("AAPL", "MSFT"),
        selected_symbols=("AAPL",),
        batch_size=1,
        cursor_key=bronze._cursor_watermark_key("incremental"),
        cursor_start=0,
        cursor_end=0,
        cursor_next=1,
    )

    def save_json(payload: object, path: str, **_kwargs: object) -> None:
        saved_json.append((payload, path))

    def save_success(_key: str, *, metadata: dict[str, object]) -> None:
        saved_success.append(metadata)

    def save_cursor(key: str, payload: dict[str, object]) -> None:
        saved_watermarks.append((key, payload))

    def write_domain(**_kwargs: object) -> None:
        if domain_artifact_error is not None:
            raise domain_artifact_error

    monkeypatch.setattr(bronze.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(bronze.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(bronze, "QuiverGatewayClient", _FakeGateway)
    monkeypatch.setattr(bronze, "_load_symbol_batch_plan", lambda _config: plan)
    monkeypatch.setattr(bronze, "_build_requests", lambda *_args, **_kwargs: requests)
    monkeypatch.setattr(bronze, "_run_id", lambda: "run-1")
    monkeypatch.setattr(bronze, "bucket_rows", lambda *_args, **_kwargs: {"A": [{"value": 1}]})
    monkeypatch.setattr(bronze.mdc, "save_json_content", save_json)
    monkeypatch.setattr(bronze, "write_domain_artifact", write_domain)
    monkeypatch.setattr(bronze, "save_last_success", save_success)
    monkeypatch.setattr(bronze, "save_watermarks", save_cursor)
    monkeypatch.setattr(bronze.mdc, "write_warning", lambda message: warnings.append(str(message)))
    monkeypatch.setattr(bronze.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(bronze.mdc, "write_error", lambda _message: None)

    exit_code = bronze.main(_config())
    return {
        "exit_code": exit_code,
        "saved_json": saved_json,
        "saved_success": saved_success,
        "saved_watermarks": saved_watermarks,
        "warnings": warnings,
    }


def test_main_one_global_request_failure_one_success_exits_zero_and_saves_last_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run_quiver_main_with_requests(
        monkeypatch,
        [_failure_request("global_feed"), _success_request("ticker_feed", "AAPL")],
    )

    assert result["exit_code"] == 0
    assert len(result["saved_success"]) == 1
    assert len(result["saved_watermarks"]) == 1


def test_main_all_requests_fail_exits_one_and_does_not_save_last_success(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _run_quiver_main_with_requests(
        monkeypatch,
        [_failure_request("global_feed"), _failure_request("ticker_feed", "AAPL")],
    )

    assert result["exit_code"] == 1
    assert result["saved_success"] == []
    assert result["saved_watermarks"] == []


def test_main_ticker_scoped_request_failure_does_not_advance_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _run_quiver_main_with_requests(
        monkeypatch,
        [_success_request("global_feed"), _failure_request("ticker_feed", "AAPL")],
    )

    assert result["exit_code"] == 0
    assert len(result["saved_success"]) == 1
    assert result["saved_watermarks"] == []
    assert any("symbol cursor not advanced" in message for message in result["warnings"])


def test_main_domain_artifact_write_failure_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(RuntimeError, match="artifact failed"):
        _run_quiver_main_with_requests(
            monkeypatch,
            [_success_request("global_feed")],
            domain_artifact_error=RuntimeError("artifact failed"),
        )
