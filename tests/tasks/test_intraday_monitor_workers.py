from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from tasks.intraday_monitor import refresh_worker, worker
from tasks.market_data import gold_market_data as gold
from tasks.market_data import silver_market_data as silver


class _FakeTransport:
    def __init__(self, handler):
        self._handler = handler

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def close(self) -> None:
        return None

    def request_json(self, method: str, path: str, *, params=None, json_body=None):
        return self._handler(method, path, json_body)


def test_intraday_monitor_worker_completes_and_only_queues_stale_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(UTC)
    requests: list[tuple[str, str, dict | None]] = []

    claim_payload = {
        "run": {
            "runId": "run-1",
            "watchlistId": "watch-1",
            "watchlistName": "Core",
            "triggerKind": "scheduled",
            "status": "claimed",
            "forceRefresh": False,
            "symbolCount": 2,
        },
        "watchlist": {
            "watchlistId": "watch-1",
            "name": "Core",
            "description": "core list",
            "enabled": True,
            "symbolCount": 2,
            "pollIntervalMinutes": 5,
            "refreshCooldownMinutes": 15,
            "autoRefreshEnabled": True,
            "marketSession": "us_equities_regular",
            "symbols": ["AAPL", "MSFT"],
        },
        "currentSymbolStatuses": [
            {
                "watchlistId": "watch-1",
                "symbol": "AAPL",
                "monitorStatus": "observed",
                "lastSuccessfulMarketRefreshAt": (now - timedelta(hours=1)).isoformat(),
            },
            {
                "watchlistId": "watch-1",
                "symbol": "MSFT",
                "monitorStatus": "observed",
                "lastSuccessfulMarketRefreshAt": (now - timedelta(minutes=5)).isoformat(),
            },
        ],
        "claimToken": "claim-1",
    }

    def _handle(method: str, path: str, json_body: dict | None):
        requests.append((method, path, json_body))
        if path == "/api/internal/intraday-monitor/claim":
            return claim_payload
        if path == "/api/internal/intraday-monitor/runs/run-1/complete":
            return {"status": "ok"}
        raise AssertionError(f"Unexpected request: {method} {path}")

    class _FakeMassiveClient:
        def get_unified_snapshot(self, *, symbols: list[str], asset_type: str):
            assert symbols == ["AAPL", "MSFT"]
            assert asset_type == "stocks"
            return {
                "results": [
                    {"ticker": "AAPL", "last_trade": {"price": 213.42, "timestamp": now.isoformat()}},
                    {"ticker": "MSFT", "last_trade": {"price": 512.15, "timestamp": now.isoformat()}},
                ]
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(worker, "preflight_dependencies", lambda: None)
    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport(_handle))
    monkeypatch.setattr(worker.MassiveGatewayClient, "from_env", lambda: _FakeMassiveClient())

    assert worker.main() == 0

    complete_body = requests[-1][2]
    assert complete_body is not None
    assert complete_body["refreshSymbols"] == ["AAPL"]
    statuses = {item["symbol"]: item for item in complete_body["symbolStatuses"]}
    assert statuses["AAPL"]["monitorStatus"] == "refresh_queued"
    assert statuses["MSFT"]["monitorStatus"] == "observed"


def test_intraday_monitor_worker_reports_failure_when_snapshot_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[tuple[str, str, dict | None]] = []

    def _handle(method: str, path: str, json_body: dict | None):
        requests.append((method, path, json_body))
        if path == "/api/internal/intraday-monitor/claim":
            return {
                "run": {
                    "runId": "run-1",
                    "watchlistId": "watch-1",
                    "watchlistName": "Core",
                    "triggerKind": "manual",
                    "status": "claimed",
                    "forceRefresh": True,
                    "symbolCount": 1,
                },
                "watchlist": {
                    "watchlistId": "watch-1",
                    "name": "Core",
                    "description": "core list",
                    "enabled": True,
                    "symbolCount": 1,
                    "pollIntervalMinutes": 5,
                    "refreshCooldownMinutes": 15,
                    "autoRefreshEnabled": True,
                    "marketSession": "us_equities_regular",
                    "symbols": ["AAPL"],
                },
                "currentSymbolStatuses": [],
                "claimToken": "claim-1",
            }
        if path == "/api/internal/intraday-monitor/runs/run-1/fail":
            return {"status": "ok"}
        raise AssertionError(f"Unexpected request: {method} {path}")

    class _FakeMassiveClient:
        def get_unified_snapshot(self, *, symbols: list[str], asset_type: str):
            raise RuntimeError("snapshot unavailable")

        def close(self) -> None:
            return None

    monkeypatch.setattr(worker, "preflight_dependencies", lambda: None)
    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport(_handle))
    monkeypatch.setattr(worker.MassiveGatewayClient, "from_env", lambda: _FakeMassiveClient())

    assert worker.main() == 1
    assert requests[-1][1] == "/api/internal/intraday-monitor/runs/run-1/fail"
    assert "snapshot unavailable" in str(requests[-1][2]["error"])


def test_intraday_refresh_worker_scopes_symbols_and_restores_debug_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[tuple[str, str, dict | None]] = []
    debug_values: list[str | None] = []
    monkeypatch.setenv("DEBUG_SYMBOLS", "SPY")

    def _handle(method: str, path: str, json_body: dict | None):
        requests.append((method, path, json_body))
        if path == "/api/internal/intraday-refresh/claim":
            return {
                "batch": {
                    "batchId": "batch-1",
                    "runId": "run-1",
                    "watchlistId": "watch-1",
                    "watchlistName": "Core",
                    "domain": "market",
                    "bucketLetter": "A",
                    "status": "claimed",
                    "symbols": ["AAPL", "MSFT"],
                    "symbolCount": 2,
                },
                "claimToken": "claim-1",
            }
        if path == "/api/internal/intraday-refresh/batches/batch-1/complete":
            return {"status": "ok"}
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(refresh_worker, "preflight_dependencies", lambda: None)
    monkeypatch.setattr(refresh_worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport(_handle))
    monkeypatch.setattr(refresh_worker.bronze_market_data, "main", lambda: debug_values.append(os.environ.get("DEBUG_SYMBOLS")) or 0)
    monkeypatch.setattr(refresh_worker.silver_market_data, "main", lambda: debug_values.append(os.environ.get("DEBUG_SYMBOLS")) or 0)
    monkeypatch.setattr(refresh_worker.gold_market_data, "main", lambda: debug_values.append(os.environ.get("DEBUG_SYMBOLS")) or 0)

    assert refresh_worker.main() == 0
    assert debug_values == ["AAPL,MSFT", "AAPL,MSFT", "AAPL,MSFT"]
    assert os.environ.get("DEBUG_SYMBOLS") == "SPY"


def test_silver_merge_preserves_untouched_bucket_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02"), pd.Timestamp("2026-01-02")],
            "symbol": ["AAPL", "AMZN"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.5, 200.5],
            "volume": [1000.0, 2000.0],
            "short_interest": [pd.NA, pd.NA],
            "short_volume": [pd.NA, pd.NA],
        }
    )
    incoming = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-03")],
            "symbol": ["AAPL"],
            "open": [110.0],
            "high": [111.0],
            "low": [109.0],
            "close": [110.5],
            "volume": [1500.0],
            "short_interest": [pd.NA],
            "short_volume": [pd.NA],
        }
    )

    monkeypatch.setattr(silver, "_load_silver_market_bucket", lambda _path: existing.copy())

    merged = silver._merge_preserved_alpha26_market_bucket_symbols(
        bucket="A",
        df_bucket=incoming,
        scoped_symbols={"AAPL"},
    )

    assert set(merged["symbol"].astype(str)) == {"AAPL", "AMZN"}
    aapl_rows = merged.loc[merged["symbol"] == "AAPL"]
    assert float(aapl_rows.iloc[-1]["close"]) == pytest.approx(110.5)


def test_gold_merge_preserves_untouched_bucket_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    existing = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02"), pd.Timestamp("2026-01-02")],
            "symbol": ["AAPL", "AMZN"],
            "close": [100.5, 200.5],
        }
    )
    incoming = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-03")],
            "symbol": ["AAPL"],
            "close": [110.5],
        }
    )

    monkeypatch.setattr(gold, "_load_gold_market_bucket", lambda _path, *, gold_container: existing.copy())

    merged = gold._merge_preserved_gold_bucket_rows(
        bucket="A",
        gold_container="gold",
        scoped_symbols={"AAPL"},
        new_frame=incoming,
    )

    assert merged is not None
    assert set(merged["symbol"].astype(str)) == {"AAPL", "AMZN"}
    aapl_rows = merged.loc[merged["symbol"] == "AAPL"]
    assert float(aapl_rows.iloc[-1]["close"]) == pytest.approx(110.5)
