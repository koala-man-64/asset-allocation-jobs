import asyncio
import json
import threading
import uuid
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.earnings_data import bronze_earnings_data as bronze

@pytest.fixture
def unique_ticker():
    return f"TEST_EARN_{uuid.uuid4().hex[:8].upper()}"


@pytest.fixture(autouse=True)
def _stub_symbol_policy_helpers(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(bronze, "clear_invalid_candidate_marker", lambda **kwargs: False)
    monkeypatch.setattr(bronze, "list_promoted_invalid_candidate_markers", lambda **kwargs: [])


def _sync_result() -> bronze.symbol_availability.SyncResult:
    return bronze.symbol_availability.SyncResult(
        provider="alpha_vantage",
        source_column="source_alpha_vantage",
        listed_count=1,
        inserted_count=0,
        disabled_count=0,
        duration_ms=1,
        lock_wait_ms=0,
    )

def _staged_frame(collected_symbol_frames: dict[str, pd.DataFrame], symbol: str) -> pd.DataFrame:
    assert symbol in collected_symbol_frames
    return collected_symbol_frames[symbol].sort_values(["date", "record_type"]).reset_index(drop=True)


async def _run_main_with_fetch_failure(
    symbols: list[str],
    fetch_error: BaseException,
    *,
    record_invalid_return: dict[str, object] | None = None,
):
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    with patch(
        "tasks.earnings_data.bronze_earnings_data._validate_environment"
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
        return_value=_sync_result(),
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
        return_value=pd.DataFrame({"Symbol": symbols}),
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
        return_value="alpha26",
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
        return_value=None,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
        return_value=mock_av,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
        side_effect=fetch_error,
    ) as mock_fetch, patch(
        "tasks.earnings_data.bronze_earnings_data.record_invalid_symbol_candidate",
        return_value=record_invalid_return or {"promoted": False, "observedRunCount": 1, "blacklistPath": None},
    ) as mock_record_invalid, patch(
        "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets"
    ) as mock_write, patch(
        "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
        return_value=0,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.list_manager"
    ) as mock_list_manager, patch(
        "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
    ) as mock_write_warning, patch(
        "tasks.earnings_data.bronze_earnings_data.mdc.write_error"
    ) as mock_write_error, patch(
        "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
    ) as mock_write_line:
        mock_list_manager.is_blacklisted.return_value = False

        exit_code = await bronze.main_async()

    return {
        "exit_code": exit_code,
        "fetch": mock_fetch,
        "record_invalid": mock_record_invalid,
        "write": mock_write,
        "warning": mock_write_warning,
        "error": mock_write_error,
        "line": mock_write_line,
    }


def test_fetch_and_save_raw(unique_ticker):
    """
    Verifies fetch_and_save_raw:
    1. Checks blacklist (mocked).
    2. Calls API gateway client (mocked).
    3. Stages canonical alpha26 rows for bucket publication.
    """
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}

    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-01-01",
                "reportedEPS": "1.6",
                "estimatedEPS": "1.5",
                "surprisePercentage": "10.0",
            }
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )
        assert wrote is True

        mock_list_manager.add_to_whitelist.assert_called_with(symbol)
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 1
        row = staged.iloc[0]
        assert row["record_type"] == "actual"
        assert pd.Timestamp(row["date"]).date().isoformat() == "2024-01-01"
        assert row["reported_eps"] == pytest.approx(1.6)
        assert row["eps_estimate"] == pytest.approx(1.5)
        assert row["surprise"] == pytest.approx(0.1)


def test_fetch_and_save_raw_applies_backfill_start_cutoff(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2023-12-31",
                "reportedEPS": "1.4",
                "estimatedEPS": "1.2",
                "surprisePercentage": "5.0",
            },
            {
                "fiscalDateEnding": "2024-03-31",
                "reportedEPS": "1.8",
                "estimatedEPS": "1.7",
                "surprisePercentage": "3.0",
            },
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=pd.Timestamp("2024-01-01"),
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )
        assert wrote is True

        staged = _staged_frame(collected_symbol_frames, symbol)
        parsed_dates = [pd.Timestamp(value).date().isoformat() for value in staged["date"].tolist()]
        assert parsed_dates == ["2024-03-31"]


def test_fetch_and_save_raw_returns_false_when_cutoff_removes_all_rows(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2023-12-31",
                "reportedEPS": "1.4",
                "estimatedEPS": "1.2",
                "surprisePercentage": "5.0",
            },
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=pd.Timestamp("2024-01-01"),
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )
        assert wrote is False

        mock_list_manager.add_to_whitelist.assert_called_with(symbol)
        assert symbol not in collected_symbol_frames


def test_fetch_and_save_raw_requires_collected_symbol_frames(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {"symbol": symbol, "quarterlyEarnings": []}

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(ValueError, match="collected_symbol_frames"):
            bronze.fetch_and_save_raw(symbol, mock_av)


def test_fetch_and_save_raw_marks_missing_earnings_history_as_coverage_unavailable(unique_ticker):
    symbol = unique_ticker
    payload = {
        "symbol": symbol,
        "quarterlyEarnings": [],
        "note": "missing earnings history",
    }
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.BronzeCoverageUnavailableError) as exc_info:
            bronze.fetch_and_save_raw(symbol, mock_av, collected_symbol_frames={})

    assert exc_info.value.reason_code == "no_earnings_records"
    assert exc_info.value.payload == payload


def test_validate_environment_requires_common_container(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "https://example.test")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://scope/.default")
    monkeypatch.setattr(bronze.cfg, "AZURE_CONTAINER_BRONZE", "bronze", raising=False)
    monkeypatch.setattr(bronze.cfg, "AZURE_CONTAINER_COMMON", "", raising=False)
    monkeypatch.setattr(bronze, "bronze_client", object())
    monkeypatch.setattr(bronze, "common_client", object())

    with pytest.raises(ValueError, match="AZURE_CONTAINER_COMMON"):
        bronze._validate_environment()


def test_validate_environment_requires_common_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "https://example.test")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://scope/.default")
    monkeypatch.setattr(bronze.cfg, "AZURE_CONTAINER_BRONZE", "bronze", raising=False)
    monkeypatch.setattr(bronze.cfg, "AZURE_CONTAINER_COMMON", "common", raising=False)
    monkeypatch.setattr(bronze, "bronze_client", object())
    monkeypatch.setattr(bronze, "common_client", None)

    with pytest.raises(RuntimeError, match="Common storage client is unavailable"):
        bronze._validate_environment()


def test_fetch_and_save_raw_merges_scheduled_calendar_rows(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-12-31",
                "reportedDate": "2025-02-10",
                "reportedEPS": "1.6",
                "estimatedEPS": "1.5",
                "surprisePercentage": "6.0",
            }
        ],
    }
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-07"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.7,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            calendar_rows=calendar_rows,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

        assert wrote is True
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 2
        scheduled = staged.loc[staged["record_type"] == "scheduled"].iloc[0]
        assert pd.Timestamp(scheduled["date"]).date().isoformat() == "2026-05-07"
        assert pd.Timestamp(scheduled["report_date"]).date().isoformat() == "2026-05-07"
        assert pd.isna(scheduled["reported_eps"])
        assert scheduled["calendar_time_of_day"] == "post-market"
        assert scheduled["calendar_currency"] == "USD"


def test_fetch_and_save_raw_replaces_stale_scheduled_row_when_calendar_date_moves(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {"symbol": symbol, "quarterlyEarnings": []}
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-08"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.8,
                "currency": "USD",
                "time_of_the_day": "post-market",
            },
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-01"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.7,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "tasks.earnings_data.bronze_earnings_data._utc_today",
        return_value=pd.Timestamp("2026-05-02"),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            calendar_rows=calendar_rows,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

        assert wrote is True
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 1
        row = staged.iloc[0]
        assert row["record_type"] == "scheduled"
        assert pd.Timestamp(row["report_date"]).date().isoformat() == "2026-05-08"
        assert pd.Timestamp(row["fiscal_date_ending"]).date().isoformat() == "2026-03-31"
        assert row["eps_estimate"] == pytest.approx(1.8)


def test_fetch_and_save_raw_actual_replaces_scheduled_row_for_same_fiscal_period(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2026-03-31",
                "reportedDate": "2026-05-09",
                "reportedEPS": "1.9",
                "estimatedEPS": "1.8",
                "surprisePercentage": "5.5",
            }
        ],
    }
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-08"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.8,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "tasks.earnings_data.bronze_earnings_data._utc_today",
        return_value=pd.Timestamp("2026-05-10"),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            calendar_rows=calendar_rows,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

        assert wrote is True
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 1
        row = staged.iloc[0]
        assert row["record_type"] == "actual"
        assert pd.Timestamp(row["fiscal_date_ending"]).date().isoformat() == "2026-03-31"
        assert pd.Timestamp(row["report_date"]).date().isoformat() == "2026-05-09"
        assert row["reported_eps"] == pytest.approx(1.9)


@pytest.mark.parametrize(
    ("payload", "expected_status", "expected_earliest"),
    [
        (
            {
                "quarterlyEarnings": [
                    {
                        "fiscalDateEnding": "2023-12-31",
                        "reportedEPS": "1.4",
                        "estimatedEPS": "1.2",
                        "surprisePercentage": "5.0",
                    },
                    {
                        "fiscalDateEnding": "2025-03-31",
                        "reportedEPS": "1.9",
                        "estimatedEPS": "1.8",
                        "surprisePercentage": "2.0",
                    },
                ]
            },
            "covered",
            date(2023, 12, 31),
        ),
        (
            {
                "quarterlyEarnings": [
                    {
                        "fiscalDateEnding": "2025-03-31",
                        "reportedEPS": "1.9",
                        "estimatedEPS": "1.8",
                        "surprisePercentage": "2.0",
                    },
                ]
            },
            "limited",
            date(2025, 3, 31),
        ),
    ],
)
def test_fetch_and_save_raw_marks_coverage_status_from_source_payload(
    unique_ticker,
    payload,
    expected_status,
    expected_earliest,
):
    symbol = unique_ticker
    payload = {"symbol": symbol, **payload}
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    coverage_summary = bronze._empty_coverage_summary()

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "tasks.earnings_data.bronze_earnings_data._mark_coverage"
    ) as mock_mark_coverage:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=date(2024, 1, 1),
            coverage_summary=coverage_summary,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

    assert wrote is True
    assert symbol in collected_symbol_frames
    _, kwargs = mock_mark_coverage.call_args
    assert kwargs["status"] == expected_status
    assert kwargs["earliest_available"] == expected_earliest


def test_main_async_logs_invalid_payload_preview(unique_ticker):
    symbol = unique_ticker
    payload = {"detail": "X" * 700}
    expected_preview = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)[:500] + "..."
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.AlphaVantageGatewayInvalidSymbolError("invalid", payload=payload),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_invalid, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ) as mock_write_warning, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 1
        assert mock_record_invalid.call_count == 1
        assert mock_record_invalid.call_args.kwargs["symbol"] == symbol
        mock_list_manager.add_to_blacklist.assert_not_called()
        warning_messages = [call.args[0] for call in mock_write_warning.call_args_list if call.args]
        assert any(
            message
            == (
                f"Bronze earnings invalid symbol candidate for {symbol}. payload_preview={expected_preview}"
            )
            for message in warning_messages
        )

    asyncio.run(run_test())


def test_main_async_logs_symbol_success(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )
    client_manager = MagicMock()
    client_manager.get_client.return_value = mock_av

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._ThreadLocalAlphaVantageClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            return_value=True,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(1, "earnings-data/buckets/index.parquet"),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ) as mock_write_line:
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        messages = [str(call.args[0]) for call in mock_write_line.call_args_list if call.args]
        assert any(
            f"Bronze earnings success: operation=symbol_processed symbol={symbol}" in message
            for message in messages
        )

    asyncio.run(run_test())


def test_main_async_reprobes_promoted_blacklisted_symbol_and_recovers(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )
    client_manager = MagicMock()
    client_manager.get_client.return_value = mock_av

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_promoted_invalid_candidate_markers",
            return_value=[{"symbol": symbol, "status": "promoted"}],
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._ThreadLocalAlphaVantageClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            return_value=True,
        ) as mock_fetch, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ) as mock_clear, patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(1, "earnings-data/buckets/index.parquet"),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.side_effect = lambda value: str(value).strip().upper() == symbol
            mock_list_manager.blacklist = MagicMock()

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_fetch.assert_called_once()
        assert mock_fetch.call_args.kwargs["skip_blacklist_check"] is True
        mock_clear.assert_called_once()
        assert mock_clear.call_args.kwargs["symbol"] == symbol

    asyncio.run(run_test())


def test_main_async_promoted_reprobe_still_invalid_updates_marker(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_promoted_invalid_candidate_markers",
            return_value=[{"symbol": symbol, "status": "promoted"}],
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.AlphaVantageGatewayInvalidSymbolError("invalid", status_code=404),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_promoted_symbol_reprobe_attempt"
        ) as mock_record, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ) as mock_clear, patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.side_effect = lambda value: str(value).strip().upper() == symbol
            mock_list_manager.blacklist = MagicMock()

            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_record.assert_called_once()
        assert mock_record.call_args.kwargs["outcome"] == "still_invalid_symbol"
        mock_clear.assert_not_called()

    asyncio.run(run_test())


def test_main_async_promoted_reprobe_transient_failure_counts_as_failed_symbol(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )
    transient_error = bronze.AlphaVantageGatewayError(
        "gateway unavailable",
        status_code=503,
        detail="service unavailable",
        payload={"path": "/api/providers/alpha-vantage/earnings"},
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_promoted_invalid_candidate_markers",
            return_value=[{"symbol": symbol, "status": "promoted"}],
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=transient_error,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_promoted_symbol_reprobe_attempt"
        ) as mock_record, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ) as mock_clear, patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_job_run_status",
            return_value=("failed", 1),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.side_effect = lambda value: str(value).strip().upper() == symbol
            mock_list_manager.blacklist = MagicMock()

            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_record.assert_called_once()
        assert mock_record.call_args.kwargs["outcome"] == "failed_alphavantagegatewayerror"
        mock_clear.assert_not_called()

    asyncio.run(run_test())


def test_main_async_writes_alpha26_buckets_and_cleans_flat_blobs(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )
    client_manager = MagicMock()
    client_manager.get_client.return_value = mock_av

    def fake_fetch_and_stage(symbol_arg, _av, **kwargs):
        kwargs["collected_symbol_frames"][symbol_arg] = pd.DataFrame({"symbol": [symbol_arg]})
        return True

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._ThreadLocalAlphaVantageClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=fake_fetch_and_stage,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(1, "earnings-data/buckets/index.parquet"),
        ) as mock_write_buckets, patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=3,
        ) as mock_delete_flat_blobs, patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_write_buckets.assert_called_once()
        collected_frames = mock_write_buckets.call_args.args[0]
        assert symbol in collected_frames
        assert collected_frames[symbol]["symbol"].tolist() == [symbol]
        assert mock_write_buckets.call_args.kwargs["run_id"]
        mock_delete_flat_blobs.assert_called_once_with()
        mock_list_manager.flush.assert_called_once_with()

    asyncio.run(run_test())


def test_main_async_logs_invalid_payload_detail_preview_when_payload_missing(unique_ticker):
    symbol = unique_ticker
    detail = "X" * 700
    expected_payload = {"status_code": 404, "detail": detail, "message": "invalid"}
    expected_preview = json.dumps(expected_payload, separators=(",", ":"), ensure_ascii=False)[:500] + "..."
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.AlphaVantageGatewayInvalidSymbolError(
                "invalid",
                status_code=404,
                detail=detail,
            ),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_invalid, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ) as mock_write_warning, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_record_invalid.assert_called_once()
        mock_list_manager.add_to_blacklist.assert_not_called()
        warning_messages = [call.args[0] for call in mock_write_warning.call_args_list if call.args]
        assert any(
            message
            == (
                f"Bronze earnings invalid symbol candidate for {symbol}. payload_preview={expected_preview}"
            )
            for message in warning_messages
        )

    asyncio.run(run_test())


def test_main_async_transient_gateway_errors_do_not_record_invalid_candidates(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    transient_error = bronze.AlphaVantageGatewayError(
        "gateway unavailable",
        status_code=502,
        detail="bad gateway",
        payload={
            "path": "/api/providers/alpha-vantage/earnings",
            "status_code": 502,
            "detail": "bad gateway",
        },
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=transient_error,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_invalid_symbol_candidate"
        ) as mock_record_invalid, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_record_invalid.assert_not_called()
        mock_list_manager.add_to_blacklist.assert_not_called()

    asyncio.run(run_test())


def test_main_async_empty_listing_status_fails_without_active_publish():
    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": []}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env"
        ) as mock_from_env, patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets"
        ) as mock_write, patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_error"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_from_env.assert_not_called()
        mock_write.assert_not_called()

    asyncio.run(run_test())


def test_sync_earnings_availability_falls_back_to_existing_symbols_on_transient_gateway_error():
    stale_symbols = pd.DataFrame({"Symbol": ["MSFT", "AAPL", None]})
    transient_error = bronze.AlphaVantageGatewayUnavailableError(
        "API gateway returned a non-CSV Alpha Vantage payload.",
        payload={"path": "/api/providers/alpha-vantage/listing-status"},
    )

    with patch(
        "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
        side_effect=transient_error,
    ) as mock_sync, patch(
        "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
        return_value=stale_symbols,
    ) as mock_get_symbols, patch(
        "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
    ) as mock_write_warning, patch(
        "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
    ) as mock_write_line:
        result = bronze._sync_earnings_availability_symbols()

    assert result is stale_symbols
    mock_sync.assert_called_once_with("earnings")
    mock_get_symbols.assert_called_once_with("earnings")
    warning_messages = [str(call.args[0]) for call in mock_write_warning.call_args_list if call.args]
    line_messages = [str(call.args[0]) for call in mock_write_line.call_args_list if call.args]
    assert any("Bronze earnings availability sync degraded:" in message for message in warning_messages)
    assert any("source=stale_postgres" in message for message in warning_messages)
    assert any("path=/api/providers/alpha-vantage/listing-status" in message for message in warning_messages)
    assert any("Bronze earnings availability sync:" in message and "degraded=true" in message for message in line_messages)


def test_sync_earnings_availability_reraises_transient_gateway_error_without_existing_symbols():
    transient_error = bronze.AlphaVantageGatewayThrottleError(
        "rate limited",
        status_code=429,
        payload={"path": "/api/providers/alpha-vantage/listing-status"},
    )

    with patch(
        "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
        side_effect=transient_error,
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
        return_value=pd.DataFrame({"Symbol": []}),
    ), patch(
        "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
    ) as mock_write_warning:
        with pytest.raises(bronze.AlphaVantageGatewayThrottleError):
            bronze._sync_earnings_availability_symbols()

    mock_write_warning.assert_not_called()


def test_main_async_calendar_failure_degrades_when_historical_output_is_complete(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.side_effect = bronze.AlphaVantageGatewayError(
        "calendar unavailable",
        status_code=503,
        payload={"path": "/api/providers/alpha-vantage/earnings-calendar"},
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            return_value=True,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(1, "earnings-data/buckets/index.parquet"),
        ) as mock_write, patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_write.assert_called_once()

    asyncio.run(run_test())


def test_main_async_calendar_failure_blocks_publish_when_history_is_incomplete(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.side_effect = bronze.AlphaVantageGatewayError(
        "calendar unavailable",
        status_code=503,
        payload={"path": "/api/providers/alpha-vantage/earnings-calendar"},
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.BronzeCoverageUnavailableError("no_earnings_records"),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets"
        ) as mock_write, patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_error"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_write.assert_not_called()

    asyncio.run(run_test())


def test_main_async_retry_exhausted_gateway_failure_records_blacklist_candidate(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    transient_error = bronze.AlphaVantageGatewayError(
        "gateway unavailable",
        status_code=503,
        detail="service unavailable",
        payload={
            "path": "/api/providers/alpha-vantage/earnings",
            "status_code": 503,
            "detail": "service unavailable",
        },
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=transient_error,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_invalid, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ) as mock_write_warning, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_record_invalid.assert_called_once()
        assert mock_record_invalid.call_args.kwargs["symbol"] == symbol
        assert (
            mock_record_invalid.call_args.kwargs["reason_code"]
            == bronze._GATEWAY_RETRY_EXHAUSTED_CANDIDATE_REASON
        )
        assert (
            mock_record_invalid.call_args.kwargs["promotion_threshold"]
            == bronze._GATEWAY_RETRY_EXHAUSTED_PROMOTION_THRESHOLD
        )
        warning_messages = [str(call.args[0]) for call in mock_write_warning.call_args_list if call.args]
        assert any("gateway retry-exhausted blacklist candidate" in message for message in warning_messages)
        assert any("Bronze AV earnings failure summary:" in message for message in warning_messages)
        assert any("status=503" in message for message in warning_messages)

    asyncio.run(run_test())


def test_main_async_gateway_retry_exhausted_candidate_can_promote_blacklist(monkeypatch):
    monkeypatch.setattr(bronze.cfg, "ALPHA_VANTAGE_MAX_WORKERS", 1, raising=False)
    symbol = "PROMOTE503"
    gateway_error = bronze.AlphaVantageGatewayError(
        "gateway unavailable",
        status_code=503,
        detail="service unavailable after provider retries",
        payload={
            "path": "/api/providers/alpha-vantage/earnings",
            "status_code": 503,
            "detail": "service unavailable after provider retries",
        },
    )

    result = asyncio.run(
        _run_main_with_fetch_failure(
            [symbol],
            gateway_error,
            record_invalid_return={
                "promoted": True,
                "observedRunCount": 3,
                "blacklistPath": "earnings-data/blacklist.csv",
            },
        )
    )

    assert result["exit_code"] == 1
    result["record_invalid"].assert_called_once()
    assert (
        result["record_invalid"].call_args.kwargs["promotion_threshold"]
        == bronze._GATEWAY_RETRY_EXHAUSTED_PROMOTION_THRESHOLD
    )

    warning_messages = [str(call.args[0]) for call in result["warning"].call_args_list if call.args]
    line_messages = [str(call.args[0]) for call in result["line"].call_args_list if call.args]

    assert any(
        "gateway retry-exhausted blacklist candidate" in message
        and "observed_run_count=3" in message
        and "promoted=true" in message
        for message in warning_messages
    )
    assert any(
        "gateway_blacklist_candidates=1" in message and "blacklist_promotions=1" in message
        for message in line_messages
    )


def test_main_async_aborts_early_after_repeated_gateway_timeouts(monkeypatch):
    monkeypatch.setattr(bronze.cfg, "ALPHA_VANTAGE_MAX_WORKERS", 1, raising=False)
    symbols = [f"TIMEOUT{i}" for i in range(6)]
    gateway_error = bronze.AlphaVantageGatewayUnavailableError(
        "gateway unavailable",
        status_code=504,
        detail="stream timeout",
        payload={
            "path": "/api/providers/alpha-vantage/earnings",
            "status_code": 504,
            "detail": "stream timeout",
        },
    )

    result = asyncio.run(_run_main_with_fetch_failure(symbols, gateway_error))

    assert result["exit_code"] == 1
    assert result["fetch"].call_count == bronze._PROVIDER_UNAVAILABLE_FAILURE_THRESHOLD
    result["write"].assert_not_called()
    result["record_invalid"].assert_not_called()

    warning_messages = [str(call.args[0]) for call in result["warning"].call_args_list if call.args]
    error_messages = [str(call.args[0]) for call in result["error"].call_args_list if call.args]
    line_messages = [str(call.args[0]) for call in result["line"].call_args_list if call.args]

    assert sum("provider unavailable; aborting remaining symbols" in message for message in warning_messages) == 1
    assert any("Bronze AV earnings failure summary:" in message for message in warning_messages)
    assert any("reason=provider_unavailable" in message for message in error_messages)
    assert any(
        "processed=3" in message and "provider_unavailable_abort=true" in message for message in line_messages
    )


def test_main_async_treats_daily_quota_throttle_as_provider_unavailable(monkeypatch):
    monkeypatch.setattr(bronze.cfg, "ALPHA_VANTAGE_MAX_WORKERS", 1, raising=False)
    symbols = [f"QUOTA{i}" for i in range(6)]
    daily_quota = bronze.AlphaVantageGatewayThrottleError(
        "Alpha Vantage daily quota exhausted.",
        status_code=429,
        detail="standard API rate limit is 25 requests per day",
        payload={
            "path": "/api/providers/alpha-vantage/earnings",
            "status_code": 429,
            "detail": "standard API rate limit is 25 requests per day",
        },
    )

    result = asyncio.run(_run_main_with_fetch_failure(symbols, daily_quota))

    assert result["exit_code"] == 1
    assert result["fetch"].call_count == bronze._PROVIDER_UNAVAILABLE_FAILURE_THRESHOLD
    result["write"].assert_not_called()
    result["record_invalid"].assert_not_called()

    warning_messages = [str(call.args[0]) for call in result["warning"].call_args_list if call.args]
    error_messages = [str(call.args[0]) for call in result["error"].call_args_list if call.args]

    assert any("provider unavailable; aborting remaining symbols" in message for message in warning_messages)
    assert any("type=AlphaVantageGatewayThrottleError status=429" in message for message in warning_messages)
    assert any("reason=provider_unavailable" in message for message in error_messages)


def test_failure_bucket_key_includes_status_and_path():
    exc = bronze.AlphaVantageGatewayError(
        "gateway unavailable",
        status_code=504,
        detail="timeout",
        payload={"path": "/api/providers/alpha-vantage/earnings"},
    )
    key = bronze._failure_bucket_key(exc)

    assert "type=AlphaVantageGatewayError" in key
    assert "status=504" in key
    assert "path=/api/providers/alpha-vantage/earnings" in key


def test_coerce_datetime_column_skips_numeric_parse_for_iso_dates(monkeypatch):
    original = bronze.pd.to_datetime

    def guarded_to_datetime(*args, **kwargs):
        if kwargs.get("unit") == "ms":
            raise AssertionError("unexpected numeric datetime parse for ISO date strings")
        return original(*args, **kwargs)

    monkeypatch.setattr(bronze.pd, "to_datetime", guarded_to_datetime)

    series = pd.Series(["2025-12-31", "2025-09-30", None], dtype="object")
    parsed = bronze._coerce_datetime_column(series)

    assert parsed.tolist()[:2] == [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")]
    assert pd.isna(parsed.iloc[2])


def test_coerce_datetime_column_parses_epoch_milliseconds_strings():
    first_ms = str(int(pd.Timestamp("2025-12-31", tz="UTC").timestamp() * 1000))
    second_ms = str(int(pd.Timestamp("2025-09-30", tz="UTC").timestamp() * 1000))

    series = pd.Series([first_ms, second_ms, None], dtype="object")
    parsed = bronze._coerce_datetime_column(series)

    assert parsed.tolist()[:2] == [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")]
    assert pd.isna(parsed.iloc[2])


def test_thread_local_alpha_vantage_client_manager_reuses_per_thread_and_closes_all():
    created: list[MagicMock] = []

    def make_client() -> MagicMock:
        client = MagicMock()
        created.append(client)
        return client

    manager = bronze._ThreadLocalAlphaVantageClientManager(factory=make_client)
    first = manager.get_client()
    second = manager.get_client()
    assert first is second

    observed_from_thread: list[MagicMock] = []

    def worker() -> None:
        observed_from_thread.append(manager.get_client())

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()

    assert len(created) == 2
    assert observed_from_thread[0] is not first

    manager.close_all()

    for client in created:
        client.close.assert_called_once_with()
