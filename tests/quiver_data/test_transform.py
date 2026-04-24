from __future__ import annotations

import pandas as pd

from tasks.quiver_data import constants
from tasks.quiver_data.transform import (
    build_government_contract_features,
    build_insider_trading_features,
    build_political_trading_features,
    bucket_rows,
    feature_safe_frame,
    normalize_bronze_batch,
    range_midpoint,
)


def test_bucket_rows_routes_records_by_symbol_bucket() -> None:
    batches = bucket_rows(
        "insiders_live",
        "insider_trading",
        [{"Ticker": "AAPL", "Date": "2026-03-01T00:00:00Z"}, {"Ticker": "MSFT", "Date": "2026-03-01T00:00:00Z"}],
    )

    assert sorted(batches.keys()) == ["A", "M"]
    assert batches["A"]["rows"][0]["Ticker"] == "AAPL"


def test_normalize_bronze_batch_uses_public_availability_rules_and_snake_case() -> None:
    frame = normalize_bronze_batch(
        {
            "source_dataset": "congress_trading_live",
            "dataset_family": "political_trading",
            "ingested_at": "2026-04-01T12:00:00Z",
            "rows": [
                {
                    "Ticker": "NVDA",
                    "ReportDate": "2026-03-31T00:00:00Z",
                    "Date": "2026-03-15T00:00:00Z",
                    "Transaction": "Purchase",
                    "Range": "$1,001 - $15,000",
                }
            ],
        }
    )

    row = frame.iloc[0]
    assert row["symbol"] == "NVDA"
    assert row["bucket"] == "N"
    assert row["public_availability_time"].startswith("2026-03-31")
    assert row["vendor_event_time"].startswith("2026-03-15")
    assert row["amount_mid_usd"] == 8000.5
    assert row["transaction"] == "Purchase"


def test_normalize_bronze_batch_parses_wall_street_bets_epoch_milliseconds() -> None:
    event_ms = int(pd.Timestamp("2026-04-01T12:00:00Z").timestamp() * 1000)
    frame = normalize_bronze_batch(
        {
            "source_dataset": "wall_street_bets_live",
            "dataset_family": "wall_street_bets",
            "ingested_at": "2026-04-01T12:05:00Z",
            "rows": [{"Ticker": "TSLA", "Time": event_ms, "Mentions": 42}],
        }
    )

    row = frame.iloc[0]
    assert row["symbol"] == "TSLA"
    assert row["vendor_event_time"].startswith("2026-04-01T12:00:00")
    assert row["public_availability_time"].startswith("2026-04-01T12:00:00")
    assert row["mentions"] == 42


def test_normalize_bronze_batch_uses_patent_date_and_symbol_rules() -> None:
    frame = normalize_bronze_batch(
        {
            "source_dataset": "patents_live",
            "dataset_family": "patents",
            "ingested_at": "2026-04-02T12:00:00Z",
            "rows": [{"Ticker": "IBM", "Date": "2026-03-29T00:00:00Z", "Patent": "Example"}],
        }
    )

    row = frame.iloc[0]
    assert row["symbol"] == "IBM"
    assert row["public_availability_time"].startswith("2026-03-29")
    assert row["vendor_event_time"].startswith("2026-03-29")
    assert row["patent"] == "Example"


def test_feature_safe_frame_drops_forward_looking_columns() -> None:
    safe = feature_safe_frame(pd.DataFrame([{"symbol": "AAPL", "price_change": 0.2, "net_signal": 1.0}]))
    assert list(safe.columns) == ["symbol", "net_signal"]
    assert "PriceChange" in constants.QUIVER_FORWARD_LOOKING_COLUMNS


def test_build_insider_and_political_features_generate_rolling_columns() -> None:
    insider_frame = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "public_availability_time": "2026-04-01T00:00:00Z",
                "acquireddisposedcode": "A",
                "transactioncode": "P",
                "shares": 10,
                "pricepershare": 100.0,
            },
            {
                "symbol": "AAPL",
                "public_availability_time": "2026-04-10T00:00:00Z",
                "acquireddisposedcode": "D",
                "transactioncode": "S",
                "shares": 5,
                "pricepershare": 110.0,
            },
        ]
    )
    political_frame = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "public_availability_time": "2026-04-05T00:00:00Z",
                "transaction": "Purchase",
                "amount_mid_usd": 10000.0,
                "chamber": "senate",
            }
        ]
    )

    insider_features = build_insider_trading_features(insider_frame)
    political_features = build_political_trading_features(political_frame)

    assert "buy_count_30d" in insider_features.columns
    assert "notional_proxy_30d" in insider_features.columns
    assert "net_amount_proxy_30d" in political_features.columns
    assert "senate_count_30d" in political_features.columns


def test_build_government_contract_features_flags_large_awards() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "PLTR",
                "public_availability_time": "2026-04-05T00:00:00Z",
                "amount_numeric": 75_000_000.0,
            }
        ]
    )
    features = build_government_contract_features(frame)

    assert features.iloc[0]["large_award_flag_30d"] == 1


def test_range_midpoint_handles_single_value() -> None:
    assert range_midpoint("$12,500") == 12500.0
