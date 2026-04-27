from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from tasks.technical_analysis.market_structure import add_market_structure_features


def _make_structure_df(close_values: list[float], *, atr_value: float = 2.0) -> pd.DataFrame:
    start = datetime(2024, 1, 1)
    return pd.DataFrame(
        {
            "date": [start + timedelta(days=index) for index in range(len(close_values))],
            "symbol": ["TEST"] * len(close_values),
            "high": [value + 0.5 for value in close_values],
            "low": [value - 0.5 for value in close_values],
            "close": close_values,
            "atr_14d": [atr_value] * len(close_values),
        }
    )


def test_donchian_channels_use_completed_history_only() -> None:
    out = add_market_structure_features(_make_structure_df([float(value) for value in range(100, 126)]))

    assert pd.isna(out.iloc[19]["donchian_high_20d"])
    assert out.iloc[20]["donchian_high_20d"] == pytest.approx(119.5)
    assert out.iloc[20]["crosses_above_donchian_high_20d"] == 1


def test_support_zone_waits_for_pivot_confirmation() -> None:
    closes = [
        30.0,
        29.0,
        28.0,
        27.0,
        26.0,
        25.0,
        24.0,
        23.0,
        22.0,
        21.0,
        20.0,
        19.0,
        18.0,
        17.0,
        16.0,
        10.0,
        17.0,
        18.0,
        19.0,
        20.0,
        21.0,
        22.0,
        23.0,
        24.0,
        25.0,
        26.0,
        27.0,
        28.0,
        29.0,
        30.0,
    ]
    out = add_market_structure_features(_make_structure_df(closes))

    assert pd.isna(out.iloc[17]["sr_support_1_mid"])
    assert out.iloc[17]["sr_support_1_touches"] == 0

    assert out.iloc[18]["sr_support_1_mid"] == pytest.approx(9.5)
    assert out.iloc[18]["sr_support_1_touches"] == 1


def test_fibonacci_levels_use_latest_confirmed_opposite_pivots() -> None:
    closes = [
        20.0,
        19.0,
        18.0,
        17.0,
        16.0,
        15.0,
        14.0,
        13.0,
        12.0,
        11.0,
        10.0,
        11.0,
        12.0,
        13.0,
        14.0,
        15.0,
        16.0,
        17.0,
        18.0,
        19.0,
        20.0,
        19.0,
        18.0,
        17.0,
        16.0,
        15.0,
        14.0,
        13.0,
    ]
    out = add_market_structure_features(_make_structure_df(closes))

    row = out.iloc[23]
    assert row["fib_swing_direction"] == 1
    assert row["fib_anchor_low"] == pytest.approx(9.5)
    assert row["fib_anchor_high"] == pytest.approx(20.5)
    assert row["fib_level_500"] == pytest.approx(15.0)


def test_liquidity_location_features_use_completed_week_and_month_history() -> None:
    dates = pd.bdate_range("2024-01-29", periods=10)
    close = [10.0, 12.0, 11.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
    df = pd.DataFrame(
        {
            "date": dates,
            "symbol": ["TEST"] * len(close),
            "high": [value + 0.5 for value in close],
            "low": [value - 0.5 for value in close],
            "close": close,
            "atr_14d": [2.0] * len(close),
        }
    )

    out = add_market_structure_features(df)
    row = out.iloc[5]

    assert row["dist_prev_week_high_atr"] == pytest.approx(-0.25)
    assert row["dist_prev_week_low_atr"] == pytest.approx(2.75)
    assert row["dist_prev_month_high_atr"] == pytest.approx(-1.25)
    assert row["dist_prev_month_low_atr"] == pytest.approx(2.75)


def test_position_in_range_uses_prior_completed_donchian_windows() -> None:
    out = add_market_structure_features(_make_structure_df([float(value) for value in range(100, 160)]))

    row = out.iloc[55]
    assert row["position_in_20d_range"] == pytest.approx(1.025)
    assert row["position_in_55d_range"] == pytest.approx((155.0 - 99.5) / (154.5 - 99.5))


def test_bearish_sweep_features_track_rejection_and_confirmation() -> None:
    dates = pd.bdate_range("2024-01-01", periods=16)
    df = pd.DataFrame(
        {
            "date": dates,
            "symbol": ["TEST"] * len(dates),
            "close": [10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 18.0, 17.0, 16.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.2, 19.5],
            "high": [10.5, 12.5, 14.5, 16.5, 18.5, 20.5, 18.5, 17.5, 16.5, 15.5, 16.5, 17.5, 18.5, 19.5, 21.2, 20.0],
            "low": [9.5, 11.5, 13.5, 15.5, 17.5, 19.5, 17.5, 16.5, 15.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.8, 19.0],
            "atr_14d": [1.0] * len(dates),
        }
    )

    out = add_market_structure_features(df)

    assert out.iloc[14]["swept_sr_resistance_1"] == 1
    assert out.iloc[14]["bearish_sweep_magnitude_atr"] > 0.0
    assert out.iloc[14]["bearish_sweep_reclaim_frac"] > 0.0
    assert out.iloc[15]["bars_since_bearish_sweep"] == pytest.approx(1.0)
    assert out.iloc[15]["bearish_confirm_after_sweep"] == 1


def test_bullish_sweep_features_track_reclaim_and_confirmation() -> None:
    dates = pd.bdate_range("2024-02-01", periods=16)
    df = pd.DataFrame(
        {
            "date": dates,
            "symbol": ["TEST"] * len(dates),
            "close": [20.0, 18.0, 16.0, 14.0, 12.0, 10.0, 12.0, 13.0, 14.0, 15.0, 14.0, 13.0, 12.0, 11.0, 10.2, 11.0],
            "high": [20.5, 18.5, 16.5, 14.5, 12.5, 10.5, 12.5, 13.5, 14.5, 15.5, 14.5, 13.5, 12.5, 11.5, 10.6, 11.3],
            "low": [19.5, 17.5, 15.5, 13.5, 11.5, 9.5, 11.5, 12.5, 13.5, 14.5, 13.5, 12.5, 11.5, 10.5, 8.8, 10.4],
            "atr_14d": [1.0] * len(dates),
        }
    )

    out = add_market_structure_features(df)

    assert out.iloc[14]["swept_sr_support_1"] == 1
    assert out.iloc[14]["bullish_sweep_magnitude_atr"] > 0.0
    assert out.iloc[14]["bullish_sweep_reclaim_frac"] > 0.0
    assert out.iloc[15]["bars_since_bullish_sweep"] == pytest.approx(1.0)
    assert out.iloc[15]["bullish_confirm_after_sweep"] == 1
