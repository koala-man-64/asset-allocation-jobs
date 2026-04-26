import numpy as np
import pandas as pd
import pytest

from tasks.market_data.gold_market_data import compute_features


def _make_market_df(rows: int = 300) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=rows, freq="D")
    close = pd.Series(np.linspace(100.0, 200.0, rows))
    high = close + 1.0
    low = close - 1.0
    open_ = close
    volume = pd.Series(np.arange(rows, dtype=float) + 1000.0)

    return pd.DataFrame(
        {
            "Date": dates.astype(str),
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
            "DividendAmount": np.zeros(rows, dtype=float),
            "SplitCoefficient": np.ones(rows, dtype=float),
            "Symbol": ["AAPL"] * rows,
        }
    )


def test_compute_features_adds_expected_columns():
    df = _make_market_df(300)
    out = compute_features(df)

    expected = {
        "return_1d",
        "return_5d",
        "return_20d",
        "return_60d",
        "rsi_14d",
        "dividend_amount",
        "split_coefficient",
        "is_dividend_day",
        "is_split_day",
        "vol_20d",
        "vol_60d",
        "rolling_max_252d",
        "drawdown_1y",
        "true_range",
        "atr_14d",
        "sma_20d",
        "sma_50d",
        "sma_200d",
        "sma_20_gt_sma_50",
        "sma_50_gt_sma_200",
        "sma_20_crosses_above_sma_50",
        "sma_20_crosses_below_sma_50",
        "sma_50_crosses_above_sma_200",
        "sma_50_crosses_below_sma_200",
        "bb_width_20d",
        "range_close",
        "volume_z_20d",
        "volume_pct_rank_252d",
        "donchian_high_20d",
        "donchian_low_55d",
        "dist_prev_week_high_atr",
        "dist_prev_week_low_atr",
        "dist_prev_month_high_atr",
        "dist_prev_month_low_atr",
        "position_in_20d_range",
        "position_in_55d_range",
        "sr_support_1_mid",
        "sr_resistance_1_mid",
        "fib_swing_direction",
        "fib_level_618",
        "swept_sr_resistance_1",
        "swept_sr_support_1",
        "bearish_sweep_magnitude_atr",
        "bullish_sweep_magnitude_atr",
        "bearish_sweep_reclaim_frac",
        "bullish_sweep_reclaim_frac",
        "bars_since_bearish_sweep",
        "bars_since_bullish_sweep",
        "bearish_confirm_after_sweep",
        "bullish_confirm_after_sweep",
        "amihud_20d",
        "amihud_z_252d",
        "dollar_volume_20d",
        "dollar_volume_z_252d",
        "liquidity_stress_score",
        "pat_doji",
        "atr_14d",
        "ha_open",
        "ha_high",
        "ha_low",
        "ha_close",
        "ichimoku_tenkan_sen_9",
        "ichimoku_kijun_sen_26",
        "ichimoku_senkou_span_a",
        "ichimoku_senkou_span_b",
        "ichimoku_senkou_span_a_26",
        "ichimoku_senkou_span_b_26",
        "ichimoku_chikou_span_26",
    }

    assert expected.issubset(set(out.columns))
    assert len(out) == 300


def test_compute_features_basic_sanity_on_monotonic_series():
    df = _make_market_df(300)
    out = compute_features(df)

    assert out["drawdown_1y"].fillna(0.0).abs().max() < 1e-9

    last = out.iloc[-1]
    assert last["sma_20_gt_sma_50"] == 1
    assert last["sma_50_gt_sma_200"] == 1
    assert 0.0 <= float(last["rsi_14d"]) <= 100.0

    assert 0.0 <= float(last["volume_pct_rank_252d"]) <= 1.0
    assert float(last["volume_pct_rank_252d"]) > 0.99


def test_compute_features_marks_dividend_and_split_event_days():
    df = _make_market_df(5)
    df.loc[2, "DividendAmount"] = 0.5
    df.loc[3, "SplitCoefficient"] = 2.0
    df.loc[4, ["DividendAmount", "SplitCoefficient"]] = [0.0, 1.0]

    out = compute_features(df)

    assert out.loc[2, "is_dividend_day"] == 1
    assert out.loc[2, "is_split_day"] == 0
    assert out.loc[3, "is_dividend_day"] == 0
    assert out.loc[3, "is_split_day"] == 1
    assert out.loc[4, "is_dividend_day"] == 0
    assert out.loc[4, "is_split_day"] == 0


def test_compute_features_does_not_emit_internal_helper_columns():
    df = _make_market_df(300)
    out = compute_features(df)

    assert not any(str(column).startswith("_") for column in out.columns)
    assert "_eq_tol" not in out.columns
    assert "_gap_tol" not in out.columns
    assert "eq_tol" not in out.columns
    assert "gap_tol" not in out.columns


def test_compute_features_requires_expected_columns():
    with pytest.raises(ValueError, match="Missing required columns"):
        compute_features(pd.DataFrame({"Date": ["2020-01-01"], "Close": [1.0]}))


def test_compute_features_liquidity_features_handle_zero_volume_without_inf() -> None:
    df = _make_market_df(300)
    df.loc[280:, "Volume"] = 0.0

    out = compute_features(df)

    assert out.iloc[-1]["dollar_volume_20d"] == pytest.approx(0.0)
    for column in (
        "amihud_20d",
        "amihud_z_252d",
        "dollar_volume_20d",
        "dollar_volume_z_252d",
        "liquidity_stress_score",
    ):
        finite = out[column].dropna()
        assert np.isfinite(finite).all(), f"{column} emitted non-finite values"
