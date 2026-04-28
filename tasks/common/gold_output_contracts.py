from __future__ import annotations

from dataclasses import dataclass
import re

import numpy as np
import pandas as pd

from tasks.common.silver_contracts import normalize_columns_to_snake_case


@dataclass(frozen=True)
class GoldOutputContract:
    columns: tuple[str, ...]
    datetime_columns: frozenset[str]
    string_columns: frozenset[str]
    integer_columns: frozenset[str]


GOLD_MARKET_OUTPUT_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dividend_amount",
    "split_coefficient",
    "is_dividend_day",
    "is_split_day",
    "return_1d",
    "return_5d",
    "return_20d",
    "return_60d",
    "rsi_14d",
    "vol_20d",
    "vol_60d",
    "rolling_max_252d",
    "drawdown_1y",
    "true_range",
    "atr_14d",
    "gap_atr",
    "sma_20d",
    "sma_50d",
    "sma_200d",
    "sma_20_gt_sma_50",
    "sma_50_gt_sma_200",
    "trend_50_200",
    "above_sma_50",
    "sma_20_crosses_above_sma_50",
    "sma_20_crosses_below_sma_50",
    "sma_50_crosses_above_sma_200",
    "sma_50_crosses_below_sma_200",
    "bb_width_20d",
    "range_close",
    "range_20",
    "compression_score",
    "volume_z_20d",
    "volume_pct_rank_252d",
    "range",
    "body",
    "is_bull",
    "is_bear",
    "upper_shadow",
    "lower_shadow",
    "body_to_range",
    "upper_to_range",
    "lower_to_range",
    "pat_doji",
    "pat_spinning_top",
    "pat_bullish_marubozu",
    "pat_bearish_marubozu",
    "pat_star_gap_up",
    "pat_star_gap_down",
    "pat_star",
    "pat_hammer",
    "pat_hanging_man",
    "pat_inverted_hammer",
    "pat_shooting_star",
    "pat_dragonfly_doji",
    "pat_gravestone_doji",
    "pat_bullish_spinning_top",
    "pat_bearish_spinning_top",
    "pat_bullish_engulfing",
    "pat_bearish_engulfing",
    "pat_bullish_harami",
    "pat_bearish_harami",
    "pat_piercing_line",
    "pat_dark_cloud_line",
    "pat_tweezer_bottom",
    "pat_tweezer_top",
    "pat_bullish_kicker",
    "pat_bearish_kicker",
    "pat_morning_star",
    "pat_morning_doji_star",
    "pat_evening_star",
    "pat_evening_doji_star",
    "pat_bullish_abandoned_baby",
    "pat_bearish_abandoned_baby",
    "pat_three_white_soldiers",
    "pat_three_black_crows",
    "pat_bullish_three_line_strike",
    "pat_bearish_three_line_strike",
    "pat_three_inside_up",
    "pat_three_outside_up",
    "pat_three_inside_down",
    "pat_three_outside_down",
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
    "donchian_high_20d",
    "donchian_low_20d",
    "dist_donchian_high_20d_atr",
    "dist_donchian_low_20d_atr",
    "above_donchian_high_20d",
    "below_donchian_low_20d",
    "crosses_above_donchian_high_20d",
    "crosses_below_donchian_low_20d",
    "donchian_high_55d",
    "donchian_low_55d",
    "dist_donchian_high_55d_atr",
    "dist_donchian_low_55d_atr",
    "above_donchian_high_55d",
    "below_donchian_low_55d",
    "crosses_above_donchian_high_55d",
    "crosses_below_donchian_low_55d",
    "dist_prev_week_high_atr",
    "dist_prev_week_low_atr",
    "dist_prev_month_high_atr",
    "dist_prev_month_low_atr",
    "position_in_20d_range",
    "position_in_55d_range",
    "sr_support_1_mid",
    "sr_support_1_low",
    "sr_support_1_high",
    "sr_support_1_touches",
    "sr_support_1_strength",
    "sr_support_1_dist_atr",
    "sr_resistance_1_mid",
    "sr_resistance_1_low",
    "sr_resistance_1_high",
    "sr_resistance_1_touches",
    "sr_resistance_1_strength",
    "sr_resistance_1_dist_atr",
    "sr_in_support_1_zone",
    "sr_in_resistance_1_zone",
    "sr_breaks_above_resistance_1",
    "sr_breaks_below_support_1",
    "sr_zone_position",
    "fib_swing_direction",
    "fib_anchor_low",
    "fib_anchor_high",
    "fib_level_236",
    "fib_level_382",
    "fib_level_500",
    "fib_level_618",
    "fib_level_786",
    "fib_nearest_level",
    "fib_nearest_dist_atr",
    "fib_in_value_zone",
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
)

GOLD_EARNINGS_OUTPUT_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "reported_eps",
    "eps_estimate",
    "surprise",
    "surprise_pct",
    "surprise_mean_4q",
    "surprise_std_8q",
    "beat_rate_8q",
    "is_earnings_day",
    "last_earnings_date",
    "days_since_earnings",
    "next_earnings_date",
    "days_until_next_earnings",
    "next_earnings_estimate",
    "next_earnings_time_of_day",
    "next_earnings_fiscal_date_ending",
    "has_upcoming_earnings",
    "is_scheduled_earnings_day",
)

GOLD_PRICE_TARGET_OUTPUT_COLUMNS: tuple[str, ...] = (
    "obs_date",
    "symbol",
    "tp_mean_est",
    "tp_std_dev_est",
    "tp_high_est",
    "tp_low_est",
    "tp_cnt_est",
    "tp_cnt_est_rev_up",
    "tp_cnt_est_rev_down",
    "disp_abs",
    "disp_norm",
    "disp_std_norm",
    "rev_net",
    "rev_ratio",
    "rev_intensity",
    "disp_norm_change_30d",
    "tp_mean_change_30d",
    "disp_z",
    "tp_mean_slope_90d",
)

_MARKET_INTEGER_BASE_COLUMNS: tuple[str, ...] = (
    "sma_20_gt_sma_50",
    "sma_50_gt_sma_200",
    "above_sma_50",
    "sma_20_crosses_above_sma_50",
    "sma_20_crosses_below_sma_50",
    "sma_50_crosses_above_sma_200",
    "sma_50_crosses_below_sma_200",
    "above_donchian_high_20d",
    "below_donchian_low_20d",
    "crosses_above_donchian_high_20d",
    "crosses_below_donchian_low_20d",
    "above_donchian_high_55d",
    "below_donchian_low_55d",
    "crosses_above_donchian_high_55d",
    "crosses_below_donchian_low_55d",
    "sr_support_1_touches",
    "sr_resistance_1_touches",
    "sr_in_support_1_zone",
    "sr_in_resistance_1_zone",
    "sr_breaks_above_resistance_1",
    "sr_breaks_below_support_1",
    "fib_swing_direction",
    "fib_in_value_zone",
    "swept_sr_resistance_1",
    "swept_sr_support_1",
    "bars_since_bearish_sweep",
    "bars_since_bullish_sweep",
    "bearish_confirm_after_sweep",
    "bullish_confirm_after_sweep",
    "is_bull",
    "is_bear",
    "is_dividend_day",
    "is_split_day",
)

GOLD_MARKET_INTEGER_COLUMNS: tuple[str, ...] = _MARKET_INTEGER_BASE_COLUMNS + tuple(
    column for column in GOLD_MARKET_OUTPUT_COLUMNS if column.startswith("pat_")
)

_CONTRACTS: dict[str, GoldOutputContract] = {
    "earnings": GoldOutputContract(
        columns=GOLD_EARNINGS_OUTPUT_COLUMNS,
        datetime_columns=frozenset(
            {
                "date",
                "last_earnings_date",
                "next_earnings_date",
                "next_earnings_fiscal_date_ending",
            }
        ),
        string_columns=frozenset({"symbol", "next_earnings_time_of_day"}),
        integer_columns=frozenset(
            {
                "is_earnings_day",
                "days_since_earnings",
                "days_until_next_earnings",
                "has_upcoming_earnings",
                "is_scheduled_earnings_day",
            }
        ),
    ),
    "market": GoldOutputContract(
        columns=GOLD_MARKET_OUTPUT_COLUMNS,
        datetime_columns=frozenset({"date"}),
        string_columns=frozenset({"symbol"}),
        integer_columns=frozenset(GOLD_MARKET_INTEGER_COLUMNS),
    ),
    "price-target": GoldOutputContract(
        columns=GOLD_PRICE_TARGET_OUTPUT_COLUMNS,
        datetime_columns=frozenset({"obs_date"}),
        string_columns=frozenset({"symbol"}),
        integer_columns=frozenset(),
    ),
}

_SUFFIX_ALIAS_PATTERN = re.compile(r"_(\d+)_([dmyqwh])(?=_|$)")


def _coerce_datetime(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    if hasattr(parsed.dtype, "tz") and parsed.dtype.tz is not None:
        parsed = parsed.dt.tz_convert(None)
    return parsed


def _coerce_nullable_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _coerce_nullable_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float64")


def _coerce_string(series: pd.Series, *, uppercase: bool = False) -> pd.Series:
    out = series.astype("string")
    if uppercase:
        out = out.str.strip().str.upper()
    else:
        out = out.str.strip()
    return out


def _normalized_gold_domain(domain: str) -> str:
    normalized = str(domain or "").strip().lower()
    if normalized not in _CONTRACTS:
        raise ValueError(f"Unsupported gold output contract domain: {domain!r}")
    return normalized


def _canonicalize_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Normalize inputs like "RSI 14D" to the contract column name "rsi_14d".
    out.columns = [_SUFFIX_ALIAS_PATTERN.sub(r"_\1\2", str(column)) for column in out.columns]
    return out


def empty_gold_output_frame(*, domain: str) -> pd.DataFrame:
    normalized_domain = _normalized_gold_domain(domain)
    contract = _CONTRACTS[normalized_domain]

    data: dict[str, pd.Series] = {}
    for column in contract.columns:
        if column in contract.datetime_columns:
            data[column] = pd.Series(dtype="datetime64[ns]")
        elif column in contract.string_columns:
            data[column] = pd.Series(dtype="string")
        elif column in contract.integer_columns:
            data[column] = pd.Series(dtype="Int64")
        else:
            data[column] = pd.Series(dtype="float64")
    return pd.DataFrame(data, columns=contract.columns)


def project_gold_output_frame(df: pd.DataFrame | None, *, domain: str) -> pd.DataFrame:
    normalized_domain = _normalized_gold_domain(domain)
    contract = _CONTRACTS[normalized_domain]

    if df is None or df.empty:
        return empty_gold_output_frame(domain=normalized_domain)

    out = _canonicalize_output_columns(normalize_columns_to_snake_case(df)).reset_index(drop=True)
    projected = pd.DataFrame(index=out.index)

    for column in contract.columns:
        if column in contract.datetime_columns:
            if column in out.columns:
                projected[column] = _coerce_datetime(out[column])
            else:
                projected[column] = pd.Series([pd.NaT] * len(out), dtype="datetime64[ns]")
        elif column in contract.string_columns:
            if column in out.columns:
                projected[column] = _coerce_string(out[column], uppercase=(column == "symbol"))
            else:
                projected[column] = pd.Series([pd.NA] * len(out), dtype="string")
        elif column in contract.integer_columns:
            if column in out.columns:
                projected[column] = _coerce_nullable_int(out[column])
            else:
                projected[column] = pd.Series([pd.NA] * len(out), dtype="Int64")
        else:
            if column in out.columns:
                projected[column] = _coerce_nullable_float(out[column])
            else:
                projected[column] = pd.Series([np.nan] * len(out), dtype="float64")

    return projected[list(contract.columns)].reset_index(drop=True)


def gold_output_columns(*, domain: str) -> tuple[str, ...]:
    normalized_domain = _normalized_gold_domain(domain)
    return _CONTRACTS[normalized_domain].columns
