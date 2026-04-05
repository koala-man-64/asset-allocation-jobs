"""Technical Analysis Indicators (built from Silver market data).

This module contains logic to derive technical analysis indicators (including candlestick patterns)
from cleaned OHLCV data. It is designed to be known as a library function called by other jobs.

Design goals
------------
- Deterministic, vectorized OHLC-based pattern detection.
- Context-aware where patterns are otherwise ambiguous (e.g., Hammer vs Hanging Man).
- Minimal dependencies: pandas/numpy only.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _coerce_datetime(series: pd.Series) -> pd.Series:
    value = pd.to_datetime(series, errors="coerce")
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.where(denominator != 0)
    return numerator.where(denom.notna()).divide(denom)


_SNAKE_CASE_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_CASE_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake_case(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return "col"

    text = _SNAKE_CASE_CAMEL_1.sub(r"\1_\2", text)
    text = _SNAKE_CASE_CAMEL_2.sub(r"\1_\2", text)
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    return text or "col"


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    names = [_to_snake_case(col) for col in out.columns]

    seen: Dict[str, int] = {}
    unique: List[str] = []
    for name in names:
        count = seen.get(name, 0) + 1
        seen[name] = count
        unique.append(name if count == 1 else f"{name}_{count}")

    out.columns = unique
    return out


def _get_int_env(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _get_float_env(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value


def _approx_equal(a: pd.Series, b: pd.Series, tol: pd.Series) -> pd.Series:
    return (a - b).abs() <= tol


def add_heikin_ashi_and_ichimoku(df: pd.DataFrame) -> pd.DataFrame:
    """Add Heikin-Ashi and Ichimoku indicator component columns.

    Notes
    -----
    - Output is aligned to the row's as-of date.
    - Shifted Ichimoku variants use positive shifts only to avoid look-ahead leakage.
    """

    out = df.copy()

    required = {"date", "open", "high", "low", "close", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        out = _snake_case_columns(out)
        missing = required.difference(out.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

    if not pd.api.types.is_datetime64_any_dtype(out["date"]):
        out["date"] = _coerce_datetime(out["date"])

    out["symbol"] = out["symbol"].astype(str)
    for col in ["open", "high", "low", "close"]:
        if not pd.api.types.is_numeric_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)

    for _, group in out.groupby("symbol", sort=False):
        idx = group.index
        o = group["open"]
        h = group["high"]
        l = group["low"]
        c = group["close"]

        # Heikin-Ashi
        ha_close = (o + h + l + c) / 4.0
        ha_open_values = np.full(len(group), np.nan, dtype=float)
        if len(group) > 0:
            first_open = o.iloc[0]
            first_close = c.iloc[0]
            if pd.notna(first_open) and pd.notna(first_close):
                ha_open_values[0] = float(first_open + first_close) / 2.0

            for i in range(1, len(group)):
                prev_open = ha_open_values[i - 1]
                prev_close = ha_close.iloc[i - 1]
                if np.isnan(prev_open) or pd.isna(prev_close):
                    ha_open_values[i] = np.nan
                else:
                    ha_open_values[i] = float(prev_open + prev_close) / 2.0

        ha_open = pd.Series(ha_open_values, index=idx, dtype=float)
        ha_high = pd.concat([h, ha_open, ha_close], axis=1).max(axis=1)
        ha_low = pd.concat([l, ha_open, ha_close], axis=1).min(axis=1)

        out.loc[idx, "ha_open"] = ha_open
        out.loc[idx, "ha_high"] = ha_high
        out.loc[idx, "ha_low"] = ha_low
        out.loc[idx, "ha_close"] = ha_close

        # Ichimoku
        high_9 = h.rolling(window=9, min_periods=9).max()
        low_9 = l.rolling(window=9, min_periods=9).min()
        tenkan = (high_9 + low_9) / 2.0

        high_26 = h.rolling(window=26, min_periods=26).max()
        low_26 = l.rolling(window=26, min_periods=26).min()
        kijun = (high_26 + low_26) / 2.0

        high_52 = h.rolling(window=52, min_periods=52).max()
        low_52 = l.rolling(window=52, min_periods=52).min()
        senkou_b = (high_52 + low_52) / 2.0

        senkou_a = (tenkan + kijun) / 2.0
        senkou_a_26 = senkou_a.shift(26)
        senkou_b_26 = senkou_b.shift(26)
        chikou_26 = c.shift(26)

        out.loc[idx, "ichimoku_tenkan_sen_9"] = tenkan
        out.loc[idx, "ichimoku_kijun_sen_26"] = kijun
        out.loc[idx, "ichimoku_senkou_span_a"] = senkou_a
        out.loc[idx, "ichimoku_senkou_span_b"] = senkou_b
        out.loc[idx, "ichimoku_senkou_span_a_26"] = senkou_a_26
        out.loc[idx, "ichimoku_senkou_span_b_26"] = senkou_b_26
        out.loc[idx, "ichimoku_chikou_span_26"] = chikou_26

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def add_candlestick_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """Compute candlestick indicator features from OHLCV data."""

    # Assume upstream has already snake_cased or aligned columns,
    # but we enforce standard names for our logic.
    out = df.copy()

    required = {"date", "open", "high", "low", "close", "volume", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        # If upstream didn't normalize, try one pass of snake_casing just in case,
        # but typically we expect the caller to have done this.
        out = _snake_case_columns(out)
        missing = required.difference(out.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Ensure types
    if not pd.api.types.is_datetime64_any_dtype(out["date"]):
        out["date"] = _coerce_datetime(out["date"])

    out["symbol"] = out["symbol"].astype(str)
    for col in ["open", "high", "low", "close", "volume"]:
        if not pd.api.types.is_numeric_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # Only sort if not monotonic to preserve caller's intent if possible,
    # but indicators strictly require time-ordering.
    if not out["date"].is_monotonic_increasing:
        out = out.sort_values(["symbol", "date"]).reset_index(drop=True)

    # --- Candle geometry ---
    o = out["open"]
    h = out["high"]
    l = out["low"]
    c = out["close"]

    out["range"] = (h - l).clip(lower=0)
    out["body"] = (c - o).abs()
    out["is_bull"] = (c > o).astype(int)
    out["is_bear"] = (c < o).astype(int)
    out["upper_shadow"] = h - pd.concat([o, c], axis=1).max(axis=1)
    out["lower_shadow"] = pd.concat([o, c], axis=1).min(axis=1) - l

    out["body_to_range"] = _safe_div(out["body"], out["range"])
    out["upper_to_range"] = _safe_div(out["upper_shadow"], out["range"])
    out["lower_to_range"] = _safe_div(out["lower_shadow"], out["range"])

    # ATR(14) for scale-aware tolerances.
    prev_close = c.groupby(out["symbol"]).shift(1)
    tr_components = pd.concat(
        [
            (h - l),
            (h - prev_close).abs(),
            (l - prev_close).abs(),
        ],
        axis=1,
    )
    out["true_range"] = tr_components.max(axis=1)
    out["atr_14d"] = out.groupby("symbol")["true_range"].transform(lambda s: s.rolling(14, min_periods=14).mean())

    # Equality tolerance used for tweezer highs/lows.
    # - 5% of ATR (when available)
    # - else 0.1% of price
    # - floor at $0.01
    tol_atr = 0.05 * out["atr_14d"].fillna(0.0)
    tol_px = 0.001 * c.abs().fillna(0.0)
    out["_eq_tol"] = np.maximum(np.maximum(tol_atr, tol_px), 0.01)

    # --- Configurable thresholds ---
    trend_window = _get_int_env("CANDLE_CONTEXT_TREND_WINDOW", default=3)

    doji_max_body_to_range = _get_float_env("CANDLE_DOJI_MAX_BODY_TO_RANGE", default=0.05)
    spinning_max_body_to_range = _get_float_env("CANDLE_SPINNING_MAX_BODY_TO_RANGE", default=0.30)
    long_body_min_body_to_range = _get_float_env("CANDLE_LONG_BODY_MIN_BODY_TO_RANGE", default=0.60)
    marubozu_min_body_to_range = _get_float_env("CANDLE_MARUBOZU_MIN_BODY_TO_RANGE", default=0.90)
    marubozu_max_shadow_to_range = _get_float_env("CANDLE_MARUBOZU_MAX_SHADOW_TO_RANGE", default=0.05)

    hammer_min_shadow_to_body = _get_float_env("CANDLE_HAMMER_MIN_SHADOW_TO_BODY", default=2.0)
    small_shadow_max_to_body = _get_float_env("CANDLE_SMALL_SHADOW_MAX_TO_BODY", default=0.25)

    # Gap tolerance for "gap-based" patterns (stars, kickers, abandoned baby).
    #
    # Daily equities can have true session gaps; crypto/FX often won't. For intraday
    # (future), strict gaps are rare, so we make gaps "tolerant" by requiring a small
    # minimum separation between real bodies.
    gap_tol_atr_frac = _get_float_env("CANDLE_GAP_TOL_ATR_FRAC", default=0.01)
    gap_tol_px_frac = _get_float_env("CANDLE_GAP_TOL_PX_FRAC", default=0.0005)
    gap_tol_floor = _get_float_env("CANDLE_GAP_TOL_FLOOR", default=0.0)
    out["_gap_tol"] = np.maximum(
        np.maximum(gap_tol_atr_frac * out["atr_14d"].fillna(0.0), gap_tol_px_frac * c.abs().fillna(0.0)),
        gap_tol_floor,
    )

    # --- Trend context helpers ---
    # We use a simple trend heuristic based on *prior* closes to avoid look-ahead.
    def _downtrend_before(offset: int) -> pd.Series:
        # Compare close[t-offset] vs close[t-offset-trend_window]
        a = c.groupby(out["symbol"]).shift(offset)
        b = c.groupby(out["symbol"]).shift(offset + trend_window)
        return a < b

    def _uptrend_before(offset: int) -> pd.Series:
        a = c.groupby(out["symbol"]).shift(offset)
        b = c.groupby(out["symbol"]).shift(offset + trend_window)
        return a > b

    # --- Base candle-type flags ---
    out["pat_doji"] = (out["body_to_range"] <= doji_max_body_to_range).astype(int)

    # Spinning top: small body + meaningful shadows on both sides.
    # (Shadow comparisons are body-scaled to avoid range=0 edge cases.)
    body = out["body"].replace(0, np.nan)
    out["pat_spinning_top"] = (
        (
            (out["body_to_range"] > doji_max_body_to_range)
            & (out["body_to_range"] <= spinning_max_body_to_range)
            & (out["upper_shadow"] >= 0.5 * body)
            & (out["lower_shadow"] >= 0.5 * body)
        )
        .fillna(False)
        .astype(int)
    )

    # Marubozu (directional)
    out["pat_bullish_marubozu"] = (
        (
            (c > o)
            & (out["body_to_range"] >= marubozu_min_body_to_range)
            & (out["upper_to_range"] <= marubozu_max_shadow_to_range)
            & (out["lower_to_range"] <= marubozu_max_shadow_to_range)
        )
        .fillna(False)
        .astype(int)
    )
    out["pat_bearish_marubozu"] = (
        (
            (c < o)
            & (out["body_to_range"] >= marubozu_min_body_to_range)
            & (out["upper_to_range"] <= marubozu_max_shadow_to_range)
            & (out["lower_to_range"] <= marubozu_max_shadow_to_range)
        )
        .fillna(False)
        .astype(int)
    )

    # Star (gap) candle relative to previous real body.
    prev_o = o.groupby(out["symbol"]).shift(1)
    prev_c = c.groupby(out["symbol"]).shift(1)
    prev_body_hi = pd.concat([prev_o, prev_c], axis=1).max(axis=1)
    prev_body_lo = pd.concat([prev_o, prev_c], axis=1).min(axis=1)
    body_hi = pd.concat([o, c], axis=1).max(axis=1)
    body_lo = pd.concat([o, c], axis=1).min(axis=1)
    gap_tol_0 = out["_gap_tol"]
    gap_tol_1 = out.groupby("symbol")["_gap_tol"].shift(1)
    gap_tol_star = pd.concat([gap_tol_0, gap_tol_1], axis=1).max(axis=1)
    gap_up = body_lo > (prev_body_hi + gap_tol_star)
    gap_down = body_hi < (prev_body_lo - gap_tol_star)
    small_body = out["body_to_range"] <= spinning_max_body_to_range
    out["pat_star_gap_up"] = (small_body & gap_up).fillna(False).astype(int)
    out["pat_star_gap_down"] = (small_body & gap_down).fillna(False).astype(int)
    out["pat_star"] = ((out["pat_star_gap_up"] == 1) | (out["pat_star_gap_down"] == 1)).astype(int)

    # --- Single-candle patterns ---
    # Hammer / Hanging man share shape; trend context differentiates.
    hammer_shape = (
        (out["body_to_range"] <= spinning_max_body_to_range)
        & (out["lower_shadow"] >= hammer_min_shadow_to_body * out["body"].replace(0, np.nan))
        & (out["upper_shadow"] <= small_shadow_max_to_body * out["body"].replace(0, np.nan))
    ).fillna(False)
    out["pat_hammer"] = (hammer_shape & _downtrend_before(offset=1)).astype(int)
    out["pat_hanging_man"] = (hammer_shape & _uptrend_before(offset=1)).astype(int)

    inv_hammer_shape = (
        (out["body_to_range"] <= spinning_max_body_to_range)
        & (out["upper_shadow"] >= hammer_min_shadow_to_body * out["body"].replace(0, np.nan))
        & (out["lower_shadow"] <= small_shadow_max_to_body * out["body"].replace(0, np.nan))
    ).fillna(False)
    out["pat_inverted_hammer"] = (inv_hammer_shape & _downtrend_before(offset=1)).astype(int)
    out["pat_shooting_star"] = (inv_hammer_shape & _uptrend_before(offset=1)).astype(int)

    # Dragonfly / Gravestone doji (doji subtypes)
    out["pat_dragonfly_doji"] = (
        (
            (out["pat_doji"] == 1)
            & (out["upper_to_range"] <= 0.10)
            & (out["lower_to_range"] >= 0.60)
            & _downtrend_before(offset=1)
        )
        .fillna(False)
        .astype(int)
    )

    out["pat_gravestone_doji"] = (
        (
            (out["pat_doji"] == 1)
            & (out["lower_to_range"] <= 0.10)
            & (out["upper_to_range"] >= 0.60)
            & _uptrend_before(offset=1)
        )
        .fillna(False)
        .astype(int)
    )

    # Bullish/Bearish spinning tops (context-specific variants)
    out["pat_bullish_spinning_top"] = ((out["pat_spinning_top"] == 1) & _downtrend_before(offset=1)).astype(int)
    out["pat_bearish_spinning_top"] = ((out["pat_spinning_top"] == 1) & _uptrend_before(offset=1)).astype(int)

    # --- Double-candle patterns (flag on candle 2) ---
    o1 = o.groupby(out["symbol"]).shift(1)
    h1 = h.groupby(out["symbol"]).shift(1)
    l1 = l.groupby(out["symbol"]).shift(1)
    c1 = c.groupby(out["symbol"]).shift(1)
    body1 = (c1 - o1).abs()
    body2 = out["body"]

    candle1_bear = c1 < o1
    candle1_bull = c1 > o1
    candle2_bull = c > o
    candle2_bear = c < o

    # Trend before candle 1 for 2-candle patterns: use offset=2 (close before candle1).
    downtrend_before_2 = _downtrend_before(offset=2)
    uptrend_before_2 = _uptrend_before(offset=2)

    # Engulfing (real body engulf)
    out["pat_bullish_engulfing"] = (
        (downtrend_before_2 & candle1_bear & candle2_bull & (o <= c1) & (c >= o1)).fillna(False).astype(int)
    )

    out["pat_bearish_engulfing"] = (
        (uptrend_before_2 & candle1_bull & candle2_bear & (o >= c1) & (c <= o1)).fillna(False).astype(int)
    )

    # Harami (body inside prior body)
    body1_hi = pd.concat([o1, c1], axis=1).max(axis=1)
    body1_lo = pd.concat([o1, c1], axis=1).min(axis=1)
    body2_hi = pd.concat([o, c], axis=1).max(axis=1)
    body2_lo = pd.concat([o, c], axis=1).min(axis=1)

    out["pat_bullish_harami"] = (
        (downtrend_before_2 & candle1_bear & candle2_bull & (body2_lo > body1_lo) & (body2_hi < body1_hi))
        .fillna(False)
        .astype(int)
    )

    out["pat_bearish_harami"] = (
        (uptrend_before_2 & candle1_bull & candle2_bear & (body2_lo > body1_lo) & (body2_hi < body1_hi))
        .fillna(False)
        .astype(int)
    )

    # Piercing line
    midpoint_1 = (o1 + c1) / 2.0
    out["pat_piercing_line"] = (
        (
            downtrend_before_2
            & candle1_bear
            & (body1 / (h1 - l1).replace(0, np.nan) >= long_body_min_body_to_range)
            & candle2_bull
            & (o < c1)
            & (c > midpoint_1)
            & (c < o1)
        )
        .fillna(False)
        .astype(int)
    )

    # Dark cloud line
    out["pat_dark_cloud_line"] = (
        (
            uptrend_before_2
            & candle1_bull
            & (body1 / (h1 - l1).replace(0, np.nan) >= long_body_min_body_to_range)
            & candle2_bear
            & (o > c1)
            & (c < midpoint_1)
            & (c > o1)
        )
        .fillna(False)
        .astype(int)
    )

    # Tweezers
    tol = out["_eq_tol"]
    out["pat_tweezer_bottom"] = (
        (downtrend_before_2 & _approx_equal(l1, l, tol) & candle1_bear & candle2_bull).fillna(False).astype(int)
    )
    out["pat_tweezer_top"] = (
        (uptrend_before_2 & _approx_equal(h1, h, tol) & candle1_bull & candle2_bear).fillna(False).astype(int)
    )

    # Kickers (gap + strong reversal candle)
    # Bullish: bearish candle followed by bullish candle gapping above prior real body.
    prior_body_hi = body1_hi
    prior_body_lo = body1_lo
    gap_tol_kicker = gap_tol_star
    out["pat_bullish_kicker"] = (
        (
            downtrend_before_2
            & candle1_bear
            & candle2_bull
            & (body_lo > (prior_body_hi + gap_tol_kicker))
            & (body2 / out["range"].replace(0, np.nan) >= long_body_min_body_to_range)
        )
        .fillna(False)
        .astype(int)
    )
    out["pat_bearish_kicker"] = (
        (
            uptrend_before_2
            & candle1_bull
            & candle2_bear
            & (body_hi < (prior_body_lo - gap_tol_kicker))
            & (body2 / out["range"].replace(0, np.nan) >= long_body_min_body_to_range)
        )
        .fillna(False)
        .astype(int)
    )

    # --- Triple-candle patterns (flag on candle 3) ---
    # Shifted candles: t-2, t-1, t
    o2 = o.groupby(out["symbol"]).shift(2)
    h2 = h.groupby(out["symbol"]).shift(2)
    l2 = l.groupby(out["symbol"]).shift(2)
    c2 = c.groupby(out["symbol"]).shift(2)

    o3 = o.groupby(out["symbol"]).shift(0)
    c3 = c

    body_c1 = (c2 - o2).abs()
    rng_c1 = (h2 - l2).replace(0, np.nan)

    # Trend before candle 1 for 3-candle patterns: offset=3 (close before candle1).
    downtrend_before_3 = _downtrend_before(offset=3)
    uptrend_before_3 = _uptrend_before(offset=3)

    # Candle 2 (t-1)
    o_mid = o1
    c_mid = c1
    rng_mid = (h1 - l1).replace(0, np.nan)
    body_mid = body1
    small_mid = body_mid / rng_mid <= spinning_max_body_to_range
    mid_is_doji = body_mid / rng_mid <= doji_max_body_to_range

    # Candle 1 (t-2)
    c1_bear = c2 < o2
    c1_bull = c2 > o2
    c1_long = body_c1 / rng_c1 >= long_body_min_body_to_range
    midpoint_c1 = (o2 + c2) / 2.0

    # Candle 3 (t)
    c3_bull = c3 > o3
    c3_bear = c3 < o3
    body_c3 = (c3 - o3).abs()
    rng_c3 = (h - l).replace(0, np.nan)
    c3_long = body_c3 / rng_c3 >= long_body_min_body_to_range

    out["pat_morning_star"] = (
        (downtrend_before_3 & c1_bear & c1_long & small_mid & c3_bull & c3_long & (c3 > midpoint_c1))
        .fillna(False)
        .astype(int)
    )

    out["pat_morning_doji_star"] = (
        (downtrend_before_3 & c1_bear & c1_long & mid_is_doji & c3_bull & c3_long & (c3 > midpoint_c1))
        .fillna(False)
        .astype(int)
    )

    out["pat_evening_star"] = (
        (uptrend_before_3 & c1_bull & c1_long & small_mid & c3_bear & c3_long & (c3 < midpoint_c1))
        .fillna(False)
        .astype(int)
    )

    out["pat_evening_doji_star"] = (
        (uptrend_before_3 & c1_bull & c1_long & mid_is_doji & c3_bear & c3_long & (c3 < midpoint_c1))
        .fillna(False)
        .astype(int)
    )

    # Abandoned baby requires gaps around the doji.
    # Use real-body gaps (more robust than high/low gaps across assets).
    c1_body_hi = pd.concat([o2, c2], axis=1).max(axis=1)
    c1_body_lo = pd.concat([o2, c2], axis=1).min(axis=1)
    mid_body_hi = pd.concat([o_mid, c_mid], axis=1).max(axis=1)
    mid_body_lo = pd.concat([o_mid, c_mid], axis=1).min(axis=1)
    c3_body_hi = pd.concat([o3, c3], axis=1).max(axis=1)
    c3_body_lo = pd.concat([o3, c3], axis=1).min(axis=1)

    gap_tol_2 = out.groupby("symbol")["_gap_tol"].shift(2)
    gap_tol_12 = pd.concat([gap_tol_1, gap_tol_2], axis=1).max(axis=1)
    gap_tol_01 = pd.concat([gap_tol_0, gap_tol_1], axis=1).max(axis=1)

    gap1_down = mid_body_hi < (c1_body_lo - gap_tol_12)
    gap2_up = c3_body_lo > (mid_body_hi + gap_tol_01)
    gap1_up = mid_body_lo > (c1_body_hi + gap_tol_12)
    gap2_down = c3_body_hi < (mid_body_lo - gap_tol_01)

    out["pat_bullish_abandoned_baby"] = (
        (downtrend_before_3 & c1_bear & mid_is_doji & gap1_down & gap2_up & c3_bull).fillna(False).astype(int)
    )
    out["pat_bearish_abandoned_baby"] = (
        (uptrend_before_3 & c1_bull & mid_is_doji & gap1_up & gap2_down & c3_bear).fillna(False).astype(int)
    )

    # Three white soldiers / three black crows
    # Shifted for three candles ending at t: t-2, t-1, t
    close_t2 = c2
    close_t1 = c1
    close_t0 = c
    open_t2 = o2
    open_t1 = o1
    open_t0 = o

    bull_t2 = close_t2 > open_t2
    bull_t1 = close_t1 > open_t1
    bull_t0 = close_t0 > open_t0
    bear_t2 = close_t2 < open_t2
    bear_t1 = close_t1 < open_t1
    bear_t0 = close_t0 < open_t0

    rising_closes = (close_t0 > close_t1) & (close_t1 > close_t2)
    falling_closes = (close_t0 < close_t1) & (close_t1 < close_t2)

    # Opens within prior real body
    body2_hi = pd.concat([open_t2, close_t2], axis=1).max(axis=1)
    body2_lo = pd.concat([open_t2, close_t2], axis=1).min(axis=1)
    body1_hi = pd.concat([open_t1, close_t1], axis=1).max(axis=1)
    body1_lo = pd.concat([open_t1, close_t1], axis=1).min(axis=1)
    open1_in_body2 = (open_t1 >= body2_lo) & (open_t1 <= body2_hi)
    open0_in_body1 = (open_t0 >= body1_lo) & (open_t0 <= body1_hi)

    out["pat_three_white_soldiers"] = (
        (bull_t2 & bull_t1 & bull_t0 & rising_closes & open1_in_body2 & open0_in_body1).fillna(False).astype(int)
    )

    out["pat_three_black_crows"] = (
        (bear_t2 & bear_t1 & bear_t0 & falling_closes & open1_in_body2 & open0_in_body1).fillna(False).astype(int)
    )

    # --- 4-candle patterns: Three Line Strike (flag on candle 4) ---
    o3p = o.groupby(out["symbol"]).shift(3)
    c3p = c.groupby(out["symbol"]).shift(3)
    o2p = o2
    c2p = c2
    o1p = o1
    c1p = c1
    o0p = o
    c0p = c

    bull_3 = c3p > o3p
    bull_2 = c2p > o2p
    bull_1 = c1p > o1p
    bear_3 = c3p < o3p
    bear_2 = c2p < o2p
    bear_1 = c1p < o1p

    rising_3 = (c1p > c2p) & (c2p > c3p)
    falling_3 = (c1p < c2p) & (c2p < c3p)

    # Continuation context:
    # - Bullish TLS: prior 3 bullish, then big bearish engulfing below first open.
    # - Bearish TLS: prior 3 bearish, then big bullish engulfing above first open.
    uptrend_before_4 = _uptrend_before(offset=4)
    downtrend_before_4 = _downtrend_before(offset=4)

    out["pat_bullish_three_line_strike"] = (
        (
            uptrend_before_4
            & bull_3
            & bull_2
            & bull_1
            & rising_3
            & (c0p < o0p)  # 4th candle bearish
            & (o0p >= c1p)
            & (c0p < o3p)
        )
        .fillna(False)
        .astype(int)
    )

    out["pat_bearish_three_line_strike"] = (
        (
            downtrend_before_4
            & bear_3
            & bear_2
            & bear_1
            & falling_3
            & (c0p > o0p)  # 4th candle bullish
            & (o0p <= c1p)
            & (c0p > o3p)
        )
        .fillna(False)
        .astype(int)
    )

    # --- Confirmations (3 candles, flag on candle 3) ---
    # Three Inside/Outside Up/Down
    # Reuse candle1 (t-2), candle2 (t-1), candle3 (t)
    # Note: our Harami/Engulfing flags are emitted on the *second* candle of the 2-candle pattern,
    # so for 3-candle confirmations we reference the prior row (shift(1)).
    bullish_harami_prev = out.groupby("symbol")["pat_bullish_harami"].shift(1).fillna(0).astype(int)
    bearish_harami_prev = out.groupby("symbol")["pat_bearish_harami"].shift(1).fillna(0).astype(int)
    bullish_engulfing_prev = out.groupby("symbol")["pat_bullish_engulfing"].shift(1).fillna(0).astype(int)
    bearish_engulfing_prev = out.groupby("symbol")["pat_bearish_engulfing"].shift(1).fillna(0).astype(int)
    out["pat_three_inside_up"] = (
        (
            downtrend_before_3
            & c1_bear
            & c1_long
            & (bullish_harami_prev == 1)  # harami on candle2
            & c3_bull
            & (c3 > o2)
        )
        .fillna(False)
        .astype(int)
    )

    out["pat_three_outside_up"] = (
        (
            downtrend_before_3
            & c1_bear
            & (bullish_engulfing_prev == 1)  # engulfing on candle2
            & c3_bull
            & (c3 > c1)
        )
        .fillna(False)
        .astype(int)
    )

    out["pat_three_inside_down"] = (
        (uptrend_before_3 & c1_bull & c1_long & (bearish_harami_prev == 1) & c3_bear & (c3 < o2))
        .fillna(False)
        .astype(int)
    )

    out["pat_three_outside_down"] = (
        (
            uptrend_before_3
            & c1_bull
            & (bearish_engulfing_prev == 1)  # engulfing on candle2
            & c3_bear
            & (c3 < c1)
        )
        .fillna(False)
        .astype(int)
    )

    out = out.replace([np.inf, -np.inf], np.nan)
    return out
