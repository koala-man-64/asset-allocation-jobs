"""Market-structure features built from daily OHLCV history.

This module adds:
- Donchian channel levels and breakout flags.
- Confirmed-pivot support/resistance zone scalars.
- Fibonacci retracement levels derived from the latest confirmed swing.

All features are aligned to the row's as-of date and avoid look-ahead leakage by
using only pivots confirmed after `_PIVOT_SPAN` future bars have elapsed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

_DONCHIAN_WINDOWS = (20, 55)
_PIVOT_SPAN = 3
_ZONE_RECENCY_BARS = 63.0
_ZONE_PRICE_PCT = 0.0075
_ZONE_ATR_MULT = 0.35


@dataclass
class _ZoneState:
    price_sum: float
    touch_count: int
    low: float
    high: float
    last_touch_index: int
    base_half_width: float

    @property
    def mid(self) -> float:
        if self.touch_count <= 0:
            return np.nan
        return self.price_sum / float(self.touch_count)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.where(denominator != 0)
    return numerator.where(denom.notna()).divide(denom)


def _zone_half_width(*, price: float, atr: float) -> float:
    atr_component = abs(float(atr)) * _ZONE_ATR_MULT if np.isfinite(atr) else 0.0
    return max(abs(float(price)) * _ZONE_PRICE_PCT, atr_component, 0.01)


def _zone_strength(zone: _ZoneState, *, current_index: int) -> float:
    age_bars = max(0, current_index - zone.last_touch_index)
    return float(zone.touch_count) * math.exp(-float(age_bars) / _ZONE_RECENCY_BARS)


def _register_zone(zones: list[_ZoneState], *, price: float, atr: float, current_index: int) -> None:
    half_width = _zone_half_width(price=price, atr=atr)
    matching_zone: _ZoneState | None = None
    matching_distance = float("inf")

    for zone in zones:
        if price < (zone.low - half_width) or price > (zone.high + half_width):
            continue
        distance = abs(zone.mid - price)
        if distance < matching_distance:
            matching_distance = distance
            matching_zone = zone

    if matching_zone is None:
        zones.append(
            _ZoneState(
                price_sum=float(price),
                touch_count=1,
                low=float(price) - half_width,
                high=float(price) + half_width,
                last_touch_index=current_index,
                base_half_width=half_width,
            )
        )
        return

    matching_zone.price_sum += float(price)
    matching_zone.touch_count += 1
    matching_zone.low = min(matching_zone.low, float(price) - half_width)
    matching_zone.high = max(matching_zone.high, float(price) + half_width)
    matching_zone.last_touch_index = current_index
    matching_zone.base_half_width = max(matching_zone.base_half_width, half_width)


def _select_support_zone(zones: list[_ZoneState], *, close: float, current_index: int) -> _ZoneState | None:
    if not np.isfinite(close):
        return None

    candidates: list[tuple[float, float, _ZoneState]] = []
    for zone in zones:
        in_zone = zone.low <= close <= zone.high
        if not in_zone and zone.mid > close:
            continue
        distance = abs(close - zone.mid) if in_zone else close - zone.mid
        candidates.append((float(distance), -_zone_strength(zone, current_index=current_index), zone))

    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def _select_resistance_zone(zones: list[_ZoneState], *, close: float, current_index: int) -> _ZoneState | None:
    if not np.isfinite(close):
        return None

    candidates: list[tuple[float, float, _ZoneState]] = []
    for zone in zones:
        in_zone = zone.low <= close <= zone.high
        if not in_zone and zone.mid < close:
            continue
        distance = abs(zone.mid - close) if in_zone else zone.mid - close
        candidates.append((float(distance), -_zone_strength(zone, current_index=current_index), zone))

    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def _confirmed_pivot_mask(series: pd.Series, *, mode: str) -> pd.Series:
    window = (2 * _PIVOT_SPAN) + 1
    if mode == "high":
        extrema = series.rolling(window=window, center=True, min_periods=window).max()
    elif mode == "low":
        extrema = series.rolling(window=window, center=True, min_periods=window).min()
    else:  # pragma: no cover - defensive only
        raise ValueError(f"Unsupported pivot mode={mode!r}")
    return series.eq(extrema) & series.notna()


def _build_structure_frame(
    *,
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    atr: pd.Series,
) -> pd.DataFrame:
    pivot_high_mask = _confirmed_pivot_mask(high, mode="high")
    pivot_low_mask = _confirmed_pivot_mask(low, mode="low")
    confirmed_pivot_high = high.where(pivot_high_mask).shift(_PIVOT_SPAN)
    confirmed_pivot_low = low.where(pivot_low_mask).shift(_PIVOT_SPAN)
    confirmed_pivot_high_atr = atr.where(pivot_high_mask).shift(_PIVOT_SPAN)
    confirmed_pivot_low_atr = atr.where(pivot_low_mask).shift(_PIVOT_SPAN)
    prev_close = close.shift(1)

    support_zones: list[_ZoneState] = []
    resistance_zones: list[_ZoneState] = []
    last_confirmed_low: tuple[int, float] | None = None
    last_confirmed_high: tuple[int, float] | None = None

    out: dict[str, list[float | int]] = {
        "sr_support_1_mid": [],
        "sr_support_1_low": [],
        "sr_support_1_high": [],
        "sr_support_1_touches": [],
        "sr_support_1_strength": [],
        "sr_support_1_dist_atr": [],
        "sr_resistance_1_mid": [],
        "sr_resistance_1_low": [],
        "sr_resistance_1_high": [],
        "sr_resistance_1_touches": [],
        "sr_resistance_1_strength": [],
        "sr_resistance_1_dist_atr": [],
        "sr_in_support_1_zone": [],
        "sr_in_resistance_1_zone": [],
        "sr_breaks_above_resistance_1": [],
        "sr_breaks_below_support_1": [],
        "sr_zone_position": [],
        "fib_swing_direction": [],
        "fib_anchor_low": [],
        "fib_anchor_high": [],
        "fib_level_236": [],
        "fib_level_382": [],
        "fib_level_500": [],
        "fib_level_618": [],
        "fib_level_786": [],
        "fib_nearest_level": [],
        "fib_nearest_dist_atr": [],
        "fib_in_value_zone": [],
    }

    for index in range(len(close)):
        confirmed_low = confirmed_pivot_low.iat[index]
        if pd.notna(confirmed_low):
            confirmed_low_atr = confirmed_pivot_low_atr.iat[index]
            _register_zone(
                support_zones,
                price=float(confirmed_low),
                atr=float(confirmed_low_atr) if pd.notna(confirmed_low_atr) else np.nan,
                current_index=index,
            )
            last_confirmed_low = (index - _PIVOT_SPAN, float(confirmed_low))

        confirmed_high = confirmed_pivot_high.iat[index]
        if pd.notna(confirmed_high):
            confirmed_high_atr = confirmed_pivot_high_atr.iat[index]
            _register_zone(
                resistance_zones,
                price=float(confirmed_high),
                atr=float(confirmed_high_atr) if pd.notna(confirmed_high_atr) else np.nan,
                current_index=index,
            )
            last_confirmed_high = (index - _PIVOT_SPAN, float(confirmed_high))

        close_value = float(close.iat[index]) if pd.notna(close.iat[index]) else np.nan
        prev_close_value = float(prev_close.iat[index]) if pd.notna(prev_close.iat[index]) else np.nan
        atr_value = float(atr.iat[index]) if pd.notna(atr.iat[index]) else np.nan

        support_zone = _select_support_zone(support_zones, close=close_value, current_index=index)
        resistance_zone = _select_resistance_zone(resistance_zones, close=close_value, current_index=index)

        if support_zone is None:
            out["sr_support_1_mid"].append(np.nan)
            out["sr_support_1_low"].append(np.nan)
            out["sr_support_1_high"].append(np.nan)
            out["sr_support_1_touches"].append(0)
            out["sr_support_1_strength"].append(0.0)
            out["sr_support_1_dist_atr"].append(np.nan)
            out["sr_in_support_1_zone"].append(0)
            out["sr_breaks_below_support_1"].append(0)
        else:
            support_strength = _zone_strength(support_zone, current_index=index)
            support_in_zone = int(support_zone.low <= close_value <= support_zone.high) if np.isfinite(close_value) else 0
            support_break = int(
                np.isfinite(close_value)
                and np.isfinite(prev_close_value)
                and close_value < support_zone.low
                and prev_close_value >= support_zone.low
            )
            out["sr_support_1_mid"].append(support_zone.mid)
            out["sr_support_1_low"].append(support_zone.low)
            out["sr_support_1_high"].append(support_zone.high)
            out["sr_support_1_touches"].append(int(support_zone.touch_count))
            out["sr_support_1_strength"].append(support_strength)
            out["sr_support_1_dist_atr"].append(
                ((close_value - support_zone.mid) / atr_value) if np.isfinite(close_value) and np.isfinite(atr_value) and atr_value != 0 else np.nan
            )
            out["sr_in_support_1_zone"].append(support_in_zone)
            out["sr_breaks_below_support_1"].append(support_break)

        if resistance_zone is None:
            out["sr_resistance_1_mid"].append(np.nan)
            out["sr_resistance_1_low"].append(np.nan)
            out["sr_resistance_1_high"].append(np.nan)
            out["sr_resistance_1_touches"].append(0)
            out["sr_resistance_1_strength"].append(0.0)
            out["sr_resistance_1_dist_atr"].append(np.nan)
            out["sr_in_resistance_1_zone"].append(0)
            out["sr_breaks_above_resistance_1"].append(0)
        else:
            resistance_strength = _zone_strength(resistance_zone, current_index=index)
            resistance_in_zone = int(resistance_zone.low <= close_value <= resistance_zone.high) if np.isfinite(close_value) else 0
            resistance_break = int(
                np.isfinite(close_value)
                and np.isfinite(prev_close_value)
                and close_value > resistance_zone.high
                and prev_close_value <= resistance_zone.high
            )
            out["sr_resistance_1_mid"].append(resistance_zone.mid)
            out["sr_resistance_1_low"].append(resistance_zone.low)
            out["sr_resistance_1_high"].append(resistance_zone.high)
            out["sr_resistance_1_touches"].append(int(resistance_zone.touch_count))
            out["sr_resistance_1_strength"].append(resistance_strength)
            out["sr_resistance_1_dist_atr"].append(
                ((resistance_zone.mid - close_value) / atr_value) if np.isfinite(close_value) and np.isfinite(atr_value) and atr_value != 0 else np.nan
            )
            out["sr_in_resistance_1_zone"].append(resistance_in_zone)
            out["sr_breaks_above_resistance_1"].append(resistance_break)

        if support_zone is not None and resistance_zone is not None and resistance_zone.mid > support_zone.mid:
            out["sr_zone_position"].append((close_value - support_zone.mid) / (resistance_zone.mid - support_zone.mid))
        else:
            out["sr_zone_position"].append(np.nan)

        fib_direction = 0
        fib_anchor_low = np.nan
        fib_anchor_high = np.nan
        fib_levels = [np.nan, np.nan, np.nan, np.nan, np.nan]

        if last_confirmed_low is not None and last_confirmed_high is not None:
            low_index, low_price = last_confirmed_low
            high_index, high_price = last_confirmed_high
            if low_index < high_index and high_price > low_price:
                fib_direction = 1
                fib_anchor_low = low_price
                fib_anchor_high = high_price
                swing_range = high_price - low_price
                fib_levels = [
                    high_price - (0.236 * swing_range),
                    high_price - (0.382 * swing_range),
                    high_price - (0.500 * swing_range),
                    high_price - (0.618 * swing_range),
                    high_price - (0.786 * swing_range),
                ]
            elif high_index < low_index and high_price > low_price:
                fib_direction = -1
                fib_anchor_low = low_price
                fib_anchor_high = high_price
                swing_range = high_price - low_price
                fib_levels = [
                    low_price + (0.236 * swing_range),
                    low_price + (0.382 * swing_range),
                    low_price + (0.500 * swing_range),
                    low_price + (0.618 * swing_range),
                    low_price + (0.786 * swing_range),
                ]

        fib_nearest_level = np.nan
        fib_nearest_dist_atr = np.nan
        fib_in_value_zone = 0
        if fib_direction != 0:
            finite_levels = [level for level in fib_levels if np.isfinite(level)]
            if finite_levels and np.isfinite(close_value):
                fib_nearest_level = min(finite_levels, key=lambda item: abs(item - close_value))
            if np.isfinite(fib_nearest_level) and np.isfinite(atr_value) and atr_value != 0:
                fib_nearest_dist_atr = (close_value - fib_nearest_level) / atr_value
            fib_value_low, fib_value_high = sorted((fib_levels[1], fib_levels[3]))
            if np.isfinite(close_value) and np.isfinite(fib_value_low) and np.isfinite(fib_value_high):
                fib_in_value_zone = int(fib_value_low <= close_value <= fib_value_high)

        out["fib_swing_direction"].append(int(fib_direction))
        out["fib_anchor_low"].append(fib_anchor_low)
        out["fib_anchor_high"].append(fib_anchor_high)
        out["fib_level_236"].append(fib_levels[0])
        out["fib_level_382"].append(fib_levels[1])
        out["fib_level_500"].append(fib_levels[2])
        out["fib_level_618"].append(fib_levels[3])
        out["fib_level_786"].append(fib_levels[4])
        out["fib_nearest_level"].append(fib_nearest_level)
        out["fib_nearest_dist_atr"].append(fib_nearest_dist_atr)
        out["fib_in_value_zone"].append(int(fib_in_value_zone))

    return pd.DataFrame(out)


def add_market_structure_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add Donchian, support/resistance, and Fibonacci features.

    The function expects single-symbol daily data with an existing `atr_14d`
    column so distance metrics remain normalized and comparable.
    """

    out = df.copy()
    required = {"date", "high", "low", "close", "atr_14d", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    for column in ["high", "low", "close", "atr_14d"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")

    symbols = out["symbol"].astype("string").str.strip().str.upper().replace("", pd.NA).dropna().unique().tolist()
    if len(symbols) > 1:
        raise ValueError(f"add_market_structure_features expects single-symbol input; received symbols={sorted(symbols)}")

    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    high = out["high"]
    low = out["low"]
    close = out["close"]
    atr = out["atr_14d"]
    prev_close = close.shift(1)

    for window in _DONCHIAN_WINDOWS:
        high_col = f"donchian_high_{window}d"
        low_col = f"donchian_low_{window}d"
        above_col = f"above_donchian_high_{window}d"
        below_col = f"below_donchian_low_{window}d"
        crosses_above_col = f"crosses_above_donchian_high_{window}d"
        crosses_below_col = f"crosses_below_donchian_low_{window}d"

        out[high_col] = high.rolling(window=window, min_periods=window).max().shift(1)
        out[low_col] = low.rolling(window=window, min_periods=window).min().shift(1)
        out[f"dist_donchian_high_{window}d_atr"] = _safe_div(out[high_col] - close, atr)
        out[f"dist_donchian_low_{window}d_atr"] = _safe_div(close - out[low_col], atr)

        above = close > out[high_col]
        below = close < out[low_col]
        out[above_col] = above.fillna(False).astype(int)
        out[below_col] = below.fillna(False).astype(int)
        out[crosses_above_col] = (above & (prev_close <= out[high_col])).fillna(False).astype(int)
        out[crosses_below_col] = (below & (prev_close >= out[low_col])).fillna(False).astype(int)

    structure = _build_structure_frame(high=high, low=low, close=close, atr=atr)
    for column in structure.columns:
        out[column] = structure[column]

    return out.replace([np.inf, -np.inf], np.nan)
