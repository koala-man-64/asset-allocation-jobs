from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd


def _decimal_quantizer(scale: int) -> Decimal:
    if scale < 0:
        raise ValueError("scale must be >= 0")
    return Decimal("1").scaleb(-scale)


def _round_value_half_up(value: float, *, quantizer: Decimal) -> float:
    if pd.isna(value):
        return value
    try:
        rounded = Decimal(str(value)).quantize(quantizer, rounding=ROUND_HALF_UP)
        return float(rounded)
    except (InvalidOperation, ValueError, TypeError):
        return value


def round_series_half_up(series: pd.Series, scale: int) -> pd.Series:
    """
    Coerce a series to numeric and round with ROUND_HALF_UP at the requested scale.
    """
    quantizer = _decimal_quantizer(scale)
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.map(lambda value: _round_value_half_up(value, quantizer=quantizer))


def apply_precision_policy(
    df: pd.DataFrame,
    *,
    price_columns: set[str],
    calculated_columns: set[str],
    price_scale: int = 2,
    calculated_scale: int = 4,
) -> pd.DataFrame:
    """
    Apply silver-layer precision rules to selected columns.

    - Price-valued columns are rounded to `price_scale`.
    - Explicitly derived columns are rounded to `calculated_scale`.
    - Non-targeted columns are not modified.
    """
    out = df.copy()
    price_targets = set(price_columns)
    calculated_targets = set(calculated_columns).difference(price_targets)

    for col in sorted(price_targets):
        if col in out.columns:
            out[col] = round_series_half_up(out[col], price_scale)

    for col in sorted(calculated_targets):
        if col in out.columns:
            out[col] = round_series_half_up(out[col], calculated_scale)

    return out
