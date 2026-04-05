from __future__ import annotations

from typing import Optional, Tuple

import os
import pandas as pd

from core import core as mdc

_DEFAULT_BACKFILL_START = pd.Timestamp("2016-01-01")
def get_latest_only_flag(domain: str, *, default: bool = True) -> bool:
    _ = (domain, default)
    return False


def _parse_env_timestamp(name: str) -> Optional[pd.Timestamp]:
    raw = os.environ.get(name)
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        ts = pd.to_datetime(text, errors="raise")
    except Exception:
        mdc.write_warning(f"Ignoring invalid {name} value: {raw!r}")
        return None
    normalized = pd.Timestamp(ts)
    if getattr(normalized, "tzinfo", None) is not None:
        normalized = normalized.tz_localize(None)
    return normalized.normalize()


def get_backfill_range() -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    start = _parse_env_timestamp("BACKFILL_START_DATE")
    if start is None:
        start = _DEFAULT_BACKFILL_START
    if start < _DEFAULT_BACKFILL_START:
        mdc.write_warning(
            "BACKFILL_START_DATE cannot be earlier than 2016-01-01; clamping to baseline cutoff."
        )
        start = _DEFAULT_BACKFILL_START

    # End-date cutoffs were removed from runtime and deployment configuration.
    return start, None


def filter_by_date(df: pd.DataFrame, date_col: str, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]) -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns:
        return df
    series = pd.to_datetime(df[date_col], errors="coerce")
    mask = series.notna()
    if start is not None:
        mask &= series >= start
    if end is not None:
        mask &= series <= end
    return df.loc[mask].copy().reset_index(drop=True)


def apply_backfill_start_cutoff(
    df: pd.DataFrame,
    *,
    date_col: str,
    backfill_start: Optional[pd.Timestamp],
    context: str,
) -> tuple[pd.DataFrame, int]:
    """
    Drop rows older than the provided start cutoff and return (filtered_df, dropped_count).
    """
    if backfill_start is None or df is None or df.empty or date_col not in df.columns:
        return df, 0

    before_count = int(len(df))
    filtered = filter_by_date(df, date_col, backfill_start, None)
    after_count = int(len(filtered))
    dropped = max(0, before_count - after_count)
    if dropped > 0:
        mdc.write_line(
            f"{context}: dropped {dropped} row(s) prior to start cutoff={backfill_start.date().isoformat()}."
        )
    return filtered, dropped
