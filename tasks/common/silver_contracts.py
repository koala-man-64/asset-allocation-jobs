from __future__ import annotations

import re
from typing import Any, Sequence

import pandas as pd

from core import core as mdc


class ContractViolation(ValueError):
    """Raised when job input does not satisfy a data contract."""


_SNAKE_CASE_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_CASE_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")


def coerce_to_naive_datetime(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    if hasattr(parsed.dtype, "tz") and parsed.dtype.tz is not None:
        parsed = parsed.dt.tz_convert(None)
    return parsed


def parse_wait_timeout_seconds(raw: str | None, *, default: float) -> float | None:
    if raw is None:
        return default
    value = str(raw).strip()
    if not value:
        return default
    if value.lower() in {"none", "inf", "infinite", "forever"}:
        return None
    try:
        parsed = float(value)
    except Exception:
        return default
    return max(0.0, parsed)


def _to_snake_case(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return "col"

    text = _SNAKE_CASE_CAMEL_1.sub(r"\1_\2", text)
    text = _SNAKE_CASE_CAMEL_2.sub(r"\1_\2", text)
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    return text or "col"


def normalize_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    names = [_to_snake_case(col) for col in out.columns]

    seen: dict[str, int] = {}
    unique: list[str] = []
    for name in names:
        count = seen.get(name, 0) + 1
        seen[name] = count
        unique.append(name if count == 1 else f"{name}_{count}")

    out.columns = unique
    return out


def require_non_empty_frame(df: pd.DataFrame, *, context: str) -> pd.DataFrame:
    if df is None:
        raise ContractViolation(f"{context}: missing dataframe payload")
    if not isinstance(df, pd.DataFrame):
        raise ContractViolation(f"{context}: expected pandas DataFrame payload, got {type(df).__name__}")
    if df.empty:
        raise ContractViolation(f"{context}: empty dataframe payload")
    return df


def normalize_date_column(
    df: pd.DataFrame,
    *,
    context: str,
    aliases: Sequence[str],
    canonical: str = "Date",
    drop_original: bool = False,
) -> pd.DataFrame:
    """
    Normalize alternate date field names to a canonical column name and coerce to datetime.
    """
    if df is None:
        raise ContractViolation(f"{context}: missing dataframe payload")

    out = df.copy()
    selected = next((name for name in aliases if name in out.columns), None)
    if selected is None:
        raise ContractViolation(
            f"{context}: missing date column among aliases={list(aliases)}"
        )

    if selected != canonical:
        if not drop_original and canonical in out.columns:
            # Keep existing canonical if already present; prefer canonical over aliases.
            out = out.copy()
        else:
            out = out.rename(columns={selected: canonical})
    else:
        out = out.copy()

    out[canonical] = coerce_to_naive_datetime(out[canonical])
    if out[canonical].isna().all():
        raise ContractViolation(f"{context}: date column '{canonical}' has no parseable values")
    return out


def align_to_existing_schema(df: pd.DataFrame, container: str, path: str) -> pd.DataFrame:
    """
    Align a dataframe to an existing Delta table schema if one already exists.
    This keeps historical column order stable and minimizes schema drift failures.
    """
    from tasks.common.delta_write_policy import prepare_delta_write_frame

    decision = prepare_delta_write_frame(
        df,
        container=container,
        path=path,
        skip_empty_without_schema=False,
    )
    return decision.frame


def assert_no_unexpected_mixed_empty(df: pd.DataFrame, *, context: str, alias: str = "Date") -> pd.DataFrame:
    """
    Keep only rows with a parseable date in the required alias, then validate non-empty.
    """
    if alias not in df.columns:
        raise ContractViolation(f"{context}: missing required '{alias}' column")

    out = df.copy()
    out = out.dropna(subset=[alias]).copy()
    if out.empty:
        raise ContractViolation(f"{context}: no valid rows after date filtering on '{alias}'")
    return out


def log_contract_violation(context: str, exc: Exception, *, severity: str = "ERROR") -> None:
    if severity == "ERROR":
        mdc.write_error(f"Data contract violation: {context}: {exc}")
    else:
        mdc.write_warning(f"Data contract warning: {context}: {exc}")
