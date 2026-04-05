from __future__ import annotations

from typing import Optional

import pandas as pd

from core import core as mdc
from core import finance_contracts
from core import layer_bucketing
from tasks.finance_data import config as cfg
from tasks.common.delta_write_policy import prepare_delta_write_frame
from tasks.common.silver_contracts import coerce_to_naive_datetime


def _finance_row_identity_columns(df: pd.DataFrame) -> list[str]:
    columns = ["symbol", "date"]
    if "timeframe" in df.columns:
        columns.append("timeframe")
    return columns


def _finance_declared_schema_columns(sub_domain: str) -> tuple[str, ...]:
    normalized_sub_domain = layer_bucketing.normalize_sub_domain(sub_domain)
    if normalized_sub_domain not in finance_contracts.SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN:
        raise ValueError(f"Unsupported finance sub-domain for contract alignment: {sub_domain}")
    expected_columns = finance_contracts.SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN[normalized_sub_domain]
    return ("date", "symbol", *expected_columns[2:])


def _prepare_finance_delta_write_frame(
    df: pd.DataFrame,
    *,
    sub_domain: str,
    path: str,
    skip_empty_without_schema: bool,
):
    return prepare_delta_write_frame(
        df,
        container=cfg.AZURE_CONTAINER_SILVER,
        path=path,
        skip_empty_without_schema=skip_empty_without_schema,
        enforced_schema_columns=_finance_declared_schema_columns(sub_domain),
    )


def _align_finance_frame_to_contract(df: pd.DataFrame, *, sub_domain: str, path: str) -> pd.DataFrame:
    return _prepare_finance_delta_write_frame(
        df,
        sub_domain=sub_domain,
        path=path,
        skip_empty_without_schema=False,
    ).frame


def _repair_symbol_column_aliases(df: pd.DataFrame, *, ticker: str) -> pd.DataFrame:
    out = df.copy()
    duplicate_symbol_cols = [
        col
        for col in out.columns
        if isinstance(col, str) and col.startswith("symbol_") and col[7:].isdigit()
    ]
    if not duplicate_symbol_cols:
        return out

    if "symbol" not in out.columns:
        first_duplicate = duplicate_symbol_cols[0]
        out = out.rename(columns={first_duplicate: "symbol"})
        duplicate_symbol_cols = duplicate_symbol_cols[1:]
        mdc.write_warning(
            f"Silver finance {ticker}: renamed duplicate column {first_duplicate} -> symbol."
        )

    for col in duplicate_symbol_cols:
        if col not in out.columns:
            continue
        primary = out["symbol"].astype("string")
        fallback = out[col].astype("string")
        conflicts = int((primary.notna() & fallback.notna() & (primary != fallback)).sum())
        if conflicts > 0:
            mdc.write_warning(
                f"Silver finance {ticker}: symbol repair conflict in {col}; "
                f"conflicting_rows={conflicts}; keeping existing symbol when both populated."
            )
        out["symbol"] = out["symbol"].combine_first(out[col])
        out = out.drop(columns=[col])
        mdc.write_warning(
            f"Silver finance {ticker}: collapsed duplicate column {col} into symbol."
        )

    return out


def _finance_sub_domain(folder_name: str) -> str:
    key = str(folder_name or "").strip().lower().replace("-", " ").replace("_", " ")
    key = " ".join(key.split())
    if key == "balance sheet":
        return "balance_sheet"
    if key == "income statement":
        return "income_statement"
    if key == "cash flow":
        return "cash_flow"
    if key == "valuation":
        return "valuation"
    return key.replace(" ", "_")


def _split_finance_bucket_rows(df_bucket: Optional[pd.DataFrame], *, ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df_bucket is None or df_bucket.empty:
        empty = pd.DataFrame()
        return empty, empty

    out = df_bucket.copy()
    if "Date" in out.columns and "date" not in out.columns:
        out = out.rename(columns={"Date": "date"})
    if "symbol" not in out.columns and "Symbol" in out.columns:
        out = out.rename(columns={"Symbol": "symbol"})
    if "date" in out.columns:
        out["date"] = coerce_to_naive_datetime(out["date"])
    if "symbol" not in out.columns:
        out["symbol"] = pd.NA
    out["symbol"] = out["symbol"].astype("string").str.upper()
    symbol = str(ticker or "").strip().upper()
    symbol_mask = out["symbol"] == symbol
    return out.loc[symbol_mask].copy(), out.loc[~symbol_mask].copy()
