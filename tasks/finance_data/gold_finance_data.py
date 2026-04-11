import os
import re
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List, Optional

import numpy as np
import pandas as pd

from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.delta_write_policy import prepare_delta_write_frame
from tasks.common.silver_contracts import coerce_to_naive_datetime, normalize_columns_to_snake_case
from core import domain_artifacts
from tasks.common import gold_checkpoint_publication
from core import layer_bucketing
from core.finance_contracts import SILVER_FINANCE_SUBDOMAINS, VALUATION_FINANCE_COLUMNS
from tasks.common.market_reconciliation import (
    collect_delta_market_symbols,
    collect_delta_silver_finance_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)
from core.gold_sync_contracts import (
    bucket_sync_is_current,
    load_domain_sync_state,
    resolve_postgres_dsn,
    sync_gold_bucket,
    sync_state_cache_entry,
    validate_sync_target_schema,
)

@dataclass(frozen=True)
class FeatureJobConfig:
    silver_container: str
    gold_container: str


@dataclass(frozen=True)
class BucketExecutionResult:
    bucket: str
    status: str
    symbols_written: int
    watermark_updated: bool


_NUMBER_RE = re.compile(r"^\s*([-+]?\d*\.?\d+)\s*([kKmMbBtT])?\s*$")
_REQUIRED_FEATURE_COLUMN_ALIASES: Dict[str, Tuple[str, ...]] = {
    "revenue": ("total_revenue", "Total Revenue", "Revenue"),
    "gross_profit": ("gross_profit", "Gross Profit"),
    "net_income": ("net_income", "Net Income", "Net Income Common Stockholders"),
    "operating_cash_flow": (
        "operating_cash_flow",
        "Operating Cash Flow",
        "Total Cash From Operating Activities",
        "Cash Flow From Continuing Operating Activities",
        "Net Cash Provided by Operating Activities",
    ),
    "long_term_debt": (
        "long_term_debt",
        "Long Term Debt",
        "Long Term Debt And Capital Lease Obligation",
        "Long Term Debt & Capital Lease Obligation",
        "Long-term Debt",
        "Long-Term Debt",
    ),
    "total_assets": ("total_assets", "Total Assets"),
    "current_assets": ("current_assets", "Current Assets", "Total Current Assets"),
    "current_liabilities": ("current_liabilities", "Current Liabilities", "Total Current Liabilities"),
    "shares_outstanding": (
        "shares_outstanding",
        "Shares Outstanding",
        "Common Stock Shares Outstanding",
        "Common Shares Outstanding",
        "Ordinary Shares Number",
        "Share Issued",
    ),
}
_OPTIONAL_OUTPUT_COLUMN_ALIASES: Dict[str, Tuple[str, ...]] = {
    "market_cap": ("market_cap", "Market Cap", "MarketCapitalization"),
    "pe_ratio": ("pe_ratio", "PE Ratio", "P/E", "PERatio"),
    **{
        column: (column,)
        for column in VALUATION_FINANCE_COLUMNS
        if column not in {"market_cap", "pe_ratio"}
    },
}
_GOLD_FINANCE_ALPHA26_SUBDOMAINS: Tuple[str, ...] = SILVER_FINANCE_SUBDOMAINS
_GOLD_FINANCE_PIOTROSKI_COLUMNS: Tuple[str, ...] = (
    "date",
    "symbol",
    *VALUATION_FINANCE_COLUMNS,
    "piotroski_roa_pos",
    "piotroski_cfo_pos",
    "piotroski_delta_roa_pos",
    "piotroski_accruals_pos",
    "piotroski_leverage_decrease",
    "piotroski_liquidity_increase",
    "piotroski_no_new_shares",
    "piotroski_gross_margin_increase",
    "piotroski_asset_turnover_increase",
    "piotroski_f_score",
)
_GOLD_FINANCE_FLOAT_COLUMNS: Tuple[str, ...] = tuple(VALUATION_FINANCE_COLUMNS)
_GOLD_FINANCE_PIOTROSKI_INTEGER_COLUMNS: Tuple[str, ...] = tuple(
    column for column in _GOLD_FINANCE_PIOTROSKI_COLUMNS if column.startswith("piotroski_")
)
_FINANCE_POSTGRES_SCHEMA_REMEDIATION_HINT = (
    "Apply deploy/sql/postgres/migrations/0033_add_gold_finance_ratio_columns.sql "
    "or rerun scripts/apply_postgres_migrations.ps1 against the target database."
)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.where(denominator != 0).divide(denominator.where(denominator != 0))


def _normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())



def _resolve_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    if df is None or df.empty:
        return None

    normalized_to_actual: Dict[str, str] = {}
    for col in df.columns:
        normalized_to_actual.setdefault(_normalize_column_name(col), col)

    for candidate in candidates:
        candidate_norm = _normalize_column_name(candidate)
        match = normalized_to_actual.get(candidate_norm)
        if match:
            return match

    return None


def _require_column(df: pd.DataFrame, *, label: str, candidates: Sequence[str]) -> str:
    resolved = _resolve_column(df, candidates)
    if resolved:
        return resolved
    raise ValueError(
        f"Missing required source column for {label}; accepted aliases={list(candidates)}"
    )


def _build_missing_source_column_message(
    label: str,
    candidates: Sequence[str],
) -> str:
    return f"Missing required source column for {label}; accepted aliases={list(candidates)}"


def _append_unique(values: List[str], item: str) -> None:
    if item not in values:
        values.append(item)


def _parse_human_number(value: Any) -> float:
    if value is None:
        return float("nan")

    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "n/a", "na", "-", "--"}:
        return float("nan")

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1].strip()

    percent = False
    if text.endswith("%"):
        percent = True
        text = text[:-1].strip()

    text = text.replace(",", "")
    match = _NUMBER_RE.match(text)
    if not match:
        try:
            parsed = float(text)
        except ValueError:
            return float("nan")
    else:
        parsed = float(match.group(1))
        suffix = (match.group(2) or "").lower()
        multiplier = {"k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12}.get(suffix, 1.0)
        parsed *= multiplier

    if percent:
        parsed /= 100.0
    if negative:
        parsed *= -1.0
    return parsed


def _coerce_numeric(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    return series.apply(_parse_human_number).astype("float64")


def _prepare_table(df: Optional[pd.DataFrame], ticker: str, *, source_label: str) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError(f"Missing required Silver source table for {source_label} ({ticker}).")

    out = normalize_columns_to_snake_case(df)

    if "date" not in out.columns:
        raise ValueError(f"Required date column missing in {source_label} for {ticker}.")

    out["date"] = coerce_to_naive_datetime(out["date"])
    out = out.dropna(subset=["date"]).copy()
    if out.empty:
        raise ValueError(f"No valid dated rows in {source_label} for {ticker}.")

    out["symbol"] = ticker
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)
    return out


def _prepare_optional_table(df: Optional[pd.DataFrame], ticker: str, *, source_label: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "symbol"])
    return _prepare_table(df, ticker, source_label=source_label)


def _preflight_feature_schema(df: pd.DataFrame) -> Dict[str, Any]:
    out = normalize_columns_to_snake_case(df)
    missing_requirements: List[str] = []

    for label, candidates in _REQUIRED_FEATURE_COLUMN_ALIASES.items():
        if _resolve_column(out, candidates) is None:
            _append_unique(
                missing_requirements,
                _build_missing_source_column_message(label, candidates),
            )

    return {
        "missing_requirements": missing_requirements,
        "available_columns": sorted(str(col) for col in out.columns),
    }


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_columns_to_snake_case(df)
    required = {"date", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["date"] = coerce_to_naive_datetime(out["date"])
    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    symbol_key = out["symbol"]

    revenue_col = _require_column(
        out, label="revenue", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["revenue"]
    )
    gross_profit_col = _require_column(
        out, label="gross_profit", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["gross_profit"]
    )
    net_income_col = _require_column(
        out,
        label="net_income",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["net_income"],
    )
    operating_cash_flow_col = _require_column(
        out,
        label="operating_cash_flow",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["operating_cash_flow"],
    )

    long_term_debt_col = _require_column(
        out,
        label="long_term_debt",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["long_term_debt"],
    )
    total_assets_col = _require_column(
        out, label="total_assets", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["total_assets"]
    )
    current_assets_col = _require_column(
        out, label="current_assets", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["current_assets"]
    )
    current_liabilities_col = _require_column(
        out,
        label="current_liabilities",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["current_liabilities"],
    )
    shares_outstanding_col = _require_column(
        out,
        label="shares_outstanding",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["shares_outstanding"],
    )

    revenue = _coerce_numeric(out[revenue_col])
    gross_profit = _coerce_numeric(out[gross_profit_col])
    net_income = _coerce_numeric(out[net_income_col])
    operating_cash_flow = _coerce_numeric(out[operating_cash_flow_col])

    out[revenue_col] = revenue
    out[gross_profit_col] = gross_profit
    out[net_income_col] = net_income
    out[operating_cash_flow_col] = operating_cash_flow

    long_term_debt = _coerce_numeric(out[long_term_debt_col])
    total_assets = _coerce_numeric(out[total_assets_col])
    current_assets = _coerce_numeric(out[current_assets_col])
    current_liabilities = _coerce_numeric(out[current_liabilities_col])
    shares_outstanding = _coerce_numeric(out[shares_outstanding_col])

    out[long_term_debt_col] = long_term_debt
    out[total_assets_col] = total_assets
    out[current_assets_col] = current_assets
    out[current_liabilities_col] = current_liabilities
    out[shares_outstanding_col] = shares_outstanding

    out["rev_qoq"] = _safe_div(revenue, revenue.groupby(symbol_key, sort=False).shift(1)) - 1.0
    out["rev_yoy"] = _safe_div(revenue, revenue.groupby(symbol_key, sort=False).shift(4)) - 1.0
    out["net_inc_yoy"] = _safe_div(net_income, net_income.groupby(symbol_key, sort=False).shift(4)) - 1.0
    out["gross_margin"] = _safe_div(gross_profit, revenue)
    out["margin_delta_qoq"] = out["gross_margin"] - out["gross_margin"].groupby(symbol_key, sort=False).shift(1)
    out["current_ratio_stmt"] = _safe_div(current_assets, current_liabilities)
    out["shares_change_yoy"] = _safe_div(shares_outstanding, shares_outstanding.groupby(symbol_key, sort=False).shift(4)) - 1.0

    net_income_ttm = net_income.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )
    operating_cash_flow_ttm = operating_cash_flow.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )
    revenue_ttm = revenue.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )
    gross_profit_ttm = gross_profit.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )

    out["net_income_ttm"] = net_income_ttm
    out["operating_cash_flow_ttm"] = operating_cash_flow_ttm
    out["roa_ttm"] = _safe_div(net_income_ttm, total_assets)
    out["long_term_debt_to_assets"] = _safe_div(long_term_debt, total_assets)
    out["gross_margin_ttm"] = _safe_div(gross_profit_ttm, revenue_ttm)
    out["asset_turnover_ttm"] = _safe_div(revenue_ttm, total_assets)
    out["shares_outstanding"] = shares_outstanding

    roa_lag = out["roa_ttm"].groupby(symbol_key, sort=False).shift(4)
    lt_debt_lag = out["long_term_debt_to_assets"].groupby(symbol_key, sort=False).shift(4)
    current_ratio_lag = out["current_ratio_stmt"].groupby(symbol_key, sort=False).shift(4)
    gross_margin_lag = out["gross_margin_ttm"].groupby(symbol_key, sort=False).shift(4)
    asset_turnover_lag = out["asset_turnover_ttm"].groupby(symbol_key, sort=False).shift(4)
    shares_outstanding_lag = out["shares_outstanding"].groupby(symbol_key, sort=False).shift(4)

    out["piotroski_roa_pos"] = (out["roa_ttm"] > 0).astype(int)
    out["piotroski_cfo_pos"] = (out["operating_cash_flow_ttm"] > 0).astype(int)
    out["piotroski_delta_roa_pos"] = (out["roa_ttm"] > roa_lag).astype(int)
    out["piotroski_accruals_pos"] = (out["operating_cash_flow_ttm"] > out["net_income_ttm"]).astype(int)
    out["piotroski_leverage_decrease"] = (out["long_term_debt_to_assets"] < lt_debt_lag).astype(int)
    out["piotroski_liquidity_increase"] = (out["current_ratio_stmt"] > current_ratio_lag).astype(int)
    out["piotroski_no_new_shares"] = (out["shares_outstanding"] <= shares_outstanding_lag).astype(int)
    out["piotroski_gross_margin_increase"] = (out["gross_margin_ttm"] > gross_margin_lag).astype(int)
    out["piotroski_asset_turnover_increase"] = (out["asset_turnover_ttm"] > asset_turnover_lag).astype(int)

    piotroski_components = [
        "piotroski_roa_pos",
        "piotroski_cfo_pos",
        "piotroski_delta_roa_pos",
        "piotroski_accruals_pos",
        "piotroski_leverage_decrease",
        "piotroski_liquidity_increase",
        "piotroski_no_new_shares",
        "piotroski_gross_margin_increase",
        "piotroski_asset_turnover_increase",
    ]
    out["piotroski_f_score"] = out[piotroski_components].sum(axis=1)

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def _build_job_config() -> FeatureJobConfig:
    silver_container = os.environ.get("AZURE_CONTAINER_SILVER")
    gold_container = os.environ.get("AZURE_CONTAINER_GOLD")

    if not silver_container or not str(silver_container).strip():
        raise ValueError("Environment variable 'AZURE_CONTAINER_SILVER' is required.")
    if not gold_container or not str(gold_container).strip():
        raise ValueError("Environment variable 'AZURE_CONTAINER_GOLD' is required.")

    return FeatureJobConfig(
        silver_container=str(silver_container).strip(),
        gold_container=str(gold_container).strip(),
    )


def _empty_gold_finance_bucket_frame() -> pd.DataFrame:
    data: dict[str, pd.Series] = {
        "date": pd.Series(dtype="datetime64[ns]"),
        "symbol": pd.Series(dtype="string"),
    }
    for column in _GOLD_FINANCE_FLOAT_COLUMNS:
        data[column] = pd.Series(dtype="float64")
    for column in _GOLD_FINANCE_PIOTROSKI_INTEGER_COLUMNS:
        data[column] = pd.Series(dtype="Int64")
    return pd.DataFrame(data, columns=_GOLD_FINANCE_PIOTROSKI_COLUMNS)


def _coerce_nullable_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _coerce_nullable_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float64")


def _project_gold_finance_piotroski_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_gold_finance_bucket_frame()

    out = normalize_columns_to_snake_case(df).reset_index(drop=True)
    projected = pd.DataFrame(index=out.index)

    if "date" in out.columns:
        projected["date"] = coerce_to_naive_datetime(out["date"])
    else:
        projected["date"] = pd.Series([pd.NaT] * len(out), dtype="datetime64[ns]")

    if "symbol" in out.columns:
        projected["symbol"] = out["symbol"].astype("string")
    else:
        projected["symbol"] = pd.Series([pd.NA] * len(out), dtype="string")

    for column, candidates in _OPTIONAL_OUTPUT_COLUMN_ALIASES.items():
        resolved = _resolve_column(out, candidates)
        if resolved:
            projected[column] = _coerce_nullable_float(out[resolved])
        else:
            projected[column] = pd.Series([np.nan] * len(out), dtype="float64")

    for column in _GOLD_FINANCE_PIOTROSKI_INTEGER_COLUMNS:
        if column in out.columns:
            projected[column] = _coerce_nullable_int(out[column])
        else:
            projected[column] = pd.Series([pd.NA] * len(out), dtype="Int64")

    return projected[list(_GOLD_FINANCE_PIOTROSKI_COLUMNS)].reset_index(drop=True)


def _gold_finance_alpha26_bucket_path(bucket: str) -> str:
    from asset_allocation_contracts.paths import DataPaths

    return DataPaths.get_gold_finance_alpha26_bucket_path(bucket)


def _normalize_sub_domain(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _load_existing_gold_finance_symbol_to_bucket_map(*, sub_domain: Optional[str] = None) -> dict[str, str]:
    out: dict[str, str] = {}
    existing = layer_bucketing.load_layer_symbol_index(layer="gold", domain="finance")
    if existing is None or existing.empty:
        return out
    if "symbol" not in existing.columns or "bucket" not in existing.columns:
        return out

    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    expected_sub_domain = _normalize_sub_domain(sub_domain)
    sub_domain_series = (
        existing["sub_domain"].fillna("").astype(str).map(_normalize_sub_domain)
        if "sub_domain" in existing.columns
        else pd.Series([""] * len(existing), index=existing.index, dtype="string")
    )

    for idx, row in existing.iterrows():
        row_sub_domain = str(sub_domain_series.loc[idx] or "")
        if expected_sub_domain and row_sub_domain != expected_sub_domain:
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        bucket = str(row.get("bucket") or "").strip().upper()
        if not symbol or bucket not in valid_buckets:
            continue
        out[symbol] = bucket
    return out


def _merge_symbol_to_bucket_map(
    existing: dict[str, str],
    *,
    touched_bucket: str,
    touched_symbol_to_bucket: dict[str, str],
) -> dict[str, str]:
    out = {symbol: current_bucket for symbol, current_bucket in existing.items() if current_bucket != touched_bucket}
    out.update(touched_symbol_to_bucket)
    return out


def _load_gold_finance_bucket_template(
    *,
    container: str,
    candidate_paths: Sequence[str],
) -> tuple[pd.DataFrame, bool]:
    from core import delta_core

    for path in candidate_paths:
        try:
            df_existing = delta_core.load_delta(container, path)
        except Exception:
            continue
        if df_existing is None:
            continue
        return _project_gold_finance_piotroski_frame(df_existing).iloc[0:0].copy(), True
    return _empty_gold_finance_bucket_frame(), False


def _run_finance_reconciliation(*, silver_container: str, gold_container: str) -> tuple[int, int]:
    from core import core as mdc
    from core import delta_core
    from asset_allocation_contracts.paths import DataPaths

    silver_client = mdc.get_storage_client(silver_container)
    gold_client = mdc.get_storage_client(gold_container)
    if silver_client is None:
        raise RuntimeError("Gold finance reconciliation requires silver storage client.")
    if gold_client is None:
        raise RuntimeError("Gold finance reconciliation requires gold storage client.")

    silver_symbols = collect_delta_silver_finance_symbols(client=silver_client)
    gold_symbols = collect_delta_market_symbols(client=gold_client, root_prefix="finance/buckets")
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_gold_finance_alpha26_bucket_path(layer_bucketing.bucket_letter(symbol))
        ],
        load_table=lambda path: delta_core.load_delta(gold_container, path),
        store_table=lambda df, path: delta_core.store_delta(df, gold_container, path, mode="overwrite"),
        delete_prefix=gold_client.delete_prefix,
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            gold_container,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    deleted_blobs = purge_stats.deleted_blobs
    if orphan_symbols:
        mdc.write_line(
            "Gold finance reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Gold finance reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Gold finance orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=[
            DataPaths.get_gold_finance_alpha26_bucket_path(bucket)
            for bucket in layer_bucketing.ALPHABET_BUCKETS
        ],
        load_table=lambda path: delta_core.load_delta(gold_container, path),
        store_table=lambda df, path: delta_core.store_delta(df, gold_container, path, mode="overwrite"),
        delete_prefix=gold_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="gold finance reconciliation cutoff",
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            gold_container,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    if cutoff_stats.rows_dropped > 0 or cutoff_stats.tables_rewritten > 0 or cutoff_stats.deleted_blobs > 0:
        mdc.write_line(
            "Gold finance reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(f"Gold finance reconciliation cutoff sweep encountered errors={cutoff_stats.errors}.")
    return len(orphan_symbols), deleted_blobs


def _run_alpha26_finance_gold(
    *,
    silver_container: str,
    gold_container: str,
    backfill_start_iso: Optional[str],
    watermarks: dict,
) -> tuple[int, int, int, int, bool, int, Optional[str]]:
    from core import core as mdc
    from core import delta_core

    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    processed = 0
    skipped_unchanged = 0
    skipped_missing_source = 0
    failed = 0
    failed_symbols = 0
    failed_buckets = 0
    failed_finalization = 0
    watermarks_dirty = False
    symbol_to_bucket = _load_existing_gold_finance_symbol_to_bucket_map()
    postgres_dsn = resolve_postgres_dsn()
    if postgres_dsn:
        try:
            validate_sync_target_schema(
                postgres_dsn,
                domain="finance",
                remediation_hint=_FINANCE_POSTGRES_SCHEMA_REMEDIATION_HINT,
            )
        except Exception as exc:
            mdc.write_error(str(exc))
            mdc.write_line(
                "layer_handoff_status transition=silver_to_gold status=blocked "
                "bucket_statuses={'postgres_schema_drift': 1} failed=1 "
                "failed_symbols=0 failed_buckets=0 failed_finalization=1"
            )
            mdc.write_line(
                "artifact_publication_status layer=gold domain=finance "
                "status=blocked reason=postgres_schema_drift failure_mode=finalization "
                "failed=1 failed_symbols=0 failed_buckets=0 failed_finalization=1 "
                "processed=0 skipped_unchanged=0 skipped_missing_source=0"
            )
            return 0, 0, 0, 1, False, 0, None
    sync_state = load_domain_sync_state(postgres_dsn, domain="finance") if postgres_dsn else {}
    bucket_results: list[BucketExecutionResult] = []
    index_path: Optional[str] = None

    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        from asset_allocation_contracts.paths import DataPaths

        silver_paths = {
            sub_domain: DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
            for sub_domain in _GOLD_FINANCE_ALPHA26_SUBDOMAINS
        }
        gold_path = _gold_finance_alpha26_bucket_path(bucket)
        commits = [
            delta_core.get_delta_last_commit(silver_container, path) for path in silver_paths.values()
        ]
        silver_commit = max([c for c in commits if c is not None], default=None)
        gold_commit = delta_core.get_delta_last_commit(gold_container, gold_path)
        watermark_key = f"bucket::{bucket}"
        prior = watermarks.get(watermark_key, {})
        skip_due_watermark = (
            silver_commit is not None
            and prior.get("silver_last_commit") is not None
            and prior.get("silver_last_commit") >= silver_commit
        )
        postgres_sync_current = (
            bucket_sync_is_current(sync_state, bucket=bucket, source_commit=silver_commit)
            if postgres_dsn
            else True
        )
        if skip_due_watermark and gold_commit is not None and postgres_sync_current:
            skipped_unchanged += 1
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="skipped_unchanged",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )
            continue

        prior_bucket_symbols = sorted(
            symbol for symbol, current_bucket in symbol_to_bucket.items() if current_bucket == bucket
        )
        df_gold_bucket: Optional[pd.DataFrame] = None
        bucket_symbol_to_bucket: dict[str, str] = {}
        bucket_symbol_failures = 0
        template_schema_available = False

        if df_gold_bucket is None and silver_commit is None:
            skipped_missing_source += 1
            template_candidates: list[str] = [gold_path] if gold_commit is not None else []
            df_gold_bucket, template_schema_available = _load_gold_finance_bucket_template(
                container=gold_container,
                candidate_paths=template_candidates,
            )

        if df_gold_bucket is None:
            tables = {
                key: delta_core.load_delta(silver_container, path)
                for key, path in silver_paths.items()
            }
            symbol_candidates: set[str] = set()
            for frame in tables.values():
                if frame is None or frame.empty or "symbol" not in frame.columns:
                    continue
                symbol_candidates.update(
                    str(sym).strip().upper()
                    for sym in frame["symbol"].dropna().astype(str).tolist()
                    if str(sym).strip()
                )

            symbol_frames: list[pd.DataFrame] = []
            for ticker in sorted(symbol_candidates):
                try:
                    df_income = _prepare_table(
                        tables.get("income_statement", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="income_statement",
                    )
                    df_balance = _prepare_table(
                        tables.get("balance_sheet", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="balance_sheet",
                    )
                    df_cashflow = _prepare_table(
                        tables.get("cash_flow", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="cash_flow",
                    )
                    df_valuation = _prepare_optional_table(
                        tables.get("valuation", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="valuation",
                    )
                except Exception as exc:
                    failed += 1
                    failed_symbols += 1
                    bucket_symbol_failures += 1
                    mdc.write_warning(f"Gold finance alpha26 source failed for {ticker}: {exc}")
                    continue

                base_dates = []
                for table in (df_income, df_balance, df_cashflow, df_valuation):
                    if table.empty:
                        continue
                    base_dates.append(table[["date", "symbol"]])
                keys = pd.concat(base_dates, ignore_index=True).drop_duplicates(
                    subset=["symbol", "date"], keep="last"
                )

                merged = keys
                for table, suffix in (
                    (df_income, "_is"),
                    (df_balance, "_bs"),
                    (df_cashflow, "_cf"),
                    (df_valuation, "_val"),
                ):
                    merged = merged.merge(table, on=["symbol", "date"], how="left", suffixes=("", suffix))

                preflight = _preflight_feature_schema(merged)
                if preflight["missing_requirements"]:
                    failed += 1
                    failed_symbols += 1
                    bucket_symbol_failures += 1
                    mdc.write_warning(
                        "Gold finance alpha26 schema preflight failed for "
                        f"{ticker}: missing={preflight['missing_requirements']} "
                        f"available_columns={preflight['available_columns']}"
                    )
                    continue

                try:
                    df_features = compute_features(merged)
                    df_features, _ = apply_backfill_start_cutoff(
                        df_features,
                        date_col="date",
                        backfill_start=backfill_start,
                        context=f"gold finance alpha26 {ticker}",
                    )
                    if df_features is None or df_features.empty:
                        continue
                    symbol_frames.append(_project_gold_finance_piotroski_frame(df_features))
                    bucket_symbol_to_bucket[ticker] = bucket
                except Exception as exc:
                    failed += 1
                    failed_symbols += 1
                    bucket_symbol_failures += 1
                    mdc.write_warning(f"Gold finance alpha26 compute failed for {ticker}: {exc}")

            if symbol_frames:
                df_gold_bucket = _project_gold_finance_piotroski_frame(
                    pd.concat(symbol_frames, ignore_index=True)
                )
            else:
                template_candidates = [gold_path] if gold_commit is not None else []
                if template_candidates:
                    df_gold_bucket, template_schema_available = _load_gold_finance_bucket_template(
                        container=gold_container,
                        candidate_paths=template_candidates,
                    )
                else:
                    df_gold_bucket = _empty_gold_finance_bucket_frame()

        bucket_failed = False
        writes_completed = 0
        write_decision = prepare_delta_write_frame(
            df_gold_bucket.reset_index(drop=True),
            container=gold_container,
            path=gold_path,
            skip_empty_without_schema=not template_schema_available,
        )
        mdc.write_line(
            "delta_write_decision layer=gold domain=finance "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={gold_path}"
        )
        if write_decision.action == "skip_empty_no_schema":
            mdc.write_line(
                f"Skipping Gold finance empty bucket write for {gold_path}: no existing Delta schema."
            )
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=skipped bucket={bucket} "
                "reason=empty_bucket_no_existing_schema symbols_in=0 symbols_out=0 failures=0"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=finance bucket={bucket} "
                "status=blocked reason=empty_bucket_no_existing_schema"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="skipped_empty_no_schema",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )
            continue
        try:
            delta_core.store_delta(write_decision.frame, gold_container, gold_path, mode="overwrite")
            if backfill_start is not None:
                delta_core.vacuum_delta_table(
                    gold_container,
                    gold_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
            try:
                domain_artifacts.write_bucket_artifact(
                    layer="gold",
                    domain="finance",
                    bucket=bucket,
                    df=write_decision.frame,
                    date_column="date",
                    job_name="gold-finance-job",
                )
            except Exception as exc:
                mdc.write_warning(f"Gold finance metadata bucket artifact write failed bucket={bucket}: {exc}")
            if postgres_dsn:
                sync_result = sync_gold_bucket(
                    domain="finance",
                    bucket=bucket,
                    frame=write_decision.frame,
                    scope_symbols=sorted(set(prior_bucket_symbols).union(bucket_symbol_to_bucket.keys())),
                    source_commit=silver_commit,
                    dsn=postgres_dsn,
                )
                sync_state[bucket] = sync_state_cache_entry(sync_result)
                mdc.write_line(
                    "postgres_gold_sync_status "
                    f"domain=finance bucket={bucket} status={sync_result.status} "
                    f"rows_out={sync_result.row_count} symbols_out={sync_result.symbol_count} "
                    f"scope_symbols={sync_result.scope_symbol_count} source_commit={silver_commit}"
                )
            writes_completed += 1
            mdc.write_line(
                "gold_finance_alpha26_write_status "
                f"bucket={bucket} path={gold_path} rows_out={len(write_decision.frame)} "
                f"symbols_out={len(bucket_symbol_to_bucket)} schema=piotroski_plus_valuation"
            )
        except Exception as exc:
            bucket_failed = True
            failed += 1
            failed_buckets += 1
            mdc.write_error(f"Gold finance alpha26 write failed bucket={bucket} path={gold_path}: {exc}")
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                f"reason=write_failure symbols_in={len(bucket_symbol_to_bucket)} symbols_out=0 "
                f"failures={bucket_symbol_failures + 1}"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=finance bucket={bucket} "
                "status=blocked reason=write_failure"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="failed_write",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )

        if bucket_failed or writes_completed <= 0:
            continue

        processed += 1
        updated_symbol_to_bucket = layer_bucketing.merge_symbol_to_bucket_map(
            symbol_to_bucket,
            touched_buckets={bucket},
            touched_symbol_to_bucket=bucket_symbol_to_bucket,
        )
        watermark_updated = False
        if silver_commit is not None:
            try:
                checkpoint = gold_checkpoint_publication.publish_gold_checkpoint_aggregate(
                    domain="finance",
                    bucket=bucket,
                    symbol_to_bucket=symbol_to_bucket,
                    touched_symbol_to_bucket=bucket_symbol_to_bucket,
                    watermarks=watermarks,
                    watermarks_key="gold_finance_features",
                    watermark_key=watermark_key,
                    source_commit=silver_commit,
                    date_column="date",
                    job_name="gold-finance-job",
                    save_watermarks_fn=save_watermarks,
                    publish_domain_artifact=False,
                )
            except Exception as exc:
                failed += 1
                failed_buckets += 1
                mdc.write_error(f"Gold finance alpha26 checkpoint failed bucket={bucket}: {exc}")
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=finance bucket={bucket} "
                    "status=blocked reason=checkpoint_failure"
                )
                bucket_results.append(
                    BucketExecutionResult(
                        bucket=bucket,
                        status="failed_checkpoint",
                        symbols_written=0,
                        watermark_updated=False,
                    )
                )
                continue
            symbol_to_bucket = checkpoint.symbol_to_bucket
            index_path = checkpoint.index_path
            watermarks_dirty = True
            watermark_updated = True
            mdc.write_line(
                f"watermark_update_status layer=gold domain=finance bucket={bucket} status=updated reason=success"
            )
        else:
            symbol_to_bucket = updated_symbol_to_bucket
            mdc.write_line(
                f"watermark_update_status layer=gold domain=finance bucket={bucket} "
                "status=blocked reason=missing_source_commit"
            )
        symbols_written = len(bucket_symbol_to_bucket)
        mdc.write_line(
            f"layer_handoff_status transition=silver_to_gold status=ok bucket={bucket} "
            f"symbols_in={symbols_written + bucket_symbol_failures} "
            f"symbols_out={symbols_written} failures={bucket_symbol_failures}"
        )
        bucket_results.append(
            BucketExecutionResult(
                bucket=bucket,
                status="ok" if bucket_symbol_failures == 0 else "ok_with_failures",
                symbols_written=symbols_written,
                watermark_updated=watermark_updated,
            )
        )

    status_counts: dict[str, int] = {}
    for result in bucket_results:
        status_counts[result.status] = int(status_counts.get(result.status, 0)) + 1
    finalization = gold_checkpoint_publication.finalize_gold_publication(
        domain="finance",
        symbol_to_bucket=symbol_to_bucket,
        date_column="date",
        job_name="gold-finance-job",
        processed=processed,
        skipped_unchanged=skipped_unchanged,
        skipped_missing_source=skipped_missing_source,
        failed_symbols=failed_symbols,
        failed_buckets=failed_buckets,
        failed_finalization=failed_finalization,
        index_path=index_path,
    )
    mdc.write_line(
        "layer_handoff_status transition=silver_to_gold status=complete "
        f"bucket_statuses={status_counts} failed={finalization.failed} "
        f"failed_symbols={finalization.failed_symbols} failed_buckets={finalization.failed_buckets} "
        f"failed_finalization={finalization.failed_finalization}"
    )
    return (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        finalization.failed,
        watermarks_dirty,
        len(symbol_to_bucket),
        finalization.index_path,
    )


def main() -> int:
    from core import core as mdc
    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying historical cutoff to gold finance features: {backfill_start_iso}")
    layer_bucketing.gold_layout_mode()
		
    watermarks = load_watermarks("gold_finance_features")
    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        alpha26_index_path,
    ) = _run_alpha26_finance_gold(
        silver_container=job_cfg.silver_container,
        gold_container=job_cfg.gold_container,
        backfill_start_iso=backfill_start_iso,
        watermarks=watermarks,
    )
    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    if failed == 0:
        try:
            reconciliation_orphans, reconciliation_deleted_blobs = _run_finance_reconciliation(
                silver_container=job_cfg.silver_container,
                gold_container=job_cfg.gold_container,
            )
        except Exception as exc:
            reconciliation_failed = 1
            mdc.write_error(f"Gold finance reconciliation failed: {exc}")
            mdc.write_line(
                "reconciliation_result layer=gold domain=finance "
                "status=failed orphan_count=unknown deleted_blobs=unknown cutoff_rows_dropped=unknown"
            )
    if watermarks_dirty and reconciliation_failed == 0:
        save_watermarks("gold_finance_features", watermarks)
    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Gold finance alpha26 complete: "
        f"processed_buckets={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_missing_source={skipped_missing_source} symbols={alpha26_symbols} "
        f"index_path={alpha26_index_path or 'unavailable'} reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} failed={total_failed}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    from core import core as mdc
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-finance-job"

    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(lambda: write_system_health_marker(layer="gold", domain="finance", job_name=job_name),),
            )
        )
