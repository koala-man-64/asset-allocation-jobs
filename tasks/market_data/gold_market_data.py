"""Gold market feature engineering job.

This module reads silver-layer market bars, computes gold-layer technical
features, and writes bucketed Delta tables for downstream consumers.

Execution flow:
1. `main()` loads diagnostics, runtime config, and backfill settings.
2. `_run_alpha26_market_gold()` iterates alphabet buckets and symbols.
3. `compute_features()` derives technical indicators from OHLCV bars.
4. Bucket tables are written to gold storage and watermarks are updated.
5. Health marker updates run at exit.
"""

import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Iterator, Optional

import numpy as np
import pandas as pd
from asset_allocation_contracts.paths import DataPaths
from asset_allocation_runtime_common.foundation.postgres import PostgresError, connect
from asset_allocation_runtime_common.shared_core.config import parse_debug_symbols

from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from asset_allocation_runtime_common.market_data import domain_artifacts
from tasks.common import gold_checkpoint_publication
from asset_allocation_runtime_common.market_data import layer_bucketing
from asset_allocation_runtime_common.market_data.market_symbols import REGIME_REQUIRED_MARKET_SYMBOLS
from tasks.technical_analysis.market_structure import add_market_structure_features
from tasks.technical_analysis.technical_indicators import (
    add_candlestick_patterns,
    add_heikin_ashi_and_ichimoku,
)
from tasks.common.silver_contracts import normalize_columns_to_snake_case
from tasks.common.delta_write_policy import prepare_delta_write_frame
from tasks.common.delta_write_sanitizer import sanitize_delta_write_frame
from tasks.common.gold_output_contracts import project_gold_output_frame
from tasks.common.market_reconciliation import (
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)
from asset_allocation_runtime_common.market_data.gold_sync_contracts import (
    bucket_sync_is_current,
    load_domain_sync_state,
    resolve_postgres_dsn,
    sync_gold_bucket,
    sync_gold_bucket_chunks,
    sync_state_cache_entry,
)


@dataclass(frozen=True)
class FeatureJobConfig:
    """Runtime configuration needed to execute the gold market job."""

    silver_container: str
    gold_container: str


@dataclass(frozen=True)
class BucketExecutionResult:
    bucket: str
    status: str
    symbols_written: int
    watermark_updated: bool


@dataclass(frozen=True)
class BucketChunkWriteResult:
    chunk_number: int
    rows: int
    symbols: int
    columns: int
    memory_mb: float
    summary: dict[str, Any]


@dataclass(frozen=True)
class GoldMarketRunResult:
    processed: int
    skipped_unchanged: int
    skipped_missing_source: int
    failed: int
    watermarks_dirty: bool
    alpha26_symbols: int
    index_path: Optional[str]
    retry_pending_buckets: int = 0

    def _tuple(self) -> tuple[int, int, int, int, bool, int, Optional[str]]:
        return (
            self.processed,
            self.skipped_unchanged,
            self.skipped_missing_source,
            self.failed,
            self.watermarks_dirty,
            self.alpha26_symbols,
            self.index_path,
        )

    def __iter__(self):
        return iter(self._tuple())

    def __len__(self) -> int:
        return len(self._tuple())

    def __getitem__(self, item):
        return self._tuple()[item]


@dataclass
class BucketStageResult:
    final_frame: Optional[pd.DataFrame]
    staging_used: bool
    staging_root: str
    staging_delta_path: str
    staging_chunk_prefix: str
    final_rows: int
    final_columns: int
    final_memory_mb: float
    bucket_input_symbols: int
    bucket_output_rows: int
    bucket_symbol_failures: int
    bucket_symbol_to_bucket: dict[str, str]
    critical_compute_failure_symbol: Optional[str]
    chunk_summaries: list[dict[str, Any]]


_SILVER_TO_GOLD_REQUIRED_COLUMNS = {
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
}
_GOLD_MARKET_SILVER_SOURCE_COLUMNS: tuple[str, ...] = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dividend_amount",
    "split_coefficient",
)
_BUCKET_PROGRESS_LOG_INTERVAL = 100
_REGIME_REQUIRED_MARKET_SYMBOL_SET = frozenset(REGIME_REQUIRED_MARKET_SYMBOLS)
_MARKET_CHUNK_SYMBOL_LIMIT = 25
_MARKET_CHUNK_ROW_LIMIT = 100_000


def _configured_scope_symbols() -> set[str]:
    return {
        str(symbol or "").strip().upper()
        for symbol in parse_debug_symbols(os.environ.get("DEBUG_SYMBOLS") or "")
        if str(symbol or "").strip()
    }


def _merge_preserved_gold_bucket_rows(
    *,
    bucket: str,
    gold_container: str,
    scoped_symbols: set[str],
    new_frame: pd.DataFrame | None,
) -> pd.DataFrame | None:
    if not scoped_symbols:
        return new_frame

    existing_frame = _load_gold_market_bucket(
        DataPaths.get_gold_market_bucket_path(bucket),
        gold_container=gold_container,
    )
    if existing_frame is None or existing_frame.empty:
        return new_frame

    preserved = existing_frame.copy()
    if "symbol" in preserved.columns:
        preserved = preserved.loc[
            ~preserved["symbol"].astype("string").str.upper().isin(scoped_symbols)
        ].copy()
    if preserved.empty:
        return new_frame
    if new_frame is None or new_frame.empty:
        return preserved.reset_index(drop=True)

    columns = list(dict.fromkeys([*preserved.columns.tolist(), *new_frame.columns.tolist()]))
    return pd.concat(
        [
            preserved.reindex(columns=columns),
            new_frame.reindex(columns=columns),
        ],
        ignore_index=True,
    )


def _frame_memory_mb(df: Optional[pd.DataFrame]) -> float:
    if df is None:
        return 0.0
    try:
        raw_bytes = int(df.memory_usage(index=True, deep=True).sum())
    except Exception:
        return 0.0
    return round(raw_bytes / (1024 * 1024), 2)


def _log_bucket_progress(
    *,
    bucket: str,
    stage: str,
    rows: Optional[int] = None,
    symbols: Optional[int] = None,
    columns: Optional[int] = None,
    memory_mb: Optional[float] = None,
    processed_symbols: Optional[int] = None,
    total_symbols: Optional[int] = None,
    output_symbols: Optional[int] = None,
    output_rows: Optional[int] = None,
    failed_symbols: Optional[int] = None,
    silver_path: Optional[str] = None,
    gold_path: Optional[str] = None,
    silver_commit_present: Optional[bool] = None,
    gold_commit_present: Optional[bool] = None,
) -> None:
    from asset_allocation_runtime_common.market_data import core as mdc
    fields = [f"bucket={bucket}", f"stage={stage}"]
    if silver_path:
        fields.append(f"silver_path={silver_path}")
    if gold_path:
        fields.append(f"gold_path={gold_path}")
    if silver_commit_present is not None:
        fields.append(f"silver_commit_present={str(bool(silver_commit_present)).lower()}")
    if gold_commit_present is not None:
        fields.append(f"gold_commit_present={str(bool(gold_commit_present)).lower()}")
    if rows is not None:
        fields.append(f"rows={int(rows)}")
    if symbols is not None:
        fields.append(f"symbols={int(symbols)}")
    if columns is not None:
        fields.append(f"columns={int(columns)}")
    if memory_mb is not None:
        fields.append(f"memory_mb={float(memory_mb):.2f}")
    if processed_symbols is not None:
        fields.append(f"processed_symbols={int(processed_symbols)}")
    if total_symbols is not None:
        fields.append(f"total_symbols={int(total_symbols)}")
    if output_symbols is not None:
        fields.append(f"output_symbols={int(output_symbols)}")
    if output_rows is not None:
        fields.append(f"output_rows={int(output_rows)}")
    if failed_symbols is not None:
        fields.append(f"failed_symbols={int(failed_symbols)}")
    mdc.write_line("gold_market_bucket_progress " + " ".join(fields))


def _coerce_datetime(series: pd.Series) -> pd.Series:
    """Parse a series to datetimes and normalize timezone-aware values to naive."""

    value = pd.to_datetime(series, errors="coerce")
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _is_retry_pending_postgres_sync_failure(exc: Exception) -> bool:
    return isinstance(exc, PostgresError) and bool(getattr(exc, "failure_transient", False))


def _postgres_sync_failure_field(exc: Exception, name: str) -> str:
    value = str(getattr(exc, name, "") or "").strip().lower()
    return value or "unknown"


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Divide two series while preserving NaN when denominator values are zero."""

    return numerator.where(denominator != 0).divide(denominator.where(denominator != 0))


def _event_flag_from_numeric(series: pd.Series, *, neutral_value: float) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    out = pd.Series(pd.NA, index=numeric.index, dtype="Int64")
    known = numeric.notna()
    if known.any():
        out.loc[known] = (numeric.loc[known] != neutral_value).astype("int64")
    return out


_SNAKE_CASE_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_CASE_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake_case(value: Any) -> str:
    """Normalize a column-like value into a stable snake_case identifier."""

    text = str(value).strip()
    if not text:
        return "col"

    text = _SNAKE_CASE_CAMEL_1.sub(r"\1_\2", text)
    text = _SNAKE_CASE_CAMEL_2.sub(r"\1_\2", text)
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    return text or "col"


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with snake_case and de-duplicated column names."""

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


def _percentile_rank_last(window: np.ndarray) -> float:
    """Return percentile rank of the window's last value within valid samples."""

    if window.size == 0:
        return np.nan
    last = window[-1]
    if np.isnan(last):
        return np.nan
    valid = window[~np.isnan(window)]
    if valid.size == 0:
        return np.nan
    return float((valid <= last).sum() / valid.size)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute gold-layer technical features from OHLCV market rows.

    Required columns after normalization:
    - date, open, high, low, close, volume, symbol

    Output includes return, volatility, drawdown, ATR/gap, moving-average trend,
    range/compression, volume context, market-structure, and candlestick/Ichimoku
    features.
    """

    # Normalize schema once so the rest of the function can use fixed names.
    out = _snake_case_columns(df)

    required = {"date", "open", "high", "low", "close", "volume", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    symbols = out["symbol"].astype("string").str.strip().str.upper().replace("", pd.NA).dropna().unique().tolist()
    if len(symbols) > 1:
        raise ValueError(f"compute_features expects single-symbol input; received symbols={sorted(symbols)}")

    # Coerce input types early. Invalid values become NaN and are handled later.
    out["date"] = _coerce_datetime(out["date"])

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["dividend_amount", "split_coefficient"]:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Keep series math deterministic by sorting and removing duplicate bars.
    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"]
    out["is_dividend_day"] = _event_flag_from_numeric(out["dividend_amount"], neutral_value=0.0)
    out["is_split_day"] = _event_flag_from_numeric(out["split_coefficient"], neutral_value=1.0)

    # Returns over multiple lookback windows.
    for window in (1, 5, 20, 60):
        out[f"return_{window}d"] = close.pct_change(periods=window)

    daily_return = out["return_1d"]
    delta = close.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    avg_gain_14 = gains.rolling(window=14, min_periods=14).mean()
    avg_loss_14 = losses.rolling(window=14, min_periods=14).mean()
    relative_strength = avg_gain_14 / avg_loss_14.replace(0.0, np.nan)
    out["rsi_14d"] = 100.0 - (100.0 / (1.0 + relative_strength))
    out.loc[(avg_loss_14 == 0.0) & (avg_gain_14 > 0.0), "rsi_14d"] = 100.0
    out.loc[(avg_gain_14 == 0.0) & (avg_loss_14 > 0.0), "rsi_14d"] = 0.0

    # Volatility measured as rolling standard deviation of daily return.
    for window in (20, 60):
        out[f"vol_{window}d"] = daily_return.rolling(window=window, min_periods=window).std()

    # Drawdown relative to rolling 1-year high.
    out["rolling_max_252d"] = close.rolling(window=252, min_periods=1).max()
    out["drawdown_1y"] = _safe_div(close, out["rolling_max_252d"]) - 1.0

    # ATR (14-day simple average true range) and normalized opening gap.
    prev_close = close.shift(1)
    true_range_components = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    )
    out["true_range"] = true_range_components.max(axis=1)
    out["atr_14d"] = out["true_range"].rolling(window=14, min_periods=14).mean()
    out["gap_atr"] = _safe_div((out["open"] - prev_close).abs(), out["atr_14d"])

    # Moving-average trend state and crossover event flags.
    for window in (20, 50, 200):
        out[f"sma_{window}d"] = close.rolling(window=window, min_periods=window).mean()

    out["sma_20_gt_sma_50"] = (out["sma_20d"] > out["sma_50d"]).astype(int)
    out["sma_50_gt_sma_200"] = (out["sma_50d"] > out["sma_200d"]).astype(int)
    out["trend_50_200"] = _safe_div(out["sma_50d"], out["sma_200d"]) - 1.0
    out["above_sma_50"] = (close > out["sma_50d"]).astype(int)

    out["sma_20_crosses_above_sma_50"] = (out["sma_20_gt_sma_50"].diff() == 1).astype(int)
    out["sma_20_crosses_below_sma_50"] = (out["sma_20_gt_sma_50"].diff() == -1).astype(int)
    out["sma_50_crosses_above_sma_200"] = (out["sma_50_gt_sma_200"].diff() == 1).astype(int)
    out["sma_50_crosses_below_sma_200"] = (out["sma_50_gt_sma_200"].diff() == -1).astype(int)

    # Compression context from Bollinger-band width and intraday range.
    close_std_20 = close.rolling(window=20, min_periods=20).std()
    bb_mid_20 = out["sma_20d"]
    bb_upper_20 = bb_mid_20 + 2 * close_std_20
    bb_lower_20 = bb_mid_20 - 2 * close_std_20
    out["bb_width_20d"] = _safe_div((bb_upper_20 - bb_lower_20), bb_mid_20)
    out["range_close"] = _safe_div((high - low), close)

    # Additional range-compression score as 1-year percentile rank.
    high_20 = high.rolling(window=20, min_periods=20).max()
    low_20 = low.rolling(window=20, min_periods=20).min()
    out["range_20"] = _safe_div((high_20 - low_20), close)
    out["compression_score"] = out["range_20"].rolling(window=252, min_periods=1).apply(_percentile_rank_last, raw=True)

    # Volume context from short-window z-score and long-window percentile rank.
    vol_mean_20 = volume.rolling(window=20, min_periods=20).mean()
    vol_std_20 = volume.rolling(window=20, min_periods=20).std()
    out["volume_z_20d"] = _safe_div((volume - vol_mean_20), vol_std_20)
    out["volume_pct_rank_252d"] = volume.rolling(window=252, min_periods=1).apply(_percentile_rank_last, raw=True)

    # Market structure features use confirmed pivots only to avoid look-ahead.
    out = add_market_structure_features(out)

    # Shared TA enrichments are centralized in `tasks.technical_analysis`.
    out = add_candlestick_patterns(out)
    out = add_heikin_ashi_and_ichimoku(out)

    # Internal helper columns (prefixed with "_") are implementation detail only.
    helper_cols = [col for col in out.columns if str(col).startswith("_")]
    if helper_cols:
        out = out.drop(columns=helper_cols, errors="ignore")

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def _build_job_config() -> FeatureJobConfig:
    """Resolve storage containers for the bucket-based gold market job."""

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


def _resolve_gold_market_reconciliation_clients(
    *,
    silver_container: str,
    gold_container: str,
):
    from asset_allocation_runtime_common.market_data import core as mdc

    silver_client = mdc.get_storage_client(silver_container)
    gold_client = mdc.get_storage_client(gold_container)
    if silver_client is None:
        raise RuntimeError("Gold market reconciliation requires silver storage client.")
    if gold_client is None:
        raise RuntimeError("Gold market reconciliation requires gold storage client.")
    return silver_client, gold_client


def _load_gold_market_bucket(path: str, *, gold_container: str) -> pd.DataFrame | None:
    from asset_allocation_runtime_common.market_data import delta_core

    return delta_core.load_delta(gold_container, path)


def _store_gold_market_bucket(df: pd.DataFrame, path: str, *, gold_container: str) -> None:
    from asset_allocation_runtime_common.market_data import delta_core

    delta_core.store_delta(sanitize_delta_write_frame(df), gold_container, path, mode="overwrite")


def _vacuum_gold_market_bucket(path: str, *, gold_container: str) -> None:
    from asset_allocation_runtime_common.market_data import delta_core

    delta_core.vacuum_delta_table(
        gold_container,
        path,
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
        full=True,
    )


def _run_market_reconciliation(*, silver_container: str, gold_container: str) -> tuple[int, int]:
    """Reconcile gold market tables with silver source symbols and backfill policy.

    Returns:
    - orphan symbol count
    - number of blobs deleted while purging orphans
    """

    from asset_allocation_runtime_common.market_data import core as mdc
    from asset_allocation_contracts.paths import DataPaths

    silver_client, gold_client = _resolve_gold_market_reconciliation_clients(
        silver_container=silver_container,
        gold_container=gold_container,
    )

    # Discover symbol sets directly from Delta table prefixes.
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="market-data")
    gold_symbols = collect_delta_market_symbols(client=gold_client, root_prefix="market")

    # Remove gold tables that no longer have an upstream silver source.
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_gold_market_bucket_path(layer_bucketing.bucket_letter(symbol))
        ],
        load_table=lambda path: _load_gold_market_bucket(path, gold_container=gold_container),
        store_table=lambda df, path: _store_gold_market_bucket(df, path, gold_container=gold_container),
        delete_prefix=gold_client.delete_prefix,
        vacuum_table=lambda path: _vacuum_gold_market_bucket(path, gold_container=gold_container),
    )
    deleted_blobs = purge_stats.deleted_blobs
    if orphan_symbols:
        mdc.write_line(
            "Gold market reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Gold market reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Gold market orphan purge encountered errors={purge_stats.errors}.")

    # Apply the same backfill cutoff policy used by active processing.
    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=layer_bucketing.all_gold_bucket_paths(domain="market"),
        load_table=lambda path: _load_gold_market_bucket(path, gold_container=gold_container),
        store_table=lambda df, path: _store_gold_market_bucket(df, path, gold_container=gold_container),
        delete_prefix=gold_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="gold market reconciliation cutoff",
        vacuum_table=lambda path: _vacuum_gold_market_bucket(path, gold_container=gold_container),
    )
    if cutoff_stats.rows_dropped > 0 or cutoff_stats.tables_rewritten > 0 or cutoff_stats.deleted_blobs > 0:
        mdc.write_line(
            "Gold market reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(f"Gold market reconciliation cutoff sweep encountered errors={cutoff_stats.errors}.")
    status = "failed" if cutoff_stats.errors > 0 else "ok"
    mdc.write_line(
        "reconciliation_result layer=gold domain=market "
        f"status={status} orphan_count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
        f"cutoff_rows_dropped={cutoff_stats.rows_dropped} cutoff_tables_rewritten={cutoff_stats.tables_rewritten} "
        f"cutoff_errors={cutoff_stats.errors}"
    )
    return len(orphan_symbols), deleted_blobs


def _load_existing_gold_symbol_to_bucket_map() -> dict[str, str]:
    out: dict[str, str] = {}
    existing = layer_bucketing.load_layer_symbol_index(layer="gold", domain="market")
    if existing is None or existing.empty:
        return out
    if "symbol" not in existing.columns or "bucket" not in existing.columns:
        return out
    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    for _, row in existing.iterrows():
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
    out = {symbol: bucket for symbol, bucket in existing.items() if bucket != touched_bucket}
    out.update(touched_symbol_to_bucket)
    return out


def _gold_market_job_run_id() -> str:
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip()
    if execution_name:
        return execution_name
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"gold-market-job-{stamp}-{os.getpid()}"


def _gold_market_staging_root(*, run_id: str, bucket: str) -> str:
    clean_bucket = str(bucket or "").strip().upper()
    return f"system/gold-market-job/runs/{run_id}/market/buckets/{clean_bucket}"


def _gold_market_staging_delta_path(*, run_id: str, bucket: str) -> str:
    return f"{_gold_market_staging_root(run_id=run_id, bucket=bucket)}/delta"


def _gold_market_staging_chunk_prefix(*, run_id: str, bucket: str) -> str:
    return f"{_gold_market_staging_root(run_id=run_id, bucket=bucket)}/chunks"


def _gold_market_staging_chunk_blob_path(*, run_id: str, bucket: str, chunk_number: int) -> str:
    return f"{_gold_market_staging_chunk_prefix(run_id=run_id, bucket=bucket)}/chunk-{int(chunk_number):05d}.parquet"


def _get_gold_market_storage_client(gold_container: str):
    from asset_allocation_runtime_common.market_data import core as mdc
    client = mdc.get_storage_client(gold_container)
    if client is None:
        raise RuntimeError(f"Gold market staging requires storage client for container={gold_container}.")
    return client


def _write_staged_market_chunk_blob(
    *,
    gold_container: str,
    blob_path: str,
    frame: pd.DataFrame,
) -> None:
    client = _get_gold_market_storage_client(gold_container)
    client.write_parquet(blob_path, frame)


def _write_staged_market_chunk(
    *,
    bucket: str,
    gold_container: str,
    staging_delta_path: str,
    chunk_blob_path: str,
    chunk_frames: list[pd.DataFrame],
    chunk_number: int,
    is_first_chunk: bool,
) -> BucketChunkWriteResult:
    from asset_allocation_runtime_common.market_data import delta_core
    if not chunk_frames:
        raise ValueError("chunk_frames must not be empty")

    chunk_frame = (
        chunk_frames[0].reset_index(drop=True)
        if len(chunk_frames) == 1
        else pd.concat(chunk_frames, ignore_index=True)
    )
    chunk_rows = int(len(chunk_frame))
    chunk_symbols = (
        int(chunk_frame["symbol"].dropna().astype("string").nunique())
        if "symbol" in chunk_frame.columns
        else 0
    )
    chunk_columns = int(len(chunk_frame.columns))
    chunk_memory_mb = _frame_memory_mb(chunk_frame)
    _log_bucket_progress(
        bucket=bucket,
        stage="chunk_flush_started",
        rows=chunk_rows,
        symbols=chunk_symbols,
        columns=chunk_columns,
        memory_mb=chunk_memory_mb,
    )

    write_decision = prepare_delta_write_frame(
        chunk_frame,
        container=gold_container,
        path=staging_delta_path,
    )
    if write_decision.action == "skip_empty_no_schema":
        raise RuntimeError(
            f"Unexpected empty staging write for gold market bucket={bucket} path={staging_delta_path}"
        )

    staged_frame = write_decision.frame
    _log_bucket_progress(
        bucket=bucket,
        stage="staging_write_started",
        rows=int(len(staged_frame)),
        symbols=chunk_symbols,
        columns=int(len(staged_frame.columns)),
        memory_mb=_frame_memory_mb(staged_frame),
    )
    delta_core.store_delta(
        staged_frame,
        gold_container,
        staging_delta_path,
        mode="overwrite" if is_first_chunk else "append",
    )
    _write_staged_market_chunk_blob(
        gold_container=gold_container,
        blob_path=chunk_blob_path,
        frame=staged_frame,
    )
    summary = domain_artifacts.summarize_frame(
        staged_frame,
        domain="market",
        date_column="date",
    )
    _log_bucket_progress(
        bucket=bucket,
        stage="staging_write_completed",
        rows=int(len(staged_frame)),
        symbols=chunk_symbols,
        columns=int(len(staged_frame.columns)),
        memory_mb=_frame_memory_mb(staged_frame),
    )
    _log_bucket_progress(
        bucket=bucket,
        stage="chunk_flush_completed",
        rows=int(len(staged_frame)),
        symbols=chunk_symbols,
        columns=int(len(staged_frame.columns)),
        memory_mb=_frame_memory_mb(staged_frame),
    )
    return BucketChunkWriteResult(
        chunk_number=chunk_number,
        rows=int(len(staged_frame)),
        symbols=chunk_symbols,
        columns=int(len(staged_frame.columns)),
        memory_mb=_frame_memory_mb(staged_frame),
        summary=summary,
    )


def _promote_staged_market_bucket(
    *,
    gold_container: str,
    staging_delta_path: str,
    gold_path: str,
) -> int:
    client = _get_gold_market_storage_client(gold_container)
    source_prefix = str(staging_delta_path or "").strip().strip("/")
    destination_prefix = str(gold_path or "").strip().strip("/")
    if not source_prefix or not destination_prefix:
        raise ValueError("staging_delta_path and gold_path are required")

    blob_infos = sorted(
        client.list_blob_infos(name_starts_with=source_prefix),
        key=lambda item: str(item.get("name") or ""),
    )
    if not blob_infos:
        raise RuntimeError(f"No staged gold market blobs found under {source_prefix}")

    client.delete_prefix(destination_prefix)
    copied = 0
    for blob in blob_infos:
        source_name = str(blob.get("name") or "").strip()
        if not source_name.startswith(source_prefix):
            continue
        suffix = source_name[len(source_prefix):].lstrip("/")
        destination_name = destination_prefix if not suffix else f"{destination_prefix}/{suffix}"
        payload = client.download_data(source_name)
        if payload is None:
            raise RuntimeError(f"Unable to download staged gold market blob {source_name}")
        client.upload_data(destination_name, payload, overwrite=True)
        copied += 1
    if copied == 0:
        raise RuntimeError(f"No staged gold market blobs copied from {source_prefix}")
    return copied


def _iter_staged_market_chunk_frames(
    *,
    gold_container: str,
    chunk_prefix: str,
) -> Iterator[pd.DataFrame]:
    client = _get_gold_market_storage_client(gold_container)
    blob_infos = sorted(
        client.list_blob_infos(name_starts_with=str(chunk_prefix or "").strip().strip("/")),
        key=lambda item: str(item.get("name") or ""),
    )
    for blob in blob_infos:
        blob_name = str(blob.get("name") or "").strip()
        if not blob_name.endswith(".parquet"):
            continue
        frame = client.read_parquet(blob_name)
        if frame is None:
            raise RuntimeError(f"Unable to load staged gold market chunk {blob_name}")
        yield frame


def _cleanup_staged_market_bucket(
    *,
    gold_container: str,
    staging_root: str,
) -> int:
    client = _get_gold_market_storage_client(gold_container)
    return client.delete_prefix(str(staging_root or "").strip().strip("/"))


def _write_gold_market_bucket_artifact_from_summaries(
    *,
    gold_container: str,
    bucket: str,
    summaries: list[dict[str, Any]],
    symbol_count: int,
    job_run_id: str,
    data_path: str,
) -> Optional[dict[str, Any]]:
    from asset_allocation_runtime_common.market_data import core as mdc
    storage_client = _get_gold_market_storage_client(gold_container)
    aggregate_summary = domain_artifacts.aggregate_summaries(
        summaries,
        symbol_count_override=symbol_count,
        date_column="date",
    )
    now = datetime.now(timezone.utc).isoformat()
    artifact_path = domain_artifacts.bucket_artifact_path(
        layer="gold",
        domain="market",
        bucket=bucket,
    )
    payload = {
        "version": domain_artifacts.ARTIFACT_VERSION,
        "scope": "bucket",
        "layer": "gold",
        "domain": "market",
        "subDomain": None,
        "bucket": str(bucket).strip().upper(),
        "rootPath": domain_artifacts.root_prefix(layer="gold", domain="market"),
        "artifactPath": artifact_path,
        "updatedAt": now,
        "computedAt": now,
        "producerJobName": "gold-market-job",
        "jobRunId": str(job_run_id or "").strip() or None,
        "runId": str(job_run_id or "").strip() or None,
        "manifestPath": None,
        "activeDataPrefix": None,
        "dataPath": str(data_path or "").strip() or None,
        **aggregate_summary,
    }
    mdc.save_json_content(payload, artifact_path, client=storage_client)
    return payload


def _persist_gold_market_bucket_checkpoint(
    *,
    bucket: str,
    watermark_key: str,
    silver_commit: Optional[float],
    watermarks: dict[str, Any],
    symbol_to_bucket: dict[str, str],
    bucket_symbol_to_bucket: dict[str, str],
    run_id: Optional[str],
) -> tuple[dict[str, str], str]:
    checkpoint = gold_checkpoint_publication.publish_gold_checkpoint_aggregate(
        domain="market",
        bucket=bucket,
        symbol_to_bucket=symbol_to_bucket,
        touched_symbol_to_bucket=bucket_symbol_to_bucket,
        watermarks=watermarks,
        watermarks_key="gold_market_features",
        watermark_key=watermark_key,
        source_commit=silver_commit,
        date_column="date",
        job_name="gold-market-job",
        save_watermarks_fn=save_watermarks,
        job_run_id=run_id,
        run_id=run_id,
        publish_domain_artifact=False,
    )
    return checkpoint.symbol_to_bucket, checkpoint.index_path


def _stage_market_bucket_outputs(
    *,
    bucket: str,
    silver_container: str,
    gold_container: str,
    silver_path: str,
    backfill_start: Optional[pd.Timestamp],
    run_id: str,
) -> BucketStageResult:
    from asset_allocation_runtime_common.market_data import delta_core
    scoped_symbols = _configured_scope_symbols()
    df_silver_bucket = delta_core.load_delta(
        silver_container,
        silver_path,
        columns=list(_GOLD_MARKET_SILVER_SOURCE_COLUMNS),
    )
    bucket_input_rows = 0
    bucket_input_symbols = 0
    bucket_input_columns = 0
    if df_silver_bucket is not None and not df_silver_bucket.empty:
        bucket_input_rows = int(len(df_silver_bucket))
        bucket_input_columns = int(len(df_silver_bucket.columns))
        if "symbol" in df_silver_bucket.columns:
            bucket_input_symbols = int(df_silver_bucket["symbol"].dropna().astype("string").nunique())
        _log_bucket_progress(
            bucket=bucket,
            stage="source_loaded",
            rows=bucket_input_rows,
            symbols=bucket_input_symbols,
            columns=bucket_input_columns,
            memory_mb=_frame_memory_mb(df_silver_bucket),
            silver_path=silver_path,
        )

    try:
        df_silver_bucket = _validate_silver_to_gold_market_bucket_contract(
            df_silver_bucket,
            bucket=bucket,
        )
    except Exception as exc:
        raise RuntimeError(f"contract_validation::{bucket_input_symbols}::{exc}") from exc
    if scoped_symbols and "symbol" in df_silver_bucket.columns:
        df_silver_bucket = df_silver_bucket.loc[
            df_silver_bucket["symbol"].astype("string").str.upper().isin(scoped_symbols)
        ].copy()
        bucket_input_rows = int(len(df_silver_bucket))
        bucket_input_symbols = (
            int(df_silver_bucket["symbol"].dropna().astype("string").nunique())
            if "symbol" in df_silver_bucket.columns
            else 0
        )
        _log_bucket_progress(
            bucket=bucket,
            stage="scope_filtered",
            rows=bucket_input_rows,
            symbols=bucket_input_symbols,
            columns=int(len(df_silver_bucket.columns)),
            memory_mb=_frame_memory_mb(df_silver_bucket),
            silver_path=silver_path,
        )
    bucket_symbol_to_bucket: dict[str, str] = {}
    critical_compute_failure_symbol: Optional[str] = None
    bucket_symbol_failures = 0
    bucket_output_rows = 0
    chunk_frames: list[pd.DataFrame] = []
    chunk_row_count = 0
    chunk_number = 0
    chunk_write_results: list[BucketChunkWriteResult] = []
    chunk_summaries: list[dict[str, Any]] = []
    staging_root = _gold_market_staging_root(run_id=run_id, bucket=bucket)
    staging_delta_path = _gold_market_staging_delta_path(run_id=run_id, bucket=bucket)
    staging_chunk_prefix = _gold_market_staging_chunk_prefix(run_id=run_id, bucket=bucket)

    try:
        for processed_symbols, (symbol, group) in enumerate(df_silver_bucket.groupby("symbol"), start=1):
            ticker = str(symbol or "").strip().upper()
            if not ticker:
                continue
            try:
                df_features = compute_features(group)
                df_features, _ = apply_backfill_start_cutoff(
                    df_features,
                    date_col="date",
                    backfill_start=backfill_start,
                    context=f"gold market alpha26 {ticker}",
                )
                if df_features is None or df_features.empty:
                    if (
                        processed_symbols == 1
                        or processed_symbols % _BUCKET_PROGRESS_LOG_INTERVAL == 0
                        or processed_symbols == bucket_input_symbols
                    ):
                        _log_bucket_progress(
                            bucket=bucket,
                            stage="symbol_progress",
                            processed_symbols=processed_symbols,
                            total_symbols=bucket_input_symbols,
                            output_symbols=len(bucket_symbol_to_bucket),
                            output_rows=bucket_output_rows,
                            failed_symbols=bucket_symbol_failures,
                        )
                    continue

                df_projected = project_gold_output_frame(df_features, domain="market")
                if df_projected is None or df_projected.empty:
                    if (
                        processed_symbols == 1
                        or processed_symbols % _BUCKET_PROGRESS_LOG_INTERVAL == 0
                        or processed_symbols == bucket_input_symbols
                    ):
                        _log_bucket_progress(
                            bucket=bucket,
                            stage="symbol_progress",
                            processed_symbols=processed_symbols,
                            total_symbols=bucket_input_symbols,
                            output_symbols=len(bucket_symbol_to_bucket),
                            output_rows=bucket_output_rows,
                            failed_symbols=bucket_symbol_failures,
                        )
                    continue

                chunk_frames.append(df_projected)
                chunk_row_count += int(len(df_projected))
                bucket_output_rows += int(len(df_projected))
                bucket_symbol_to_bucket[ticker] = bucket

                if len(chunk_frames) >= _MARKET_CHUNK_SYMBOL_LIMIT or chunk_row_count >= _MARKET_CHUNK_ROW_LIMIT:
                    if chunk_number == 0:
                        _cleanup_staged_market_bucket(gold_container=gold_container, staging_root=staging_root)
                    chunk_number += 1
                    chunk_result = _write_staged_market_chunk(
                        bucket=bucket,
                        gold_container=gold_container,
                        staging_delta_path=staging_delta_path,
                        chunk_blob_path=_gold_market_staging_chunk_blob_path(
                            run_id=run_id,
                            bucket=bucket,
                            chunk_number=chunk_number,
                        ),
                        chunk_frames=chunk_frames,
                        chunk_number=chunk_number,
                        is_first_chunk=chunk_number == 1,
                    )
                    chunk_write_results.append(chunk_result)
                    chunk_summaries.append(chunk_result.summary)
                    chunk_frames.clear()
                    chunk_row_count = 0
            except Exception as exc:
                if _is_regime_required_market_symbol(ticker):
                    critical_compute_failure_symbol = ticker
                    bucket_symbol_failures += 1
                    _log_bucket_progress(
                        bucket=bucket,
                        stage="symbol_progress",
                        processed_symbols=processed_symbols,
                        total_symbols=bucket_input_symbols,
                        output_symbols=len(bucket_symbol_to_bucket),
                        output_rows=bucket_output_rows,
                        failed_symbols=bucket_symbol_failures,
                    )
                    raise RuntimeError(f"critical_symbol::{ticker}::{exc}") from exc
                bucket_symbol_failures += 1
                from asset_allocation_runtime_common.market_data import core as mdc
                mdc.write_warning(f"Gold market alpha26 compute failed for {ticker}: {exc}")
            if (
                processed_symbols == 1
                or processed_symbols % _BUCKET_PROGRESS_LOG_INTERVAL == 0
                or processed_symbols == bucket_input_symbols
            ):
                _log_bucket_progress(
                    bucket=bucket,
                    stage="symbol_progress",
                    processed_symbols=processed_symbols,
                    total_symbols=bucket_input_symbols,
                    output_symbols=len(bucket_symbol_to_bucket),
                    output_rows=bucket_output_rows,
                    failed_symbols=bucket_symbol_failures,
                )

        if chunk_frames:
            if chunk_number == 0:
                _cleanup_staged_market_bucket(gold_container=gold_container, staging_root=staging_root)
            chunk_number += 1
            chunk_result = _write_staged_market_chunk(
                bucket=bucket,
                gold_container=gold_container,
                staging_delta_path=staging_delta_path,
                chunk_blob_path=_gold_market_staging_chunk_blob_path(
                    run_id=run_id,
                    bucket=bucket,
                    chunk_number=chunk_number,
                ),
                chunk_frames=chunk_frames,
                chunk_number=chunk_number,
                is_first_chunk=chunk_number == 1,
            )
            chunk_write_results.append(chunk_result)
            chunk_summaries.append(chunk_result.summary)
            chunk_frames.clear()
            chunk_row_count = 0
    except RuntimeError as exc:
        message = str(exc)
        if message.startswith("critical_symbol::"):
            _, critical_symbol, detail = message.split("::", 2)
            critical_compute_failure_symbol = critical_symbol
            from asset_allocation_runtime_common.market_data import core as mdc
            mdc.write_error(f"Gold market alpha26 compute failed for critical symbol {critical_symbol}: {detail}")
        else:
            raise
    finally:
        del df_silver_bucket

    _log_bucket_progress(
        bucket=bucket,
        stage="compute_complete",
        processed_symbols=bucket_input_symbols,
        total_symbols=bucket_input_symbols,
        output_symbols=len(bucket_symbol_to_bucket),
        output_rows=bucket_output_rows,
        failed_symbols=bucket_symbol_failures,
    )

    if chunk_write_results:
        final_columns = chunk_write_results[-1].columns
        final_memory_mb = chunk_write_results[-1].memory_mb
        final_frame: Optional[pd.DataFrame] = None
        staging_used = True
        final_rows = bucket_output_rows
        _log_bucket_progress(
            bucket=bucket,
            stage="bucket_frame_ready",
            rows=final_rows,
            symbols=len(bucket_symbol_to_bucket),
            columns=final_columns,
            memory_mb=final_memory_mb,
            output_rows=bucket_output_rows,
            output_symbols=len(bucket_symbol_to_bucket),
            failed_symbols=bucket_symbol_failures,
        )
    else:
        final_frame = project_gold_output_frame(pd.DataFrame(columns=["date", "symbol"]), domain="market")
        staging_used = False
        final_rows = int(len(final_frame))
        final_columns = int(len(final_frame.columns))
        final_memory_mb = _frame_memory_mb(final_frame)
        _log_bucket_progress(
            bucket=bucket,
            stage="bucket_frame_ready",
            rows=final_rows,
            symbols=0,
            columns=final_columns,
            memory_mb=final_memory_mb,
            output_rows=bucket_output_rows,
            output_symbols=len(bucket_symbol_to_bucket),
            failed_symbols=bucket_symbol_failures,
        )

    return BucketStageResult(
        final_frame=final_frame,
        staging_used=staging_used,
        staging_root=staging_root,
        staging_delta_path=staging_delta_path,
        staging_chunk_prefix=staging_chunk_prefix,
        final_rows=final_rows,
        final_columns=final_columns,
        final_memory_mb=final_memory_mb,
        bucket_input_symbols=bucket_input_symbols,
        bucket_output_rows=bucket_output_rows,
        bucket_symbol_failures=bucket_symbol_failures,
        bucket_symbol_to_bucket=bucket_symbol_to_bucket,
        critical_compute_failure_symbol=critical_compute_failure_symbol,
        chunk_summaries=chunk_summaries,
    )


def _normalize_market_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _is_regime_required_market_symbol(value: object) -> bool:
    return _normalize_market_symbol(value) in _REGIME_REQUIRED_MARKET_SYMBOL_SET


def _verify_postgres_critical_market_symbols(
    *,
    dsn: str,
    sync_state: dict[str, dict[str, Any]],
) -> None:
    from asset_allocation_runtime_common.market_data import core as mdc
    required_symbols = tuple(REGIME_REQUIRED_MARKET_SYMBOLS)
    required_buckets = {
        symbol: layer_bucketing.bucket_letter(symbol)
        for symbol in required_symbols
    }
    missing_sync: list[str] = []
    for symbol, bucket in required_buckets.items():
        state = sync_state.get(bucket, {})
        status = str(state.get("status") or "").strip().lower()
        if status != "success":
            missing_sync.append(f"{symbol}:{bucket}:{status or 'missing'}")

    try:
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, COUNT(*) AS row_count
                    FROM gold.market_data
                    WHERE symbol = ANY(%s)
                    GROUP BY symbol
                    """,
                    (list(required_symbols),),
                )
                rows = cur.fetchall()
    except Exception as exc:
        mdc.write_line(
            "postgres_gold_critical_symbol_status "
            "domain=market status=failed reason=query_failed"
        )
        raise ValueError(
            "Gold market critical-symbol verification failed: unable to query gold.market_data "
            f"for {required_symbols}: {type(exc).__name__}: {exc}"
        ) from exc

    observed_symbols = {
        _normalize_market_symbol(symbol)
        for symbol, row_count in rows
        if int(row_count or 0) > 0
    }
    missing_symbols = sorted(set(required_symbols).difference(observed_symbols))

    if missing_sync or missing_symbols:
        mdc.write_line(
            "postgres_gold_critical_symbol_status "
            "domain=market status=failed "
            f"missing_symbols={missing_symbols or ['none']} "
            f"missing_sync={missing_sync or ['none']}"
        )
        raise ValueError(
            "Gold market critical-symbol verification failed: "
            f"missing_symbols={missing_symbols or ['none']} "
            f"missing_sync={missing_sync or ['none']}"
        )

    mdc.write_line(
        "postgres_gold_critical_symbol_status "
        "domain=market status=ok "
        f"symbols={list(required_symbols)} "
        f"buckets={sorted(set(required_buckets.values()))}"
    )


def _validate_silver_to_gold_market_bucket_contract(
    df_silver_bucket: pd.DataFrame,
    *,
    bucket: str,
) -> pd.DataFrame:
    if df_silver_bucket is None:
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: source frame is None.")

    normalized = normalize_columns_to_snake_case(df_silver_bucket.copy())
    missing = sorted(_SILVER_TO_GOLD_REQUIRED_COLUMNS.difference(set(normalized.columns)))
    if missing:
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: missing required columns={missing}")

    if normalized.empty:
        return normalized

    parsed_dates = pd.to_datetime(normalized["date"], errors="coerce").dropna()
    if parsed_dates.empty:
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: no parseable date values.")

    symbols = normalized["symbol"].astype("string").str.strip().str.upper()
    if symbols.empty or symbols.eq("").all():
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: no non-empty symbols.")

    return normalized


def _run_alpha26_market_gold(
    *,
    silver_container: str,
    gold_container: str,
    backfill_start_iso: Optional[str],
    watermarks: dict,
) -> GoldMarketRunResult:
    """Build and write bucketed gold market tables from silver alpha26 inputs.

    Processing model:
    - Iterate alphabetical buckets from `layer_bucketing.ALPHABET_BUCKETS`.
    - Skip unchanged buckets via commit watermarks unless force-rebuild is enabled.
    - Compute features for each symbol, then write one consolidated Delta table per bucket.

    Returns:
    - processed bucket count
    - skipped unchanged bucket count
    - skipped missing-source bucket count
    - failure count
    - watermark dirty flag
    - indexed symbol count
    - symbol index path (if available)
    - retry-pending bucket count
    """

    from asset_allocation_runtime_common.market_data import core as mdc
    from asset_allocation_runtime_common.market_data import delta_core
    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    scoped_symbols = _configured_scope_symbols()

    # Track per-run outcomes for caller status and logging.
    failed = 0
    failed_symbols = 0
    failed_buckets = 0
    failed_finalization = 0
    retry_pending_buckets = 0
    processed = 0
    skipped_unchanged = 0
    skipped_missing_source = 0
    symbol_to_bucket = _load_existing_gold_symbol_to_bucket_map()
    postgres_dsn = resolve_postgres_dsn()
    sync_state = load_domain_sync_state(postgres_dsn, domain="market") if postgres_dsn else {}
    run_id = _gold_market_job_run_id()
    watermarks_dirty = False
    bucket_results: list[BucketExecutionResult] = []
    index_path: Optional[str] = None

    # Each bucket maps to one silver source table and one gold destination table.
    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        silver_path = DataPaths.get_silver_market_bucket_path(bucket)
        gold_path = DataPaths.get_gold_market_bucket_path(bucket)
        watermark_key = f"bucket::{bucket}"
        silver_commit = delta_core.get_delta_last_commit(silver_container, silver_path)
        gold_commit = delta_core.get_delta_last_commit(gold_container, gold_path)
        prior = watermarks.get(watermark_key, {})
        postgres_sync_current = (
            bucket_sync_is_current(sync_state, bucket=bucket, source_commit=silver_commit) if postgres_dsn else True
        )
        _log_bucket_progress(
            bucket=bucket,
            stage="bucket_start",
            silver_path=silver_path,
            gold_path=gold_path,
            silver_commit_present=silver_commit is not None,
            gold_commit_present=gold_commit is not None,
        )

        # Skip stable buckets to reduce compute/write overhead on no-change runs.
        if (
            silver_commit is not None
            and prior.get("silver_last_commit") is not None
            and prior.get("silver_last_commit") >= silver_commit
            and gold_commit is not None
            and postgres_sync_current
        ):
            skipped_unchanged += 1
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="skipped_unchanged",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )
            _log_bucket_progress(
                bucket=bucket,
                stage="skipped_unchanged",
                silver_commit_present=True,
                gold_commit_present=True,
            )
            continue

        prior_bucket_symbols = sorted(
            symbol for symbol, current_bucket in symbol_to_bucket.items() if current_bucket == bucket
        )
        scope_symbols = sorted(set(prior_bucket_symbols))
        stage_result: Optional[BucketStageResult] = None
        bucket_input_symbols = 0
        bucket_symbol_failures = 0
        bucket_output_rows = 0
        bucket_symbol_to_bucket: dict[str, str] = {}
        write_rows = 0
        write_columns = 0
        write_memory_mb = 0.0

        # Missing source still writes an empty table to keep state deterministic.
        if silver_commit is None:
            skipped_missing_source += 1
            _log_bucket_progress(
                bucket=bucket,
                stage="missing_source",
                silver_path=silver_path,
                gold_path=gold_path,
            )
            final_frame = project_gold_output_frame(pd.DataFrame(columns=["date", "symbol"]), domain="market")
            stage_result = BucketStageResult(
                final_frame=final_frame,
                staging_used=False,
                staging_root=_gold_market_staging_root(run_id=run_id, bucket=bucket),
                staging_delta_path=_gold_market_staging_delta_path(run_id=run_id, bucket=bucket),
                staging_chunk_prefix=_gold_market_staging_chunk_prefix(run_id=run_id, bucket=bucket),
                final_rows=int(len(final_frame)),
                final_columns=int(len(final_frame.columns)),
                final_memory_mb=_frame_memory_mb(final_frame),
                bucket_input_symbols=0,
                bucket_output_rows=0,
                bucket_symbol_failures=0,
                bucket_symbol_to_bucket={},
                critical_compute_failure_symbol=None,
                chunk_summaries=[],
            )
        else:
            try:
                stage_result = _stage_market_bucket_outputs(
                    bucket=bucket,
                    silver_container=silver_container,
                    gold_container=gold_container,
                    silver_path=silver_path,
                    backfill_start=backfill_start,
                    run_id=run_id,
                )
            except RuntimeError as exc:
                message = str(exc)
                if message.startswith("contract_validation::"):
                    _, raw_symbol_count, detail = message.split("::", 2)
                    bucket_input_symbols = int(raw_symbol_count or 0)
                    failed += 1
                    failed_buckets += 1
                    mdc.write_error(detail)
                    mdc.write_line(
                        f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                        f"reason=contract_validation symbols_in={bucket_input_symbols} symbols_out=0 "
                        f"failures={max(bucket_input_symbols, 1)}"
                    )
                    mdc.write_line(
                        f"watermark_update_status layer=gold domain=market bucket={bucket} "
                        "status=blocked reason=contract_validation"
                    )
                    bucket_results.append(
                        BucketExecutionResult(
                            bucket=bucket,
                            status="failed_contract",
                            symbols_written=0,
                            watermark_updated=False,
                        )
                    )
                    continue
                failed += 1
                failed_buckets += 1
                mdc.write_error(f"Gold market alpha26 write failed bucket={bucket}: {exc}")
                mdc.write_line(
                    f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                    "reason=write_failure symbols_in=0 symbols_out=0 failures=1"
                )
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} status=blocked reason=write_failure"
                )
                bucket_results.append(
                    BucketExecutionResult(
                        bucket=bucket,
                        status="failed_write",
                        symbols_written=0,
                        watermark_updated=False,
                    )
                )
                try:
                    _cleanup_staged_market_bucket(
                        gold_container=gold_container,
                        staging_root=_gold_market_staging_root(run_id=run_id, bucket=bucket),
                    )
                except Exception as cleanup_exc:
                    mdc.write_warning(f"Gold market staging cleanup failed bucket={bucket}: {cleanup_exc}")
                continue
            except Exception as exc:
                failed += 1
                failed_buckets += 1
                mdc.write_error(f"Gold market alpha26 write failed bucket={bucket}: {exc}")
                mdc.write_line(
                    f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                    "reason=write_failure symbols_in=0 symbols_out=0 failures=1"
                )
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} status=blocked reason=write_failure"
                )
                bucket_results.append(
                    BucketExecutionResult(
                        bucket=bucket,
                        status="failed_write",
                        symbols_written=0,
                        watermark_updated=False,
                    )
                )
                try:
                    _cleanup_staged_market_bucket(
                        gold_container=gold_container,
                        staging_root=_gold_market_staging_root(run_id=run_id, bucket=bucket),
                    )
                except Exception as cleanup_exc:
                    mdc.write_warning(f"Gold market staging cleanup failed bucket={bucket}: {cleanup_exc}")
                continue

        bucket_input_symbols = stage_result.bucket_input_symbols
        bucket_symbol_failures = stage_result.bucket_symbol_failures
        bucket_output_rows = stage_result.bucket_output_rows
        bucket_symbol_to_bucket = stage_result.bucket_symbol_to_bucket
        scope_symbols = sorted(set(scope_symbols).union(bucket_symbol_to_bucket.keys()))
        bucket_scope_symbols = (
            sorted(symbol for symbol in scope_symbols if symbol in scoped_symbols)
            if scoped_symbols
            else scope_symbols
        )
        failed += bucket_symbol_failures
        failed_symbols += bucket_symbol_failures

        critical_compute_failure_symbol = stage_result.critical_compute_failure_symbol
        if critical_compute_failure_symbol is not None:
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                f"reason=compute_failure symbols_in={bucket_input_symbols} symbols_out=0 "
                f"failures={bucket_symbol_failures} critical_symbol=true symbol={critical_compute_failure_symbol}"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=market bucket={bucket} "
                f"status=blocked reason=compute_failure critical_symbol=true symbol={critical_compute_failure_symbol}"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="failed_compute",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )
            if silver_commit is not None:
                try:
                    _cleanup_staged_market_bucket(
                        gold_container=gold_container,
                        staging_root=_gold_market_staging_root(run_id=run_id, bucket=bucket),
                    )
                except Exception as exc:
                    mdc.write_warning(f"Gold market staging cleanup failed bucket={bucket}: {exc}")
            continue

        # Persist bucket output and checkpoint after successful write/sync.
        try:
            if stage_result.staging_used:
                if scoped_symbols:
                    staged_frames = list(
                        _iter_staged_market_chunk_frames(
                            gold_container=gold_container,
                            chunk_prefix=stage_result.staging_chunk_prefix,
                        )
                    )
                    staged_frame = pd.concat(staged_frames, ignore_index=True) if staged_frames else pd.DataFrame()
                    staged_frame = _merge_preserved_gold_bucket_rows(
                        bucket=bucket,
                        gold_container=gold_container,
                        scoped_symbols=scoped_symbols,
                        new_frame=staged_frame,
                    )
                    write_decision = prepare_delta_write_frame(
                        staged_frame,
                        container=gold_container,
                        path=gold_path,
                    )
                    write_rows = int(len(write_decision.frame))
                    write_columns = int(len(write_decision.frame.columns))
                    write_memory_mb = _frame_memory_mb(write_decision.frame)
                    _log_bucket_progress(
                        bucket=bucket,
                        stage="write_ready",
                        rows=write_rows,
                        columns=write_columns,
                        memory_mb=write_memory_mb,
                        output_symbols=len(bucket_symbol_to_bucket),
                    )
                    mdc.write_line(
                        "delta_write_decision layer=gold domain=market "
                        f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
                        f"reason={write_decision.reason} path={gold_path}"
                    )
                    if write_decision.action == "skip_empty_no_schema":
                        mdc.write_line(f"Skipping Gold market empty bucket write for {gold_path}: no existing Delta schema.")
                        mdc.write_line(
                            f"layer_handoff_status transition=silver_to_gold status=skipped bucket={bucket} "
                            "reason=empty_bucket_no_existing_schema symbols_in=0 symbols_out=0 failures=0"
                        )
                        mdc.write_line(
                            f"watermark_update_status layer=gold domain=market bucket={bucket} "
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
                        _log_bucket_progress(
                            bucket=bucket,
                            stage="skipped_empty_no_schema",
                            rows=write_rows,
                            output_symbols=0,
                        )
                        continue

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
                            domain="market",
                            bucket=bucket,
                            df=write_decision.frame,
                            date_column="date",
                            job_name="gold-market-job",
                            job_run_id=run_id,
                            run_id=run_id,
                            data_path=gold_path,
                            source_commit=silver_commit,
                        )
                    except Exception as exc:
                        mdc.write_warning(f"Gold market metadata bucket artifact write failed bucket={bucket}: {exc}")
                    if postgres_dsn:
                        sync_result = sync_gold_bucket(
                            domain="market",
                            bucket=bucket,
                            frame=write_decision.frame,
                            scope_symbols=bucket_scope_symbols,
                            source_commit=silver_commit,
                            dsn=postgres_dsn,
                        )
                        sync_state[bucket] = sync_state_cache_entry(sync_result)
                        mdc.write_line(
                            "postgres_gold_sync_status "
                            f"domain=market bucket={bucket} status={sync_result.status} "
                            f"rows_out={sync_result.row_count} symbols_out={sync_result.symbol_count} "
                            f"scope_symbols={sync_result.scope_symbol_count} source_commit={silver_commit}"
                        )
                else:
                    write_rows = stage_result.final_rows
                    write_columns = stage_result.final_columns
                    write_memory_mb = stage_result.final_memory_mb
                    _log_bucket_progress(
                        bucket=bucket,
                        stage="write_ready",
                        rows=write_rows,
                        columns=write_columns,
                        memory_mb=write_memory_mb,
                        output_symbols=len(bucket_symbol_to_bucket),
                    )
                    mdc.write_line(
                        "delta_write_decision layer=gold domain=market "
                        f"bucket={bucket} action=write reason=chunked_staged_publish path={gold_path}"
                    )
                    _promote_staged_market_bucket(
                        gold_container=gold_container,
                        staging_delta_path=stage_result.staging_delta_path,
                        gold_path=gold_path,
                    )
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
                        _write_gold_market_bucket_artifact_from_summaries(
                            gold_container=gold_container,
                            bucket=bucket,
                            summaries=stage_result.chunk_summaries,
                            symbol_count=len(bucket_symbol_to_bucket),
                            job_run_id=run_id,
                            data_path=gold_path,
                        )
                    except Exception as exc:
                        mdc.write_warning(f"Gold market metadata bucket artifact write failed bucket={bucket}: {exc}")
                    if postgres_dsn:
                        sync_result = sync_gold_bucket_chunks(
                            domain="market",
                            bucket=bucket,
                            frames=lambda: _iter_staged_market_chunk_frames(
                                gold_container=gold_container,
                                chunk_prefix=stage_result.staging_chunk_prefix,
                            ),
                            scope_symbols=bucket_scope_symbols,
                            source_commit=silver_commit,
                            dsn=postgres_dsn,
                        )
                        sync_state[bucket] = sync_state_cache_entry(sync_result)
                        mdc.write_line(
                            "postgres_gold_sync_status "
                            f"domain=market bucket={bucket} status={sync_result.status} "
                            f"rows_out={sync_result.row_count} symbols_out={sync_result.symbol_count} "
                            f"scope_symbols={sync_result.scope_symbol_count} source_commit={silver_commit}"
                        )
            else:
                write_frame = _merge_preserved_gold_bucket_rows(
                    bucket=bucket,
                    gold_container=gold_container,
                    scoped_symbols=scoped_symbols,
                    new_frame=stage_result.final_frame,
                )
                write_decision = prepare_delta_write_frame(
                    write_frame,
                    container=gold_container,
                    path=gold_path,
                )
                write_rows = int(len(write_decision.frame))
                write_columns = int(len(write_decision.frame.columns))
                write_memory_mb = _frame_memory_mb(write_decision.frame)
                _log_bucket_progress(
                    bucket=bucket,
                    stage="write_ready",
                    rows=write_rows,
                    columns=write_columns,
                    memory_mb=write_memory_mb,
                    output_symbols=len(bucket_symbol_to_bucket),
                )
                mdc.write_line(
                    "delta_write_decision layer=gold domain=market "
                    f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
                    f"reason={write_decision.reason} path={gold_path}"
                )
                if write_decision.action == "skip_empty_no_schema":
                    mdc.write_line(f"Skipping Gold market empty bucket write for {gold_path}: no existing Delta schema.")
                    mdc.write_line(
                        f"layer_handoff_status transition=silver_to_gold status=skipped bucket={bucket} "
                        "reason=empty_bucket_no_existing_schema symbols_in=0 symbols_out=0 failures=0"
                    )
                    mdc.write_line(
                        f"watermark_update_status layer=gold domain=market bucket={bucket} "
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
                    _log_bucket_progress(
                        bucket=bucket,
                        stage="skipped_empty_no_schema",
                        rows=write_rows,
                        output_symbols=0,
                    )
                    continue

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
                        domain="market",
                        bucket=bucket,
                        df=write_decision.frame,
                        date_column="date",
                        job_name="gold-market-job",
                        job_run_id=run_id,
                        run_id=run_id,
                        data_path=gold_path,
                        source_commit=silver_commit,
                    )
                except Exception as exc:
                    mdc.write_warning(f"Gold market metadata bucket artifact write failed bucket={bucket}: {exc}")
                if postgres_dsn:
                    sync_result = sync_gold_bucket(
                        domain="market",
                        bucket=bucket,
                        frame=write_decision.frame,
                        scope_symbols=bucket_scope_symbols,
                        source_commit=silver_commit,
                        dsn=postgres_dsn,
                    )
                    sync_state[bucket] = sync_state_cache_entry(sync_result)
                    mdc.write_line(
                        "postgres_gold_sync_status "
                        f"domain=market bucket={bucket} status={sync_result.status} "
                        f"rows_out={sync_result.row_count} symbols_out={sync_result.symbol_count} "
                        f"scope_symbols={sync_result.scope_symbol_count} source_commit={silver_commit}"
                    )

            watermark_updated = False
            updated_symbol_to_bucket = layer_bucketing.merge_symbol_to_bucket_map(
                symbol_to_bucket,
                touched_buckets={bucket},
                touched_symbol_to_bucket=bucket_symbol_to_bucket,
            )
            if silver_commit is not None and bucket_symbol_failures == 0:
                try:
                    symbol_to_bucket, index_path = _persist_gold_market_bucket_checkpoint(
                        bucket=bucket,
                        watermark_key=watermark_key,
                        silver_commit=silver_commit,
                        watermarks=watermarks,
                        symbol_to_bucket=symbol_to_bucket,
                        bucket_symbol_to_bucket=bucket_symbol_to_bucket,
                        run_id=run_id,
                    )
                except Exception as exc:
                    failed += 1
                    failed_buckets += 1
                    mdc.write_error(f"Gold market alpha26 checkpoint failed bucket={bucket}: {exc}")
                    mdc.write_line(
                        f"watermark_update_status layer=gold domain=market bucket={bucket} "
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
                    _log_bucket_progress(
                        bucket=bucket,
                        stage="checkpoint_failed",
                        output_symbols=len(bucket_symbol_to_bucket),
                        output_rows=bucket_output_rows,
                        failed_symbols=bucket_symbol_failures,
                    )
                    continue
                watermarks_dirty = True
                watermark_updated = True
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} status=updated reason=success"
                )
            elif silver_commit is not None:
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} "
                    "status=blocked reason=symbol_compute_failures"
                )
            else:
                symbol_to_bucket = updated_symbol_to_bucket
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} "
                    "status=blocked reason=missing_source_commit"
                )

            processed += 1
            symbols_written = len(bucket_symbol_to_bucket)
            bucket_status = "ok" if bucket_symbol_failures == 0 else "ok_with_failures"
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status={bucket_status} bucket={bucket} "
                f"symbols_in={symbols_written + bucket_symbol_failures} symbols_out={symbols_written} "
                f"failures={bucket_symbol_failures}"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status=bucket_status,
                    symbols_written=symbols_written,
                    watermark_updated=watermark_updated,
                )
            )
            _log_bucket_progress(
                bucket=bucket,
                stage="write_completed",
                rows=write_rows,
                symbols=symbols_written,
                columns=write_columns,
                memory_mb=write_memory_mb,
                output_rows=bucket_output_rows,
                failed_symbols=bucket_symbol_failures,
            )
        except Exception as exc:
            if _is_retry_pending_postgres_sync_failure(exc):
                retry_pending_buckets += 1
                failure_stage = _postgres_sync_failure_field(exc, "failure_stage")
                failure_category = _postgres_sync_failure_field(exc, "failure_category")
                failure_error_class = str(getattr(exc, "failure_error_class", "") or type(exc).__name__).strip()
                mdc.write_warning(
                    "Gold market alpha26 write retry pending "
                    f"bucket={bucket} failure_stage={failure_stage} "
                    f"failure_category={failure_category} error_class={failure_error_class}: {exc}"
                )
                mdc.write_line(
                    f"layer_handoff_status transition=silver_to_gold status=retry_pending bucket={bucket} "
                    f"reason=transient_write_failure symbols_in={len(bucket_symbol_to_bucket) + bucket_symbol_failures} "
                    f"symbols_out=0 failures={bucket_symbol_failures} failure_stage={failure_stage} "
                    f"failure_category={failure_category}"
                )
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} "
                    "status=blocked reason=write_retry_pending"
                )
                bucket_results.append(
                    BucketExecutionResult(
                        bucket=bucket,
                        status="retry_pending",
                        symbols_written=0,
                        watermark_updated=False,
                    )
                )
                _log_bucket_progress(
                    bucket=bucket,
                    stage="write_retry_pending",
                    output_symbols=len(bucket_symbol_to_bucket),
                    output_rows=bucket_output_rows,
                    failed_symbols=bucket_symbol_failures,
                )
            else:
                failed += 1
                failed_buckets += 1
                mdc.write_error(f"Gold market alpha26 write failed bucket={bucket}: {exc}")
                mdc.write_line(
                    f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                    f"reason=write_failure symbols_in={len(bucket_symbol_to_bucket) + bucket_symbol_failures} "
                    f"symbols_out=0 failures={bucket_symbol_failures + 1}"
                )
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} status=blocked reason=write_failure"
                )
                bucket_results.append(
                    BucketExecutionResult(
                        bucket=bucket,
                        status="failed_write",
                        symbols_written=0,
                        watermark_updated=False,
                    )
                )
                _log_bucket_progress(
                    bucket=bucket,
                    stage="write_failed",
                    output_symbols=len(bucket_symbol_to_bucket),
                    output_rows=bucket_output_rows,
                    failed_symbols=bucket_symbol_failures,
                )
        finally:
            if silver_commit is not None:
                try:
                    _cleanup_staged_market_bucket(
                        gold_container=gold_container,
                        staging_root=_gold_market_staging_root(run_id=run_id, bucket=bucket),
                    )
                except Exception as exc:
                    mdc.write_warning(f"Gold market staging cleanup failed bucket={bucket}: {exc}")

    status_counts: dict[str, int] = {}
    for result in bucket_results:
        status_counts[result.status] = int(status_counts.get(result.status, 0)) + 1
    publication_reason: Optional[str] = None
    if failed == 0 and retry_pending_buckets == 0 and postgres_dsn:
        try:
            _verify_postgres_critical_market_symbols(dsn=postgres_dsn, sync_state=sync_state)
        except Exception as exc:
            failed += 1
            failed_finalization += 1
            publication_reason = "critical_symbol_verification_failed"
            mdc.write_error(str(exc))

    finalization = gold_checkpoint_publication.finalize_gold_publication(
        domain="market",
        symbol_to_bucket=symbol_to_bucket,
        date_column="date",
        job_name="gold-market-job",
        processed=processed,
        skipped_unchanged=skipped_unchanged,
        skipped_missing_source=skipped_missing_source,
        failed_symbols=failed_symbols,
        failed_buckets=failed_buckets,
        failed_finalization=failed_finalization,
        deferred_buckets=retry_pending_buckets,
        publication_reason=publication_reason,
        index_path=index_path,
        job_run_id=run_id,
        run_id=run_id,
        source_commit=silver_commit,
    )
    mdc.write_line(
        "layer_handoff_status transition=silver_to_gold status=complete "
        f"bucket_statuses={status_counts} failed={finalization.failed} "
        f"failed_symbols={finalization.failed_symbols} failed_buckets={finalization.failed_buckets} "
        f"failed_finalization={finalization.failed_finalization} "
        f"deferred_buckets={finalization.deferred_buckets}"
    )

    return GoldMarketRunResult(
        processed=processed,
        skipped_unchanged=skipped_unchanged,
        skipped_missing_source=skipped_missing_source,
        failed=finalization.failed,
        watermarks_dirty=watermarks_dirty,
        alpha26_symbols=len(symbol_to_bucket),
        index_path=finalization.index_path,
        retry_pending_buckets=finalization.deferred_buckets,
    )


def main() -> int:
    """Run the gold market feature engineering pipeline and return process exit code."""

    from asset_allocation_runtime_common.market_data import core as mdc
    # Emit environment diagnostics to simplify operations troubleshooting.
    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying historical cutoff to gold market features: {backfill_start_iso}")

    # Ensure layout mode is resolved before writing outputs.
    layer_bucketing.gold_layout_mode()

    # Watermarks make bucket processing incremental and idempotent.
    watermarks = load_watermarks("gold_market_features")
    run_result = _run_alpha26_market_gold(
        silver_container=job_cfg.silver_container,
        gold_container=job_cfg.gold_container,
        backfill_start_iso=backfill_start_iso,
        watermarks=watermarks,
    )
    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        alpha26_index_path,
    ) = run_result
    retry_pending_buckets = int(getattr(run_result, "retry_pending_buckets", 0) or 0)

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    if failed == 0 and retry_pending_buckets == 0:
        try:
            reconciliation_orphans, reconciliation_deleted_blobs = _run_market_reconciliation(
                silver_container=job_cfg.silver_container,
                gold_container=job_cfg.gold_container,
            )
        except Exception as exc:
            reconciliation_failed = 1
            mdc.write_error(f"Gold market reconciliation failed: {exc}")
            mdc.write_line(
                "reconciliation_result layer=gold domain=market "
                "status=failed orphan_count=unknown deleted_blobs=unknown cutoff_rows_dropped=unknown"
            )
    elif retry_pending_buckets > 0:
        mdc.write_warning(
            f"Skipping gold market reconciliation: retry_pending_buckets={retry_pending_buckets}"
        )

    if watermarks_dirty and reconciliation_failed == 0:
        save_watermarks("gold_market_features", watermarks)

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Gold market alpha26 complete: "
        f"processed_buckets={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_missing_source={skipped_missing_source} symbols={alpha26_symbols} "
        f"index_path={alpha26_index_path or 'unavailable'} reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} failed={total_failed} "
        f"retry_pending_buckets={retry_pending_buckets}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    from asset_allocation_runtime_common.market_data import core as mdc
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-market-job"

    with mdc.JobLock(job_name, conflict_policy="wait_then_fail", wait_timeout_seconds=90):
        # Ensure the API dependency is awake before running the batch job.
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="gold", domain="market", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
