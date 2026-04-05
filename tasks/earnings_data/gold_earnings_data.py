import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd
from core import core as mdc
from tasks.common.delta_write_policy import prepare_delta_write_frame
from tasks.common.gold_output_contracts import project_gold_output_frame

from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from core import domain_artifacts
from tasks.common import gold_checkpoint_publication
from core import layer_bucketing
from tasks.common.market_reconciliation import (
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)
from core.gold_sync_contracts import (
    bucket_sync_is_current,
    load_domain_sync_state,
    resolve_postgres_dsn,
    sync_gold_bucket,
    sync_state_cache_entry,
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


def _load_existing_gold_earnings_symbol_to_bucket_map() -> dict[str, str]:
    out: dict[str, str] = {}
    existing = layer_bucketing.load_layer_symbol_index(layer="gold", domain="earnings")
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


def _failure_log_value(value: object) -> str:
    text = " ".join(str(value or "").split())
    return text.replace('"', "'").replace(" ", "_") or "n/a"


def _log_failure_counter(
    *,
    stage: str,
    failed: int,
    failed_symbols: int,
    failed_buckets: int,
    failed_finalization: int,
    bucket: Optional[str] = None,
    ticker: Optional[str] = None,
    exc: Optional[BaseException] = None,
) -> None:
    error_class = getattr(exc, "failure_error_class", None) or (
        type(exc).__name__ if exc is not None else "none"
    )
    failure_source = getattr(exc, "failure_stage", None) or stage
    failure_category = getattr(exc, "failure_category", None) or "unclassified"
    transient_value = bool(getattr(exc, "failure_transient", False))
    mdc.write_line(
        "gold_earnings_failure_counter "
        f"stage={_failure_log_value(stage)} failure_source={_failure_log_value(failure_source)} "
        f"failure_category={_failure_log_value(failure_category)} "
        f"bucket={_failure_log_value(bucket or 'n/a')} ticker={_failure_log_value(ticker or 'n/a')} "
        f"exception_type={_failure_log_value(error_class)} transient={str(transient_value).lower()} "
        f"counter_value={int(failed)} failed_symbols={int(failed_symbols)} "
        f"failed_buckets={int(failed_buckets)} failed_finalization={int(failed_finalization)}"
    )


def _coerce_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        value = pd.to_datetime(series, errors="coerce", utc=True)
    else:
        numeric = pd.to_numeric(series, errors="coerce")
        numeric_dates = pd.to_datetime(numeric, errors="coerce", unit="ms", utc=True)
        value = pd.to_datetime(series, errors="coerce", utc=True)
        value = value.where(numeric.isna(), numeric_dates)
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.where(denominator != 0).divide(denominator.where(denominator != 0))


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

    seen: dict[str, int] = {}
    unique: list[str] = []
    for name in names:
        count = seen.get(name, 0) + 1
        seen[name] = count
        unique.append(name if count == 1 else f"{name}_{count}")

    out.columns = unique
    return out


def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


def _canonicalize_earnings_events(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "report_date",
                "fiscal_date_ending",
                "reported_eps",
                "eps_estimate",
                "surprise",
                "record_type",
                "is_future_event",
                "calendar_time_of_day",
                "calendar_currency",
            ]
        )

    out = _snake_case_columns(df)
    out = out.drop(columns=["source_hash", "ingested_at"], errors="ignore")

    if "symbol" not in out.columns:
        out["symbol"] = pd.NA
    out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()

    if "date" not in out.columns and "report_date" in out.columns:
        out["date"] = out["report_date"]
    for column in ("date", "report_date", "fiscal_date_ending"):
        if column in out.columns:
            out[column] = _coerce_datetime(out[column])
        else:
            out[column] = pd.NaT

    if "record_type" not in out.columns:
        out["record_type"] = "actual"
    out["record_type"] = out["record_type"].astype("string").str.strip().str.lower()
    out.loc[~out["record_type"].isin({"actual", "scheduled"}), "record_type"] = "actual"
    out.loc[out["record_type"].isna() | (out["record_type"] == ""), "record_type"] = "actual"

    actual_missing_fiscal = out["record_type"].eq("actual") & out["fiscal_date_ending"].isna()
    out.loc[actual_missing_fiscal, "fiscal_date_ending"] = out.loc[actual_missing_fiscal, "date"]
    scheduled_missing_date = out["record_type"].eq("scheduled") & out["date"].isna() & out["report_date"].notna()
    out.loc[scheduled_missing_date, "date"] = out.loc[scheduled_missing_date, "report_date"]

    for column in ("reported_eps", "eps_estimate", "surprise"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
        else:
            out[column] = np.nan

    if "is_future_event" in out.columns:
        parsed_future = pd.Series(pd.to_numeric(out["is_future_event"], errors="coerce"), index=out.index, dtype="Float64")
    else:
        parsed_future = pd.Series(pd.NA, index=out.index, dtype="Float64")
    inferred_future = pd.Series(
        out["record_type"].eq("scheduled") & out["report_date"].notna() & (out["report_date"] >= _utc_today()),
        index=out.index,
        dtype="boolean",
    ).astype("Float64")
    out["is_future_event"] = parsed_future.fillna(inferred_future).fillna(0).astype(int)

    if "calendar_time_of_day" not in out.columns:
        out["calendar_time_of_day"] = pd.NA
    if "calendar_currency" not in out.columns:
        out["calendar_currency"] = pd.NA

    out = out.dropna(subset=["date", "symbol"]).copy()
    return out[
        [
            "date",
            "symbol",
            "report_date",
            "fiscal_date_ending",
            "reported_eps",
            "eps_estimate",
            "surprise",
            "record_type",
            "is_future_event",
            "calendar_time_of_day",
            "calendar_currency",
        ]
    ].reset_index(drop=True)


def _resample_daily_ffill(
    df: pd.DataFrame,
    date_col: str,
    *,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if date_col not in df.columns:
        return df

    out = df.copy()
    out[date_col] = _coerce_datetime(out[date_col])
    out = out.dropna(subset=[date_col]).copy()
    if out.empty:
        return out

    out = out.sort_values(date_col).copy()
    out = out.drop_duplicates(subset=[date_col], keep="last").copy()

    out = out.set_index(date_col)
    range_start = pd.to_datetime(start_date).normalize() if start_date is not None else out.index.min()
    range_end = pd.to_datetime(end_date).normalize() if end_date is not None else out.index.max()
    if range_end < range_start:
        range_end = range_start
    full_range = pd.date_range(start=range_start, end=range_end, freq="D")
    out = out.reindex(full_range)
    out = out.ffill()
    out = out.reset_index().rename(columns={"index": date_col})
    return out


def _compute_actual_feature_frame(actual_rows: pd.DataFrame, *, extend_to: Optional[pd.Timestamp]) -> pd.DataFrame:
    out = actual_rows.copy()
    required = {"date", "symbol", "reported_eps", "eps_estimate"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["date"] = _coerce_datetime(out["date"])
    out["reported_eps"] = pd.to_numeric(out["reported_eps"], errors="coerce")
    out["eps_estimate"] = pd.to_numeric(out["eps_estimate"], errors="coerce")
    out["surprise"] = pd.to_numeric(out.get("surprise"), errors="coerce")
    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    estimate_abs = out["eps_estimate"].abs()
    out["surprise_pct"] = _safe_div(out["reported_eps"] - out["eps_estimate"], estimate_abs)

    grouped_surprise = out.groupby("symbol", sort=False)["surprise_pct"]
    out["surprise_mean_4q"] = grouped_surprise.transform(
        lambda series: series.rolling(window=4, min_periods=4).mean()
    )
    out["surprise_std_8q"] = grouped_surprise.transform(
        lambda series: series.rolling(window=8, min_periods=8).std()
    )

    beat = (out["surprise_pct"] > 0).astype(float)
    beat = beat.where(out["surprise_pct"].notna())
    out["beat_rate_8q"] = beat.groupby(out["symbol"], sort=False).transform(
        lambda series: series.rolling(window=8, min_periods=8).mean()
    )

    out["is_earnings_day"] = 1
    out["last_earnings_date"] = out["date"]

    out = _resample_daily_ffill(out, "date", end_date=extend_to)
    out["is_earnings_day"] = (out["date"] == out["last_earnings_date"]).astype(int)
    out["days_since_earnings"] = pd.array((out["date"] - out["last_earnings_date"]).dt.days, dtype="Int64")
    return out


def _build_scheduled_only_frame(
    *,
    symbol: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    full_range = pd.date_range(start=start_date.normalize(), end=end_date.normalize(), freq="D")
    out = pd.DataFrame({"date": full_range})
    out["symbol"] = str(symbol or "").strip().upper()
    for column in (
        "reported_eps",
        "eps_estimate",
        "surprise",
        "surprise_pct",
        "surprise_mean_4q",
        "surprise_std_8q",
        "beat_rate_8q",
    ):
        out[column] = np.nan
    out["is_earnings_day"] = 0
    out["last_earnings_date"] = pd.NaT
    out["days_since_earnings"] = pd.array([pd.NA] * len(out), dtype="Int64")
    return out


def _attach_upcoming_earnings_fields(frame: pd.DataFrame, scheduled_rows: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["next_earnings_date"] = pd.NaT
    out["days_until_next_earnings"] = pd.array([pd.NA] * len(out), dtype="Int64")
    out["next_earnings_estimate"] = np.nan
    out["next_earnings_time_of_day"] = pd.NA
    out["next_earnings_fiscal_date_ending"] = pd.NaT
    out["has_upcoming_earnings"] = 0
    out["is_scheduled_earnings_day"] = 0

    if scheduled_rows is None or scheduled_rows.empty or out.empty:
        return out

    scheduled = scheduled_rows.copy()
    scheduled["report_date"] = _coerce_datetime(scheduled["report_date"])
    scheduled["fiscal_date_ending"] = _coerce_datetime(scheduled["fiscal_date_ending"])
    scheduled["eps_estimate"] = pd.to_numeric(scheduled["eps_estimate"], errors="coerce")
    scheduled = scheduled.dropna(subset=["report_date"]).sort_values(["report_date", "date"]).copy()
    scheduled = scheduled.drop_duplicates(subset=["report_date"], keep="last").reset_index(drop=True)
    if scheduled.empty:
        return out

    report_dates = scheduled["report_date"].to_numpy(dtype="datetime64[ns]")
    row_dates = pd.to_datetime(out["date"]).to_numpy(dtype="datetime64[ns]")
    positions = report_dates.searchsorted(row_dates, side="left")
    valid_mask = positions < len(scheduled)
    if valid_mask.any():
        valid_rows = np.flatnonzero(valid_mask)
        valid_positions = positions[valid_mask]
        next_rows = scheduled.iloc[valid_positions]
        out.loc[valid_rows, "next_earnings_date"] = next_rows["report_date"].to_numpy()
        out.loc[valid_rows, "next_earnings_estimate"] = next_rows["eps_estimate"].to_numpy()
        out.loc[valid_rows, "next_earnings_time_of_day"] = next_rows["calendar_time_of_day"].to_numpy()
        out.loc[valid_rows, "next_earnings_fiscal_date_ending"] = next_rows["fiscal_date_ending"].to_numpy()
        out.loc[valid_rows, "has_upcoming_earnings"] = 1

    day_delta = (pd.to_datetime(out["next_earnings_date"], errors="coerce") - pd.to_datetime(out["date"])).dt.days
    out["days_until_next_earnings"] = pd.array(day_delta, dtype="Int64")

    scheduled_days = set(pd.to_datetime(scheduled["report_date"]).dt.normalize())
    out["is_scheduled_earnings_day"] = pd.to_datetime(out["date"]).dt.normalize().isin(scheduled_days).astype(int)
    return out


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    normalized_input = _snake_case_columns(df)
    input_columns = set(normalized_input.columns)
    base_missing = {"date", "symbol"}.difference(input_columns)
    if base_missing:
        raise ValueError(f"Missing required columns: {sorted(base_missing)}")

    canonical = _canonicalize_earnings_events(df)

    if canonical.empty:
        missing_actual = {"reported_eps", "eps_estimate"}.difference(input_columns)
        if missing_actual:
            raise ValueError(f"Missing required columns: {sorted(missing_actual)}")
        return pd.DataFrame()

    if canonical["record_type"].eq("actual").any():
        missing_actual = {"reported_eps", "eps_estimate"}.difference(input_columns)
        if missing_actual:
            raise ValueError(f"Missing required columns: {sorted(missing_actual)}")

    frames: list[pd.DataFrame] = []
    today = _utc_today()
    for symbol, group in canonical.groupby("symbol", sort=False):
        actual_rows = group.loc[group["record_type"] == "actual"].copy()
        scheduled_rows = group.loc[group["record_type"] == "scheduled"].copy()
        scheduled_rows = scheduled_rows.loc[
            scheduled_rows["report_date"].notna() & (scheduled_rows["report_date"] >= today)
        ].copy()

        latest_scheduled_date = (
            pd.to_datetime(scheduled_rows["report_date"]).max() if not scheduled_rows.empty else None
        )
        if actual_rows.empty:
            if scheduled_rows.empty:
                continue
            feature_frame = _build_scheduled_only_frame(
                symbol=str(symbol),
                start_date=today,
                end_date=latest_scheduled_date,
            )
        else:
            extend_to = latest_scheduled_date if latest_scheduled_date is not None else actual_rows["date"].max()
            if extend_to is not None:
                extend_to = max(pd.Timestamp(extend_to).normalize(), actual_rows["date"].max().normalize())
            feature_frame = _compute_actual_feature_frame(actual_rows, extend_to=extend_to)

        feature_frame = _attach_upcoming_earnings_fields(feature_frame, scheduled_rows)
        frames.append(feature_frame)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
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


def _run_earnings_reconciliation(*, silver_container: str, gold_container: str) -> tuple[int, int]:
    from core import core as mdc
    from core import delta_core
    from core.pipeline import DataPaths

    silver_client = mdc.get_storage_client(silver_container)
    gold_client = mdc.get_storage_client(gold_container)
    if silver_client is None:
        raise RuntimeError("Gold earnings reconciliation requires silver storage client.")
    if gold_client is None:
        raise RuntimeError("Gold earnings reconciliation requires gold storage client.")

    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="earnings-data")
    gold_symbols = collect_delta_market_symbols(client=gold_client, root_prefix="earnings")
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_gold_earnings_bucket_path(layer_bucketing.bucket_letter(symbol))
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
            "Gold earnings reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Gold earnings reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Gold earnings orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=layer_bucketing.all_gold_bucket_paths(domain="earnings"),
        load_table=lambda path: delta_core.load_delta(gold_container, path),
        store_table=lambda df, path: delta_core.store_delta(df, gold_container, path, mode="overwrite"),
        delete_prefix=gold_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="gold earnings reconciliation cutoff",
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
            "Gold earnings reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(f"Gold earnings reconciliation cutoff sweep encountered errors={cutoff_stats.errors}.")
    status = "failed" if cutoff_stats.errors > 0 else "ok"
    mdc.write_line(
        "reconciliation_result layer=gold domain=earnings "
        f"status={status} orphan_count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
        f"cutoff_rows_dropped={cutoff_stats.rows_dropped} cutoff_tables_rewritten={cutoff_stats.tables_rewritten} "
        f"cutoff_errors={cutoff_stats.errors}"
    )
    return len(orphan_symbols), deleted_blobs


def _run_alpha26_earnings_gold(
    *,
    silver_container: str,
    gold_container: str,
    backfill_start_iso: Optional[str],
    watermarks: dict,
) -> tuple[int, int, int, int, bool, int, Optional[str]]:
    from core import core as mdc
    from core.pipeline import DataPaths
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
    symbol_to_bucket = _load_existing_gold_earnings_symbol_to_bucket_map()
    postgres_dsn = resolve_postgres_dsn()
    sync_state = load_domain_sync_state(postgres_dsn, domain="earnings") if postgres_dsn else {}
    bucket_results: list[BucketExecutionResult] = []
    index_path: Optional[str] = None

    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        silver_path = DataPaths.get_silver_earnings_bucket_path(bucket)
        gold_path = DataPaths.get_gold_earnings_bucket_path(bucket)
        watermark_key = f"bucket::{bucket}"
        silver_commit = delta_core.get_delta_last_commit(silver_container, silver_path)
        gold_commit = delta_core.get_delta_last_commit(gold_container, gold_path)
        prior = watermarks.get(watermark_key, {})
        postgres_sync_current = (
            bucket_sync_is_current(sync_state, bucket=bucket, source_commit=silver_commit)
            if postgres_dsn
            else True
        )
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
            continue

        prior_bucket_symbols = sorted(
            symbol for symbol, current_bucket in symbol_to_bucket.items() if current_bucket == bucket
        )
        bucket_symbol_to_bucket: dict[str, str] = {}
        bucket_symbol_failures = 0
        scheduled_rows_retained = 0
        if silver_commit is None:
            skipped_missing_source += 1
            df_gold_bucket = project_gold_output_frame(pd.DataFrame(columns=["date", "symbol"]), domain="earnings")
        else:
            df_silver_bucket = delta_core.load_delta(silver_container, silver_path)
            if (
                df_silver_bucket is not None
                and not df_silver_bucket.empty
                and "record_type" in df_silver_bucket.columns
            ):
                scheduled_rows_retained = int(
                    df_silver_bucket["record_type"].astype("string").str.strip().str.lower().eq("scheduled").sum()
                )
            symbol_frames: list[pd.DataFrame] = []
            if df_silver_bucket is not None and not df_silver_bucket.empty and "symbol" in df_silver_bucket.columns:
                for symbol, group in df_silver_bucket.groupby("symbol"):
                    ticker = str(symbol or "").strip().upper()
                    if not ticker:
                        continue
                    try:
                        df_features = compute_features(group.copy())
                        df_features, _ = apply_backfill_start_cutoff(
                            df_features,
                            date_col="date",
                            backfill_start=backfill_start,
                            context=f"gold earnings alpha26 {ticker}",
                        )
                        if df_features is None or df_features.empty:
                            continue
                        symbol_frames.append(df_features)
                        bucket_symbol_to_bucket[ticker] = bucket
                    except Exception as exc:
                        failed += 1
                        failed_symbols += 1
                        bucket_symbol_failures += 1
                        _log_failure_counter(
                            stage="compute_features",
                            bucket=bucket,
                            ticker=ticker,
                            exc=exc,
                            failed=failed,
                            failed_symbols=failed_symbols,
                            failed_buckets=failed_buckets,
                            failed_finalization=failed_finalization,
                        )
                        mdc.write_warning(f"Gold earnings alpha26 compute failed for {ticker}: {exc}")
            if symbol_frames:
                df_gold_bucket = project_gold_output_frame(
                    pd.concat(symbol_frames, ignore_index=True),
                    domain="earnings",
                )
            else:
                df_gold_bucket = project_gold_output_frame(pd.DataFrame(columns=["date", "symbol"]), domain="earnings")

        future_date_range_max = None
        symbols_with_upcoming_earnings = 0
        if not df_gold_bucket.empty and "date" in df_gold_bucket.columns:
            max_date = pd.to_datetime(df_gold_bucket["date"], errors="coerce").max()
            if pd.notna(max_date):
                future_date_range_max = pd.Timestamp(max_date).date().isoformat()
        if (
            not df_gold_bucket.empty
            and "has_upcoming_earnings" in df_gold_bucket.columns
            and "symbol" in df_gold_bucket.columns
        ):
            symbols_with_upcoming_earnings = int(
                df_gold_bucket.loc[
                    pd.to_numeric(df_gold_bucket["has_upcoming_earnings"], errors="coerce").fillna(0).astype(int) == 1,
                    "symbol",
                ]
                .astype("string")
                .str.upper()
                .nunique()
            )
        mdc.write_line(
            "gold_earnings_bucket_summary "
            f"bucket={bucket} scheduled_rows_retained={scheduled_rows_retained} "
            f"symbols_with_upcoming_earnings={symbols_with_upcoming_earnings} "
            f"future_date_range_max={future_date_range_max or 'n/a'}"
        )

        write_decision = prepare_delta_write_frame(
            df_gold_bucket.reset_index(drop=True),
            container=gold_container,
            path=gold_path,
        )
        mdc.write_line(
            "delta_write_decision layer=gold domain=earnings "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={gold_path}"
        )
        if write_decision.action == "skip_empty_no_schema":
            mdc.write_line(
                f"Skipping Gold earnings empty bucket write for {gold_path}: no existing Delta schema."
            )
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=skipped bucket={bucket} "
                "reason=empty_bucket_no_existing_schema symbols_in=0 symbols_out=0 failures=0"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=earnings bucket={bucket} "
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
                    domain="earnings",
                    bucket=bucket,
                    df=write_decision.frame,
                    date_column="date",
                    job_name="gold-earnings-job",
                )
            except Exception as exc:
                mdc.write_warning(f"Gold earnings metadata bucket artifact write failed bucket={bucket}: {exc}")
            if postgres_dsn:
                sync_result = sync_gold_bucket(
                    domain="earnings",
                    bucket=bucket,
                    frame=write_decision.frame,
                    scope_symbols=sorted(set(prior_bucket_symbols).union(bucket_symbol_to_bucket.keys())),
                    source_commit=silver_commit,
                    dsn=postgres_dsn,
                )
                sync_state[bucket] = sync_state_cache_entry(sync_result)
                mdc.write_line(
                    "postgres_gold_sync_status "
                    f"domain=earnings bucket={bucket} status={sync_result.status} "
                    f"rows_out={sync_result.row_count} symbols_out={sync_result.symbol_count} "
                    f"scope_symbols={sync_result.scope_symbol_count} source_commit={silver_commit}"
                )
            processed += 1
            updated_symbol_to_bucket = _merge_symbol_to_bucket_map(
                symbol_to_bucket,
                touched_bucket=bucket,
                touched_symbol_to_bucket=bucket_symbol_to_bucket,
            )
            watermark_updated = False
            if silver_commit is not None:
                try:
                    checkpoint = gold_checkpoint_publication.publish_gold_checkpoint_aggregate(
                        domain="earnings",
                        bucket=bucket,
                        symbol_to_bucket=symbol_to_bucket,
                        touched_symbol_to_bucket=bucket_symbol_to_bucket,
                        watermarks=watermarks,
                        watermarks_key="gold_earnings_features",
                        watermark_key=watermark_key,
                        source_commit=silver_commit,
                        date_column="date",
                        job_name="gold-earnings-job",
                        save_watermarks_fn=save_watermarks,
                        publish_domain_artifact=False,
                    )
                except Exception as exc:
                    failed += 1
                    failed_buckets += 1
                    _log_failure_counter(
                        stage="checkpoint_publication",
                        bucket=bucket,
                        exc=exc,
                        failed=failed,
                        failed_symbols=failed_symbols,
                        failed_buckets=failed_buckets,
                        failed_finalization=failed_finalization,
                    )
                    mdc.write_error(f"Gold earnings alpha26 checkpoint failed bucket={bucket}: {exc}")
                    mdc.write_line(
                        f"watermark_update_status layer=gold domain=earnings bucket={bucket} "
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
                    f"watermark_update_status layer=gold domain=earnings bucket={bucket} status=updated reason=success"
                )
            else:
                symbol_to_bucket = updated_symbol_to_bucket
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=earnings bucket={bucket} "
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
        except Exception as exc:
            failed += 1
            failed_buckets += 1
            _log_failure_counter(
                stage="bucket_write",
                bucket=bucket,
                exc=exc,
                failed=failed,
                failed_symbols=failed_symbols,
                failed_buckets=failed_buckets,
                failed_finalization=failed_finalization,
            )
            mdc.write_error(f"Gold earnings alpha26 write failed bucket={bucket}: {exc}")
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                f"reason=write_failure symbols_in={len(bucket_symbol_to_bucket)} symbols_out=0 "
                f"failures={bucket_symbol_failures + 1}"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=earnings bucket={bucket} "
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

    status_counts: dict[str, int] = {}
    for result in bucket_results:
        status_counts[result.status] = int(status_counts.get(result.status, 0)) + 1
    finalization = gold_checkpoint_publication.finalize_gold_publication(
        domain="earnings",
        symbol_to_bucket=symbol_to_bucket,
        date_column="date",
        job_name="gold-earnings-job",
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
        mdc.write_line(f"Applying historical cutoff to gold earnings features: {backfill_start_iso}")
    layer_bucketing.gold_layout_mode()

    watermarks = load_watermarks("gold_earnings_features")
    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        alpha26_index_path,
    ) = _run_alpha26_earnings_gold(
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
            reconciliation_orphans, reconciliation_deleted_blobs = _run_earnings_reconciliation(
                silver_container=job_cfg.silver_container,
                gold_container=job_cfg.gold_container,
            )
        except Exception as exc:
            reconciliation_failed = 1
            _log_failure_counter(
                stage="reconciliation",
                exc=exc,
                failed=failed + reconciliation_failed,
                failed_symbols=0,
                failed_buckets=0,
                failed_finalization=reconciliation_failed,
            )
            mdc.write_error(f"Gold earnings reconciliation failed: {exc}")
            mdc.write_line(
                "reconciliation_result layer=gold domain=earnings "
                "status=failed orphan_count=unknown deleted_blobs=unknown cutoff_rows_dropped=unknown"
            )
    if watermarks_dirty and reconciliation_failed == 0:
        save_watermarks("gold_earnings_features", watermarks)
    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Gold earnings alpha26 complete: "
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

    job_name = "gold-earnings-job"
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(lambda: write_system_health_marker(layer="gold", domain="earnings", job_name=job_name),),
            )
        )
