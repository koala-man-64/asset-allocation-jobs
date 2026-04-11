from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import pandas as pd

from core import core as mdc
from core import config as cfg
from core import delta_core
from asset_allocation_contracts.paths import DataPaths
from core import bronze_bucketing
from core import domain_artifacts
from core import layer_bucketing
from tasks.common.backfill import (
    apply_backfill_start_cutoff,
    filter_by_date,
    get_backfill_range,
    get_latest_only_flag,
)
from tasks.common.watermarks import (
    check_blob_unchanged,
    load_last_success,
    load_watermarks,
    normalize_watermark_blob_name,
    save_last_success,
    save_watermarks,
    should_process_blob_since_last_success,
)
from tasks.common.delta_write_policy import prepare_delta_write_frame
from tasks.common.silver_contracts import (
    align_to_existing_schema,
    normalize_columns_to_snake_case,
)
from tasks.common.silver_precision import apply_precision_policy
from tasks.common.market_reconciliation import (
    collect_bronze_earnings_symbols_from_blob_infos,
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)
_ALPHA26_EARNINGS_MIN_COLUMNS = [
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _concat_non_empty_frames(
    frames: list[Optional[pd.DataFrame]],
    *,
    columns: list[str],
    sort: bool = False,
) -> pd.DataFrame:
    cleaned_frames = [
        frame.dropna(axis="columns", how="all")
        for frame in frames
        if frame is not None and not frame.empty and not frame.dropna(how="all").empty
    ]
    cleaned_frames = [frame for frame in cleaned_frames if not frame.empty]
    if not cleaned_frames:
        return pd.DataFrame(columns=columns)
    if len(cleaned_frames) == 1:
        return cleaned_frames[0].reindex(columns=columns).copy()
    return pd.concat(cleaned_frames, ignore_index=True, sort=sort).reindex(columns=columns)


def _coerce_datetime_column(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        parsed_default = pd.to_datetime(series, errors="coerce", utc=True)
        return parsed_default.dt.tz_localize(None)
    numeric = pd.to_numeric(series, errors="coerce")
    parsed_numeric = pd.to_datetime(numeric, errors="coerce", unit="ms", utc=True)
    parsed_default = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed_default.where(numeric.isna(), parsed_numeric).dt.tz_localize(None)


def _split_earnings_bucket_rows(df_bucket: Optional[pd.DataFrame], *, ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df_bucket is None or df_bucket.empty:
        empty = pd.DataFrame()
        return empty, empty

    out = normalize_columns_to_snake_case(df_bucket)
    if "date" in out.columns:
        out["date"] = _coerce_datetime_column(out["date"])
    else:
        out["date"] = pd.NaT
    if "report_date" in out.columns:
        out["report_date"] = _coerce_datetime_column(out["report_date"])
    else:
        out["report_date"] = pd.NaT
    if "fiscal_date_ending" in out.columns:
        out["fiscal_date_ending"] = _coerce_datetime_column(out["fiscal_date_ending"])
    else:
        out["fiscal_date_ending"] = pd.NaT
    if "symbol" not in out.columns:
        out["symbol"] = pd.NA
    out["symbol"] = out["symbol"].astype("string").str.upper()
    symbol = str(ticker or "").strip().upper()
    symbol_mask = out["symbol"] == symbol
    return out.loc[symbol_mask].copy(), out.loc[~symbol_mask].copy()


def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(_utc_now().date())


def _parse_alpha26_bucket_from_blob_name(blob_name: str, *, prefix: str) -> Optional[str]:
    return bronze_bucketing.parse_bucket_from_blob_name(blob_name, expected_prefix=prefix)


def _restore_blob_watermark(
    watermarks: dict,
    *,
    blob_name: str,
    prior_signature: Optional[dict],
) -> None:
    watermark_key = normalize_watermark_blob_name(blob_name)
    if prior_signature is None:
        watermarks.pop(watermark_key, None)
        return
    watermarks[watermark_key] = dict(prior_signature)


def _canonicalize_earnings_frame(df: Optional[pd.DataFrame], *, ticker: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=_ALPHA26_EARNINGS_MIN_COLUMNS)

    out = normalize_columns_to_snake_case(df).copy()
    out = out.drop(columns=["source_hash", "ingested_at"], errors="ignore")
    if ticker is not None:
        out["symbol"] = str(ticker).strip().upper()
    elif "symbol" not in out.columns:
        out["symbol"] = pd.NA
    out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()

    if "date" not in out.columns and "report_date" in out.columns:
        out["date"] = out["report_date"]
    for column in ("date", "report_date", "fiscal_date_ending"):
        if column in out.columns:
            out[column] = _coerce_datetime_column(out[column])
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
            out[column] = pd.NA

    if "is_future_event" in out.columns:
        parsed_future = pd.Series(
            pd.to_numeric(out["is_future_event"], errors="coerce"), index=out.index, dtype="Float64"
        )
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
    out["calendar_time_of_day"] = out["calendar_time_of_day"].astype("string")
    if "calendar_currency" not in out.columns:
        out["calendar_currency"] = pd.NA

    out = out.dropna(subset=["date"]).copy()
    return out[_ALPHA26_EARNINGS_MIN_COLUMNS].reset_index(drop=True)


def _event_identity_key(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="string")
    report_dates = pd.to_datetime(df["report_date"], errors="coerce")
    fiscal_dates = pd.to_datetime(df["fiscal_date_ending"], errors="coerce")
    base_dates = pd.to_datetime(df["date"], errors="coerce")
    preferred = fiscal_dates.where(fiscal_dates.notna(), report_dates.where(report_dates.notna(), base_dates))
    return preferred.dt.strftime("%Y-%m-%d").fillna("")


def _dedupe_earnings_events(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return canonical

    work = canonical.copy()
    work["_event_identity"] = _event_identity_key(work)

    actual = (
        work.loc[work["record_type"] == "actual"]
        .sort_values(["symbol", "date", "report_date", "fiscal_date_ending"])
        .drop_duplicates(subset=["symbol", "_event_identity"], keep="last")
    )
    scheduled = (
        work.loc[work["record_type"] == "scheduled"]
        .sort_values(["symbol", "report_date", "date"])
        .drop_duplicates(subset=["symbol", "_event_identity"], keep="last")
    )
    actual_keys = {
        (str(symbol), str(event_identity))
        for symbol, event_identity in actual[["symbol", "_event_identity"]].itertuples(index=False, name=None)
    }
    scheduled = scheduled.loc[
        scheduled.apply(lambda row: (str(row["symbol"]), str(row["_event_identity"])) not in actual_keys, axis=1)
    ].copy()

    out = _concat_non_empty_frames(
        [actual, scheduled],
        columns=_ALPHA26_EARNINGS_MIN_COLUMNS,
        sort=False,
    )
    out = out.drop(columns=["_event_identity"], errors="ignore")
    return out[_ALPHA26_EARNINGS_MIN_COLUMNS].sort_values(["date", "record_type"]).reset_index(drop=True)


def process_file(blob_name: str) -> bool:
    """
    Backwards-compatible wrapper (tests/local tooling) that processes a blob by name.

    Production uses `process_blob()` with `last_modified` metadata for freshness checks.
    """
    return process_blob({"name": blob_name}, watermarks={}) != "failed"


def _process_symbol_frame(
    *,
    ticker: str,
    df_new: pd.DataFrame,
    source_name: str,
    include_history: bool = True,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    bucket = layer_bucketing.bucket_letter(ticker)
    cloud_path = DataPaths.get_silver_earnings_bucket_path(bucket)
    out = _canonicalize_earnings_frame(
        df_new.drop(columns=[col for col in df_new.columns if "Unnamed" in str(col)], errors="ignore"),
        ticker=ticker,
    )
    if out.empty:
        mdc.write_error(
            f"Failed to normalize earnings payload for {source_name}: no valid rows after canonicalization."
        )
        return "failed"

    backfill_start, backfill_end = get_backfill_range()
    if backfill_start or backfill_end:
        out = filter_by_date(out, "date", backfill_start, backfill_end)
        latest_only = False
    else:
        latest_only = get_latest_only_flag("EARNINGS", default=True)
    if "record_type" in out.columns and out["record_type"].eq("scheduled").any():
        latest_only = False
    if not include_history:
        latest_only = False

    if latest_only and "date" in out.columns and not out.empty:
        latest_date = out["date"].max()
        out = out[out["date"] == latest_date].copy()

    existing_bucket = (
        delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, cloud_path) if (persist or include_history) else None
    )
    df_history, df_other_symbols = _split_earnings_bucket_rows(existing_bucket, ticker=ticker)
    if not include_history:
        df_history = pd.DataFrame()
    df_merged = _dedupe_earnings_events(
        _concat_non_empty_frames(
            [df_history, out],
            columns=_ALPHA26_EARNINGS_MIN_COLUMNS,
            sort=False,
        )
    )

    df_merged, _ = apply_backfill_start_cutoff(
        df_merged,
        date_col="date",
        backfill_start=backfill_start,
        context=f"silver earnings {ticker}",
    )
    if backfill_start is not None and df_merged.empty:
        if not persist:
            return "ok"
        if silver_client is not None:
            df_remaining = _canonicalize_earnings_frame(df_other_symbols)
            if "symbol" in df_remaining.columns:
                df_remaining["symbol"] = df_remaining["symbol"].astype("string").str.upper()
            if df_remaining.empty:
                deleted = silver_client.delete_prefix(cloud_path)
                mdc.write_line(
                    f"Silver earnings backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                    f"deleted {deleted} blob(s) under {cloud_path}."
                )
            else:
                delta_core.store_delta(df_remaining.reset_index(drop=True), cfg.AZURE_CONTAINER_SILVER, cloud_path)
                delta_core.vacuum_delta_table(
                    cfg.AZURE_CONTAINER_SILVER,
                    cloud_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
                mdc.write_line(f"Silver earnings backfill purge for {ticker}: removed symbol rows from {cloud_path}.")
            return "ok"
        mdc.write_warning(
            f"Silver earnings backfill purge for {ticker} could not update {cloud_path}: storage client unavailable."
        )
        return "failed"

    df_merged = _canonicalize_earnings_frame(df_merged, ticker=ticker)
    df_merged = apply_precision_policy(
        df_merged,
        price_columns=set(),
        calculated_columns=set(),
        price_scale=2,
        calculated_scale=4,
    )
    try:
        if not persist:
            if alpha26_bucket_frames is None:
                raise ValueError("alpha26_bucket_frames must be provided when persist=False.")
            alpha26_bucket_frames.setdefault(bucket, []).append(df_merged.copy())
        else:
            df_other_symbols = _canonicalize_earnings_frame(df_other_symbols)
            if "symbol" in df_other_symbols.columns:
                df_other_symbols["symbol"] = df_other_symbols["symbol"].astype("string").str.upper()
            parts_to_store = [frame for frame in (df_other_symbols, df_merged) if frame is not None and not frame.empty]
            if parts_to_store:
                df_bucket_to_store = _concat_non_empty_frames(
                    parts_to_store,
                    columns=_ALPHA26_EARNINGS_MIN_COLUMNS,
                ).reset_index(drop=True)
            else:
                df_bucket_to_store = pd.DataFrame(columns=_ALPHA26_EARNINGS_MIN_COLUMNS)
            df_bucket_to_store = align_to_existing_schema(df_bucket_to_store, cfg.AZURE_CONTAINER_SILVER, cloud_path)
            delta_core.store_delta(df_bucket_to_store, cfg.AZURE_CONTAINER_SILVER, cloud_path)
            if backfill_start is not None:
                delta_core.vacuum_delta_table(
                    cfg.AZURE_CONTAINER_SILVER,
                    cloud_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
        mdc.write_line(
            "precision_policy_applied domain=earnings "
            f"ticker={ticker} price_cols=none calc_cols=none rows={len(df_merged)}"
        )
    except Exception as exc:
        mdc.write_error(f"Failed to write Silver Delta for {ticker}: {exc}")
        return "failed"

    if persist:
        mdc.write_line(f"Updated Silver Delta bucket {cloud_path} for {ticker} (rows={len(df_merged)})")
    return "ok"


def process_blob(
    blob: dict,
    *,
    watermarks: dict,
    include_history: bool = True,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    blob_name = blob["name"]  # earnings-data/{symbol}.json
    watermark_key = normalize_watermark_blob_name(blob_name)
    if not blob_name.endswith(".json"):
        return "skipped_non_json"

    prefix_len = len(cfg.EARNINGS_DATA_PREFIX) + 1
    ticker = blob_name[prefix_len:].replace(".json", "")
    mdc.write_line(f"Processing {ticker} from {blob_name}...")
    unchanged, signature = check_blob_unchanged(blob, watermarks.get(watermark_key))
    if unchanged:
        return "skipped_unchanged"

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_new = pd.read_json(BytesIO(raw_bytes), orient="records")
    except Exception as exc:
        mdc.write_error(f"Failed to read/parse {blob_name}: {exc}")
        return "failed"

    status = _process_symbol_frame(
        ticker=ticker,
        df_new=df_new,
        source_name=blob_name,
        include_history=include_history,
        persist=persist,
        alpha26_bucket_frames=alpha26_bucket_frames,
    )
    if status == "ok" and signature:
        signature["updated_at"] = _utc_now_iso()
        watermarks[watermark_key] = signature
    return status


def process_alpha26_bucket_blob(
    blob: dict,
    *,
    watermarks: dict,
    include_history: bool = False,
    persist: bool = False,
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    blob_name = str(blob.get("name", ""))
    watermark_key = normalize_watermark_blob_name(blob_name)
    if not blob_name.endswith(".parquet"):
        return "skipped_non_parquet"

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(watermark_key))
    if unchanged:
        return "skipped_unchanged"

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_bucket = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as exc:
        mdc.write_error(f"Failed to read earnings alpha26 bucket {blob_name}: {exc}")
        return "failed"

    if df_bucket is None or df_bucket.empty:
        if signature:
            signature["updated_at"] = _utc_now_iso()
            watermarks[watermark_key] = signature
        return "ok"

    symbol_col = "symbol" if "symbol" in df_bucket.columns else ("Symbol" if "Symbol" in df_bucket.columns else None)
    if symbol_col is None:
        mdc.write_error(f"Missing symbol column in earnings alpha26 bucket {blob_name}.")
        return "failed"

    debug_symbols = set(getattr(cfg, "DEBUG_SYMBOLS", []) or [])
    has_failed = False
    for symbol, group in df_bucket.groupby(symbol_col):
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            continue
        if debug_symbols and ticker not in debug_symbols:
            continue
        status = _process_symbol_frame(
            ticker=ticker,
            df_new=group.copy(),
            source_name=blob_name,
            include_history=include_history,
            persist=persist,
            alpha26_bucket_frames=alpha26_bucket_frames,
        )
        if status == "failed":
            has_failed = True

    if not has_failed and signature:
        signature["updated_at"] = _utc_now_iso()
        watermarks[watermark_key] = signature
    return "failed" if has_failed else "ok"


def _write_alpha26_earnings_buckets(
    bucket_frames: dict[str, list[pd.DataFrame]],
    *,
    touched_buckets: Optional[set[str]] = None,
) -> tuple[int, Optional[str], Optional[int]]:
    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    selected_buckets = {
        str(bucket).strip().upper()
        for bucket in (touched_buckets if touched_buckets is not None else valid_buckets)
        if str(bucket).strip()
    }
    if not selected_buckets:
        return 0, None, len(_ALPHA26_EARNINGS_MIN_COLUMNS)

    invalid_buckets = selected_buckets.difference(valid_buckets)
    if invalid_buckets:
        raise ValueError(f"Invalid alpha26 bucket(s) for Silver write: {sorted(invalid_buckets)}")

    existing_symbol_to_bucket = layer_bucketing.load_layer_symbol_to_bucket_map(layer="silver", domain="earnings")
    is_partial_update = selected_buckets != valid_buckets
    if is_partial_update and not existing_symbol_to_bucket:
        raise RuntimeError(
            "Silver earnings incremental alpha26 write blocked: existing silver earnings symbol index is missing."
        )

    touched_symbol_to_bucket: dict[str, str] = {}
    for bucket in sorted(selected_buckets):
        bucket_path = DataPaths.get_silver_earnings_bucket_path(bucket)
        parts = bucket_frames.get(bucket, [])
        if parts:
            df_bucket = _concat_non_empty_frames(
                parts,
                columns=_ALPHA26_EARNINGS_MIN_COLUMNS,
            )
            if "symbol" in df_bucket.columns and "date" in df_bucket.columns:
                df_bucket["symbol"] = df_bucket["symbol"].astype(str).str.upper()
                df_bucket["date"] = pd.to_datetime(df_bucket["date"], errors="coerce")
                df_bucket = df_bucket.dropna(subset=["symbol", "date"]).copy()
                df_bucket = df_bucket.sort_values(["symbol", "date"]).drop_duplicates(
                    subset=["symbol", "date"], keep="last"
                )
                for symbol in df_bucket["symbol"].dropna().astype(str).tolist():
                    if symbol:
                        touched_symbol_to_bucket[symbol] = bucket
            else:
                df_bucket = pd.DataFrame(columns=_ALPHA26_EARNINGS_MIN_COLUMNS)
        else:
            df_bucket = pd.DataFrame(columns=_ALPHA26_EARNINGS_MIN_COLUMNS)
        scheduled_rows_retained = 0
        symbols_with_upcoming_earnings = 0
        future_date_range_max = None
        if not df_bucket.empty and "record_type" in df_bucket.columns:
            scheduled_mask = df_bucket["record_type"].astype("string").str.strip().str.lower().eq("scheduled")
            scheduled_rows_retained = int(scheduled_mask.sum())
            if scheduled_mask.any() and "symbol" in df_bucket.columns:
                symbols_with_upcoming_earnings = int(
                    df_bucket.loc[scheduled_mask, "symbol"].astype("string").str.upper().nunique()
                )
        if not df_bucket.empty and "date" in df_bucket.columns:
            max_date = pd.to_datetime(df_bucket["date"], errors="coerce").max()
            if pd.notna(max_date):
                future_date_range_max = pd.Timestamp(max_date).date().isoformat()
        mdc.write_line(
            "silver_earnings_bucket_summary "
            f"bucket={bucket} scheduled_rows_retained={scheduled_rows_retained} "
            f"symbols_with_upcoming_earnings={symbols_with_upcoming_earnings} "
            f"future_date_range_max={future_date_range_max or 'n/a'}"
        )
        write_decision = prepare_delta_write_frame(
            df_bucket.reset_index(drop=True),
            container=cfg.AZURE_CONTAINER_SILVER,
            path=bucket_path,
        )
        mdc.write_line(
            "delta_write_decision layer=silver domain=earnings "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={bucket_path}"
        )
        if write_decision.action == "skip_empty_no_schema":
            mdc.write_line(f"Skipping Silver earnings empty bucket write for {bucket_path}: no existing Delta schema.")
            continue
        delta_core.store_delta(
            write_decision.frame,
            cfg.AZURE_CONTAINER_SILVER,
            bucket_path,
            mode="overwrite",
        )
        try:
            domain_artifacts.write_bucket_artifact(
                layer="silver",
                domain="earnings",
                bucket=bucket,
                df=write_decision.frame,
                date_column="date",
                client=silver_client,
                job_name="silver-earnings-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Silver earnings metadata bucket artifact write failed bucket={bucket}: {exc}")
    symbol_to_bucket = layer_bucketing.merge_symbol_to_bucket_map(
        existing_symbol_to_bucket,
        touched_buckets=selected_buckets,
        touched_symbol_to_bucket=touched_symbol_to_bucket,
    )
    index_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="earnings",
        symbol_to_bucket=symbol_to_bucket,
    )
    column_count: Optional[int] = len(_ALPHA26_EARNINGS_MIN_COLUMNS)
    if index_path:
        try:
            payload = domain_artifacts.write_domain_artifact(
                layer="silver",
                domain="earnings",
                date_column="date",
                client=silver_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=index_path,
                job_name="silver-earnings-job",
            )
            column_count = domain_artifacts.extract_column_count(payload)
        except Exception as exc:
            mdc.write_warning(f"Silver earnings metadata artifact write failed: {exc}")
    return len(symbol_to_bucket), index_path, column_count


def _run_earnings_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver earnings reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_earnings_symbols_from_blob_infos(bronze_blob_list)
    earnings_prefix = str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")).strip("/")
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix=earnings_prefix)
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_silver_earnings_bucket_path(layer_bucketing.bucket_letter(symbol))
        ],
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            cfg.AZURE_CONTAINER_SILVER,
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
            "Silver earnings reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Silver earnings reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Silver earnings orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=layer_bucketing.all_silver_bucket_paths(domain="earnings"),
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="silver earnings reconciliation cutoff",
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            cfg.AZURE_CONTAINER_SILVER,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    if cutoff_stats.rows_dropped > 0 or cutoff_stats.tables_rewritten > 0 or cutoff_stats.deleted_blobs > 0:
        mdc.write_line(
            "Silver earnings reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(f"Silver earnings reconciliation cutoff sweep encountered errors={cutoff_stats.errors}.")
    return len(orphan_symbols), deleted_blobs


def main():
    mdc.log_environment_diagnostics()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver earnings data: {backfill_start.date().isoformat()}")
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
    force_rebuild = layer_bucketing.silver_alpha26_force_rebuild()

    mdc.write_line("Listing Bronze files...")
    earnings_prefix = str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")).strip("/")
    watermarks = load_watermarks("bronze_earnings_data")
    last_success = load_last_success("silver_earnings_data")
    watermarks_dirty = False
    blob_list = bronze_bucketing.list_active_bucket_blob_infos("earnings", bronze_client)

    checkpoint_skipped = 0
    candidate_blobs: list[dict] = []
    for blob in blob_list:
        watermark_key = normalize_watermark_blob_name(blob.get("name"))
        prior = watermarks.get(watermark_key)
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_rebuild,
        )
        if should_process:
            candidate_blobs.append(blob)
        else:
            checkpoint_skipped += 1

    if last_success is not None:
        mdc.write_line(
            "Silver earnings checkpoint filter: "
            f"last_success={last_success.isoformat()} candidates={len(candidate_blobs)} skipped_checkpoint={checkpoint_skipped}"
        )
    mdc.write_line(f"Found {len(blob_list)} files total; {len(candidate_blobs)} candidate files to process.")

    processed = 0
    failed = 0
    skipped_unchanged = 0
    skipped_other = 0
    alpha26_staged_rows = 0
    alpha26_flush_count = 0
    alpha26_written_symbols = 0
    alpha26_index_path: Optional[str] = None
    alpha26_column_count: Optional[int] = len(_ALPHA26_EARNINGS_MIN_COLUMNS)
    for blob in candidate_blobs:
        blob_name = str(blob.get("name", ""))
        watermark_key = normalize_watermark_blob_name(blob_name)
        prior_signature = dict(watermarks[watermark_key]) if isinstance(watermarks.get(watermark_key), dict) else None
        alpha26_bucket_frames: dict[str, list[pd.DataFrame]] = {}
        status = process_alpha26_bucket_blob(
            blob,
            watermarks=watermarks,
            include_history=False,
            persist=False,
            alpha26_bucket_frames=alpha26_bucket_frames,
        )
        if status == "ok":
            processed += 1
            staged_rows = layer_bucketing.count_staged_frame_rows(alpha26_bucket_frames)
            alpha26_staged_rows += staged_rows
            if staged_rows == 0:
                watermarks_dirty = True
                continue
            touched_bucket = _parse_alpha26_bucket_from_blob_name(blob_name, prefix=earnings_prefix)
            if not touched_bucket:
                _restore_blob_watermark(watermarks, blob_name=blob_name, prior_signature=prior_signature)
                failed += 1
                mdc.write_error(
                    f"Silver earnings alpha26 write failed: unable to resolve bucket from blob {blob_name!r}."
                )
                break
            try:
                alpha26_written_symbols, alpha26_index_path, alpha26_column_count = _write_alpha26_earnings_buckets(
                    alpha26_bucket_frames,
                    touched_buckets={touched_bucket},
                )
                alpha26_flush_count += 1
                watermarks_dirty = True
                mdc.write_line(
                    "Silver earnings alpha26 buckets written: "
                    f"touched_buckets=1 symbols={alpha26_written_symbols} "
                    f"index_path={alpha26_index_path or 'unavailable'}"
                )
            except Exception as exc:
                _restore_blob_watermark(watermarks, blob_name=blob_name, prior_signature=prior_signature)
                failed += 1
                mdc.write_error(f"Silver earnings alpha26 bucket write failed: {exc}")
                break
        elif status == "skipped_unchanged":
            skipped_unchanged += 1
        elif status.startswith("skipped"):
            skipped_other += 1
        else:
            failed += 1

    if failed == 0:
        if alpha26_staged_rows == 0:
            mdc.write_line("Silver earnings alpha26 bucket write skipped: no staged rows.")
        elif alpha26_flush_count == 0:
            failed += 1
            mdc.write_error("Silver earnings alpha26 bucket write blocked: staged rows were never flushed.")

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver earnings job complete: "
        f"processed={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_other={skipped_other} skipped_checkpoint={checkpoint_skipped} "
        f"alpha26_staged_rows={alpha26_staged_rows} "
        f"alpha26_symbols={alpha26_written_symbols} "
        f"reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} "
        f"failed={total_failed}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_earnings_data", watermarks)
    if total_failed == 0:
        save_last_success(
            "silver_earnings_data",
            metadata={
                "total_blobs": len(blob_list),
                "candidates": len(candidate_blobs),
                "processed": processed,
                "skipped_checkpoint": checkpoint_skipped,
                "skipped_unchanged": skipped_unchanged,
                "skipped_other": skipped_other,
                "alpha26_staged_rows": alpha26_staged_rows,
                "alpha26_symbols": alpha26_written_symbols,
                "alpha26_index_path": alpha26_index_path,
                "column_count": alpha26_column_count,
                "reconciled_orphans": reconciliation_orphans,
                "reconciliation_deleted_blobs": reconciliation_deleted_blobs,
            },
        )
        return 0
    return 1


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "silver-earnings-job"
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="silver", domain="earnings", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
