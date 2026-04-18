from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import numpy as np
import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import delta_core
from tasks.price_target_data import config as cfg
from asset_allocation_contracts.paths import DataPaths
from asset_allocation_runtime_common.market_data import bronze_bucketing
from asset_allocation_runtime_common.market_data import domain_artifacts
from asset_allocation_runtime_common.market_data import layer_bucketing
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
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
    ContractViolation,
    align_to_existing_schema,
    assert_no_unexpected_mixed_empty,
    log_contract_violation,
    normalize_date_column,
    normalize_columns_to_snake_case,
)
from tasks.common.silver_precision import apply_precision_policy
from tasks.common.market_reconciliation import (
    collect_bronze_price_target_symbols_from_blob_infos,
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)
_PRICE_TARGET_PRICE_COLUMNS = {"tp_mean_est", "tp_high_est", "tp_low_est"}
_PRICE_TARGET_CALCULATED_COLUMNS = {"tp_std_dev_est"}
_ALPHA26_PRICE_TARGET_MIN_COLUMNS = [
    "obs_date",
    "symbol",
    "tp_mean_est",
    "tp_std_dev_est",
    "tp_high_est",
    "tp_low_est",
    "tp_cnt_est",
    "tp_cnt_est_rev_up",
    "tp_cnt_est_rev_down",
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


def _split_price_target_bucket_rows(
    df_bucket: Optional[pd.DataFrame],
    *,
    ticker: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df_bucket is None or df_bucket.empty:
        empty = pd.DataFrame()
        return empty, empty

    out = df_bucket.copy()
    if "obs_date" in out.columns:
        out["obs_date"] = pd.to_datetime(out["obs_date"], errors="coerce")
    if "symbol" not in out.columns:
        out["symbol"] = pd.NA
    out["symbol"] = out["symbol"].astype("string").str.upper()
    symbol = str(ticker or "").strip().upper()
    symbol_mask = out["symbol"] == symbol
    return out.loc[symbol_mask].copy(), out.loc[~symbol_mask].copy()


def _parse_alpha26_bucket_from_blob_name(blob_name: str) -> Optional[str]:
    return bronze_bucketing.parse_bucket_from_blob_name(blob_name, expected_prefix="price-target-data")


def _extract_ticker(blob_name: str) -> str:
    return str(blob_name or "").replace("price-target-data/", "").replace(".parquet", "")


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
    silver_path = DataPaths.get_silver_price_target_bucket_path(bucket)
    backfill_start, _ = get_backfill_range()
    out = df_new.copy()
    out = out.drop(columns=["ingested_at", "source_hash"], errors="ignore")

    column_names = [
        "symbol",
        "obs_date",
        "tp_mean_est",
        "tp_std_dev_est",
        "tp_high_est",
        "tp_low_est",
        "tp_cnt_est",
        "tp_cnt_est_rev_up",
        "tp_cnt_est_rev_down",
    ]

    if out.empty:
        return "skipped_empty"

    try:
        out = normalize_date_column(
            out,
            context=f"price target date parse {source_name}",
            aliases=("obs_date",),
            canonical="obs_date",
        )
        out = assert_no_unexpected_mixed_empty(out, context=f"price target date filter {source_name}", alias="obs_date")
        out["obs_date"] = out["obs_date"].dt.normalize()
    except ContractViolation as exc:
        log_contract_violation(
            f"price-target preflight failed for {ticker} in {source_name}",
            exc,
            severity="ERROR",
        )
        return "failed"

    out, _ = apply_backfill_start_cutoff(
        out,
        date_col="obs_date",
        backfill_start=backfill_start,
        context=f"silver price-target {ticker}",
    )
    if backfill_start is not None and out.empty:
        if not persist:
            return "ok"
        if silver_client is not None:
            deleted = silver_client.delete_prefix(silver_path)
            mdc.write_line(
                f"Silver price-target backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                f"deleted {deleted} blob(s) under {silver_path}."
            )
            return "ok"
        mdc.write_warning(
            f"Silver price-target backfill purge for {ticker} could not delete {silver_path}: storage client unavailable."
        )
        return "failed"

    out = out.sort_values(by="obs_date")
    today = pd.to_datetime("today").normalize()
    if not out.empty:
        latest_obs = out["obs_date"].max()
        if latest_obs < today:
            all_dates = pd.date_range(start=out["obs_date"].min(), end=today)
            df_dates = pd.DataFrame({"obs_date": all_dates})
            out = df_dates.merge(out, on="obs_date", how="left")
            out = out.ffill()

    out["symbol"] = ticker
    for col in column_names:
        if col not in out.columns:
            out[col] = np.nan
    out = out[column_names]

    out = out.set_index("obs_date")
    out = out[~out.index.duplicated(keep="last")]
    full_range = pd.date_range(start=out.index.min(), end=out.index.max(), freq="D")
    out = out.reindex(full_range)
    out.ffill(inplace=True)
    out = out.reset_index().rename(columns={"index": "obs_date"})
    out["symbol"] = ticker

    existing_bucket = (
        delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, silver_path)
        if (persist or include_history)
        else None
    )
    df_history, df_other_symbols = _split_price_target_bucket_rows(existing_bucket, ticker=ticker)
    if not include_history:
        df_history = pd.DataFrame()
    if df_history is None or df_history.empty:
        df_merged = out
    else:
        df_history = align_to_existing_schema(df_history, container=cfg.AZURE_CONTAINER_SILVER, path=silver_path)
        if "obs_date" in df_history.columns:
            df_history["obs_date"] = pd.to_datetime(df_history["obs_date"], errors="coerce")
        df_merged = pd.concat([df_history, out], ignore_index=True)

    df_merged = df_merged.drop_duplicates(subset=["obs_date", "symbol"], keep="last")
    df_merged = df_merged.sort_values(by=["obs_date", "symbol"])
    df_merged = df_merged.reset_index(drop=True)

    df_merged, _ = apply_backfill_start_cutoff(
        df_merged,
        date_col="obs_date",
        backfill_start=backfill_start,
        context=f"silver price-target merged {ticker}",
    )
    if backfill_start is not None and df_merged.empty:
        if not persist:
            return "ok"
        if silver_client is not None:
            if "symbol" in df_other_symbols.columns:
                df_other_symbols["symbol"] = df_other_symbols["symbol"].astype("string").str.upper()
            if df_other_symbols.empty:
                deleted = silver_client.delete_prefix(silver_path)
                mdc.write_line(
                    f"Silver price-target merged purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                    f"deleted {deleted} blob(s) under {silver_path}."
                )
            else:
                delta_core.store_delta(
                    df_other_symbols.reset_index(drop=True),
                    cfg.AZURE_CONTAINER_SILVER,
                    silver_path,
                    mode="overwrite",
                )
                delta_core.vacuum_delta_table(
                    cfg.AZURE_CONTAINER_SILVER,
                    silver_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
                mdc.write_line(
                    f"Silver price-target merged purge for {ticker}: removed symbol rows from {silver_path}."
                )
            return "ok"
        mdc.write_warning(
            f"Silver price-target merged purge for {ticker} could not delete {silver_path}: storage client unavailable."
        )
        return "failed"

    df_merged = normalize_columns_to_snake_case(df_merged)
    df_merged = apply_precision_policy(
        df_merged,
        price_columns=_PRICE_TARGET_PRICE_COLUMNS,
        calculated_columns=_PRICE_TARGET_CALCULATED_COLUMNS,
        price_scale=2,
        calculated_scale=4,
    )

    if not persist:
        if alpha26_bucket_frames is None:
            raise ValueError("alpha26_bucket_frames must be provided when persist=False.")
        alpha26_bucket_frames.setdefault(bucket, []).append(df_merged.copy())
    else:
        if "symbol" in df_other_symbols.columns:
            df_other_symbols["symbol"] = df_other_symbols["symbol"].astype("string").str.upper()
        df_bucket_to_store = _concat_non_empty_frames(
            [df_other_symbols, df_merged],
            columns=_ALPHA26_PRICE_TARGET_MIN_COLUMNS,
        ).reset_index(drop=True)
        delta_core.store_delta(df_bucket_to_store, cfg.AZURE_CONTAINER_SILVER, silver_path, mode="overwrite")
        if backfill_start is not None:
            delta_core.vacuum_delta_table(
                cfg.AZURE_CONTAINER_SILVER,
                silver_path,
                retention_hours=0,
                dry_run=False,
                enforce_retention_duration=False,
                full=True,
            )
    applied_price_cols = sorted(col for col in _PRICE_TARGET_PRICE_COLUMNS if col in df_merged.columns)
    applied_calc_cols = sorted(col for col in _PRICE_TARGET_CALCULATED_COLUMNS if col in df_merged.columns)
    price_cols_str = ",".join(applied_price_cols) if applied_price_cols else "none"
    calc_cols_str = ",".join(applied_calc_cols) if applied_calc_cols else "none"
    mdc.write_line(
        "precision_policy_applied domain=price-target "
        f"ticker={ticker} price_cols={price_cols_str} calc_cols={calc_cols_str} rows={len(df_merged)}"
    )
    if persist:
        mdc.write_line(f"Updated Silver {ticker}")
    return "ok"


def process_blob(
    blob,
    *,
    watermarks: dict,
    include_history: bool = True,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    blob_name = str(blob.get("name", ""))
    watermark_key = normalize_watermark_blob_name(blob_name)
    if not blob_name.endswith(".parquet"):
        return "skipped_non_parquet"

    ticker = _extract_ticker(blob_name)
    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS and ticker not in cfg.DEBUG_SYMBOLS:
        return "skipped_debug_symbols"

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(watermark_key))
    if unchanged:
        return "skipped_unchanged"

    mdc.write_line(f"Processing {ticker}...")
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_new = pd.read_parquet(BytesIO(raw_bytes))
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
    except Exception as exc:
        mdc.write_error(f"Failed to process {ticker}: {exc}")
        return "failed"


def process_alpha26_bucket_blob(
    blob,
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
        mdc.write_error(f"Failed to read price-target alpha26 bucket {blob_name}: {exc}")
        return "failed"

    if df_bucket is None or df_bucket.empty:
        if signature:
            signature["updated_at"] = _utc_now_iso()
            watermarks[watermark_key] = signature
        return "ok"

    symbol_col = "symbol" if "symbol" in df_bucket.columns else ("Symbol" if "Symbol" in df_bucket.columns else None)
    if symbol_col is None:
        mdc.write_error(f"Missing symbol column in price-target alpha26 bucket {blob_name}.")
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


def _write_alpha26_price_target_buckets(
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
        return 0, None, len(_ALPHA26_PRICE_TARGET_MIN_COLUMNS)

    invalid_buckets = selected_buckets.difference(valid_buckets)
    if invalid_buckets:
        raise ValueError(f"Invalid alpha26 bucket(s) for Silver write: {sorted(invalid_buckets)}")

    existing_symbol_to_bucket = layer_bucketing.load_layer_symbol_to_bucket_map(
        layer="silver",
        domain="price-target",
    )
    is_partial_update = selected_buckets != valid_buckets
    if is_partial_update and not existing_symbol_to_bucket:
        raise RuntimeError(
            "Silver price-target incremental alpha26 write blocked: existing silver price-target symbol index is missing."
        )

    touched_symbol_to_bucket: dict[str, str] = {}
    for bucket in sorted(selected_buckets):
        bucket_path = DataPaths.get_silver_price_target_bucket_path(bucket)
        parts = bucket_frames.get(bucket, [])
        if parts:
            df_bucket = _concat_non_empty_frames(
                parts,
                columns=_ALPHA26_PRICE_TARGET_MIN_COLUMNS,
            )
            if "symbol" in df_bucket.columns and "obs_date" in df_bucket.columns:
                df_bucket["symbol"] = df_bucket["symbol"].astype(str).str.upper()
                df_bucket["obs_date"] = pd.to_datetime(df_bucket["obs_date"], errors="coerce")
                df_bucket = df_bucket.dropna(subset=["symbol", "obs_date"]).copy()
                df_bucket = df_bucket.sort_values(["symbol", "obs_date"]).drop_duplicates(
                    subset=["symbol", "obs_date"], keep="last"
                )
                for symbol in df_bucket["symbol"].dropna().astype(str).tolist():
                    if symbol:
                        touched_symbol_to_bucket[symbol] = bucket
            else:
                df_bucket = pd.DataFrame(columns=_ALPHA26_PRICE_TARGET_MIN_COLUMNS)
        else:
            df_bucket = pd.DataFrame(columns=_ALPHA26_PRICE_TARGET_MIN_COLUMNS)
        write_decision = prepare_delta_write_frame(
            df_bucket.reset_index(drop=True),
            container=cfg.AZURE_CONTAINER_SILVER,
            path=bucket_path,
        )
        mdc.write_line(
            "delta_write_decision layer=silver domain=price-target "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={bucket_path}"
        )
        if write_decision.action == "skip_empty_no_schema":
            mdc.write_line(
                f"Skipping Silver price-target empty bucket write for {bucket_path}: no existing Delta schema."
            )
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
                domain="price-target",
                bucket=bucket,
                df=write_decision.frame,
                date_column="obs_date",
                client=silver_client,
                job_name="silver-price-target-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Silver price-target metadata bucket artifact write failed bucket={bucket}: {exc}")
    symbol_to_bucket = layer_bucketing.merge_symbol_to_bucket_map(
        existing_symbol_to_bucket,
        touched_buckets=selected_buckets,
        touched_symbol_to_bucket=touched_symbol_to_bucket,
    )
    index_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="price-target",
        symbol_to_bucket=symbol_to_bucket,
    )
    column_count: Optional[int] = len(_ALPHA26_PRICE_TARGET_MIN_COLUMNS)
    if index_path:
        try:
            payload = domain_artifacts.write_domain_artifact(
                layer="silver",
                domain="price-target",
                date_column="obs_date",
                client=silver_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=index_path,
                job_name="silver-price-target-job",
            )
            column_count = domain_artifacts.extract_column_count(payload)
        except Exception as exc:
            mdc.write_warning(f"Silver price-target metadata artifact write failed: {exc}")
    return len(symbol_to_bucket), index_path, column_count


def _run_price_target_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver price-target reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_price_target_symbols_from_blob_infos(bronze_blob_list)
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="price-target-data")
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_silver_price_target_bucket_path(layer_bucketing.bucket_letter(symbol))
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
            "Silver price-target reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Silver price-target reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Silver price-target orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=layer_bucketing.all_silver_bucket_paths(domain="price-target"),
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        date_column_candidates=("obs_date",),
        backfill_start=backfill_start,
        context="silver price-target reconciliation cutoff",
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
            "Silver price-target reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(
            f"Silver price-target reconciliation cutoff sweep encountered errors={cutoff_stats.errors}."
        )
    return len(orphan_symbols), deleted_blobs


def main():
    mdc.log_environment_diagnostics()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver price-target data: {backfill_start.date().isoformat()}")
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
    force_rebuild = layer_bucketing.silver_alpha26_force_rebuild()
    mdc.write_line("Listing Bronze Price Target files...")
    watermarks = load_watermarks("bronze_price_target_data")
    last_success = load_last_success("silver_price_target_data")
    watermarks_dirty = False

    blob_list = bronze_bucketing.list_active_bucket_blob_infos("price-target", bronze_client)
    checkpoint_skipped = 0
    forced_schema_migration = 0
    candidate_blobs: list[dict] = []
    for blob in blob_list:
        blob_name = str(blob.get("name", ""))
        force_reprocess = False
        watermark_key = normalize_watermark_blob_name(blob_name)
        prior = watermarks.get(watermark_key)
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_reprocess or force_rebuild,
        )
        if should_process:
            candidate_blobs.append(blob)
        else:
            checkpoint_skipped += 1

    if last_success is not None:
        mdc.write_line(
            "Silver price target checkpoint filter: "
            f"last_success={last_success.isoformat()} candidates={len(candidate_blobs)} "
            f"skipped_checkpoint={checkpoint_skipped} forced_schema_migration={forced_schema_migration}"
        )
    mdc.write_line(f"Found {len(blob_list)} blobs total; {len(candidate_blobs)} candidate blobs. Processing...")

    ok_or_skipped = 0
    failed = 0
    skipped_unchanged = 0
    skipped_other = 0
    alpha26_staged_rows = 0
    alpha26_flush_count = 0
    alpha26_written_symbols = 0
    alpha26_index_path: Optional[str] = None
    alpha26_column_count: Optional[int] = len(_ALPHA26_PRICE_TARGET_MIN_COLUMNS)
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
            ok_or_skipped += 1
            staged_rows = layer_bucketing.count_staged_frame_rows(alpha26_bucket_frames)
            alpha26_staged_rows += staged_rows
            if staged_rows == 0:
                watermarks_dirty = True
                continue
            touched_bucket = _parse_alpha26_bucket_from_blob_name(blob_name)
            if not touched_bucket:
                _restore_blob_watermark(watermarks, blob_name=blob_name, prior_signature=prior_signature)
                failed += 1
                mdc.write_error(
                    f"Silver price-target alpha26 write failed: unable to resolve bucket from blob {blob_name!r}."
                )
                break
            try:
                alpha26_written_symbols, alpha26_index_path, alpha26_column_count = (
                    _write_alpha26_price_target_buckets(
                        alpha26_bucket_frames,
                        touched_buckets={touched_bucket},
                    )
                )
                alpha26_flush_count += 1
                watermarks_dirty = True
                mdc.write_line(
                    "Silver price-target alpha26 buckets written: "
                    f"touched_buckets=1 symbols={alpha26_written_symbols} "
                    f"index_path={alpha26_index_path or 'unavailable'}"
                )
            except Exception as exc:
                _restore_blob_watermark(watermarks, blob_name=blob_name, prior_signature=prior_signature)
                failed += 1
                mdc.write_error(f"Silver price-target alpha26 bucket write failed: {exc}")
                break
        elif status == "skipped_unchanged":
            skipped_unchanged += 1
            ok_or_skipped += 1
        elif status.startswith("skipped"):
            skipped_other += 1
            ok_or_skipped += 1
        else:
            failed += 1

    if failed == 0:
        if alpha26_staged_rows == 0:
            mdc.write_line("Silver price-target alpha26 bucket write skipped: no staged rows.")
        elif alpha26_flush_count == 0:
            failed += 1
            mdc.write_error("Silver price-target alpha26 bucket write blocked: staged rows were never flushed.")

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    if failed == 0:
        try:
            reconciliation_orphans, reconciliation_deleted_blobs = _run_price_target_reconciliation(
                bronze_blob_list=blob_list
            )
        except Exception as exc:
            reconciliation_failed = 1
            mdc.write_error(f"Silver price-target reconciliation failed: {exc}")
            mdc.write_line(
                "reconciliation_result layer=silver domain=price-target "
                "status=failed orphan_count=unknown deleted_blobs=unknown cutoff_rows_dropped=unknown"
            )

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver price target job complete: "
        f"ok_or_skipped={ok_or_skipped} skipped_unchanged={skipped_unchanged} skipped_other={skipped_other} "
        f"skipped_checkpoint={checkpoint_skipped} alpha26_staged_rows={alpha26_staged_rows} "
        f"alpha26_symbols={alpha26_written_symbols} "
        f"reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} failed={total_failed}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_price_target_data", watermarks)
    if total_failed == 0:
        save_last_success(
            "silver_price_target_data",
            metadata={
                "total_blobs": len(blob_list),
                "candidates": len(candidate_blobs),
                "ok_or_skipped": ok_or_skipped,
                "skipped_checkpoint": checkpoint_skipped,
                "forced_schema_migration": forced_schema_migration,
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

    job_name = "silver-price-target-job"
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="silver", domain="price-target", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
