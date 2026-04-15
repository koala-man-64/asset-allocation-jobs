from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
import os
import time
from typing import Any, Optional, Tuple

import pandas as pd
import json

from core import core as mdc
from core import delta_core
from tasks.finance_data import config as cfg
from tasks.finance_data.silver_frames import (
    _align_finance_frame_to_contract,
    _finance_row_identity_columns,
    _finance_sub_domain,
    _prepare_finance_delta_write_frame,
    _repair_symbol_column_aliases,
    _split_finance_bucket_rows,
)
from tasks.finance_data.silver_parsing import (
    _read_finance_json,
    _utc_today,
    resample_daily_ffill,
)
from asset_allocation_contracts.paths import DataPaths
from core import bronze_bucketing
from core import domain_artifacts
from core import layer_bucketing
from core.finance_contracts import (
    SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN,
    SILVER_FINANCE_REPORT_TYPE_TO_LAYOUT,
    SILVER_FINANCE_SUBDOMAINS,
)
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.watermarks import (
    build_blob_signature,
    load_last_success,
    load_watermarks,
    normalize_watermark_blob_name,
    save_last_success,
    save_watermarks,
    should_process_blob_since_last_success,
)
from tasks.common.silver_contracts import (
    ContractViolation,
    assert_no_unexpected_mixed_empty,
    coerce_to_naive_datetime,
    log_contract_violation,
    normalize_date_column,
    normalize_columns_to_snake_case,
    parse_wait_timeout_seconds,
    require_non_empty_frame,
)
from tasks.common.silver_precision import apply_precision_policy
from tasks.common.market_reconciliation import (
    collect_bronze_finance_symbols_from_blob_infos,
    collect_delta_silver_finance_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)


@dataclass(frozen=True)
class BlobProcessResult:
    blob_name: str
    silver_path: Optional[str]
    ticker: Optional[str]
    status: str  # ok|skipped|failed
    rows_written: Optional[int] = None
    error: Optional[str] = None
    reason: Optional[str] = None
    watermark_signature: Optional[dict[str, Optional[str]]] = None


@dataclass
class _FinanceAlpha26FlushState:
    staged_rows: int = 0
    flush_count: int = 0
    written_symbols: int = 0
    written_symbols_by_subdomain: Optional[dict[str, int]] = None
    index_path: Optional[str] = None
    column_count: Optional[int] = None
    cached_symbol_maps: Optional[dict[str, dict[str, str]]] = None
    index_recovery_source: Optional[str] = None


_ALPHA26_REPORT_TYPE_TO_TABLE: dict[str, tuple[str, str]] = dict(SILVER_FINANCE_REPORT_TYPE_TO_LAYOUT)
_DEFAULT_FINANCE_SHARED_LOCK = "finance-pipeline-shared"
_DEFAULT_SILVER_SHARED_LOCK_WAIT_SECONDS = 3600.0
_FINANCE_ALPHA26_SUBDOMAINS: Tuple[str, ...] = SILVER_FINANCE_SUBDOMAINS
_FINANCE_VALUATION_CALCULATED_COLUMNS = set(SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN["valuation"][2:])


def _list_alpha26_finance_bucket_candidates() -> tuple[list[dict], int]:
    listed_blobs = bronze_bucketing.list_active_bucket_blob_infos("finance", bronze_client)
    blobs = [
        blob
        for blob in listed_blobs
        if str(blob.get("name", "")).endswith(".parquet")
    ]
    blobs.sort(key=lambda item: str(item.get("name", "")))
    unsupported = max(0, len(listed_blobs) - len(blobs))
    if unsupported > 0:
        mdc.write_line(
            f"Silver finance alpha26 input filter removed {unsupported} unsupported Bronze blob candidate(s); "
            f"processing {len(blobs)} bucket inputs."
        )
    return blobs, 0


def _build_alpha26_checkpoint_candidates(
    *,
    blobs: list[dict],
    watermarks: dict,
    last_success: Optional[datetime],
    force_reprocess: bool = False,
) -> tuple[list[dict], int]:
    checkpoint_skipped = 0
    candidates: list[dict] = []
    for blob in blobs:
        prior = watermarks.get(normalize_watermark_blob_name(blob.get("name")))
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_reprocess,
        )
        if should_process:
            candidates.append(blob)
        else:
            checkpoint_skipped += 1
    return candidates, checkpoint_skipped


def _log_alpha26_blob_results(*, blob_name: str, results: list[BlobProcessResult]) -> None:
    ok_count = sum(1 for result in results if result.status == "ok")
    skipped_count = sum(1 for result in results if result.status == "skipped")
    no_data_results = [result for result in results if result.status == "skipped" and result.reason == "no_data"]
    failed_results = [result for result in results if result.status == "failed"]
    row_count = sum(int(result.rows_written or 0) for result in results if result.status == "ok")
    summary = (
        "Silver finance blob processed: "
        f"blob={blob_name} ok={ok_count} skipped={skipped_count} skippedNoData={len(no_data_results)} "
        f"failed={len(failed_results)} rows={row_count}"
    )
    no_data_preview: list[str] = []
    for result in no_data_results[:3]:
        ticker = result.ticker or "n/a"
        error = str(result.error or "no data").replace("\n", " ").strip()
        if len(error) > 160:
            error = error[:157] + "..."
        no_data_preview.append(f"{ticker}: {error}")
    if len(no_data_results) > 3:
        no_data_preview.append("...")

    if not failed_results:
        if no_data_preview:
            mdc.write_line(f"{summary} no_data={' | '.join(no_data_preview)}")
            return
        mdc.write_line(summary)
        return

    failure_preview: list[str] = []
    for result in failed_results[:3]:
        ticker = result.ticker or "n/a"
        error = str(result.error or "unknown error").replace("\n", " ").strip()
        if len(error) > 160:
            error = error[:157] + "..."
        failure_preview.append(f"{ticker}: {error}")
    if len(failed_results) > 3:
        failure_preview.append("...")
    log_fn = mdc.write_error if ok_count == 0 and skipped_count == 0 else mdc.write_warning
    message = f"{summary} failures={' | '.join(failure_preview)}"
    if no_data_preview:
        message = f"{message} no_data={' | '.join(no_data_preview)}"
    log_fn(message)


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


def _empty_finance_symbol_maps() -> dict[str, dict[str, str]]:
    return {sub_domain: {} for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS}


def _copy_finance_symbol_maps(symbol_maps: Optional[dict[str, dict[str, str]]]) -> dict[str, dict[str, str]]:
    out = _empty_finance_symbol_maps()
    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    for sub_domain, mapping in (symbol_maps or {}).items():
        normalized_sub_domain = layer_bucketing.normalize_sub_domain(sub_domain)
        if normalized_sub_domain not in out or not isinstance(mapping, dict):
            continue
        for symbol, bucket in mapping.items():
            clean_symbol = str(symbol or "").strip().upper()
            clean_bucket = str(bucket or "").strip().upper()
            if not clean_symbol or clean_bucket not in valid_buckets:
                continue
            out[normalized_sub_domain][clean_symbol] = clean_bucket
    return out


def _finance_symbol_maps_have_values(symbol_maps: Optional[dict[str, dict[str, str]]]) -> bool:
    return any(bool(mapping) for mapping in (symbol_maps or {}).values())


def _collect_frame_symbol_to_bucket_map(frame: Optional[pd.DataFrame], *, bucket: str) -> dict[str, str]:
    if frame is None or frame.empty or "symbol" not in frame.columns:
        return {}
    clean_bucket = str(bucket or "").strip().upper()
    if clean_bucket not in set(layer_bucketing.ALPHABET_BUCKETS):
        return {}
    out: dict[str, str] = {}
    for value in frame["symbol"].dropna().tolist():
        symbol = str(value or "").strip().upper()
        if symbol:
            out[symbol] = clean_bucket
    return out


def _load_existing_finance_symbol_maps() -> dict[str, dict[str, str]]:
    out = _empty_finance_symbol_maps()
    existing = layer_bucketing.load_layer_symbol_index(layer="silver", domain="finance")
    if existing is None or existing.empty:
        return out
    if "symbol" not in existing.columns or "bucket" not in existing.columns:
        return out

    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    normalized_sub = (
        existing["sub_domain"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace("-", "_", regex=False)
    )
    existing = existing.assign(_normalized_sub_domain=normalized_sub)
    for _, row in existing.iterrows():
        sub_domain = layer_bucketing.normalize_sub_domain(row.get("_normalized_sub_domain"))
        if not sub_domain or sub_domain not in out:
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        bucket = str(row.get("bucket") or "").strip().upper()
        if not symbol or bucket not in valid_buckets:
            continue
        out[sub_domain][symbol] = bucket
    return out


def _rebuild_finance_symbol_maps_from_storage() -> dict[str, dict[str, str]]:
    rebuilt = _empty_finance_symbol_maps()
    for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS:
        for bucket in layer_bucketing.ALPHABET_BUCKETS:
            silver_bucket_path = DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
            df_existing = delta_core.load_delta(
                cfg.AZURE_CONTAINER_SILVER,
                silver_bucket_path,
                columns=["symbol"],
            )
            if df_existing is None or df_existing.empty:
                continue
            rebuilt[sub_domain].update(_collect_frame_symbol_to_bucket_map(df_existing, bucket=bucket))
    return rebuilt


def _seed_finance_symbol_maps_from_staged_frames(
    bucket_frames: dict[tuple[str, str], list[pd.DataFrame]],
    *,
    selected_keys: set[tuple[str, str]],
) -> dict[str, dict[str, str]]:
    seeded = _empty_finance_symbol_maps()
    for sub_domain, bucket in sorted(selected_keys):
        for frame in bucket_frames.get((sub_domain, bucket), []):
            seeded[sub_domain].update(_collect_frame_symbol_to_bucket_map(frame, bucket=bucket))
    return seeded


def _resolve_existing_finance_symbol_maps(
    *,
    bucket_frames: dict[tuple[str, str], list[pd.DataFrame]],
    selected_keys: set[tuple[str, str]],
    recovery_state: Optional[_FinanceAlpha26FlushState] = None,
) -> tuple[dict[str, dict[str, str]], bool]:
    if recovery_state is not None and recovery_state.cached_symbol_maps is not None:
        return _copy_finance_symbol_maps(recovery_state.cached_symbol_maps), False

    existing = _load_existing_finance_symbol_maps()
    if _finance_symbol_maps_have_values(existing):
        if recovery_state is not None:
            recovery_state.cached_symbol_maps = _copy_finance_symbol_maps(existing)
            recovery_state.index_recovery_source = "shared-index"
        return existing, False

    rebuilt = _rebuild_finance_symbol_maps_from_storage()
    recovery_source: Optional[str] = None
    if _finance_symbol_maps_have_values(rebuilt):
        existing = rebuilt
        recovery_source = "persisted-silver-buckets"
    else:
        seeded = _seed_finance_symbol_maps_from_staged_frames(bucket_frames, selected_keys=selected_keys)
        if _finance_symbol_maps_have_values(seeded):
            existing = seeded
            recovery_source = "staged-frames"
        else:
            existing = _empty_finance_symbol_maps()

    if recovery_state is not None:
        recovery_state.cached_symbol_maps = _copy_finance_symbol_maps(existing)
        recovery_state.index_recovery_source = recovery_source

    if recovery_source is None:
        return existing, False

    recovered_symbols = sum(len(mapping) for mapping in existing.values())
    mdc.write_line(
        "Silver finance symbol index recovered: "
        f"source={recovery_source} symbols={recovered_symbols} touchedKeys={len(selected_keys)}"
    )
    return existing, True


def _write_alpha26_finance_silver_buckets(
    bucket_frames: dict[tuple[str, str], list[pd.DataFrame]],
    *,
    touched_bucket_keys: Optional[set[tuple[str, str]]] = None,
    recovery_state: Optional[_FinanceAlpha26FlushState] = None,
) -> tuple[int, Optional[str], Optional[int]]:
    valid_keys = {
        (sub_domain, str(bucket).strip().upper())
        for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
        for bucket in layer_bucketing.ALPHABET_BUCKETS
    }
    selected_keys = {
        (str(sub_domain).strip().lower().replace("-", "_"), str(bucket).strip().upper())
        for sub_domain, bucket in (touched_bucket_keys if touched_bucket_keys is not None else valid_keys)
        if str(sub_domain).strip() and str(bucket).strip()
    }
    if not selected_keys:
        return 0, None, None

    invalid_keys = selected_keys.difference(valid_keys)
    if invalid_keys:
        raise ValueError(f"Invalid alpha26 finance bucket key(s) for Silver write: {sorted(invalid_keys)}")

    is_partial_update = selected_keys != valid_keys
    existing_symbols_by_sub_domain = _empty_finance_symbol_maps()
    recovered_missing_index = False
    if is_partial_update:
        existing_symbols_by_sub_domain, recovered_missing_index = _resolve_existing_finance_symbol_maps(
            bucket_frames=bucket_frames,
            selected_keys=selected_keys,
            recovery_state=recovery_state,
        )

    touched_symbols_by_sub_domain = _empty_finance_symbol_maps()
    for sub_domain, bucket in sorted(selected_keys):
        silver_bucket_path = DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
        parts = bucket_frames.get((sub_domain, bucket), [])
        if parts:
            df_bucket = pd.concat(parts, ignore_index=True)
            if "symbol" in df_bucket.columns and "date" in df_bucket.columns:
                df_bucket["symbol"] = df_bucket["symbol"].astype(str).str.upper()
                df_bucket["date"] = coerce_to_naive_datetime(df_bucket["date"])
                df_bucket = df_bucket.dropna(subset=["symbol", "date"]).copy()
                identity_columns = _finance_row_identity_columns(df_bucket)
                df_bucket = df_bucket.sort_values(identity_columns).drop_duplicates(
                    subset=identity_columns,
                    keep="last",
                )
                for symbol in df_bucket["symbol"].dropna().astype(str).tolist():
                    if symbol:
                        touched_symbols_by_sub_domain[sub_domain][symbol] = bucket
            else:
                df_bucket = pd.DataFrame(columns=["date", "symbol"])
        else:
            df_bucket = pd.DataFrame(columns=["date", "symbol"])

        write_decision = _prepare_finance_delta_write_frame(
            df_bucket.reset_index(drop=True),
            sub_domain=sub_domain,
            path=silver_bucket_path,
            skip_empty_without_schema=True,
        )
        mdc.write_line(
            "delta_write_decision layer=silver domain=finance "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={silver_bucket_path}"
        )
        if write_decision.action == "skip_empty_no_schema":
            mdc.write_line(
                f"Skipping Silver finance empty bucket write for {silver_bucket_path}: no existing Delta schema."
            )
            continue

        delta_core.store_delta(
            write_decision.frame,
            cfg.AZURE_CONTAINER_SILVER,
            silver_bucket_path,
            mode="overwrite",
            schema_mode="overwrite",
        )
        try:
            domain_artifacts.write_bucket_artifact(
                layer="silver",
                domain="finance",
                sub_domain=sub_domain,
                bucket=bucket,
                df=write_decision.frame,
                date_column="date",
                client=silver_client,
                job_name="silver-finance-job",
            )
        except Exception as exc:
            mdc.write_warning(
                f"Silver finance metadata bucket artifact write failed sub_domain={sub_domain} bucket={bucket}: {exc}"
            )

    symbols_by_sub_domain: dict[str, dict[str, str]] = {}
    touched_sub_domains = {
        sub_domain
        for sub_domain, _bucket in selected_keys
    }
    if is_partial_update:
        touched_buckets_by_sub_domain: dict[str, set[str]] = {sub_domain: set() for sub_domain in touched_sub_domains}
        for sub_domain, bucket in selected_keys:
            touched_buckets_by_sub_domain.setdefault(sub_domain, set()).add(bucket)

        for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS:
            if sub_domain not in touched_sub_domains:
                symbols_by_sub_domain[sub_domain] = dict(existing_symbols_by_sub_domain.get(sub_domain, {}))
                continue
            symbols_by_sub_domain[sub_domain] = layer_bucketing.merge_symbol_to_bucket_map(
                existing_symbols_by_sub_domain.get(sub_domain, {}),
                touched_buckets=touched_buckets_by_sub_domain.get(sub_domain, set()),
                touched_symbol_to_bucket=touched_symbols_by_sub_domain.get(sub_domain, {}),
            )
    else:
        symbols_by_sub_domain = {
            sub_domain: dict(touched_symbols_by_sub_domain.get(sub_domain, {}))
            for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
        }

    symbol_to_bucket: dict[str, str] = {}
    for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS:
        symbol_to_bucket.update(symbols_by_sub_domain.get(sub_domain, {}))

    root_index_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="finance",
        symbol_to_bucket=symbol_to_bucket,
    )
    index_path = root_index_path
    column_count: Optional[int] = None
    finance_subdomain_artifacts: dict[str, dict[str, Any]] = {}
    sub_domains_to_write = (
        list(_FINANCE_ALPHA26_SUBDOMAINS)
        if recovered_missing_index
        else (sorted(touched_sub_domains) if is_partial_update else list(_FINANCE_ALPHA26_SUBDOMAINS))
    )
    for sub_domain in sub_domains_to_write:
        sub_index_path = layer_bucketing.write_layer_symbol_index(
            layer="silver",
            domain="finance",
            symbol_to_bucket=symbols_by_sub_domain.get(sub_domain, {}),
            sub_domain=sub_domain,
        )
        if sub_index_path:
            try:
                payload = domain_artifacts.write_domain_artifact(
                    layer="silver",
                    domain="finance",
                    sub_domain=sub_domain,
                    date_column="date",
                    client=silver_client,
                    symbol_count_override=len(symbols_by_sub_domain.get(sub_domain, {})),
                    symbol_index_path=sub_index_path,
                    job_name="silver-finance-job",
                )
                if payload is not None:
                    finance_subdomain_artifacts[sub_domain] = payload
            except Exception as exc:
                mdc.write_warning(
                    f"Silver finance metadata artifact write failed for sub_domain={sub_domain}: {exc}"
                )
            index_path = sub_index_path
    if is_partial_update:
        for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS:
            if sub_domain in finance_subdomain_artifacts:
                continue
            payload = domain_artifacts.load_domain_artifact(
                layer="silver",
                domain="finance",
                client=silver_client,
                sub_domain=sub_domain,
            )
            if payload is not None:
                finance_subdomain_artifacts[sub_domain] = payload
    if root_index_path:
        try:
            payload = domain_artifacts.write_domain_artifact(
                layer="silver",
                domain="finance",
                date_column="date",
                client=silver_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=root_index_path,
                job_name="silver-finance-job",
                finance_subdomains=finance_subdomain_artifacts,
            )
            column_count = domain_artifacts.extract_column_count(payload)
        except Exception as exc:
            mdc.write_warning(f"Silver finance metadata artifact write failed: {exc}")
    if recovery_state is not None:
        recovery_state.written_symbols_by_subdomain = {
            sub_domain: len(symbols_by_sub_domain.get(sub_domain, {}))
            for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
        }
        recovery_state.cached_symbol_maps = _copy_finance_symbol_maps(symbols_by_sub_domain)
        if recovery_state.index_recovery_source == "shared-index":
            recovery_state.index_recovery_source = None
    return len(symbol_to_bucket), index_path, column_count


def _flush_alpha26_finance_staged_frames(
    bucket_frames: dict[tuple[str, str], list[pd.DataFrame]],
    *,
    touched_bucket_keys: set[tuple[str, str]],
    flush_state: _FinanceAlpha26FlushState,
) -> None:
    staged_rows = layer_bucketing.count_staged_frame_rows(bucket_frames)
    if staged_rows == 0:
        return

    written_symbols, index_path, column_count = _write_alpha26_finance_silver_buckets(
        bucket_frames,
        touched_bucket_keys=touched_bucket_keys,
        recovery_state=flush_state,
    )
    flush_state.staged_rows += staged_rows
    flush_state.flush_count += 1
    flush_state.written_symbols = written_symbols
    flush_state.index_path = index_path
    flush_state.column_count = column_count


def _run_finance_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver finance reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_finance_symbols_from_blob_infos(bronze_blob_list)
    silver_symbols = collect_delta_silver_finance_symbols(client=silver_client)
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_silver_finance_bucket_path(sub_domain, layer_bucketing.bucket_letter(symbol))
            for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
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
            "Silver finance reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Silver finance reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Silver finance orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=[
            DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
            for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
            for bucket in layer_bucketing.ALPHABET_BUCKETS
        ],
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="silver finance reconciliation cutoff",
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
            "Silver finance reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(
            f"Silver finance reconciliation cutoff sweep encountered errors={cutoff_stats.errors}."
        )
    status = "failed" if cutoff_stats.errors > 0 else "ok"
    mdc.write_line(
        "reconciliation_result layer=silver domain=finance "
        f"status={status} orphan_count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
        f"cutoff_rows_dropped={cutoff_stats.rows_dropped} cutoff_tables_rewritten={cutoff_stats.tables_rewritten} "
        f"cutoff_errors={cutoff_stats.errors}"
    )
    return len(orphan_symbols), deleted_blobs


def _process_finance_frame(
    *,
    blob_name: str,
    ticker: str,
    folder_name: str,
    suffix: str,
    silver_path: str,
    df_raw: pd.DataFrame,
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp],
    signature: Optional[dict[str, Optional[str]]],
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[tuple[str, str], list[pd.DataFrame]]] = None,
) -> BlobProcessResult:
    is_optional_valuation = suffix == "quarterly_valuation_measures"
    sub_domain = _finance_sub_domain(folder_name)
    if df_raw is None or df_raw.empty:
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="skipped",
            error=None if is_optional_valuation else f"Empty finance payload: {blob_name}",
            reason=None if is_optional_valuation else "no_data",
        )

    df_clean = df_raw
    try:
        df_clean = require_non_empty_frame(df_clean, context=f"finance preflight {blob_name}")
        df_clean = normalize_date_column(
            df_clean,
            context=f"finance date parse {blob_name}",
            aliases=("Date", "date"),
            canonical="Date",
        )
        df_clean = assert_no_unexpected_mixed_empty(df_clean, context=f"finance date filter {blob_name}", alias="Date")
    except ContractViolation as exc:
        log_contract_violation(f"finance preflight failed for {blob_name}", exc, severity="ERROR")
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="failed",
            error=str(exc),
        )

    df_clean, _ = apply_backfill_start_cutoff(
        df_clean,
        date_col="Date",
        backfill_start=backfill_start,
        context=f"silver finance {ticker}",
    )
    existing_bucket = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, silver_path) if persist else None
    df_history, df_other_symbols = _split_finance_bucket_rows(existing_bucket, ticker=ticker)

    if backfill_start is not None and (df_clean is None or df_clean.empty):
        if not persist:
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                ticker=ticker,
                status="ok",
                rows_written=0,
            )
        if silver_client is not None:
            df_remaining = df_other_symbols.copy()
            if not df_remaining.empty:
                df_remaining = normalize_columns_to_snake_case(df_remaining)
                df_remaining = _repair_symbol_column_aliases(df_remaining, ticker=ticker)
                if "symbol" in df_remaining.columns:
                    df_remaining["symbol"] = df_remaining["symbol"].astype("string").str.upper()
            if df_remaining.empty:
                deleted = silver_client.delete_prefix(silver_path)
                mdc.write_line(
                    f"Silver finance backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                    f"deleted {deleted} blob(s) under {silver_path}."
                )
            else:
                df_remaining = _align_finance_frame_to_contract(
                    df_remaining.reset_index(drop=True),
                    sub_domain=sub_domain,
                    path=silver_path,
                )
                delta_core.store_delta(
                    df_remaining,
                    cfg.AZURE_CONTAINER_SILVER,
                    silver_path,
                    mode="overwrite",
                    schema_mode="overwrite",
                )
                delta_core.vacuum_delta_table(
                    cfg.AZURE_CONTAINER_SILVER,
                    silver_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
                mdc.write_line(f"Silver finance backfill purge for {ticker}: removed symbol rows from {silver_path}.")
            watermark_signature = None
            if signature:
                watermark_signature = dict(signature)
                watermark_signature["updated_at"] = datetime.now(timezone.utc).isoformat()
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                ticker=ticker,
                status="ok",
                rows_written=0,
                watermark_signature=watermark_signature,
            )
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="failed",
            error=f"Storage client unavailable for cutoff purge {silver_path}.",
        )

    df_clean = resample_daily_ffill(df_clean, extend_to=desired_end)
    if df_clean is None or df_clean.empty:
        return BlobProcessResult(
            blob_name=blob_name,
            silver_path=silver_path,
            ticker=ticker,
            status="skipped" if is_optional_valuation else "failed",
            error=None if is_optional_valuation else "No valid dated rows after cleaning/resample.",
        )

    df_clean = _align_finance_frame_to_contract(df_clean, sub_domain=sub_domain, path=silver_path)
    if not df_history.empty:
        df_history = _align_finance_frame_to_contract(df_history, sub_domain=sub_domain, path=silver_path)
        df_clean = pd.concat([df_history, df_clean], ignore_index=True)
    df_clean = normalize_columns_to_snake_case(df_clean)
    df_clean = _repair_symbol_column_aliases(df_clean, ticker=ticker)
    if "date" in df_clean.columns:
        df_clean["date"] = coerce_to_naive_datetime(df_clean["date"])
    if "symbol" in df_clean.columns:
        df_clean["symbol"] = df_clean["symbol"].astype("string").str.upper()
        identity_columns = _finance_row_identity_columns(df_clean)
        df_clean = df_clean.sort_values(identity_columns).drop_duplicates(subset=identity_columns, keep="last")
        df_clean = df_clean.reset_index(drop=True)
    applied_calculated_columns = set()
    if suffix == "quarterly_valuation_measures":
        applied_calculated_columns = {
            col for col in _FINANCE_VALUATION_CALCULATED_COLUMNS if col in df_clean.columns
        }
    df_clean = apply_precision_policy(
        df_clean,
        price_columns=set(),
        calculated_columns=applied_calculated_columns,
        price_scale=2,
        calculated_scale=4,
    )
    if not persist:
        if alpha26_bucket_frames is None:
            raise ValueError("alpha26_bucket_frames must be provided when persist=False.")
        bucket = layer_bucketing.bucket_letter(ticker)
        alpha26_bucket_frames.setdefault((sub_domain, bucket), []).append(df_clean.copy())
    else:
        df_other_symbols = normalize_columns_to_snake_case(df_other_symbols)
        df_other_symbols = _repair_symbol_column_aliases(df_other_symbols, ticker=ticker)
        if "date" in df_other_symbols.columns:
            df_other_symbols["date"] = coerce_to_naive_datetime(df_other_symbols["date"])
        if "symbol" in df_other_symbols.columns:
            df_other_symbols["symbol"] = df_other_symbols["symbol"].astype("string").str.upper()
        df_bucket_to_store = pd.concat([df_other_symbols, df_clean], ignore_index=True)
        if "symbol" in df_bucket_to_store.columns and "date" in df_bucket_to_store.columns:
            identity_columns = _finance_row_identity_columns(df_bucket_to_store)
            df_bucket_to_store = df_bucket_to_store.sort_values(identity_columns).drop_duplicates(
                subset=identity_columns,
                keep="last",
            )
        df_bucket_to_store = _align_finance_frame_to_contract(
            df_bucket_to_store.reset_index(drop=True),
            sub_domain=sub_domain,
            path=silver_path,
        )
        delta_core.store_delta(
            df_bucket_to_store,
            cfg.AZURE_CONTAINER_SILVER,
            silver_path,
            mode="overwrite",
            schema_mode="overwrite",
        )
        if backfill_start is not None:
            delta_core.vacuum_delta_table(
                cfg.AZURE_CONTAINER_SILVER,
                silver_path,
                retention_hours=0,
                dry_run=False,
                enforce_retention_duration=False,
                full=True,
            )
    mdc.write_line(
        "precision_policy_applied domain=finance "
        f"ticker={ticker} report_suffix={suffix} "
        f"price_cols=none calc_cols={','.join(sorted(applied_calculated_columns)) if applied_calculated_columns else 'none'} "
        f"rows={len(df_clean)}"
    )
    if persist:
        mdc.write_line(f"Updated Silver {silver_path} for ticker={ticker} rows={len(df_clean)}")
    watermark_signature = None
    if signature:
        watermark_signature = dict(signature)
        watermark_signature["updated_at"] = datetime.now(timezone.utc).isoformat()
    return BlobProcessResult(
        blob_name=blob_name,
        silver_path=silver_path,
        ticker=ticker,
        status="ok",
        rows_written=int(len(df_clean)),
        watermark_signature=watermark_signature,
    )


def process_alpha26_bucket_blob(
    blob: dict,
    *,
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp],
    watermarks: dict,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[tuple[str, str], list[pd.DataFrame]]] = None,
) -> list[BlobProcessResult]:
    blob_name = str(blob.get("name", ""))
    watermark_key = normalize_watermark_blob_name(blob_name)
    signature = build_blob_signature(blob)

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_bucket = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as exc:
        return [
            BlobProcessResult(
                blob_name=blob_name,
                silver_path=None,
                ticker=None,
                status="failed",
                error=f"Failed to read alpha26 bucket {blob_name}: {exc}",
            )
        ]

    if df_bucket is None or df_bucket.empty:
        if signature:
            signature["updated_at"] = datetime.now(timezone.utc).isoformat()
            watermarks[watermark_key] = signature
        return [BlobProcessResult(blob_name=blob_name, silver_path=None, ticker=None, status="skipped")]

    debug_symbols = set(getattr(cfg, "DEBUG_SYMBOLS", []) or [])
    results: list[BlobProcessResult] = []
    for _, row in df_bucket.iterrows():
        ticker = str(row.get("symbol") or "").strip().upper()
        report_type = str(row.get("report_type") or "").strip().lower()
        if not ticker or not report_type:
            continue
        if debug_symbols and ticker not in debug_symbols:
            continue
        mapped = _ALPHA26_REPORT_TYPE_TO_TABLE.get(report_type)
        if not mapped:
            results.append(
                BlobProcessResult(
                    blob_name=blob_name,
                    silver_path=None,
                    ticker=ticker,
                    status="failed",
                    error=f"Unsupported alpha26 report_type={report_type}",
                )
            )
            continue
        folder_name, suffix = mapped
        sub_domain = _finance_sub_domain(folder_name)
        bucket = layer_bucketing.bucket_letter(ticker)
        silver_path = DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
        payload_raw = row.get("payload_json")
        try:
            payload = json.loads(str(payload_raw))
        except Exception as exc:
            results.append(
                BlobProcessResult(
                    blob_name=blob_name,
                    silver_path=silver_path,
                    ticker=ticker,
                    status="failed",
                    error=f"Invalid payload_json for {ticker}/{report_type}: {exc}",
                )
            )
            continue
        try:
            raw_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            df_raw = _read_finance_json(raw_json, ticker=ticker, report_type=report_type)
            result = _process_finance_frame(
                blob_name=blob_name,
                ticker=ticker,
                folder_name=folder_name,
                suffix=suffix,
                silver_path=silver_path,
                df_raw=df_raw,
                desired_end=desired_end,
                backfill_start=backfill_start,
                signature=None,
                persist=persist,
                alpha26_bucket_frames=alpha26_bucket_frames,
            )
            results.append(result)
        except Exception as exc:
            results.append(
                BlobProcessResult(
                    blob_name=blob_name,
                    silver_path=silver_path,
                    ticker=ticker,
                    status="failed",
                    error=f"Failed alpha26 process for {ticker}/{report_type}: {exc}",
                )
            )

    if all(result.status != "failed" for result in results) and signature:
        signature["updated_at"] = datetime.now(timezone.utc).isoformat()
        watermarks[watermark_key] = signature
    return results or [BlobProcessResult(blob_name=blob_name, silver_path=None, ticker=None, status="skipped")]


def _process_alpha26_candidate_blobs(
    *,
    candidate_blobs: list[dict],
    desired_end: pd.Timestamp,
    backfill_start: Optional[pd.Timestamp],
    watermarks: dict,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[tuple[str, str], list[pd.DataFrame]]] = None,
    flush_state: Optional[_FinanceAlpha26FlushState] = None,
) -> tuple[list[BlobProcessResult], float]:
    ingest_started = time.perf_counter()
    results: list[BlobProcessResult] = []
    call_kwargs = {
        "desired_end": desired_end,
        "backfill_start": backfill_start,
        "watermarks": watermarks,
    }
    if (not persist) or (alpha26_bucket_frames is not None):
        call_kwargs["persist"] = persist
        call_kwargs["alpha26_bucket_frames"] = alpha26_bucket_frames
    for blob in candidate_blobs:
        blob_name = str(blob.get("name", ""))
        watermark_key = normalize_watermark_blob_name(blob_name)
        prior_signature = dict(watermarks[watermark_key]) if isinstance(watermarks.get(watermark_key), dict) else None
        blob_bucket_frames = (
            {}
            if flush_state is not None and not persist
            else alpha26_bucket_frames
        )
        if blob_bucket_frames is not None:
            call_kwargs["alpha26_bucket_frames"] = blob_bucket_frames
        blob_results = process_alpha26_bucket_blob(
            blob,
            **call_kwargs,
        )
        if flush_state is not None and not persist:
            has_failed = any(result.status == "failed" for result in blob_results)
            if not has_failed:
                touched_bucket_keys = {
                    key
                    for key, parts in (blob_bucket_frames or {}).items()
                    if any(frame is not None and len(frame) > 0 for frame in parts)
                }
                try:
                    _flush_alpha26_finance_staged_frames(
                        blob_bucket_frames or {},
                        touched_bucket_keys=touched_bucket_keys,
                        flush_state=flush_state,
                    )
                    if touched_bucket_keys:
                        mdc.write_line(
                            "Silver finance alpha26 buckets written: "
                            f"touched_keys={len(touched_bucket_keys)} symbols={flush_state.written_symbols} "
                            f"index_path={flush_state.index_path or 'unavailable'}"
                        )
                except Exception as exc:
                    _restore_blob_watermark(watermarks, blob_name=blob_name, prior_signature=prior_signature)
                    blob_results = [
                        BlobProcessResult(
                            blob_name=blob_name,
                            silver_path=None,
                            ticker=None,
                            status="failed",
                            error=f"Silver finance alpha26 bucket write failed: {exc}",
                        )
                    ]
        _log_alpha26_blob_results(blob_name=blob_name, results=blob_results)
        results.extend(blob_results)
        # Watermarks are updated per-bucket internally on all-success.
        for result in blob_results:
            if result.status == "ok" and result.watermark_signature:
                watermarks[normalize_watermark_blob_name(result.blob_name)] = result.watermark_signature
    ingest_elapsed = time.perf_counter() - ingest_started
    return results, ingest_elapsed


def main() -> int:
    mdc.log_environment_diagnostics()
    run_started_at = datetime.now(timezone.utc)
    watermarks = load_watermarks("bronze_finance_data")
    last_success = load_last_success("silver_finance_data")
    watermarks_dirty = False
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
    force_rebuild = layer_bucketing.silver_alpha26_force_rebuild()

    desired_end = _utc_today()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver finance data: {backfill_start.date().isoformat()}")
    mdc.write_line("Listing Bronze Finance files...")

    blob_list, _ = _list_alpha26_finance_bucket_candidates()
    candidate_blobs, checkpoint_skipped = _build_alpha26_checkpoint_candidates(
        blobs=blob_list,
        watermarks=watermarks,
        last_success=last_success,
        force_reprocess=force_rebuild,
    )
    if last_success is not None:
        mdc.write_line(
            "Silver finance checkpoint filter: "
            f"last_success={last_success.isoformat()} candidates={len(candidate_blobs)} "
            f"skipped_checkpoint={checkpoint_skipped}"
        )
    mdc.write_line(
        f"Found {len(blob_list)} files total; {len(candidate_blobs)} candidate files to process."
    )

    alpha26_flush_state = _FinanceAlpha26FlushState()
    all_results: list[BlobProcessResult] = []
    total_ingest_elapsed = 0.0
    if candidate_blobs:
        all_results, total_ingest_elapsed = _process_alpha26_candidate_blobs(
            candidate_blobs=candidate_blobs,
            desired_end=desired_end,
            backfill_start=backfill_start,
            watermarks=watermarks,
            persist=False,
            flush_state=alpha26_flush_state,
        )
        watermarks_dirty = True

    processed = sum(1 for r in all_results if r.status == "ok")
    skipped = sum(1 for r in all_results if r.status == "skipped")
    skipped_no_data = sum(1 for r in all_results if r.status == "skipped" and r.reason == "no_data")
    failed = sum(1 for r in all_results if r.status == "failed")
    attempts = len(all_results)
    distinct_tickers = len({str(r.ticker).strip() for r in all_results if r.ticker})
    rows_written = sum(int(r.rows_written or 0) for r in all_results if r.status == "ok")
    alpha26_staged_rows = alpha26_flush_state.staged_rows
    alpha26_written_symbols = alpha26_flush_state.written_symbols
    alpha26_written_symbols_by_subdomain = {
        sub_domain: int((alpha26_flush_state.written_symbols_by_subdomain or {}).get(sub_domain, 0))
        for sub_domain in _FINANCE_ALPHA26_SUBDOMAINS
    }
    alpha26_index_path: Optional[str] = alpha26_flush_state.index_path
    alpha26_column_count: Optional[int] = alpha26_flush_state.column_count
    if failed == 0:
        if alpha26_staged_rows == 0:
            mdc.write_line("Silver finance alpha26 bucket write skipped: no staged rows.")
        elif alpha26_flush_state.flush_count == 0:
            failed += 1
            mdc.write_error("Silver finance alpha26 bucket write blocked: staged rows were never flushed.")
    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "silver_finance_run_summary layer=silver domain=finance phase=alpha26 "
        f"subdomain_symbol_counts={alpha26_written_symbols_by_subdomain} "
        f"alpha26_symbols={alpha26_written_symbols} alpha26_staged_rows={alpha26_staged_rows}"
    )
    mdc.write_line(
        "Silver finance ingest complete: "
        f"attempts={attempts}, ok={processed}, skipped={skipped}, skippedNoData={skipped_no_data}, "
        f"failed={total_failed}, "
        f"skippedCheckpoint={checkpoint_skipped}, "
        f"distinctSymbols={distinct_tickers}, rowsWritten={rows_written}, alpha26StagedRows={alpha26_staged_rows}, "
        f"alpha26Symbols={alpha26_written_symbols}, "
        f"elapsedSec={total_ingest_elapsed:.2f}, "
        f"reconciled_orphans={reconciliation_orphans}, "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_finance_data", watermarks)

    run_ended_at = datetime.now(timezone.utc)
    if total_failed == 0:
        checkpoint_metadata = {
            "total_blobs": len(blob_list),
            "candidates": len(candidate_blobs),
            "attempts": attempts,
            "processed": processed,
            "skipped": skipped,
            "skipped_no_data": skipped_no_data,
            "failed": total_failed,
            "skipped_checkpoint": checkpoint_skipped,
            "rows_written": rows_written,
            "alpha26_staged_rows": alpha26_staged_rows,
            "alpha26_symbols": alpha26_written_symbols,
            "alpha26_index_path": alpha26_index_path,
            "column_count": alpha26_column_count,
            "elapsed_seconds": round(total_ingest_elapsed, 3),
            "run_started_at": run_started_at.isoformat(),
            "run_ended_at": run_ended_at.isoformat(),
            "reconciled_orphans": reconciliation_orphans,
            "reconciliation_deleted_blobs": reconciliation_deleted_blobs,
        }
        save_last_success(
            "silver_finance_data",
            when=run_ended_at,
            metadata=checkpoint_metadata,
        )
        return 0
    return 1


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "silver-finance-job"
    shared_lock_name = (os.environ.get("FINANCE_PIPELINE_SHARED_LOCK_NAME") or _DEFAULT_FINANCE_SHARED_LOCK).strip()
    shared_wait_timeout = parse_wait_timeout_seconds(
        os.environ.get("SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS"),
        default=_DEFAULT_SILVER_SHARED_LOCK_WAIT_SECONDS,
    )
    with mdc.JobLock(shared_lock_name, conflict_policy="wait_then_fail", wait_timeout_seconds=shared_wait_timeout):
        with mdc.JobLock(job_name, conflict_policy="fail"):
            ensure_api_awake_from_env(required=True)
            raise SystemExit(
                run_logged_job(
                    job_name=job_name,
                    run=main,
                    on_success=(
                        lambda: write_system_health_marker(layer="silver", domain="finance", job_name=job_name),
                        trigger_next_job_from_env,
                    ),
                )
            )
