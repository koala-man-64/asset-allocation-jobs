
import os
import asyncio
import pandas as pd
import nasdaqdatalink
import hashlib
from datetime import datetime, date, timedelta, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import symbol_availability
from tasks.price_target_data import config as cfg
from asset_allocation_runtime_common.market_data.pipeline import ListManager
from asset_allocation_runtime_common.market_data import bronze_bucketing
from tasks.common.bronze_alpha26_publish import publish_alpha26_bronze_domain
from tasks.common.bronze_observability import log_bronze_success, should_log_bronze_success
from tasks.common.bronze_symbol_policy import build_bronze_run_id
from tasks.common.job_status import resolve_job_run_status
from tasks.common.bronze_backfill_coverage import (
    extract_min_date_from_dataframe,
    load_coverage_marker,
    resolve_backfill_start_date,
    should_force_backfill,
    write_coverage_marker,
)

# Initialize Client
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(
    bronze_client,
    "price-target-data",
    auto_flush=False,
    allow_blacklist_updates=False,
)

BATCH_SIZE = 50
PRICE_TARGET_FULL_HISTORY_START_DATE = date(2020, 1, 1)
_COVERAGE_DOMAIN = "price-target"
_COVERAGE_PROVIDER = "nasdaq"
_BUCKET_COLUMNS = [
    "symbol",
    "obs_date",
    "tp_mean_est",
    "tp_std_dev_est",
    "tp_high_est",
    "tp_low_est",
    "tp_cnt_est",
    "tp_cnt_est_rev_up",
    "tp_cnt_est_rev_down",
    "ingested_at",
    "source_hash",
]


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_test_environment() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or _is_truthy(os.environ.get("TEST_MODE"))


def _truncate_trace_text(value: object, *, limit: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _format_failure_reason(exc: BaseException) -> str:
    reason_parts = [f"type={type(exc).__name__}"]
    message = str(exc).strip()
    if message:
        reason_parts.append(f"message={_truncate_trace_text(message, limit=220)}")
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = payload.get("path")
        if path:
            reason_parts.append(f"path={_truncate_trace_text(path, limit=96)}")
    return " ".join(reason_parts)


def _failure_bucket_key(exc: BaseException) -> str:
    key = f"type={type(exc).__name__}"
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = str(payload.get("path") or "").strip()
        if path:
            key += f" path={_truncate_trace_text(path, limit=80)}"
    return key


def _validate_environment() -> None:
    required = ["AZURE_CONTAINER_BRONZE", "NASDAQ_API_KEY"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing env vars: " + " ".join(missing))
    
    nasdaqdatalink.ApiConfig.api_key = os.environ.get('NASDAQ_API_KEY')

def _empty_coverage_summary() -> dict[str, int]:
    return {
        "coverage_checked": 0,
        "coverage_forced_refetch": 0,
        "coverage_marked_covered": 0,
        "coverage_marked_limited": 0,
        "coverage_skipped_limited_marker": 0,
    }


def _mark_coverage(
    *,
    symbol: str,
    backfill_start: date,
    status: str,
    earliest_available: Optional[date],
    summary: dict,
) -> None:
    try:
        write_coverage_marker(
            common_client=common_client,
            domain=_COVERAGE_DOMAIN,
            symbol=symbol,
            backfill_start=backfill_start,
            coverage_status=status,
            earliest_available=earliest_available,
            provider=_COVERAGE_PROVIDER,
        )
        key = "coverage_marked_covered" if status == "covered" else "coverage_marked_limited"
        summary[key] = int(summary.get(key, 0) or 0) + 1
    except Exception as exc:
        mdc.write_warning(f"Failed to write price-target coverage marker for {symbol}: {exc}")


def _load_existing_price_target_df(symbol: str) -> pd.DataFrame:
    blob_path = f"price-target-data/{symbol}.parquet"
    try:
        raw = mdc.read_raw_bytes(blob_path, client=bronze_client)
    except Exception:
        return pd.DataFrame()
    if not raw:
        return pd.DataFrame()
    try:
        return pd.read_parquet(BytesIO(raw))
    except Exception:
        return pd.DataFrame()


def _normalize_price_target_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _coerce_blob_last_modified(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _list_flat_price_target_blob_infos() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for blob in bronze_client.list_blob_infos(name_starts_with="price-target-data/"):
        name = str(blob.get("name") or "").strip()
        if not name.endswith(".parquet") or "/buckets/" in name:
            continue
        symbol = _normalize_price_target_symbol(name.rsplit("/", 1)[-1].removesuffix(".parquet"))
        if not symbol:
            continue
        out[symbol] = {
            "name": name,
            "last_modified": _coerce_blob_last_modified(blob.get("last_modified")),
        }
    return out


def _normalize_alpha26_existing_price_target_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if "symbol" not in out.columns or "obs_date" not in out.columns:
        return pd.DataFrame()
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    out["obs_date"] = pd.to_datetime(out["obs_date"], errors="coerce", utc=True).dt.tz_localize(None)
    out = out.dropna(subset=["symbol", "obs_date"]).copy()
    if out.empty:
        return pd.DataFrame()
    rename_map = {"symbol": "ticker"}
    out = out.rename(columns=rename_map)
    drop_columns = [column for column in ("ingested_at", "source_hash") if column in out.columns]
    if drop_columns:
        out = out.drop(columns=drop_columns)
    return out.reset_index(drop=True)


def _load_alpha26_existing_price_target_frames(*, symbols: set[str]) -> dict[str, pd.DataFrame]:
    if not symbols:
        return {}
    out: dict[str, pd.DataFrame] = {}
    touched_buckets = sorted({bronze_bucketing.bucket_letter(symbol) for symbol in symbols if symbol})
    for bucket in touched_buckets:
        try:
            bucket_df = bronze_bucketing.read_bucket_parquet(
                client=bronze_client,
                prefix="price-target-data",
                bucket=bucket,
            )
        except Exception:
            continue
        normalized = _normalize_alpha26_existing_price_target_frame(bucket_df)
        if normalized.empty or "ticker" not in normalized.columns:
            continue
        filtered = normalized[normalized["ticker"].isin(symbols)].copy()
        if filtered.empty:
            continue
        for symbol, group in filtered.groupby("ticker", sort=False):
            clean_symbol = _normalize_price_target_symbol(symbol)
            if not clean_symbol:
                continue
            out[clean_symbol] = group.reset_index(drop=True)
    return out


def _extract_max_obs_date(df: pd.DataFrame) -> Optional[date]:
    if df.empty or "obs_date" not in df.columns:
        return None
    parsed = pd.to_datetime(df["obs_date"], errors="coerce", utc=True).dropna()
    if parsed.empty:
        return None
    try:
        return parsed.max().date()
    except Exception:
        return None


def _extract_min_obs_date(df: pd.DataFrame) -> Optional[date]:
    return extract_min_date_from_dataframe(df, date_col="obs_date")


def _delete_price_target_blob_for_cutoff(
    symbol: str,
    *,
    min_date: date,
    summary: dict,
) -> None:
    blob_path = f"price-target-data/{symbol}.parquet"
    try:
        bronze_client.delete_file(blob_path)
        list_manager.add_to_whitelist(symbol)
        summary["deleted"] += 1
    except Exception as exc:
        mdc.write_error(f"Failed to delete cutoff Bronze {symbol}: {exc}")
        summary["save_failed"] += 1
    finally:
        summary["filtered_missing"] += 1
        mdc.write_line(
            f"No data for {symbol} on/after {min_date.strftime('%Y-%m-%d')}; deleted bronze {blob_path}."
        )


def _normalize_bucket_symbol_df(symbol: str, symbol_df: pd.DataFrame) -> pd.DataFrame:
    out = symbol_df.copy()
    out["symbol"] = str(symbol).upper()
    if "obs_date" not in out.columns:
        out["obs_date"] = pd.NaT
    out["obs_date"] = pd.to_datetime(out["obs_date"], errors="coerce", utc=True).dt.tz_localize(None)
    out = out.dropna(subset=["obs_date"]).copy()
    for col in [
        "tp_mean_est",
        "tp_std_dev_est",
        "tp_high_est",
        "tp_low_est",
        "tp_cnt_est",
        "tp_cnt_est_rev_up",
        "tp_cnt_est_rev_down",
    ]:
        if col not in out.columns:
            out[col] = pd.NA
    source_json = out.to_json(orient="records", date_format="iso")
    out["source_hash"] = hashlib.sha256(source_json.encode("utf-8")).hexdigest()
    out["ingested_at"] = datetime.now(timezone.utc).isoformat()
    out = out[_BUCKET_COLUMNS]
    return out.reset_index(drop=True)


def _write_alpha26_price_target_buckets(
    symbol_frames: Dict[str, pd.DataFrame],
    *,
    run_id: str,
) -> tuple[int, Optional[str]]:
    bucket_frames = bronze_bucketing.empty_bucket_frames(_BUCKET_COLUMNS)
    symbol_to_bucket: dict[str, str] = {}

    for symbol, frame in symbol_frames.items():
        if frame is None or frame.empty:
            continue
        normalized = _normalize_bucket_symbol_df(symbol, frame)
        if normalized.empty:
            continue
        bucket = bronze_bucketing.bucket_letter(symbol)
        symbol_to_bucket[str(symbol).upper()] = bucket
        if bucket_frames[bucket].empty:
            bucket_frames[bucket] = normalized
        else:
            bucket_frames[bucket] = pd.concat([bucket_frames[bucket], normalized], ignore_index=True)

    publish_result = publish_alpha26_bronze_domain(
        domain="price-target",
        root_prefix="price-target-data",
        bucket_frames=bucket_frames,
        bucket_columns=_BUCKET_COLUMNS,
        date_column="obs_date",
        symbol_to_bucket=symbol_to_bucket,
        storage_client=bronze_client,
        job_name="bronze-price-target-job",
        run_id=run_id,
    )
    log_bronze_success(
        domain="price-target",
        operation="metadata_artifacts_written",
        bucket_artifacts_written=publish_result.file_count,
        domain_artifact_written=True,
        symbol_index_path=publish_result.index_path or "n/a",
        manifest_path=publish_result.manifest_path or "n/a",
    )
    return publish_result.written_symbols, publish_result.index_path


def _delete_flat_symbol_blobs() -> int:
    deleted = 0
    for blob in bronze_client.list_blob_infos(name_starts_with="price-target-data/"):
        name = str(blob.get("name") or "")
        if not name.endswith(".parquet"):
            continue
        if "/buckets/" in name:
            continue
        try:
            bronze_client.delete_file(name)
            deleted += 1
        except Exception as exc:
            mdc.write_warning(f"Failed deleting flat price target blob {name}: {exc}")
    return deleted


async def process_batch_bronze(
    symbols: List[str],
    semaphore: asyncio.Semaphore,
    *,
    backfill_start: Optional[date] = None,
    write_symbol_files: bool = True,
    collected_symbol_frames: Optional[Dict[str, pd.DataFrame]] = None,
    alpha26_mode: bool = False,
    success_progress: Optional[Dict[str, int]] = None,
    flat_blob_infos: Optional[Dict[str, Dict[str, Any]]] = None,
    alpha26_existing_frames: Optional[Dict[str, pd.DataFrame]] = None,
) -> dict:
    batch_summary = {
        "requested": len(symbols),
        "stale": 0,
        "api_rows": 0,
        "saved": 0,
        "deleted": 0,
        "save_failed": 0,
        "filtered_missing": 0,
        "api_error": False,
    }
    batch_summary.update(_empty_coverage_summary())
    batch_failure_counts: Dict[str, int] = {}
    batch_failure_examples: Dict[str, str] = {}
    successful_symbols = 0

    def _record_batch_failure(scope: str, exc: BaseException) -> None:
        reason = _format_failure_reason(exc)
        key = f"scope={scope} {_failure_bucket_key(exc)}"
        batch_failure_counts[key] = batch_failure_counts.get(key, 0) + 1
        batch_failure_examples.setdefault(key, f"scope={scope} {reason}")

    def _log_symbol_processed_success(
        symbol: str,
        *,
        disposition: str,
        coverage_status: Optional[str] = None,
        row_count: Optional[int] = None,
    ) -> None:
        nonlocal successful_symbols
        if success_progress is None:
            successful_symbols += 1
            success_count = successful_symbols
        else:
            success_progress["count"] = int(success_progress.get("count", 0) or 0) + 1
            success_count = int(success_progress["count"])
        if not should_log_bronze_success(success_count):
            return
        log_bronze_success(
            domain="price-target",
            operation="symbol_processed",
            symbol=symbol,
            disposition=disposition,
            success_count=success_count,
            coverage_status=coverage_status,
            row_count=row_count,
        )

    async with semaphore:
        # Determine stale symbols and symbol-level incremental start windows.
        stale_symbols: List[str] = []
        symbol_start_dates: Dict[str, date] = {}
        symbol_has_existing_blob: Dict[str, bool] = {}
        existing_frames: Dict[str, pd.DataFrame] = {}
        symbol_force_backfill: Dict[str, bool] = {}
        symbol_existing_min: Dict[str, Optional[date]] = {}
        default_start_date = backfill_start or PRICE_TARGET_FULL_HISTORY_START_DATE

        for sym in symbols:
            normalized_symbol = _normalize_price_target_symbol(sym)
            if not normalized_symbol:
                continue
            if alpha26_mode:
                force_backfill = False
                existing_df = (
                    alpha26_existing_frames.get(normalized_symbol, pd.DataFrame()).copy()
                    if alpha26_existing_frames is not None
                    else pd.DataFrame()
                )
                if not existing_df.empty:
                    symbol_has_existing_blob[normalized_symbol] = True
                    existing_frames[normalized_symbol] = existing_df
                    if backfill_start is not None:
                        batch_summary["coverage_checked"] += 1
                        existing_min = _extract_min_obs_date(existing_df)
                        symbol_existing_min[normalized_symbol] = existing_min
                        marker = load_coverage_marker(
                            common_client=common_client,
                            domain=_COVERAGE_DOMAIN,
                            symbol=normalized_symbol,
                        )
                        force_backfill, skipped_limited_marker = should_force_backfill(
                            existing_min_date=existing_min,
                            backfill_start=backfill_start,
                            marker=marker,
                        )
                        if skipped_limited_marker:
                            batch_summary["coverage_skipped_limited_marker"] += 1
                        if force_backfill:
                            batch_summary["coverage_forced_refetch"] += 1
                        elif existing_min is not None and existing_min <= backfill_start:
                            _mark_coverage(
                                symbol=normalized_symbol,
                                backfill_start=backfill_start,
                                status="covered",
                                earliest_available=existing_min,
                                summary=batch_summary,
                            )
                    existing_max = _extract_max_obs_date(existing_df)
                    if existing_max is not None and not force_backfill:
                        symbol_start_dates[normalized_symbol] = existing_max + timedelta(days=1)
                symbol_force_backfill[normalized_symbol] = force_backfill
                if force_backfill and backfill_start is not None:
                    symbol_start_dates[normalized_symbol] = backfill_start
                elif normalized_symbol not in symbol_start_dates:
                    symbol_start_dates[normalized_symbol] = default_start_date
                stale_symbols.append(normalized_symbol)
                continue
            blob_path = f"price-target-data/{normalized_symbol}.parquet"
            force_backfill = False
            try:
                blob_info = (flat_blob_infos or {}).get(normalized_symbol)
                exists = blob_info is not None
                last_modified = _coerce_blob_last_modified((blob_info or {}).get("last_modified"))
                if not exists:
                    blob = bronze_client.get_blob_client(blob_path)
                    exists = bool(blob.exists())
                    if exists:
                        props = blob.get_blob_properties()
                        last_modified = _coerce_blob_last_modified(getattr(props, "last_modified", None))
                symbol_has_existing_blob[normalized_symbol] = exists
                if exists:
                    is_recent = (
                        last_modified is not None
                        and (datetime.now(timezone.utc) - last_modified).total_seconds() < 24 * 3600
                    )
                    if not is_recent or backfill_start is not None:
                        existing_df = _load_existing_price_target_df(normalized_symbol)
                        existing_frames[normalized_symbol] = existing_df
                    if backfill_start is not None:
                        batch_summary["coverage_checked"] += 1
                        existing_min = _extract_min_obs_date(existing_df)
                        symbol_existing_min[normalized_symbol] = existing_min
                        marker = load_coverage_marker(
                            common_client=common_client,
                            domain=_COVERAGE_DOMAIN,
                            symbol=normalized_symbol,
                        )
                        force_backfill, skipped_limited_marker = should_force_backfill(
                            existing_min_date=existing_min,
                            backfill_start=backfill_start,
                            marker=marker,
                        )
                        if skipped_limited_marker:
                            batch_summary["coverage_skipped_limited_marker"] += 1
                        if force_backfill:
                            batch_summary["coverage_forced_refetch"] += 1
                        elif existing_min is not None and existing_min <= backfill_start:
                            _mark_coverage(
                                symbol=normalized_symbol,
                                backfill_start=backfill_start,
                                status="covered",
                                earliest_available=existing_min,
                                summary=batch_summary,
                            )
                    if is_recent and not force_backfill:
                        continue
                    existing_max = _extract_max_obs_date(existing_df)
                    if existing_max is not None and not force_backfill:
                        symbol_start_dates[normalized_symbol] = existing_max + timedelta(days=1)
            except Exception:
                pass

            symbol_force_backfill[normalized_symbol] = force_backfill
            if force_backfill and backfill_start is not None:
                symbol_start_dates[normalized_symbol] = backfill_start
            elif normalized_symbol not in symbol_start_dates:
                symbol_start_dates[normalized_symbol] = default_start_date
            stale_symbols.append(normalized_symbol)

        batch_summary["stale"] = len(stale_symbols)
        if not stale_symbols:
            return batch_summary

        min_date = min(symbol_start_dates.get(sym, default_start_date) for sym in stale_symbols)

        loop = asyncio.get_event_loop()
        api_error_message = ""

        def fetch_api():
            nonlocal api_error_message
            try:
                tickers_str = ",".join(stale_symbols)
                return nasdaqdatalink.get_table(
                    "ZACKS/TP",
                    ticker=tickers_str,
                    obs_date={"gte": min_date.strftime("%Y-%m-%d")},
                )
            except Exception as e:
                api_error_message = str(e)
                _record_batch_failure("api_fetch", e)
                mdc.write_error(f"API Batch Error: {_format_failure_reason(e)}")
                return pd.DataFrame()

        mdc.write_line(f"Fetching {len(stale_symbols)} symbols from Nasdaq...")
        if _is_test_environment():
            # Avoid threadpool usage in test/sandbox environments.
            batch_df = fetch_api()
        else:
            batch_df = await loop.run_in_executor(None, fetch_api)

        if not batch_df.empty and "obs_date" in batch_df.columns:
            min_ts = pd.Timestamp(min_date)
            parsed_obs_date = pd.to_datetime(batch_df["obs_date"], errors="coerce", utc=True)
            batch_df = batch_df.copy()
            batch_df["obs_date"] = parsed_obs_date.dt.tz_localize(None)
            batch_df = batch_df.loc[batch_df["obs_date"].notna() & (batch_df["obs_date"] >= min_ts)].copy()

        if api_error_message:
            batch_summary["api_error"] = True

        grouped: Dict[str, pd.DataFrame] = {}
        if not batch_df.empty:
            batch_summary["api_rows"] = int(len(batch_df))
            for symbol, group_df in batch_df.groupby("ticker"):
                grouped[str(symbol)] = group_df.copy()
        elif stale_symbols and not api_error_message:
            if backfill_start is None:
                mdc.write_warning(
                    f"Nasdaq batch returned no rows for stale symbols (count={len(stale_symbols)})."
                )

        for sym in stale_symbols:
            symbol_min = symbol_start_dates.get(sym, default_start_date)
            force_backfill = bool(symbol_force_backfill.get(sym))
            symbol_df = grouped.get(sym, pd.DataFrame()).copy()
            existing_df = existing_frames.get(sym)
            has_existing_rows = existing_df is not None and not existing_df.empty
            coverage_status: Optional[str] = None
            if not symbol_df.empty and "obs_date" in symbol_df.columns:
                symbol_df = symbol_df.loc[symbol_df["obs_date"] >= pd.Timestamp(symbol_min)].copy()

            if symbol_df.empty:
                if backfill_start is not None and force_backfill:
                    if alpha26_mode and has_existing_rows and collected_symbol_frames is not None:
                        collected_symbol_frames[sym] = existing_df.copy()
                    _mark_coverage(
                        symbol=sym,
                        backfill_start=backfill_start,
                        status="limited",
                        earliest_available=symbol_existing_min.get(sym),
                        summary=batch_summary,
                    )
                    batch_summary["filtered_missing"] += 1
                    list_manager.add_to_whitelist(sym)
                    _log_symbol_processed_success(
                        sym,
                        disposition="limited_no_rows",
                        coverage_status="limited",
                    )
                    continue
                if has_existing_rows:
                    if alpha26_mode and collected_symbol_frames is not None:
                        collected_symbol_frames[sym] = existing_df.copy()
                    list_manager.add_to_whitelist(sym)
                    _log_symbol_processed_success(sym, disposition="skipped_no_new_rows")
                    continue
                if backfill_start is not None:
                    failures_before_delete = int(batch_summary.get("save_failed", 0) or 0)
                    _delete_price_target_blob_for_cutoff(sym, min_date=symbol_min, summary=batch_summary)
                    if int(batch_summary.get("save_failed", 0) or 0) == failures_before_delete:
                        _log_symbol_processed_success(
                            sym,
                            disposition="deleted_cutoff",
                        )
                    continue
                batch_summary["filtered_missing"] += 1
                # Incremental no-op: no rows newer than the symbol-level watermark.
                if bool(symbol_has_existing_blob.get(sym)) or symbol_min > PRICE_TARGET_FULL_HISTORY_START_DATE:
                    list_manager.add_to_whitelist(sym)
                    _log_symbol_processed_success(sym, disposition="skipped_no_new_rows")
                    continue
                mdc.write_line(f"Bronze price target coverage unavailable for {sym}; no rows returned.")
                continue

            try:
                if existing_df is None and bool(symbol_has_existing_blob.get(sym)):
                    existing_df = _load_existing_price_target_df(sym)
                if existing_df is not None and not existing_df.empty:
                    symbol_df = pd.concat([existing_df, symbol_df], ignore_index=True, sort=False)
                    symbol_df = symbol_df.drop_duplicates().reset_index(drop=True)
                if "obs_date" in symbol_df.columns:
                    symbol_df = symbol_df.sort_values("obs_date").reset_index(drop=True)

                if backfill_start is not None and (force_backfill or symbol_min <= backfill_start):
                    earliest_available = _extract_min_obs_date(symbol_df)
                    coverage_status = (
                        "covered"
                        if earliest_available is not None and earliest_available <= backfill_start
                        else "limited"
                    )
                    _mark_coverage(
                        symbol=sym,
                        backfill_start=backfill_start,
                        status=coverage_status,
                        earliest_available=earliest_available,
                        summary=batch_summary,
                    )

                if write_symbol_files:
                    raw_parquet = symbol_df.to_parquet(index=False)
                    mdc.store_raw_bytes(raw_parquet, f"price-target-data/{sym}.parquet", client=bronze_client)
                elif collected_symbol_frames is not None:
                    collected_symbol_frames[sym] = symbol_df.copy()
                list_manager.add_to_whitelist(sym)
                batch_summary["saved"] += 1
                _log_symbol_processed_success(
                    sym,
                    disposition="written" if write_symbol_files else "collected",
                    coverage_status=coverage_status,
                    row_count=len(symbol_df.index),
                )
            except Exception as e:
                _record_batch_failure(f"symbol={sym}", e)
                mdc.write_error(f"Failed to save {sym}: {_format_failure_reason(e)}")
                batch_summary["save_failed"] += 1

        if batch_failure_counts:
            ordered = sorted(batch_failure_counts.items(), key=lambda item: item[1], reverse=True)
            summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
            mdc.write_warning(f"Bronze price target batch failure summary: {summary}")
            for name, _ in ordered[:3]:
                example = batch_failure_examples.get(name)
                if example:
                    mdc.write_warning(f"Bronze price target batch failure example ({name}): {example}")

        mdc.write_line(
            "Bronze price target batch summary: requested={requested} stale={stale} api_rows={api_rows} "
            "saved={saved} deleted={deleted} save_failed={save_failed} filtered_missing={filtered_missing} "
            "api_error={api_error}".format(**batch_summary)
        )
        batch_summary["failure_counts"] = dict(batch_failure_counts)
        batch_summary["failure_examples"] = dict(batch_failure_examples)
        return batch_summary

async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()
    
    list_manager.load()
    backfill_start = resolve_backfill_start_date()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to bronze price-target data: {backfill_start.isoformat()}")

    sync_result = symbol_availability.sync_domain_availability("price-target")
    mdc.write_line(
        "Bronze price-target availability sync: "
        f"provider={sync_result.provider} listed_count={sync_result.listed_count} "
        f"inserted_count={sync_result.inserted_count} disabled_count={sync_result.disabled_count} "
        f"duration_ms={sync_result.duration_ms} lock_wait_ms={sync_result.lock_wait_ms}"
    )
    df_symbols = symbol_availability.get_domain_symbols("price-target").dropna(subset=["Symbol"])
    provider_available_count = int(len(df_symbols))

    symbols = []
    blacklist_skipped = 0
    for _, row in df_symbols.iterrows():
        sym = row['Symbol']
        if pd.isna(sym) or not isinstance(sym, str):
            continue
        if '.' in sym:
            continue
        if list_manager.is_blacklisted(sym):
            blacklist_skipped += 1
            continue
        symbols.append(sym)

    debug_filtered = 0
    if cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        filtered_symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        debug_filtered = len(symbols) - len(filtered_symbols)
        symbols = filtered_symbols

    mdc.write_line(
        "Bronze price-target symbol selection: "
        f"provider_available_count={provider_available_count} "
        f"blacklist_skipped={blacklist_skipped} "
        f"debug_filtered={debug_filtered} "
        f"final_scheduled={len(symbols)}"
    )

    alpha26_mode = bronze_bucketing.is_alpha26_mode()
    run_id = build_bronze_run_id(_COVERAGE_DOMAIN)
    chunked_symbols = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    semaphore = asyncio.Semaphore(3)
    success_progress = {"count": 0}
    normalized_scheduled_symbols = {_normalize_price_target_symbol(symbol) for symbol in symbols if _normalize_price_target_symbol(symbol)}
    flat_blob_infos = None if alpha26_mode else _list_flat_price_target_blob_infos()
    alpha26_existing_frames = (
        _load_alpha26_existing_price_target_frames(symbols=normalized_scheduled_symbols) if alpha26_mode else None
    )

    bucket_symbol_frames: Dict[str, pd.DataFrame] = {}
    mdc.write_line(f"Starting Bronze Price Target Ingestion for {len(symbols)} symbols...")
    tasks = [
        process_batch_bronze(
            chunk,
            semaphore,
            backfill_start=backfill_start,
            write_symbol_files=not alpha26_mode,
            collected_symbol_frames=bucket_symbol_frames if alpha26_mode else None,
            alpha26_mode=alpha26_mode,
            success_progress=success_progress,
            flat_blob_infos=flat_blob_infos,
            alpha26_existing_frames=alpha26_existing_frames,
        )
        for chunk in chunked_symbols
    ]
    batch_exception_count = 0
    failure_counts: Dict[str, int] = {}
    failure_examples: Dict[str, str] = {}
    aggregate = {
        "requested": 0,
        "stale": 0,
        "api_rows": 0,
        "saved": 0,
        "deleted": 0,
        "save_failed": 0,
        "filtered_missing": 0,
        "api_error_batches": 0,
        "coverage_checked": 0,
        "coverage_forced_refetch": 0,
        "coverage_marked_covered": 0,
        "coverage_marked_limited": 0,
        "coverage_skipped_limited_marker": 0,
    }
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                batch_exception_count += 1
                failure_key = f"scope=batch_idx={idx} {_failure_bucket_key(result)}"
                failure_counts[failure_key] = failure_counts.get(failure_key, 0) + 1
                failure_examples.setdefault(
                    failure_key,
                    f"scope=batch_idx={idx} {_format_failure_reason(result)}",
                )
                mdc.write_error(
                    f"Bronze price target batch exception idx={idx}: {_format_failure_reason(result)}"
                )
                continue
            if not isinstance(result, dict):
                continue
            aggregate["requested"] += int(result.get("requested", 0) or 0)
            aggregate["stale"] += int(result.get("stale", 0) or 0)
            aggregate["api_rows"] += int(result.get("api_rows", 0) or 0)
            aggregate["saved"] += int(result.get("saved", 0) or 0)
            aggregate["deleted"] += int(result.get("deleted", 0) or 0)
            aggregate["save_failed"] += int(result.get("save_failed", 0) or 0)
            aggregate["filtered_missing"] += int(result.get("filtered_missing", 0) or 0)
            aggregate["coverage_checked"] += int(result.get("coverage_checked", 0) or 0)
            aggregate["coverage_forced_refetch"] += int(result.get("coverage_forced_refetch", 0) or 0)
            aggregate["coverage_marked_covered"] += int(result.get("coverage_marked_covered", 0) or 0)
            aggregate["coverage_marked_limited"] += int(result.get("coverage_marked_limited", 0) or 0)
            aggregate["coverage_skipped_limited_marker"] += int(
                result.get("coverage_skipped_limited_marker", 0) or 0
            )
            if bool(result.get("api_error", False)):
                aggregate["api_error_batches"] += 1
            for key, count in dict(result.get("failure_counts", {}) or {}).items():
                try:
                    delta = int(count or 0)
                except Exception:
                    delta = 0
                if delta <= 0:
                    continue
                failure_counts[str(key)] = failure_counts.get(str(key), 0) + delta
            for key, example in dict(result.get("failure_examples", {}) or {}).items():
                if not example:
                    continue
                failure_examples.setdefault(str(key), str(example))
    finally:
        alpha26_written_symbols = 0
        alpha26_index_path: Optional[str] = None
        flat_deleted = 0
        alpha26_publish_succeeded = not alpha26_mode
        if alpha26_mode:
            try:
                alpha26_written_symbols, alpha26_index_path = _write_alpha26_price_target_buckets(
                    bucket_symbol_frames,
                    run_id=run_id,
                )
                alpha26_publish_succeeded = True
                flat_deleted = _delete_flat_symbol_blobs()
                mdc.write_line(
                    "Bronze price-target alpha26 buckets written: "
                    f"symbols={alpha26_written_symbols} index={alpha26_index_path or 'n/a'} "
                    f"flat_deleted={flat_deleted}"
                )
            except Exception as exc:
                batch_exception_count += 1
                mdc.write_error(f"Bronze price-target alpha26 bucket write failed: {exc}")
        if alpha26_publish_succeeded:
            try:
                list_manager.flush()
            except Exception as exc:
                mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")
            else:
                log_bronze_success(domain="price-target", operation="list_flush")
        job_status, exit_code = resolve_job_run_status(
            failed_count=(
                batch_exception_count
                + int(aggregate.get("save_failed", 0) or 0)
                + int(aggregate.get("api_error_batches", 0) or 0)
            ),
            warning_count=0,
        )
        mdc.write_line(
            "Bronze price target overall summary: requested={requested} stale={stale} api_rows={api_rows} "
            "saved={saved} deleted={deleted} save_failed={save_failed} filtered_missing={filtered_missing} "
            "coverage_checked={coverage_checked} coverage_forced_refetch={coverage_forced_refetch} "
            "coverage_marked_covered={coverage_marked_covered} coverage_marked_limited={coverage_marked_limited} "
            "coverage_skipped_limited_marker={coverage_skipped_limited_marker} "
            "api_error_batches={api_error_batches} "
            "batch_exceptions={batch_exception_count} job_status={job_status}".format(
                batch_exception_count=batch_exception_count,
                job_status=job_status,
                **aggregate,
            )
        )
        if failure_counts:
            ordered = sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)
            summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
            mdc.write_warning(f"Bronze price target failure summary: {summary}")
            for name, _ in ordered[:3]:
                example = failure_examples.get(name)
                if example:
                    mdc.write_warning(f"Bronze price target failure example ({name}): {example}")
        mdc.write_line("Bronze Ingestion Complete.")
    return exit_code


def main() -> int:
    return asyncio.run(main_async())

if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-price-target-job"
    with mdc.JobLock("nasdaq", conflict_policy="wait_then_fail", wait_timeout_seconds=None):
        with mdc.JobLock(job_name, conflict_policy="fail"):
            ensure_api_awake_from_env(required=True)
            raise SystemExit(
                run_logged_job(
                    job_name=job_name,
                    run=main,
                    on_success=(
                        lambda: write_system_health_marker(layer="bronze", domain="price-target", job_name=job_name),
                        trigger_next_job_from_env,
                    ),
                )
            )
