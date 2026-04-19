
import pandas as pd
from datetime import UTC, datetime
from io import BytesIO
from typing import Optional

from tasks.market_data import config as cfg
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import delta_core
from asset_allocation_contracts.paths import DataPaths
from asset_allocation_runtime_common.market_data import bronze_bucketing
from asset_allocation_runtime_common.market_data import domain_artifacts
from asset_allocation_runtime_common.market_data import layer_bucketing
from tasks.common.backfill import (
    apply_backfill_start_cutoff,
    filter_by_date,
    get_backfill_range,
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
from tasks.common.delta_write_sanitizer import sanitize_delta_write_frame
from tasks.common.silver_contracts import normalize_columns_to_snake_case
from tasks.common.silver_precision import apply_precision_policy
from tasks.common.market_reconciliation import (
    collect_bronze_market_symbols_from_blob_infos,
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)

# Suppress warnings

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

_SUPPLEMENTAL_MARKET_COLUMNS = ("ShortInterest", "ShortVolume")
_CORPORATE_ACTION_MARKET_COLUMNS = ("DividendAmount", "SplitCoefficient")
_OPTIONAL_MARKET_COLUMNS = (*_SUPPLEMENTAL_MARKET_COLUMNS, *_CORPORATE_ACTION_MARKET_COLUMNS)
_REMOVED_MARKET_COLUMNS = ("FloatShares", "float_shares", "shares_float", "free_float", "float")
_INDEX_ARTIFACT_COLUMN_NAMES = {
    "index",
    "level_0",
    "index_level_0",
}
_MARKET_PRICE_COLUMNS = {"open", "high", "low", "close"}
_ALPHA26_MARKET_MIN_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "short_interest",
    "short_volume",
    "dividend_amount",
    "split_coefficient",
]
_ALPHA26_MARKET_NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "short_interest",
    "short_volume",
    "dividend_amount",
    "split_coefficient",
]
_BRONZE_TO_SILVER_REQUIRED_COLUMNS = {
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
}


def _empty_alpha26_market_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            "symbol": pd.Series(dtype="string"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "short_interest": pd.Series(dtype="float64"),
            "short_volume": pd.Series(dtype="float64"),
            "dividend_amount": pd.Series(dtype="float64"),
            "split_coefficient": pd.Series(dtype="float64"),
        }
    )


def _normalize_market_datetime_series(values) -> pd.Series:
    return pd.to_datetime(values, errors="coerce", utc=True).dt.tz_localize(None)


def _coerce_alpha26_market_bucket_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in _ALPHA26_MARKET_MIN_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    out["date"] = _normalize_market_datetime_series(out["date"])
    out["symbol"] = out["symbol"].astype("string").str.upper()
    for col in _ALPHA26_MARKET_NUMERIC_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["symbol", "date"]).copy()
    if out.empty:
        return _empty_alpha26_market_frame()

    out = out.sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"], keep="last")
    return out[_ALPHA26_MARKET_MIN_COLUMNS].reset_index(drop=True)


def _debug_symbol_scope() -> set[str]:
    return {
        str(symbol or "").strip().upper()
        for symbol in (getattr(cfg, "DEBUG_SYMBOLS", []) or [])
        if str(symbol or "").strip()
    }


def _merge_preserved_alpha26_market_bucket_symbols(
    *,
    bucket: str,
    df_bucket: pd.DataFrame,
    scoped_symbols: set[str],
) -> pd.DataFrame:
    normalized_bucket = _coerce_alpha26_market_bucket_frame(df_bucket)
    if not scoped_symbols:
        return normalized_bucket

    existing_bucket = _load_silver_market_bucket(DataPaths.get_silver_market_bucket_path(bucket))
    if existing_bucket is None or existing_bucket.empty:
        return normalized_bucket

    existing_bucket = _coerce_alpha26_market_bucket_frame(existing_bucket)
    if existing_bucket.empty:
        return normalized_bucket

    preserved = existing_bucket.loc[
        ~existing_bucket["symbol"].astype("string").str.upper().isin(scoped_symbols)
    ].copy()
    if preserved.empty:
        return normalized_bucket
    if normalized_bucket.empty:
        return preserved.reset_index(drop=True)
    return _coerce_alpha26_market_bucket_frame(pd.concat([preserved, normalized_bucket], ignore_index=True))


def _parse_alpha26_bucket_from_blob_name(blob_name: str) -> Optional[str]:
    return bronze_bucketing.parse_bucket_from_blob_name(blob_name, expected_prefix="market-data")


def _load_existing_silver_symbol_to_bucket_map() -> dict[str, str]:
    return layer_bucketing.load_layer_symbol_to_bucket_map(layer="silver", domain="market")


def _merge_symbol_to_bucket_map(
    existing: dict[str, str],
    *,
    touched_buckets: set[str],
    touched_symbol_to_bucket: dict[str, str],
) -> dict[str, str]:
    return layer_bucketing.merge_symbol_to_bucket_map(
        existing,
        touched_buckets=touched_buckets,
        touched_symbol_to_bucket=touched_symbol_to_bucket,
    )


def _read_bronze_market_bucket_bytes(blob_name: str) -> bytes:
    return mdc.read_raw_bytes(blob_name, client=bronze_client)


def _load_silver_market_bucket(path: str) -> pd.DataFrame | None:
    return delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path)


def _store_silver_market_bucket(df: pd.DataFrame, path: str) -> None:
    delta_core.store_delta(sanitize_delta_write_frame(df), cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite")


def _vacuum_silver_market_bucket(path: str) -> None:
    delta_core.vacuum_delta_table(
        cfg.AZURE_CONTAINER_SILVER,
        path,
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
        full=True,
    )


def _validate_bronze_to_silver_market_bucket_contract(df_bucket: pd.DataFrame, *, source_name: str) -> None:
    if df_bucket is None or df_bucket.empty:
        return

    normalized_cols = {_normalize_col_name(col): col for col in df_bucket.columns}
    missing = sorted(key for key in _BRONZE_TO_SILVER_REQUIRED_COLUMNS if key not in normalized_cols)
    if missing:
        raise ValueError(
            f"bronze_to_silver contract violation for {source_name}: missing required columns={missing}"
        )

    date_col = normalized_cols["date"]
    parsed_dates = _normalize_market_datetime_series(df_bucket[date_col]).dropna()
    if parsed_dates.empty:
        raise ValueError(
            f"bronze_to_silver contract violation for {source_name}: no parseable date values."
        )

    symbol_col = normalized_cols["symbol"]
    symbols = df_bucket[symbol_col].astype("string").str.strip().str.upper()
    if symbols.empty or symbols.eq("").all():
        raise ValueError(
            f"bronze_to_silver contract violation for {source_name}: no non-empty symbols."
        )


def _validate_silver_market_bucket_output_contract(df_bucket: pd.DataFrame, *, bucket: str) -> None:
    if df_bucket is None:
        raise ValueError(f"bronze_to_silver contract violation for bucket={bucket}: frame is None.")

    missing = [col for col in _ALPHA26_MARKET_MIN_COLUMNS if col not in df_bucket.columns]
    if missing:
        raise ValueError(
            f"bronze_to_silver contract violation for bucket={bucket}: missing output columns={missing}"
        )

    if df_bucket.empty:
        return

    parsed_dates = _normalize_market_datetime_series(df_bucket["date"])
    if parsed_dates.isna().any():
        raise ValueError(
            f"bronze_to_silver contract violation for bucket={bucket}: invalid date values in output frame."
        )

    symbols = df_bucket["symbol"].astype("string").str.strip().str.upper()
    if symbols.eq("").any():
        raise ValueError(
            f"bronze_to_silver contract violation for bucket={bucket}: blank symbols in output frame."
        )


def _normalize_col_name(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def _rename_market_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Normalize common OHLCV casing for defensive parsing.
    canonical_map = {
        "date": "Date",
        "timestamp": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "symbol": "Symbol",
    }
    rename_map = {src: dest for src, dest in canonical_map.items() if src in out.columns and dest not in out.columns}
    if rename_map:
        out = out.rename(columns=rename_map)

    # Normalize supplemental metric aliases from Bronze market payloads.
    supplemental_aliases = {
        "shortinterest": "ShortInterest",
        "shortinterestshares": "ShortInterest",
        "sharesshort": "ShortInterest",
        "shortvolume": "ShortVolume",
        "shortvolumeshares": "ShortVolume",
        "volumeshort": "ShortVolume",
        "dividendamount": "DividendAmount",
        "splitcoefficient": "SplitCoefficient",
    }
    normalized_cols = {_normalize_col_name(col): col for col in out.columns}
    alias_renames: dict[str, str] = {}
    for alias_key, canonical in supplemental_aliases.items():
        source_col = normalized_cols.get(alias_key)
        if source_col and source_col != canonical and canonical not in out.columns:
            alias_renames[source_col] = canonical
    if alias_renames:
        out = out.rename(columns=alias_renames)

    return out


def _drop_removed_market_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    to_drop = [col for col in _REMOVED_MARKET_COLUMNS if col in out.columns]
    if to_drop:
        out = out.drop(columns=to_drop)
    return out


def _ensure_numeric_market_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in ("Open", "High", "Low", "Close"):
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "Volume" not in out.columns:
        out["Volume"] = 0.0
    out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce")

    for col in _OPTIONAL_MARKET_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out


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
            f"Silver market {ticker}: renamed duplicate column {first_duplicate} -> symbol."
        )

    for col in duplicate_symbol_cols:
        if col not in out.columns:
            continue
        primary = out["symbol"].astype("string")
        fallback = out[col].astype("string")
        conflicts = int((primary.notna() & fallback.notna() & (primary != fallback)).sum())
        if conflicts > 0:
            mdc.write_warning(
                f"Silver market {ticker}: symbol repair conflict in {col}; "
                f"conflicting_rows={conflicts}; keeping existing symbol when both populated."
            )
        out["symbol"] = out["symbol"].combine_first(out[col])
        out = out.drop(columns=[col])
        mdc.write_warning(
            f"Silver market {ticker}: collapsed duplicate column {col} into symbol."
        )

    return out


def _drop_index_artifact_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    to_drop: list[str] = []
    for col in out.columns:
        normalized = str(col).strip().lower()
        if normalized in _INDEX_ARTIFACT_COLUMN_NAMES:
            to_drop.append(col)
            continue
        if normalized.startswith("unnamed_"):
            suffix = normalized[len("unnamed_") :]
            if suffix.replace("_", "").isdigit():
                to_drop.append(col)
                continue
        if normalized.startswith("index_level_"):
            suffix = normalized[len("index_level_") :]
            if suffix.replace("_", "").isdigit():
                to_drop.append(col)
                continue
    if to_drop:
        out = out.drop(columns=to_drop)
    return out


def _process_symbol_frame(
    *,
    ticker: str,
    df_new: pd.DataFrame,
    source_name: str,
    alpha26_bucket_frames: dict[str, list[pd.DataFrame]],
) -> str:
    bucket = layer_bucketing.bucket_letter(ticker)
    out = df_new.copy()
    out = out.drop(columns=["source_hash", "ingested_at"], errors="ignore")

    if "Adj Close" in out.columns:
        out = out.drop("Adj Close", axis=1)

    out = _rename_market_columns(out)
    if "Date" not in out.columns:
        mdc.write_error(f"Missing Date column in {source_name}; skipping {ticker}.")
        return "failed"

    out["Date"] = _normalize_market_datetime_series(out["Date"])
    out = out.dropna(subset=["Date"])
    if out.empty:
        return "skipped_empty"

    required_cols = ["Open", "High", "Low", "Close"]
    missing_cols = [col for col in required_cols if col not in out.columns]
    if missing_cols:
        mdc.write_error(f"Missing required columns in {source_name} for {ticker}: {missing_cols}")
        return "failed"

    out = _drop_removed_market_columns(out)
    out = _ensure_numeric_market_columns(out)

    backfill_start, backfill_end = get_backfill_range()
    out = filter_by_date(out, "Date", backfill_start, backfill_end)

    out["Symbol"] = ticker
    staged_frame = out.sort_values(by=["Date", "Symbol", "Volume"], ascending=[True, True, False])
    staged_frame = staged_frame.drop_duplicates(subset=["Date", "Symbol"], keep="last")
    staged_frame = staged_frame.reset_index(drop=True)

    staged_frame, _ = apply_backfill_start_cutoff(
        staged_frame,
        date_col="Date",
        backfill_start=backfill_start,
        context=f"silver market {ticker}",
    )

    staged_frame = _drop_removed_market_columns(staged_frame)
    staged_frame = _ensure_numeric_market_columns(staged_frame)
    cols_to_drop = [
        "index",
        "Beta (5Y Monthly)",
        "PE Ratio (TTM)",
        "1y Target Est",
        "EPS (TTM)",
        "Earnings Date",
        "Forward Dividend & Yield",
        "Market Cap",
    ]
    staged_frame = staged_frame.drop(columns=[c for c in cols_to_drop if c in staged_frame.columns])

    if backfill_start is not None and staged_frame.empty:
        return "ok"

    try:
        staged_frame = normalize_columns_to_snake_case(staged_frame)
        staged_frame = _repair_symbol_column_aliases(staged_frame, ticker=ticker)
        staged_frame = _drop_index_artifact_columns(staged_frame)
        staged_frame = apply_precision_policy(
            staged_frame,
            price_columns=_MARKET_PRICE_COLUMNS,
            calculated_columns=set(),
            price_scale=2,
            calculated_scale=4,
        )
        alpha26_bucket_frames.setdefault(bucket, []).append(staged_frame.copy())
        applied_price_cols = sorted(col for col in _MARKET_PRICE_COLUMNS if col in staged_frame.columns)
        price_cols_str = ",".join(applied_price_cols) if applied_price_cols else "none"
        mdc.write_line(
            "precision_policy_applied domain=market "
            f"ticker={ticker} price_cols={price_cols_str} calc_cols=none rows={len(staged_frame)}"
        )
    except Exception as exc:
        mdc.write_error(f"Failed to stage Silver market rows for {ticker}: {exc}")
        return "failed"

    return "ok"


def process_alpha26_bucket_blob(
    blob: dict,
    *,
    watermarks: dict,
    alpha26_bucket_frames: dict[str, list[pd.DataFrame]],
    force_reprocess: bool = False,
) -> str:
    blob_name = str(blob.get("name", ""))
    watermark_key = normalize_watermark_blob_name(blob_name)
    if not blob_name.endswith(".parquet"):
        return "skipped_non_parquet"

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(watermark_key))
    if unchanged and not force_reprocess:
        return "skipped_unchanged"

    try:
        raw_bytes = _read_bronze_market_bucket_bytes(blob_name)
        df_bucket = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as exc:
        mdc.write_error(f"Failed to read market alpha26 bucket {blob_name}: {exc}")
        mdc.write_line(
            f"layer_handoff_status transition=bronze_to_silver status=failed source={blob_name} reason=read_error"
        )
        return "failed"

    if df_bucket is None or df_bucket.empty:
        if signature:
            signature["updated_at"] = datetime.now(UTC).isoformat()
            watermarks[watermark_key] = signature
        bucket = _parse_alpha26_bucket_from_blob_name(blob_name) or "unknown"
        mdc.write_line(
            f"layer_handoff_status transition=bronze_to_silver status=ok source={blob_name} bucket={bucket} "
            "symbols_in=0 symbols_out=0 failures=0"
        )
        return "ok"

    try:
        _validate_bronze_to_silver_market_bucket_contract(df_bucket, source_name=blob_name)
    except Exception as exc:
        mdc.write_error(str(exc))
        bucket = _parse_alpha26_bucket_from_blob_name(blob_name) or "unknown"
        mdc.write_line(
            f"layer_handoff_status transition=bronze_to_silver status=failed source={blob_name} bucket={bucket} "
            "reason=contract_validation"
        )
        return "failed"

    symbol_col = "symbol" if "symbol" in df_bucket.columns else ("Symbol" if "Symbol" in df_bucket.columns else None)
    if symbol_col is None:
        mdc.write_error(f"Missing symbol column in market alpha26 bucket {blob_name}.")
        bucket = _parse_alpha26_bucket_from_blob_name(blob_name) or "unknown"
        mdc.write_line(
            f"layer_handoff_status transition=bronze_to_silver status=failed source={blob_name} bucket={bucket} "
            "reason=missing_symbol_column"
        )
        return "failed"

    debug_symbols = set(getattr(cfg, "DEBUG_SYMBOLS", []) or [])
    has_failed = False
    input_symbols = 0
    output_symbols = 0
    failed_symbols = 0
    for symbol, group in df_bucket.groupby(symbol_col):
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            continue
        if debug_symbols and ticker not in debug_symbols:
            continue
        input_symbols += 1
        status = _process_symbol_frame(
            ticker=ticker,
            df_new=group.copy(),
            source_name=blob_name,
            alpha26_bucket_frames=alpha26_bucket_frames,
        )
        if status == "failed":
            has_failed = True
            failed_symbols += 1
        elif status == "ok":
            output_symbols += 1

    if not has_failed and signature:
        signature["updated_at"] = datetime.now(UTC).isoformat()
        watermarks[watermark_key] = signature
    bucket = _parse_alpha26_bucket_from_blob_name(blob_name) or "unknown"
    status_text = "failed" if has_failed else "ok"
    mdc.write_line(
        f"layer_handoff_status transition=bronze_to_silver status={status_text} source={blob_name} bucket={bucket} "
        f"symbols_in={input_symbols} symbols_out={output_symbols} failures={failed_symbols}"
    )
    return status_text


def _write_alpha26_market_buckets(
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
        return 0, None

    invalid_buckets = selected_buckets.difference(valid_buckets)
    if invalid_buckets:
        raise ValueError(f"Invalid alpha26 bucket(s) for Silver write: {sorted(invalid_buckets)}")

    existing_symbol_to_bucket = _load_existing_silver_symbol_to_bucket_map()
    is_partial_update = selected_buckets != valid_buckets
    if is_partial_update and not existing_symbol_to_bucket:
        raise RuntimeError(
            "Silver market incremental alpha26 write blocked: existing silver market symbol index is missing."
        )

    touched_symbol_to_bucket: dict[str, str] = {}
    scoped_symbols = _debug_symbol_scope()
    for bucket in sorted(selected_buckets):
        silver_bucket_path = DataPaths.get_silver_market_bucket_path(bucket)
        parts = bucket_frames.get(bucket, [])
        if parts:
            df_bucket = pd.concat(parts, ignore_index=True)
        else:
            df_bucket = _empty_alpha26_market_frame()

        df_bucket = _merge_preserved_alpha26_market_bucket_symbols(
            bucket=bucket,
            df_bucket=df_bucket,
            scoped_symbols=scoped_symbols,
        )
        _validate_silver_market_bucket_output_contract(df_bucket, bucket=bucket)
        for symbol in df_bucket["symbol"].dropna().astype(str).tolist():
            if symbol:
                touched_symbol_to_bucket[symbol] = bucket
        write_decision = prepare_delta_write_frame(
            df_bucket.reset_index(drop=True),
            container=cfg.AZURE_CONTAINER_SILVER,
            path=silver_bucket_path,
            skip_empty_without_schema=False,
        )
        mdc.write_line(
            "delta_write_decision layer=silver domain=market "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={silver_bucket_path}"
        )
        _store_silver_market_bucket(write_decision.frame, silver_bucket_path)
        try:
            domain_artifacts.write_bucket_artifact(
                layer="silver",
                domain="market",
                bucket=bucket,
                df=write_decision.frame,
                date_column="date",
                client=silver_client,
                job_name="silver-market-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Silver market metadata bucket artifact write failed bucket={bucket}: {exc}")
        mdc.write_line(
            f"layer_handoff_status transition=bronze_to_silver status=ok bucket={bucket} "
            f"rows_out={len(df_bucket)} symbols_out={df_bucket['symbol'].nunique() if 'symbol' in df_bucket.columns else 0}"
        )

    symbol_to_bucket = _merge_symbol_to_bucket_map(
        existing_symbol_to_bucket,
        touched_buckets=selected_buckets,
        touched_symbol_to_bucket=touched_symbol_to_bucket,
    )
    index_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="market",
        symbol_to_bucket=symbol_to_bucket,
    )
    column_count: Optional[int] = len(_ALPHA26_MARKET_MIN_COLUMNS)
    if index_path:
        try:
            payload = domain_artifacts.write_domain_artifact(
                layer="silver",
                domain="market",
                date_column="date",
                client=silver_client,
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=index_path,
                job_name="silver-market-job",
            )
            column_count = domain_artifacts.extract_column_count(payload)
        except Exception as exc:
            mdc.write_warning(f"Silver market metadata artifact write failed: {exc}")
    return len(symbol_to_bucket), index_path, column_count


def _count_staged_bucket_rows(bucket_frames: dict[str, list[pd.DataFrame]]) -> int:
    return layer_bucketing.count_staged_frame_rows(bucket_frames)


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


def _detect_missing_alpha26_market_buckets() -> tuple[bool, set[str]]:
    if silver_client is None:
        return False, set()
    try:
        blob_infos = silver_client.list_blob_infos(name_starts_with="market-data/buckets/")
    except Exception as exc:
        mdc.write_warning(f"Silver market alpha26 bootstrap probe failed: {exc}")
        return False, set()

    present_buckets: set[str] = set()
    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    for blob in blob_infos:
        name = str(blob.get("name", "")).strip("/")
        parts = name.split("/")
        if len(parts) < 3:
            continue
        bucket = parts[2].strip().upper()
        if bucket in valid_buckets:
            present_buckets.add(bucket)

    missing = valid_buckets.difference(present_buckets)
    return bool(missing), missing


def _run_market_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver market reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_market_symbols_from_blob_infos(bronze_blob_list)
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="market-data")
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_silver_market_bucket_path(layer_bucketing.bucket_letter(symbol))
        ],
        load_table=_load_silver_market_bucket,
        store_table=_store_silver_market_bucket,
        delete_prefix=silver_client.delete_prefix,
        vacuum_table=_vacuum_silver_market_bucket,
    )
    deleted_blobs = purge_stats.deleted_blobs
    if orphan_symbols:
        mdc.write_line(
            "Silver market reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Silver market reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Silver market orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=layer_bucketing.all_silver_bucket_paths(domain="market"),
        load_table=_load_silver_market_bucket,
        store_table=_store_silver_market_bucket,
        delete_prefix=silver_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="silver market reconciliation cutoff",
        vacuum_table=_vacuum_silver_market_bucket,
    )
    if cutoff_stats.rows_dropped > 0 or cutoff_stats.tables_rewritten > 0 or cutoff_stats.deleted_blobs > 0:
        mdc.write_line(
            "Silver market reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(
            f"Silver market reconciliation cutoff sweep encountered errors={cutoff_stats.errors}."
        )
    status = "failed" if cutoff_stats.errors > 0 else "ok"
    mdc.write_line(
        "reconciliation_result layer=silver domain=market "
        f"status={status} orphan_count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
        f"cutoff_rows_dropped={cutoff_stats.rows_dropped} cutoff_tables_rewritten={cutoff_stats.tables_rewritten} "
        f"cutoff_errors={cutoff_stats.errors}"
    )
    return len(orphan_symbols), deleted_blobs


def main():
    mdc.log_environment_diagnostics()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver market data: {backfill_start.date().isoformat()}")
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
    force_rebuild = layer_bucketing.silver_alpha26_force_rebuild()
    
    mdc.write_line("Listing Bronze files...")
    watermarks = load_watermarks("bronze_market_data")
    last_success = load_last_success("silver_market_data")
    watermarks_dirty = False

    blob_list = bronze_bucketing.list_active_bucket_blob_infos("market", bronze_client)

    checkpoint_skipped = 0
    candidate_blobs: list[dict] = []
    bootstrap_missing, missing_buckets = _detect_missing_alpha26_market_buckets()
    force_checkpoint_rebuild = bool(force_rebuild or bootstrap_missing)
    if bootstrap_missing:
        missing_list = ",".join(sorted(missing_buckets))
        mdc.write_warning(
            "Silver market alpha26 bootstrap required: "
            f"missing_bucket_tables={missing_list}; forcing bronze replay."
        )
    for blob in blob_list:
        watermark_key = normalize_watermark_blob_name(blob.get("name"))
        prior = watermarks.get(watermark_key)
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_checkpoint_rebuild,
        )
        if should_process:
            candidate_blobs.append(blob)
        else:
            checkpoint_skipped += 1

    if last_success is not None:
        mdc.write_line(
            "Silver market checkpoint filter: "
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
    alpha26_column_count: Optional[int] = len(_ALPHA26_MARKET_MIN_COLUMNS)
    for blob in candidate_blobs:
        blob_name = str(blob.get("name", ""))
        watermark_key = normalize_watermark_blob_name(blob_name)
        prior_signature = dict(watermarks[watermark_key]) if isinstance(watermarks.get(watermark_key), dict) else None
        alpha26_bucket_frames: dict[str, list[pd.DataFrame]] = {}
        status = process_alpha26_bucket_blob(
            blob,
            watermarks=watermarks,
            alpha26_bucket_frames=alpha26_bucket_frames,
            force_reprocess=force_checkpoint_rebuild,
        )
        if status == "ok":
            processed += 1
            touched = _parse_alpha26_bucket_from_blob_name(str(blob.get("name", "")))
            staged_rows = _count_staged_bucket_rows(alpha26_bucket_frames)
            alpha26_staged_rows += staged_rows
            if staged_rows == 0:
                watermarks_dirty = True
                continue
            if not touched:
                _restore_blob_watermark(watermarks, blob_name=blob_name, prior_signature=prior_signature)
                failed += 1
                mdc.write_error(
                    f"Silver market alpha26 write failed: unable to resolve bucket from blob {blob.get('name')!r}."
                )
                break
            try:
                alpha26_written_symbols, alpha26_index_path, alpha26_column_count = _write_alpha26_market_buckets(
                    alpha26_bucket_frames,
                    touched_buckets={touched},
                )
                alpha26_flush_count += 1
                watermarks_dirty = True
                mdc.write_line(
                    "Silver market alpha26 buckets written: "
                    f"touched_buckets=1 symbols={alpha26_written_symbols} "
                    f"index_path={alpha26_index_path or 'unavailable'}"
                )
            except Exception as exc:
                _restore_blob_watermark(watermarks, blob_name=blob_name, prior_signature=prior_signature)
                failed += 1
                mdc.write_error(f"Silver market alpha26 bucket write failed: {exc}")
                break
        elif status == "skipped_unchanged":
            skipped_unchanged += 1
        elif status.startswith("skipped"):
            skipped_other += 1
        else:
            failed += 1

    if failed == 0:
        if alpha26_staged_rows == 0:
            mdc.write_line("Silver market alpha26 bucket write skipped: no staged rows.")
        elif alpha26_flush_count == 0:
            failed += 1
            mdc.write_error("Silver market alpha26 bucket write blocked: staged rows were never flushed.")

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    if failed == 0:
        try:
            reconciliation_orphans, reconciliation_deleted_blobs = _run_market_reconciliation(
                bronze_blob_list=blob_list
            )
        except Exception as exc:
            reconciliation_failed = 1
            mdc.write_error(f"Silver market reconciliation failed: {exc}")
            mdc.write_line(
                "reconciliation_result layer=silver domain=market "
                "status=failed orphan_count=unknown deleted_blobs=unknown cutoff_rows_dropped=unknown"
            )

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver market job complete: "
        f"processed={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_other={skipped_other} skipped_checkpoint={checkpoint_skipped} "
        f"alpha26_staged_rows={alpha26_staged_rows} "
        f"alpha26_symbols={alpha26_written_symbols} "
        f"reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} "
        f"failed={total_failed}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_market_data", watermarks)
    if total_failed == 0:
        save_last_success(
            "silver_market_data",
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

    job_name = "silver-market-job"
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="silver", domain="market", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
