from __future__ import annotations

import asyncio
import json
import os
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from io import StringIO
from typing import Any, Callable, Optional, Dict, Sequence

import pandas as pd

from asset_allocation_runtime_common.providers.alpha_vantage_gateway_client import (
    AlphaVantageGatewayClient,
    AlphaVantageGatewayError,
    AlphaVantageGatewayInvalidSymbolError,
    AlphaVantageGatewayThrottleError,
    AlphaVantageGatewayUnavailableError,
)
from asset_allocation_runtime_common.market_data import symbol_availability
from asset_allocation_runtime_common.foundation import config as cfg
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data.pipeline import ListManager
from asset_allocation_runtime_common.market_data import bronze_bucketing
from tasks.common.bronze_alpha26_publish import publish_alpha26_bronze_domain
from tasks.common.bronze_observability import log_bronze_success, should_log_bronze_success
from tasks.common.bronze_symbol_policy import (
    BronzeCoverageUnavailableError,
    build_bronze_run_id,
    clear_invalid_candidate_marker,
    list_promoted_invalid_candidate_markers,
    record_promoted_symbol_reprobe_attempt,
    record_invalid_symbol_candidate,
    validate_bronze_storage_clients,
)
from tasks.common.job_status import resolve_job_run_status
from tasks.common.bronze_backfill_coverage import (
    extract_min_date_from_rows,
    normalize_date,
    resolve_backfill_start_date,
    write_coverage_marker,
)
from tasks.common.backfill import filter_by_date
from tasks.common.silver_contracts import normalize_columns_to_snake_case


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(
    bronze_client,
    cfg.EARNINGS_DATA_PREFIX,
    auto_flush=False,
    allow_blacklist_updates=False,
)


_COVERAGE_DOMAIN = "earnings"
_COVERAGE_PROVIDER = "alpha-vantage"
_INVALID_CANDIDATE_REASON = "provider_invalid_symbol"
_PROMOTED_REPROBE_LIMIT = 25
_EARNINGS_CALENDAR_HORIZONS = frozenset({"3month", "6month", "12month"})
_EARNINGS_CALENDAR_EXPECTED_COLUMNS = (
    "symbol",
    "name",
    "reportDate",
    "fiscalDateEnding",
    "estimate",
    "currency",
    "timeOfTheDay",
)
_CANONICAL_EARNINGS_COLUMNS = [
    "symbol",
    "date",
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
_BUCKET_COLUMNS = [
    *_CANONICAL_EARNINGS_COLUMNS,
    "ingested_at",
    "source_hash",
]


def _empty_coverage_summary() -> dict[str, int]:
    return {
        "coverage_checked": 0,
        "coverage_forced_refetch": 0,
        "coverage_marked_covered": 0,
        "coverage_marked_limited": 0,
        "coverage_skipped_limited_marker": 0,
    }


def _empty_event_summary() -> dict[str, int]:
    return {
        "scheduled_rows_retained": 0,
        "actual_over_scheduled_replacements": 0,
    }


def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


def _empty_canonical_earnings_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_BUCKET_COLUMNS)


def _normalize_calendar_horizon(value: object) -> str:
    text = str(value or "").strip().lower() or "12month"
    if text not in _EARNINGS_CALENDAR_HORIZONS:
        raise ValueError(
            f"Invalid ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON={value!r}; expected one of 3month, 6month, 12month."
        )
    return text


def _format_payload_preview(payload: Any, *, max_chars: int = 500) -> Optional[str]:
    if payload is None:
        return None
    try:
        if isinstance(payload, (dict, list, tuple)):
            text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        else:
            text = str(payload)
    except Exception:
        try:
            text = repr(payload)
        except Exception:
            return None
    text = str(text).replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return None
    if len(text) > max_chars:
        return text[:max_chars] + "..."
    return text


def _format_invalid_candidate_warning(symbol: str, exc: BaseException, *, promoted: bool) -> str:
    message = f"Bronze earnings invalid symbol candidate for {symbol}."
    if promoted:
        message += " Promoted to domain blacklist after 2 runs."
    preview_payload = getattr(exc, "payload", None)
    if preview_payload is None:
        preview_payload = {}
        status_code = getattr(exc, "status_code", None)
        if status_code is not None:
            preview_payload["status_code"] = status_code
        detail = getattr(exc, "detail", None)
        if detail:
            preview_payload["detail"] = detail
        exc_message = str(exc).strip()
        if exc_message and exc_message != detail:
            preview_payload["message"] = exc_message
        if not preview_payload:
            preview_payload = None
    preview = _format_payload_preview(preview_payload, max_chars=500)
    if preview:
        return f"{message} payload_preview={preview}"
    return message


def _mark_coverage(
    *,
    symbol: str,
    backfill_start: date,
    status: str,
    earliest_available: Optional[date],
    coverage_summary: dict[str, int],
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
        if status == "covered":
            coverage_summary["coverage_marked_covered"] += 1
        elif status == "limited":
            coverage_summary["coverage_marked_limited"] += 1
    except Exception as exc:
        mdc.write_warning(f"Failed to write earnings coverage marker for {symbol}: {exc}")


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _validate_environment() -> None:
    if not os.environ.get("ASSET_ALLOCATION_API_BASE_URL"):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_BASE_URL' is strictly required.")
    if not os.environ.get("ASSET_ALLOCATION_API_SCOPE"):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_SCOPE' is strictly required.")
    validate_bronze_storage_clients(
        bronze_container_name=cfg.AZURE_CONTAINER_BRONZE,
        common_container_name=cfg.AZURE_CONTAINER_COMMON,
        bronze_client=bronze_client,
        common_client=common_client,
    )


def _normalize_earnings_provider_symbols(df_symbols: pd.DataFrame) -> list[str]:
    symbols: list[str] = []
    raw_symbols = df_symbols["Symbol"].astype(str).tolist() if "Symbol" in df_symbols.columns else []
    for raw in raw_symbols:
        if "." in raw:
            continue
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        symbols.append(symbol)
    return list(dict.fromkeys(symbols))


def _select_promoted_earnings_reprobe_symbols(
    *,
    provider_symbols: list[str],
    scheduled_symbols: list[str],
) -> list[str]:
    blacklisted_provider_symbols = {symbol for symbol in provider_symbols if list_manager.is_blacklisted(symbol)}
    if not blacklisted_provider_symbols:
        return []

    scheduled_symbol_set = {str(symbol or "").strip().upper() for symbol in scheduled_symbols if str(symbol or "").strip()}
    selected: list[str] = []
    for marker in list_promoted_invalid_candidate_markers(common_client=common_client, domain=_COVERAGE_DOMAIN):
        symbol = str(marker.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        if symbol not in blacklisted_provider_symbols or symbol in scheduled_symbol_set:
            continue
        selected.append(symbol)
        if len(selected) >= _PROMOTED_REPROBE_LIMIT:
            break
    return selected


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "n/a", "na", "-"}:
        return None
    # Alpha Vantage sometimes returns numeric strings.
    try:
        return float(text)
    except Exception:
        return None


def _coerce_surprise_fraction(payload: dict[str, Any]) -> Optional[float]:
    """
    Return Surprise as a fraction (e.g. 0.05 for +5%).

    The prior ingestion stored surprise percentage as a fraction; maintain that
    convention for compatibility with downstream features.
    """
    percent = _coerce_float(payload.get("surprisePercentage"))
    if percent is not None:
        return percent / 100.0
    # Fall back to Alpha Vantage 'surprise' if present (absolute). Do not convert.
    return _coerce_float(payload.get("surprise"))


def _coerce_datetime_column(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        parsed_default = pd.to_datetime(series, errors="coerce", utc=True)
        return parsed_default.dt.tz_localize(None)
    numeric = pd.to_numeric(series, errors="coerce")
    numeric_mask = numeric.notna()
    if numeric_mask.all():
        return pd.to_datetime(numeric, errors="coerce", unit="ms", utc=True).dt.tz_localize(None)
    if not numeric_mask.any():
        parsed_default = pd.to_datetime(series, errors="coerce", utc=True)
        return parsed_default.dt.tz_localize(None)
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
    parsed.loc[~numeric_mask] = pd.to_datetime(series.loc[~numeric_mask], errors="coerce", utc=True)
    parsed.loc[numeric_mask] = pd.to_datetime(numeric.loc[numeric_mask], errors="coerce", unit="ms", utc=True)
    return parsed.dt.tz_localize(None)


def _canonicalize_earnings_frame(df: Optional[pd.DataFrame], *, symbol: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_canonical_earnings_frame()

    out = normalize_columns_to_snake_case(df).copy()
    if symbol is not None:
        out["symbol"] = str(symbol).strip().upper()
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

    for column in ("calendar_time_of_day", "calendar_currency", "ingested_at", "source_hash"):
        if column not in out.columns:
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

    out = out.dropna(subset=["date"]).copy()
    return out[_BUCKET_COLUMNS].reset_index(drop=True)


def _event_identity_key(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="string")
    report_dates = pd.to_datetime(df["report_date"], errors="coerce")
    fiscal_dates = pd.to_datetime(df["fiscal_date_ending"], errors="coerce")
    base_dates = pd.to_datetime(df["date"], errors="coerce")
    # Earnings dates can move. When available, fiscal quarter end is the stable event identity.
    preferred = fiscal_dates.where(fiscal_dates.notna(), report_dates.where(report_dates.notna(), base_dates))
    return preferred.dt.strftime("%Y-%m-%d").fillna("")


def _dedupe_canonical_earnings_events(df: Optional[pd.DataFrame]) -> tuple[pd.DataFrame, int]:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return canonical, 0

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
    scheduled_mask = scheduled.apply(
        lambda row: (str(row["symbol"]), str(row["_event_identity"])) not in actual_keys,
        axis=1,
    )
    filtered_scheduled = scheduled.loc[scheduled_mask].copy()
    replacements = int(len(scheduled) - len(filtered_scheduled))

    merge_frames = [frame for frame in (actual, filtered_scheduled) if frame is not None and not frame.empty]
    out = (
        pd.concat(merge_frames, ignore_index=True, sort=False)
        if merge_frames
        else pd.DataFrame(columns=_BUCKET_COLUMNS)
    )
    out = out.drop(columns=["_event_identity"], errors="ignore")
    out = out.sort_values(["symbol", "date", "record_type"]).reset_index(drop=True)
    return out[_BUCKET_COLUMNS], replacements


def _stamp_canonical_earnings_frame(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    canonical = _canonicalize_earnings_frame(df)
    if canonical.empty:
        return canonical
    payload = canonical[_CANONICAL_EARNINGS_COLUMNS].to_json(orient="records", date_format="iso")
    now = datetime.now(timezone.utc).isoformat()
    source_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    canonical["ingested_at"] = now
    canonical["source_hash"] = source_hash
    return canonical[_BUCKET_COLUMNS].reset_index(drop=True)


def _parse_historical_earnings_records(
    symbol: str,
    payload: dict[str, Any],
    *,
    backfill_start: Optional[date] = None,
) -> pd.DataFrame:
    rows = []
    for item in payload.get("quarterlyEarnings") or []:
        if not isinstance(item, dict):
            continue
        date_raw = item.get("fiscalDateEnding") or item.get("reportedDate")
        if not date_raw:
            continue
        rows.append(
            {
                "symbol": symbol,
                "date": str(date_raw).strip(),
                "report_date": str(item.get("reportedDate") or "").strip() or None,
                "fiscal_date_ending": str(item.get("fiscalDateEnding") or "").strip() or None,
                "reported_eps": _coerce_float(item.get("reportedEPS")),
                "eps_estimate": _coerce_float(item.get("estimatedEPS")),
                "surprise": _coerce_surprise_fraction(item),
                "record_type": "actual",
                "is_future_event": 0,
                "calendar_time_of_day": None,
                "calendar_currency": None,
            }
        )

    df = pd.DataFrame(rows, columns=_CANONICAL_EARNINGS_COLUMNS)
    if df.empty:
        return _empty_canonical_earnings_frame()

    df = _canonicalize_earnings_frame(df, symbol=symbol)
    backfill_start_ts = pd.Timestamp(backfill_start) if backfill_start is not None else None
    df = filter_by_date(df, "date", backfill_start_ts, None)
    df = df.sort_values(["date"]).drop_duplicates(subset=["date", "symbol"], keep="last").reset_index(drop=True)
    return df


def _extract_source_earliest_earnings_date(payload: dict[str, Any]) -> Optional[date]:
    rows = payload.get("quarterlyEarnings")
    if not isinstance(rows, list):
        return None
    return extract_min_date_from_rows(rows, date_keys=("fiscalDateEnding", "reportedDate", "date"))


def _parse_earnings_calendar_csv(
    csv_text: str,
    *,
    symbols: Sequence[str],
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    text = str(csv_text or "").strip()
    if not text:
        raise ValueError("Alpha Vantage earnings calendar response was empty.")

    try:
        df = pd.read_csv(StringIO(text))
    except Exception as exc:
        preview = _format_payload_preview(text)
        raise ValueError(f"Unable to parse Alpha Vantage earnings calendar CSV. payload_preview={preview}") from exc

    missing_columns = [column for column in _EARNINGS_CALENDAR_EXPECTED_COLUMNS if column not in df.columns]
    if missing_columns:
        preview = _format_payload_preview(text)
        raise ValueError(
            "Alpha Vantage earnings calendar CSV missing required columns "
            f"{missing_columns}. payload_preview={preview}"
        )

    total_rows = int(len(df))
    normalized = normalize_columns_to_snake_case(df).copy()
    normalized["symbol"] = normalized["symbol"].astype("string").str.strip().str.upper()
    normalized = normalized[normalized["symbol"].notna() & (normalized["symbol"] != "")].copy()
    normalized["report_date"] = _coerce_datetime_column(normalized["report_date"])
    normalized["fiscal_date_ending"] = _coerce_datetime_column(normalized["fiscal_date_ending"])
    normalized["estimate"] = pd.to_numeric(normalized.get("estimate"), errors="coerce")

    symbol_set = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    matched = normalized[normalized["symbol"].isin(symbol_set)].copy()
    grouped: dict[str, pd.DataFrame] = {}
    for symbol, group in matched.groupby("symbol", dropna=True):
        grouped[str(symbol)] = group.reset_index(drop=True)

    max_report_date = matched["report_date"].dropna().max() if "report_date" in matched.columns else None
    summary = {
        "calendar_rows_fetched": total_rows,
        "calendar_symbols_matched": int(matched["symbol"].nunique()) if not matched.empty else 0,
        "calendar_symbols_ignored": int(normalized["symbol"].nunique() - matched["symbol"].nunique())
        if not normalized.empty
        else 0,
        "calendar_max_report_date": max_report_date.date().isoformat() if pd.notna(max_report_date) else None,
    }
    return grouped, summary


def _build_scheduled_earnings_rows(symbol: str, calendar_rows: Optional[pd.DataFrame]) -> pd.DataFrame:
    if calendar_rows is None or calendar_rows.empty:
        return _empty_canonical_earnings_frame()

    out = normalize_columns_to_snake_case(calendar_rows).copy()
    out["symbol"] = str(symbol).strip().upper()
    out["date"] = out.get("report_date")
    out["reported_eps"] = pd.NA
    out["eps_estimate"] = pd.to_numeric(out.get("estimate"), errors="coerce")
    out["surprise"] = pd.NA
    out["record_type"] = "scheduled"
    out["calendar_time_of_day"] = out.get("time_of_the_day")
    out["calendar_currency"] = out.get("currency")
    out["is_future_event"] = 1
    return _canonicalize_earnings_frame(out, symbol=symbol)


def _normalize_bucket_df(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    out = _canonicalize_earnings_frame(df, symbol=symbol)
    if out.empty:
        return out
    if out["source_hash"].isna().all() or out["ingested_at"].isna().all():
        out = _stamp_canonical_earnings_frame(out)
    return out[_BUCKET_COLUMNS].reset_index(drop=True)


def _write_alpha26_earnings_buckets(
    symbol_frames: Dict[str, pd.DataFrame],
    *,
    run_id: str,
) -> tuple[int, Optional[str]]:
    bucket_frames = bronze_bucketing.empty_bucket_frames(_BUCKET_COLUMNS)
    symbol_to_bucket: dict[str, str] = {}
    for symbol, frame in symbol_frames.items():
        if frame is None or frame.empty:
            continue
        normalized = _normalize_bucket_df(symbol, frame)
        if normalized.empty:
            continue
        bucket = bronze_bucketing.bucket_letter(symbol)
        symbol_to_bucket[str(symbol).upper()] = bucket
        if bucket_frames[bucket].empty:
            bucket_frames[bucket] = normalized
        else:
            bucket_frames[bucket] = pd.concat(
                [frame for frame in (bucket_frames[bucket], normalized) if frame is not None and not frame.empty],
                ignore_index=True,
            )

    publish_result = publish_alpha26_bronze_domain(
        domain="earnings",
        root_prefix=str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")),
        bucket_frames=bucket_frames,
        bucket_columns=_BUCKET_COLUMNS,
        date_column="date",
        symbol_to_bucket=symbol_to_bucket,
        storage_client=bronze_client,
        job_name="bronze-earnings-job",
        run_id=run_id,
    )
    log_bronze_success(
        domain="earnings",
        operation="metadata_artifacts_written",
        bucket_artifacts_written=publish_result.file_count,
        domain_artifact_written=True,
        symbol_index_path=publish_result.index_path or "n/a",
        manifest_path=publish_result.manifest_path or "n/a",
    )
    return publish_result.written_symbols, publish_result.index_path


def _safe_close_alpha_vantage_client(client: AlphaVantageGatewayClient | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


class _ThreadLocalAlphaVantageClientManager:
    def __init__(self, factory: Callable[[], AlphaVantageGatewayClient] | None = None) -> None:
        self._factory = factory or AlphaVantageGatewayClient.from_env
        self._lock = threading.Lock()
        self._clients: dict[int, AlphaVantageGatewayClient] = {}

    def get_client(self) -> AlphaVantageGatewayClient:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.get(thread_id)
            if current is not None:
                return current
            fresh_client = self._factory()
            self._clients[thread_id] = fresh_client
            return fresh_client

    def close_all(self) -> None:
        with self._lock:
            for client in list(self._clients.values()):
                _safe_close_alpha_vantage_client(client)
            self._clients.clear()


def _delete_flat_symbol_blobs() -> int:
    deleted = 0
    prefix = str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")).strip("/")
    for blob in bronze_client.list_blob_infos(name_starts_with=f"{prefix}/"):
        name = str(blob.get("name") or "")
        if not name.endswith(".json"):
            continue
        if name.endswith("whitelist.csv") or name.endswith("blacklist.csv"):
            continue
        if "/buckets/" in name:
            continue
        try:
            bronze_client.delete_file(name)
            deleted += 1
        except Exception as exc:
            mdc.write_warning(f"Failed deleting flat earnings blob {name}: {exc}")
    return deleted


def _fetch_earnings_calendar_by_symbol(
    *,
    av: AlphaVantageGatewayClient,
    symbols: Sequence[str],
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    if not symbols:
        return {}, {
            "calendar_rows_fetched": 0,
            "calendar_symbols_matched": 0,
            "calendar_symbols_ignored": 0,
            "calendar_max_report_date": None,
        }

    horizon = _normalize_calendar_horizon(getattr(cfg, "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON", "12month"))
    csv_text = av.get_earnings_calendar_csv(horizon=horizon)
    grouped, summary = _parse_earnings_calendar_csv(csv_text, symbols=symbols)
    summary["calendar_horizon"] = horizon
    return grouped, summary


def fetch_and_save_raw(
    symbol: str,
    av: AlphaVantageGatewayClient,
    *,
    backfill_start: Optional[date] = None,
    coverage_summary: Optional[dict[str, int]] = None,
    event_summary: Optional[dict[str, int]] = None,
    calendar_rows: Optional[pd.DataFrame] = None,
    collected_symbol_frames: Optional[Dict[str, pd.DataFrame]] = None,
    collected_lock: Optional[threading.Lock] = None,
    skip_blacklist_check: bool = False,
) -> bool:
    """
    Fetch earnings for a single symbol and stage canonical alpha26 Bronze rows for bucket publication.

    Returns True when a symbol frame was staged, False when the symbol produced no rows after filtering.
    """
    coverage_summary = coverage_summary if coverage_summary is not None else _empty_coverage_summary()
    event_summary = event_summary if event_summary is not None else _empty_event_summary()
    if not skip_blacklist_check and list_manager.is_blacklisted(symbol):
        return False
    if collected_symbol_frames is None:
        raise ValueError("collected_symbol_frames is required for bronze earnings alpha26 staging.")

    resolved_backfill_start_raw = normalize_date(backfill_start)
    resolved_backfill_start = (
        pd.Timestamp(resolved_backfill_start_raw).date() if resolved_backfill_start_raw is not None else None
    )
    scheduled_rows = _build_scheduled_earnings_rows(symbol, calendar_rows)

    payload: Optional[dict[str, Any]] = None
    source_earliest: Optional[date] = None
    payload = av.get_earnings(symbol=symbol)
    if not isinstance(payload, dict):
        raise AlphaVantageGatewayError(
            "Unexpected Alpha Vantage earnings response type.", payload={"symbol": symbol}
        )

    source_records = payload.get("quarterlyEarnings") or []
    has_source_records = any(
        isinstance(item, dict) and (item.get("fiscalDateEnding") or item.get("reportedDate"))
        for item in source_records
    )
    source_earliest_raw = _extract_source_earliest_earnings_date(payload)
    source_earliest = pd.Timestamp(source_earliest_raw).date() if source_earliest_raw is not None else None
    actual_rows = _parse_historical_earnings_records(symbol, payload, backfill_start=resolved_backfill_start)

    merge_parts = [
        frame
        for frame in (actual_rows, scheduled_rows)
        if frame is not None and not frame.empty and not frame.dropna(how="all").empty
    ]
    if merge_parts:
        cleaned_merge_parts = [
            frame.dropna(axis="columns", how="all")
            for frame in merge_parts
            if not frame.dropna(axis="columns", how="all").empty
        ]
        merged = pd.concat(cleaned_merge_parts, ignore_index=True, sort=False)
    else:
        merged = _empty_canonical_earnings_frame()
    merged, actual_replacements = _dedupe_canonical_earnings_events(merged)
    if resolved_backfill_start is not None:
        merged = filter_by_date(merged, "date", pd.Timestamp(resolved_backfill_start), None)
    merged = _stamp_canonical_earnings_frame(merged)

    if merged is None or merged.empty:
        if not has_source_records and scheduled_rows.empty:
            raise BronzeCoverageUnavailableError(
                "no_earnings_records",
                detail="No quarterly or scheduled earnings records found.",
                payload=payload or {"symbol": symbol},
            )
        if resolved_backfill_start is not None:
            if source_earliest is not None:
                marker_status = "covered" if source_earliest <= resolved_backfill_start else "limited"
                _mark_coverage(
                    symbol=symbol,
                    backfill_start=resolved_backfill_start,
                    status=marker_status,
                    earliest_available=source_earliest,
                    coverage_summary=coverage_summary,
                )
            list_manager.add_to_whitelist(symbol)
            return False

    event_summary["scheduled_rows_retained"] += int(merged["record_type"].eq("scheduled").sum())
    event_summary["actual_over_scheduled_replacements"] += int(actual_replacements)
    if resolved_backfill_start is not None and source_earliest is not None:
        marker_status = "covered" if source_earliest <= resolved_backfill_start else "limited"
        _mark_coverage(
            symbol=symbol,
            backfill_start=resolved_backfill_start,
            status=marker_status,
            earliest_available=source_earliest,
            coverage_summary=coverage_summary,
        )

    if collected_lock is not None:
        with collected_lock:
            collected_symbol_frames[symbol] = merged.copy()
    else:
        collected_symbol_frames[symbol] = merged.copy()
    list_manager.add_to_whitelist(symbol)
    return True


def _format_failure_reason(exc: BaseException) -> str:
    reason_parts = [f"type={type(exc).__name__}"]
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        reason_parts.append(f"status={status_code}")
    detail = getattr(exc, "detail", None)
    if detail:
        reason_parts.append(f"detail={str(detail)[:220]}")
    else:
        message = str(exc).strip()
        if message:
            reason_parts.append(f"message={message[:220]}")
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = payload.get("path")
        if path:
            reason_parts.append(f"path={path}")
    return " ".join(reason_parts)


def _failure_bucket_key(exc: BaseException) -> str:
    status_code = getattr(exc, "status_code", None)
    key = f"type={type(exc).__name__} status={status_code if status_code is not None else 'n/a'}"
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = str(payload.get("path") or "").strip()
        if path:
            key += f" path={path[:80]}"
    return key


def _sync_earnings_availability_symbols() -> pd.DataFrame:
    try:
        sync_result = symbol_availability.sync_domain_availability("earnings")
    except (AlphaVantageGatewayThrottleError, AlphaVantageGatewayUnavailableError) as exc:
        df_symbols_raw = symbol_availability.get_domain_symbols("earnings")
        if "Symbol" not in df_symbols_raw.columns or df_symbols_raw["Symbol"].dropna().empty:
            raise

        listed_count = int(df_symbols_raw["Symbol"].dropna().shape[0])
        mdc.write_warning(
            "Bronze earnings availability sync degraded: "
            f"provider=alpha_vantage source=stale_postgres listed_count={listed_count} "
            f"reason={_format_failure_reason(exc)}"
        )
        mdc.write_line(
            "Bronze earnings availability sync: "
            f"provider=alpha_vantage listed_count={listed_count} "
            "inserted_count=0 disabled_count=0 duration_ms=0 lock_wait_ms=0 degraded=true"
        )
        return df_symbols_raw

    mdc.write_line(
        "Bronze earnings availability sync: "
        f"provider={sync_result.provider} listed_count={sync_result.listed_count} "
        f"inserted_count={sync_result.inserted_count} disabled_count={sync_result.disabled_count} "
        f"duration_ms={sync_result.duration_ms} lock_wait_ms={sync_result.lock_wait_ms}"
    )
    return symbol_availability.get_domain_symbols("earnings")


async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()

    list_manager.load()

    df_symbols_raw = _sync_earnings_availability_symbols()
    if "Symbol" not in df_symbols_raw.columns or df_symbols_raw["Symbol"].dropna().empty:
        mdc.write_error(
            "Bronze earnings provider unavailable: listing status returned no symbols; "
            "withholding active alpha26 publish."
        )
        return 1

    df_symbols = df_symbols_raw.dropna(subset=["Symbol"]).copy()
    provider_available_count = int(len(df_symbols))
    provider_symbols = _normalize_earnings_provider_symbols(df_symbols)
    symbols = []
    blacklist_skipped = 0
    for sym in provider_symbols:
        if list_manager.is_blacklisted(sym):
            blacklist_skipped += 1
            continue
        symbols.append(sym)

    debug_symbol_set = {str(symbol or "").strip().upper() for symbol in getattr(cfg, "DEBUG_SYMBOLS", []) if str(symbol or "").strip()}
    debug_filtered = 0
    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        filtered_symbols = [s for s in symbols if s in debug_symbol_set]
        debug_filtered = len(symbols) - len(filtered_symbols)
        symbols = filtered_symbols

    reprobe_symbols = _select_promoted_earnings_reprobe_symbols(
        provider_symbols=provider_symbols,
        scheduled_symbols=symbols,
    )
    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS:
        reprobe_symbols = [symbol for symbol in reprobe_symbols if symbol in debug_symbol_set]
    reprobe_symbol_set = set(reprobe_symbols)
    execution_symbols = reprobe_symbols + [symbol for symbol in symbols if symbol not in reprobe_symbol_set]

    if not execution_symbols:
        mdc.write_error(
            "Bronze earnings has no execution symbols after filters; withholding active alpha26 publish."
        )
        return 1

    mdc.write_line(
        "Bronze earnings symbol selection: "
        f"provider_available_count={provider_available_count} "
        f"blacklist_skipped={blacklist_skipped} "
        f"debug_filtered={debug_filtered} "
        f"reprobe_scheduled={len(reprobe_symbols)} "
        f"final_scheduled={len(symbols)}"
    )

    bronze_bucketing.bronze_layout_mode()
    mdc.write_line(f"Starting Alpha Vantage Bronze Earnings Ingestion for {len(execution_symbols)} symbols...")

    calendar_degraded = False
    av = AlphaVantageGatewayClient.from_env()
    try:
        try:
            calendar_rows_by_symbol, calendar_summary = _fetch_earnings_calendar_by_symbol(av=av, symbols=execution_symbols)
        except Exception as exc:
            calendar_degraded = True
            calendar_rows_by_symbol = {}
            calendar_summary = {
                "calendar_horizon": _normalize_calendar_horizon(
                    getattr(cfg, "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON", "12month")
                ),
                "calendar_rows_fetched": 0,
                "calendar_symbols_matched": 0,
                "calendar_symbols_ignored": 0,
                "calendar_max_report_date": None,
                "calendar_degraded": True,
            }
            mdc.write_warning(
                "Bronze AV earnings calendar unavailable; continuing with historical earnings only. "
                f"detail={_format_failure_reason(exc)}"
            )
    finally:
        _safe_close_alpha_vantage_client(av)
    backfill_start = resolve_backfill_start_date()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to bronze earnings data: {backfill_start.isoformat()}")

    mdc.write_line(
        "Bronze AV earnings calendar: "
        f"horizon={calendar_summary.get('calendar_horizon', '12month')} "
        f"calendar_rows_fetched={calendar_summary.get('calendar_rows_fetched', 0)} "
        f"calendar_symbols_matched={calendar_summary.get('calendar_symbols_matched', 0)} "
        f"calendar_symbols_ignored={calendar_summary.get('calendar_symbols_ignored', 0)} "
        f"calendar_max_report_date={calendar_summary.get('calendar_max_report_date') or 'n/a'}"
    )

    max_workers = max(1, int(cfg.ALPHA_VANTAGE_MAX_WORKERS))
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="alpha-vantage-earnings")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)
    client_manager = _ThreadLocalAlphaVantageClientManager()

    run_id = build_bronze_run_id(_COVERAGE_DOMAIN)
    progress = {
        "processed": 0,
        "written": 0,
        "skipped": 0,
        "failed": 0,
        "invalid_candidates": 0,
        "unavailable": 0,
        "blacklist_promotions": 0,
        "reprobe_recovered": 0,
        "reprobe_retained": 0,
    }
    coverage_progress = _empty_coverage_summary()
    event_progress = _empty_event_summary()
    failure_counts: dict[str, int] = {}
    failure_examples: dict[str, str] = {}
    progress_lock = asyncio.Lock()
    collected_symbol_frames: Dict[str, pd.DataFrame] = {}
    collected_lock: Optional[threading.Lock] = threading.Lock()

    def worker(symbol: str, *, is_reprobe: bool = False) -> tuple[bool, dict[str, int], dict[str, int]]:
        av = client_manager.get_client()
        coverage_summary = _empty_coverage_summary()
        event_summary = _empty_event_summary()
        call_kwargs: dict[str, Any] = {
            "backfill_start": backfill_start,
            "coverage_summary": coverage_summary,
            "event_summary": event_summary,
            "calendar_rows": calendar_rows_by_symbol.get(symbol),
            "collected_symbol_frames": collected_symbol_frames,
            "collected_lock": collected_lock,
        }
        if is_reprobe:
            call_kwargs["skip_blacklist_check"] = True
        wrote = fetch_and_save_raw(
            symbol,
            av,
            **call_kwargs,
        )
        return wrote, coverage_summary, event_summary

    async def record_failure(symbol: str, exc: BaseException) -> None:
        failure_reason = _format_failure_reason(exc)
        failure_key = _failure_bucket_key(exc)
        async with progress_lock:
            progress["failed"] += 1
            failure_counts[failure_key] = failure_counts.get(failure_key, 0) + 1
            failure_examples.setdefault(failure_key, f"symbol={symbol} {failure_reason}")
            failed_total = progress["failed"]
            key_total = failure_counts[failure_key]

        # Sample detailed failures to avoid log flooding while still exposing root causes.
        if key_total <= 3 or failed_total % 250 == 0:
            mdc.write_warning(
                "Bronze AV earnings failure: symbol={symbol} {reason} total_failed={failed_total} "
                "key_failed={key_total}".format(
                    symbol=symbol,
                    reason=failure_reason,
                    failed_total=failed_total,
                    key_total=key_total,
                )
            )

    async def run_symbol(symbol: str) -> None:
        is_reprobe = symbol in reprobe_symbol_set
        async with semaphore:
            try:
                wrote, coverage_summary, symbol_event_summary = await loop.run_in_executor(
                    executor,
                    lambda: worker(symbol, is_reprobe=is_reprobe),
                )
                clear_invalid_candidate_marker(
                    common_client=common_client,
                    bronze_client=bronze_client,
                    domain=_COVERAGE_DOMAIN,
                    symbol=symbol,
                )
                list_manager.blacklist.discard(symbol)
                success_count = 0
                disposition = "written" if wrote else "skipped"
                async with progress_lock:
                    if wrote:
                        progress["written"] += 1
                    else:
                        progress["skipped"] += 1
                    if is_reprobe:
                        progress["reprobe_recovered"] += 1
                    for key in coverage_progress:
                        coverage_progress[key] += int(coverage_summary.get(key, 0) or 0)
                    for key in event_progress:
                        event_progress[key] += int(symbol_event_summary.get(key, 0) or 0)
                    success_count = progress["written"] + progress["skipped"]
                if should_log_bronze_success(success_count):
                    log_bronze_success(
                        domain="earnings",
                        operation="symbol_processed",
                        symbol=symbol,
                        disposition=disposition,
                        success_count=success_count,
                        scheduled_rows_retained=symbol_event_summary.get("scheduled_rows_retained", 0),
                        actual_replacements=symbol_event_summary.get("actual_over_scheduled_replacements", 0),
                    )
            except BronzeCoverageUnavailableError as exc:
                if is_reprobe:
                    record_promoted_symbol_reprobe_attempt(
                        common_client=common_client,
                        domain=_COVERAGE_DOMAIN,
                        symbol=symbol,
                        outcome="still_unavailable",
                    )
                    async with progress_lock:
                        progress["unavailable"] += 1
                        progress["reprobe_retained"] += 1
                        retained_total = progress["reprobe_retained"]
                    if retained_total <= 20:
                        mdc.write_warning(
                            f"Bronze earnings promoted re-probe retained blacklist: symbol={symbol} reason={exc.reason_code} detail={exc}"
                        )
                    return
                should_log = False
                async with progress_lock:
                    progress["unavailable"] += 1
                    should_log = progress["unavailable"] <= 20
                if should_log:
                    mdc.write_warning(
                        f"Bronze earnings coverage unavailable: symbol={symbol} reason={exc.reason_code} detail={exc}"
                    )
            except AlphaVantageGatewayInvalidSymbolError as exc:
                if is_reprobe:
                    record_promoted_symbol_reprobe_attempt(
                        common_client=common_client,
                        domain=_COVERAGE_DOMAIN,
                        symbol=symbol,
                        outcome="still_invalid_symbol",
                    )
                    async with progress_lock:
                        progress["reprobe_retained"] += 1
                        retained_total = progress["reprobe_retained"]
                    if retained_total <= 20:
                        mdc.write_warning(
                            f"Bronze earnings promoted re-probe still invalid: symbol={symbol} status=404"
                        )
                    return
                promotion = record_invalid_symbol_candidate(
                    common_client=common_client,
                    bronze_client=bronze_client,
                    domain=_COVERAGE_DOMAIN,
                    symbol=symbol,
                    provider=_COVERAGE_PROVIDER,
                    reason_code=_INVALID_CANDIDATE_REASON,
                    run_id=run_id,
                )
                should_log = False
                async with progress_lock:
                    progress["invalid_candidates"] += 1
                    if promotion.get("promoted"):
                        progress["blacklist_promotions"] += 1
                    should_log = progress["invalid_candidates"] <= 20
                if should_log:
                    mdc.write_warning(
                        _format_invalid_candidate_warning(
                            symbol,
                            exc,
                            promoted=bool(promotion.get("promoted")),
                        )
                    )
            except AlphaVantageGatewayThrottleError as exc:
                if is_reprobe:
                    record_promoted_symbol_reprobe_attempt(
                        common_client=common_client,
                        domain=_COVERAGE_DOMAIN,
                        symbol=symbol,
                        outcome=f"failed_{type(exc).__name__.lower()}",
                    )
                await record_failure(symbol, exc)
            except AlphaVantageGatewayError as exc:
                if is_reprobe:
                    record_promoted_symbol_reprobe_attempt(
                        common_client=common_client,
                        domain=_COVERAGE_DOMAIN,
                        symbol=symbol,
                        outcome=f"failed_{type(exc).__name__.lower()}",
                    )
                await record_failure(symbol, exc)
            except Exception as exc:
                if is_reprobe:
                    record_promoted_symbol_reprobe_attempt(
                        common_client=common_client,
                        domain=_COVERAGE_DOMAIN,
                        symbol=symbol,
                        outcome=f"failed_{type(exc).__name__.lower()}",
                    )
                await record_failure(symbol, exc)
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 500 == 0:
                        mdc.write_line(
                            "Bronze AV earnings progress: processed={processed} written={written} skipped={skipped} "
                            "invalid_candidates={invalid_candidates} unavailable={unavailable} "
                            "blacklist_promotions={blacklist_promotions} reprobe_recovered={reprobe_recovered} "
                            "reprobe_retained={reprobe_retained} failed={failed}".format(**progress)
                        )

    try:
        await asyncio.gather(*(run_symbol(s) for s in execution_symbols))
    finally:
        try:
            executor.shutdown(wait=True, cancel_futures=False)
        except Exception:
            pass
        try:
            client_manager.close_all()
        except Exception:
            pass

    publish_block_reason: str | None = None
    if progress["failed"] > 0:
        publish_block_reason = "symbol_failures"
    elif progress["written"] <= 0:
        publish_block_reason = "empty_output"
    elif calendar_degraded and (progress["unavailable"] > 0 or progress["invalid_candidates"] > 0):
        publish_block_reason = "calendar_degraded_incomplete_history"

    if publish_block_reason:
        progress["failed"] += 1
        mdc.write_error(
            "Bronze earnings alpha26 publish withheld: "
            f"reason={publish_block_reason} calendar_degraded={str(calendar_degraded).lower()} "
            f"written={progress['written']} skipped={progress['skipped']} unavailable={progress['unavailable']} "
            f"invalid_candidates={progress['invalid_candidates']}"
        )
    else:
        try:
            written_symbols, index_path = _write_alpha26_earnings_buckets(collected_symbol_frames, run_id=run_id)
            flat_deleted = _delete_flat_symbol_blobs()
            mdc.write_line(
                "Bronze earnings alpha26 buckets written: "
                f"symbols={written_symbols} index={index_path or 'n/a'} flat_deleted={flat_deleted}"
            )
            try:
                list_manager.flush()
            except Exception as exc:
                mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")
            else:
                log_bronze_success(domain="earnings", operation="list_flush")
        except Exception as exc:
            progress["failed"] += 1
            mdc.write_error(f"Bronze earnings alpha26 bucket write failed: {exc}")

    if failure_counts:
        ordered = sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)
        summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
        mdc.write_warning(f"Bronze AV earnings failure summary: {summary}")
        for name, _ in ordered[:3]:
            example = failure_examples.get(name)
            if example:
                mdc.write_warning(f"Bronze AV earnings failure example ({name}): {example}")

    job_status, exit_code = resolve_job_run_status(
        failed_count=progress["failed"],
        warning_count=progress["invalid_candidates"] + (1 if calendar_degraded else 0),
    )
    mdc.write_line(
        "Bronze AV earnings ingest complete: processed={processed} written={written} skipped={skipped} "
        "invalid_candidates={invalid_candidates} unavailable={unavailable} "
        "blacklist_promotions={blacklist_promotions} reprobe_recovered={reprobe_recovered} "
        "reprobe_retained={reprobe_retained} failed={failed} coverage_checked={coverage_checked} "
        "coverage_forced_refetch={coverage_forced_refetch} coverage_marked_covered={coverage_marked_covered} "
        "coverage_marked_limited={coverage_marked_limited} coverage_skipped_limited_marker={coverage_skipped_limited_marker} "
        "scheduled_rows_retained={scheduled_rows_retained} actual_over_scheduled_replacements={actual_over_scheduled_replacements} "
        "job_status={job_status}".format(
            **progress,
            **coverage_progress,
            **event_progress,
            job_status=job_status,
        )
    )
    return exit_code


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "bronze-earnings-job"
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="bronze", domain="earnings", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
