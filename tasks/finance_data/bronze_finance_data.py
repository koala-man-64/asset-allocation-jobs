from __future__ import annotations

import asyncio
import collections
import json
import os
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

import pandas as pd

from core.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayError,
    MassiveGatewayNotFoundError,
    MassiveGatewayRateLimitError,
)
from core import symbol_availability
from core import core as mdc
from core.pipeline import ListManager
from tasks.common.bronze_backfill_coverage import (
    load_coverage_marker,
    normalize_date,
    resolve_backfill_start_date,
    should_force_backfill,
    write_coverage_marker,
)
from tasks.common.bronze_observability import log_bronze_success, should_log_bronze_success
from tasks.common.bronze_symbol_policy import (
    BronzeCoverageUnavailableError,
    build_bronze_run_id,
    clear_invalid_candidate_marker,
    is_explicit_invalid_candidate,
    record_invalid_symbol_candidate,
)
from core import bronze_bucketing
from tasks.common.bronze_alpha26_publish import publish_alpha26_bronze_domain
from tasks.common.job_status import resolve_job_run_status
from tasks.common.silver_contracts import parse_wait_timeout_seconds
from tasks.finance_data import config as cfg


bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
common_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
list_manager = ListManager(bronze_client, "finance-data", auto_flush=False, allow_blacklist_updates=False)


REPORTS = [
    {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    },
    {
        "folder": "Cash Flow",
        "file_suffix": "quarterly_cash-flow",
        "report": "cash_flow",
    },
    {
        "folder": "Income Statement",
        "file_suffix": "quarterly_financials",
        "report": "income_statement",
    },
    {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "valuation",
    },
]


FINANCE_REPORT_STALE_DAYS = max(0, int(getattr(cfg, "MASSIVE_FINANCE_FRESH_DAYS", 7)))
_RECOVERY_MAX_ATTEMPTS = 3
_RECOVERY_SLEEP_SECONDS = 5.0
_DEFAULT_SHARED_FINANCE_LOCK = "finance-pipeline-shared"
_COVERAGE_DOMAIN = "finance"
_COVERAGE_PROVIDER = "massive"
_INVALID_CANDIDATE_REASON = "core_statements_provider_invalid"
_FINANCE_SCHEMA_VERSION = 2
_CORE_FINANCE_REPORTS = frozenset({"balance_sheet", "cash_flow", "income_statement"})
_TRACE_FINANCE_ENABLED = (os.environ.get("MASSIVE_FINANCE_TRACE_ENABLED") or "").strip().lower() in {
    "1",
    "true",
    "t",
    "yes",
    "y",
    "on",
}
try:
    _TRACE_FINANCE_SUCCESS_LIMIT = max(
        0, int(str(os.environ.get("MASSIVE_FINANCE_TRACE_SUCCESS_LIMIT") or "40").strip())
    )
except Exception:
    _TRACE_FINANCE_SUCCESS_LIMIT = 40
try:
    _TRACE_FINANCE_ANOMALY_LIMIT = max(
        1, int(str(os.environ.get("MASSIVE_FINANCE_TRACE_ANOMALY_LIMIT") or "200").strip())
    )
except Exception:
    _TRACE_FINANCE_ANOMALY_LIMIT = 200
_TRACE_FINANCE_COUNTERS: collections.Counter[str] = collections.Counter()
_TRACE_FINANCE_LOCK = threading.Lock()
_RAW_FINANCE_VOLATILE_KEYS = frozenset({"request_id", "status", "next_url"})
_BUCKET_COLUMNS = [
    "symbol",
    "report_type",
    "payload_json",
    "source_min_date",
    "source_max_date",
    "ingested_at",
    "payload_hash",
]


@dataclass
class _FinanceSymbolOutcome:
    wrote: int
    valid_symbol: bool
    invalid_candidate: bool
    coverage_unavailable: bool
    invalid_evidence: list[tuple[str, BaseException]]
    failures: list[tuple[str, BaseException]]
    coverage_summary: dict[str, int]


def _empty_coverage_summary() -> dict[str, int]:
    return {
        "coverage_checked": 0,
        "coverage_forced_refetch": 0,
        "coverage_marked_covered": 0,
        "coverage_marked_limited": 0,
        "coverage_skipped_limited_marker": 0,
        "provider_statement_requests": 0,
        "provider_statement_empty_raw_payloads": 0,
        "provider_statement_nonempty_raw_payloads": 0,
        "provider_statement_unexpected_raw_payloads": 0,
        "provider_statement_canonical_rows": 0,
        "provider_statement_canonical_empty_payloads": 0,
        "provider_valuation_requests": 0,
        "provider_valuation_empty_raw_payloads": 0,
        "provider_valuation_nonempty_raw_payloads": 0,
        "provider_valuation_errors": 0,
        "provider_valuation_canonical_rows": 0,
        "provider_valuation_canonical_empty_payloads": 0,
    }


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
        mdc.write_warning(f"Failed to write finance coverage marker for {symbol}: {exc}")


def _is_core_finance_report(report_name: object) -> bool:
    return str(report_name or "").strip().lower() in _CORE_FINANCE_REPORTS


def _is_valuation_finance_report(report_name: object) -> bool:
    return str(report_name or "").strip().lower() == "valuation"


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _truncate_trace_text(value: object, *, limit: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _summarize_massive_payload(payload: Any, *, report_name: Optional[str] = None) -> str:
    if not isinstance(payload, dict):
        return f"payload_type={type(payload).__name__}"

    parts: list[str] = []
    status = payload.get("status")
    if status is not None:
        parts.append(f"provider_status={_truncate_trace_text(status, limit=32)}")
    request_id = payload.get("request_id")
    if request_id:
        parts.append(f"request_id={_truncate_trace_text(request_id, limit=48)}")
    results = payload.get("results")
    if isinstance(results, list):
        parts.append(f"results_len={len(results)}")
    else:
        parts.append(f"results_type={type(results).__name__ if results is not None else 'None'}")
    try:
        dates = sorted(_payload_report_dates(payload, report_name=report_name))
    except Exception:
        dates = []
    if dates:
        parts.append(f"usable_dates={len(dates)}")
        parts.append(f"earliest_date={dates[0].isoformat()}")
        parts.append(f"latest_date={dates[-1].isoformat()}")
    elif isinstance(results, list) and results:
        parts.append("usable_dates=0")
    parts.append(f"next_url={'present' if payload.get('next_url') else 'none'}")
    if payload.get("error"):
        parts.append(f"error={_truncate_trace_text(payload.get('error'))}")
    return " ".join(parts) or "payload_summary=empty"


def _summarize_exception(exc: BaseException) -> str:
    parts = [f"type={type(exc).__name__}"]
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        parts.append(f"status={status_code}")
    detail = getattr(exc, "detail", None)
    if detail:
        parts.append(f"detail={_truncate_trace_text(detail)}")
    elif str(exc).strip():
        parts.append(f"message={_truncate_trace_text(str(exc))}")
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = payload.get("path")
        if path:
            parts.append(f"path={_truncate_trace_text(path, limit=96)}")
        payload_detail = payload.get("detail")
        if payload_detail and payload_detail != detail:
            parts.append(f"payload_detail={_truncate_trace_text(payload_detail)}")
    return " ".join(parts)


def _emit_bounded_trace(category: str, message: str, *, warning: bool = True, limit: Optional[int] = None) -> None:
    max_logs = _TRACE_FINANCE_ANOMALY_LIMIT if limit is None else max(0, int(limit))
    with _TRACE_FINANCE_LOCK:
        seen = int(_TRACE_FINANCE_COUNTERS.get(category, 0))
        if seen >= max_logs:
            return
        current = seen + 1
        _TRACE_FINANCE_COUNTERS[category] = current
    prefix = f"[finance-trace:{category}#{current}] "
    if warning:
        mdc.write_warning(prefix + message)
    else:
        mdc.write_line(prefix + message)
    if current == max_logs:
        mdc.write_line(f"[finance-trace:{category}] further logs suppressed after {max_logs} entries.")


def _log_finance_payload_observation(
    *,
    symbol: str,
    report_name: str,
    timeframe: Optional[str],
    payload: Any,
    anomaly: bool,
) -> None:
    if anomaly:
        _emit_bounded_trace(
            "provider_response_anomaly",
            f"Massive finance response symbol={symbol} report={report_name} "
            f"timeframe={timeframe or 'n/a'} {_summarize_massive_payload(payload, report_name=report_name)}",
            warning=True,
        )
        return
    if not _TRACE_FINANCE_ENABLED:
        return
    _emit_bounded_trace(
        "provider_response_success",
        f"Massive finance response symbol={symbol} report={report_name} "
        f"timeframe={timeframe or 'n/a'} {_summarize_massive_payload(payload, report_name=report_name)}",
        warning=False,
        limit=_TRACE_FINANCE_SUCCESS_LIMIT,
    )


def _validate_environment() -> None:
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")
    if not os.environ.get("ASSET_ALLOCATION_API_BASE_URL"):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_BASE_URL' is strictly required.")
    if not os.environ.get("ASSET_ALLOCATION_API_SCOPE"):
        raise ValueError("Environment variable 'ASSET_ALLOCATION_API_SCOPE' is strictly required.")


def _is_fresh(blob_last_modified: Optional[datetime], *, fresh_days: int) -> bool:
    if blob_last_modified is None:
        return False
    try:
        age = datetime.now(timezone.utc) - blob_last_modified
    except Exception:
        return False
    return age <= timedelta(days=max(0, fresh_days))


def _json_dumps_compact(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _decode_payload_json(raw: Any) -> Optional[dict[str, Any]]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _parse_ingested_at(value: Any) -> Optional[datetime]:
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


def _build_finance_bucket_row(
    *,
    symbol: str,
    report_type: str,
    payload: dict[str, Any],
    source_min_date: Optional[date],
    source_max_date: Optional[date],
) -> dict[str, Any]:
    payload_json = _json_dumps_compact(payload)
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return {
        "symbol": str(symbol).strip().upper(),
        "report_type": str(report_type).strip().lower(),
        "payload_json": payload_json,
        "source_min_date": source_min_date.isoformat() if source_min_date is not None else None,
        "source_max_date": source_max_date.isoformat() if source_max_date is not None else None,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "payload_hash": payload_hash,
    }


def _load_alpha26_finance_row_map(*, symbols: set[str]) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        try:
            df = bronze_bucketing.read_bucket_parquet(
                client=bronze_client,
                prefix="finance-data",
                bucket=bucket,
            )
        except Exception:
            continue
        if df is None or df.empty:
            continue
        if "symbol" not in df.columns or "report_type" not in df.columns:
            continue
        for _, row in df.iterrows():
            symbol = str(row.get("symbol") or "").strip().upper()
            report_type = str(row.get("report_type") or "").strip().lower()
            if not symbol or not report_type:
                continue
            if symbols and symbol not in symbols:
                continue
            candidate = {
                "symbol": symbol,
                "report_type": report_type,
                "payload_json": row.get("payload_json"),
                "source_min_date": row.get("source_min_date"),
                "source_max_date": row.get("source_max_date"),
                "ingested_at": row.get("ingested_at"),
                "payload_hash": row.get("payload_hash"),
            }
            key = (symbol, report_type)
            existing = out.get(key)
            if existing is None:
                out[key] = candidate
                continue
            existing_ts = _parse_ingested_at(existing.get("ingested_at"))
            candidate_ts = _parse_ingested_at(candidate.get("ingested_at"))
            if existing_ts is None and candidate_ts is not None:
                out[key] = candidate
                continue
            if existing_ts is not None and candidate_ts is not None and candidate_ts >= existing_ts:
                out[key] = candidate
    return out


def _upsert_alpha26_finance_row(
    *,
    row_key: tuple[str, str],
    row: dict[str, Any],
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]],
    alpha26_lock: Optional[threading.Lock],
) -> None:
    if alpha26_rows is None:
        return
    if alpha26_lock is not None:
        with alpha26_lock:
            alpha26_rows[row_key] = row
    else:
        alpha26_rows[row_key] = row


def _remove_alpha26_finance_row(
    *,
    row_key: tuple[str, str],
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]],
    alpha26_lock: Optional[threading.Lock],
) -> None:
    if alpha26_rows is None:
        return
    if alpha26_lock is not None:
        with alpha26_lock:
            alpha26_rows.pop(row_key, None)
    else:
        alpha26_rows.pop(row_key, None)


def _write_alpha26_finance_buckets(
    alpha26_rows: dict[tuple[str, str], dict[str, Any]],
    *,
    run_id: str,
) -> tuple[int, Optional[str], Optional[int]]:
    bucket_frames = bronze_bucketing.empty_bucket_frames(_BUCKET_COLUMNS)
    if alpha26_rows:
        frame = pd.DataFrame(list(alpha26_rows.values()), columns=_BUCKET_COLUMNS)
    else:
        frame = pd.DataFrame(columns=_BUCKET_COLUMNS)

    if not frame.empty:
        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["report_type"] = frame["report_type"].astype(str).str.strip().str.lower()
        frame["ingested_at_sort"] = pd.to_datetime(frame.get("ingested_at"), errors="coerce", utc=True)
        frame = frame.sort_values("ingested_at_sort").drop(columns=["ingested_at_sort"])
        frame = frame.drop_duplicates(subset=["symbol", "report_type"], keep="last").reset_index(drop=True)
        for bucket, part in bronze_bucketing.split_df_by_bucket(frame, symbol_column="symbol").items():
            bucket_frames[bucket] = part

    symbols = sorted({str(key[0]).upper() for key in alpha26_rows.keys()})
    symbol_to_bucket = {symbol: bronze_bucketing.bucket_letter(symbol) for symbol in symbols}
    publish_result = publish_alpha26_bronze_domain(
        domain="finance",
        root_prefix="finance-data",
        bucket_frames=bucket_frames,
        bucket_columns=_BUCKET_COLUMNS,
        date_column="date",
        symbol_to_bucket=symbol_to_bucket,
        storage_client=bronze_client,
        job_name="bronze-finance-job",
        run_id=run_id,
        metadata={
            "provider": _COVERAGE_PROVIDER,
            "schema_version": _FINANCE_SCHEMA_VERSION,
            "column_count": len(_BUCKET_COLUMNS),
        },
    )
    column_count: Optional[int] = len(_BUCKET_COLUMNS)
    log_bronze_success(
        domain="finance",
        operation="metadata_artifacts_written",
        bucket_artifacts_written=publish_result.file_count,
        domain_artifact_written=True,
        symbol_index_path=publish_result.index_path or "n/a",
        manifest_path=publish_result.manifest_path or "n/a",
    )
    return publish_result.written_symbols, publish_result.index_path, column_count


def _delete_flat_finance_symbol_blobs() -> int:
    deleted = 0
    allowed_folders = {str(report.get("folder")) for report in REPORTS}
    for blob in bronze_client.list_blob_infos(name_starts_with="finance-data/"):
        name = str(blob.get("name") or "")
        if "/buckets/" in name:
            continue
        parts = name.strip("/").split("/")
        if len(parts) != 3:
            continue
        if parts[0] != "finance-data":
            continue
        if parts[1] not in allowed_folders:
            continue
        if not parts[2].endswith(".json"):
            continue
        try:
            bronze_client.delete_file(name)
            deleted += 1
        except Exception as exc:
            mdc.write_warning(f"Failed deleting flat finance blob {name}: {exc}")
    return deleted


def _parse_iso_date(raw: Any) -> Optional[date]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except Exception:
        return None


def _normalize_report_name(report_name: Any) -> str:
    return str(report_name or "").strip().lower()


def _is_raw_finance_payload(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("results"), list)


def _is_canonical_finance_payload(payload: dict[str, Any], *, report_name: str) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("schema_version") == _FINANCE_SCHEMA_VERSION
        and str(payload.get("provider") or "").strip().lower() == _COVERAGE_PROVIDER
        and str(payload.get("report_type") or "").strip().lower() == _normalize_report_name(report_name)
    )


def _is_supported_finance_payload(payload: Any, *, report_name: str) -> bool:
    return _is_raw_finance_payload(payload) or _is_canonical_finance_payload(payload or {}, report_name=report_name)


def _is_reusable_finance_payload(payload: Any, *, report_name: str) -> bool:
    del report_name
    return _is_raw_finance_payload(payload)


def _extract_report_date_from_row(report_name: str, row: dict[str, Any]) -> Optional[date]:
    if not isinstance(row, dict):
        return None
    if _normalize_report_name(report_name) == "valuation":
        return _parse_iso_date(row.get("date") or row.get("as_of") or row.get("period_end") or row.get("report_period"))
    return _parse_iso_date(
        row.get("period_end")
        or row.get("period_of_report_date")
        or row.get("report_period")
        or row.get("date")
        or row.get("as_of")
    )


def _payload_row_items(payload: dict[str, Any], *, report_name: str) -> list[dict[str, Any]]:
    normalized_report = _normalize_report_name(report_name or payload.get("report_type"))
    if _is_canonical_finance_payload(payload, report_name=normalized_report):
        if normalized_report == "valuation":
            rows = payload.get("rows")
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
            row = {
                "as_of": payload.get("as_of"),
                "market_cap": payload.get("market_cap"),
                "pe_ratio": payload.get("pe_ratio"),
            }
            return [row] if row["as_of"] else []
        rows = payload.get("rows")
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    if _is_raw_finance_payload(payload):
        results = payload.get("results")
        return [row for row in results if isinstance(row, dict)] if isinstance(results, list) else []
    return []


def _payload_report_dates(payload: dict[str, Any], *, report_name: Optional[str] = None) -> list[date]:
    normalized_report = _normalize_report_name(report_name or payload.get("report_type"))
    out: list[date] = []
    for row in _payload_row_items(payload, report_name=normalized_report):
        row_date = _extract_report_date_from_row(normalized_report, row)
        if row_date is not None:
            out.append(row_date)
    return out


def _extract_latest_finance_report_date(payload: dict[str, Any], *, report_name: Optional[str] = None) -> Optional[date]:
    dates = _payload_report_dates(payload, report_name=report_name)
    if not dates:
        return None
    return max(dates)


def _extract_source_earliest_finance_date(payload: dict[str, Any], *, report_name: Optional[str] = None) -> Optional[date]:
    dates = _payload_report_dates(payload, report_name=report_name)
    if not dates:
        return None
    return min(dates)


def _payload_has_dates_on_or_after(
    payload: dict[str, Any],
    *,
    report_name: str,
    cutoff: Optional[date],
) -> bool:
    if cutoff is None:
        return True
    return any(report_date >= cutoff for report_date in _payload_report_dates(payload, report_name=report_name))


def _count_usable_payload_rows(payload: dict[str, Any], *, report_name: str) -> int:
    return sum(
        1
        for row in _payload_row_items(payload, report_name=report_name)
        if _extract_report_date_from_row(report_name, row) is not None
    )


def _stable_finance_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {key: value for key, value in payload.items() if key not in _RAW_FINANCE_VOLATILE_KEYS}


def _fetch_massive_finance_payload(
    *,
    symbol: str,
    report_name: str,
    massive_client: MassiveGatewayClient,
    coverage_summary: Optional[dict[str, int]] = None,
) -> dict[str, Any]:
    if report_name == "valuation":
        if coverage_summary is not None:
            coverage_summary["provider_valuation_requests"] += 1
        try:
            payload = massive_client.get_ratios(
                symbol=symbol,
                pagination=True,
            )
        except BaseException as exc:
            if coverage_summary is not None:
                coverage_summary["provider_valuation_errors"] += 1
            _emit_bounded_trace(
                "valuation_error",
                f"Massive valuation fetch failed symbol={symbol} report={report_name} {_summarize_exception(exc)}",
                warning=True,
            )
            raise
        if not isinstance(payload, dict):
            raise MassiveGatewayError(
                "Unexpected Massive valuation response type.",
                payload={"symbol": symbol, "report": report_name},
            )
        results = payload.get("results")
        if isinstance(results, list):
            if results:
                if coverage_summary is not None:
                    coverage_summary["provider_valuation_nonempty_raw_payloads"] += 1
                    coverage_summary["provider_valuation_canonical_rows"] += _count_usable_payload_rows(
                        payload,
                        report_name=report_name,
                    )
                _log_finance_payload_observation(
                    symbol=symbol,
                    report_name=report_name,
                    timeframe=None,
                    payload=payload,
                    anomaly=False,
                )
            else:
                if coverage_summary is not None:
                    coverage_summary["provider_valuation_empty_raw_payloads"] += 1
                _log_finance_payload_observation(
                    symbol=symbol,
                    report_name=report_name,
                    timeframe=None,
                    payload=payload,
                    anomaly=True,
                )
        else:
            _emit_bounded_trace(
                "valuation_unexpected_payload",
                f"Massive valuation payload missing results list symbol={symbol} report={report_name} "
                f"{_summarize_massive_payload(payload, report_name=report_name)}",
                warning=True,
            )
        return payload

    if coverage_summary is not None:
        coverage_summary["provider_statement_requests"] += 1
    try:
        payload = massive_client.get_finance_report(
            symbol=symbol,
            report=report_name,
            pagination=True,
        )
    except BaseException as exc:
        _emit_bounded_trace(
            "statement_error",
            f"Massive statement fetch failed symbol={symbol} report={report_name} {_summarize_exception(exc)}",
            warning=True,
        )
        raise
    if not isinstance(payload, dict):
        if coverage_summary is not None:
            coverage_summary["provider_statement_unexpected_raw_payloads"] += 1
        _emit_bounded_trace(
            "statement_unexpected_payload",
            f"Massive statement payload was not a dict symbol={symbol} report={report_name} "
            f"payload_type={type(payload).__name__}",
            warning=True,
        )
        raise MassiveGatewayError(
            "Unexpected Massive statement response type.",
            payload={"symbol": symbol, "report": report_name},
        )
    results = payload.get("results")
    if isinstance(results, list):
        if results:
            if coverage_summary is not None:
                coverage_summary["provider_statement_nonempty_raw_payloads"] += 1
                coverage_summary["provider_statement_canonical_rows"] += _count_usable_payload_rows(
                    payload,
                    report_name=report_name,
                )
            _log_finance_payload_observation(
                symbol=symbol,
                report_name=report_name,
                timeframe=None,
                payload=payload,
                anomaly=False,
            )
        else:
            if coverage_summary is not None:
                coverage_summary["provider_statement_empty_raw_payloads"] += 1
            _log_finance_payload_observation(
                symbol=symbol,
                report_name=report_name,
                timeframe=None,
                payload=payload,
                anomaly=True,
            )
    else:
        if coverage_summary is not None:
            coverage_summary["provider_statement_unexpected_raw_payloads"] += 1
        _emit_bounded_trace(
            "statement_unexpected_payload",
            f"Massive statement payload missing results list symbol={symbol} report={report_name} "
            f"{_summarize_massive_payload(payload)}",
            warning=True,
        )
    return payload


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float, bool)):
        return True
    text = str(value).strip()
    if not text:
        return False
    return text.lower() not in {"none", "null", "nan", "n/a", "na", "-", "not available"}


def _is_empty_finance_payload(payload: dict[str, Any], *, report_name: str) -> bool:
    if not payload:
        return True

    payload_report_type = _normalize_report_name(payload.get("report_type") or report_name)
    if _is_raw_finance_payload(payload):
        results = payload.get("results")
        if not isinstance(results, list) or not results:
            return True
        return not any(
            _extract_report_date_from_row(payload_report_type, row) is not None
            for row in results
            if isinstance(row, dict)
        )

    if not _is_canonical_finance_payload(payload, report_name=payload_report_type):
        return True

    if payload_report_type == "valuation":
        rows = _payload_row_items(payload, report_name=payload_report_type)
        if rows:
            return not any(
                _extract_report_date_from_row(payload_report_type, row) is not None
                for row in rows
                if isinstance(row, dict)
            )
        return not _has_non_empty_value(payload.get("as_of")) or not any(
            _has_non_empty_value(payload.get(key)) for key in ("market_cap", "pe_ratio")
        )

    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return True
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _has_non_empty_value(row.get("date")):
            return False
    return True


def fetch_and_save_raw(
    symbol: str,
    report: dict[str, str],
    massive_client: MassiveGatewayClient,
    *,
    backfill_start: Optional[date] = None,
    coverage_summary: Optional[dict[str, int]] = None,
    alpha26_mode: bool = True,
    alpha26_existing_row: Optional[dict[str, Any]] = None,
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]] = None,
    alpha26_lock: Optional[threading.Lock] = None,
) -> bool:
    """
    Fetch a finance report via the API-hosted Massive gateway and store raw provider JSON in Bronze buckets.

    Returns True when a write occurred, False when skipped (fresh/no-op).
    """
    coverage_summary = coverage_summary if coverage_summary is not None else _empty_coverage_summary()
    if list_manager.is_blacklisted(symbol):
        return False

    if not alpha26_mode:
        raise ValueError("Bronze finance only supports alpha26 bucket mode.")

    report_name = report["report"]
    row_key = (str(symbol).strip().upper(), str(report_name).strip().lower())
    resolved_backfill_start = normalize_date(backfill_start)
    existing_payload: Optional[dict[str, Any]] = None
    existing_min_date: Optional[date] = None
    existing_payload_current = False
    force_backfill = False
    existing_row = dict(alpha26_existing_row or {})

    try:
        if existing_row:
            existing_payload = _decode_payload_json(existing_row.get("payload_json"))
            existing_payload_supported = isinstance(existing_payload, dict) and _is_supported_finance_payload(
                existing_payload,
                report_name=report_name,
            )
            existing_payload_current = isinstance(existing_payload, dict) and _is_reusable_finance_payload(
                existing_payload,
                report_name=report_name,
            )
            if resolved_backfill_start is not None:
                coverage_summary["coverage_checked"] += 1
                if existing_payload_supported:
                    existing_min_date = _extract_source_earliest_finance_date(
                        existing_payload or {},
                        report_name=report_name,
                    )
                marker = load_coverage_marker(
                    common_client=common_client,
                    domain=_COVERAGE_DOMAIN,
                    symbol=symbol,
                )
                force_backfill, skipped_limited_marker = should_force_backfill(
                    existing_min_date=existing_min_date,
                    backfill_start=resolved_backfill_start,
                    marker=marker,
                )
                if skipped_limited_marker:
                    coverage_summary["coverage_skipped_limited_marker"] += 1
                if force_backfill:
                    coverage_summary["coverage_forced_refetch"] += 1
                elif existing_min_date is not None and existing_min_date <= resolved_backfill_start:
                    _mark_coverage(
                        symbol=symbol,
                        backfill_start=resolved_backfill_start,
                        status="covered",
                        earliest_available=existing_min_date,
                        coverage_summary=coverage_summary,
                    )
            ingested_at = _parse_ingested_at(existing_row.get("ingested_at"))
            if (
                existing_payload_current
                and _is_fresh(ingested_at, fresh_days=FINANCE_REPORT_STALE_DAYS)
                and not force_backfill
            ):
                list_manager.add_to_whitelist(symbol)
                return False
    except Exception:
        pass

    payload = _fetch_massive_finance_payload(
        symbol=symbol,
        report_name=report_name,
        massive_client=massive_client,
        coverage_summary=coverage_summary,
    )
    if _is_empty_finance_payload(payload, report_name=report_name):
        summary_key = (
            "provider_valuation_canonical_empty_payloads"
            if report_name == "valuation"
            else "provider_statement_canonical_empty_payloads"
        )
        coverage_summary[summary_key] += 1
        _emit_bounded_trace(
            "canonical_empty_payload",
            f"Massive finance raw payload empty symbol={symbol} report={report_name} "
            f"payload_dates={','.join(d.isoformat() for d in _payload_report_dates(payload, report_name=report_name)) or 'none'}",
            warning=True,
        )
        raise BronzeCoverageUnavailableError(
            "empty_finance_payload",
            detail=f"Massive returned empty finance payload for {symbol} report={report_name}.",
            payload={"symbol": symbol, "report": report_name},
        )
    source_earliest = _extract_source_earliest_finance_date(payload, report_name=report_name)
    has_cutoff_coverage = _payload_has_dates_on_or_after(
        payload,
        report_name=report_name,
        cutoff=resolved_backfill_start,
    )
    if resolved_backfill_start is not None and not has_cutoff_coverage:
        if force_backfill:
            _mark_coverage(
                symbol=symbol,
                backfill_start=resolved_backfill_start,
                status="limited",
                earliest_available=source_earliest,
                coverage_summary=coverage_summary,
            )
        if existing_row:
            _remove_alpha26_finance_row(
                row_key=row_key,
                alpha26_rows=alpha26_rows,
                alpha26_lock=alpha26_lock,
            )
            mdc.write_line(
                f"No finance rows on/after {resolved_backfill_start.isoformat()} for {symbol} report={report_name}; "
                "removed alpha26 row."
            )
            list_manager.add_to_whitelist(symbol)
            return True
        list_manager.add_to_whitelist(symbol)
        return False

    if resolved_backfill_start is not None and force_backfill:
        marker_status = (
            "covered" if source_earliest is not None and source_earliest <= resolved_backfill_start else "limited"
        )
        _mark_coverage(
            symbol=symbol,
            backfill_start=resolved_backfill_start,
            status=marker_status,
            earliest_available=source_earliest,
            coverage_summary=coverage_summary,
        )

    if existing_payload is not None:
        if _stable_finance_payload(existing_payload) == _stable_finance_payload(payload):
            list_manager.add_to_whitelist(symbol)
            return False
        if resolved_backfill_start is None and existing_payload_current:
            incoming_latest = _extract_latest_finance_report_date(payload, report_name=report_name)
            existing_latest = _extract_latest_finance_report_date(existing_payload, report_name=report_name)
            if incoming_latest is not None and existing_latest is not None and incoming_latest <= existing_latest:
                list_manager.add_to_whitelist(symbol)
                return False

    source_min = _extract_source_earliest_finance_date(payload, report_name=report_name)
    source_max = _extract_latest_finance_report_date(payload, report_name=report_name)
    bucket_row = _build_finance_bucket_row(
        symbol=symbol,
        report_type=report_name,
        payload=payload,
        source_min_date=source_min,
        source_max_date=source_max,
    )
    _upsert_alpha26_finance_row(
        row_key=row_key,
        row=bucket_row,
        alpha26_rows=alpha26_rows,
        alpha26_lock=alpha26_lock,
    )
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


def _failure_bucket_key(report_name: str, exc: BaseException) -> str:
    status_code = getattr(exc, "status_code", None)
    key = f"report={report_name} type={type(exc).__name__} status={status_code if status_code is not None else 'n/a'}"
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        path = str(payload.get("path") or "").strip()
        if path:
            key += f" path={_truncate_trace_text(path, limit=80)}"
    return key


def _safe_close_massive_client(client: MassiveGatewayClient | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except Exception:
        pass


class _ThreadLocalMassiveClientManager:
    def __init__(self, factory: Callable[[], MassiveGatewayClient] | None = None) -> None:
        self._factory = factory or MassiveGatewayClient.from_env
        self._lock = threading.Lock()
        self._generation = 0
        self._clients: dict[int, tuple[int, MassiveGatewayClient]] = {}

    def get_client(self) -> MassiveGatewayClient:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.get(thread_id)
            if current and current[0] == self._generation:
                return current[1]
            if current:
                _safe_close_massive_client(current[1])
            fresh_client = self._factory()
            self._clients[thread_id] = (self._generation, fresh_client)
            return fresh_client

    def reset_current(self) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            current = self._clients.pop(thread_id, None)
        if current:
            _safe_close_massive_client(current[1])

    def close_all(self) -> None:
        with self._lock:
            for _, client in list(self._clients.values()):
                _safe_close_massive_client(client)
            self._clients.clear()


def _is_recoverable_massive_error(exc: BaseException) -> bool:
    if isinstance(exc, MassiveGatewayNotFoundError):
        return False

    if isinstance(exc, MassiveGatewayRateLimitError):
        return True

    if isinstance(exc, MassiveGatewayError):
        status_code = getattr(exc, "status_code", None)
        if status_code in {408, 429, 500, 502, 503, 504}:
            return True
    else:
        status_code = getattr(exc, "status_code", None)
        if status_code in {408, 429, 500, 502, 503, 504}:
            return True

    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True

    message = " ".join(
        part
        for part in (
            str(exc).strip().lower(),
            str(getattr(exc, "detail", "") or "").strip().lower(),
        )
        if part
    )
    transient_markers = (
        "timeout",
        "timed out",
        "connection",
        "server disconnected",
        "remoteprotocolerror",
        "readerror",
        "connecterror",
        "gateway unavailable",
    )
    return any(marker in message for marker in transient_markers)


def _process_symbol_with_recovery(
    symbol: str,
    client_manager: _ThreadLocalMassiveClientManager,
    *,
    backfill_start: Optional[date] = None,
    alpha26_mode: bool = False,
    alpha26_rows: Optional[dict[tuple[str, str], dict[str, Any]]] = None,
    alpha26_lock: Optional[threading.Lock] = None,
    max_attempts: int = _RECOVERY_MAX_ATTEMPTS,
    sleep_seconds: float = _RECOVERY_SLEEP_SECONDS,
) -> _FinanceSymbolOutcome:
    attempts = max(1, int(max_attempts))
    sleep_seconds = max(0.0, float(sleep_seconds))
    pending_reports = list(REPORTS)
    wrote = 0
    final_failures: list[tuple[str, BaseException]] = []
    coverage_summary = _empty_coverage_summary()
    invalid_evidence: list[tuple[str, BaseException]] = []
    core_successful_reports: set[str] = set()
    core_invalid_reports: set[str] = set()
    coverage_unavailable = False

    for attempt in range(1, attempts + 1):
        next_pending: list[dict[str, str]] = []
        transient_failures: list[tuple[str, BaseException]] = []

        for report in pending_reports:
            report_name = str(report.get("report") or "unknown")
            try:
                massive_client = client_manager.get_client()
                call_kwargs: dict[str, Any] = {
                    "backfill_start": backfill_start,
                    "coverage_summary": coverage_summary,
                }
                if alpha26_mode:
                    alpha26_existing_row: Optional[dict[str, Any]] = None
                    if alpha26_rows is not None:
                        key = (str(symbol).strip().upper(), str(report_name).strip().lower())
                        if alpha26_lock is not None:
                            with alpha26_lock:
                                existing = alpha26_rows.get(key)
                        else:
                            existing = alpha26_rows.get(key)
                        if isinstance(existing, dict):
                            alpha26_existing_row = dict(existing)
                    call_kwargs.update(
                        {
                            "alpha26_mode": True,
                            "alpha26_existing_row": alpha26_existing_row,
                            "alpha26_rows": alpha26_rows,
                            "alpha26_lock": alpha26_lock,
                        }
                    )
                report_wrote = fetch_and_save_raw(symbol, report, massive_client, **call_kwargs)
                if report_wrote:
                    wrote += 1
                if _is_core_finance_report(report_name):
                    core_successful_reports.add(report_name)
            except BronzeCoverageUnavailableError as exc:
                coverage_unavailable = True
                _emit_bounded_trace(
                    "coverage_unavailable",
                    f"Bronze finance coverage unavailable symbol={symbol} report={report_name} "
                    f"{_summarize_exception(exc)}",
                    warning=True,
                )
            except MassiveGatewayNotFoundError as exc:
                _emit_bounded_trace(
                    "gateway_not_found",
                    f"Bronze finance gateway not found symbol={symbol} report={report_name} "
                    f"{_summarize_exception(exc)}",
                    warning=True,
                )
                if report_name == "valuation":
                    coverage_unavailable = True
                    continue
                if _is_core_finance_report(report_name) and is_explicit_invalid_candidate(exc):
                    core_invalid_reports.add(report_name)
                    invalid_evidence.append((report_name, exc))
                    continue
                final_failures.append((report_name, exc))
            except BaseException as exc:
                if _is_recoverable_massive_error(exc) and attempt < attempts:
                    next_pending.append(report)
                    transient_failures.append((report_name, exc))
                    _emit_bounded_trace(
                        "transient_failure",
                        f"Bronze finance transient provider failure symbol={symbol} report={report_name} "
                        f"attempt={attempt}/{attempts} {_summarize_exception(exc)}",
                        warning=True,
                    )
                else:
                    final_failures.append((report_name, exc))
                    _emit_bounded_trace(
                        "final_failure",
                        f"Bronze finance final provider failure symbol={symbol} report={report_name} "
                        f"attempt={attempt}/{attempts} {_summarize_exception(exc)}",
                        warning=True,
                    )

        if not next_pending:
            break

        report_labels = ",".join(sorted({name for name, _ in transient_failures})) or "unknown"
        transient_details = " | ".join(
            f"report={name} {_truncate_trace_text(_format_failure_reason(exc), limit=260)}"
            for name, exc in transient_failures[:3]
        )
        mdc.write_warning(
            f"Transient Massive error for {symbol}; attempt {attempt}/{attempts} failed for report(s) "
            f"[{report_labels}]. Sleeping {sleep_seconds:.1f}s and retrying remaining reports. "
            f"details={transient_details or 'n/a'}"
        )
        client_manager.reset_current()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        pending_reports = next_pending

    nonfatal_valuation_failures: list[tuple[str, BaseException]] = []
    retained_failures: list[tuple[str, BaseException]] = []
    for report_name, exc in final_failures:
        if core_successful_reports and _is_valuation_finance_report(report_name) and _is_recoverable_massive_error(exc):
            nonfatal_valuation_failures.append((report_name, exc))
            continue
        retained_failures.append((report_name, exc))
    final_failures = retained_failures
    if nonfatal_valuation_failures:
        coverage_unavailable = True
        invalid_evidence.extend(nonfatal_valuation_failures)
        detail_preview = " | ".join(
            f"report={name} {_truncate_trace_text(_format_failure_reason(exc), limit=220)}"
            for name, exc in nonfatal_valuation_failures[:3]
        )
        _emit_bounded_trace(
            "valuation_nonfatal_failure",
            f"Bronze finance valuation failures downgraded to coverage-unavailable symbol={symbol} "
            f"core_reports={','.join(sorted(core_successful_reports)) or 'none'} details={detail_preview or 'n/a'}",
            warning=True,
        )

    invalid_candidate = not core_successful_reports and core_invalid_reports == _CORE_FINANCE_REPORTS
    if invalid_candidate:
        return _FinanceSymbolOutcome(
            wrote=wrote,
            valid_symbol=False,
            invalid_candidate=True,
            coverage_unavailable=False,
            invalid_evidence=invalid_evidence,
            failures=final_failures,
            coverage_summary=coverage_summary,
        )

    return _FinanceSymbolOutcome(
        wrote=wrote,
        valid_symbol=bool(core_successful_reports),
        invalid_candidate=False,
        coverage_unavailable=coverage_unavailable or bool(invalid_evidence),
        invalid_evidence=invalid_evidence,
        failures=final_failures,
        coverage_summary=coverage_summary,
    )


async def main_async() -> int:
    mdc.log_environment_diagnostics()
    _validate_environment()

    list_manager.load()

    sync_result = symbol_availability.sync_domain_availability("finance")
    mdc.write_line(
        "Bronze finance availability sync: "
        f"provider={sync_result.provider} listed_count={sync_result.listed_count} "
        f"inserted_count={sync_result.inserted_count} disabled_count={sync_result.disabled_count} "
        f"duration_ms={sync_result.duration_ms} lock_wait_ms={sync_result.lock_wait_ms}"
    )
    df_symbols = symbol_availability.get_domain_symbols("finance").dropna(subset=["Symbol"]).copy()
    provider_available_count = int(len(df_symbols))

    symbols: list[str] = []
    blacklist_skipped = 0
    for sym in df_symbols["Symbol"].astype(str).tolist():
        if "." in sym:
            continue
        if list_manager.is_blacklisted(sym):
            blacklist_skipped += 1
            continue
        symbols.append(sym)
    symbols = list(dict.fromkeys(symbols))

    debug_filtered = 0
    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        filtered_symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        debug_filtered = len(symbols) - len(filtered_symbols)
        symbols = filtered_symbols

    mdc.write_line(
        "Bronze finance symbol selection: "
        f"provider_available_count={provider_available_count} "
        f"blacklist_skipped={blacklist_skipped} "
        f"debug_filtered={debug_filtered} "
        f"final_scheduled={len(symbols)}"
    )
    run_id = build_bronze_run_id(_COVERAGE_DOMAIN)

    alpha26_mode = bronze_bucketing.is_alpha26_mode()
    if not alpha26_mode:
        raise RuntimeError("Bronze finance only supports alpha26 bucket mode.")

    symbol_set = {str(s).strip().upper() for s in symbols}
    alpha26_rows: dict[tuple[str, str], dict[str, Any]] = _load_alpha26_finance_row_map(symbols=symbol_set)
    alpha26_lock: Optional[threading.Lock] = threading.Lock()
    mdc.write_line(f"Loaded existing finance alpha26 seed rows: reports={len(alpha26_rows)} symbols={len(symbol_set)}.")

    mdc.write_line(f"Starting Massive Bronze Finance Ingestion for {len(symbols)} symbols...")

    client_manager = _ThreadLocalMassiveClientManager()
    backfill_start = resolve_backfill_start_date()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to bronze finance data: {backfill_start.isoformat()}")

    max_workers = max(
        1,
        int(
            getattr(
                cfg,
                "MASSIVE_MAX_WORKERS",
                32,
            )
        ),
    )
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="massive-finance")
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(max_workers)

    progress = {
        "processed": 0,
        "written": 0,
        "skipped": 0,
        "failed": 0,
        "invalid_candidates": 0,
        "unavailable": 0,
        "blacklist_promotions": 0,
    }
    coverage_progress = _empty_coverage_summary()
    retry_next_run: set[str] = set()
    failure_counts: dict[str, int] = {}
    failure_examples: dict[str, str] = {}
    progress_lock = asyncio.Lock()

    def worker(symbol: str) -> _FinanceSymbolOutcome:
        return _process_symbol_with_recovery(
            symbol,
            client_manager,
            backfill_start=backfill_start,
            alpha26_mode=alpha26_mode,
            alpha26_rows=alpha26_rows if alpha26_mode else None,
            alpha26_lock=alpha26_lock if alpha26_mode else None,
        )

    async def record_failures(symbol: str, failures: list[tuple[str, BaseException]]) -> None:
        failure_reasons: list[str] = []
        async with progress_lock:
            progress["failed"] += 1
            retry_next_run.add(symbol)
            for report_name, exc in failures:
                failure_reason = _format_failure_reason(exc)
                failure_reasons.append(f"report={report_name} {failure_reason}")
                failure_key = _failure_bucket_key(report_name, exc)
                failure_counts[failure_key] = failure_counts.get(failure_key, 0) + 1
                failure_examples.setdefault(
                    failure_key,
                    f"symbol={symbol} report={report_name} {failure_reason}",
                )
            failed_total = progress["failed"]

        # Sample detailed failures to avoid log flooding while still exposing root causes.
        if failed_total <= 20 or failed_total % 250 == 0:
            summary = " | ".join(failure_reasons[:4])
            mdc.write_warning(
                "Bronze finance symbol failure: symbol={symbol} total_failed={failed_total} details={summary}".format(
                    symbol=symbol,
                    failed_total=failed_total,
                    summary=summary,
                )
            )

    async def run_symbol(symbol: str) -> None:
        async with semaphore:
            try:
                result = await loop.run_in_executor(executor, worker, symbol)
                if result.valid_symbol:
                    try:
                        clear_invalid_candidate_marker(
                            common_client=common_client,
                            domain=_COVERAGE_DOMAIN,
                            symbol=symbol,
                        )
                    except Exception as exc:
                        mdc.write_warning(f"Failed to clear finance invalid-candidate marker for {symbol}: {exc}")

                should_log = False
                async with progress_lock:
                    if result.coverage_unavailable and not result.invalid_candidate:
                        progress["unavailable"] += 1
                    for key in coverage_progress:
                        coverage_progress[key] += int(result.coverage_summary.get(key, 0) or 0)

                if result.invalid_candidate:
                    promotion = record_invalid_symbol_candidate(
                        common_client=common_client,
                        bronze_client=bronze_client,
                        domain=_COVERAGE_DOMAIN,
                        symbol=symbol,
                        provider=_COVERAGE_PROVIDER,
                        reason_code=_INVALID_CANDIDATE_REASON,
                        run_id=run_id,
                    )
                    async with progress_lock:
                        progress["invalid_candidates"] += 1
                        if promotion.get("promoted"):
                            progress["blacklist_promotions"] += 1
                        should_log = progress["invalid_candidates"] <= 20
                    if should_log:
                        reports = ",".join(sorted({name for name, _ in result.invalid_evidence})) or "unknown"
                        evidence = (
                            _summarize_exception(result.invalid_evidence[0][1]) if result.invalid_evidence else "none"
                        )
                        message = (
                            f"Bronze finance invalid symbol candidate: symbol={symbol} reports={reports} "
                            f"observed_runs={promotion.get('observedRunCount', 1)} evidence={evidence}"
                        )
                        if promotion.get("promoted"):
                            message += " promoted_to_domain_blacklist_after_2_runs=true"
                        mdc.write_warning(message)
                elif result.failures:
                    await record_failures(symbol, result.failures)
                else:
                    success_count = 0
                    disposition = "written" if result.wrote else "skipped"
                    async with progress_lock:
                        if result.wrote:
                            progress["written"] += 1
                        else:
                            progress["skipped"] += 1
                        success_count = progress["written"] + progress["skipped"]
                    if should_log_bronze_success(success_count):
                        log_bronze_success(
                            domain="finance",
                            operation="symbol_processed",
                            symbol=symbol,
                            disposition=disposition,
                            reports_written=result.wrote,
                            success_count=success_count,
                            coverage_unavailable=result.coverage_unavailable,
                        )
                    if result.coverage_unavailable:
                        async with progress_lock:
                            should_log = progress["unavailable"] <= 20
                    if should_log:
                        reports = ",".join(sorted({name for name, _ in result.invalid_evidence})) or "unknown"
                        evidence = (
                            _summarize_exception(result.invalid_evidence[0][1]) if result.invalid_evidence else "none"
                        )
                        mdc.write_warning(
                            f"Bronze finance coverage unavailable: symbol={symbol} reports={reports} evidence={evidence}"
                        )
            except Exception as exc:
                await record_failures(symbol, [("unknown", exc)])
            finally:
                async with progress_lock:
                    progress["processed"] += 1
                    if progress["processed"] % 250 == 0:
                        mdc.write_line(
                            "Bronze finance progress: processed={processed} written={written} skipped={skipped} "
                            "invalid_candidates={invalid_candidates} unavailable={unavailable} "
                            "blacklist_promotions={blacklist_promotions} failed={failed}".format(**progress)
                        )

    try:
        await asyncio.gather(*(run_symbol(s) for s in symbols), return_exceptions=True)
    finally:
        try:
            executor.shutdown(wait=True, cancel_futures=False)
        except Exception:
            pass
        try:
            client_manager.close_all()
        except Exception:
            pass

    alpha26_column_count: Optional[int] = len(_BUCKET_COLUMNS)
    try:
        written_symbols, index_path, alpha26_column_count = _write_alpha26_finance_buckets(
            alpha26_rows,
            run_id=run_id,
        )
        flat_deleted = _delete_flat_finance_symbol_blobs()
        mdc.write_line(
            "Bronze finance alpha26 buckets written: "
            f"symbols={written_symbols} index={index_path or 'n/a'} flat_deleted={flat_deleted}"
        )
        try:
            list_manager.flush()
        except Exception as exc:
            mdc.write_warning(f"Failed to flush whitelist/blacklist updates: {exc}")
        else:
            log_bronze_success(domain="finance", operation="list_flush")
    except Exception as exc:
        progress["failed"] += 1
        mdc.write_error(f"Bronze finance alpha26 bucket write failed: {exc}")

    if failure_counts:
        ordered = sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)
        summary = ", ".join(f"{name}={count}" for name, count in ordered[:8])
        mdc.write_warning(f"Bronze finance failure summary: {summary}")
        for name, _ in ordered[:3]:
            example = failure_examples.get(name)
            if example:
                mdc.write_warning(f"Bronze finance failure example ({name}): {example}")
    if retry_next_run:
        preview = ", ".join(sorted(retry_next_run)[:50])
        suffix = " ..." if len(retry_next_run) > 50 else ""
        mdc.write_line(
            f"Retry-on-next-run candidates (not promoted): count={len(retry_next_run)} symbols={preview}{suffix}"
        )

    job_status, exit_code = resolve_job_run_status(
        failed_count=progress["failed"],
        warning_count=progress["invalid_candidates"],
    )
    mdc.write_line(
        "Bronze Massive finance ingest complete: processed={processed} written={written} skipped={skipped} "
        "invalid_candidates={invalid_candidates} unavailable={unavailable} "
        "blacklist_promotions={blacklist_promotions} failed={failed} coverage_checked={coverage_checked} "
        "coverage_forced_refetch={coverage_forced_refetch} coverage_marked_covered={coverage_marked_covered} "
        "coverage_marked_limited={coverage_marked_limited} coverage_skipped_limited_marker={coverage_skipped_limited_marker} "
        "provider_statement_requests={provider_statement_requests} "
        "provider_statement_empty_raw_payloads={provider_statement_empty_raw_payloads} "
        "provider_statement_nonempty_raw_payloads={provider_statement_nonempty_raw_payloads} "
        "provider_statement_unexpected_raw_payloads={provider_statement_unexpected_raw_payloads} "
        "provider_statement_canonical_rows={provider_statement_canonical_rows} "
        "provider_statement_canonical_empty_payloads={provider_statement_canonical_empty_payloads} "
        "provider_valuation_requests={provider_valuation_requests} "
        "provider_valuation_empty_raw_payloads={provider_valuation_empty_raw_payloads} "
        "provider_valuation_nonempty_raw_payloads={provider_valuation_nonempty_raw_payloads} "
        "provider_valuation_errors={provider_valuation_errors} "
        "provider_valuation_canonical_rows={provider_valuation_canonical_rows} "
        "provider_valuation_canonical_empty_payloads={provider_valuation_canonical_empty_payloads} "
        "job_status={job_status}".format(
            **progress,
            **coverage_progress,
            job_status=job_status,
        )
    )
    return exit_code


def main() -> int:
    return asyncio.run(main_async())


def run_bronze_finance_job_entrypoint(
    *,
    job_name: str = "bronze-finance-job",
    run_logged_job_fn: Optional[Callable[..., int]] = None,
    ensure_api_awake_fn: Optional[Callable[..., None]] = None,
    trigger_next_job_fn: Optional[Callable[[], None]] = None,
    write_system_health_marker_fn: Optional[Callable[..., None]] = None,
    job_lock_factory: Optional[Callable[..., Any]] = None,
    shared_lock_name: Optional[str] = None,
    shared_wait_timeout: Optional[float] = None,
) -> int:
    if run_logged_job_fn is None:
        from tasks.common.job_entrypoint import run_logged_job as run_logged_job_fn
    if ensure_api_awake_fn is None:
        from tasks.common.job_trigger import ensure_api_awake_from_env as ensure_api_awake_fn
    if trigger_next_job_fn is None:
        from tasks.common.job_trigger import trigger_next_job_from_env as trigger_next_job_fn
    if write_system_health_marker_fn is None:
        from tasks.common.system_health_markers import (
            write_system_health_marker as write_system_health_marker_fn,
        )
    if job_lock_factory is None:
        job_lock_factory = mdc.JobLock
    if shared_lock_name is None:
        shared_lock_name = (os.environ.get("FINANCE_PIPELINE_SHARED_LOCK_NAME") or _DEFAULT_SHARED_FINANCE_LOCK).strip()
    if shared_wait_timeout is None:
        shared_wait_timeout = parse_wait_timeout_seconds(
            os.environ.get("BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS"),
            default=0.0,
        )

    ensure_api_awake_fn(required=True)

    def _write_success_marker() -> None:
        write_system_health_marker_fn(layer="bronze", domain="finance", job_name=job_name)

    with job_lock_factory(
        shared_lock_name,
        conflict_policy="wait_then_fail",
        wait_timeout_seconds=shared_wait_timeout,
    ):
        with job_lock_factory(job_name, conflict_policy="fail"):
            exit_code = int(
                run_logged_job_fn(
                    job_name=job_name,
                    run=main,
                    on_success=(_write_success_marker,),
                )
            )
    if exit_code == 0:
        trigger_next_job_fn()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(run_bronze_finance_job_entrypoint())
