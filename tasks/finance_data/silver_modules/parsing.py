from __future__ import annotations

from datetime import datetime, timezone
import json
import re
from typing import Any, Optional

import pandas as pd

from asset_allocation_contracts.finance import (
    SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN,
    SILVER_FINANCE_SOURCE_ALIASES_BY_SUBDOMAIN,
)
from tasks.common.silver_contracts import coerce_to_naive_datetime


_KEY_NORMALIZER = re.compile(r"[^a-z0-9]+")
_STATEMENT_TIMEFRAMES = frozenset({"quarterly", "annual"})


def _normalize_key(name: Any) -> str:
    return _KEY_NORMALIZER.sub("", str(name).strip().lower())


def _try_parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None

    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"none", "null", "nan", "n/a", "na", "-"}:
        return None

    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def _get_first_value(payload: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    normalized = {_normalize_key(k): v for k, v in payload.items()}
    for candidate in candidates:
        value = normalized.get(_normalize_key(candidate))
        if value is not None:
            return value
    return None


def _normalize_finance_report_type(report_type: Any) -> str:
    return str(report_type or "").strip().lower()


def _extract_finance_row_date(item: dict[str, Any], *, report_type: str) -> Optional[pd.Timestamp]:
    if not isinstance(item, dict):
        return None
    normalized_report = _normalize_finance_report_type(report_type)
    if normalized_report == "valuation":
        raw_value = item.get("date") or item.get("as_of") or item.get("period_end") or item.get("report_period")
    else:
        raw_value = (
            item.get("date")
            or item.get("period_end")
            or item.get("period_of_report_date")
            or item.get("report_period")
            or item.get("as_of")
        )
    parsed = pd.to_datetime(raw_value, errors="coerce", utc=True, format="mixed")
    if pd.isna(parsed):
        return None
    return parsed.tz_convert(None)


def _is_canonical_finance_payload(payload: dict[str, Any], *, report_type: str) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("schema_version") == 2
        and str(payload.get("provider") or "").strip().lower() == "massive"
        and _normalize_finance_report_type(payload.get("report_type")) == _normalize_finance_report_type(report_type)
    )


def _is_raw_finance_payload(payload: dict[str, Any]) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("results"), list)


def _read_valuation_payload(payload: dict[str, Any], *, ticker: str, report_type: str) -> pd.DataFrame:
    alias_map = SILVER_FINANCE_SOURCE_ALIASES_BY_SUBDOMAIN["valuation"]
    expected_columns = SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN["valuation"]
    if _is_canonical_finance_payload(payload, report_type=report_type):
        rows = payload.get("rows")
        if isinstance(rows, list):
            reports = rows
        elif payload.get("as_of") is not None:
            reports = [payload]
        else:
            return pd.DataFrame(columns=["Date", "Symbol", *expected_columns[2:]])
    else:
        results = payload.get("results")
        if not isinstance(results, list) or not results:
            return pd.DataFrame(columns=["Date", "Symbol", *expected_columns[2:]])
        reports = results

    prepared_rows: list[tuple[pd.Timestamp, int, dict[str, Any]]] = []
    for index, item in enumerate(reports):
        if not isinstance(item, dict):
            continue
        report_date = _extract_finance_row_date(item, report_type="valuation")
        if report_date is None:
            continue
        row: dict[str, Any] = {"Date": report_date, "Symbol": ticker}
        for column in expected_columns[2:]:
            row[column] = _try_parse_float(_get_first_value(item, alias_map[column]))
        if not any(row.get(column) is not None for column in expected_columns[2:]):
            continue
        prepared_rows.append((report_date, index, row))

    if not prepared_rows:
        return pd.DataFrame(columns=["Date", "Symbol", *expected_columns[2:]])

    deduped: dict[pd.Timestamp, dict[str, Any]] = {}
    for report_date, index, row in sorted(prepared_rows, key=lambda entry: (entry[0], entry[1])):
        del index
        deduped[report_date] = row

    ordered_rows = [deduped[key] for key in sorted(deduped.keys())]
    df = pd.DataFrame(ordered_rows)
    for column in expected_columns[2:]:
        if column not in df.columns:
            df[column] = pd.Series(dtype="float64")
    df["Date"] = coerce_to_naive_datetime(df["Date"])
    df = df.dropna(subset=["Date"]).sort_values(["Date"]).reset_index(drop=True)
    return df[["Date", "Symbol", *expected_columns[2:]]]


def _read_statement_payload(payload: dict[str, Any], *, ticker: str, report_type: str) -> pd.DataFrame:
    alias_map = SILVER_FINANCE_SOURCE_ALIASES_BY_SUBDOMAIN[report_type]
    expected_columns = SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN[report_type]

    if _is_canonical_finance_payload(payload, report_type=report_type):
        reports = payload.get("rows")
        if reports is None:
            raise ValueError(f"Finance payload rows are required for {ticker}/{report_type}.")
        if not isinstance(reports, list) or not reports:
            return pd.DataFrame()
    elif _is_raw_finance_payload(payload):
        reports = payload.get("results")
        if not isinstance(reports, list) or not reports:
            return pd.DataFrame()
    else:
        raise ValueError(f"Unsupported finance payload schema for {ticker}/{report_type}.")

    prepared_rows: list[tuple[pd.Timestamp, str, int, dict[str, Any]]] = []
    for index, item in enumerate(reports):
        if not isinstance(item, dict):
            continue
        report_date = _extract_finance_row_date(item, report_type=report_type)
        if report_date is None:
            continue
        timeframe = str(_get_first_value(item, alias_map["timeframe"]) or "").strip().lower()
        if timeframe not in _STATEMENT_TIMEFRAMES:
            continue
        row: dict[str, Any] = {"Date": report_date, "Symbol": ticker, "timeframe": timeframe}
        for column in expected_columns[2:]:
            if column == "timeframe":
                continue
            row[column] = _try_parse_float(_get_first_value(item, alias_map[column]))
        metric_columns = [column for column in expected_columns[2:] if column != "timeframe"]
        if not any(row.get(column) is not None for column in metric_columns):
            continue
        prepared_rows.append((report_date, timeframe, index, row))

    if not prepared_rows:
        return pd.DataFrame()

    deduped: dict[tuple[pd.Timestamp, str], dict[str, Any]] = {}
    for report_date, timeframe, index, row in sorted(prepared_rows, key=lambda entry: (entry[0], entry[1], entry[2])):
        del index
        deduped[(report_date, timeframe)] = row

    ordered_rows = [
        deduped[key]
        for key in sorted(deduped.keys(), key=lambda item: (item[0], item[1]))
    ]
    df = pd.DataFrame(ordered_rows)
    if df.empty:
        return df

    for column in expected_columns[2:]:
        if column not in df.columns:
            if column == "timeframe":
                df[column] = pd.Series(dtype="string")
            else:
                df[column] = pd.Series(dtype="float64")
    df["Date"] = coerce_to_naive_datetime(df["Date"])
    df = df.dropna(subset=["Date"]).sort_values(["Date", "timeframe"]).reset_index(drop=True)
    return df[["Date", "Symbol", *expected_columns[2:]]]


def _read_finance_json(raw_bytes: bytes, *, ticker: str, report_type: str) -> pd.DataFrame:
    payload = json.loads(raw_bytes.decode("utf-8"))
    sub_domain = _normalize_finance_report_type(report_type)
    if sub_domain not in SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN:
        return pd.DataFrame()

    if not isinstance(payload, dict):
        raise ValueError(f"Finance payload for {ticker}/{sub_domain} must be a JSON object.")
    payload_report_type = _normalize_finance_report_type(payload.get("report_type"))
    looks_canonical = payload.get("schema_version") == 2 and str(payload.get("provider") or "").strip().lower() == "massive"
    if looks_canonical and payload_report_type and payload_report_type != sub_domain:
        raise ValueError(
            f"Finance payload report_type mismatch for {ticker}: expected {sub_domain}, got {payload_report_type or 'missing'}."
        )
    if not _is_canonical_finance_payload(payload, report_type=sub_domain) and not _is_raw_finance_payload(payload):
        raise ValueError(f"Unsupported finance payload schema for {ticker}/{sub_domain}.")

    if sub_domain == "valuation":
        return _read_valuation_payload(payload, ticker=ticker, report_type=sub_domain)

    return _read_statement_payload(payload, ticker=ticker, report_type=sub_domain)


def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


def resample_daily_ffill(df: pd.DataFrame, *, extend_to: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    if "Date" not in df.columns:
        return df

    df = df.copy()
    df["Date"] = coerce_to_naive_datetime(df["Date"])
    df = df.dropna(subset=["Date"])
    if df.empty:
        return df

    group_columns = [column for column in ("Symbol", "timeframe") if column in df.columns]
    grouped_frames: list[pd.DataFrame] = []
    grouped = [(None, df)] if not group_columns else list(df.groupby(group_columns, dropna=False, sort=True))

    for group_key, group_frame in grouped:
        group = group_frame.copy()
        group = group.sort_values(["Date"]).drop_duplicates(subset=["Date"], keep="last")
        group = group.set_index("Date").sort_index()
        if group.empty:
            continue

        end = group.index.max()
        if extend_to is not None and extend_to > end:
            end = extend_to

        full_range = pd.date_range(start=group.index.min(), end=end, freq="D", name="Date")
        group_daily = group.reindex(full_range).ffill().reset_index()

        if group_columns:
            key_values = group_key if isinstance(group_key, tuple) else (group_key,)
            for column, value in zip(group_columns, key_values):
                group_daily[column] = value
        grouped_frames.append(group_daily)

    if not grouped_frames:
        return pd.DataFrame(columns=df.reset_index(drop=True).columns)
    return pd.concat(grouped_frames, ignore_index=True)
