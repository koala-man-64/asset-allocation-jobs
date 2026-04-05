from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Optional, Sequence

import pandas as pd

from core import core as mdc
from tasks.common.backfill import get_backfill_range

_COVERAGE_MARKER_PREFIX = "system/backfill_coverage/bronze"


def resolve_backfill_start_date() -> Optional[date]:
    backfill_start, _ = get_backfill_range()
    if backfill_start is None:
        return None
    try:
        return pd.Timestamp(backfill_start).date()
    except Exception:
        return None


def normalize_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return pd.Timestamp(text).date()
    except Exception:
        return None


def extract_min_date_from_dataframe(df: pd.DataFrame, *, date_col: str) -> Optional[date]:
    if df is None or df.empty or date_col not in df.columns:
        return None
    parsed = pd.to_datetime(df[date_col], errors="coerce", utc=True).dropna()
    if parsed.empty:
        return None
    try:
        return parsed.min().date()
    except Exception:
        return None


def extract_min_date_from_rows(rows: Sequence[dict[str, Any]], *, date_keys: Sequence[str]) -> Optional[date]:
    earliest: Optional[date] = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in date_keys:
            candidate = normalize_date(row.get(key))
            if candidate is None:
                continue
            if earliest is None or candidate < earliest:
                earliest = candidate
    return earliest


def extract_min_date_from_payload_sections(
    payload: dict[str, Any],
    *,
    section_keys: Sequence[str],
    date_keys: Sequence[str],
) -> Optional[date]:
    earliest: Optional[date] = None
    for section_key in section_keys:
        section = payload.get(section_key)
        if not isinstance(section, list):
            continue
        section_earliest = extract_min_date_from_rows(section, date_keys=date_keys)
        if section_earliest is None:
            continue
        if earliest is None or section_earliest < earliest:
            earliest = section_earliest
    return earliest


def marker_blob_path(*, domain: str, symbol: str) -> str:
    resolved_domain = str(domain or "").strip().lower()
    resolved_symbol = str(symbol or "").strip().upper()
    return f"{_COVERAGE_MARKER_PREFIX}/{resolved_domain}/{resolved_symbol}.json"


def load_coverage_marker(*, common_client: Any, domain: str, symbol: str) -> Optional[dict[str, Any]]:
    if common_client is None:
        return None
    path = marker_blob_path(domain=domain, symbol=symbol)
    try:
        raw = mdc.read_raw_bytes(path, client=common_client, missing_ok=True)
    except Exception:
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _marker_backfill_start(marker: dict[str, Any]) -> Optional[date]:
    return normalize_date(marker.get("backfillStart"))


def marker_status_for_backfill(*, marker: Optional[dict[str, Any]], backfill_start: Optional[date]) -> Optional[str]:
    if marker is None or backfill_start is None:
        return None
    marker_start = _marker_backfill_start(marker)
    if marker_start != backfill_start:
        return None
    status = str(marker.get("coverageStatus") or "").strip().lower()
    if status in {"covered", "limited"}:
        return status
    return None


def should_force_backfill(
    *,
    existing_min_date: Optional[date],
    backfill_start: Optional[date],
    marker: Optional[dict[str, Any]],
) -> tuple[bool, bool]:
    """
    Returns (force_backfill, skipped_limited_marker).
    """
    if backfill_start is None or existing_min_date is None:
        return False, False
    if existing_min_date <= backfill_start:
        return False, False
    status = marker_status_for_backfill(marker=marker, backfill_start=backfill_start)
    if status == "limited":
        return False, True
    return True, False


def write_coverage_marker(
    *,
    common_client: Any,
    domain: str,
    symbol: str,
    backfill_start: date,
    coverage_status: str,
    earliest_available: Optional[date],
    provider: str,
) -> None:
    status = str(coverage_status or "").strip().lower()
    if status not in {"covered", "limited"}:
        raise ValueError(f"Unsupported coverage_status={coverage_status!r}.")

    marker = {
        "layer": "bronze",
        "domain": str(domain or "").strip().lower(),
        "symbol": str(symbol or "").strip().upper(),
        "backfillStart": backfill_start.isoformat(),
        "coverageStatus": status,
        "earliestAvailable": earliest_available.isoformat() if earliest_available else None,
        "validatedAt": datetime.now(timezone.utc).isoformat(),
        "provider": str(provider or "").strip().lower() or "unknown",
    }
    raw = json.dumps(marker, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    mdc.store_raw_bytes(raw, marker_blob_path(domain=domain, symbol=symbol), client=common_client)
