from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation.datetime_utils import parse_utc_datetime, utc_isoformat
from tasks.common.watermarks import blob_last_modified_utc


SCHEMA_VERSION = 1
STATE_COLUMNS = [
    "schema_version",
    "domain",
    "sub_domain",
    "bucket",
    "entity_key",
    "symbol",
    "source_signature",
    "source_max_ingested_at",
    "source_min_date",
    "source_max_date",
    "as_of_date",
    "processor_version",
    "last_processed_at",
    "last_success_run_id",
    "status",
    "row_count",
    "output_paths_json",
]


class ProcessedStateError(RuntimeError):
    pass


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def processed_state_enabled() -> bool:
    return _env_bool("SILVER_PROCESSED_STATE_ENABLED", default=True)


def processed_state_fail_open() -> bool:
    return _env_bool("SILVER_PROCESSED_STATE_FAIL_OPEN", default=True)


def processed_state_force_rebuild() -> bool:
    return _env_bool("SILVER_PROCESSED_STATE_FORCE_REBUILD", default=False)


def processed_state_path(domain: str) -> str:
    cleaned = str(domain or "").strip().replace(" ", "-").replace("_", "-")
    if not cleaned:
        raise ValueError("Processed-state domain is required.")
    return f"system/silver-processed-state/{cleaned}/latest.parquet"


def empty_processed_state_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=STATE_COLUMNS)


def _common_client() -> Any:
    return getattr(mdc, "common_storage_client", None)


def _normalize_state_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return empty_processed_state_frame()
    out = frame.copy()
    for column in STATE_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    out = out[STATE_COLUMNS].copy()
    out["schema_version"] = pd.to_numeric(out["schema_version"], errors="coerce").fillna(SCHEMA_VERSION).astype("int64")
    out["row_count"] = pd.to_numeric(out["row_count"], errors="coerce").fillna(0).astype("int64")
    for column in set(STATE_COLUMNS) - {"schema_version", "row_count"}:
        out[column] = out[column].astype("string")
    return out


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def load_processed_state(domain: str) -> pd.DataFrame:
    if not processed_state_enabled():
        return empty_processed_state_frame()
    client = _common_client()
    if client is None:
        return empty_processed_state_frame()
    path = processed_state_path(domain)
    try:
        frame = mdc.load_parquet(path, client=client)
        return _normalize_state_frame(frame)
    except Exception as exc:
        if processed_state_fail_open():
            mdc.write_warning(f"Silver processed-state load failed open domain={domain} path={path}: {exc}")
            return empty_processed_state_frame()
        raise ProcessedStateError(f"Failed to load Silver processed state domain={domain} path={path}: {exc}") from exc


def save_processed_state(domain: str, frame: pd.DataFrame) -> None:
    if not processed_state_enabled():
        return
    client = _common_client()
    if client is None:
        mdc.write_warning(f"Silver processed-state save skipped domain={domain}: common storage client unavailable.")
        return
    path = processed_state_path(domain)
    try:
        mdc.store_parquet(_normalize_state_frame(frame), path, client=client)
    except Exception as exc:
        raise ProcessedStateError(f"Failed to save Silver processed state domain={domain} path={path}: {exc}") from exc


def make_entity_key(*, domain: str, bucket: str, symbol: str | None = None, entity_id: str | None = None) -> str:
    cleaned_domain = str(domain or "").strip().lower()
    cleaned_bucket = str(bucket or "").strip().upper()
    cleaned_entity = str(entity_id if entity_id is not None else symbol or "").strip().upper()
    if not cleaned_domain or not cleaned_entity:
        raise ValueError("domain and symbol/entity_id are required for processed-state keys.")
    return "|".join([cleaned_domain, cleaned_bucket, cleaned_entity])


def stable_frame_signature(frame: pd.DataFrame | None, *, exclude_columns: Iterable[str] = ()) -> str | None:
    if frame is None or frame.empty:
        return None
    excluded = {str(column).lower() for column in exclude_columns}
    selected_columns = [column for column in frame.columns if str(column).lower() not in excluded]
    if not selected_columns:
        return None
    normalized = frame[selected_columns].copy()
    normalized.columns = [str(column) for column in normalized.columns]
    normalized = normalized.reindex(columns=sorted(normalized.columns))
    for column in normalized.columns:
        values = normalized[column]
        if pd.api.types.is_datetime64_any_dtype(values):
            normalized[column] = pd.to_datetime(values, errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            normalized[column] = values.astype("string")
    normalized = normalized.fillna("<NA>")
    sort_columns = list(normalized.columns)
    normalized = normalized.sort_values(sort_columns, kind="mergesort").reset_index(drop=True)
    payload = normalized.to_json(orient="records", date_format="iso", date_unit="ns")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _iso_or_none(value: Any) -> str | None:
    parsed = parse_utc_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return utc_isoformat(parsed.astimezone(timezone.utc))


def _date_or_none(value: Any) -> str | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date().isoformat()


def max_source_timestamp_iso(frame: pd.DataFrame | None, *, blob: Mapping[str, Any] | None = None) -> str | None:
    candidates: list[pd.Timestamp] = []
    if frame is not None and not frame.empty:
        for column in ("ingested_at", "updated_at", "last_updated", "source_updated_at"):
            if column in frame.columns:
                parsed = pd.to_datetime(frame[column], errors="coerce", utc=True).dropna()
                if not parsed.empty:
                    candidates.append(pd.Timestamp(parsed.max()).to_pydatetime())
    if candidates:
        return utc_isoformat(max(candidates).astimezone(timezone.utc))
    if blob is not None:
        return _iso_or_none(blob_last_modified_utc(dict(blob)))
    return None


def source_date_bounds(frame: pd.DataFrame | None, *, columns: Sequence[str]) -> tuple[str | None, str | None]:
    if frame is None or frame.empty:
        return None, None
    parsed_parts = []
    for column in columns:
        if column in frame.columns:
            parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
            if not parsed.empty:
                parsed_parts.append(parsed)
    if not parsed_parts:
        return None, None
    combined = pd.concat(parsed_parts, ignore_index=True)
    return _date_or_none(combined.min()), _date_or_none(combined.max())


def build_processed_state_record(
    *,
    domain: str,
    bucket: str,
    entity_key: str,
    processor_version: str,
    source_frame: pd.DataFrame | None = None,
    source_blob: Mapping[str, Any] | None = None,
    sub_domain: str | None = None,
    symbol: str | None = None,
    source_signature: str | None = None,
    source_max_ingested_at: str | None = None,
    source_date_columns: Sequence[str] = (),
    as_of_date: str | None = None,
    last_success_run_id: str | None = None,
    status: str = "ok",
    row_count: int | None = None,
    output_paths: Sequence[str] = (),
    processed_at: datetime | None = None,
) -> dict[str, Any]:
    min_date, max_date = source_date_bounds(source_frame, columns=source_date_columns)
    processed_time = processed_at or datetime.now(timezone.utc)
    if processed_time.tzinfo is None:
        processed_time = processed_time.replace(tzinfo=timezone.utc)
    return {
        "schema_version": SCHEMA_VERSION,
        "domain": str(domain or "").strip(),
        "sub_domain": str(sub_domain or "").strip(),
        "bucket": str(bucket or "").strip().upper(),
        "entity_key": str(entity_key or "").strip(),
        "symbol": str(symbol or "").strip().upper(),
        "source_signature": source_signature or stable_frame_signature(source_frame, exclude_columns=("ingested_at",)),
        "source_max_ingested_at": source_max_ingested_at or max_source_timestamp_iso(source_frame, blob=source_blob),
        "source_min_date": min_date,
        "source_max_date": max_date,
        "as_of_date": str(as_of_date or "").strip(),
        "processor_version": str(processor_version or "").strip(),
        "last_processed_at": utc_isoformat(processed_time.astimezone(timezone.utc)),
        "last_success_run_id": str(last_success_run_id or "").strip(),
        "status": str(status or "").strip() or "ok",
        "row_count": int(row_count if row_count is not None else (0 if source_frame is None else len(source_frame))),
        "output_paths_json": json.dumps([str(path) for path in output_paths if str(path).strip()], separators=(",", ":")),
    }


def build_processed_state_index(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    state = _normalize_state_frame(frame)
    if state.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in state.to_dict(orient="records"):
        entity_key = _text(row.get("entity_key")).strip()
        if entity_key:
            out[entity_key] = row
    return out


def should_process_entity(
    current: Mapping[str, Any],
    prior: Mapping[str, Any] | None,
    *,
    force_reprocess: bool = False,
) -> tuple[bool, str]:
    if force_reprocess:
        return True, "force_reprocess"
    if not prior:
        return True, "no_prior_state"
    if _text(prior.get("status")).strip().lower() != "ok":
        return True, "prior_not_successful"
    if _text(prior.get("processor_version")) != _text(current.get("processor_version")):
        return True, "processor_version"

    current_signature = _text(current.get("source_signature")).strip()
    prior_signature = _text(prior.get("source_signature")).strip()
    if current_signature and prior_signature:
        if current_signature != prior_signature:
            return True, "source_signature"
        current_as_of = _text(current.get("as_of_date")).strip()
        prior_as_of = _text(prior.get("as_of_date")).strip()
        if current_as_of and prior_as_of != current_as_of:
            return True, "as_of_date"
        return False, "processed_state"

    current_source_ts = parse_utc_datetime(_text(current.get("source_max_ingested_at")))
    last_processed_at = parse_utc_datetime(_text(prior.get("last_processed_at")))
    if current_source_ts is not None and last_processed_at is not None:
        if current_source_ts.tzinfo is None:
            current_source_ts = current_source_ts.replace(tzinfo=timezone.utc)
        if last_processed_at.tzinfo is None:
            last_processed_at = last_processed_at.replace(tzinfo=timezone.utc)
        if current_source_ts.astimezone(timezone.utc) <= last_processed_at.astimezone(timezone.utc):
            return False, "processed_state_timestamp"
    return True, "source_freshness"


def merge_processed_state_updates(existing: pd.DataFrame | None, updates: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    base = _normalize_state_frame(existing)
    if not updates:
        return base
    update_frame = _normalize_state_frame(pd.DataFrame([dict(update) for update in updates]))
    if update_frame.empty:
        return base

    key_columns = ["domain", "sub_domain", "bucket", "entity_key"]
    update_keys = {
        tuple(_text(row.get(column)) for column in key_columns)
        for row in update_frame.to_dict(orient="records")
    }
    if not base.empty:
        keep_mask = [
            tuple(_text(row.get(column)) for column in key_columns) not in update_keys
            for row in base.to_dict(orient="records")
        ]
        base = base.loc[keep_mask].copy()
    merged = pd.concat([base, update_frame], ignore_index=True)
    return _normalize_state_frame(merged)


def new_stats() -> dict[str, int]:
    return {
        "entities_seen": 0,
        "entities_changed": 0,
        "entities_skipped_state": 0,
        "entities_reprocessed_calendar": 0,
        "output_buckets_touched": 0,
        "processed_state_updates": 0,
    }


def record_decision(stats: dict[str, int], *, should_process: bool, reason: str) -> None:
    stats["entities_seen"] = int(stats.get("entities_seen", 0)) + 1
    if should_process:
        stats["entities_changed"] = int(stats.get("entities_changed", 0)) + 1
        if reason == "as_of_date":
            stats["entities_reprocessed_calendar"] = int(stats.get("entities_reprocessed_calendar", 0)) + 1
    elif reason.startswith("processed_state"):
        stats["entities_skipped_state"] = int(stats.get("entities_skipped_state", 0)) + 1
