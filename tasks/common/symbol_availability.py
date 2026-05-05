from __future__ import annotations

"""Job-side guardrails around runtime-common symbol availability sync.

The authoritative implementation lives in ``asset_allocation_runtime_common``.
This module keeps bronze jobs from failing an entire Massive availability sync
when the provider emits individual placeholder or otherwise invalid ticker rows.
"""

import threading
from typing import Any

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.shared_core import symbol_availability as _owner
from asset_allocation_runtime_common.market_data.symbol_identity import InvalidSymbolInputError

DomainName = _owner.DomainName
ProviderName = _owner.ProviderName
SyncResult = _owner.SyncResult
get_domain_symbols = _owner.get_domain_symbols
get_symbol_availability_mask = _owner.get_symbol_availability_mask

_NORMALIZER_PATCH_LOCK = threading.RLock()
_INVALID_SYMBOL_LOG_SAMPLE_LIMIT = 5


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _log_invalid_massive_symbols(invalid_symbols: list[tuple[str, str]]) -> None:
    if not invalid_symbols:
        return
    sample = ", ".join(f"{symbol}:{reason}" for symbol, reason in invalid_symbols[:_INVALID_SYMBOL_LOG_SAMPLE_LIMIT])
    mdc.write_warning(
        "Massive ticker sync skipped invalid symbol records: "
        f"count={len(invalid_symbols)} sample={sample}"
    )


def _normalize_massive_records(records: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    alias_resolution_count = 0
    invalid_symbols: list[tuple[str, str]] = []

    for record in records:
        if not isinstance(record, dict):
            continue
        raw_symbol = record.get("Symbol") or record.get("symbol") or record.get("ticker")
        normalized_raw = _normalize_symbol(raw_symbol)
        if not normalized_raw:
            continue
        try:
            symbol = _owner._normalize_massive_symbol(raw_symbol)
        except InvalidSymbolInputError as exc:
            invalid_symbols.append((normalized_raw, str(exc)))
            continue
        if symbol != normalized_raw:
            alias_resolution_count += 1
        rows.append(
            {
                "Symbol": symbol,
                "Name": record.get("Name") or record.get("name"),
                "Exchange": record.get("Exchange") or record.get("exchange") or record.get("primary_exchange"),
                "AssetType": record.get("AssetType") or record.get("asset_type") or record.get("type"),
                "source_massive": True,
            }
        )

    _log_invalid_massive_symbols(invalid_symbols)
    if not rows:
        out = pd.DataFrame(columns=["Symbol", "Name", "Exchange", "AssetType", "source_massive"])
        out.attrs["alias_resolution_count"] = alias_resolution_count
        out.attrs["alias_resolution_failure_count"] = len(invalid_symbols)
        return out

    out = pd.DataFrame(rows)
    out["Symbol"] = out["Symbol"].astype(str).str.strip().str.upper()
    out = out[out["Symbol"].ne("")]
    out = out.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
    out.attrs["alias_resolution_count"] = alias_resolution_count
    out.attrs["alias_resolution_failure_count"] = len(invalid_symbols)
    return out


def sync_domain_availability(domain: DomainName) -> SyncResult:
    with _NORMALIZER_PATCH_LOCK:
        original_normalizer = _owner._normalize_massive_records
        _owner._normalize_massive_records = _normalize_massive_records
        try:
            return _owner.sync_domain_availability(domain)
        finally:
            _owner._normalize_massive_records = original_normalizer


def __getattr__(name: str) -> Any:
    return getattr(_owner, name)
