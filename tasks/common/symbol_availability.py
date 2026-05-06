from __future__ import annotations

"""Compatibility re-export for runtime-common symbol availability."""

from typing import Any

from asset_allocation_runtime_common.shared_core import symbol_availability as _owner

DomainName = _owner.DomainName
ProviderName = _owner.ProviderName
SyncResult = _owner.SyncResult
get_domain_symbols = _owner.get_domain_symbols
get_symbol_availability_mask = _owner.get_symbol_availability_mask
sync_domain_availability = _owner.sync_domain_availability


def __getattr__(name: str) -> Any:
    return getattr(_owner, name)
