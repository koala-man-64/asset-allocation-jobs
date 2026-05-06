from __future__ import annotations

from tasks.common import symbol_availability


def test_symbol_availability_shim_reexports_runtime_common_owner() -> None:
    assert symbol_availability.get_domain_symbols is symbol_availability._owner.get_domain_symbols
    assert symbol_availability.get_symbol_availability_mask is symbol_availability._owner.get_symbol_availability_mask
    assert symbol_availability.sync_domain_availability is symbol_availability._owner.sync_domain_availability

