from __future__ import annotations

from pathlib import Path

import asset_allocation_runtime_common.foundation.debug_symbols as foundation_debug_symbols
import asset_allocation_runtime_common.foundation.runtime_config as foundation_runtime_config
import asset_allocation_runtime_common.market_data.core as market_core
import asset_allocation_runtime_common.market_data.delta_core as market_delta_core

from tests.test_support.runtime_common import owner_module_for


_STALE_PATCH_PREFIXES = tuple(
    "".join(parts)
    for parts in (
        ('patch("', "core.core."),
        ("patch('", "core.core."),
        ('patch("', "core.delta_core."),
        ("patch('", "core.delta_core."),
        ('monkeypatch.setattr("', "core.ranking_engine.service."),
        ("monkeypatch.setattr('", "core.ranking_engine.service."),
    )
)


def test_runtime_common_wrapper_owner_modules_are_stable() -> None:
    assert owner_module_for(market_core.get_symbols).__name__ == "asset_allocation_runtime_common.shared_core.core"
    assert owner_module_for(market_delta_core.store_delta).__name__ == "asset_allocation_runtime_common.shared_core.delta_core"
    assert (
        owner_module_for(foundation_debug_symbols.refresh_debug_symbols_from_db).__name__
        == "asset_allocation_runtime_common.shared_core.debug_symbols"
    )
    assert (
        owner_module_for(foundation_runtime_config.apply_runtime_config_to_env).__name__
        == "asset_allocation_runtime_common.shared_core.runtime_config"
    )


def test_tests_do_not_use_stale_runtime_common_patch_prefixes() -> None:
    tests_root = Path(__file__).resolve().parent
    offenders: list[str] = []

    for path in sorted(tests_root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for prefix in _STALE_PATCH_PREFIXES:
            if prefix in text:
                offenders.append(f"{path.relative_to(tests_root)}::{prefix}")

    assert offenders == []
