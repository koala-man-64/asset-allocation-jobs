from __future__ import annotations

import pytest

from asset_allocation_contracts.symbol_enrichment import (
    SymbolEnrichmentResolveResponse,
    SymbolProfileValues,
    SymbolProviderFacts,
)

from core.symbol_cleanup_runtime import (
    SymbolCleanupContext,
    build_symbol_cleanup_plan,
    merge_symbol_cleanup_result,
    validate_ai_response,
)


def _provider_facts(**overrides):
    payload = {
        "symbol": "SPY",
        "name": "SPDR S&P 500 ETF Trust",
        "description": "Exchange traded fund tracking the S&P 500.",
        "sector": None,
        "industry": None,
        "industry2": None,
        "country": "US",
        "exchange": "NASDAQ",
        "assetType": "ETF",
        "ipoDate": None,
        "delistingDate": None,
        "status": "Active",
        "isOptionable": True,
        "sourceNasdaq": True,
        "sourceMassive": True,
        "sourceAlphaVantage": False,
    }
    payload.update(overrides)
    return SymbolProviderFacts.model_validate(payload)


def test_build_symbol_cleanup_plan_prefers_deterministic_fields_and_skips_locked() -> None:
    context = SymbolCleanupContext(
        provider_facts=_provider_facts(),
        current_profile=SymbolProfileValues(),
        locked_fields={"country_of_risk"},
    )

    plan = build_symbol_cleanup_plan(
        mode="fill_missing",
        requested_fields=[
            "security_type_norm",
            "exchange_mic",
            "country_of_risk",
            "is_etf",
            "sector_norm",
        ],
        context=context,
    )

    assert plan.deterministic_profile.security_type_norm == "etf"
    assert plan.deterministic_profile.exchange_mic == "XNAS"
    assert plan.deterministic_profile.is_etf is True
    assert plan.deterministic_profile.country_of_risk is None
    assert plan.ai_requested_fields == ["sector_norm"]


def test_validate_ai_response_rejects_provider_contradiction() -> None:
    response = SymbolEnrichmentResolveResponse(
        symbol="SPY",
        profile=SymbolProfileValues(is_etf=False),
        model="gpt-5.4-mini",
        confidence=0.92,
        sourceFingerprint="abc123",
        warnings=[],
    )

    with pytest.raises(ValueError, match="is_etf contradicts provider facts"):
        validate_ai_response(
            requested_fields=["is_etf"],
            provider_facts=_provider_facts(),
            response=response,
        )


def test_merge_symbol_cleanup_result_keeps_deterministic_updates_when_ai_not_needed() -> None:
    merged = merge_symbol_cleanup_result(
        symbol="SPY",
        deterministic_profile=SymbolProfileValues(exchange_mic="XNAS", is_etf=True),
        ai_response=None,
    )

    assert merged is not None
    assert merged.model == "deterministic-symbol-cleanup"
    assert merged.profile.exchange_mic == "XNAS"
    assert merged.profile.is_etf is True
