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


def test_fill_missing_treats_blank_and_placeholder_profile_values_as_missing() -> None:
    context = SymbolCleanupContext(
        provider_facts=_provider_facts(),
        current_profile=SymbolProfileValues(
            exchange_mic=" unknown ",
            sector_norm="N/A",
        ),
        locked_fields=set(),
    )

    plan = build_symbol_cleanup_plan(
        mode="fill_missing",
        requested_fields=["exchange_mic", "sector_norm"],
        context=context,
    )

    assert plan.deterministic_profile.exchange_mic == "XNAS"
    assert plan.ai_requested_fields == ["sector_norm"]


def test_validate_ai_response_returns_canonical_string_values() -> None:
    response = SymbolEnrichmentResolveResponse(
        symbol="SPY",
        profile=SymbolProfileValues(
            sector_norm="  Financial   Services ",
            listing_status_norm=" Active ",
        ),
        model="gpt-5.4-mini",
        confidence=0.92,
        sourceFingerprint="abc123",
        warnings=[],
    )

    normalized = validate_ai_response(
        requested_fields=["sector_norm", "listing_status_norm"],
        provider_facts=_provider_facts(),
        response=response,
    )

    assert normalized.profile.sector_norm == "Financial Services"
    assert normalized.profile.listing_status_norm == "active"


def test_validate_ai_response_rejects_empty_or_partial_response() -> None:
    response = SymbolEnrichmentResolveResponse(
        symbol="SPY",
        profile=SymbolProfileValues(sector_norm="Financial Services"),
        model="gpt-5.4-mini",
        confidence=0.92,
        sourceFingerprint="abc123",
        warnings=[],
    )

    with pytest.raises(ValueError, match="industry_norm"):
        validate_ai_response(
            requested_fields=["sector_norm", "industry_norm"],
            provider_facts=_provider_facts(),
            response=response,
        )


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


@pytest.mark.parametrize(
    ("profile", "match"),
    [
        (SymbolProfileValues(sector_norm=" "), "blank text"),
        (SymbolProfileValues(sector_norm="unknown"), "placeholder text"),
        (SymbolProfileValues(security_type_norm="crypto"), "Unsupported security_type_norm"),
        (SymbolProfileValues(listing_status_norm="pending"), "Unsupported listing_status_norm"),
        (SymbolProfileValues(exchange_mic="XNYS"), "exchange_mic contradiction"),
        (SymbolProfileValues(listing_status_norm="active"), "contradicts delisting date"),
    ],
)
def test_validate_ai_response_rejects_invalid_ai_values(profile: SymbolProfileValues, match: str) -> None:
    response = SymbolEnrichmentResolveResponse(
        symbol="SPY",
        profile=profile,
        model="gpt-5.4-mini",
        confidence=0.92,
        sourceFingerprint="abc123",
        warnings=[],
    )

    with pytest.raises(ValueError, match=match):
        validate_ai_response(
            requested_fields=[field for field, value in profile.model_dump().items() if value is not None],
            provider_facts=_provider_facts(delistingDate="2024-01-01"),
            response=response,
        )


def test_validate_ai_response_rejects_symbol_mismatch() -> None:
    response = SymbolEnrichmentResolveResponse(
        symbol="QQQ",
        profile=SymbolProfileValues(sector_norm="Technology"),
        model="gpt-5.4-mini",
        confidence=0.92,
        sourceFingerprint="abc123",
        warnings=[],
    )

    with pytest.raises(ValueError, match="Resolved symbol mismatch"):
        validate_ai_response(
            requested_fields=["sector_norm"],
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
