from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, cast

from asset_allocation_contracts.symbol_enrichment import (
    SymbolEnrichmentField,
    SymbolEnrichmentResolveResponse,
    SymbolProfileValues,
    SymbolProviderFacts,
)
from asset_allocation_runtime_common.foundation.postgres import connect


_UNSET = object()
_MIC_BY_EXCHANGE = {
    "NASDAQ": "XNAS",
    "NASDAQ GLOBAL MARKET": "XNAS",
    "NASDAQ CAPITAL MARKET": "XNAS",
    "NASDAQ GLOBAL SELECT": "XNAS",
    "NYSE": "XNYS",
    "NEW YORK STOCK EXCHANGE": "XNYS",
    "NYSE MKT": "XASE",
    "NYSE AMERICAN": "XASE",
    "AMEX": "XASE",
    "ARCA": "ARCX",
    "NYSE ARCA": "ARCX",
    "BATS": "BATS",
    "CBOE BZX": "BATS",
    "OTC": "OTCM",
    "OTCQX": "OTCM",
    "OTCQB": "OTCM",
    "PINK": "OTCM",
}
_STATUS_MAP = {
    "active": "active",
    "listed": "active",
    "trading": "active",
    "tradable": "active",
    "delisted": "delisted",
    "inactive": "inactive",
    "suspended": "suspended",
    "halted": "suspended",
    "bankrupt": "bankrupt",
    "acquired": "acquired",
    "merged": "acquired",
}
_PLACEHOLDER_TEXT = {
    "n/a",
    "na",
    "none",
    "null",
    "other",
    "tbd",
    "unknown",
    "unspecified",
    "not available",
    "not provided",
}
_ALLOWED_SECURITY_TYPES = {
    "adr",
    "closed_end_fund",
    "common_equity",
    "etf",
    "other",
    "preferred",
    "unit",
    "warrant",
}
_ALLOWED_LISTING_STATUS = {
    "active",
    "acquired",
    "bankrupt",
    "delisted",
    "inactive",
    "suspended",
}
_DETERMINISTIC_FIELDS = {
    "country_of_risk",
    "exchange_mic",
    "is_adr",
    "is_cef",
    "is_etf",
    "is_preferred",
    "listing_status_norm",
    "security_type_norm",
    "share_class",
}


@dataclass(frozen=True)
class SymbolCleanupContext:
    provider_facts: SymbolProviderFacts
    current_profile: SymbolProfileValues
    locked_fields: set[str]


@dataclass(frozen=True)
class SymbolCleanupPlan:
    deterministic_profile: SymbolProfileValues
    ai_requested_fields: list[SymbolEnrichmentField]


def _normalize_symbol(value: str) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise ValueError("symbol is required.")
    return symbol


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_string_value(value: object) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return re.sub(r"\s+", " ", text)


def _normalized_optionable(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().upper()
    if text in {"Y", "YES", "TRUE", "T", "1"}:
        return True
    if text in {"N", "NO", "FALSE", "F", "0"}:
        return False
    return None


def _text_corpus(provider_facts: SymbolProviderFacts) -> str:
    parts = [
        provider_facts.symbol,
        provider_facts.name,
        provider_facts.description,
        provider_facts.assetType,
        provider_facts.exchange,
        provider_facts.sector,
        provider_facts.industry,
        provider_facts.industry2,
    ]
    return " ".join(str(part or "").strip().lower() for part in parts if part is not None)


def _has_any_keyword(corpus: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in corpus for keyword in keywords)


def _infer_flag(provider_facts: SymbolProviderFacts, field_name: str) -> bool | object:
    corpus = _text_corpus(provider_facts)
    if field_name == "is_etf" and _has_any_keyword(corpus, (" etf", "exchange traded fund", "exchange-traded fund", "index fund")):
        return True
    if field_name == "is_cef" and _has_any_keyword(corpus, ("closed-end", "closed end", "cef")):
        return True
    if field_name == "is_preferred" and _has_any_keyword(corpus, ("preferred", "preference share", "preference stock")):
        return True
    if field_name == "is_adr" and _has_any_keyword(corpus, (" adr", "american depositary", "depositary receipt")):
        return True
    return _UNSET


def _infer_exchange_mic(provider_facts: SymbolProviderFacts) -> str | object:
    exchange = _normalize_string_value(provider_facts.exchange)
    if exchange is None:
        return _UNSET
    return _MIC_BY_EXCHANGE.get(exchange.upper(), _UNSET)


def _infer_country_of_risk(provider_facts: SymbolProviderFacts) -> str | object:
    country = _normalize_string_value(provider_facts.country)
    if country is None:
        return _UNSET
    return country


def _infer_listing_status(provider_facts: SymbolProviderFacts) -> str | object:
    if _clean_text(provider_facts.delistingDate):
        return "delisted"
    status = _normalize_string_value(provider_facts.status)
    if status is None:
        return _UNSET
    for raw_token, normalized in _STATUS_MAP.items():
        if raw_token in status.lower():
            return normalized
    return _UNSET


def _infer_share_class(provider_facts: SymbolProviderFacts) -> str | object:
    name = _clean_text(provider_facts.name) or ""
    symbol = provider_facts.symbol
    match = re.search(r"\bclass\s+([a-z0-9]+)\b", name, flags=re.IGNORECASE)
    if match:
        return f"Class {match.group(1).upper()}"
    suffix_match = re.search(r"[.\-]([A-Z])$", symbol.upper())
    if suffix_match:
        return f"Class {suffix_match.group(1)}"
    return _UNSET


def _infer_security_type(provider_facts: SymbolProviderFacts, inferred_flags: dict[str, bool | object]) -> str | object:
    if inferred_flags.get("is_etf") is True:
        return "etf"
    if inferred_flags.get("is_cef") is True:
        return "closed_end_fund"
    if inferred_flags.get("is_preferred") is True:
        return "preferred"
    if inferred_flags.get("is_adr") is True:
        return "adr"
    asset_type = (_normalize_string_value(provider_facts.assetType) or "").lower()
    if not asset_type:
        return _UNSET
    if "etf" in asset_type or "fund" in asset_type:
        return "etf"
    if "preferred" in asset_type:
        return "preferred"
    if "adr" in asset_type or "depositary" in asset_type:
        return "adr"
    if "unit" in asset_type:
        return "unit"
    if "warrant" in asset_type:
        return "warrant"
    if any(token in asset_type for token in ("stock", "equity", "common")):
        return "common_equity"
    return _UNSET


def load_symbol_cleanup_context(dsn: str, symbol: str) -> SymbolCleanupContext:
    resolved_symbol = _normalize_symbol(symbol)
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    symbol,
                    name,
                    description,
                    sector,
                    industry,
                    industry_2,
                    country,
                    exchange,
                    asset_type,
                    ipo_date,
                    delisting_date,
                    status,
                    COALESCE(
                        is_optionable,
                        CASE
                            WHEN upper(trim(COALESCE(optionable, ''))) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
                            WHEN upper(trim(COALESCE(optionable, ''))) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
                            ELSE NULL
                        END
                    ) AS is_optionable,
                    source_nasdaq,
                    source_massive,
                    source_alpha_vantage
                FROM core.symbols
                WHERE symbol = %s
                """,
                (resolved_symbol,),
            )
            provider_row = cur.fetchone()
            if provider_row is None:
                raise LookupError(f"Symbol '{resolved_symbol}' not found in core.symbols.")

            cur.execute(
                """
                SELECT
                    security_type_norm,
                    exchange_mic,
                    country_of_risk,
                    sector_norm,
                    industry_group_norm,
                    industry_norm,
                    is_adr,
                    is_etf,
                    is_cef,
                    is_preferred,
                    share_class,
                    listing_status_norm,
                    issuer_summary_short
                FROM core.symbol_profiles
                WHERE symbol = %s
                """,
                (resolved_symbol,),
            )
            profile_row = cur.fetchone()

            cur.execute(
                """
                SELECT field_name
                FROM core.symbol_profile_overrides
                WHERE symbol = %s AND is_locked = TRUE
                """,
                (resolved_symbol,),
            )
            locked_fields = {str(row[0]) for row in cur.fetchall()}

    provider_facts = SymbolProviderFacts.model_validate(
        {
            "symbol": provider_row[0],
            "name": provider_row[1],
            "description": provider_row[2],
            "sector": provider_row[3],
            "industry": provider_row[4],
            "industry2": provider_row[5],
            "country": provider_row[6],
            "exchange": provider_row[7],
            "assetType": provider_row[8],
            "ipoDate": provider_row[9],
            "delistingDate": provider_row[10],
            "status": provider_row[11],
            "isOptionable": _normalized_optionable(provider_row[12]),
            "sourceNasdaq": provider_row[13],
            "sourceMassive": provider_row[14],
            "sourceAlphaVantage": provider_row[15],
        }
    )
    current_profile = SymbolProfileValues.model_validate(
        {
            "security_type_norm": profile_row[0] if profile_row else None,
            "exchange_mic": profile_row[1] if profile_row else None,
            "country_of_risk": profile_row[2] if profile_row else None,
            "sector_norm": profile_row[3] if profile_row else None,
            "industry_group_norm": profile_row[4] if profile_row else None,
            "industry_norm": profile_row[5] if profile_row else None,
            "is_adr": profile_row[6] if profile_row else None,
            "is_etf": profile_row[7] if profile_row else None,
            "is_cef": profile_row[8] if profile_row else None,
            "is_preferred": profile_row[9] if profile_row else None,
            "share_class": profile_row[10] if profile_row else None,
            "listing_status_norm": profile_row[11] if profile_row else None,
            "issuer_summary_short": profile_row[12] if profile_row else None,
        }
    )
    return SymbolCleanupContext(
        provider_facts=provider_facts,
        current_profile=current_profile,
        locked_fields=locked_fields,
    )


def build_symbol_cleanup_plan(
    *,
    mode: str,
    requested_fields: list[SymbolEnrichmentField],
    context: SymbolCleanupContext,
) -> SymbolCleanupPlan:
    current_values = context.current_profile.model_dump(mode="json")
    deterministic_updates: dict[str, Any] = {}
    ai_requested_fields: list[SymbolEnrichmentField] = []
    inferred_flags = {
        "is_adr": _infer_flag(context.provider_facts, "is_adr"),
        "is_etf": _infer_flag(context.provider_facts, "is_etf"),
        "is_cef": _infer_flag(context.provider_facts, "is_cef"),
        "is_preferred": _infer_flag(context.provider_facts, "is_preferred"),
    }
    deterministic_candidates: dict[str, Any] = {
        "exchange_mic": _infer_exchange_mic(context.provider_facts),
        "country_of_risk": _infer_country_of_risk(context.provider_facts),
        "listing_status_norm": _infer_listing_status(context.provider_facts),
        "share_class": _infer_share_class(context.provider_facts),
        "security_type_norm": _infer_security_type(context.provider_facts, inferred_flags),
        **inferred_flags,
    }

    for requested_field in requested_fields:
        field_name = cast(str, requested_field)
        if field_name in context.locked_fields:
            continue
        current_value = current_values.get(field_name)
        if mode == "fill_missing" and current_value is not None:
            continue

        candidate = deterministic_candidates.get(field_name, _UNSET)
        if candidate is not _UNSET:
            if current_value != candidate:
                deterministic_updates[field_name] = candidate
            continue

        ai_requested_fields.append(requested_field)

    return SymbolCleanupPlan(
        deterministic_profile=SymbolProfileValues.model_validate(deterministic_updates),
        ai_requested_fields=ai_requested_fields,
    )


def validate_ai_response(
    *,
    requested_fields: list[SymbolEnrichmentField],
    provider_facts: SymbolProviderFacts,
    response: SymbolEnrichmentResolveResponse,
) -> None:
    if _normalize_symbol(response.symbol) != _normalize_symbol(provider_facts.symbol):
        raise ValueError(
            f"Resolved symbol mismatch: expected '{provider_facts.symbol}', got '{response.symbol}'."
        )

    allowed_fields = {cast(str, field) for field in requested_fields}
    response_values = response.profile.model_dump(mode="json")
    for field_name, value in response_values.items():
        if value is None:
            continue
        if field_name not in allowed_fields:
            raise ValueError(f"AI returned unsupported field '{field_name}' for symbol '{provider_facts.symbol}'.")
        if isinstance(value, str):
            normalized = _normalize_string_value(value)
            if normalized is None:
                raise ValueError(f"AI returned blank text for '{field_name}' on symbol '{provider_facts.symbol}'.")
            if normalized.lower() in _PLACEHOLDER_TEXT:
                raise ValueError(
                    f"AI returned placeholder text for '{field_name}' on symbol '{provider_facts.symbol}': '{normalized}'."
                )
            if field_name == "security_type_norm" and normalized not in _ALLOWED_SECURITY_TYPES:
                raise ValueError(f"Unsupported security_type_norm '{normalized}' for symbol '{provider_facts.symbol}'.")
            if field_name == "listing_status_norm" and normalized not in _ALLOWED_LISTING_STATUS:
                raise ValueError(f"Unsupported listing_status_norm '{normalized}' for symbol '{provider_facts.symbol}'.")

    if response.profile.exchange_mic is not None:
        inferred_mic = _infer_exchange_mic(provider_facts)
        if inferred_mic is not _UNSET and response.profile.exchange_mic != inferred_mic:
            raise ValueError(
                f"exchange_mic contradiction for symbol '{provider_facts.symbol}': "
                f"expected '{inferred_mic}', got '{response.profile.exchange_mic}'."
            )

    if response.profile.listing_status_norm == "active" and _clean_text(provider_facts.delistingDate):
        raise ValueError(f"listing_status_norm contradicts delisting date for symbol '{provider_facts.symbol}'.")

    for field_name in ("is_etf", "is_cef", "is_preferred", "is_adr"):
        inferred = _infer_flag(provider_facts, field_name)
        resolved_value = getattr(response.profile, field_name)
        if inferred is True and resolved_value is False:
            raise ValueError(f"{field_name} contradicts provider facts for symbol '{provider_facts.symbol}'.")


def merge_symbol_cleanup_result(
    *,
    symbol: str,
    deterministic_profile: SymbolProfileValues,
    ai_response: SymbolEnrichmentResolveResponse | None,
) -> SymbolEnrichmentResolveResponse | None:
    deterministic_values = {
        field: value
        for field, value in deterministic_profile.model_dump(mode="json").items()
        if value is not None
    }
    ai_values = (
        {
            field: value
            for field, value in ai_response.profile.model_dump(mode="json").items()
            if value is not None
        }
        if ai_response is not None
        else {}
    )
    merged_values = {**deterministic_values, **ai_values}
    if not merged_values:
        return None

    warnings: list[str] = list(ai_response.warnings) if ai_response is not None else []
    if deterministic_values:
        warnings.append(f"deterministic_fields={','.join(sorted(deterministic_values))}")

    source_fingerprint = ai_response.sourceFingerprint if ai_response is not None else _build_fingerprint(
        symbol=symbol,
        payload=merged_values,
    )
    model_name = ai_response.model if ai_response is not None else "deterministic-symbol-cleanup"
    confidence = ai_response.confidence if ai_response is not None else 1.0

    return SymbolEnrichmentResolveResponse(
        symbol=_normalize_symbol(symbol),
        profile=SymbolProfileValues.model_validate(merged_values),
        model=model_name,
        confidence=confidence,
        sourceFingerprint=source_fingerprint,
        warnings=warnings,
    )


def _build_fingerprint(*, symbol: str, payload: dict[str, Any]) -> str:
    serialized = json.dumps({"symbol": _normalize_symbol(symbol), "payload": payload}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:24]
