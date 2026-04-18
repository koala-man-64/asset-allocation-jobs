from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from asset_allocation_runtime_common.foundation import config as cfg
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.providers.alpha_vantage_gateway_client import AlphaVantageGatewayInvalidSymbolError
from asset_allocation_runtime_common.providers.massive_gateway_client import MassiveGatewayNotFoundError

_INVALID_CANDIDATE_MARKER_PREFIX = "system/invalid_symbol_candidates/bronze"
_PROMOTION_THRESHOLD = 2
_BLACKLIST_UPDATE_LOCK = threading.Lock()


class BronzeCoverageUnavailableError(Exception):
    def __init__(
        self,
        reason_code: str,
        *,
        detail: Optional[str] = None,
        payload: Any = None,
    ) -> None:
        self.reason_code = str(reason_code or "").strip().lower() or "coverage_unavailable"
        self.detail = detail
        self.payload = payload
        message = detail or self.reason_code.replace("_", " ")
        super().__init__(message)


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_domain(value: object) -> str:
    normalized = str(value or "").strip().lower().replace("_", "-")
    if normalized not in {"finance", "market", "earnings", "price-target"}:
        raise ValueError(f"Unsupported Bronze domain={value!r}.")
    return normalized


def _normalize_reason_code(value: object) -> str:
    return str(value or "").strip().lower() or "provider_invalid_symbol"


def build_bronze_run_id(domain: str) -> str:
    normalized_domain = _normalize_domain(domain).replace("-", "_")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"bronze-{normalized_domain}-{timestamp}"


def domain_blacklist_path(domain: str) -> str:
    normalized_domain = _normalize_domain(domain)
    if normalized_domain == "market":
        return "market-data/blacklist.csv"
    if normalized_domain == "finance":
        return "finance-data/blacklist.csv"
    if normalized_domain == "earnings":
        prefix = getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data") or "earnings-data"
        return f"{str(prefix).strip().strip('/')}/blacklist.csv"
    return "price-target-data/blacklist.csv"


def invalid_candidate_marker_path(*, domain: str, symbol: str) -> str:
    normalized_domain = _normalize_domain(domain)
    normalized_symbol = _normalize_symbol(symbol)
    return f"{_INVALID_CANDIDATE_MARKER_PREFIX}/{normalized_domain}/{normalized_symbol}.json"


def load_invalid_candidate_marker(
    *,
    common_client: Any,
    domain: str,
    symbol: str,
) -> Optional[dict[str, Any]]:
    if common_client is None:
        return None
    path = invalid_candidate_marker_path(domain=domain, symbol=symbol)
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


def _store_invalid_candidate_marker(
    *,
    common_client: Any,
    domain: str,
    symbol: str,
    marker: dict[str, Any],
) -> None:
    if common_client is None:
        return
    raw = json.dumps(marker, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    mdc.store_raw_bytes(raw, invalid_candidate_marker_path(domain=domain, symbol=symbol), client=common_client)


def clear_invalid_candidate_marker(
    *,
    common_client: Any,
    domain: str,
    symbol: str,
) -> bool:
    if common_client is None:
        return False
    marker = load_invalid_candidate_marker(common_client=common_client, domain=domain, symbol=symbol)
    if not marker:
        return False
    if str(marker.get("status") or "").strip().lower() == "promoted":
        return False
    path = invalid_candidate_marker_path(domain=domain, symbol=symbol)
    common_client.delete_file(path)
    return True


def is_explicit_invalid_candidate(exc: BaseException) -> bool:
    if isinstance(exc, AlphaVantageGatewayInvalidSymbolError):
        return True
    if isinstance(exc, MassiveGatewayNotFoundError):
        return getattr(exc, "status_code", None) == 404
    return False


def record_invalid_symbol_candidate(
    *,
    common_client: Any,
    bronze_client: Any,
    domain: str,
    symbol: str,
    provider: str,
    reason_code: str,
    run_id: str,
    promotion_threshold: int = _PROMOTION_THRESHOLD,
) -> dict[str, Any]:
    normalized_domain = _normalize_domain(domain)
    normalized_symbol = _normalize_symbol(symbol)
    normalized_reason = _normalize_reason_code(reason_code)
    observed_at = datetime.now(timezone.utc).isoformat()
    threshold = max(1, int(promotion_threshold))
    marker = load_invalid_candidate_marker(common_client=common_client, domain=normalized_domain, symbol=normalized_symbol)
    if isinstance(marker, dict) and str(marker.get("status") or "").strip().lower() == "promoted":
        return {
            "promoted": False,
            "already_promoted": True,
            "observedRunCount": int(marker.get("observedRunCount", threshold) or threshold),
            "blacklistPath": str(marker.get("blacklistPath") or "").strip() or domain_blacklist_path(normalized_domain),
        }

    first_observed_at = observed_at
    observed_run_count = 0
    marker_reason = None
    if isinstance(marker, dict):
        marker_reason = _normalize_reason_code(marker.get("reasonCode"))
        first_observed_at = str(marker.get("firstObservedAt") or observed_at)
        observed_run_count = int(marker.get("observedRunCount", 0) or 0)
    if marker_reason and marker_reason != normalized_reason:
        first_observed_at = observed_at
        observed_run_count = 0

    if not isinstance(marker, dict) or str(marker.get("lastRunId") or "").strip() != str(run_id).strip():
        observed_run_count += 1

    promoted = observed_run_count >= threshold
    promoted_at = observed_at if promoted else None
    blacklist_path = domain_blacklist_path(normalized_domain) if promoted else None
    if promoted:
        with _BLACKLIST_UPDATE_LOCK:
            mdc.update_csv_set(blacklist_path, normalized_symbol, client=bronze_client)

    marker_payload = {
        "layer": "bronze",
        "domain": normalized_domain,
        "symbol": normalized_symbol,
        "provider": str(provider or "").strip().lower() or "unknown",
        "status": "promoted" if promoted else "candidate",
        "reasonCode": normalized_reason,
        "observedRunCount": observed_run_count,
        "firstObservedAt": first_observed_at,
        "lastObservedAt": observed_at,
        "lastRunId": str(run_id or "").strip(),
        "promotedAt": promoted_at,
        "blacklistPath": blacklist_path,
    }
    _store_invalid_candidate_marker(
        common_client=common_client,
        domain=normalized_domain,
        symbol=normalized_symbol,
        marker=marker_payload,
    )
    return {
        "promoted": promoted,
        "already_promoted": False,
        "observedRunCount": observed_run_count,
        "blacklistPath": blacklist_path,
    }
