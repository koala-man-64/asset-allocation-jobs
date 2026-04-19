from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

from asset_allocation_runtime_common.foundation import config as cfg
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.providers.alpha_vantage_gateway_client import AlphaVantageGatewayInvalidSymbolError
from asset_allocation_runtime_common.providers.massive_gateway_client import MassiveGatewayNotFoundError

_INVALID_CANDIDATE_MARKER_PREFIX = "system/invalid_symbol_candidates/bronze"
_PROMOTION_THRESHOLD = 2
_BLACKLIST_UPDATE_LOCK = threading.Lock()
_PROMOTED_STATUS = "promoted"
_CANDIDATE_STATUS = "candidate"


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


def _normalize_reprobe_outcome(value: object) -> str:
    return str(value or "").strip().lower().replace(" ", "_") or "unknown"


def _parse_marker_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _require_common_client(common_client: Any) -> None:
    if common_client is None:
        raise RuntimeError("Common storage client is unavailable for Bronze symbol-policy state.")


def _require_bronze_client(bronze_client: Any) -> None:
    if bronze_client is None:
        raise RuntimeError("Bronze storage client is unavailable for Bronze symbol-policy state.")


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


def validate_bronze_storage_clients(
    *,
    bronze_container_name: str,
    common_container_name: str,
    bronze_client: Any,
    common_client: Any,
) -> None:
    if not str(bronze_container_name or "").strip():
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")
    if not str(common_container_name or "").strip():
        raise ValueError("Environment variable 'AZURE_CONTAINER_COMMON' is strictly required.")
    if bronze_client is None:
        raise RuntimeError(
            f"Bronze storage client is unavailable for container '{str(bronze_container_name).strip()}'."
        )
    if common_client is None:
        raise RuntimeError(
            f"Common storage client is unavailable for container '{str(common_container_name).strip()}'."
        )


def load_invalid_candidate_marker(
    *,
    common_client: Any,
    domain: str,
    symbol: str,
) -> Optional[dict[str, Any]]:
    _require_common_client(common_client)
    path = invalid_candidate_marker_path(domain=domain, symbol=symbol)
    try:
        raw = mdc.read_raw_bytes(path, client=common_client, missing_ok=True)
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise RuntimeError(f"Failed reading Bronze invalid-symbol marker '{path}': {exc}") from exc
    if not raw:
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Bronze invalid-symbol marker '{path}' contains invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Bronze invalid-symbol marker '{path}' must decode to a JSON object.")
    return parsed


def _store_invalid_candidate_marker(
    *,
    common_client: Any,
    domain: str,
    symbol: str,
    marker: dict[str, Any],
) -> None:
    _require_common_client(common_client)
    raw = json.dumps(marker, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    mdc.store_raw_bytes(raw, invalid_candidate_marker_path(domain=domain, symbol=symbol), client=common_client)


def _promoted_marker_sort_key(marker: dict[str, Any]) -> tuple[int, datetime, datetime]:
    last_reprobe_at = _parse_marker_timestamp(marker.get("lastReprobeAt"))
    promoted_at = _parse_marker_timestamp(marker.get("promotedAt")) or datetime.min.replace(tzinfo=timezone.utc)
    if last_reprobe_at is None:
        return (0, promoted_at, promoted_at)
    return (1, last_reprobe_at, promoted_at)


def list_promoted_invalid_candidate_markers(
    *,
    common_client: Any,
    domain: str,
) -> list[dict[str, Any]]:
    _require_common_client(common_client)
    normalized_domain = _normalize_domain(domain)
    prefix = f"{_INVALID_CANDIDATE_MARKER_PREFIX}/{normalized_domain}/"
    promoted_markers: list[dict[str, Any]] = []
    try:
        blob_infos = list(common_client.list_blob_infos(name_starts_with=prefix))
    except Exception as exc:
        raise RuntimeError(f"Failed listing Bronze invalid-symbol markers under '{prefix}': {exc}") from exc

    for blob in blob_infos:
        path = str((blob or {}).get("name") or "").strip()
        if not path.endswith(".json") or not path.startswith(prefix):
            continue
        symbol = path[len(prefix) : -5].strip()
        if not symbol:
            continue
        marker = load_invalid_candidate_marker(common_client=common_client, domain=normalized_domain, symbol=symbol)
        if not marker:
            continue
        if str(marker.get("status") or "").strip().lower() != _PROMOTED_STATUS:
            continue
        promoted_markers.append(
            {
                **marker,
                "domain": normalized_domain,
                "symbol": _normalize_symbol(marker.get("symbol") or symbol),
                "path": path,
            }
        )

    promoted_markers.sort(key=_promoted_marker_sort_key)
    return promoted_markers


def remove_symbol_from_domain_blacklist(
    *,
    bronze_client: Any,
    domain: str,
    symbol: str,
    blacklist_path: str | None = None,
) -> bool:
    _require_bronze_client(bronze_client)
    normalized_domain = _normalize_domain(domain)
    normalized_symbol = _normalize_symbol(symbol)
    resolved_blacklist_path = str(blacklist_path or "").strip() or domain_blacklist_path(normalized_domain)
    with _BLACKLIST_UPDATE_LOCK:
        try:
            existing_symbols = mdc.load_ticker_list(resolved_blacklist_path, client=bronze_client)
        except FileNotFoundError:
            return False
        normalized_existing = [_normalize_symbol(item) for item in existing_symbols if _normalize_symbol(item)]
        if normalized_symbol not in normalized_existing:
            return False
        remaining = sorted(item for item in set(normalized_existing) if item != normalized_symbol)
        mdc.store_csv(pd.DataFrame(remaining, columns=["Symbol"]), resolved_blacklist_path, client=bronze_client)
    return True


def record_promoted_symbol_reprobe_attempt(
    *,
    common_client: Any,
    domain: str,
    symbol: str,
    outcome: str,
) -> dict[str, Any]:
    normalized_domain = _normalize_domain(domain)
    normalized_symbol = _normalize_symbol(symbol)
    marker = load_invalid_candidate_marker(common_client=common_client, domain=normalized_domain, symbol=normalized_symbol)
    if not marker:
        raise RuntimeError(
            f"Promoted Bronze invalid-symbol marker is missing for domain='{normalized_domain}' symbol='{normalized_symbol}'."
        )
    status = str(marker.get("status") or "").strip().lower()
    if status != _PROMOTED_STATUS:
        raise RuntimeError(
            f"Expected promoted Bronze invalid-symbol marker for domain='{normalized_domain}' symbol='{normalized_symbol}', "
            f"but found status='{status or 'missing'}'."
        )

    updated = dict(marker)
    updated["lastReprobeAt"] = datetime.now(timezone.utc).isoformat()
    updated["lastReprobeOutcome"] = _normalize_reprobe_outcome(outcome)
    updated["reprobeAttemptCount"] = int(updated.get("reprobeAttemptCount", 0) or 0) + 1
    _store_invalid_candidate_marker(
        common_client=common_client,
        domain=normalized_domain,
        symbol=normalized_symbol,
        marker=updated,
    )
    return updated


def clear_invalid_symbol_state_on_success(
    *,
    common_client: Any,
    bronze_client: Any,
    domain: str,
    symbol: str,
) -> dict[str, Any]:
    normalized_domain = _normalize_domain(domain)
    normalized_symbol = _normalize_symbol(symbol)
    marker = load_invalid_candidate_marker(common_client=common_client, domain=normalized_domain, symbol=normalized_symbol)
    if not marker:
        return {"cleared": False, "recovered": False, "blacklistPath": None}

    status = str(marker.get("status") or "").strip().lower()
    path = invalid_candidate_marker_path(domain=normalized_domain, symbol=normalized_symbol)
    blacklist_path = str(marker.get("blacklistPath") or "").strip() or domain_blacklist_path(normalized_domain)
    if status == _PROMOTED_STATUS:
        remove_symbol_from_domain_blacklist(
            bronze_client=bronze_client,
            domain=normalized_domain,
            symbol=normalized_symbol,
            blacklist_path=blacklist_path,
        )
        common_client.delete_file(path)
        return {"cleared": True, "recovered": True, "blacklistPath": blacklist_path}

    common_client.delete_file(path)
    return {"cleared": True, "recovered": False, "blacklistPath": None}


def clear_invalid_candidate_marker(
    *,
    common_client: Any,
    bronze_client: Any = None,
    domain: str,
    symbol: str,
) -> bool:
    result = clear_invalid_symbol_state_on_success(
        common_client=common_client,
        bronze_client=bronze_client,
        domain=domain,
        symbol=symbol,
    )
    return bool(result.get("cleared"))


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
    if isinstance(marker, dict) and str(marker.get("status") or "").strip().lower() == _PROMOTED_STATUS:
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
        "status": _PROMOTED_STATUS if promoted else _CANDIDATE_STATUS,
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
