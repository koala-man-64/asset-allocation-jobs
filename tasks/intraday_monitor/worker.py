from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport
from asset_allocation_runtime_common.providers.massive_gateway_client import MassiveGatewayClient

from tasks.common.intraday_contracts_compat import (
    IntradayMonitorClaimRequest,
    IntradayMonitorClaimResponse,
    IntradayMonitorCompleteRequest,
    IntradayMonitorEvent,
    IntradayMonitorFailRequest,
    IntradayMonitorRunSummary,
    IntradaySymbolStatus,
    IntradayWatchlistDetail,
)

logger = logging.getLogger("asset-allocation.tasks.intraday-monitor")

_SNAPSHOT_BATCH_SIZE = 250
_SNAPSHOT_ASSET_TYPE = "stocks"


def _execution_name() -> str | None:
    value = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip()
    return value or None


def _log_lifecycle(phase: str, **fields: object) -> None:
    parts = [f"phase={phase}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    logger.info("intraday_monitor_event %s", " ".join(parts))


def _safe_close(client: object) -> None:
    close = getattr(client, "close", None)
    if callable(close):
        close()


def _chunk_symbols(symbols: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size))
    return [symbols[index : index + size] for index in range(0, len(symbols), size)]


def _normalize_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise ValueError("Symbol values must be non-empty.")
    return symbol


def _normalize_key(name: object) -> str:
    return "".join(ch for ch in str(name or "").strip().lower() if ch.isalnum())


def _extract_payload_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list):
            return [row for row in results if isinstance(row, dict)]
        if isinstance(results, dict):
            return [results]
        return [payload]
    return []


def _extract_snapshot_symbol(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("ticker", "symbol"):
        symbol = str(payload.get(key) or "").strip().upper()
        if symbol:
            return symbol

    details = payload.get("details")
    if isinstance(details, dict):
        for key in ("ticker", "symbol"):
            symbol = str(details.get(key) or "").strip().upper()
            if symbol:
                return symbol
    return None


def _extract_first_numeric(payload: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    normalized = {_normalize_key(key): value for key, value in payload.items()}
    for key in keys:
        raw = normalized.get(_normalize_key(key))
        if raw is None:
            continue
        try:
            return float(raw)
        except Exception:
            continue
    return None


def _extract_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        seconds = float(value)
        if abs(seconds) > 10_000_000_000:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
        parsed = datetime.fromisoformat(text)
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    except Exception:
        return None


def _extract_snapshot_observation(
    payload: dict[str, Any],
    *,
    observed_at: datetime,
) -> tuple[datetime, float | None]:
    candidate_blocks: list[dict[str, Any]] = []
    for key in ("last_trade", "lastTrade", "session", "day", "daily_bar", "bar"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidate_blocks.append(nested)
    candidate_blocks.append(payload)

    timestamp = None
    price = None
    for block in candidate_blocks:
        if price is None:
            price = _extract_first_numeric(block, ("price", "last", "close", "c", "value"))
        if timestamp is None:
            for key in ("updated", "timestamp", "t", "time", "as_of", "window_start"):
                timestamp = _extract_datetime(block.get(key))
                if timestamp is not None:
                    break
        if price is not None and timestamp is not None:
            break

    return timestamp or observed_at, price


def _fetch_snapshot_rows(symbols: list[str]) -> dict[str, dict[str, Any]]:
    client = MassiveGatewayClient.from_env()
    rows_by_symbol: dict[str, dict[str, Any]] = {}
    try:
        for chunk in _chunk_symbols(symbols, _SNAPSHOT_BATCH_SIZE):
            payload = client.get_unified_snapshot(symbols=chunk, asset_type=_SNAPSHOT_ASSET_TYPE)
            for row in _extract_payload_rows(payload):
                symbol = _extract_snapshot_symbol(row)
                if not symbol:
                    continue
                rows_by_symbol[symbol] = row
    finally:
        _safe_close(client)
    return rows_by_symbol


def _refresh_due(
    *,
    run: IntradayMonitorRunSummary,
    watchlist: IntradayWatchlistDetail,
    current_status: IntradaySymbolStatus | None,
    observed_at: datetime,
) -> bool:
    if run.forceRefresh:
        return True
    if not watchlist.autoRefreshEnabled:
        return False
    if current_status is None or current_status.lastSuccessfulMarketRefreshAt is None:
        return True
    last_refresh = current_status.lastSuccessfulMarketRefreshAt
    if last_refresh.tzinfo is None:
        last_refresh = last_refresh.replace(tzinfo=UTC)
    return last_refresh <= observed_at - timedelta(minutes=watchlist.refreshCooldownMinutes)


def _build_completion_payload(
    *,
    run: IntradayMonitorRunSummary,
    watchlist: IntradayWatchlistDetail,
    current_statuses: list[IntradaySymbolStatus],
) -> tuple[list[IntradaySymbolStatus], list[IntradayMonitorEvent], list[str]]:
    observed_at = datetime.now(UTC)
    current_status_by_symbol = {item.symbol: item for item in current_statuses}
    snapshot_rows = _fetch_snapshot_rows(list(watchlist.symbols))

    symbol_statuses: list[IntradaySymbolStatus] = []
    events: list[IntradayMonitorEvent] = []
    refresh_symbols: list[str] = []

    for raw_symbol in watchlist.symbols:
        symbol = _normalize_symbol(raw_symbol)
        current_status = current_status_by_symbol.get(symbol)
        snapshot_row = snapshot_rows.get(symbol)
        if snapshot_row is None:
            symbol_statuses.append(
                IntradaySymbolStatus(
                    watchlistId=watchlist.watchlistId,
                    symbol=symbol,
                    monitorStatus="failed",
                    lastSuccessfulMarketRefreshAt=(
                        current_status.lastSuccessfulMarketRefreshAt if current_status is not None else None
                    ),
                    lastRunId=run.runId,
                    lastError="Snapshot payload missing for symbol.",
                )
            )
            events.append(
                IntradayMonitorEvent(
                    runId=run.runId,
                    watchlistId=watchlist.watchlistId,
                    symbol=symbol,
                    eventType="snapshot_missing",
                    severity="warning",
                    message="Snapshot payload missing for symbol.",
                    details={"symbol": symbol},
                )
            )
            continue

        last_snapshot_at, last_observed_price = _extract_snapshot_observation(snapshot_row, observed_at=observed_at)
        queue_refresh = _refresh_due(
            run=run,
            watchlist=watchlist,
            current_status=current_status,
            observed_at=observed_at,
        )
        monitor_status = "refresh_queued" if queue_refresh else "observed"
        if queue_refresh:
            refresh_symbols.append(symbol)

        symbol_statuses.append(
            IntradaySymbolStatus(
                watchlistId=watchlist.watchlistId,
                symbol=symbol,
                monitorStatus=monitor_status,
                lastSnapshotAt=last_snapshot_at,
                lastObservedPrice=last_observed_price,
                lastSuccessfulMarketRefreshAt=(
                    current_status.lastSuccessfulMarketRefreshAt if current_status is not None else None
                ),
                lastRunId=run.runId,
                lastError=None,
            )
        )
        events.append(
            IntradayMonitorEvent(
                runId=run.runId,
                watchlistId=watchlist.watchlistId,
                symbol=symbol,
                eventType="snapshot_observed",
                severity="info",
                message="Fetched latest snapshot.",
                details={
                    "symbol": symbol,
                    "queuedRefresh": queue_refresh,
                    "observedPrice": last_observed_price,
                },
            )
        )

    failed_count = sum(1 for item in symbol_statuses if item.monitorStatus == "failed")
    events.append(
        IntradayMonitorEvent(
            runId=run.runId,
            watchlistId=watchlist.watchlistId,
            eventType="snapshot_poll_completed",
            severity="warning" if failed_count else "info",
            message="Completed intraday snapshot poll.",
            details={
                "observedSymbolCount": len(symbol_statuses) - failed_count,
                "failedSymbolCount": failed_count,
                "eligibleRefreshCount": len(refresh_symbols),
                "forceRefresh": run.forceRefresh,
            },
        )
    )
    return symbol_statuses, events, refresh_symbols


def preflight_dependencies() -> None:
    transport = ControlPlaneTransport.from_env()
    try:
        transport.probe("/api/internal/intraday/ready")
    finally:
        transport.close()


def main() -> int:
    execution_name = _execution_name()
    try:
        preflight_dependencies()
    except Exception:
        logger.exception("Intraday monitor preflight failed.")
        return 1

    with ControlPlaneTransport.from_env() as transport:
        claim = IntradayMonitorClaimResponse.model_validate(
            transport.request_json(
                "POST",
                "/api/internal/intraday-monitor/claim",
                json_body=IntradayMonitorClaimRequest(executionName=execution_name).model_dump(
                    mode="json",
                    exclude_none=True,
                ),
            )
        )
        if claim.run is None or claim.watchlist is None or not claim.claimToken:
            logger.info("No queued intraday monitor runs found.")
            return 0

        run = claim.run
        watchlist = claim.watchlist
        _log_lifecycle(
            "claim",
            run_id=run.runId,
            watchlist_id=watchlist.watchlistId,
            execution_name=execution_name,
            symbol_count=len(watchlist.symbols),
            force_refresh=run.forceRefresh,
        )

        try:
            symbol_statuses, events, refresh_symbols = _build_completion_payload(
                run=run,
                watchlist=watchlist,
                current_statuses=list(claim.currentSymbolStatuses),
            )
            complete_payload = IntradayMonitorCompleteRequest(
                claimToken=claim.claimToken,
                symbolStatuses=symbol_statuses,
                events=events,
                refreshSymbols=refresh_symbols,
            )
            transport.request_json(
                "POST",
                f"/api/internal/intraday-monitor/runs/{run.runId}/complete",
                json_body=complete_payload.model_dump(mode="json", exclude_none=True),
            )
            _log_lifecycle(
                "complete",
                run_id=run.runId,
                watchlist_id=watchlist.watchlistId,
                observed_symbol_count=len(symbol_statuses),
                refresh_symbol_count=len(refresh_symbols),
            )
            return 0
        except Exception as exc:
            logger.exception("Intraday monitor run failed: run_id=%s", run.runId)
            try:
                transport.request_json(
                    "POST",
                    f"/api/internal/intraday-monitor/runs/{run.runId}/fail",
                    json_body=IntradayMonitorFailRequest(
                        claimToken=claim.claimToken,
                        error=str(exc),
                    ).model_dump(mode="json"),
                )
            except Exception:
                logger.exception("Intraday monitor failure reporting failed: run_id=%s", run.runId)
            return 1


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from asset_allocation_runtime_common.market_data import core as mdc

    job_name = "intraday-monitor-job"
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                log_info=logger.info,
                log_warning=logger.warning,
                log_error=logger.error,
                log_exception=logger.exception,
            )
        )
