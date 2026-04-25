from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from asset_allocation_runtime_common.market_data import core as mdc

try:
    from asset_allocation_runtime_common.providers.quiver_gateway_client import QuiverGatewayClient
except Exception:  # pragma: no cover - runtime package pin can lag during local multi-repo work
    QuiverGatewayClient = None  # type: ignore[assignment]

from tasks.common.job_status import resolve_job_run_status
from tasks.common.watermarks import load_watermarks, save_last_success, save_watermarks
from tasks.quiver_data import constants
from tasks.quiver_data.config import QuiverDataConfig
from tasks.quiver_data.storage import computed_at_iso, write_domain_artifact
from tasks.quiver_data.transform import bucket_rows
from tasks.quiver_data.universe import resolve_symbol_universe

_CURSOR_KEY_PREFIX = "quiver_bronze_cursor"
_UNIVERSE_SOURCE = "core_symbols"


@dataclass(frozen=True)
class SymbolBatchPlan:
    universe_symbols: tuple[str, ...]
    selected_symbols: tuple[str, ...]
    batch_size: int
    cursor_key: str
    cursor_start: int
    cursor_end: int
    cursor_next: int


@dataclass(frozen=True)
class RequestFetchResult:
    rows: list[dict[str, Any]]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class QuiverSourceRequest:
    source_dataset: str
    dataset_family: str
    requested_symbol: str | None
    paginated: bool
    fetch: Callable[[], RequestFetchResult]


class QuiverRequestFetchError(RuntimeError):
    def __init__(self, message: str, *, metadata: dict[str, Any]) -> None:
        super().__init__(message)
        self.metadata = metadata


class PaginationLimitExceeded(QuiverRequestFetchError):
    pass


def _run_id() -> str:
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip()
    if execution_name:
        return execution_name
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"quiver-bronze-{stamp}-{os.getpid()}"


def _runtime_job_name(default_name: str) -> str:
    configured = str(os.environ.get("CONTAINER_APP_JOB_NAME") or "").strip()
    return configured or default_name


def _last_success_key(job_mode: str) -> str:
    return f"bronze_quiver_data_{str(job_mode or '').strip().lower()}"


def _cursor_watermark_key(job_mode: str) -> str:
    return f"{_CURSOR_KEY_PREFIX}_{str(job_mode or '').strip().lower()}"


def _batch_size_for_mode(config: QuiverDataConfig) -> int:
    return max(1, config.symbol_batch_size())


def plan_symbol_batch(config: QuiverDataConfig, *, universe_symbols: tuple[str, ...], cursor_next: int) -> SymbolBatchPlan:
    cursor_key = _cursor_watermark_key(config.job_mode)
    batch_size = _batch_size_for_mode(config)
    if not universe_symbols:
        return SymbolBatchPlan(
            universe_symbols=(),
            selected_symbols=(),
            batch_size=batch_size,
            cursor_key=cursor_key,
            cursor_start=0,
            cursor_end=0,
            cursor_next=0,
        )

    universe_size = len(universe_symbols)
    cursor_start = max(0, int(cursor_next)) % universe_size
    count = min(batch_size, universe_size)
    selected_symbols = tuple(universe_symbols[(cursor_start + offset) % universe_size] for offset in range(count))
    next_index = (cursor_start + count) % universe_size
    cursor_end = (next_index - 1) % universe_size if count else cursor_start
    return SymbolBatchPlan(
        universe_symbols=universe_symbols,
        selected_symbols=selected_symbols,
        batch_size=batch_size,
        cursor_key=cursor_key,
        cursor_start=cursor_start,
        cursor_end=cursor_end,
        cursor_next=next_index,
    )


def _load_symbol_batch_plan(config: QuiverDataConfig) -> SymbolBatchPlan:
    universe_symbols = resolve_symbol_universe(config)
    cursor_items = load_watermarks(_cursor_watermark_key(config.job_mode)) if universe_symbols else {}
    raw_cursor_next = cursor_items.get("next_index", 0) if isinstance(cursor_items, dict) else 0
    try:
        cursor_next = int(raw_cursor_next or 0)
    except Exception:
        cursor_next = 0
    return plan_symbol_batch(config, universe_symbols=universe_symbols, cursor_next=cursor_next)


def _persist_symbol_batch_plan(plan: SymbolBatchPlan, *, job_mode: str) -> None:
    if not plan.universe_symbols:
        return
    save_watermarks(
        plan.cursor_key,
        {
            "next_index": plan.cursor_next,
            "cursor_start": plan.cursor_start,
            "cursor_end": plan.cursor_end,
            "selected_symbols": list(plan.selected_symbols),
            "universe_size": len(plan.universe_symbols),
            "job_mode": job_mode,
            "updated_at": computed_at_iso(),
        },
    )


def _payload_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    return [dict(item) for item in payload if isinstance(item, dict)]


def _request_metadata(
    request: QuiverSourceRequest,
    *,
    pages_fetched: int,
    rows_fetched: int,
    stop_reason: str,
    cap_hit: bool = False,
    failed_page: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "sourceDataset": request.source_dataset,
        "datasetFamily": request.dataset_family,
        "requestedSymbol": request.requested_symbol,
        "paginated": request.paginated,
        "pagesFetched": int(pages_fetched),
        "rowsFetched": int(rows_fetched),
        "pageSize": None,
        "maxPages": None,
        "stopReason": stop_reason,
        "capHit": bool(cap_hit),
    }
    if failed_page is not None:
        metadata["failedPage"] = int(failed_page)
    if error_type:
        metadata["errorType"] = str(error_type)
    if error_message:
        metadata["errorMessage"] = str(error_message)[:240]
    return metadata


def _request_metadata_with_config(
    request: QuiverSourceRequest,
    config: QuiverDataConfig,
    *,
    pages_fetched: int,
    rows_fetched: int,
    stop_reason: str,
    cap_hit: bool = False,
    failed_page: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    metadata = _request_metadata(
        request,
        pages_fetched=pages_fetched,
        rows_fetched=rows_fetched,
        stop_reason=stop_reason,
        cap_hit=cap_hit,
        failed_page=failed_page,
        error_type=error_type,
        error_message=error_message,
    )
    metadata["pageSize"] = config.page_size if request.paginated else None
    metadata["maxPages"] = config.max_pages_per_request if request.paginated else None
    return metadata


def _fetch_single_request(request: QuiverSourceRequest, callback: Callable[[], Any]) -> RequestFetchResult:
    try:
        rows = _payload_rows(callback())
    except Exception as exc:
        metadata = _request_metadata(
            request,
            pages_fetched=0,
            rows_fetched=0,
            stop_reason="failed",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise QuiverRequestFetchError(f"{request.source_dataset} failed: {type(exc).__name__}: {exc}", metadata=metadata) from exc
    metadata = _request_metadata(
        request,
        pages_fetched=1,
        rows_fetched=len(rows),
        stop_reason="single_request",
    )
    return RequestFetchResult(rows=rows, metadata=metadata)


def _fetch_paginated_request(
    request: QuiverSourceRequest,
    config: QuiverDataConfig,
    page_callback: Callable[[int], Any],
) -> RequestFetchResult:
    rows: list[dict[str, Any]] = []
    pages_fetched = 0
    page = 1
    while True:
        try:
            payload = page_callback(page)
        except Exception as exc:
            metadata = _request_metadata_with_config(
                request,
                config,
                pages_fetched=pages_fetched,
                rows_fetched=len(rows),
                stop_reason="failed",
                failed_page=page,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise QuiverRequestFetchError(f"{request.source_dataset} page {page} failed: {type(exc).__name__}: {exc}", metadata=metadata) from exc

        page_rows = _payload_rows(payload)
        rows.extend(page_rows)
        pages_fetched += 1

        if not isinstance(payload, list):
            stop_reason = "non_list_payload"
            break
        if not page_rows:
            stop_reason = "empty_page"
            break
        if len(page_rows) < config.page_size:
            stop_reason = "short_page"
            break
        if config.max_pages_per_request > 0 and page >= config.max_pages_per_request:
            metadata = _request_metadata_with_config(
                request,
                config,
                pages_fetched=pages_fetched,
                rows_fetched=len(rows),
                stop_reason="max_pages_reached",
                cap_hit=True,
            )
            raise PaginationLimitExceeded(
                (
                    f"{request.source_dataset} ({request.requested_symbol or 'all'}) reached "
                    f"QUIVER_DATA_MAX_PAGES_PER_REQUEST={config.max_pages_per_request} "
                    f"with a full page of {config.page_size} rows"
                ),
                metadata=metadata,
            )
        page += 1

    metadata = _request_metadata_with_config(
        request,
        config,
        pages_fetched=pages_fetched,
        rows_fetched=len(rows),
        stop_reason=stop_reason,
    )
    return RequestFetchResult(rows=rows, metadata=metadata)


def _single_request(
    source_dataset: str,
    dataset_family: str,
    requested_symbol: str | None,
    callback: Callable[[], Any],
) -> QuiverSourceRequest:
    def fetch() -> RequestFetchResult:
        return _fetch_single_request(request, callback)

    request = QuiverSourceRequest(
        source_dataset=source_dataset,
        dataset_family=dataset_family,
        requested_symbol=requested_symbol,
        paginated=False,
        fetch=fetch,
    )
    return request


def _paginated_request(
    source_dataset: str,
    dataset_family: str,
    requested_symbol: str | None,
    config: QuiverDataConfig,
    page_callback: Callable[[int], Any],
) -> QuiverSourceRequest:
    def fetch() -> RequestFetchResult:
        return _fetch_paginated_request(request, config, page_callback)

    request = QuiverSourceRequest(
        source_dataset=source_dataset,
        dataset_family=dataset_family,
        requested_symbol=requested_symbol,
        paginated=True,
        fetch=fetch,
    )
    return request


def _log_request_fetch(metadata: dict[str, Any]) -> None:
    if not metadata.get("paginated"):
        return
    mdc.write_line(
        "Quiver pagination summary: "
        f"source_dataset={metadata.get('sourceDataset')} "
        f"requested_symbol={metadata.get('requestedSymbol') or 'all'} "
        f"pages_fetched={metadata.get('pagesFetched')} "
        f"rows_fetched={metadata.get('rowsFetched')} "
        f"stop_reason={metadata.get('stopReason')} "
        f"cap_hit={str(metadata.get('capHit')).lower()}"
    )


def _build_incremental_live_requests(client: Any, config: QuiverDataConfig) -> list[QuiverSourceRequest]:
    return [
        _single_request("congress_trading_live", "political_trading", None, lambda: client.get_live_congress_trading()),
        _single_request("senate_trading_live", "political_trading", None, lambda: client.get_live_senate_trading()),
        _single_request("house_trading_live", "political_trading", None, lambda: client.get_live_house_trading()),
        _single_request("government_contracts_live", "government_contracts", None, lambda: client.get_live_gov_contracts()),
        _paginated_request(
            "government_contracts_all_live",
            "government_contracts_all",
            None,
            config,
            lambda page: client.get_live_gov_contracts_all(page=page, page_size=config.page_size),
        ),
        _paginated_request(
            "lobbying_live",
            "lobbying",
            None,
            config,
            lambda page: client.get_live_lobbying(page=page, page_size=config.page_size),
        ),
        _single_request("congress_holdings_live", "congress_holdings", None, lambda: client.get_live_congress_holdings()),
        _paginated_request(
            "insiders_live_all",
            "insider_trading",
            None,
            config,
            lambda page: client.get_live_insiders(page=page, page_size=config.page_size),
        ),
        _single_request("wall_street_bets_live", "wall_street_bets", None, lambda: client.get_live_wall_street_bets()),
        _single_request("patents_live", "patents", None, lambda: client.get_live_patents()),
    ]


def _build_incremental_ticker_requests(
    client: Any,
    config: QuiverDataConfig,
    *,
    selected_symbols: tuple[str, ...],
) -> list[QuiverSourceRequest]:
    requests: list[QuiverSourceRequest] = []
    for ticker in selected_symbols:
        requests.extend(
            [
                _paginated_request(
                    "insiders_live",
                    "insider_trading",
                    ticker,
                    config,
                    lambda page, ticker=ticker: client.get_live_insiders(
                        ticker=ticker,
                        page=page,
                        page_size=config.page_size,
                    ),
                ),
                _paginated_request(
                    "sec13f_live",
                    "institutional_holdings",
                    ticker,
                    config,
                    lambda page, ticker=ticker: client.get_live_sec13f(
                        ticker=ticker,
                        today=config.sec13f_today_only,
                        page=page,
                        page_size=config.page_size,
                    ),
                ),
                _paginated_request(
                    "sec13fchanges_live",
                    "institutional_holding_changes",
                    ticker,
                    config,
                    lambda page, ticker=ticker: client.get_live_sec13f_changes(
                        ticker=ticker,
                        today=config.sec13f_today_only,
                        page=page,
                        page_size=config.page_size,
                    ),
                ),
                _single_request("etf_holdings_live", "etf_holdings", ticker, lambda ticker=ticker: client.get_live_etf_holdings(ticker=ticker)),
            ]
        )
    return requests


def _build_historical_backfill_requests(
    client: Any,
    *,
    selected_symbols: tuple[str, ...],
    config: QuiverDataConfig,
) -> list[QuiverSourceRequest]:
    requests: list[QuiverSourceRequest] = [
        _single_request("wall_street_bets_historical_all", "wall_street_bets", None, lambda: client.get_live_wall_street_bets(count_all=True)),
    ]
    for ticker in selected_symbols:
        requests.extend(
            [
                _single_request(
                    "congress_trading_historical",
                    "political_trading",
                    ticker,
                    lambda ticker=ticker: client.get_historical_congress_trading(ticker=ticker),
                ),
                _single_request(
                    "senate_trading_historical",
                    "political_trading",
                    ticker,
                    lambda ticker=ticker: client.get_historical_senate_trading(ticker=ticker),
                ),
                _single_request(
                    "house_trading_historical",
                    "political_trading",
                    ticker,
                    lambda ticker=ticker: client.get_historical_house_trading(ticker=ticker),
                ),
                _single_request(
                    "government_contracts_historical",
                    "government_contracts",
                    ticker,
                    lambda ticker=ticker: client.get_historical_gov_contracts(ticker=ticker),
                ),
                _single_request(
                    "government_contracts_all_historical",
                    "government_contracts_all",
                    ticker,
                    lambda ticker=ticker: client.get_historical_gov_contracts_all(ticker=ticker),
                ),
                _paginated_request(
                    "lobbying_historical",
                    "lobbying",
                    ticker,
                    config,
                    lambda page, ticker=ticker: client.get_historical_lobbying(
                        ticker=ticker,
                        page=page,
                        page_size=config.page_size,
                    ),
                ),
                _single_request(
                    "wall_street_bets_historical",
                    "wall_street_bets",
                    ticker,
                    lambda ticker=ticker: client.get_historical_wall_street_bets(ticker=ticker),
                ),
                _single_request("patents_historical", "patents", ticker, lambda ticker=ticker: client.get_historical_patents(ticker=ticker)),
            ]
        )
    return requests


def _build_requests(
    client: Any,
    config: QuiverDataConfig,
    *,
    selected_symbols: tuple[str, ...] | None = None,
) -> list[QuiverSourceRequest]:
    symbols = tuple(selected_symbols or ())
    if config.job_mode == "historical_backfill":
        return _build_historical_backfill_requests(client, selected_symbols=symbols, config=config)
    return _build_incremental_live_requests(client, config) + _build_incremental_ticker_requests(client, config, selected_symbols=symbols)


def main() -> int:
    mdc.log_environment_diagnostics()
    if QuiverGatewayClient is None:
        raise RuntimeError("QuiverGatewayClient is unavailable. Update asset-allocation-runtime-common before running Quiver jobs.")

    config = QuiverDataConfig.from_env()
    bronze_client = mdc.get_storage_client(config.bronze_container)
    if bronze_client is None:
        raise RuntimeError(f"Storage client unavailable for container {config.bronze_container!r}.")

    gateway_client = QuiverGatewayClient.from_env()
    run_id = _run_id()
    job_name = _runtime_job_name(
        constants.BRONZE_BACKFILL_JOB_NAME if config.job_mode == "historical_backfill" else constants.BRONZE_JOB_NAME
    )
    symbol_batch_plan = _load_symbol_batch_plan(config)
    batch_paths: list[str] = []
    request_fetches: list[dict[str, Any]] = []
    warnings: list[str] = []
    failures: list[str] = []

    try:
        for request in _build_requests(
            gateway_client,
            config,
            selected_symbols=symbol_batch_plan.selected_symbols,
        ):
            try:
                result = request.fetch()
                request_fetches.append(result.metadata)
                _log_request_fetch(result.metadata)
                batches = bucket_rows(
                    request.source_dataset,
                    request.dataset_family,
                    result.rows,
                    requested_symbol=request.requested_symbol,
                )
                for bucket, batch in batches.items():
                    path = constants.bronze_raw_path(run_id, request.source_dataset, bucket)
                    mdc.save_json_content(batch, path, client=bronze_client)
                    batch_paths.append(path)
            except QuiverRequestFetchError as exc:
                request_fetches.append(dict(exc.metadata))
                _log_request_fetch(exc.metadata)
                message = f"{request.source_dataset} ({request.requested_symbol or 'all'}) failed: {type(exc).__name__}: {exc}"
                mdc.write_warning(message)
                failures.append(message)
            except Exception as exc:
                metadata = _request_metadata_with_config(
                    request,
                    config,
                    pages_fetched=0,
                    rows_fetched=0,
                    stop_reason="failed",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                request_fetches.append(metadata)
                _log_request_fetch(metadata)
                message = f"{request.source_dataset} ({request.requested_symbol or 'all'}) failed: {type(exc).__name__}: {exc}"
                mdc.write_warning(message)
                failures.append(message)
    finally:
        gateway_client.close()

    manifest = {
        "version": 1,
        "runId": run_id,
        "layer": "bronze",
        "domain": constants.domain_slug_for_layer("bronze"),
        "jobName": job_name,
        "jobMode": config.job_mode,
        "universeSource": _UNIVERSE_SOURCE,
        "updatedAt": computed_at_iso(),
        "batchPaths": batch_paths,
        "warnings": warnings,
        "failures": failures,
        "requestFetches": request_fetches,
        "selectedSymbols": list(symbol_batch_plan.selected_symbols),
        "universeSymbolCount": len(symbol_batch_plan.universe_symbols),
        "symbolBatchSize": symbol_batch_plan.batch_size,
        "cursorStart": symbol_batch_plan.cursor_start,
        "cursorEnd": symbol_batch_plan.cursor_end,
        "cursorNext": symbol_batch_plan.cursor_next,
    }
    mdc.save_json_content(manifest, constants.bronze_manifest_path(run_id), client=bronze_client)
    write_domain_artifact(
        client=bronze_client,
        layer="bronze",
        job_name=job_name,
        run_id=run_id,
        tables={},
        extra_metadata=manifest,
    )
    _persist_symbol_batch_plan(symbol_batch_plan, job_mode=config.job_mode)

    status, exit_code = resolve_job_run_status(failed_count=len(failures), warning_count=len(warnings))
    save_last_success(
        _last_success_key(config.job_mode),
        metadata={
            "run_id": run_id,
            "status": status,
            "job_mode": config.job_mode,
            "universe_source": _UNIVERSE_SOURCE,
            "batch_count": len(batch_paths),
            "selected_symbols": list(symbol_batch_plan.selected_symbols),
            "universe_symbol_count": len(symbol_batch_plan.universe_symbols),
            "failures": failures,
        },
    )
    return exit_code


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = _runtime_job_name(constants.BRONZE_JOB_NAME)
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(
                        layer="bronze",
                        domain=constants.domain_slug_for_layer("bronze"),
                        job_name=job_name,
                    ),
                    trigger_next_job_from_env,
                ),
            )
        )
