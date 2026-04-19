from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable

from asset_allocation_runtime_common.market_data import core as mdc

try:
    from asset_allocation_runtime_common.providers.quiver_gateway_client import QuiverGatewayClient
except Exception:  # pragma: no cover - runtime package pin can lag during local multi-repo work
    QuiverGatewayClient = None  # type: ignore[assignment]

from tasks.common.job_status import resolve_job_run_status
from tasks.common.watermarks import save_last_success
from tasks.quiver_data import constants
from tasks.quiver_data.config import QuiverDataConfig
from tasks.quiver_data.storage import computed_at_iso, write_domain_artifact
from tasks.quiver_data.transform import bucket_rows


def _run_id() -> str:
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip()
    if execution_name:
        return execution_name
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"quiver-bronze-{stamp}-{os.getpid()}"


def _build_requests(client: Any, config: QuiverDataConfig) -> list[tuple[str, str, str | None, Callable[[], Any]]]:
    requests: list[tuple[str, str, str | None, Callable[[], Any]]] = [
        ("congress_trading_live", "political_trading", None, lambda: client.get_live_congress_trading()),
        ("senate_trading_live", "political_trading", None, lambda: client.get_live_senate_trading()),
        ("house_trading_live", "political_trading", None, lambda: client.get_live_house_trading()),
        ("government_contracts_live", "government_contracts", None, lambda: client.get_live_gov_contracts()),
        ("government_contracts_all_live", "government_contracts_all", None, lambda: client.get_live_gov_contracts_all(page=1, page_size=config.page_size)),
        ("lobbying_live", "lobbying", None, lambda: client.get_live_lobbying(page=1, page_size=config.page_size)),
        ("congress_holdings_live", "congress_holdings", None, lambda: client.get_live_congress_holdings()),
    ]

    for ticker in config.historical_tickers:
        requests.extend(
            [
                ("congress_trading_historical", "political_trading", ticker, lambda ticker=ticker: client.get_historical_congress_trading(ticker=ticker)),
                ("senate_trading_historical", "political_trading", ticker, lambda ticker=ticker: client.get_historical_senate_trading(ticker=ticker)),
                ("house_trading_historical", "political_trading", ticker, lambda ticker=ticker: client.get_historical_house_trading(ticker=ticker)),
                ("government_contracts_historical", "government_contracts", ticker, lambda ticker=ticker: client.get_historical_gov_contracts(ticker=ticker)),
                ("government_contracts_all_historical", "government_contracts_all", ticker, lambda ticker=ticker: client.get_historical_gov_contracts_all(ticker=ticker)),
                ("insiders_live", "insider_trading", ticker, lambda ticker=ticker: client.get_live_insiders(ticker=ticker, page=1, page_size=config.page_size)),
                ("sec13f_live", "institutional_holdings", ticker, lambda ticker=ticker: client.get_live_sec13f(ticker=ticker, today=config.sec13f_today_only, page=1, page_size=config.page_size)),
                ("sec13fchanges_live", "institutional_holding_changes", ticker, lambda ticker=ticker: client.get_live_sec13f_changes(ticker=ticker, today=config.sec13f_today_only, page=1, page_size=config.page_size)),
                ("lobbying_historical", "lobbying", ticker, lambda ticker=ticker: client.get_historical_lobbying(ticker=ticker, page=1, page_size=config.page_size)),
                ("etf_holdings_live", "etf_holdings", ticker, lambda ticker=ticker: client.get_live_etf_holdings(ticker=ticker)),
            ]
        )
    return requests


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
    batch_paths: list[str] = []
    warnings: list[str] = []
    failures: list[str] = []

    try:
        for source_dataset, dataset_family, requested_symbol, callback in _build_requests(gateway_client, config):
            try:
                payload = callback()
                rows = payload if isinstance(payload, list) else []
                batches = bucket_rows(source_dataset, dataset_family, rows, requested_symbol=requested_symbol)
                for bucket, batch in batches.items():
                    path = constants.bronze_raw_path(run_id, source_dataset, bucket)
                    mdc.save_json_content(batch, path, client=bronze_client)
                    batch_paths.append(path)
            except Exception as exc:
                message = f"{source_dataset} ({requested_symbol or 'all'}) failed: {type(exc).__name__}: {exc}"
                mdc.write_warning(message)
                failures.append(message)
    finally:
        gateway_client.close()

    manifest = {
        "version": 1,
        "runId": run_id,
        "layer": "bronze",
        "domain": constants.DOMAIN_SLUG,
        "updatedAt": computed_at_iso(),
        "batchPaths": batch_paths,
        "warnings": warnings,
        "failures": failures,
        "historicalTickers": list(config.historical_tickers),
    }
    mdc.save_json_content(manifest, constants.bronze_manifest_path(run_id), client=bronze_client)
    write_domain_artifact(
        client=bronze_client,
        layer="bronze",
        job_name=constants.BRONZE_JOB_NAME,
        run_id=run_id,
        tables={},
        extra_metadata=manifest,
    )

    status, exit_code = resolve_job_run_status(failed_count=len(failures), warning_count=len(warnings))
    save_last_success(
        "bronze_quiver_data",
        metadata={
            "run_id": run_id,
            "status": status,
            "batch_count": len(batch_paths),
            "historical_tickers": list(config.historical_tickers),
            "failures": failures,
        },
    )
    return exit_code


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = constants.BRONZE_JOB_NAME
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="bronze", domain="quiver-data", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
