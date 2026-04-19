from __future__ import annotations

import logging
import os
from contextlib import contextmanager

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport
from asset_allocation_runtime_common.shared_core import config as runtime_config

from tasks.common.intraday_contracts_compat import (
    IntradayRefreshClaimRequest,
    IntradayRefreshClaimResponse,
    IntradayRefreshCompleteRequest,
    IntradayRefreshFailRequest,
)
from tasks.market_data import bronze_market_data, silver_market_data, gold_market_data

logger = logging.getLogger("asset-allocation.tasks.intraday-refresh")


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
    logger.info("intraday_refresh_event %s", " ".join(parts))


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


@contextmanager
def _scoped_debug_symbols(symbols: list[str]):
    normalized = _normalize_symbols(symbols)
    prior_env = os.environ.get("DEBUG_SYMBOLS")
    had_runtime_symbols = "DEBUG_SYMBOLS" in runtime_config.__dict__
    prior_runtime_symbols = list(getattr(runtime_config, "DEBUG_SYMBOLS", []) or [])
    prior_settings_symbols = list(getattr(runtime_config.settings, "DEBUG_SYMBOLS", []) or [])
    had_silver_symbols = "DEBUG_SYMBOLS" in silver_market_data.cfg.__dict__
    prior_silver_symbols = list(getattr(silver_market_data.cfg, "DEBUG_SYMBOLS", []) or [])
    had_bronze_symbols = "DEBUG_SYMBOLS" in bronze_market_data.cfg.__dict__
    prior_bronze_symbols = list(getattr(bronze_market_data.cfg, "DEBUG_SYMBOLS", []) or [])

    scoped_value = ",".join(normalized)
    os.environ["DEBUG_SYMBOLS"] = scoped_value
    runtime_config.settings.DEBUG_SYMBOLS = list(normalized)
    runtime_config.DEBUG_SYMBOLS = list(normalized)
    bronze_market_data.cfg.DEBUG_SYMBOLS = list(normalized)
    silver_market_data.cfg.DEBUG_SYMBOLS = list(normalized)
    try:
        yield normalized
    finally:
        if prior_env is None:
            os.environ.pop("DEBUG_SYMBOLS", None)
        else:
            os.environ["DEBUG_SYMBOLS"] = prior_env
        runtime_config.settings.DEBUG_SYMBOLS = list(prior_settings_symbols)
        if had_runtime_symbols:
            runtime_config.DEBUG_SYMBOLS = list(prior_runtime_symbols)
        else:
            runtime_config.__dict__.pop("DEBUG_SYMBOLS", None)
        if had_bronze_symbols:
            bronze_market_data.cfg.DEBUG_SYMBOLS = list(prior_bronze_symbols)
        else:
            bronze_market_data.cfg.__dict__.pop("DEBUG_SYMBOLS", None)
        if had_silver_symbols:
            silver_market_data.cfg.DEBUG_SYMBOLS = list(prior_silver_symbols)
        else:
            silver_market_data.cfg.__dict__.pop("DEBUG_SYMBOLS", None)


def preflight_dependencies() -> None:
    transport = ControlPlaneTransport.from_env()
    try:
        transport.probe("/api/internal/intraday/ready")
    finally:
        transport.close()


def _run_market_refresh_pipeline(symbols: list[str]) -> None:
    with _scoped_debug_symbols(symbols):
        bronze_exit = bronze_market_data.main()
        if bronze_exit != 0:
            raise RuntimeError(f"Bronze market refresh failed with exit code {bronze_exit}.")
        silver_exit = silver_market_data.main()
        if silver_exit != 0:
            raise RuntimeError(f"Silver market refresh failed with exit code {silver_exit}.")
        gold_exit = gold_market_data.main()
        if gold_exit != 0:
            raise RuntimeError(f"Gold market refresh failed with exit code {gold_exit}.")


def main() -> int:
    execution_name = _execution_name()
    try:
        preflight_dependencies()
    except Exception:
        logger.exception("Intraday refresh preflight failed.")
        return 1

    with ControlPlaneTransport.from_env() as transport:
        claim = IntradayRefreshClaimResponse.model_validate(
            transport.request_json(
                "POST",
                "/api/internal/intraday-refresh/claim",
                json_body=IntradayRefreshClaimRequest(executionName=execution_name).model_dump(
                    mode="json",
                    exclude_none=True,
                ),
            )
        )
        if claim.batch is None or not claim.claimToken:
            logger.info("No queued intraday refresh batches found.")
            return 0

        batch = claim.batch
        _log_lifecycle(
            "claim",
            batch_id=batch.batchId,
            watchlist_id=batch.watchlistId,
            bucket=batch.bucketLetter,
            symbol_count=len(batch.symbols),
            execution_name=execution_name,
        )

        try:
            _run_market_refresh_pipeline(list(batch.symbols))
            transport.request_json(
                "POST",
                f"/api/internal/intraday-refresh/batches/{batch.batchId}/complete",
                json_body=IntradayRefreshCompleteRequest(claimToken=claim.claimToken).model_dump(mode="json"),
            )
            _log_lifecycle(
                "complete",
                batch_id=batch.batchId,
                watchlist_id=batch.watchlistId,
                symbol_count=len(batch.symbols),
            )
            return 0
        except Exception as exc:
            logger.exception("Intraday refresh batch failed: batch_id=%s", batch.batchId)
            try:
                transport.request_json(
                    "POST",
                    f"/api/internal/intraday-refresh/batches/{batch.batchId}/fail",
                    json_body=IntradayRefreshFailRequest(
                        claimToken=claim.claimToken,
                        error=str(exc),
                    ).model_dump(mode="json"),
                )
            except Exception:
                logger.exception("Intraday refresh failure reporting failed: batch_id=%s", batch.batchId)
            return 1


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from asset_allocation_runtime_common.market_data import core as mdc

    job_name = "intraday-market-refresh-job"
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
