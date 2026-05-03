from __future__ import annotations

import logging
import json
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Callable

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport
from asset_allocation_runtime_common.shared_core import config as runtime_config

from tasks.common.intraday_runtime import market_layer_lock, require_intraday_lock_prerequisites
from tasks.common.intraday_contracts_compat import (
    IntradayRefreshClaimRequest,
    IntradayRefreshClaimResponse,
    IntradayRefreshCompleteRequest,
    IntradayRefreshFailRequest,
)
from tasks.common.market_refresh_scope import (
    SCOPE_MODE_ENV,
    SCOPE_SYMBOLS_ENV,
    normalize_scope_symbols,
)
from tasks.common.secret_redaction import safe_exception_message
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


def _log_metric(phase: str, **fields: object) -> None:
    parts = [f"phase={phase}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    logger.info("intraday_refresh_metric %s", " ".join(parts))


def _age_seconds(value: datetime | None, *, now: datetime | None = None) -> int | None:
    if value is None:
        return None
    observed = now or datetime.now(UTC)
    timestamp = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return max(0, int((observed - timestamp).total_seconds()))


@contextmanager
def _scoped_market_refresh_symbols(symbols: list[str]):
    normalized = list(normalize_scope_symbols(symbols))
    prior_env = os.environ.get("DEBUG_SYMBOLS")
    prior_scope_mode = os.environ.get(SCOPE_MODE_ENV)
    prior_scope_symbols = os.environ.get(SCOPE_SYMBOLS_ENV)
    had_runtime_symbols = "DEBUG_SYMBOLS" in runtime_config.__dict__
    prior_runtime_symbols = list(getattr(runtime_config, "DEBUG_SYMBOLS", []) or [])
    prior_settings_symbols = list(getattr(runtime_config.settings, "DEBUG_SYMBOLS", []) or [])
    had_silver_symbols = "DEBUG_SYMBOLS" in silver_market_data.cfg.__dict__
    prior_silver_symbols = list(getattr(silver_market_data.cfg, "DEBUG_SYMBOLS", []) or [])
    had_bronze_symbols = "DEBUG_SYMBOLS" in bronze_market_data.cfg.__dict__
    prior_bronze_symbols = list(getattr(bronze_market_data.cfg, "DEBUG_SYMBOLS", []) or [])

    scoped_value = ",".join(normalized)
    os.environ[SCOPE_MODE_ENV] = "intraday"
    os.environ[SCOPE_SYMBOLS_ENV] = scoped_value
    os.environ["DEBUG_SYMBOLS"] = scoped_value
    runtime_config.settings.DEBUG_SYMBOLS = list(normalized)
    runtime_config.DEBUG_SYMBOLS = list(normalized)
    bronze_market_data.cfg.DEBUG_SYMBOLS = list(normalized)
    silver_market_data.cfg.DEBUG_SYMBOLS = list(normalized)
    try:
        yield normalized
    finally:
        if prior_scope_mode is None:
            os.environ.pop(SCOPE_MODE_ENV, None)
        else:
            os.environ[SCOPE_MODE_ENV] = prior_scope_mode
        if prior_scope_symbols is None:
            os.environ.pop(SCOPE_SYMBOLS_ENV, None)
        else:
            os.environ[SCOPE_SYMBOLS_ENV] = prior_scope_symbols
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


_scoped_debug_symbols = _scoped_market_refresh_symbols


def preflight_dependencies() -> None:
    transport = ControlPlaneTransport.from_env()
    try:
        transport.probe("/api/internal/intraday/ready")
    finally:
        transport.close()


def _validate_refresh_claim(claim: IntradayRefreshClaimResponse):
    if claim.batch is None and claim.claimToken is None:
        return None
    if claim.batch is None or claim.claimToken is None:
        raise ValueError("Malformed intraday refresh claim: batch and claimToken must both be present.")

    batch = claim.batch
    domain = str(getattr(batch, "domain", "") or "").strip().lower()
    if domain != "market":
        raise ValueError(f"Unsupported intraday refresh domain: {domain or 'missing'}.")
    if str(getattr(batch, "status", "") or "").strip().lower() != "claimed":
        raise ValueError(f"Malformed intraday refresh claim: batch status must be claimed for {batch.batchId}.")

    symbols = list(normalize_scope_symbols(list(batch.symbols)))
    if not symbols:
        raise ValueError(f"Malformed intraday refresh claim: batch {batch.batchId} contains no symbols.")
    if int(batch.symbolCount or 0) != len(symbols):
        raise ValueError(
            f"Malformed intraday refresh claim: batch {batch.batchId} symbolCount={batch.symbolCount} "
            f"does not match unique symbols={len(symbols)}."
        )

    bucket = str(batch.bucketLetter or "").strip().upper()
    if len(bucket) != 1:
        raise ValueError(f"Malformed intraday refresh claim: batch {batch.batchId} bucketLetter is invalid.")
    return batch, str(claim.claimToken), symbols


def _run_stage(stage: str, run: Callable[[], int]) -> None:
    started = time.perf_counter()
    with market_layer_lock(stage) as lock_outcome:
        exit_code = run()
    duration_ms = int((time.perf_counter() - started) * 1000)
    _log_metric(
        "stage",
        stage=stage,
        duration_ms=duration_ms,
        exit_code=exit_code,
        lock_outcome=lock_outcome,
    )
    if exit_code != 0:
        raise RuntimeError(f"{stage.title()} market refresh failed with exit code {exit_code}.")


def _run_market_refresh_pipeline(symbols: list[str]) -> None:
    with _scoped_market_refresh_symbols(symbols):
        _run_stage("bronze", bronze_market_data.main)
        _run_stage("silver", silver_market_data.main)
        _run_stage("gold", gold_market_data.main)


def main() -> int:
    execution_name = _execution_name()
    try:
        preflight_dependencies()
    except Exception:
        logger.exception("Intraday refresh preflight failed.")
        return 1

    with ControlPlaneTransport.from_env() as transport:
        try:
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
        except Exception as exc:
            logger.error("Intraday refresh claim failed: %s", safe_exception_message(exc, phase="claim"))
            _log_metric("claim", status="failed", error_type=type(exc).__name__)
            return 1
        try:
            active_claim = _validate_refresh_claim(claim)
        except Exception as exc:
            logger.error("Malformed intraday refresh claim: %s", safe_exception_message(exc, phase="claim"))
            _log_metric("claim", status="malformed", error_type=type(exc).__name__)
            return 1

        if active_claim is None:
            logger.info("No queued intraday refresh batches found.")
            _log_metric("claim", status="no_work")
            return 0

        batch, claim_token, symbols = active_claim
        _log_lifecycle(
            "claim",
            batch_id=batch.batchId,
            watchlist_id=batch.watchlistId,
            bucket=batch.bucketLetter,
            symbol_count=len(symbols),
            execution_name=execution_name,
        )
        _log_metric(
            "claim",
            status="claimed",
            symbol_count=len(symbols),
            batch_age_seconds=_age_seconds(batch.createdAt),
        )

        try:
            pipeline_started = time.perf_counter()
            _run_market_refresh_pipeline(symbols)
            _log_metric(
                "pipeline",
                status="ok",
                duration_ms=int((time.perf_counter() - pipeline_started) * 1000),
                symbol_count=len(symbols),
            )
        except Exception as exc:
            error = safe_exception_message(exc, phase="pipeline")
            logger.error("Intraday refresh batch failed: batch_id=%s error=%s", batch.batchId, error)
            try:
                transport.request_json(
                    "POST",
                    f"/api/internal/intraday-refresh/batches/{batch.batchId}/fail",
                    json_body=IntradayRefreshFailRequest(
                        claimToken=claim_token,
                        error=error,
                    ).model_dump(mode="json"),
                )
            except Exception as fail_exc:
                logger.error(
                    "Intraday refresh failure reporting failed: batch_id=%s error=%s",
                    batch.batchId,
                    safe_exception_message(fail_exc, phase="fail_report"),
                )
            _log_metric("pipeline", status="failed", error_type=type(exc).__name__, symbol_count=len(symbols))
            return 1

        try:
            complete_payload = IntradayRefreshCompleteRequest(claimToken=claim_token)
            complete_body = complete_payload.model_dump(mode="json")
            payload_bytes = len(json.dumps(complete_body, separators=(",", ":")).encode("utf-8"))
            transport.request_json(
                "POST",
                f"/api/internal/intraday-refresh/batches/{batch.batchId}/complete",
                json_body=complete_body,
            )
            _log_lifecycle(
                "complete",
                batch_id=batch.batchId,
                watchlist_id=batch.watchlistId,
                symbol_count=len(symbols),
            )
            _log_metric("complete", status="ok", symbol_count=len(symbols), payload_bytes=payload_bytes)
            return 0
        except Exception as exc:
            logger.error(
                "Intraday refresh completion status unknown: batch_id=%s error=%s",
                batch.batchId,
                safe_exception_message(exc, phase="complete"),
            )
            _log_lifecycle("completion_unknown", batch_id=batch.batchId, error_type=type(exc).__name__)
            _log_metric("complete", status="unknown", error_type=type(exc).__name__, symbol_count=len(symbols))
            return 1


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from asset_allocation_runtime_common.market_data import core as mdc

    job_name = "intraday-market-refresh-job"
    require_intraday_lock_prerequisites(job_name)
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
