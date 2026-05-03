from __future__ import annotations

import logging
import os
import re
import time as monotonic_time

from asset_allocation_contracts.symbol_enrichment import SymbolCleanupWorkItem, SymbolEnrichmentResolveRequest
from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport
from asset_allocation_runtime_common.foundation.postgres import connect
from asset_allocation_runtime_common.symbol_enrichment_repository import SymbolEnrichmentRepository

from core.symbol_cleanup_runtime import (
    build_symbol_cleanup_plan,
    load_symbol_cleanup_context,
    merge_symbol_cleanup_result,
    validate_ai_response,
)

logger = logging.getLogger("asset-allocation.tasks.symbol-cleanup")

_EXECUTION_BUDGET_SECONDS = 1500.0
_FAILURE_REASON_LIMIT = 500
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(password|secret|token|key|dsn|authorization|bearer)\b\s*[:=]\s*[^,\s;]+"
)
_URL_CREDENTIAL_PATTERN = re.compile(r"://[^/@\s]+@")


class SymbolCleanupItemError(ValueError):
    """A data or business-rule failure that can be reported against one work item."""


def _require_env(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


def _log_lifecycle(phase: str, **fields: object) -> None:
    parts = [f"phase={phase}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    logger.info("symbol_cleanup_lifecycle_event %s", " ".join(parts))


def _probe_postgres(dsn: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()


def _probe_symbol_cleanup_schema(dsn: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
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
                LIMIT 0
                """
            )
            cur.execute(
                """
                SELECT field_name
                FROM core.symbol_profile_overrides
                WHERE is_locked = TRUE
                LIMIT 0
                """
            )


def _probe_control_plane(transport: ControlPlaneTransport) -> None:
    try:
        transport.request_json("GET", "/api/internal/symbol-cleanup/runs/__symbol_cleanup_preflight__")
    except ControlPlaneRequestError as exc:
        if exc.status_code == 404:
            return
        raise


def _log_stage_timing(phase: str, started_at: float, **fields: object) -> None:
    _log_lifecycle(
        phase,
        duration_sec=f"{(monotonic_time.monotonic() - started_at):.2f}",
        **fields,
    )


def preflight_dependencies(*, dsn: str, execution_name: str | None) -> None:
    transport: ControlPlaneTransport | None = None
    started_at = monotonic_time.monotonic()
    try:
        _log_lifecycle("preflight_start", execution_name=execution_name)
        transport = ControlPlaneTransport.from_env()
        _probe_postgres(dsn)
        _probe_symbol_cleanup_schema(dsn)
        _log_stage_timing("preflight_postgres_ok", started_at, execution_name=execution_name)
        _probe_control_plane(transport)
        _log_stage_timing("preflight_control_plane_ok", started_at, execution_name=execution_name)
        _log_lifecycle("preflight_ok", execution_name=execution_name)
    finally:
        if transport is not None:
            transport.close()


def _execution_budget_exhausted(started_at: float) -> bool:
    return (monotonic_time.monotonic() - started_at) >= _EXECUTION_BUDGET_SECONDS


def _sanitize_failure_reason(error: Exception) -> str:
    reason = str(error).strip() or error.__class__.__name__
    reason = re.sub(r"\s+", " ", reason)
    reason = _URL_CREDENTIAL_PATTERN.sub("://<redacted>@", reason)
    reason = _SECRET_ASSIGNMENT_PATTERN.sub(lambda match: f"{match.group(1)}=<redacted>", reason)
    if len(reason) <= _FAILURE_REASON_LIMIT:
        return reason
    return f"{reason[: _FAILURE_REASON_LIMIT - 3]}..."


def _validate_claimed_work(work: SymbolCleanupWorkItem, *, execution_name: str | None) -> None:
    if work.status != "claimed":
        raise SymbolCleanupItemError(
            f"Symbol cleanup work '{work.workId}' is not claimed: status={work.status}."
        )
    if not work.requestedFields:
        raise SymbolCleanupItemError(f"Symbol cleanup work '{work.workId}' did not request any fields.")
    if execution_name and work.executionName and work.executionName != execution_name:
        raise SymbolCleanupItemError(
            f"Symbol cleanup work '{work.workId}' was claimed by '{work.executionName}', not '{execution_name}'."
        )


def process_work_item(
    *,
    repo: SymbolEnrichmentRepository,
    dsn: str,
    work_id: str,
    run_id: str,
    symbol: str,
    requested_fields: list[str],
    execution_name: str | None,
) -> object | None:
    run = repo.get_run(run_id)
    if run is None:
        raise SymbolCleanupItemError(f"Symbol cleanup run '{run_id}' not found.")
    if run.status != "running":
        raise SymbolCleanupItemError(f"Symbol cleanup run '{run_id}' is not running: status={run.status}.")

    try:
        context = load_symbol_cleanup_context(dsn, symbol)
    except LookupError as exc:
        raise SymbolCleanupItemError(str(exc)) from exc
    plan = build_symbol_cleanup_plan(
        mode=run.mode,
        requested_fields=requested_fields,
        context=context,
    )
    ai_response = None
    if plan.ai_requested_fields:
        request_payload = SymbolEnrichmentResolveRequest(
            symbol=context.provider_facts.symbol,
            overwriteMode=run.mode,
            requestedFields=plan.ai_requested_fields,
            providerFacts=context.provider_facts,
            currentProfile=context.current_profile,
        )
        try:
            ai_response = repo.resolve_symbol_profile(request_payload)
        except ControlPlaneRequestError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Symbol enrichment resolve failed for symbol '{symbol}'.") from exc
        ai_response = validate_ai_response(
            requested_fields=plan.ai_requested_fields,
            provider_facts=context.provider_facts,
            response=ai_response,
        )

    result = merge_symbol_cleanup_result(
        symbol=context.provider_facts.symbol,
        deterministic_profile=plan.deterministic_profile,
        ai_response=ai_response,
    )
    _log_lifecycle(
        "work_resolved",
        execution_name=execution_name,
        work_id=work_id,
        run_id=run_id,
        symbol=symbol,
        ai_requested=len(plan.ai_requested_fields),
        deterministic_fields=len(
            [value for value in plan.deterministic_profile.model_dump(mode="json").values() if value is not None]
        ),
        completed_with_result=result is not None,
    )
    return result


def main() -> int:
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip() or None
    transport: ControlPlaneTransport | None = None
    repo: SymbolEnrichmentRepository | None = None
    try:
        dsn = _require_env("POSTGRES_DSN")
        preflight_dependencies(dsn=dsn, execution_name=execution_name)
    except Exception:
        logger.exception("symbol_cleanup_preflight_failed execution_name=%s", execution_name or "-")
        return 1

    try:
        transport = ControlPlaneTransport.from_env()
        repo = SymbolEnrichmentRepository(transport=transport)
        pass_started_at = monotonic_time.monotonic()
        claimed_count = 0
        completed_count = 0
        failed_count = 0
        deferred_to_next_run = False

        while True:
            if _execution_budget_exhausted(pass_started_at):
                deferred_to_next_run = claimed_count > 0
                _log_lifecycle(
                    "budget_exhausted",
                    execution_name=execution_name,
                    claimed_count=claimed_count,
                    completed_count=completed_count,
                    failed_count=failed_count,
                    budget_seconds=f"{_EXECUTION_BUDGET_SECONDS:.0f}",
                )
                break

            work = repo.claim_work(execution_name=execution_name)
            if work is None:
                if claimed_count == 0:
                    logger.info("No queued symbol cleanup work found.")
                    _log_lifecycle("no_work", execution_name=execution_name)
                break

            claimed_count += 1
            _log_lifecycle(
                "claim",
                execution_name=execution_name,
                work_id=work.workId,
                run_id=work.runId,
                symbol=work.symbol,
                attempt_count=work.attemptCount,
                claimed_count=claimed_count,
            )

            try:
                _validate_claimed_work(work, execution_name=execution_name)
                result = process_work_item(
                    repo=repo,
                    dsn=dsn,
                    work_id=work.workId,
                    run_id=work.runId,
                    symbol=work.symbol,
                    requested_fields=work.requestedFields,
                    execution_name=execution_name,
                )
            except (SymbolCleanupItemError, ValueError, LookupError) as exc:
                failed_count += 1
                failure_reason = _sanitize_failure_reason(exc)
                logger.exception("Symbol cleanup work failed: work_id=%s symbol=%s", work.workId, work.symbol)
                try:
                    repo.fail_work(work.workId, error=failure_reason)
                except Exception:
                    logger.exception("Symbol cleanup failure reporting failed: work_id=%s", work.workId)
                    _log_lifecycle(
                        "fail_report_failed",
                        execution_name=execution_name,
                        work_id=work.workId,
                        run_id=work.runId,
                        symbol=work.symbol,
                        failure_reason=failure_reason,
                        failed_count=failed_count,
                    )
                    return 1
                _log_lifecycle(
                    "fail_reported",
                    execution_name=execution_name,
                    work_id=work.workId,
                    run_id=work.runId,
                    symbol=work.symbol,
                    failure_reason=failure_reason,
                    failed_count=failed_count,
                )
                continue
            except Exception as exc:
                logger.exception("Symbol cleanup pass aborted while processing work_id=%s symbol=%s", work.workId, work.symbol)
                _log_lifecycle(
                    "abort",
                    execution_name=execution_name,
                    work_id=work.workId,
                    run_id=work.runId,
                    symbol=work.symbol,
                    failure_reason=_sanitize_failure_reason(exc),
                )
                return 1

            try:
                repo.complete_work(work.workId, result=result)
            except Exception as exc:
                logger.exception("Symbol cleanup completion reporting failed: work_id=%s symbol=%s", work.workId, work.symbol)
                _log_lifecycle(
                    "complete_report_failed",
                    execution_name=execution_name,
                    work_id=work.workId,
                    run_id=work.runId,
                    symbol=work.symbol,
                    failure_reason=_sanitize_failure_reason(exc),
                )
                return 1

            completed_count += 1
            _log_lifecycle(
                "complete",
                execution_name=execution_name,
                work_id=work.workId,
                run_id=work.runId,
                symbol=work.symbol,
                result_applied=result is not None,
                completed_count=completed_count,
            )

        _log_lifecycle(
            "pass_complete",
            execution_name=execution_name,
            claimed_count=claimed_count,
            completed_count=completed_count,
            failed_count=failed_count,
            deferred_to_next_run=deferred_to_next_run,
            duration_sec=f"{(monotonic_time.monotonic() - pass_started_at):.2f}",
        )
        return 1 if failed_count else 0
    except Exception as exc:
        logger.exception("Symbol cleanup pass aborted before completion.")
        _log_lifecycle(
            "abort",
            execution_name=execution_name,
            failure_reason=str(exc),
        )
        return 1
    finally:
        if transport is not None:
            transport.close()


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    raise SystemExit(
        run_logged_job(
            job_name="symbol-cleanup-job",
            run=main,
            log_info=logger.info,
            log_warning=logger.warning,
            log_error=logger.error,
            log_exception=logger.exception,
        )
    )
