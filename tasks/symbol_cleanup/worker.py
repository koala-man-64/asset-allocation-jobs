from __future__ import annotations

import logging
import os
import time as monotonic_time

from asset_allocation_contracts.symbol_enrichment import SymbolEnrichmentResolveRequest
from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport
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


def preflight_dependencies(*, dsn: str, execution_name: str | None) -> None:
    started_at = monotonic_time.monotonic()
    _log_lifecycle("preflight_start", execution_name=execution_name)
    _probe_postgres(dsn)
    _log_lifecycle(
        "preflight_ok",
        execution_name=execution_name,
        duration_sec=f"{(monotonic_time.monotonic() - started_at):.2f}",
    )


def _execution_budget_exhausted(started_at: float) -> bool:
    return (monotonic_time.monotonic() - started_at) >= _EXECUTION_BUDGET_SECONDS


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
        raise LookupError(f"Symbol cleanup run '{run_id}' not found.")

    context = load_symbol_cleanup_context(dsn, symbol)
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
        ai_response = repo.resolve_symbol_profile(request_payload)
        validate_ai_response(
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
    dsn = _require_env("POSTGRES_DSN")
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip() or None
    transport: ControlPlaneTransport | None = None
    repo: SymbolEnrichmentRepository | None = None
    try:
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
                result = process_work_item(
                    repo=repo,
                    dsn=dsn,
                    work_id=work.workId,
                    run_id=work.runId,
                    symbol=work.symbol,
                    requested_fields=work.requestedFields,
                    execution_name=execution_name,
                )
                repo.complete_work(work.workId, result=result)
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
            except Exception as exc:
                failed_count += 1
                logger.exception("Symbol cleanup work failed: work_id=%s symbol=%s", work.workId, work.symbol)
                _log_lifecycle(
                    "fail",
                    execution_name=execution_name,
                    work_id=work.workId,
                    run_id=work.runId,
                    symbol=work.symbol,
                    failure_reason=str(exc),
                    failed_count=failed_count,
                )
                try:
                    repo.fail_work(work.workId, error=str(exc))
                except Exception:
                    logger.exception("Symbol cleanup failure reporting failed: work_id=%s", work.workId)

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
