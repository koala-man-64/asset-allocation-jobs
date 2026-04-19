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
    work = None
    try:
        preflight_dependencies(dsn=dsn, execution_name=execution_name)
    except Exception:
        logger.exception("symbol_cleanup_preflight_failed execution_name=%s", execution_name or "-")
        return 1

    try:
        transport = ControlPlaneTransport.from_env()
        repo = SymbolEnrichmentRepository(transport=transport)
        work = repo.claim_work(execution_name=execution_name)
        if work is None:
            logger.info("No queued symbol cleanup work found.")
            _log_lifecycle("no_work", execution_name=execution_name)
            return 0

        _log_lifecycle(
            "claim",
            execution_name=execution_name,
            work_id=work.workId,
            run_id=work.runId,
            symbol=work.symbol,
            attempt_count=work.attemptCount,
        )

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
        _log_lifecycle(
            "complete",
            execution_name=execution_name,
            work_id=work.workId,
            run_id=work.runId,
            symbol=work.symbol,
            result_applied=result is not None,
        )
        return 0
    except Exception as exc:
        logger.exception("Symbol cleanup work failed: work_id=%s symbol=%s", getattr(work, "workId", "-"), getattr(work, "symbol", "-"))
        _log_lifecycle(
            "fail",
            execution_name=execution_name,
            work_id=getattr(work, "workId", None),
            run_id=getattr(work, "runId", None),
            symbol=getattr(work, "symbol", None),
            failure_reason=str(exc),
        )
        if repo is not None and work is not None:
            try:
                repo.fail_work(work.workId, error=str(exc))
            except Exception:
                logger.exception("Symbol cleanup failure reporting failed: work_id=%s", work.workId)
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
