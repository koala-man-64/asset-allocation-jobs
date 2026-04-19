from __future__ import annotations

import logging
import os
import time as monotonic_time

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport

from core.portfolio_materialization import materialize_portfolio_bundle
from core.portfolio_repository import PortfolioMaterializationRepository, PortfolioMaterializationWorkItem
from core.postgres import connect
from tasks.common.system_health_markers import write_system_health_marker

logger = logging.getLogger("asset-allocation.tasks.portfolio")


def _require_env(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


def _optional_env(name: str) -> str | None:
    value = str(os.environ.get(name) or "").strip()
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
    logger.info("portfolio_materialization_event %s", " ".join(parts))


def _probe_postgres(dsn: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()


def _probe_control_plane(transport: ControlPlaneTransport) -> None:
    transport.probe("/api/internal/portfolio-materializations/ready")


def _log_stage_timing(phase: str, started_at: float, **fields: object) -> None:
    duration = monotonic_time.monotonic() - started_at
    _log_lifecycle(phase, duration_sec=f"{duration:.2f}", **fields)


def preflight_dependencies(*, dsn: str, execution_name: str | None, explicit_account_id: str | None) -> None:
    transport: ControlPlaneTransport | None = None
    preflight_started_at = monotonic_time.monotonic()
    try:
        _log_lifecycle(
            "preflight_start",
            execution_name=execution_name,
            account_id=explicit_account_id,
        )
        transport = ControlPlaneTransport.from_env()
        _probe_postgres(dsn)
        _log_stage_timing(
            "preflight_postgres_ok",
            preflight_started_at,
            execution_name=execution_name,
            account_id=explicit_account_id,
        )
        _probe_control_plane(transport)
        _log_stage_timing(
            "preflight_control_plane_ok",
            preflight_started_at,
            execution_name=execution_name,
            account_id=explicit_account_id,
        )
        _log_lifecycle(
            "preflight_ok",
            execution_name=execution_name,
            account_id=explicit_account_id,
        )
    finally:
        if transport is not None:
            transport.close()


def _resolve_work_item(repo: PortfolioMaterializationRepository, execution_name: str | None) -> PortfolioMaterializationWorkItem | None:
    explicit_account_id = _optional_env("PORTFOLIO_ACCOUNT_ID")
    explicit_claim_token = _optional_env("PORTFOLIO_MATERIALIZATION_CLAIM_TOKEN")
    if explicit_account_id and explicit_claim_token:
        return PortfolioMaterializationWorkItem(
            account_id=explicit_account_id,
            claim_token=explicit_claim_token,
        )
    return repo.claim_next_materialization(execution_name=execution_name)


def main() -> int:
    dsn = _require_env("POSTGRES_DSN")
    execution_name = _optional_env("CONTAINER_APP_JOB_EXECUTION_NAME")
    explicit_account_id = _optional_env("PORTFOLIO_ACCOUNT_ID")
    try:
        preflight_dependencies(dsn=dsn, execution_name=execution_name, explicit_account_id=explicit_account_id)
    except Exception:
        return 1

    repo = PortfolioMaterializationRepository(dsn=dsn)
    work = _resolve_work_item(repo, execution_name)
    if work is None:
        logger.info("No dirty portfolio materializations found.")
        repo.transport.close()
        return 0

    account_id = work.account_id
    claim_token = work.claim_token
    try:
        repo.start_materialization(account_id, claim_token=claim_token, execution_name=execution_name)
        _log_lifecycle("claim", execution_name=execution_name, account_id=account_id)
        repo.update_heartbeat(account_id, claim_token=claim_token)
        bundle = repo.get_materialization_bundle(account_id, claim_token=claim_token)
        result = materialize_portfolio_bundle(
            dsn,
            bundle,
            heartbeat=lambda: repo.update_heartbeat(account_id, claim_token=claim_token),
        )
        repo.complete_materialization(
            account_id,
            claim_token=claim_token,
            snapshot=result.snapshot,
            history=list(result.history),
            positions=list(result.positions),
            alerts=list(result.alerts),
            dependency_fingerprint=result.dependency_fingerprint,
            dependency_state=result.dependency_state,
        )
        write_system_health_marker(
            layer="platinum",
            domain="portfolio",
            job_name="portfolio-materialization-worker",
            metadata={"accountId": account_id, "asOf": result.snapshot.asOf.isoformat()},
        )
        _log_lifecycle(
            "complete",
            execution_name=execution_name,
            account_id=account_id,
            as_of=result.snapshot.asOf.isoformat(),
            nav=f"{result.snapshot.nav:.2f}",
            alerts=result.snapshot.openAlertCount,
        )
        return 0
    except Exception as exc:
        logger.exception("Portfolio materialization failed: account_id=%s", account_id)
        _log_lifecycle(
            "fail",
            execution_name=execution_name,
            account_id=account_id,
            failure_reason=str(exc),
        )
        try:
            repo.fail_materialization(account_id, claim_token=claim_token, error=str(exc))
        except Exception:
            logger.exception("Portfolio materialization failure reporting failed: account_id=%s", account_id)
        return 1
    finally:
        repo.transport.close()


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    raise SystemExit(
        run_logged_job(
            job_name="portfolio-materialization-worker",
            run=main,
            log_info=logger.info,
            log_warning=logger.warning,
            log_error=logger.error,
            log_exception=logger.exception,
        )
    )
