from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from asset_allocation_runtime_common.market_data import core as mdc

from tasks.common.job_status import resolve_job_run_status
from tasks.common.watermarks import save_last_success
from tasks.economic_catalyst_data import constants
from tasks.economic_catalyst_data.config import EconomicCatalystConfig
from tasks.economic_catalyst_data.postgres_sync import upsert_source_state
from tasks.economic_catalyst_data.sources import fetch_requested_sources
from tasks.economic_catalyst_data.storage import computed_at_iso, write_domain_artifact


def _run_id() -> str:
    execution_name = str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip()
    if execution_name:
        return execution_name
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"economic-catalyst-bronze-{stamp}-{os.getpid()}"


def _persist_source_batches(*, client: Any, run_id: str, batches) -> list[str]:
    paths: list[str] = []
    for batch in batches:
        path = constants.bronze_raw_path(run_id, batch.source_name, batch.dataset_name)
        mdc.save_json_content(batch.to_payload(), path, client=client)
        paths.append(path)
    return paths


def _persist_manifest(
    *,
    client: Any,
    run_id: str,
    batch_paths: list[str],
    warnings: list[str],
    failures: list[str],
    enabled_sources: tuple[str, ...],
    poll_mode: str,
) -> dict[str, Any]:
    payload = {
        "version": 1,
        "runId": run_id,
        "layer": "bronze",
        "domain": constants.DOMAIN_SLUG,
        "updatedAt": computed_at_iso(),
        "batchPaths": batch_paths,
        "enabledSources": list(enabled_sources),
        "pollMode": poll_mode,
        "warnings": list(warnings),
        "failures": list(failures),
    }
    mdc.save_json_content(payload, constants.bronze_manifest_path(run_id), client=client)
    return payload


def _selected_sources(config: EconomicCatalystConfig, *, now: datetime) -> tuple[str, tuple[str, ...]]:
    enabled_sources = config.enabled_sources()
    if not enabled_sources:
        return "general", ()
    general_minutes = max(int(config.general_poll_minutes), 1)
    if now.minute % general_minutes == 0:
        return "general", enabled_sources
    hot_sources = tuple(
        source_name
        for source_name in enabled_sources
        if source_name in constants.STRUCTURED_VENDOR_SOURCES
        or source_name in constants.HEADLINE_SOURCES
        or source_name == "fred_releases"
    )
    return "hot_window", hot_sources or enabled_sources


def main() -> int:
    mdc.log_environment_diagnostics()
    config = EconomicCatalystConfig.from_env()
    bronze_client = mdc.get_storage_client(config.bronze_container)
    if bronze_client is None:
        raise RuntimeError(f"Storage client unavailable for container {config.bronze_container!r}.")

    run_id = _run_id()
    now = datetime.now(timezone.utc)
    poll_mode, selected_sources = _selected_sources(config, now=now)
    batches, warnings, failures = fetch_requested_sources(config, now=now, source_names=selected_sources)
    batch_paths = _persist_source_batches(client=bronze_client, run_id=run_id, batches=batches)
    manifest = _persist_manifest(
        client=bronze_client,
        run_id=run_id,
        batch_paths=batch_paths,
        warnings=warnings,
        failures=failures,
        enabled_sources=selected_sources,
        poll_mode=poll_mode,
    )
    write_domain_artifact(
        client=bronze_client,
        layer="bronze",
        job_name=constants.BRONZE_JOB_NAME,
        run_id=run_id,
        tables={},
        warnings=warnings,
        extra_metadata=manifest,
    )

    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if dsn:
        for batch in batches:
            try:
                upsert_source_state(
                    dsn,
                    source_name=batch.source_name,
                    dataset_name=batch.dataset_name,
                    state_type="cursor",
                    cursor_value=batch.request_url,
                    last_ingested_at=datetime.now(timezone.utc),
                    metadata={"requestUrl": batch.request_url, "payloadFormat": batch.payload_format},
                )
            except Exception as exc:
                mdc.write_warning(f"Failed to update economic catalyst source_state for {batch.source_name}: {exc}")

    status, exit_code = resolve_job_run_status(failed_count=len(failures), warning_count=len(warnings))
    save_last_success(
        "bronze_economic_catalyst_data",
        metadata={
            "run_id": run_id,
            "status": status,
            "poll_mode": poll_mode,
            "batch_count": len(batches),
            "enabled_sources": list(selected_sources),
            "warnings": warnings,
            "failures": failures,
            "batch_paths": batch_paths,
        },
    )
    mdc.write_line(
        "Economic catalyst bronze complete: "
        f"run_id={run_id} poll_mode={poll_mode} batches={len(batches)} warnings={len(warnings)} "
        f"failures={len(failures)} status={status}"
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
                    lambda: write_system_health_marker(layer="bronze", domain="economic-catalyst", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
