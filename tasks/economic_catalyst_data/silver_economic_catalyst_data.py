from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc

from tasks.common.job_status import resolve_job_run_status
from tasks.common.watermarks import (
    load_last_success,
    load_watermarks,
    normalize_watermark_blob_name,
    save_last_success,
    save_watermarks,
    should_process_blob_since_last_success,
)
from tasks.economic_catalyst_data import constants
from tasks.economic_catalyst_data.config import EconomicCatalystConfig
from tasks.economic_catalyst_data.storage import load_blob_infos, load_json_batches, load_parquet_snapshot, write_domain_artifact, write_parquet_snapshot
from tasks.economic_catalyst_data.transform import (
    canonicalize_source_state,
    dedupe_source_frames,
    parse_raw_batches_to_source_frames,
)


_WATERMARK_KEY = "economic_catalyst_bronze_raw"


def _candidate_blob_infos(*, client, watermarks: dict[str, object], last_success) -> tuple[list[dict], int]:
    all_blobs = load_blob_infos(client=client, prefix=constants.BRONZE_ROOT_PREFIX)
    candidates: list[dict] = []
    skipped = 0
    for blob in all_blobs:
        name = str(blob.get("name") or "").strip()
        if not name.endswith(".json") or name.endswith("manifest.json"):
            continue
        watermark_key = normalize_watermark_blob_name(name)
        prior = watermarks.get(watermark_key)
        if should_process_blob_since_last_success(blob, prior_signature=prior, last_success_at=last_success):
            candidates.append(blob)
        else:
            skipped += 1
    return candidates, skipped


def main() -> int:
    mdc.log_environment_diagnostics()
    config = EconomicCatalystConfig.from_env()
    bronze_client = mdc.get_storage_client(config.bronze_container)
    silver_client = mdc.get_storage_client(config.silver_container)
    if bronze_client is None or silver_client is None:
        raise RuntimeError("Economic catalyst silver requires both bronze and silver storage clients.")

    watermarks = load_watermarks(_WATERMARK_KEY)
    last_success = load_last_success("silver_economic_catalyst_data")
    candidate_blobs, skipped = _candidate_blob_infos(client=bronze_client, watermarks=watermarks, last_success=last_success)
    if not candidate_blobs:
        mdc.write_line("Economic catalyst silver skipped: no changed bronze raw blobs.")
        return 0

    batches = load_json_batches(client=bronze_client, blob_infos=candidate_blobs)
    new_source_events, new_source_headlines, new_quarantine = parse_raw_batches_to_source_frames(batches)

    existing_source_events = load_parquet_snapshot(
        client=silver_client,
        path=constants.silver_state_table_path("source_events_raw"),
        columns=constants.INTERNAL_SOURCE_EVENT_COLUMNS,
    )
    existing_source_headlines = load_parquet_snapshot(
        client=silver_client,
        path=constants.silver_state_table_path("source_headlines_raw"),
        columns=constants.INTERNAL_SOURCE_HEADLINE_COLUMNS,
    )
    existing_quarantine = load_parquet_snapshot(
        client=silver_client,
        path=constants.silver_state_table_path("quarantine"),
        columns=constants.QUARANTINE_COLUMNS,
    )

    source_events, source_headlines, quarantine = dedupe_source_frames(
        existing_source_events=existing_source_events,
        existing_source_headlines=existing_source_headlines,
        existing_quarantine=existing_quarantine,
        new_source_events=new_source_events,
        new_source_headlines=new_source_headlines,
        new_quarantine=new_quarantine,
    )
    canonical = canonicalize_source_state(
        source_events=source_events,
        source_headlines=source_headlines,
        existing_quarantine=quarantine,
    )

    write_parquet_snapshot(
        client=silver_client,
        path=constants.silver_state_table_path("source_events_raw"),
        frame=source_events,
    )
    write_parquet_snapshot(
        client=silver_client,
        path=constants.silver_state_table_path("source_headlines_raw"),
        frame=source_headlines,
    )
    write_parquet_snapshot(
        client=silver_client,
        path=constants.silver_state_table_path("quarantine"),
        frame=quarantine,
    )

    for table_name in ("events", "event_versions", "headlines", "headline_versions", "mentions", "quarantine"):
        frame = canonical[table_name] if table_name != "quarantine" else quarantine
        write_parquet_snapshot(client=silver_client, path=constants.silver_table_path(table_name), frame=frame)

    for blob in candidate_blobs:
        watermarks[normalize_watermark_blob_name(str(blob.get("name") or ""))] = {
            "etag": blob.get("etag"),
            "last_modified": str(blob.get("last_modified") or ""),
        }
    save_watermarks(_WATERMARK_KEY, watermarks)
    write_domain_artifact(
        client=silver_client,
        layer="silver",
        job_name=constants.SILVER_JOB_NAME,
        run_id=str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip(),
        tables={
            "source_events_raw": source_events,
            "source_headlines_raw": source_headlines,
            "events": canonical["events"],
            "event_versions": canonical["event_versions"],
            "headlines": canonical["headlines"],
            "headline_versions": canonical["headline_versions"],
            "mentions": canonical["mentions"],
            "quarantine": quarantine,
        },
        extra_metadata={
            "candidateBlobCount": len(candidate_blobs),
            "skippedBlobCount": skipped,
        },
    )
    status, exit_code = resolve_job_run_status(failed_count=0, warning_count=0 if quarantine.empty else len(quarantine))
    save_last_success(
        "silver_economic_catalyst_data",
        when=datetime.now(timezone.utc),
        metadata={
            "status": status,
            "candidate_blob_count": len(candidate_blobs),
            "skipped_blob_count": skipped,
            "source_event_rows": int(len(source_events)),
            "source_headline_rows": int(len(source_headlines)),
            "event_rows": int(len(canonical["events"])),
            "headline_rows": int(len(canonical["headlines"])),
            "quarantine_rows": int(len(quarantine)),
        },
    )
    mdc.write_line(
        "Economic catalyst silver complete: "
        f"candidate_blobs={len(candidate_blobs)} source_event_rows={len(source_events)} "
        f"source_headline_rows={len(source_headlines)} event_rows={len(canonical['events'])} "
        f"headline_rows={len(canonical['headlines'])} quarantine_rows={len(quarantine)}"
    )
    return exit_code


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = constants.SILVER_JOB_NAME
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(layer="silver", domain="economic-catalyst", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )

