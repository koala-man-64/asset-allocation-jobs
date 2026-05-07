from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
from asset_allocation_runtime_common.market_data import core as mdc

from tasks.common.job_status import resolve_job_run_status
from tasks.common.silver_processed_state import (
    build_processed_state_index,
    build_processed_state_record,
    load_processed_state,
    make_entity_key,
    merge_processed_state_updates,
    new_stats as new_processed_state_stats,
    processed_state_enabled,
    processed_state_force_rebuild,
    record_decision as record_processed_state_decision,
    save_processed_state,
    should_process_entity,
    stable_frame_signature,
)
from tasks.common.watermarks import (
    load_last_success,
    load_watermarks,
    normalize_watermark_blob_name,
    save_last_success,
    save_watermarks,
    should_process_blob_since_last_success,
)
from tasks.quiver_data import constants
from tasks.quiver_data.config import QuiverDataConfig
from tasks.quiver_data.storage import (
    load_blob_infos,
    load_parquet_snapshot,
    read_json_batches,
    write_domain_artifact,
    write_parquet_snapshot,
)
from tasks.quiver_data.transform import merge_normalized_frames, normalize_bronze_batch

_WATERMARK_KEY = "quiver_bronze_raw"
_PROCESSED_STATE_DOMAIN = "quiver"
_PROCESSED_STATE_PROCESSOR_VERSION = "silver-quiver-v1"


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
    config = QuiverDataConfig.from_env()
    bronze_client = mdc.get_storage_client(config.bronze_container)
    silver_client = mdc.get_storage_client(config.silver_container)
    if bronze_client is None or silver_client is None:
        raise RuntimeError("Quiver silver requires both bronze and silver storage clients.")

    watermarks = load_watermarks(_WATERMARK_KEY)
    last_success = load_last_success("silver_quiver_data")
    candidate_blobs, skipped = _candidate_blob_infos(client=bronze_client, watermarks=watermarks, last_success=last_success)
    if not candidate_blobs:
        mdc.write_line("Quiver silver skipped: no changed bronze raw blobs.")
        return 0

    processed_state_active = processed_state_enabled() and getattr(mdc, "common_storage_client", None) is not None
    processed_state = load_processed_state(_PROCESSED_STATE_DOMAIN) if processed_state_active else None
    processed_state_index = build_processed_state_index(processed_state) if processed_state_active else None
    processed_state_updates: list[dict] = []
    processed_state_stats = new_processed_state_stats()
    force_processed_state = processed_state_force_rebuild()

    tables: dict[str, object] = {}
    batches = read_json_batches(client=bronze_client, blob_infos=candidate_blobs)
    successful_blob_names = {
        str(batch.get("__source_blob_name") or "").strip()
        for batch in batches
        if str(batch.get("__source_blob_name") or "").strip()
    }
    for batch in batches:
        frame = normalize_bronze_batch(batch)
        source_blob_name = str(batch.get("__source_blob_name") or "").strip()
        if frame.empty:
            if source_blob_name:
                source_dataset = str(batch.get("source_dataset") or "").strip()
                dataset_family = constants.normalize_quiver_dataset(
                    constants.dataset_family_for_source(source_dataset)
                    if source_dataset in dict(constants.SOURCE_DATASETS)
                    else batch.get("dataset_family")
                )
                bucket = str(batch.get("bucket") or "X").strip().upper()
                entity_key = make_entity_key(
                    domain=_PROCESSED_STATE_DOMAIN,
                    bucket=bucket,
                    entity_id=f"{source_dataset}:{dataset_family}:{bucket}",
                )
                processed_state_updates.append(
                    build_processed_state_record(
                        domain=_PROCESSED_STATE_DOMAIN,
                        bucket=bucket,
                        entity_key=entity_key,
                        sub_domain=dataset_family,
                        processor_version=_PROCESSED_STATE_PROCESSOR_VERSION,
                        source_frame=pd.DataFrame(
                            [{"payload_signature": stable_frame_signature(pd.DataFrame([batch]))}]
                        ),
                        source_blob={"name": source_blob_name},
                        output_paths=[constants.silver_table_path(dataset_family, bucket)],
                        row_count=0,
                    )
                )
            continue
        dataset_family = str(frame["dataset_family"].iloc[0])
        source_dataset = str(frame["source_dataset"].iloc[0])
        bucket = str(frame["bucket"].iloc[0])
        path = constants.silver_table_path(dataset_family, bucket)
        entity_key = make_entity_key(
            domain=_PROCESSED_STATE_DOMAIN,
            bucket=bucket,
            entity_id=f"{source_dataset}:{dataset_family}:{bucket}",
        )
        current_state = build_processed_state_record(
            domain=_PROCESSED_STATE_DOMAIN,
            bucket=bucket,
            entity_key=entity_key,
            sub_domain=dataset_family,
            processor_version=_PROCESSED_STATE_PROCESSOR_VERSION,
            source_frame=frame,
            source_blob={"name": source_blob_name},
            source_date_columns=("public_availability_time", "vendor_event_time"),
            output_paths=[path],
        )
        if processed_state_index is not None:
            should_process, reason = should_process_entity(
                current_state,
                processed_state_index.get(entity_key),
                force_reprocess=force_processed_state,
            )
            record_processed_state_decision(
                processed_state_stats,
                should_process=should_process,
                reason=reason,
            )
            if not should_process:
                continue
        existing = load_parquet_snapshot(client=silver_client, path=path)
        if "source_hash" in frame.columns and existing is not None and not existing.empty and "source_hash" in existing.columns:
            existing_hashes = {
                str(value)
                for value in existing["source_hash"].dropna().astype(str).tolist()
                if str(value).strip()
            }
            new_hashes = {
                str(value)
                for value in frame["source_hash"].dropna().astype(str).tolist()
                if str(value).strip()
            }.difference(existing_hashes)
            if not new_hashes:
                processed_state_updates.append(current_state)
                continue
        merged = merge_normalized_frames(existing, frame)
        write_parquet_snapshot(client=silver_client, path=path, frame=merged)
        tables[f"{dataset_family}:{bucket}"] = merged
        processed_state_updates.append(current_state)

    for blob in candidate_blobs:
        blob_name = str(blob.get("name") or "")
        if blob_name not in successful_blob_names:
            continue
        watermarks[normalize_watermark_blob_name(blob_name)] = {
            "etag": blob.get("etag"),
            "last_modified": str(blob.get("last_modified") or ""),
        }
    if tables:
        write_domain_artifact(
            client=silver_client,
            layer="silver",
            job_name=constants.SILVER_JOB_NAME,
            run_id=str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip(),
            tables={name: frame for name, frame in tables.items() if hasattr(frame, "__len__")},
            extra_metadata={"candidateBlobCount": len(candidate_blobs), "skippedBlobCount": skipped},
        )
    if processed_state_updates and processed_state_active:
        save_processed_state(
            _PROCESSED_STATE_DOMAIN,
            merge_processed_state_updates(processed_state, processed_state_updates),
        )
    save_watermarks(_WATERMARK_KEY, watermarks)

    status, exit_code = resolve_job_run_status(failed_count=0, warning_count=0)
    mdc.write_line(
        "silver_processed_state_summary layer=silver domain=quiver "
        f"entities_seen={processed_state_stats['entities_seen']} "
        f"entities_changed={processed_state_stats['entities_changed']} "
        f"entities_skipped_state={processed_state_stats['entities_skipped_state']} "
        f"entities_reprocessed_calendar={processed_state_stats['entities_reprocessed_calendar']} "
        f"output_buckets_touched={len(tables)} "
        f"processed_state_updates={len(processed_state_updates)}"
    )
    save_last_success(
        "silver_quiver_data",
        when=datetime.now(timezone.utc),
        metadata={
            "status": status,
            "candidate_blob_count": len(candidate_blobs),
            "parsed_blob_count": len(successful_blob_names),
            "skipped_blob_count": skipped,
            "tables_written": len(tables),
        },
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
                    lambda: write_system_health_marker(
                        layer="silver",
                        domain=constants.domain_slug_for_layer("silver"),
                        job_name=job_name,
                    ),
                    trigger_next_job_from_env,
                ),
            )
        )
