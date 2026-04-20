from __future__ import annotations

import os
from datetime import datetime, timezone

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

    tables: dict[str, object] = {}
    batches = read_json_batches(client=bronze_client, blob_infos=candidate_blobs)
    for batch in batches:
        frame = normalize_bronze_batch(batch)
        if frame.empty:
            continue
        dataset_family = str(frame["dataset_family"].iloc[0])
        bucket = str(frame["bucket"].iloc[0])
        path = constants.silver_table_path(dataset_family, bucket)
        existing = load_parquet_snapshot(client=silver_client, path=path)
        merged = merge_normalized_frames(existing, frame)
        write_parquet_snapshot(client=silver_client, path=path, frame=merged)
        tables[f"{dataset_family}:{bucket}"] = merged

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
        tables={name: frame for name, frame in tables.items() if hasattr(frame, "__len__")},
        extra_metadata={"candidateBlobCount": len(candidate_blobs), "skippedBlobCount": skipped},
    )

    status, exit_code = resolve_job_run_status(failed_count=0, warning_count=0)
    save_last_success(
        "silver_quiver_data",
        when=datetime.now(timezone.utc),
        metadata={"status": status, "candidate_blob_count": len(candidate_blobs), "skipped_blob_count": skipped},
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
