from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc

from tasks.common.watermarks import save_last_success
from tasks.quiver_data import constants
from tasks.quiver_data.config import QuiverDataConfig
from tasks.quiver_data.storage import load_parquet_snapshot, write_domain_artifact, write_parquet_snapshot
from tasks.quiver_data.transform import (
    build_government_contract_features,
    build_insider_trading_features,
    build_institutional_holding_change_features,
    build_political_trading_features,
)


def _load_feature_source(*, client, dataset_family: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for bucket in tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ("X",):
        path = constants.silver_table_path(dataset_family, bucket)
        frame = load_parquet_snapshot(client=client, path=path)
        if frame is not None and not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def main() -> int:
    mdc.log_environment_diagnostics()
    config = QuiverDataConfig.from_env()
    silver_client = mdc.get_storage_client(config.silver_container)
    gold_client = mdc.get_storage_client(config.gold_container)
    if silver_client is None or gold_client is None:
        raise RuntimeError("Quiver gold requires both silver and gold storage clients.")

    feature_sources = {
        "insider_trading": build_insider_trading_features(_load_feature_source(client=silver_client, dataset_family="insider_trading")),
        "institutional_holding_changes": build_institutional_holding_change_features(
            _load_feature_source(client=silver_client, dataset_family="institutional_holding_changes")
        ),
        "political_trading": build_political_trading_features(
            _load_feature_source(client=silver_client, dataset_family="political_trading")
        ),
        "government_contracts": build_government_contract_features(
            pd.concat(
                [
                    _load_feature_source(client=silver_client, dataset_family="government_contracts"),
                    _load_feature_source(client=silver_client, dataset_family="government_contracts_all"),
                ],
                ignore_index=True,
                sort=False,
            )
        ),
    }

    table_payloads: dict[str, pd.DataFrame] = {}
    for dataset_family, frame in feature_sources.items():
        if frame.empty:
            continue
        for bucket, bucket_frame in frame.groupby(frame["symbol"].fillna("").map(constants.normalize_bucket), sort=False):
            path = constants.feature_table_path(dataset_family, bucket)
            write_parquet_snapshot(client=gold_client, path=path, frame=bucket_frame.reset_index(drop=True))
            table_payloads[f"{dataset_family}:{bucket}"] = bucket_frame.reset_index(drop=True)

    write_domain_artifact(
        client=gold_client,
        layer="gold",
        job_name=constants.GOLD_JOB_NAME,
        run_id=str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip(),
        tables=table_payloads,
        extra_metadata={"featureDatasets": list(table_payloads.keys())},
    )
    save_last_success(
        "gold_quiver_data",
        when=datetime.now(timezone.utc),
        metadata={"feature_datasets": list(table_payloads.keys())},
    )
    return 0


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = constants.GOLD_JOB_NAME
    with mdc.JobLock(job_name, conflict_policy="fail"):
        ensure_api_awake_from_env(required=True)
        raise SystemExit(
            run_logged_job(
                job_name=job_name,
                run=main,
                on_success=(
                    lambda: write_system_health_marker(
                        layer="gold",
                        domain=constants.domain_slug_for_layer("gold"),
                        job_name=job_name,
                    ),
                    trigger_next_job_from_env,
                ),
            )
        )
