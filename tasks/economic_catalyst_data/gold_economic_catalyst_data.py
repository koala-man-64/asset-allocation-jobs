from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc

from tasks.common.watermarks import save_last_success
from tasks.economic_catalyst_data import constants
from tasks.economic_catalyst_data.config import EconomicCatalystConfig
from tasks.economic_catalyst_data.postgres_sync import replace_postgres_tables, upsert_source_state
from tasks.economic_catalyst_data.storage import load_parquet_snapshot, write_domain_artifact, write_parquet_snapshot
from tasks.economic_catalyst_data.transform import build_entity_daily


def _source_commit(*frames: pd.DataFrame) -> str:
    digest = hashlib.md5()
    for frame in frames:
        digest.update(str(frame.shape).encode("utf-8"))
        if not frame.empty:
            digest.update(frame.head(250).to_json(date_format="iso", orient="records").encode("utf-8"))
    return digest.hexdigest()


def main() -> int:
    mdc.log_environment_diagnostics()
    config = EconomicCatalystConfig.from_env()
    silver_client = mdc.get_storage_client(config.silver_container)
    gold_client = mdc.get_storage_client(config.gold_container)
    if silver_client is None or gold_client is None:
        raise RuntimeError("Economic catalyst gold requires both silver and gold storage clients.")

    events = load_parquet_snapshot(client=silver_client, path=constants.silver_table_path("events"), columns=constants.EVENT_COLUMNS)
    event_versions = load_parquet_snapshot(
        client=silver_client,
        path=constants.silver_table_path("event_versions"),
        columns=constants.EVENT_VERSION_COLUMNS,
    )
    headlines = load_parquet_snapshot(
        client=silver_client,
        path=constants.silver_table_path("headlines"),
        columns=constants.HEADLINE_COLUMNS,
    )
    headline_versions = load_parquet_snapshot(
        client=silver_client,
        path=constants.silver_table_path("headline_versions"),
        columns=constants.HEADLINE_VERSION_COLUMNS,
    )
    mentions = load_parquet_snapshot(
        client=silver_client,
        path=constants.silver_table_path("mentions"),
        columns=constants.MENTION_COLUMNS,
    )
    entity_daily = build_entity_daily(events=events, headlines=headlines, mentions=mentions)

    for table_name, frame in (
        ("events", events),
        ("event_versions", event_versions),
        ("headlines", headlines),
        ("headline_versions", headline_versions),
        ("mentions", mentions),
        ("entity_daily", entity_daily),
    ):
        write_parquet_snapshot(client=gold_client, path=constants.gold_table_path(table_name), frame=frame)

    source_commit = _source_commit(events, event_versions, headlines, headline_versions, mentions, entity_daily)
    write_domain_artifact(
        client=gold_client,
        layer="gold",
        job_name=constants.GOLD_JOB_NAME,
        run_id=str(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME") or "").strip(),
        tables={
            "events": events,
            "event_versions": event_versions,
            "headlines": headlines,
            "headline_versions": headline_versions,
            "mentions": mentions,
            "entity_daily": entity_daily,
        },
        extra_metadata={"sourceCommit": source_commit},
    )

    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if dsn:
        replace_postgres_tables(
            dsn,
            events=events,
            event_versions=event_versions,
            headlines=headlines,
            headline_versions=headline_versions,
            mentions=mentions,
            entity_daily=entity_daily,
        )
        for dataset_name, frame in (
            ("events", events),
            ("event_versions", event_versions),
            ("headlines", headlines),
            ("headline_versions", headline_versions),
            ("mentions", mentions),
            ("entity_daily", entity_daily),
        ):
            last_effective_at = None
            if not frame.empty and "effective_at" in frame.columns:
                parsed = pd.to_datetime(frame["effective_at"], errors="coerce", utc=True).dropna()
                if not parsed.empty:
                    last_effective_at = parsed.max().to_pydatetime()
            last_published_at = None
            if not frame.empty and "published_at" in frame.columns:
                parsed = pd.to_datetime(frame["published_at"], errors="coerce", utc=True).dropna()
                if not parsed.empty:
                    last_published_at = parsed.max().to_pydatetime()
            upsert_source_state(
                dsn,
                source_name="gold_economic_catalyst",
                dataset_name=dataset_name,
                state_type="gold_apply",
                cursor_value=dataset_name,
                source_commit=source_commit,
                last_effective_at=last_effective_at,
                last_published_at=last_published_at,
                last_ingested_at=datetime.now(timezone.utc),
                metadata={"rowCount": int(len(frame))},
            )

    save_last_success(
        "gold_economic_catalyst_data",
        when=datetime.now(timezone.utc),
        metadata={
            "source_commit": source_commit,
            "event_rows": int(len(events)),
            "event_version_rows": int(len(event_versions)),
            "headline_rows": int(len(headlines)),
            "headline_version_rows": int(len(headline_versions)),
            "mention_rows": int(len(mentions)),
            "entity_daily_rows": int(len(entity_daily)),
            "postgres_applied": bool(dsn),
        },
    )
    mdc.write_line(
        "Economic catalyst gold complete: "
        f"events={len(events)} event_versions={len(event_versions)} headlines={len(headlines)} "
        f"headline_versions={len(headline_versions)} mentions={len(mentions)} entity_daily={len(entity_daily)} "
        f"postgres_applied={str(bool(dsn)).lower()}"
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
                    lambda: write_system_health_marker(layer="gold", domain="economic-catalyst", job_name=job_name),
                    trigger_next_job_from_env,
                ),
            )
        )
