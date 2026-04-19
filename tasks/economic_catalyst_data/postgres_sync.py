from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Sequence

import pandas as pd

from asset_allocation_runtime_common.foundation.postgres import connect, copy_rows
from asset_allocation_runtime_common.market_data import core as mdc

from tasks.economic_catalyst_data import constants


@dataclass(frozen=True)
class ApplyConfig:
    table: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    delete_missing: bool = True


APPLY_CONFIGS: tuple[ApplyConfig, ...] = (
    ApplyConfig("gold.economic_catalyst_events", constants.EVENT_COLUMNS, ("event_id",)),
    ApplyConfig("gold.economic_catalyst_event_versions", constants.EVENT_VERSION_COLUMNS, ("version_id",)),
    ApplyConfig("gold.economic_catalyst_headlines", constants.HEADLINE_COLUMNS, ("headline_id",)),
    ApplyConfig("gold.economic_catalyst_headline_versions", constants.HEADLINE_VERSION_COLUMNS, ("version_id",)),
    ApplyConfig(
        "gold.economic_catalyst_mentions",
        constants.MENTION_COLUMNS,
        ("item_kind", "item_id", "entity_type", "entity_key"),
    ),
    ApplyConfig(
        "gold.economic_catalyst_entity_daily",
        constants.ENTITY_DAILY_COLUMNS,
        ("as_of_date", "entity_type", "entity_key"),
    ),
)


def _quote(identifier: str) -> str:
    return '"' + str(identifier or "").replace('"', '""') + '"'


def _coerce_cell(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.to_pydatetime()
        return value.tz_convert("UTC").to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return value


def _frame_rows(frame: pd.DataFrame, columns: Sequence[str]) -> list[tuple[Any, ...]]:
    if frame is None or frame.empty:
        return []
    return [
        tuple(_coerce_cell(row.get(column)) for column in columns)
        for row in frame.loc[:, list(columns)].to_dict(orient="records")
    ]


def _cursor_rowcount(cur: Any) -> int:
    try:
        value = int(getattr(cur, "rowcount", 0) or 0)
    except Exception:
        value = 0
    return max(value, 0)


def _ensure_connection_is_writable(cur: Any) -> None:
    cur.execute("SHOW transaction_read_only")
    row = cur.fetchone()
    if row and str(row[0]).strip().lower() == "on":
        raise RuntimeError("Postgres target is transaction_read_only=on")
    cur.execute("SELECT pg_is_in_recovery()")
    recovery_row = cur.fetchone()
    if recovery_row and bool(recovery_row[0]):
        raise RuntimeError("Postgres target is in recovery mode")


def _stage_name(table: str) -> str:
    return f"economic_catalyst_stage_{str(table or '').split('.')[-1]}"


def _create_stage(cur: Any, *, config: ApplyConfig) -> str:
    name = _stage_name(config.table)
    quoted_keys = ", ".join(_quote(column) for column in config.key_columns)
    cur.execute(f"CREATE TEMP TABLE {name} (LIKE {config.table} INCLUDING DEFAULTS) ON COMMIT DROP")
    cur.execute(f"CREATE UNIQUE INDEX {name}_key_idx ON {name} ({quoted_keys})")
    return f"pg_temp.{name}"


def _key_match(left_alias: str, right_alias: str, columns: Iterable[str]) -> str:
    return " AND ".join(
        f"{left_alias}.{_quote(column)} = {right_alias}.{_quote(column)}"
        for column in columns
    )


def _changed_match(left_alias: str, right_alias: str, columns: Iterable[str]) -> str:
    comparisons = [
        f"{left_alias}.{_quote(column)} IS DISTINCT FROM {right_alias}.{_quote(column)}"
        for column in columns
    ]
    return " OR ".join(comparisons) if comparisons else "FALSE"


def _delete_missing(cur: Any, *, config: ApplyConfig, stage_table: str) -> int:
    if not config.delete_missing:
        return 0
    match_sql = _key_match("stage", "target", config.key_columns)
    cur.execute(
        f"""
        DELETE FROM {config.table} AS target
        WHERE NOT EXISTS (
            SELECT 1
            FROM {stage_table} AS stage
            WHERE {match_sql}
        )
        """
    )
    return _cursor_rowcount(cur)


def _upsert_changed(cur: Any, *, config: ApplyConfig, stage_table: str) -> int:
    non_key_columns = tuple(column for column in config.columns if column not in config.key_columns)
    quoted_columns = ", ".join(_quote(column) for column in config.columns)
    assignment_sql = ", ".join(
        f"{_quote(column)} = EXCLUDED.{_quote(column)}"
        for column in non_key_columns
    )
    change_sql = _changed_match("target", "stage", non_key_columns)
    key_match_sql = _key_match("stage", "target", config.key_columns)
    conflict_sql = ", ".join(_quote(column) for column in config.key_columns)
    where_sql = "TRUE"
    if non_key_columns:
        where_sql = (
            f"NOT EXISTS (SELECT 1 FROM {config.table} AS target WHERE {key_match_sql}) "
            f"OR EXISTS (SELECT 1 FROM {config.table} AS target WHERE {key_match_sql} AND ({change_sql}))"
        )
    cur.execute(
        f"""
        INSERT INTO {config.table} AS target ({quoted_columns})
        SELECT {quoted_columns}
        FROM {stage_table} AS stage
        WHERE {where_sql}
        ON CONFLICT ({conflict_sql}) DO UPDATE
        SET {assignment_sql}
        """
    )
    return _cursor_rowcount(cur)


def _apply_table(cur: Any, *, config: ApplyConfig, frame: pd.DataFrame) -> None:
    stage_table = _create_stage(cur, config=config)
    rows = _frame_rows(frame, config.columns)
    if rows:
        copy_rows(cur, table=stage_table, columns=config.columns, rows=rows)
    staged_rows = len(rows)
    deleted_rows = _delete_missing(cur, config=config, stage_table=stage_table)
    upserted_rows = _upsert_changed(cur, config=config, stage_table=stage_table) if rows else 0
    unchanged_rows = max(staged_rows - upserted_rows, 0)
    mdc.write_line(
        "economic_catalyst_postgres_apply_stats "
        f"table={config.table} staged_rows={staged_rows} deleted_rows={deleted_rows} "
        f"upserted_rows={upserted_rows} unchanged_rows={unchanged_rows}"
    )


def replace_postgres_tables(
    dsn: str,
    *,
    events: pd.DataFrame,
    event_versions: pd.DataFrame,
    headlines: pd.DataFrame,
    headline_versions: pd.DataFrame,
    mentions: pd.DataFrame,
    entity_daily: pd.DataFrame,
) -> None:
    frames = {
        "gold.economic_catalyst_events": events,
        "gold.economic_catalyst_event_versions": event_versions,
        "gold.economic_catalyst_headlines": headlines,
        "gold.economic_catalyst_headline_versions": headline_versions,
        "gold.economic_catalyst_mentions": mentions,
        "gold.economic_catalyst_entity_daily": entity_daily,
    }
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            _ensure_connection_is_writable(cur)
            for config in APPLY_CONFIGS:
                _apply_table(cur, config=config, frame=frames[config.table])


def upsert_source_state(
    dsn: str,
    *,
    source_name: str,
    dataset_name: str,
    state_type: str,
    cursor_value: str | None = None,
    source_commit: str | None = None,
    last_effective_at: datetime | None = None,
    last_published_at: datetime | None = None,
    last_source_updated_at: datetime | None = None,
    last_ingested_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    payload = json.dumps(metadata or {}, ensure_ascii=False, default=str)
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            _ensure_connection_is_writable(cur)
            cur.execute(
                """
                INSERT INTO core.economic_catalyst_source_state (
                    source_name,
                    dataset_name,
                    state_type,
                    cursor_value,
                    source_commit,
                    last_effective_at,
                    last_published_at,
                    last_source_updated_at,
                    last_ingested_at,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (source_name, dataset_name, state_type) DO UPDATE
                SET cursor_value = EXCLUDED.cursor_value,
                    source_commit = EXCLUDED.source_commit,
                    last_effective_at = EXCLUDED.last_effective_at,
                    last_published_at = EXCLUDED.last_published_at,
                    last_source_updated_at = EXCLUDED.last_source_updated_at,
                    last_ingested_at = EXCLUDED.last_ingested_at,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                (
                    str(source_name or "").strip(),
                    str(dataset_name or "").strip(),
                    str(state_type or "").strip(),
                    cursor_value,
                    source_commit,
                    last_effective_at,
                    last_published_at,
                    last_source_updated_at,
                    last_ingested_at,
                    payload,
                ),
            )

