from __future__ import annotations

from typing import Any

import pandas as pd

from tasks.economic_catalyst_data import constants
from tasks.economic_catalyst_data import postgres_sync


class _FakeCursor:
    def __init__(self, *, fetchone_rows=None, rowcount_map: dict[str, int] | None = None) -> None:
        self.fetchone_rows = list(fetchone_rows or [])
        self.rowcount_map = dict(rowcount_map or {})
        self.executed: list[tuple[str, object]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))
        self.rowcount = 0
        for pattern, value in self.rowcount_map.items():
            if pattern in sql:
                self.rowcount = int(value)
                break

    def fetchone(self):
        if not self.fetchone_rows:
            return None
        return self.fetchone_rows.pop(0)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _frame(columns: tuple[str, ...], values: dict[str, Any]) -> pd.DataFrame:
    row = {column: pd.NA for column in columns}
    row.update(values)
    return pd.DataFrame([row], columns=list(columns))


def test_replace_postgres_tables_uses_staged_apply_for_all_serving_tables(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[("off",), (False,)],
        rowcount_map={
            "DELETE FROM gold.economic_catalyst_events AS target": 1,
            "INSERT INTO gold.economic_catalyst_events AS target": 1,
            "DELETE FROM gold.economic_catalyst_event_versions AS target": 1,
            "INSERT INTO gold.economic_catalyst_event_versions AS target": 1,
            "DELETE FROM gold.economic_catalyst_headlines AS target": 1,
            "INSERT INTO gold.economic_catalyst_headlines AS target": 1,
            "DELETE FROM gold.economic_catalyst_headline_versions AS target": 1,
            "INSERT INTO gold.economic_catalyst_headline_versions AS target": 1,
            "DELETE FROM gold.economic_catalyst_mentions AS target": 1,
            "INSERT INTO gold.economic_catalyst_mentions AS target": 1,
            "DELETE FROM gold.economic_catalyst_entity_daily AS target": 1,
            "INSERT INTO gold.economic_catalyst_entity_daily AS target": 1,
        },
    )
    copied_tables: list[str] = []
    messages: list[str] = []

    monkeypatch.setattr(postgres_sync, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(postgres_sync, "copy_rows", lambda _cur, *, table, columns, rows: copied_tables.append(str(table)))
    monkeypatch.setattr(postgres_sync.mdc, "write_line", lambda message: messages.append(str(message)))

    event = _frame(
        constants.EVENT_COLUMNS,
        {
            "event_id": "event-1",
            "event_key": "event-1",
            "event_name": "Consumer Price Index",
            "event_group": "Inflation",
            "event_type": "macro_release",
            "importance_tier": "high",
            "impact_domain": "macro",
            "source_name": "bls_release_calendar",
            "source_event_key": "cpi:2026-03",
            "effective_at": pd.Timestamp("2026-04-10T12:30:00Z"),
            "published_at": pd.Timestamp("2026-03-15T12:00:00Z"),
            "source_updated_at": pd.Timestamp("2026-04-10T12:31:00Z"),
            "ingested_at": pd.Timestamp("2026-04-10T12:31:00Z"),
            "time_precision": "exact",
            "schedule_status": "released",
            "is_confirmed": True,
            "actual_numeric": 3.1,
            "consensus_numeric": 2.9,
            "previous_numeric": 2.8,
            "revised_previous_numeric": 2.85,
            "market_sensitivity_tags_json": "[]",
            "sector_tags_json": "[]",
            "factor_tags_json": "[\"inflation\"]",
            "is_high_impact": True,
            "is_routine": True,
            "is_revisionable": True,
            "withdrawal_flag": False,
            "source_hash": "event-hash",
            "provenance_json": "{\"contributors\": []}",
        },
    )
    event_version = _frame(
        constants.EVENT_VERSION_COLUMNS,
        {
            "version_id": "event-version-1",
            "event_id": "event-1",
            "version_seq": 1,
            "version_kind": "release",
            "version_observed_at": pd.Timestamp("2026-04-10T12:31:00Z"),
            **event.iloc[0].to_dict(),
        },
    )
    headline = _frame(
        constants.HEADLINE_COLUMNS,
        {
            "headline_id": "headline-1",
            "headline_key": "headline-1",
            "source_name": "massive_news",
            "source_item_id": "headline-1",
            "headline": "Fed signals inflation focus",
            "summary": "Updated summary",
            "published_at": pd.Timestamp("2026-04-18T12:00:00Z"),
            "source_updated_at": pd.Timestamp("2026-04-18T12:05:00Z"),
            "ingested_at": pd.Timestamp("2026-04-18T12:05:00Z"),
            "country": "US",
            "region": "North America",
            "event_group": "CentralBankPolicy",
            "importance_tier": "high",
            "relevance_tier": "high",
            "withdrawal_flag": False,
            "tags_json": "[\"macro\"]",
            "tickers_json": "[\"SPY\"]",
            "channels_json": "[\"news\"]",
            "source_hash": "headline-hash",
            "provenance_json": "{\"contributors\": []}",
        },
    )
    headline_version = _frame(
        constants.HEADLINE_VERSION_COLUMNS,
        {
            "version_id": "headline-version-1",
            "headline_id": "headline-1",
            "version_seq": 1,
            "version_kind": "publish",
            "version_observed_at": pd.Timestamp("2026-04-18T12:05:00Z"),
            **headline.iloc[0].to_dict(),
        },
    )
    mention = _frame(
        constants.MENTION_COLUMNS,
        {
            "item_kind": "headline",
            "item_id": "headline-1",
            "entity_type": "symbol",
            "entity_key": "SPY",
            "relevance_tier": "high",
            "confidence": 1.0,
            "mapping_rule_version": "v1",
            "source_name": "massive_news",
            "published_at": pd.Timestamp("2026-04-18T12:00:00Z"),
            "effective_at": pd.Timestamp("2026-04-18T12:00:00Z"),
            "ingested_at": pd.Timestamp("2026-04-18T12:05:00Z"),
        },
    )
    entity_daily = _frame(
        constants.ENTITY_DAILY_COLUMNS,
        {
            "as_of_date": pd.Timestamp("2026-04-18").date(),
            "entity_type": "symbol",
            "entity_key": "SPY",
            "headline_count": 1,
            "event_count": 0,
            "high_impact_event_count": 0,
            "release_count": 0,
            "scheduled_count": 0,
            "policy_event_count": 0,
            "inflation_event_count": 0,
            "labor_event_count": 0,
            "growth_event_count": 0,
            "rates_event_count": 0,
            "last_published_at": pd.Timestamp("2026-04-18T12:00:00Z"),
            "last_effective_at": pd.Timestamp("2026-04-18T12:00:00Z"),
            "ingested_at": pd.Timestamp("2026-04-18T12:05:00Z"),
        },
    )

    postgres_sync.replace_postgres_tables(
        "postgresql://test",
        events=event,
        event_versions=event_version,
        headlines=headline,
        headline_versions=headline_version,
        mentions=mention,
        entity_daily=entity_daily,
    )

    assert copied_tables == [
        "pg_temp.economic_catalyst_stage_economic_catalyst_events",
        "pg_temp.economic_catalyst_stage_economic_catalyst_event_versions",
        "pg_temp.economic_catalyst_stage_economic_catalyst_headlines",
        "pg_temp.economic_catalyst_stage_economic_catalyst_headline_versions",
        "pg_temp.economic_catalyst_stage_economic_catalyst_mentions",
        "pg_temp.economic_catalyst_stage_economic_catalyst_entity_daily",
    ]
    assert any("DELETE FROM gold.economic_catalyst_events AS target" in sql for sql, _ in cursor.executed)
    assert any("DELETE FROM gold.economic_catalyst_entity_daily AS target" in sql for sql, _ in cursor.executed)
    assert any("INSERT INTO gold.economic_catalyst_headline_versions AS target" in sql for sql, _ in cursor.executed)
    assert all("TRUNCATE TABLE gold.economic_catalyst_events" not in sql for sql, _ in cursor.executed)
    assert sum("economic_catalyst_postgres_apply_stats" in message for message in messages) == 6
