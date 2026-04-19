from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from tasks.economic_catalyst_data import constants
from tasks.economic_catalyst_data import transform


def _frame(records: list[dict[str, object]], columns: tuple[str, ...]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame.loc[:, list(columns)].copy()


def test_canonicalize_source_state_fuses_event_sources_and_builds_versions_mentions() -> None:
    official_row = transform._build_source_event_record(
        source_name="bls_release_calendar",
        dataset_name="release_calendar",
        source_event_key="cpi:2026-03",
        event_name="Consumer Price Index",
        effective_at=datetime(2026, 4, 10, 12, 30, tzinfo=timezone.utc),
        published_at=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
        source_updated_at=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc),
        period_label="2026-03",
        time_precision="exact",
        schedule_status="scheduled",
        summary="Official BLS release calendar.",
        source_url="https://example.com/bls",
    )
    vendor_row = transform._build_source_event_record(
        source_name="nasdaq_tables",
        dataset_name="macro_table",
        source_event_key="cpi:2026-03",
        event_name="Consumer Price Index",
        effective_at=datetime(2026, 4, 10, 12, 30, tzinfo=timezone.utc),
        published_at=datetime(2026, 4, 10, 12, 30, tzinfo=timezone.utc),
        source_updated_at=datetime(2026, 4, 10, 12, 31, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 4, 10, 12, 31, tzinfo=timezone.utc),
        period_label="2026-03",
        country="US",
        region="North America",
        currency="USD",
        actual_numeric=3.1,
        consensus_numeric=2.9,
        previous_numeric=2.8,
        revised_previous_numeric=2.85,
        unit="%",
        time_precision="exact",
        schedule_status="released",
        summary="Structured vendor overlay.",
        source_url="https://example.com/nasdaq",
    )

    canonical = transform.canonicalize_source_state(
        source_events=_frame([official_row, vendor_row], constants.INTERNAL_SOURCE_EVENT_COLUMNS),
        source_headlines=_frame([], constants.INTERNAL_SOURCE_HEADLINE_COLUMNS),
        existing_quarantine=_frame([], constants.QUARANTINE_COLUMNS),
    )

    events = canonical["events"]
    versions = canonical["event_versions"]
    mentions = canonical["mentions"]

    assert len(events) == 1
    row = events.iloc[0]
    assert row["source_name"] == "bls_release_calendar"
    assert row["official_source_name"] == "bls_release_calendar"
    assert row["actual_numeric"] == 3.1
    assert row["consensus_numeric"] == 2.9
    assert row["surprise_abs"] == pytest.approx(0.2)
    assert row["schedule_status"] == "released"
    assert len(versions) == 2
    assert versions["version_kind"].tolist() == ["schedule", "release"]
    assert set(mentions["entity_type"]) >= {"country", "region", "currency", "indicator", "factor"}


def test_canonicalize_source_state_tracks_headline_edits_and_entity_daily_rollup() -> None:
    headline_v1 = transform._build_source_headline_record(
        source_name="massive_news",
        dataset_name="benzinga_news",
        source_item_id="headline-1",
        headline="Fed signals inflation focus",
        summary="Initial summary",
        url="https://example.com/news/1",
        author="Desk",
        published_at=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        source_updated_at=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        country="US",
        region="North America",
        event_group="CentralBankPolicy",
        importance_tier="high",
        relevance_tier="high",
        tickers=["SPY"],
        tags=["macro", "policy"],
    )
    headline_v2 = transform._build_source_headline_record(
        source_name="massive_news",
        dataset_name="benzinga_news",
        source_item_id="headline-1",
        headline="Fed signals inflation focus",
        summary="Updated summary after release",
        url="https://example.com/news/1",
        author="Desk",
        published_at=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        source_updated_at=datetime(2026, 4, 18, 12, 5, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 4, 18, 12, 5, tzinfo=timezone.utc),
        country="US",
        region="North America",
        event_group="CentralBankPolicy",
        importance_tier="high",
        relevance_tier="high",
        tickers=["SPY"],
        tags=["macro", "policy"],
    )

    canonical = transform.canonicalize_source_state(
        source_events=_frame([], constants.INTERNAL_SOURCE_EVENT_COLUMNS),
        source_headlines=_frame([headline_v1, headline_v2], constants.INTERNAL_SOURCE_HEADLINE_COLUMNS),
        existing_quarantine=_frame([], constants.QUARANTINE_COLUMNS),
    )
    headlines = canonical["headlines"]
    headline_versions = canonical["headline_versions"]
    mentions = canonical["mentions"]
    entity_daily = transform.build_entity_daily(events=canonical["events"], headlines=headlines, mentions=mentions)

    assert len(headlines) == 1
    assert headlines.iloc[0]["summary"] == "Updated summary after release"
    assert headline_versions["version_kind"].tolist() == ["publish", "edit"]
    assert ("symbol" in set(mentions["entity_type"])) is True
    symbol_bucket = entity_daily.loc[
        (entity_daily["entity_type"] == "symbol") & (entity_daily["entity_key"] == "SPY")
    ].iloc[0]
    assert symbol_bucket["headline_count"] == 1
    assert symbol_bucket["event_count"] == 0
