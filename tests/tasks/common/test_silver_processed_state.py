from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from tasks.common import silver_processed_state as state


def test_signature_ignores_ingested_at() -> None:
    left = pd.DataFrame(
        [
            {"symbol": "AAPL", "date": "2026-01-01", "close": 10.0, "ingested_at": "2026-01-01T00:00:00Z"}
        ]
    )
    right = pd.DataFrame(
        [
            {"symbol": "AAPL", "date": "2026-01-01", "close": 10.0, "ingested_at": "2026-01-02T00:00:00Z"}
        ]
    )

    assert state.stable_frame_signature(left, exclude_columns=("ingested_at",)) == state.stable_frame_signature(
        right,
        exclude_columns=("ingested_at",),
    )


def test_matching_signature_skips_even_when_source_timestamp_is_newer() -> None:
    frame = pd.DataFrame(
        [
            {"symbol": "AAPL", "date": "2026-01-01", "close": 10.0, "ingested_at": "2026-01-02T00:00:00Z"}
        ]
    )
    current = state.build_processed_state_record(
        domain="market",
        bucket="A",
        entity_key=state.make_entity_key(domain="market", bucket="A", symbol="AAPL"),
        symbol="AAPL",
        processor_version="v1",
        source_frame=frame,
        processed_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    prior = dict(current)
    prior["last_processed_at"] = "2026-01-01T00:00:00Z"

    should_process, reason = state.should_process_entity(current, prior)

    assert should_process is False
    assert reason == "processed_state"


def test_as_of_change_forces_reprocess() -> None:
    current = {
        "status": "ok",
        "processor_version": "v1",
        "source_signature": "same",
        "as_of_date": "2026-05-07",
    }
    prior = {
        "status": "ok",
        "processor_version": "v1",
        "source_signature": "same",
        "as_of_date": "2026-05-06",
    }

    should_process, reason = state.should_process_entity(current, prior)

    assert should_process is True
    assert reason == "as_of_date"


def test_timestamp_fallback_skips_when_source_was_already_processed() -> None:
    current = {
        "status": "ok",
        "processor_version": "v1",
        "source_signature": "",
        "source_max_ingested_at": "2026-05-06T12:00:00Z",
    }
    prior = {
        "status": "ok",
        "processor_version": "v1",
        "source_signature": "",
        "last_processed_at": "2026-05-07T12:00:00Z",
    }

    should_process, reason = state.should_process_entity(current, prior)

    assert should_process is False
    assert reason == "processed_state_timestamp"


def test_merge_processed_state_updates_replaces_same_entity() -> None:
    existing = pd.DataFrame(
        [
            {
                "schema_version": 1,
                "domain": "market",
                "sub_domain": "",
                "bucket": "A",
                "entity_key": "market|A|AAPL",
                "symbol": "AAPL",
                "source_signature": "old",
                "source_max_ingested_at": "",
                "source_min_date": "",
                "source_max_date": "",
                "as_of_date": "",
                "processor_version": "v1",
                "last_processed_at": "2026-05-06T00:00:00Z",
                "last_success_run_id": "",
                "status": "ok",
                "row_count": 1,
                "output_paths_json": "[]",
            }
        ]
    )
    update = dict(existing.iloc[0])
    update["source_signature"] = "new"
    update["last_processed_at"] = "2026-05-07T00:00:00Z"

    merged = state.merge_processed_state_updates(existing, [update])

    assert len(merged) == 1
    assert str(merged.iloc[0]["source_signature"]) == "new"
