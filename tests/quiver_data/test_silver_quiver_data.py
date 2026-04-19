from __future__ import annotations

import pandas as pd

from tasks.quiver_data.transform import merge_normalized_frames, normalize_bronze_batch


def test_merge_normalized_frames_dedupes_by_source_hash() -> None:
    batch = {
        "source_dataset": "insiders_live",
        "dataset_family": "insider_trading",
        "rows": [{"Ticker": "AAPL", "Date": "2026-04-01T00:00:00Z", "uploaded": "2026-04-02T00:00:00Z"}],
    }
    frame = normalize_bronze_batch(batch)
    merged = merge_normalized_frames(frame, frame)

    assert len(merged) == 1
    assert merged.iloc[0]["symbol"] == "AAPL"


def test_normalize_bronze_batch_returns_empty_frame_for_invalid_rows() -> None:
    frame = normalize_bronze_batch({"source_dataset": "broken", "dataset_family": "lobbying", "rows": ["bad"]})
    assert isinstance(frame, pd.DataFrame)
    assert frame.empty
