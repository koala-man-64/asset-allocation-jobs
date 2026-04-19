from __future__ import annotations

import pandas as pd

from tasks.quiver_data.transform import (
    build_government_contract_features,
    build_institutional_holding_change_features,
)


def test_institutional_holding_change_features_capture_breadth_and_normalized_change() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "NVDA",
                "public_availability_time": "2026-04-01T00:00:00Z",
                "change_pct": 0.25,
                "held_normalized": 10.0,
            },
            {
                "symbol": "NVDA",
                "public_availability_time": "2026-04-02T00:00:00Z",
                "change_pct": -0.10,
                "held_normalized": 8.0,
            },
        ]
    )
    features = build_institutional_holding_change_features(frame)

    assert "breadth_30d" in features.columns
    assert "held_normalized_30d" in features.columns


def test_government_contract_features_do_not_emit_forward_looking_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "PLTR",
                "public_availability_time": "2026-04-01T00:00:00Z",
                "amount_numeric": 10_000_000.0,
                "spy_change": 0.2,
            }
        ]
    )
    features = build_government_contract_features(frame)

    assert "spy_change" not in features.columns
