from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from asset_allocation_runtime_common.domain.regime import (
    build_regime_outputs,
    classify_regime_row,
    compute_curve_state,
    compute_trend_state,
    next_business_session,
)


def test_compute_states_use_canonical_boundaries() -> None:
    assert compute_trend_state(0.02) == "near_zero"
    assert compute_trend_state(0.0201) == "positive"
    assert compute_trend_state(-0.02) == "near_zero"
    assert compute_trend_state(-0.0201) == "negative"

    assert compute_curve_state(0.50) == "contango"
    assert compute_curve_state(0.49) == "flat"
    assert compute_curve_state(-0.50) == "inverted"
    assert compute_curve_state(-0.49) == "flat"


def test_classify_regime_row_activates_multiple_labels_independently() -> None:
    row = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "spy_close": 110.0,
            "qqq_close": 120.0,
            "spy_sma_200d": 100.0,
            "qqq_sma_200d": 100.0,
            "return_20d": 0.04,
            "vix_spot_close": 14.0,
            "atr_14d": 2.5,
            "bb_width_20d": 0.10,
            "gap_atr": 0.20,
            "volume_pct_rank_252d": 0.55,
            "hy_oas_z_20d": 0.10,
            "acwi_return_20d": 0.03,
            "rates_event_flag": False,
            "vix_gt_32_streak": 0,
        }
    )

    active_regimes = set(row["active_regimes"])
    signals_by_code = {signal["regime_code"]: signal for signal in row["signals"]}

    assert "trending_up" in active_regimes
    assert "low_volatility" in active_regimes
    assert "unclassified" not in active_regimes
    assert signals_by_code["trending_up"]["signal_state"] == "active"
    assert signals_by_code["macro_alignment"]["signal_state"] == "inactive"
    assert row["halt_flag"] is False


def test_classify_regime_row_marks_unclassified_when_inputs_are_incomplete() -> None:
    row = classify_regime_row(
        {
            "inputs_complete_flag": False,
            "return_20d": None,
            "vix_spot_close": None,
        }
    )

    signals_by_code = {signal["regime_code"]: signal for signal in row["signals"]}
    assert row["active_regimes"] == ["unclassified"]
    assert signals_by_code["unclassified"]["signal_state"] == "active"
    assert signals_by_code["trending_up"]["signal_state"] == "insufficient_data"


def test_next_business_session_skips_weekends_and_market_holidays() -> None:
    assert next_business_session(pd.Timestamp("2026-03-06").date()).isoformat() == "2026-03-09"
    assert next_business_session(pd.Timestamp("2026-04-02").date()).isoformat() == "2026-04-06"


def test_build_regime_outputs_emits_normalized_history_latest_and_enter_exit_transitions() -> None:
    inputs = pd.DataFrame(
        [
            {
                "as_of_date": "2026-03-02",
                "spy_close": 110.0,
                "qqq_close": 120.0,
                "spy_sma_200d": 100.0,
                "qqq_sma_200d": 100.0,
                "return_20d": 0.04,
                "acwi_return_20d": 0.03,
                "atr_14d": 2.5,
                "bb_width_20d": 0.10,
                "gap_atr": 0.20,
                "volume_pct_rank_252d": 0.55,
                "vix_spot_close": 14.0,
                "vix3m_close": 15.0,
                "vix_slope": 1.0,
                "hy_oas_z_20d": 0.10,
                "rates_event_flag": False,
                "rsi_14d": 74.0,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-03-04",
                "spy_close": 90.0,
                "qqq_close": 92.0,
                "spy_sma_200d": 100.0,
                "qqq_sma_200d": 100.0,
                "return_20d": -0.05,
                "acwi_return_20d": -0.04,
                "atr_14d": 4.5,
                "bb_width_20d": 0.16,
                "gap_atr": 0.85,
                "volume_pct_rank_252d": 0.10,
                "vix_spot_close": 28.0,
                "vix3m_close": 26.5,
                "vix_slope": -1.5,
                "hy_oas_z_20d": 1.5,
                "rates_event_flag": True,
                "rsi_14d": 42.0,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
        ]
    )

    history, latest, transitions = build_regime_outputs(
        inputs,
        model_name="default-regime",
        model_version=3,
        computed_at=datetime(2026, 4, 7, tzinfo=timezone.utc),
    )

    assert len(history) == 16
    assert history["effective_from_date"].drop_duplicates().tolist()[0].isoformat() == "2026-03-03"
    assert history["effective_from_date"].drop_duplicates().tolist()[1].isoformat() == "2026-03-05"
    assert latest["as_of_date"].nunique() == 1
    assert latest.iloc[0]["as_of_date"].isoformat() == "2026-03-04"
    assert latest["regime_code"].nunique() == 8

    transition_pairs = {
        (row["regime_code"], row["transition_type"])
        for row in transitions.to_dict("records")
    }
    assert ("trending_up", "entered") in transition_pairs
    assert ("trending_up", "exited") in transition_pairs
    assert ("trending_down", "entered") in transition_pairs
