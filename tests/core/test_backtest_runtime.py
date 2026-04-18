from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from core.backtest_runtime import (
    ResolvedBacktestDefinition,
    _apply_rebalance_target,
    _build_snapshot_symbol_index,
    _market_row,
    _maybe_update_heartbeat,
    _regime_context_for_session,
    _periods_per_year_from_bar_size,
    _rolling_window_periods,
    _score_snapshot,
    _compute_rolling_metrics,
    _compute_summary,
    validate_backtest_submission,
)
from core.ranking_engine.contracts import RankingSchemaConfig
from core.strategy_engine.contracts import StrategyConfig, UniverseDefinition
from core.strategy_engine.position_state import PositionState
from core.strategy_engine import universe as universe_service


def _sample_universe() -> UniverseDefinition:
    return UniverseDefinition.model_validate(
        {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {
                        "kind": "condition",
                        "table": "market_data",
                        "column": "close",
                        "operator": "gt",
                        "value": 1,
                    }
                ],
            },
        }
    )


def _sample_definition() -> ResolvedBacktestDefinition:
    universe = _sample_universe()
    return ResolvedBacktestDefinition(
        strategy_name="mom-spy-res",
        strategy_version=3,
        strategy_config=StrategyConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "rebalance": "weekly",
                "longOnly": True,
                "topN": 2,
                "lookbackWindow": 20,
                "holdingPeriod": 5,
                "costModel": "default",
                "rankingSchemaName": "quality",
                "intrabarConflictPolicy": "stop_first",
                "exits": [],
            }
        ),
        strategy_config_raw={
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "longOnly": True,
            "topN": 2,
            "lookbackWindow": 20,
            "holdingPeriod": 5,
            "costModel": "default",
            "rankingSchemaName": "quality",
            "intrabarConflictPolicy": "stop_first",
            "exits": [],
        },
        strategy_universe=universe,
        ranking_schema_name="quality",
        ranking_schema_version=7,
        ranking_schema=RankingSchemaConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "f1",
                                "table": "market_data",
                                "column": "return_20d",
                                "weight": 1,
                                "direction": "desc",
                                "missingValuePolicy": "exclude",
                                "transforms": [],
                            }
                        ],
                        "transforms": [],
                    }
                ],
                "overallTransforms": [],
            }
        ),
        ranking_universe_name="large-cap-quality",
        ranking_universe_version=5,
        ranking_universe=universe,
    )


def test_score_snapshot_breaks_ties_by_symbol() -> None:
    ranked = _score_snapshot(
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-03T14:30:00Z")] * 3,
                "symbol": ["MSFT", "AAPL", "NVDA"],
                "market_data__close": [10.0, 10.0, 10.0],
                "market_data__return_20d": [0.5, 0.5, 0.2],
            }
        ),
        definition=_sample_definition(),
        rebalance_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
    )

    assert ranked["symbol"].tolist()[:2] == ["AAPL", "MSFT"]
    assert ranked["ordinal"].tolist()[:2] == [1, 2]


def test_validate_backtest_submission_rejects_intraday_coverage_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    specs = {
        "market_data": universe_service.UniverseTableSpec(
            name="market_data",
            as_of_column="as_of_ts",
            as_of_kind="intraday",
            columns={
                "open": universe_service.UniverseColumnSpec("open", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "high": universe_service.UniverseColumnSpec("high", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "low": universe_service.UniverseColumnSpec("low", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "close": universe_service.UniverseColumnSpec("close", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "volume": universe_service.UniverseColumnSpec("volume", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "return_20d": universe_service.UniverseColumnSpec("return_20d", "double precision", "number", universe_service._NUMBER_OPERATORS),
            },
        )
    }
    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)
    monkeypatch.setattr(
        "core.backtest_runtime._load_run_schedule",
        lambda *args, **kwargs: [
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        ],
    )
    monkeypatch.setattr(
        "core.backtest_runtime._load_exact_coverage",
        lambda *args, **kwargs: {datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc)},
    )

    with pytest.raises(ValueError) as exc:
        validate_backtest_submission(
            "postgresql://test:test@localhost:5432/asset_allocation",
            definition=_sample_definition(),
            start_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            end_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
            bar_size="5m",
        )

    assert "Intraday feature coverage gap" in str(exc.value)


def test_validate_backtest_submission_rejects_regime_coverage_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = ResolvedBacktestDefinition(
        **(_sample_definition().__dict__ | {"regime_model_name": "default-regime", "regime_model_version": 1})
    )
    specs = {
        "market_data": universe_service.UniverseTableSpec(
            name="market_data",
            as_of_column="as_of_ts",
            as_of_kind="intraday",
            columns={
                "open": universe_service.UniverseColumnSpec("open", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "high": universe_service.UniverseColumnSpec("high", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "low": universe_service.UniverseColumnSpec("low", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "close": universe_service.UniverseColumnSpec("close", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "volume": universe_service.UniverseColumnSpec("volume", "double precision", "number", universe_service._NUMBER_OPERATORS),
                "return_20d": universe_service.UniverseColumnSpec("return_20d", "double precision", "number", universe_service._NUMBER_OPERATORS),
            },
        )
    }
    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: specs)
    monkeypatch.setattr(
        "core.backtest_runtime._load_run_schedule",
        lambda *args, **kwargs: [
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        ],
    )
    monkeypatch.setattr(
        "core.backtest_runtime._load_exact_coverage",
        lambda *args, **kwargs: {
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        },
    )
    monkeypatch.setattr(
        "core.backtest_runtime._validate_regime_history_coverage",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("Regime history coverage gap")),
    )

    with pytest.raises(ValueError) as exc:
        validate_backtest_submission(
            "postgresql://test:test@localhost:5432/asset_allocation",
            definition=definition,
            start_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            end_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
            bar_size="5m",
        )

    assert "Regime history coverage gap" in str(exc.value)


def test_regime_context_blocks_and_scales_exposure() -> None:
    policy = StrategyConfig.model_validate(
        {
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "longOnly": True,
            "topN": 2,
            "lookbackWindow": 20,
            "holdingPeriod": 5,
            "costModel": "default",
            "rankingSchemaName": "quality",
            "intrabarConflictPolicy": "stop_first",
            "regimePolicy": {
                "enabled": True,
                "modelName": "default-regime",
                "targetGrossExposureByRegime": {
                    "trending_bull": 1.0,
                    "trending_bear": 0.5,
                    "choppy_mean_reversion": 0.75,
                    "high_vol": 0.0,
                    "unclassified": 0.0,
                },
                "blockOnTransition": True,
                "blockOnUnclassified": True,
                "honorHaltFlag": True,
                "onBlocked": "skip_entries",
            },
            "exits": [],
        }
    ).regimePolicy

    confirmed = _regime_context_for_session(
        policy,
        {
            "regime_code": "trending_bear",
            "regime_status": "confirmed",
            "halt_flag": False,
            "matched_rule_id": "trending_bear",
        },
    )
    assert confirmed["blocked"] is False
    assert confirmed["exposure_multiplier"] == 0.5

    transition = _regime_context_for_session(
        policy,
        {
            "regime_code": "trending_bear",
            "regime_status": "transition",
            "halt_flag": False,
        },
    )
    assert transition["blocked"] is True
    assert transition["blocked_reason"] == "transition"
    assert transition["blocked_action"] == "skip_entries"


def test_snapshot_symbol_index_reuses_preindexed_rows() -> None:
    snapshot = pd.DataFrame(
        {
            "symbol": ["MSFT", "AAPL", "AAPL"],
            "market_data__close": [10.0, 20.0, 21.0],
        }
    )

    index = _build_snapshot_symbol_index(snapshot)

    assert sorted(index) == ["AAPL", "MSFT"]
    assert _market_row(index, "AAPL")["market_data__close"] == 20.0
    assert _market_row(index, "NVDA") is None


def test_maybe_update_heartbeat_throttles_until_interval_elapses(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    moments = iter([0.0, 10.0, 75.0])

    class _FakeRepo:
        def update_heartbeat(self, run_id: str) -> None:
            calls.append(run_id)

    monkeypatch.setattr("core.backtest_runtime.monotonic_time.monotonic", lambda: next(moments))

    state = {"interval_seconds": 60.0, "last_heartbeat_at": None}

    assert _maybe_update_heartbeat(_FakeRepo(), run_id="run-123", state=state, phase="start", force=False) is True
    assert _maybe_update_heartbeat(_FakeRepo(), run_id="run-123", state=state, phase="loop", force=False) is False
    assert _maybe_update_heartbeat(_FakeRepo(), run_id="run-123", state=state, phase="loop", force=False) is True
    assert calls == ["run-123", "run-123"]


def test_apply_rebalance_target_preserves_existing_position_state() -> None:
    original = PositionState(
        symbol="AAPL",
        entry_date=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
        entry_price=100.0,
        quantity=10.0,
        bars_held=7,
        highest_since_entry=125.0,
        lowest_since_entry=94.0,
    )

    resized = _apply_rebalance_target(
        original,
        symbol="AAPL",
        entry_date=datetime(2026, 3, 3, 15, 0, tzinfo=timezone.utc),
        entry_price=110.0,
        target_quantity=15.0,
    )

    assert resized is not None
    assert resized.quantity == 15.0
    assert resized.entry_date == original.entry_date
    assert resized.entry_price == original.entry_price
    assert resized.bars_held == original.bars_held
    assert resized.highest_since_entry == original.highest_since_entry
    assert resized.lowest_since_entry == original.lowest_since_entry


def test_cadence_aware_metrics_scale_from_intraday_bar_size() -> None:
    periods_per_year = _periods_per_year_from_bar_size("5m")
    window_periods = _rolling_window_periods(periods_per_year=periods_per_year)
    timeseries = pd.DataFrame(
        {
            "date": [
                "2026-03-03T14:30:00Z",
                "2026-03-03T14:35:00Z",
            ],
            "portfolio_value": [100.0, 100.0003000002],
            "drawdown": [0.0, 0.0],
            "period_return": [0.000001, 0.000002],
            "daily_return": [0.000001, 0.000002],
            "cumulative_return": [0.000001, 0.000003000002],
            "cash": [0.0, 0.0],
            "gross_exposure": [1.0, 1.0],
            "net_exposure": [1.0, 1.0],
            "turnover": [0.0, 0.0],
            "commission": [0.0, 0.0],
            "slippage_cost": [0.0, 0.0],
            "trade_count": [0, 0],
        }
    )
    trades = pd.DataFrame(
        columns=[
            "execution_date",
            "symbol",
            "quantity",
            "price",
            "notional",
            "commission",
            "slippage_cost",
            "cash_after",
        ]
    )

    summary = _compute_summary(
        timeseries,
        trades,
        run_id="run-123",
        run_name="intraday-test",
        periods_per_year=periods_per_year,
    )
    rolling = _compute_rolling_metrics(
        timeseries,
        periods_per_year=periods_per_year,
        window_periods=2,
    )

    assert periods_per_year == pytest.approx(19656.0)
    assert window_periods == 4914
    assert summary["annualized_return"] == pytest.approx(((1.000003000002) ** (periods_per_year / 2.0)) - 1.0)
    assert summary["annualized_volatility"] == pytest.approx(
        pd.Series([0.000001, 0.000002]).std(ddof=0) * (periods_per_year ** 0.5)
    )
    assert rolling["window_days"].tolist() == [63, 63]
    assert rolling["window_periods"].tolist() == [2, 2]
    assert rolling["rolling_return"].iloc[-1] == pytest.approx((1.000001 * 1.000002) - 1.0)
