from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic import ValidationError

from core.backtest_runtime import (
    ResolvedBacktestDefinition,
    _apply_rebalance_target,
    _apply_trade_to_position,
    _build_intraday_frames_by_timestamp,
    _build_snapshot_symbol_index,
    _prepare_slow_snapshot_frames,
    _snapshot_for_timestamp,
    execute_backtest_run,
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
from asset_allocation_runtime_common import BACKTEST_RESULTS_SCHEMA_VERSION
from asset_allocation_runtime_common.ranking_engine.contracts import RankingSchemaConfig
from asset_allocation_runtime_common.strategy_engine.position_state import PositionState
from asset_allocation_runtime_common.strategy_engine.contracts import StrategyConfig
from asset_allocation_runtime_common.strategy_engine import universe as universe_service


def _sample_universe() -> SimpleNamespace:
    return SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gt",
                    "value": 1,
                }
            ],
        },
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


def test_regime_context_surfaces_primary_regime_and_signals_observationally() -> None:
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
                "modelName": "default-regime",
                "mode": "observe_only",
            },
            "exits": [],
        }
    ).regimePolicy

    confirmed = _regime_context_for_session(
        policy,
        {
            "active_regimes": ["trending_down", "high_volatility"],
            "signals": [
                {"regime_code": "trending_down", "signal_state": "active", "score": 1.0},
                {"regime_code": "high_volatility", "signal_state": "active", "score": 0.67},
            ],
            "halt_flag": False,
        },
    )
    assert confirmed["regime_code"] == "trending_down"
    assert confirmed["regime_status"] == "confirmed"
    assert confirmed["primary_regime_code"] == "trending_down"
    assert confirmed["active_regimes"] == ["trending_down", "high_volatility"]
    assert confirmed["signals"][0]["regime_code"] == "trending_down"
    assert confirmed["halt_flag"] is False


def test_strategy_config_rejects_legacy_default_regime_policy() -> None:
    with pytest.raises(ValidationError, match="observe_only"):
        StrategyConfig.model_validate(
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
                    "modelName": "default-regime",
                    "targetGrossExposureByRegime": {
                        "trending_up": 1.0,
                        "trending_down": 0.5,
                        "mean_reverting": 0.75,
                        "low_volatility": 1.0,
                        "high_volatility": 0.0,
                        "liquidity_stress": 0.0,
                        "macro_alignment": 1.0,
                        "unclassified": 0.0,
                    },
                    "blockOnTransition": True,
                    "blockOnUnclassified": True,
                    "honorHaltFlag": True,
                },
                "exits": [],
            }
        )


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


def test_snapshot_for_timestamp_matches_preindexed_session_cache() -> None:
    ts = datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc)
    intraday_frames = {
        "market_data": pd.DataFrame(
            {
                "as_of": [ts, ts, datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc)],
                "symbol": ["AAPL", "MSFT", "AAPL"],
                "market_data__close": [100.0, 200.0, 101.0],
            }
        ),
        "signals": pd.DataFrame(
            {
                "as_of": [ts],
                "symbol": ["AAPL"],
                "signals__momentum": [1.5],
            }
        ),
    }
    slow_frames = {
        "fundamentals": pd.DataFrame(
            {
                "as_of": [datetime(2026, 3, 2, tzinfo=timezone.utc), datetime(2026, 3, 2, tzinfo=timezone.utc)],
                "symbol": ["AAPL", "MSFT"],
                "fundamentals__pe": [20.0, 25.0],
            }
        )
    }

    baseline = _snapshot_for_timestamp(ts, intraday_frames=intraday_frames, slow_frames=slow_frames)
    optimized = _snapshot_for_timestamp(
        ts,
        intraday_frames_by_ts=_build_intraday_frames_by_timestamp(intraday_frames),
        prepared_slow_frames=_prepare_slow_snapshot_frames(slow_frames),
    )

    baseline = baseline.sort_values("symbol").reset_index(drop=True)
    optimized = optimized.sort_values("symbol").reset_index(drop=True)
    pd.testing.assert_frame_equal(optimized, baseline)


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
        pd.DataFrame(columns=["realized_pnl", "realized_return"]),
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


def test_compute_summary_reports_cost_drag_and_closed_position_stats() -> None:
    timeseries = pd.DataFrame(
        {
            "date": [
                "2026-03-03T14:30:00Z",
                "2026-03-03T14:35:00Z",
                "2026-03-03T14:40:00Z",
            ],
            "portfolio_value": [100000.0, 102000.0, 101000.0],
            "gross_portfolio_value": [100000.0, 102500.0, 101500.0],
            "drawdown": [0.0, 0.0, -0.009803921568627416],
            "period_return": [0.0, 0.02, -0.009803921568627416],
            "daily_return": [0.0, 0.02, -0.009803921568627416],
            "cumulative_return": [0.0, 0.02, 0.01],
            "cash": [100000.0, 2000.0, 101000.0],
            "gross_exposure": [0.0, 0.98, 0.0],
            "net_exposure": [0.0, 0.94, 0.0],
            "turnover": [0.0, 0.98, 0.99],
            "commission": [0.0, 20.0, 5.0],
            "slippage_cost": [0.0, 8.0, 2.0],
            "trade_count": [0, 1, 1],
        }
    )
    trades = pd.DataFrame(
        {
            "commission": [20.0, 5.0],
            "slippage_cost": [8.0, 2.0],
        }
    )
    closed_positions = pd.DataFrame(
        {
            "realized_pnl": [200.0, -100.0],
            "realized_return": [0.05, -0.02],
        }
    )

    summary = _compute_summary(
        timeseries,
        trades,
        closed_positions,
        run_id="run-123",
        run_name="cost-drag-test",
        periods_per_year=252.0,
        initial_cash_override=100000.0,
    )

    assert summary["total_return"] == pytest.approx(0.01)
    assert summary["gross_total_return"] == pytest.approx(0.015)
    assert summary["total_commission"] == pytest.approx(25.0)
    assert summary["total_slippage_cost"] == pytest.approx(10.0)
    assert summary["total_transaction_cost"] == pytest.approx(35.0)
    assert summary["cost_drag_bps"] == pytest.approx(50.0)
    assert summary["avg_gross_exposure"] == pytest.approx((0.0 + 0.98 + 0.0) / 3.0)
    assert summary["avg_net_exposure"] == pytest.approx((0.0 + 0.94 + 0.0) / 3.0)
    assert summary["closed_positions"] == 2
    assert summary["winning_positions"] == 1
    assert summary["losing_positions"] == 1
    assert summary["hit_rate"] == pytest.approx(0.5)
    assert summary["avg_win_pnl"] == pytest.approx(200.0)
    assert summary["avg_loss_pnl"] == pytest.approx(-100.0)
    assert summary["payoff_ratio"] == pytest.approx(2.0)
    assert summary["profit_factor"] == pytest.approx(2.0)
    assert summary["expectancy_pnl"] == pytest.approx(50.0)
    assert summary["expectancy_return"] == pytest.approx(0.015)
    assert summary["sortino_ratio"] > 0.0
    assert summary["calmar_ratio"] > 0.0


def test_compute_summary_handles_empty_and_single_row_inputs() -> None:
    empty_summary = _compute_summary(
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        run_id="run-empty",
        run_name=None,
        periods_per_year=252.0,
    )
    single_row_timeseries = pd.DataFrame(
        {
            "date": ["2026-03-03T14:30:00Z"],
            "portfolio_value": [100000.0],
            "gross_portfolio_value": [100100.0],
            "drawdown": [0.0],
            "period_return": [0.0],
            "daily_return": [0.0],
            "cumulative_return": [0.0],
            "cash": [40000.0],
            "gross_exposure": [1.2],
            "net_exposure": [0.6],
            "turnover": [0.0],
            "commission": [0.0],
            "slippage_cost": [0.0],
            "trade_count": [0],
        }
    )
    single_row_summary = _compute_summary(
        single_row_timeseries,
        pd.DataFrame(columns=["commission", "slippage_cost"]),
        pd.DataFrame(columns=["realized_pnl", "realized_return"]),
        run_id="run-single",
        run_name="single-row",
        periods_per_year=252.0,
        initial_cash_override=100000.0,
    )
    rolling = _compute_rolling_metrics(single_row_timeseries, periods_per_year=252.0, window_periods=1)

    assert empty_summary["final_equity"] == 0.0
    assert empty_summary["gross_total_return"] == 0.0
    assert single_row_summary["annualized_volatility"] == 0.0
    assert single_row_summary["sharpe_ratio"] == 0.0
    assert single_row_summary["gross_total_return"] == pytest.approx(0.001)
    assert single_row_summary["avg_net_exposure"] == pytest.approx(0.6)
    assert rolling["gross_exposure_avg"].iloc[0] == pytest.approx(1.2)
    assert rolling["net_exposure_avg"].iloc[0] == pytest.approx(0.6)


def test_apply_trade_to_position_tracks_partial_reductions_until_flat() -> None:
    opened, closed_position = _apply_trade_to_position(
        None,
        symbol="AAPL",
        ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
        quantity_delta=10.0,
        trade_price=100.0,
        commission=1.0,
        slippage=0.5,
        position_id="pos-1",
    )
    assert opened is not None
    assert closed_position is None
    assert opened.position_id == "pos-1"
    assert opened.average_cost == pytest.approx(100.0)

    resized, closed_position = _apply_trade_to_position(
        opened,
        symbol="AAPL",
        ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        quantity_delta=5.0,
        trade_price=110.0,
        commission=1.0,
        slippage=0.0,
    )
    assert resized is not None
    assert closed_position is None
    assert resized.quantity == pytest.approx(15.0)
    assert resized.average_cost == pytest.approx((10.0 * 100.0 + 5.0 * 110.0) / 15.0)
    assert resized.resize_count == 1

    reduced, closed_position = _apply_trade_to_position(
        resized,
        symbol="AAPL",
        ts=datetime(2026, 3, 3, 14, 40, tzinfo=timezone.utc),
        quantity_delta=-6.0,
        trade_price=120.0,
        commission=0.5,
        slippage=0.0,
    )
    assert reduced is not None
    assert closed_position is None
    assert reduced.quantity == pytest.approx(9.0)
    assert reduced.resize_count == 2

    flattened, closed_position = _apply_trade_to_position(
        reduced,
        symbol="AAPL",
        ts=datetime(2026, 3, 3, 14, 45, tzinfo=timezone.utc),
        quantity_delta=-9.0,
        trade_price=115.0,
        commission=0.5,
        slippage=0.0,
        exit_reason="rebalance_exit",
    )
    assert flattened is None
    assert closed_position is not None
    assert closed_position["position_id"] == "pos-1"
    assert closed_position["resize_count"] == 2
    assert closed_position["exit_reason"] == "rebalance_exit"
    assert closed_position["realized_pnl"] > 0.0
    assert closed_position["realized_return"] > 0.0


def test_execute_backtest_run_publishes_full_results_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    schedule = [
        datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
        datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        datetime(2026, 3, 3, 14, 40, tzinfo=timezone.utc),
    ]
    run_state = {
        "run_id": "run-123",
        "status": "queued",
        "start_ts": schedule[0],
        "end_ts": schedule[-1],
        "bar_size": "5m",
        "strategy_name": "mom-spy-res",
        "strategy_version": 3,
        "run_name": "Publish path",
        "regime_model_name": None,
        "regime_model_version": None,
    }
    captured: dict[str, object] = {}
    heartbeats: list[str] = []

    class _FakeRepo:
        def __init__(self, _dsn: str) -> None:
            self._state = run_state

        def get_run(self, run_id: str):  # type: ignore[no-untyped-def]
            if run_id != self._state["run_id"]:
                return None
            return dict(self._state)

        def start_run(self, run_id: str, execution_name: str | None = None) -> None:
            assert run_id == self._state["run_id"]
            self._state["status"] = "running"
            self._state["execution_name"] = execution_name

        def update_heartbeat(self, run_id: str) -> None:
            heartbeats.append(run_id)

        def complete_run(self, run_id: str, summary: dict[str, object]) -> None:
            assert run_id == self._state["run_id"]
            captured["completed_summary"] = dict(summary)
            self._state["status"] = "completed"

    def _definition() -> ResolvedBacktestDefinition:
        base = _sample_definition()
        return ResolvedBacktestDefinition(
            **(
                base.__dict__
                | {
                    "strategy_config_raw": {
                        **base.strategy_config_raw,
                        "initialCash": 100000.0,
                        "costs": {"commissionBps": 10.0, "slippageBps": 5.0},
                    }
                }
            )
        )

    def _snapshot_for_ts(current_ts: datetime, **_: object) -> pd.DataFrame:
        prices = {
            schedule[0]: {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
            schedule[1]: {"open": 100.0, "high": 101.0, "low": 99.5, "close": 101.0},
            schedule[2]: {"open": 102.0, "high": 102.5, "low": 101.5, "close": 102.0},
        }[current_ts]
        return pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "market_data__open": prices["open"],
                    "market_data__high": prices["high"],
                    "market_data__low": prices["low"],
                    "market_data__close": prices["close"],
                }
            ]
        )

    def _ranking_for_ts(
        _snapshot: pd.DataFrame,
        *,
        definition: ResolvedBacktestDefinition,
        rebalance_ts: datetime,
        target_weight_multiplier: float = 1.0,
    ) -> pd.DataFrame:
        assert definition.strategy_name == "mom-spy-res"
        assert target_weight_multiplier == pytest.approx(1.0)
        if rebalance_ts == schedule[0]:
            rows = [
                {
                    "rebalance_ts": rebalance_ts.isoformat(),
                    "ordinal": 1,
                    "symbol": "AAPL",
                    "score": 1.0,
                    "selected": True,
                    "target_weight": 1.0,
                }
            ]
        else:
            rows = [
                {
                    "rebalance_ts": rebalance_ts.isoformat(),
                    "ordinal": 1,
                    "symbol": "AAPL",
                    "score": 0.2,
                    "selected": False,
                    "target_weight": 0.0,
                }
            ]
        return pd.DataFrame(rows)

    def _capture_persist(
        dsn: str,
        *,
        run_id: str,
        summary: dict[str, object],
        timeseries_rows,
        rolling_metric_rows,
        trade_rows,
        closed_position_rows,
        selection_trace_rows,
        regime_trace_rows,
        results_schema_version: int,
    ) -> None:
        captured["dsn"] = dsn
        captured["run_id"] = run_id
        captured["summary"] = dict(summary)
        captured["timeseries_rows"] = list(timeseries_rows)
        captured["rolling_metric_rows"] = list(rolling_metric_rows)
        captured["trade_rows"] = list(trade_rows)
        captured["closed_position_rows"] = list(closed_position_rows)
        captured["selection_trace_rows"] = list(selection_trace_rows)
        captured["regime_trace_rows"] = list(regime_trace_rows)
        captured["results_schema_version"] = results_schema_version

    monkeypatch.setattr("core.backtest_runtime.BacktestRepository", _FakeRepo)
    monkeypatch.setattr("core.backtest_runtime.resolve_backtest_definition", lambda *args, **kwargs: _definition())
    monkeypatch.setattr("core.backtest_runtime.validate_backtest_submission", lambda *args, **kwargs: schedule)
    monkeypatch.setattr("core.backtest_runtime._load_regime_schedule_map", lambda *args, **kwargs: {})
    monkeypatch.setattr("core.backtest_runtime._required_columns", lambda definition: {})
    monkeypatch.setattr("core.backtest_runtime._load_intraday_session_frames", lambda *args, **kwargs: {})
    monkeypatch.setattr("core.backtest_runtime._load_slow_frames", lambda *args, **kwargs: {})
    monkeypatch.setattr("core.backtest_runtime._snapshot_for_timestamp", _snapshot_for_ts)
    monkeypatch.setattr("core.backtest_runtime._score_snapshot", _ranking_for_ts)
    monkeypatch.setattr(universe_service, "_load_gold_table_specs", lambda _dsn: {})
    monkeypatch.setattr("core.backtest_runtime.persist_backtest_results", _capture_persist)

    result = execute_backtest_run("postgresql://test", run_id="run-123")

    summary = result["summary"]
    assert summary["gross_total_return"] == pytest.approx(0.02)
    assert summary["total_transaction_cost"] == pytest.approx(303.0)
    assert summary["cost_drag_bps"] == pytest.approx(30.3)
    assert summary["closed_positions"] == 1
    assert captured["summary"] == captured["completed_summary"]
    assert captured["results_schema_version"] == BACKTEST_RESULTS_SCHEMA_VERSION
    assert captured["results_schema_version"] == BACKTEST_RESULTS_SCHEMA_VERSION
    assert len(captured["timeseries_rows"]) == 2
    assert len(captured["rolling_metric_rows"]) == 2
    assert len(captured["trade_rows"]) == 2
    assert len(captured["closed_position_rows"]) == 1
    assert len(captured["selection_trace_rows"]) == 2
    assert len(captured["regime_trace_rows"]) == 3
    first_trade, second_trade = captured["trade_rows"]
    assert first_trade["trade_role"] == "entry"
    assert second_trade["trade_role"] == "exit"
    assert first_trade["position_id"] == second_trade["position_id"]
    assert captured["closed_position_rows"][0]["exit_reason"] == "rebalance_exit"
    assert heartbeats
