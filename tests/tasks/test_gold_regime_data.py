from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import pytest

from asset_allocation_contracts.strategy_publication import StrategyPublicationReconcileSignalResponse
from asset_allocation_runtime_common.market_data.market_symbols import REGIME_REQUIRED_MARKET_SYMBOLS
from tasks.regime_data import gold_regime_data as regime_job


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


def _market_row(symbol: str, as_of_date: str, **overrides: Any) -> dict[str, Any]:
    base = {
        "symbol": symbol,
        "date": as_of_date,
        "close": 100.0,
        "return_1d": 0.01,
        "return_20d": 0.03,
        "sma_200d": 95.0,
        "atr_14d": 2.0,
        "gap_atr": 0.2,
        "bb_width_20d": 0.1,
        "volume_pct_rank_252d": 0.6,
        "rsi_14d": 55.0,
    }
    base.update(overrides)
    return base


def _macro_row(as_of_date: str, **overrides: Any) -> dict[str, Any]:
    base = {
        "as_of_date": as_of_date,
        "rate_2y": 4.2,
        "rate_10y": 4.6,
        "curve_2s10s": 0.4,
        "hy_oas": 3.6,
        "hy_oas_z_20d": 0.2,
        "rates_event_flag": False,
        "computed_at": pd.Timestamp("2026-03-20T12:00:00Z"),
    }
    base.update(overrides)
    return base


def test_regime_job_uses_shared_required_market_symbol_contract() -> None:
    assert regime_job.REGIME_REQUIRED_MARKET_SYMBOLS == REGIME_REQUIRED_MARKET_SYMBOLS
    assert REGIME_REQUIRED_MARKET_SYMBOLS == ("SPY", "QQQ", "IWM", "ACWI", "^VIX", "^VIX3M")


def test_validate_required_market_series_reports_missing_symbols() -> None:
    frame = pd.DataFrame(
        [
            _market_row("SPY", "2026-03-03"),
            _market_row("QQQ", "2026-03-03"),
            _market_row("^VIX", "2026-03-03", return_1d=None, return_20d=None),
        ]
    )

    normalized = regime_job._normalize_market_series(frame)

    with pytest.raises(ValueError) as excinfo:
        regime_job._validate_required_market_series(normalized)

    message = str(excinfo.value)
    assert "missing required regime symbols" in message
    assert "IWM" in message
    assert "ACWI" in message


def test_record_regime_reconcile_signal_posts_durable_publication_fingerprint(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    messages: list[str] = []

    class _FakeStrategyPublicationRepository:
        def record_reconcile_signal(self, *, job_key: str, source_fingerprint: str, metadata: dict[str, Any]):
            calls.append(
                {
                    "job_key": job_key,
                    "source_fingerprint": source_fingerprint,
                    "metadata": dict(metadata),
                }
            )
            return StrategyPublicationReconcileSignalResponse(
                jobKey=job_key,
                sourceFingerprint=source_fingerprint,
                status="pending",
                created=True,
                createdAt="2026-04-23T21:00:00Z",
                updatedAt="2026-04-23T21:00:00Z",
            )

    monkeypatch.setattr(regime_job, "StrategyPublicationRepository", _FakeStrategyPublicationRepository)
    monkeypatch.setattr(regime_job.mdc, "write_line", lambda msg: messages.append(str(msg)))

    regime_job._record_regime_reconcile_signal(
        publish_state={
            "published_as_of_date": "2026-03-20",
            "input_as_of_date": "2026-03-20",
            "history_rows": 3,
            "latest_rows": 2,
            "transition_rows": 1,
            "active_models": [{"model_name": "default-regime", "model_version": 3}],
        },
        source_fingerprint="fp-123",
        domain_artifact_path="regime/_metadata/domain.json",
    )

    assert calls == [
        {
            "job_key": "regime",
            "source_fingerprint": "fp-123",
            "metadata": {
                "publishedAsOfDate": "2026-03-20",
                "inputAsOfDate": "2026-03-20",
                "historyRows": 3,
                "latestRows": 2,
                "transitionRows": 1,
                "activeModels": [{"model_name": "default-regime", "model_version": 3}],
                "domainArtifactPath": "regime/_metadata/domain.json",
                "producerJobName": "gold-regime-job",
            },
        }
    ]
    assert any("Gold regime reconcile signal recorded" in message for message in messages)


def test_build_inputs_daily_joins_market_and_macro_inputs_and_marks_complete_rows() -> None:
    market_series = regime_job._normalize_market_series(
        pd.DataFrame(
            [
                _market_row("SPY", "2026-03-20", close=110.0, sma_200d=100.0, atr_14d=2.5, gap_atr=0.2, bb_width_20d=0.1, volume_pct_rank_252d=0.55, rsi_14d=62.0),
                _market_row("QQQ", "2026-03-20", close=120.0, return_20d=0.05, sma_200d=105.0),
                _market_row("IWM", "2026-03-20", close=95.0, return_20d=0.01),
                _market_row("ACWI", "2026-03-20", close=88.0, return_20d=0.02),
                _market_row("^VIX", "2026-03-20", close=14.0, return_1d=None, return_20d=None, sma_200d=None, atr_14d=None, gap_atr=None, bb_width_20d=None, volume_pct_rank_252d=None, rsi_14d=None),
                _market_row("^VIX3M", "2026-03-20", close=15.1, return_1d=None, return_20d=None, sma_200d=None, atr_14d=None, gap_atr=None, bb_width_20d=None, volume_pct_rank_252d=None, rsi_14d=None),
                _market_row("SPY", "2026-03-21", close=111.0, sma_200d=100.5, atr_14d=2.7, gap_atr=0.25, bb_width_20d=0.11, volume_pct_rank_252d=0.57, rsi_14d=64.0),
                _market_row("QQQ", "2026-03-21", close=121.0, return_20d=0.04, sma_200d=106.0),
                _market_row("IWM", "2026-03-21", close=96.0, return_20d=0.01),
                _market_row("ACWI", "2026-03-21", close=89.0, return_20d=0.02),
                _market_row("^VIX", "2026-03-21", close=15.0, return_1d=None, return_20d=None, sma_200d=None, atr_14d=None, gap_atr=None, bb_width_20d=None, volume_pct_rank_252d=None, rsi_14d=None),
                _market_row("^VIX3M", "2026-03-21", close=15.8, return_1d=None, return_20d=None, sma_200d=None, atr_14d=None, gap_atr=None, bb_width_20d=None, volume_pct_rank_252d=None, rsi_14d=None),
            ]
        )
    )
    macro_inputs = regime_job._normalize_macro_inputs(
        pd.DataFrame(
            [
                _macro_row("2026-03-20", rates_event_flag=False),
                _macro_row("2026-03-21", rate_10y=None),
            ]
        )
    )

    inputs = regime_job._build_inputs_daily(
        market_series,
        macro_inputs,
        computed_at=datetime(2026, 3, 22, tzinfo=timezone.utc),
    )

    assert inputs["as_of_date"].tolist() == [date(2026, 3, 20), date(2026, 3, 21)]
    assert inputs.iloc[0]["qqq_close"] == 120.0
    assert inputs.iloc[0]["hy_oas_z_20d"] == pytest.approx(0.2)
    assert bool(inputs.iloc[0]["inputs_complete_flag"]) is True
    assert bool(inputs.iloc[1]["inputs_complete_flag"]) is False


def test_publish_window_skips_trailing_incomplete_rows() -> None:
    inputs = pd.DataFrame(
        [
            {
                **_macro_row("2026-03-20"),
                "spy_close": 110.0,
                "qqq_close": 120.0,
                "iwm_close": 95.0,
                "acwi_close": 88.0,
                "return_1d": 0.01,
                "return_20d": 0.04,
                "qqq_return_20d": 0.05,
                "iwm_return_20d": 0.01,
                "acwi_return_20d": 0.02,
                "spy_sma_200d": 100.0,
                "qqq_sma_200d": 105.0,
                "atr_14d": 2.5,
                "gap_atr": 0.2,
                "bb_width_20d": 0.1,
                "rsi_14d": 55.0,
                "volume_pct_rank_252d": 0.55,
                "vix_spot_close": 14.0,
                "vix3m_close": 15.0,
                "vix_slope": 1.0,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                **_macro_row("2026-03-21", rate_10y=None),
                "spy_close": 111.0,
                "qqq_close": 121.0,
                "iwm_close": 96.0,
                "acwi_close": 89.0,
                "return_1d": 0.01,
                "return_20d": 0.04,
                "qqq_return_20d": 0.05,
                "iwm_return_20d": 0.01,
                "acwi_return_20d": 0.02,
                "spy_sma_200d": 100.0,
                "qqq_sma_200d": 105.0,
                "atr_14d": 2.5,
                "gap_atr": 0.2,
                "bb_width_20d": 0.1,
                "rsi_14d": 55.0,
                "volume_pct_rank_252d": 0.55,
                "vix_spot_close": 14.0,
                "vix3m_close": 15.0,
                "vix_slope": 1.0,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": False,
            },
        ]
    )
    market_series = regime_job._normalize_market_series(
        pd.DataFrame([_market_row(symbol, "2026-03-20") for symbol in REGIME_REQUIRED_MARKET_SYMBOLS])
    )
    macro_inputs = regime_job._normalize_macro_inputs(pd.DataFrame([_macro_row("2026-03-20")]))

    window = regime_job._resolve_publish_window(inputs, market_series=market_series, macro_inputs=macro_inputs)
    published_inputs = regime_job._published_inputs(inputs, window=window)

    assert window.published_as_of_date == date(2026, 3, 20)
    assert window.input_as_of_date == date(2026, 3, 21)
    assert window.skipped_trailing_input_dates == (date(2026, 3, 21),)
    assert pd.to_datetime(published_inputs["as_of_date"]).dt.date.tolist() == [date(2026, 3, 20)]


def test_write_storage_parquet_outputs_writes_macro_and_regime_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    parquet_paths: list[str] = []

    class _FakeClient:
        def write_parquet(self, path: str, frame: pd.DataFrame) -> None:
            parquet_paths.append(path)
            assert isinstance(frame, pd.DataFrame)

    monkeypatch.setattr(regime_job.mdc, "get_storage_client", lambda _container: _FakeClient())

    regime_job._write_storage_parquet_outputs(
        gold_container="gold",
        macro_inputs=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")]}),
        inputs=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")]}),
        history=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "regime_code": ["trending_up"]}),
        latest=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "regime_code": ["trending_up"]}),
        transitions=pd.DataFrame({"effective_from_date": [pd.Timestamp("2026-03-20")], "regime_code": ["trending_up"]}),
    )

    assert parquet_paths == [
        "regime/macro_inputs.parquet",
        "regime/inputs.parquet",
        "regime/history.parquet",
        "regime/latest.parquet",
        "regime/transitions.parquet",
    ]


def test_replace_postgres_tables_uses_staged_apply_for_macro_and_regime_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[("off",), ("off",), (False,)],
        rowcount_map={
            "DELETE FROM gold.regime_macro_inputs_daily AS target": 1,
            "INSERT INTO gold.regime_macro_inputs_daily AS target": 1,
            "DELETE FROM gold.regime_inputs_daily AS target": 1,
            "INSERT INTO gold.regime_inputs_daily AS target": 1,
            "DELETE FROM gold.regime_history AS target": 8,
            "INSERT INTO gold.regime_history AS target": 8,
            "DELETE FROM gold.regime_latest AS target": 8,
            "INSERT INTO gold.regime_latest AS target": 8,
            "DELETE FROM gold.regime_transitions AS target": 2,
            "INSERT INTO gold.regime_transitions AS target": 2,
        },
    )
    messages: list[str] = []
    copied_tables: list[str] = []

    monkeypatch.setattr(regime_job, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(regime_job.mdc, "write_line", lambda msg: messages.append(str(msg)))

    def _fake_copy_rows(_cur: Any, *, table: str, columns, rows) -> None:
        copied_tables.append(str(table))
        assert rows is not None

    monkeypatch.setattr(regime_job, "copy_rows", _fake_copy_rows)

    regime_job._replace_postgres_tables(
        "postgresql://test",
        macro_inputs=pd.DataFrame([_macro_row("2026-03-20")]),
        inputs=pd.DataFrame(
            [
                {
                    "as_of_date": pd.Timestamp("2026-03-20"),
                    "spy_close": 110.0,
                    "qqq_close": 120.0,
                    "iwm_close": 95.0,
                    "acwi_close": 88.0,
                    "return_1d": 0.01,
                    "return_20d": 0.04,
                    "qqq_return_20d": 0.05,
                    "iwm_return_20d": 0.01,
                    "acwi_return_20d": 0.02,
                    "spy_sma_200d": 100.0,
                    "qqq_sma_200d": 105.0,
                    "atr_14d": 2.5,
                    "gap_atr": 0.2,
                    "bb_width_20d": 0.1,
                    "rsi_14d": 55.0,
                    "volume_pct_rank_252d": 0.55,
                    "vix_spot_close": 14.0,
                    "vix3m_close": 15.0,
                    "vix_slope": 1.0,
                    "hy_oas": 3.6,
                    "hy_oas_z_20d": 0.2,
                    "rate_2y": 4.2,
                    "rate_10y": 4.6,
                    "curve_2s10s": 0.4,
                    "rates_event_flag": False,
                    "vix_gt_32_streak": 0,
                    "inputs_complete_flag": True,
                    "computed_at": pd.Timestamp("2026-03-20T12:00:00Z"),
                }
            ]
        ),
        history=pd.DataFrame(
            [
                {
                    "as_of_date": pd.Timestamp("2026-03-20"),
                    "effective_from_date": pd.Timestamp("2026-03-23"),
                    "model_name": "default-regime",
                    "model_version": 3,
                    "regime_code": "trending_up",
                    "display_name": "Trending (Up)",
                    "signal_state": "active",
                    "score": 1.0,
                    "activation_threshold": 0.6,
                    "is_active": True,
                    "matched_rule_id": "trending_up",
                    "halt_flag": False,
                    "halt_reason": None,
                    "evidence_json": "{}",
                    "computed_at": pd.Timestamp("2026-03-20T12:00:00Z"),
                }
            ]
        ),
        latest=pd.DataFrame(
            [
                {
                    "model_name": "default-regime",
                    "model_version": 3,
                    "as_of_date": pd.Timestamp("2026-03-20"),
                    "effective_from_date": pd.Timestamp("2026-03-23"),
                    "regime_code": "trending_up",
                    "display_name": "Trending (Up)",
                    "signal_state": "active",
                    "score": 1.0,
                    "activation_threshold": 0.6,
                    "is_active": True,
                    "matched_rule_id": "trending_up",
                    "halt_flag": False,
                    "halt_reason": None,
                    "evidence_json": "{}",
                    "computed_at": pd.Timestamp("2026-03-20T12:00:00Z"),
                }
            ]
        ),
        transitions=pd.DataFrame(
            [
                {
                    "model_name": "default-regime",
                    "model_version": 3,
                    "effective_from_date": pd.Timestamp("2026-03-23"),
                    "regime_code": "trending_up",
                    "transition_type": "entered",
                    "prior_score": None,
                    "new_score": 1.0,
                    "activation_threshold": 0.6,
                    "trigger_rule_id": "trending_up",
                    "computed_at": pd.Timestamp("2026-03-20T12:00:00Z"),
                }
            ]
        ),
        active_models=[("default-regime", 3)],
    )

    assert copied_tables == [
        "pg_temp.regime_active_models_scope",
        "pg_temp.regime_stage_regime_macro_inputs_daily",
        "pg_temp.regime_stage_regime_inputs_daily",
        "pg_temp.regime_stage_regime_history",
        "pg_temp.regime_stage_regime_latest",
        "pg_temp.regime_stage_regime_transitions",
    ]
    assert sum("gold_regime_postgres_apply_stats" in message for message in messages) == 5
    assert any("DELETE FROM gold.regime_macro_inputs_daily AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_history AS target" in sql for sql, _params in cursor.executed)


def test_main_returns_retry_pending_when_latest_macro_join_is_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    logged_states: list[dict[str, Any]] = []

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")
    monkeypatch.setattr(
        regime_job,
        "_load_market_series",
        lambda _dsn: regime_job._normalize_market_series(
            pd.DataFrame([_market_row(symbol, "2026-03-20") for symbol in REGIME_REQUIRED_MARKET_SYMBOLS])
        ),
    )
    monkeypatch.setattr(
        regime_job,
        "_load_macro_inputs",
        lambda _dsn: regime_job._normalize_macro_inputs(pd.DataFrame([_macro_row("2026-03-20")])),
    )
    monkeypatch.setattr(
        regime_job,
        "_build_inputs_daily",
        lambda _market, _macro, computed_at: pd.DataFrame(
            [
                {
                    **_macro_row("2026-03-20"),
                    "spy_close": 110.0,
                    "qqq_close": 120.0,
                    "iwm_close": 95.0,
                    "acwi_close": 88.0,
                    "return_1d": 0.01,
                    "return_20d": 0.04,
                    "qqq_return_20d": 0.05,
                    "iwm_return_20d": 0.01,
                    "acwi_return_20d": 0.02,
                    "spy_sma_200d": 100.0,
                    "qqq_sma_200d": 105.0,
                    "atr_14d": 2.5,
                    "gap_atr": 0.2,
                    "bb_width_20d": 0.1,
                    "rsi_14d": 55.0,
                    "volume_pct_rank_252d": 0.55,
                    "vix_spot_close": 14.0,
                    "vix3m_close": 15.0,
                    "vix_slope": 1.0,
                    "vix_gt_32_streak": 0,
                    "inputs_complete_flag": True,
                },
                {
                    **_macro_row("2026-03-21", rate_10y=None),
                    "spy_close": 111.0,
                    "qqq_close": 121.0,
                    "iwm_close": 96.0,
                    "acwi_close": 89.0,
                    "return_1d": 0.01,
                    "return_20d": 0.04,
                    "qqq_return_20d": 0.05,
                    "iwm_return_20d": 0.01,
                    "acwi_return_20d": 0.02,
                    "spy_sma_200d": 100.0,
                    "qqq_sma_200d": 105.0,
                    "atr_14d": 2.5,
                    "gap_atr": 0.2,
                    "bb_width_20d": 0.1,
                    "rsi_14d": 55.0,
                    "volume_pct_rank_252d": 0.55,
                    "vix_spot_close": 14.0,
                    "vix3m_close": 15.0,
                    "vix_slope": 1.0,
                    "vix_gt_32_streak": 0,
                    "inputs_complete_flag": False,
                },
            ]
        ),
    )
    monkeypatch.setattr(
        regime_job.RegimeRepository,
        "list_active_regime_model_revisions",
        lambda self: [{"name": "default-regime", "version": 3, "config": {}}],
    )
    monkeypatch.setattr(regime_job, "log_regime_publication_status", lambda state, failed_finalization=0: logged_states.append(dict(state)))
    monkeypatch.setattr(regime_job.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(regime_job.mdc, "write_warning", lambda _msg: None)
    monkeypatch.setattr(regime_job.mdc, "write_line", lambda _msg: None)

    result = regime_job.main()

    assert result == 2
    assert logged_states[0]["status"] == "retry_pending"
    assert logged_states[0]["published_as_of_date"] == "2026-03-20"
