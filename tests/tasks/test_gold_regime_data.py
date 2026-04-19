from __future__ import annotations

from typing import Any
from datetime import date, datetime, timezone

import pandas as pd
import pytest

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


def test_regime_job_uses_shared_required_market_symbol_contract() -> None:
    assert regime_job.REGIME_REQUIRED_MARKET_SYMBOLS == REGIME_REQUIRED_MARKET_SYMBOLS


def test_validate_required_market_series_reports_missing_symbols() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["SPY", "SPY", "^VIX"],
            "date": ["2026-03-03", "2026-03-04", "2026-03-04"],
            "close": [580.0, 582.0, 21.5],
            "return_1d": [0.01, 0.003, None],
            "return_20d": [0.04, 0.05, None],
        }
    )

    normalized = regime_job._normalize_market_series(frame)

    with pytest.raises(ValueError) as excinfo:
        regime_job._validate_required_market_series(normalized)

    message = str(excinfo.value)
    assert "missing required regime symbols" in message
    assert REGIME_REQUIRED_MARKET_SYMBOLS[-1] in message
    assert "coverage=" in message


def test_validate_required_market_series_logs_fast_fail_context(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = regime_job._normalize_market_series(
        pd.DataFrame(
            {
                "symbol": ["SPY", "^VIX"],
                "date": ["2026-03-03", "2026-03-03"],
                "close": [580.0, 21.5],
                "return_1d": [0.01, None],
                "return_20d": [0.04, None],
            }
        )
    )
    errors: list[str] = []

    monkeypatch.setattr(regime_job, "_summarize_market_sync_state", lambda _dsn: "market_sync_state=empty")
    monkeypatch.setattr(regime_job.mdc, "write_error", lambda msg: errors.append(str(msg)))

    with pytest.raises(ValueError) as excinfo:
        regime_job._validate_required_market_series(frame, dsn="postgresql://example")

    message = str(excinfo.value)
    assert "Gold regime fast-fail" in message
    assert "market_sync_state=empty" in message
    assert "gold-market-job" in message
    assert errors == [message]


def test_assert_complete_regime_inputs_reports_non_overlapping_series() -> None:
    market_series = regime_job._validate_required_market_series(
        regime_job._normalize_market_series(
            pd.DataFrame(
                {
                    "symbol": ["SPY", "SPY", "^VIX", "^VIX3M"],
                    "date": ["2026-03-03", "2026-03-04", "2026-03-05", "2026-03-05"],
                    "close": [580.0, 582.0, 21.5, 22.0],
                    "return_1d": [0.01, 0.003, None, None],
                    "return_20d": [0.04, 0.05, None, None],
                }
            )
        )
    )

    inputs = regime_job._build_inputs_daily(
        market_series,
        computed_at=datetime(2026, 3, 9, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError) as excinfo:
        regime_job._assert_complete_regime_inputs(inputs, market_series=market_series)

    message = str(excinfo.value)
    assert "no complete SPY/^VIX/^VIX3M rows" in message
    assert "inputs_range=" in message
    assert "coverage=" in message


def test_write_storage_parquet_outputs_writes_all_regime_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    parquet_paths: list[str] = []

    class _FakeClient:
        def write_parquet(self, path: str, frame: pd.DataFrame) -> None:
            parquet_paths.append(path)
            assert isinstance(frame, pd.DataFrame)

    monkeypatch.setattr(regime_job.mdc, "get_storage_client", lambda _container: _FakeClient())

    inputs = pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "symbol": ["SPY"]})
    history = pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "regime_code": ["risk_on"]})
    latest = history.copy()
    transitions = pd.DataFrame({"effective_from_date": [pd.Timestamp("2026-03-20")]})

    regime_job._write_storage_parquet_outputs(
        gold_container="gold",
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
    )

    assert parquet_paths == [
        "regime/inputs.parquet",
        "regime/history.parquet",
        "regime/latest.parquet",
        "regime/transitions.parquet",
    ]


def test_build_revision_inputs_uses_model_halt_threshold_for_streak_and_overlay() -> None:
    inputs = pd.DataFrame(
        [
            {
                "as_of_date": "2026-03-18",
                "return_1d": -0.01,
                "return_20d": -0.05,
                "rvol_10d_ann": 31.0,
                "vix_spot_close": 27.0,
                "vix3m_close": 26.0,
                "vix_slope": -1.0,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-03-19",
                "return_1d": -0.01,
                "return_20d": -0.05,
                "rvol_10d_ann": 31.0,
                "vix_spot_close": 29.0,
                "vix3m_close": 28.0,
                "vix_slope": -1.0,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-03-20",
                "return_1d": -0.01,
                "return_20d": -0.05,
                "rvol_10d_ann": 31.0,
                "vix_spot_close": 29.0,
                "vix3m_close": 28.0,
                "vix_slope": -1.0,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
        ]
    )

    revision_inputs, resolved_config = regime_job._build_revision_inputs(
        inputs,
        config={"haltVixThreshold": 28.0, "haltVixStreakDays": 2},
    )
    history, latest, _transitions = regime_job.build_regime_outputs(
        revision_inputs,
        model_name="default-regime",
        model_version=7,
        config=resolved_config,
        computed_at=datetime(2026, 3, 21, tzinfo=timezone.utc),
    )

    assert revision_inputs["vix_gt_32_streak"].tolist() == [0, 1, 2]
    assert history["vix_gt_32_streak"].tolist() == [0, 1, 2]
    assert bool(latest.iloc[0]["halt_flag"]) is True
    assert latest.iloc[0]["halt_reason"] is not None


def test_publish_window_skips_trailing_incomplete_latest_inputs() -> None:
    inputs = pd.DataFrame(
        [
            {
                "as_of_date": "2026-03-18",
                "return_1d": 0.01,
                "return_20d": 0.04,
                "rvol_10d_ann": 14.0,
                "vix_spot_close": 18.0,
                "vix3m_close": 18.8,
                "vix_slope": 0.8,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-03-19",
                "return_1d": -0.02,
                "return_20d": -0.05,
                "rvol_10d_ann": 20.0,
                "vix_spot_close": 24.0,
                "vix3m_close": 23.1,
                "vix_slope": -0.9,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-03-20",
                "return_1d": pd.NA,
                "return_20d": pd.NA,
                "rvol_10d_ann": pd.NA,
                "vix_spot_close": 25.0,
                "vix3m_close": pd.NA,
                "vix_slope": pd.NA,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": False,
            },
        ]
    )
    market_series = pd.DataFrame(columns=["symbol", "date", "close", "return_1d", "return_20d"])

    window = regime_job._resolve_publish_window(inputs, market_series=market_series)
    metadata = regime_job._publish_window_metadata(window)
    warnings = regime_job._publish_window_warnings(window)
    published_inputs = regime_job._published_inputs(inputs, window=window)
    full_history, full_latest, full_transitions = regime_job.build_regime_outputs(
        inputs,
        model_name="default-regime",
        model_version=1,
        computed_at=datetime(2026, 3, 21, tzinfo=timezone.utc),
    )
    published_history, published_latest, published_transitions = regime_job.build_regime_outputs(
        published_inputs,
        model_name="default-regime",
        model_version=1,
        computed_at=datetime(2026, 3, 21, tzinfo=timezone.utc),
    )

    assert window.published_as_of_date == date(2026, 3, 19)
    assert window.input_as_of_date == date(2026, 3, 20)
    assert window.skipped_trailing_input_dates == (date(2026, 3, 20),)
    assert metadata["skipped_trailing_input_dates"] == ["2026-03-20"]
    assert warnings == [
        "Trailing incomplete regime input dates skipped from published regime surfaces: 2026-03-20. "
        "Published regime state remains capped at 2026-03-19."
    ]
    assert pd.to_datetime(inputs["as_of_date"]).dt.date.tolist() == [
        date(2026, 3, 18),
        date(2026, 3, 19),
        date(2026, 3, 20),
    ]
    assert pd.to_datetime(published_inputs["as_of_date"]).dt.date.tolist() == [
        date(2026, 3, 18),
        date(2026, 3, 19),
    ]
    assert full_latest.iloc[0]["as_of_date"].isoformat() == "2026-03-20"
    assert full_latest.iloc[0]["regime_status"] == "unclassified"
    assert published_history["as_of_date"].max().isoformat() == "2026-03-19"
    assert published_latest.iloc[0]["as_of_date"].isoformat() == "2026-03-19"
    assert len(published_transitions) <= len(full_transitions)


def test_replace_postgres_tables_uses_staged_apply_for_all_regime_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[("off",), (False,)],
        rowcount_map={
            "DELETE FROM gold.regime_inputs_daily AS target": 1,
            "INSERT INTO gold.regime_inputs_daily AS target": 1,
            "DELETE FROM gold.regime_history AS target": 2,
            "INSERT INTO gold.regime_history AS target": 2,
            "DELETE FROM gold.regime_latest AS target": 1,
            "INSERT INTO gold.regime_latest AS target": 1,
            "DELETE FROM gold.regime_transitions AS target": 1,
            "INSERT INTO gold.regime_transitions AS target": 1,
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

    inputs = pd.DataFrame(
        {
            "as_of_date": [pd.Timestamp("2026-03-20")],
            "spy_close": [580.0],
            "return_1d": [0.01],
            "return_20d": [0.05],
            "rvol_10d_ann": [18.0],
            "vix_spot_close": [21.5],
            "vix3m_close": [22.1],
            "vix_slope": [0.6],
            "trend_state": ["up"],
            "curve_state": ["contango"],
            "vix_gt_32_streak": [0],
            "inputs_complete_flag": [True],
            "computed_at": [pd.Timestamp("2026-03-20T12:00:00Z")],
        }
    )
    history = pd.DataFrame(
        {
            "as_of_date": [pd.Timestamp("2026-03-20"), pd.Timestamp("2026-03-21")],
            "effective_from_date": [pd.Timestamp("2026-03-20"), pd.Timestamp("2026-03-20")],
            "model_name": ["default-regime", "default-regime"],
            "model_version": [1, 1],
            "regime_code": ["risk_on", "risk_on"],
            "regime_status": ["active", "active"],
            "matched_rule_id": ["rule-1", "rule-1"],
            "halt_flag": [False, False],
            "halt_reason": [pd.NA, pd.NA],
            "spy_return_20d": [0.05, 0.06],
            "rvol_10d_ann": [18.0, 17.5],
            "vix_spot_close": [21.5, 20.8],
            "vix3m_close": [22.1, 21.6],
            "vix_slope": [0.6, 0.8],
            "trend_state": ["up", "up"],
            "curve_state": ["contango", "contango"],
            "vix_gt_32_streak": [0, 0],
            "computed_at": [pd.Timestamp("2026-03-20T12:00:00Z"), pd.Timestamp("2026-03-21T12:00:00Z")],
        }
    )
    latest = history.tail(1).copy()
    transitions = pd.DataFrame(
        {
            "model_name": ["default-regime"],
            "model_version": [1],
            "effective_from_date": [pd.Timestamp("2026-03-20")],
            "prior_regime_code": [pd.NA],
            "new_regime_code": ["risk_on"],
            "trigger_rule_id": ["rule-1"],
            "computed_at": [pd.Timestamp("2026-03-20T12:00:00Z")],
        }
    )

    regime_job._replace_postgres_tables(
        "postgresql://test",
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_models=[("default-regime", 1)],
    )

    assert copied_tables == [
        "pg_temp.regime_active_models_scope",
        "pg_temp.regime_stage_regime_inputs_daily",
        "pg_temp.regime_stage_regime_history",
        "pg_temp.regime_stage_regime_latest",
        "pg_temp.regime_stage_regime_transitions",
    ]
    assert all("TRUNCATE TABLE gold.regime_inputs_daily" not in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_inputs_daily AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_history AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_latest AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_transitions AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_inputs_daily AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_history AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_latest AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_transitions AS target" in sql for sql, _params in cursor.executed)
    assert any("IS DISTINCT FROM" in sql for sql, _params in cursor.executed)
    history_delete_sql = next(
        sql for sql, _params in cursor.executed if "DELETE FROM gold.regime_history AS target" in sql
    )
    assert "pg_temp.regime_active_models_scope AS scope" in history_delete_sql
    assert "pg_temp.regime_stage_regime_history AS stage" in history_delete_sql
    assert all("pg_advisory_lock" not in sql and "LOCK TABLE" not in sql for sql, _params in cursor.executed)
    assert sum("gold_regime_postgres_apply_stats" in message for message in messages) == 4


def test_replace_postgres_tables_deletes_scoped_model_rows_even_when_stage_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[("off",), (False,)],
        rowcount_map={
            "DELETE FROM gold.regime_inputs_daily AS target": 3,
            "DELETE FROM gold.regime_history AS target": 2,
            "DELETE FROM gold.regime_latest AS target": 1,
            "DELETE FROM gold.regime_transitions AS target": 1,
        },
    )
    messages: list[str] = []
    copied_tables: list[str] = []

    monkeypatch.setattr(regime_job, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(regime_job.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(
        regime_job,
        "copy_rows",
        lambda _cur, *, table, columns, rows: copied_tables.append(str(table)),
    )

    regime_job._replace_postgres_tables(
        "postgresql://test",
        inputs=pd.DataFrame(columns=regime_job._INPUTS_COLUMNS),
        history=pd.DataFrame(columns=regime_job._HISTORY_COLUMNS),
        latest=pd.DataFrame(columns=regime_job._HISTORY_COLUMNS),
        transitions=pd.DataFrame(columns=regime_job._TRANSITIONS_COLUMNS),
        active_models=[("default-regime", 1)],
    )

    assert copied_tables == ["pg_temp.regime_active_models_scope"]
    assert any("DELETE FROM gold.regime_inputs_daily AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_history AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_latest AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_transitions AS target" in sql for sql, _params in cursor.executed)
    assert any(
        "gold_regime_postgres_apply_stats table=gold.regime_history staged_rows=0 "
        "deleted_rows=2 upserted_rows=0 unchanged_rows=0 scope=active_models scope_models=1"
        in message
        for message in messages
    )


def test_main_fails_closed_on_stale_eod_input_without_publishing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")
    logged_status: list[tuple[dict[str, object], int]] = []
    call_counts = {"replace": 0, "storage": 0, "finalize": 0, "trigger": 0}

    monkeypatch.setattr(regime_job.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(regime_job, "_load_market_series", lambda _dsn: pd.DataFrame())
    monkeypatch.setattr(
        regime_job.RegimeRepository,
        "list_active_regime_model_revisions",
        lambda self: [{"name": "default-regime", "version": 2, "config": {}}],
    )
    monkeypatch.setattr(
        regime_job,
        "_build_inputs_daily",
        lambda market_series, *, computed_at: pd.DataFrame(
            {
                "as_of_date": ["2026-03-19", "2026-03-20"],
                "return_1d": [0.01, pd.NA],
                "return_20d": [0.04, pd.NA],
                "rvol_10d_ann": [14.0, pd.NA],
                "vix_spot_close": [18.0, 25.0],
                "vix3m_close": [18.8, pd.NA],
                "vix_slope": [0.8, pd.NA],
                "vix_gt_32_streak": [0, 0],
                "inputs_complete_flag": [True, False],
            }
        ),
    )
    monkeypatch.setattr(
        regime_job,
        "_resolve_publish_window",
        lambda inputs, *, market_series: regime_job._RegimePublishWindow(
            published_as_of_date=date(2026, 3, 19),
            input_as_of_date=date(2026, 3, 20),
            skipped_trailing_input_dates=(date(2026, 3, 20),),
        ),
    )
    monkeypatch.setattr(
        regime_job,
        "_replace_postgres_tables",
        lambda *args, **kwargs: call_counts.__setitem__("replace", call_counts["replace"] + 1),
    )
    monkeypatch.setattr(
        regime_job,
        "_write_storage_parquet_outputs",
        lambda *args, **kwargs: call_counts.__setitem__("storage", call_counts["storage"] + 1),
    )
    monkeypatch.setattr(
        regime_job,
        "finalize_regime_publication",
        lambda *args, **kwargs: call_counts.__setitem__("finalize", call_counts["finalize"] + 1),
    )
    monkeypatch.setattr(
        regime_job,
        "trigger_next_job_from_env",
        lambda: call_counts.__setitem__("trigger", call_counts["trigger"] + 1),
    )
    monkeypatch.setattr(
        regime_job,
        "log_regime_publication_status",
        lambda state, failed_finalization=0: logged_status.append((dict(state), int(failed_finalization))),
    )

    exit_code = regime_job.main()

    assert exit_code == 2
    assert call_counts == {"replace": 0, "storage": 0, "finalize": 0, "trigger": 0}
    assert logged_status == [
        (
            {
                "as_of_date": "2026-03-19",
                "published_as_of_date": "2026-03-19",
                "input_as_of_date": "2026-03-20",
                "history_rows": 0,
                "latest_rows": 0,
                "transition_rows": 0,
                "active_models": [],
                "downstream_triggered": False,
                "warnings": [
                    "Trailing incomplete regime input dates skipped from published regime surfaces: 2026-03-20. "
                    "Published regime state remains capped at 2026-03-19."
                ],
                "status": "retry_pending",
                "reason": "stale_eod_input",
                "failure_mode": "none",
            },
            0,
        )
    ]
