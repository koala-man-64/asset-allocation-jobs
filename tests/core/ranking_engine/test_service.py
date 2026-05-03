from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

import asset_allocation_runtime_common.ranking_engine.service as service_module
from asset_allocation_runtime_common.ranking_engine.naming import build_scoped_identifier, slugify_strategy_output_table
from asset_allocation_runtime_common.ranking_engine.service import (
    _MaterializationContext,
    _ResolvedDateRange,
    _apply_transforms,
    _compute_rankings_dataframe,
    _evaluate_universe_mask,
    _load_source_date_bounds,
    _normalize_loaded_column,
    _persist_materialization,
    _resolve_date_range,
    _write_rankings_to_platinum,
)
from asset_allocation_runtime_common.ranking_engine.contracts import RankingSchemaConfig
from asset_allocation_runtime_common.strategy_engine.contracts import StrategyConfig


def _build_strategy_config(*, ranking_schema_name: str = "quality") -> StrategyConfig:
    return StrategyConfig.model_validate(
        {
            "universeConfigName": "large-cap-quality",
            "rankingSchemaName": ranking_schema_name,
            "rebalance": "monthly",
            "longOnly": True,
            "topN": 20,
            "lookbackWindow": 63,
            "holdingPeriod": 21,
            "costModel": "default",
            "intrabarConflictPolicy": "stop_first",
            "exits": [],
        }
    )


def _build_ranking_schema_config() -> RankingSchemaConfig:
    return RankingSchemaConfig.model_validate(
        {
            "universeConfigName": "quality-universe",
            "groups": [
                {
                    "name": "quality",
                    "weight": 1,
                    "factors": [
                        {
                            "name": "momentum",
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
    )


def _build_universe() -> SimpleNamespace:
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
                    "value": 10,
                }
            ],
        },
    )


def _build_context() -> _MaterializationContext:
    universe = _build_universe()
    return _MaterializationContext(
        strategy_name="alpha",
        output_table_name="mom_spy_res",
        strategy_config=_build_strategy_config(),
        ranking_schema_name="quality",
        ranking_schema_version=3,
        ranking_schema=_build_ranking_schema_config(),
        strategy_universe=universe,
        ranking_universe=universe,
        table_specs={},
        required_columns={"market_data": {"return_20d"}},
    )


def test_slugify_strategy_output_table_normalizes_invalid_characters() -> None:
    assert slugify_strategy_output_table("My Strategy / 2026") == "my_strategy_2026"


def test_build_scoped_identifier_stays_within_postgres_limit() -> None:
    identifier = build_scoped_identifier("x" * 63, "symbol", "date", "idx")

    assert len(identifier) <= 63
    assert identifier.endswith("_idx")


def test_apply_transforms_runs_in_order() -> None:
    series = pd.Series([1.0, 2.0, 3.0])
    dates = pd.Series([date(2026, 3, 7), date(2026, 3, 7), date(2026, 3, 7)])
    transformed = _apply_transforms(
        series,
        dates,
        [
            type("Transform", (), {"type": "negate", "params": {}})(),
            type("Transform", (), {"type": "abs", "params": {}})(),
            type("Transform", (), {"type": "percentile_rank", "params": {}})(),
        ],
    )

    assert transformed.tolist() == [1 / 3, 2 / 3, 1.0]


def test_evaluate_universe_mask_handles_nested_groups() -> None:
    universe = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gt",
                    "value": 10,
                },
                {
                    "kind": "group",
                    "operator": "or",
                    "clauses": [
                        {
                            "kind": "condition",
                            "field": "quality.piotroski_f_score",
                            "operator": "gte",
                            "value": 7,
                        },
                        {
                            "kind": "condition",
                            "field": "returns.return_20d",
                            "operator": "gt",
                            "value": 0.1,
                        },
                    ],
                },
            ],
        },
    )
    frame = pd.DataFrame(
        {
            "market_data__close": [12.0, 9.0, 15.0],
            "finance_data__piotroski_f_score": [5, 8, 6],
            "market_data__return_20d": [0.05, 0.2, 0.15],
        }
    )

    mask = _evaluate_universe_mask(frame, universe.root)

    assert mask.tolist() == [False, False, True]


def test_evaluate_universe_mask_handles_string_and_null_safely() -> None:
    frame = pd.DataFrame(
        {
            "market_data__sector": pd.Series(["Technology", "Financials", pd.NA], dtype="string"),
        }
    )
    equals_node = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "security.sector",
                    "operator": "eq",
                    "value": "Technology",
                }
            ],
        },
    )
    not_in_node = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "security.sector",
                    "operator": "not_in",
                    "values": ["Financials"],
                }
            ],
        },
    )

    assert _evaluate_universe_mask(frame, equals_node.root).tolist() == [True, False, False]
    assert _evaluate_universe_mask(frame, not_in_node.root).tolist() == [True, False, False]


def test_evaluate_universe_mask_handles_date_datetime_boolean_and_numeric_types() -> None:
    frame = pd.DataFrame(
        {
            "market_data__trade_date": [date(2026, 3, 7), date(2026, 3, 8), None],
            "market_data__timestamp": pd.to_datetime(
                ["2026-03-07T12:00:00Z", "2026-03-08T13:00:00Z", None],
                utc=True,
            ).tz_localize(None),
            "market_data__active": pd.Series([True, False, pd.NA], dtype="boolean"),
            "market_data__close": [10.0, 12.0, None],
        }
    )
    date_node = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.trade_date",
                    "operator": "gte",
                    "value": "2026-03-08",
                }
            ],
        },
    )
    datetime_node = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.timestamp",
                    "operator": "gte",
                    "value": "2026-03-08T00:00:00Z",
                }
            ],
        },
    )
    boolean_node = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "security.is_active",
                    "operator": "eq",
                    "value": True,
                }
            ],
        },
    )
    numeric_node = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gte",
                    "value": 11,
                }
            ],
        },
    )

    assert _evaluate_universe_mask(frame, date_node.root).tolist() == [False, True, False]
    assert _evaluate_universe_mask(frame, datetime_node.root).tolist() == [False, True, False]
    assert _evaluate_universe_mask(frame, boolean_node.root).tolist() == [True, False, False]
    assert _evaluate_universe_mask(frame, numeric_node.root).tolist() == [False, True, False]


def test_normalize_loaded_column_preserves_supported_types() -> None:
    string_series = _normalize_loaded_column(pd.Series(["AAPL", None]), value_kind="string")
    boolean_series = _normalize_loaded_column(pd.Series([True, None]), value_kind="boolean")
    date_series = _normalize_loaded_column(pd.Series(["2026-03-07", None]), value_kind="date")
    datetime_series = _normalize_loaded_column(pd.Series(["2026-03-07T12:00:00Z", None]), value_kind="datetime")
    number_series = _normalize_loaded_column(pd.Series(["1.25", "bad"]), value_kind="number")

    assert str(string_series.dtype) == "string"
    assert string_series.iloc[0] == "AAPL"
    assert pd.isna(string_series.iloc[1])

    assert str(boolean_series.dtype) == "boolean"
    assert bool(boolean_series.iloc[0]) is True
    assert pd.isna(boolean_series.iloc[1])

    assert date_series.iloc[0] == date(2026, 3, 7)
    assert pd.isna(date_series.iloc[1])

    assert str(datetime_series.dtype) == "datetime64[ns]"
    assert datetime_series.iloc[0].to_pydatetime() == datetime(2026, 3, 7, 12, 0)
    assert pd.isna(datetime_series.iloc[1])

    assert number_series.iloc[0] == 1.25
    assert pd.isna(number_series.iloc[1])


def test_resolve_date_range_defaults_to_watermark_plus_one_day(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        service_module,
        "_load_source_date_bounds",
        lambda _dsn, **_kwargs: (date(2026, 3, 1), date(2026, 3, 10)),
    )
    monkeypatch.setattr(
        service_module,
        "_get_ranking_watermark",
        lambda _dsn, _strategy_name: date(2026, 3, 7),
    )

    resolved = _resolve_date_range(
        "postgresql://test",
        strategy_name="alpha",
        strategy_config=_build_strategy_config(),
        ranking_schema=_build_ranking_schema_config(),
        start_date=None,
        end_date=None,
        table_specs={"market_data": object()},
        strategy_universe=_build_universe(),
        ranking_universe=_build_universe(),
        required_columns={"market_data": {"close"}},
    )

    assert resolved.start_date == date(2026, 3, 8)
    assert resolved.end_date == date(2026, 3, 10)
    assert resolved.previous_watermark == date(2026, 3, 7)
    assert resolved.noop is False


def test_resolve_date_range_returns_noop_when_output_is_current(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        service_module,
        "_load_source_date_bounds",
        lambda _dsn, **_kwargs: (date(2026, 3, 1), date(2026, 3, 10)),
    )
    monkeypatch.setattr(
        service_module,
        "_get_ranking_watermark",
        lambda _dsn, _strategy_name: date(2026, 3, 10),
    )

    resolved = _resolve_date_range(
        "postgresql://test",
        strategy_name="alpha",
        strategy_config=_build_strategy_config(),
        ranking_schema=_build_ranking_schema_config(),
        start_date=None,
        end_date=None,
        table_specs={"market_data": object()},
        strategy_universe=_build_universe(),
        ranking_universe=_build_universe(),
        required_columns={"market_data": {"close"}},
    )

    assert resolved.start_date == date(2026, 3, 10)
    assert resolved.end_date == date(2026, 3, 10)
    assert resolved.previous_watermark == date(2026, 3, 10)
    assert resolved.noop is True
    assert resolved.reason == "Ranking output already current."


def test_resolve_date_range_rejects_invalid_explicit_range(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        service_module,
        "_load_source_date_bounds",
        lambda _dsn, **_kwargs: (date(2026, 3, 1), date(2026, 3, 10)),
    )
    monkeypatch.setattr(service_module, "_get_ranking_watermark", lambda _dsn, _strategy_name: None)

    with pytest.raises(ValueError, match="Resolved ranking date range is invalid"):
        _resolve_date_range(
            "postgresql://test",
            strategy_name="alpha",
            strategy_config=_build_strategy_config(),
            ranking_schema=_build_ranking_schema_config(),
            start_date=date(2026, 3, 11),
            end_date=date(2026, 3, 10),
            table_specs={"market_data": object()},
            strategy_universe=_build_universe(),
            ranking_universe=_build_universe(),
            required_columns={"market_data": {"close"}},
        )


def test_compute_rankings_dataframe_intersects_strategy_and_ranking_universes(monkeypatch) -> None:
    strategy_universe = _build_universe()
    ranking_universe = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "quality.piotroski_f_score",
                    "operator": "gte",
                    "value": 7,
                }
            ],
        },
    )
    monkeypatch.setattr(
        service_module.universe_service,
        "_load_gold_table_specs",
        lambda _dsn: {},
    )
    monkeypatch.setattr(service_module, "_resolve_strategy_universe", lambda _dsn, _config: strategy_universe)
    monkeypatch.setattr(service_module, "_resolve_ranking_universe", lambda _dsn, _schema: ranking_universe)
    monkeypatch.setattr(
        service_module,
        "_load_table_frames",
        lambda _dsn, **kwargs: {
            "market_data": pd.DataFrame(
                {
                    "date": [date(2026, 3, 7), date(2026, 3, 7), date(2026, 3, 7)],
                    "symbol": ["AAPL", "MSFT", "TSLA"],
                    "market_data__close": [20.0, 22.0, 5.0],
                    "market_data__return_20d": [0.3, 0.2, 0.9],
                }
            ),
            "finance_data": pd.DataFrame(
                {
                    "date": [date(2026, 3, 7), date(2026, 3, 7), date(2026, 3, 7)],
                    "symbol": ["AAPL", "MSFT", "TSLA"],
                    "finance_data__piotroski_f_score": [8, 6, 9],
                }
            ),
        },
    )

    ranked = _compute_rankings_dataframe(
        "postgresql://test:test@localhost:5432/asset_allocation",
        strategy_config=StrategyConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "rebalance": "monthly",
                "longOnly": True,
                "topN": 20,
                "lookbackWindow": 63,
                "holdingPeriod": 21,
                "costModel": "default",
                "intrabarConflictPolicy": "stop_first",
                "exits": [],
            }
        ),
        ranking_schema=RankingSchemaConfig.model_validate(
            {
                "universeConfigName": "quality-universe",
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "momentum",
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
        start_date=date(2026, 3, 7),
        end_date=date(2026, 3, 7),
    )

    assert ranked.to_dict("records") == [
        {
            "date": date(2026, 3, 7),
            "symbol": "AAPL",
            "score": 0.3,
            "rank": 1,
        }
    ]


def test_compute_rankings_dataframe_excludes_null_piotroski_rows(monkeypatch) -> None:
    strategy_universe = _build_universe()
    ranking_universe = SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "quality.piotroski_f_score",
                    "operator": "gte",
                    "value": 7,
                }
            ],
        },
    )
    monkeypatch.setattr(
        service_module.universe_service,
        "_load_gold_table_specs",
        lambda _dsn: {},
    )
    monkeypatch.setattr(service_module, "_resolve_strategy_universe", lambda _dsn, _config: strategy_universe)
    monkeypatch.setattr(service_module, "_resolve_ranking_universe", lambda _dsn, _schema: ranking_universe)
    monkeypatch.setattr(
        service_module,
        "_load_table_frames",
        lambda _dsn, **kwargs: {
            "market_data": pd.DataFrame(
                {
                    "date": [date(2026, 3, 7), date(2026, 3, 7)],
                    "symbol": ["AAPL", "MSFT"],
                    "market_data__close": [20.0, 22.0],
                    "market_data__return_20d": [0.3, 0.2],
                }
            ),
            "finance_data": pd.DataFrame(
                {
                    "date": [date(2026, 3, 7), date(2026, 3, 7)],
                    "symbol": ["AAPL", "MSFT"],
                    "finance_data__piotroski_f_score": [pd.NA, 8],
                }
            ),
        },
    )

    ranked = _compute_rankings_dataframe(
        "postgresql://test:test@localhost:5432/asset_allocation",
        strategy_config=StrategyConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "rebalance": "monthly",
                "longOnly": True,
                "topN": 20,
                "lookbackWindow": 63,
                "holdingPeriod": 21,
                "costModel": "default",
                "intrabarConflictPolicy": "stop_first",
                "exits": [],
            }
        ),
        ranking_schema=RankingSchemaConfig.model_validate(
            {
                "universeConfigName": "quality-universe",
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "momentum",
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
        start_date=date(2026, 3, 7),
        end_date=date(2026, 3, 7),
    )

    assert ranked.to_dict("records") == [
        {
            "date": date(2026, 3, 7),
            "symbol": "MSFT",
            "score": 0.2,
            "rank": 1,
        }
    ]


class _FakeCursor:
    def __init__(
        self,
        fetchone_results: list[tuple[object, ...] | None] | None = None,
        fetchall_results: list[list[tuple[object, ...]]] | None = None,
    ) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_results = list(fetchall_results or [])

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self) -> tuple[object, ...] | None:
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        if self._fetchall_results:
            return self._fetchall_results.pop(0)
        return []


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


class _TransactionalCursor:
    def __init__(self, pending: list[tuple[str, object]]) -> None:
        self.pending = pending

    def __enter__(self) -> "_TransactionalCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _TransactionalConnection:
    def __init__(self, committed: list[tuple[str, object]]) -> None:
        self._committed = committed
        self._pending: list[tuple[str, object]] = []

    def __enter__(self) -> "_TransactionalConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self._committed.extend(self._pending)
        return False

    def cursor(self) -> _TransactionalCursor:
        return _TransactionalCursor(self._pending)


def test_load_source_date_bounds_raises_when_no_candidate_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor(fetchall_results=[[], []])
    monkeypatch.setattr(service_module, "connect", lambda _dsn: _FakeConnection(cursor))

    with pytest.raises(ValueError, match="No ranking source data is available"):
        _load_source_date_bounds(
            "postgresql://test",
            table_specs={
                "market_data": type(
                    "Spec",
                    (),
                    {"as_of_column": "date", "columns": {"close": object()}},
                )()
            },
            required_columns={"market_data": {"close"}},
        )


def test_write_rankings_to_platinum_copies_score_and_rank(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor()
    copied: dict[str, object] = {}
    monkeypatch.setattr(
        service_module,
        "copy_rows",
        lambda _cur, *, table, columns, rows: copied.update(
            {
                "table": table,
                "columns": columns,
                "rows": list(rows),
            }
        ),
    )

    row_count = _write_rankings_to_platinum(
        cursor,
        table_name="mom_spy_res",
        ranked=pd.DataFrame(
            [
                {
                    "date": date(2026, 3, 7),
                    "symbol": "AAPL",
                    "score": 0.91,
                    "rank": 1,
                }
            ]
        ),
        start_date=date(2026, 3, 7),
        end_date=date(2026, 3, 7),
    )

    assert row_count == 1
    assert copied["columns"] == ("date", "symbol", "rank", "score", "last_updated_date")
    assert copied["rows"][0][:4] == (date(2026, 3, 7), "AAPL", 1, 0.91)


def test_persist_materialization_commits_run_write_and_watermark_atomically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    committed_actions: list[tuple[str, object]] = []
    monkeypatch.setattr(
        service_module,
        "connect",
        lambda _dsn: _TransactionalConnection(committed_actions),
    )

    def fake_insert(cursor, **kwargs) -> None:
        cursor.pending.append(("insert_run", kwargs["status"]))

    def fake_write(cursor, **kwargs) -> int:
        row_count = int(len(kwargs["ranked"]))
        cursor.pending.append(("copy", row_count))
        return row_count

    def fake_update(cursor, **kwargs) -> None:
        cursor.pending.append(("update_run", kwargs["status"]))

    def fake_watermark(cursor, **kwargs) -> None:
        cursor.pending.append(("watermark", kwargs["last_ranked_date"]))

    monkeypatch.setattr(service_module, "_insert_ranking_run", fake_insert)
    monkeypatch.setattr(service_module, "_write_rankings_to_platinum", fake_write)
    monkeypatch.setattr(service_module, "_update_ranking_run", fake_update)
    monkeypatch.setattr(service_module, "_upsert_ranking_watermark", fake_watermark)

    rows_written = _persist_materialization(
        "postgresql://test",
        run_id="run-123",
        context=_build_context(),
        resolved_range=_ResolvedDateRange(
            start_date=date(2026, 3, 7),
            end_date=date(2026, 3, 8),
            source_start_date=date(2026, 3, 1),
            source_end_date=date(2026, 3, 8),
            previous_watermark=date(2026, 3, 6),
        ),
        ranked=pd.DataFrame(
            [
                {
                    "date": date(2026, 3, 7),
                    "symbol": "AAPL",
                    "score": 0.91,
                    "rank": 1,
                }
            ]
        ),
        triggered_by="manual",
        date_count=1,
    )

    assert rows_written == 1
    assert committed_actions == [
        ("insert_run", "running"),
        ("copy", 1),
        ("update_run", "success"),
        ("watermark", date(2026, 3, 8)),
    ]


def test_persist_materialization_rolls_back_when_watermark_update_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    attempted_actions: list[str] = []
    committed_actions: list[tuple[str, object]] = []
    monkeypatch.setattr(
        service_module,
        "connect",
        lambda _dsn: _TransactionalConnection(committed_actions),
    )

    def fake_insert(cursor, **kwargs) -> None:
        attempted_actions.append("insert_run")
        cursor.pending.append(("insert_run", kwargs["status"]))

    def fake_write(cursor, **kwargs) -> int:
        attempted_actions.append("copy")
        row_count = int(len(kwargs["ranked"]))
        cursor.pending.append(("copy", row_count))
        return row_count

    def fake_update(cursor, **kwargs) -> None:
        attempted_actions.append("update_run")
        cursor.pending.append(("update_run", kwargs["status"]))

    def failing_watermark(cursor, **kwargs) -> None:
        attempted_actions.append("watermark")
        raise RuntimeError("watermark failure")

    monkeypatch.setattr(service_module, "_insert_ranking_run", fake_insert)
    monkeypatch.setattr(service_module, "_write_rankings_to_platinum", fake_write)
    monkeypatch.setattr(service_module, "_update_ranking_run", fake_update)
    monkeypatch.setattr(service_module, "_upsert_ranking_watermark", failing_watermark)

    with pytest.raises(RuntimeError, match="watermark failure"):
        _persist_materialization(
            "postgresql://test",
            run_id="run-123",
            context=_build_context(),
            resolved_range=_ResolvedDateRange(
                start_date=date(2026, 3, 7),
                end_date=date(2026, 3, 8),
                source_start_date=date(2026, 3, 1),
                source_end_date=date(2026, 3, 8),
                previous_watermark=date(2026, 3, 6),
            ),
            ranked=pd.DataFrame(
                [
                    {
                        "date": date(2026, 3, 7),
                        "symbol": "AAPL",
                        "score": 0.91,
                        "rank": 1,
                    }
                ]
            ),
            triggered_by="manual",
            date_count=1,
        )

    assert attempted_actions == ["insert_run", "copy", "update_run", "watermark"]
    assert committed_actions == []
