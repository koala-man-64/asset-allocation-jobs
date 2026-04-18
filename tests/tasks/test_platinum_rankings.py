from __future__ import annotations

import pytest

import tasks.ranking.platinum_rankings as platinum_rankings


class _FakeStrategyRepository:
    requested_names: list[str] = []

    def __init__(self, _dsn: str) -> None:
        pass

    def list_strategies(self) -> list[dict[str, object]]:
        type(self).requested_names = []
        return [
            {"name": "zeta"},
            {"name": "alpha", "config": {"rankingSchemaName": "quality"}, "output_table_name": "alpha_table"},
            {"name": "duplicate"},
            {"name": "duplicate"},
            {"name": "missing-schema"},
            {"name": "blank-schema"},
            {"name": ""},
            {},
        ]

    def get_strategy(self, name: str) -> dict[str, object] | None:
        type(self).requested_names.append(name)
        details = {
            "alpha": {"config": {"rankingSchemaName": "quality"}},
            "blank-schema": {"config": {"rankingSchemaName": "   "}},
            "duplicate": {"config": {"rankingSchemaName": "quality"}},
            "missing-schema": {"config": {"rankingSchemaName": "missing"}},
            "zeta": {"config": {"rankingSchemaName": "momentum"}},
        }
        return details.get(name)


class _FakeRankingRepository:
    def __init__(self, _dsn: str) -> None:
        pass

    def list_ranking_schemas(self) -> list[dict[str, str]]:
        return [
            {"name": "momentum"},
            {"name": "quality"},
        ]


def test_resolve_strategy_candidates_filters_invalid_configs_and_sorts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platinum_rankings, "StrategyRepository", _FakeStrategyRepository)
    monkeypatch.setattr(platinum_rankings, "RankingRepository", _FakeRankingRepository)

    candidates = platinum_rankings._resolve_strategy_candidates("postgresql://test")

    assert [candidate["name"] for candidate in candidates] == ["alpha", "duplicate", "zeta"]
    assert "alpha" not in _FakeStrategyRepository.requested_names


def test_main_materializes_each_eligible_strategy_without_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(
        platinum_rankings,
        "_resolve_strategy_candidates",
        lambda _dsn: [
            {"name": "alpha", "config": {"rankingSchemaName": "quality"}},
            {"name": "zeta", "config": {"rankingSchemaName": "momentum"}},
        ],
    )
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")

    def fake_materialize(dsn: str, **kwargs: object) -> dict[str, object]:
        calls.append((dsn, kwargs))
        return {
            "strategyName": str(kwargs["strategy_name"]),
            "rankingSchemaName": "quality",
            "outputTableName": "platinum_table",
            "rowCount": 1,
            "dateCount": 1,
            "runId": f"run-{kwargs['strategy_name']}",
            "status": "success",
            "startDate": "2026-03-07",
            "endDate": "2026-03-07",
            "previousWatermark": None,
            "currentWatermark": "2026-03-07",
            "reason": None,
        }

    monkeypatch.setattr(platinum_rankings, "materialize_strategy_rankings", fake_materialize)

    result = platinum_rankings.main()

    assert result == 0
    assert calls == [
        (
            "postgresql://test",
            {
                "strategy_name": "alpha",
                "triggered_by": "job",
                "strategy_payload": {"name": "alpha", "config": {"rankingSchemaName": "quality"}},
            },
        ),
        (
            "postgresql://test",
            {
                "strategy_name": "zeta",
                "triggered_by": "job",
                "strategy_payload": {"name": "zeta", "config": {"rankingSchemaName": "momentum"}},
            },
        ),
    ]


def test_main_continues_after_strategy_failure_and_returns_non_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(
        platinum_rankings,
        "_resolve_strategy_candidates",
        lambda _dsn: [
            {"name": "alpha", "config": {"rankingSchemaName": "quality"}},
            {"name": "zeta", "config": {"rankingSchemaName": "momentum"}},
        ],
    )
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")

    def fake_materialize(_dsn: str, **kwargs: object) -> dict[str, object]:
        strategy_name = str(kwargs["strategy_name"])
        calls.append(strategy_name)
        if strategy_name == "alpha":
            raise RuntimeError("boom")
        return {
            "strategyName": strategy_name,
            "rankingSchemaName": "quality",
            "outputTableName": "platinum_table",
            "rowCount": 1,
            "dateCount": 1,
            "runId": f"run-{strategy_name}",
            "status": "success",
            "startDate": "2026-03-07",
            "endDate": "2026-03-07",
            "previousWatermark": None,
            "currentWatermark": "2026-03-07",
            "reason": None,
        }

    monkeypatch.setattr(platinum_rankings, "materialize_strategy_rankings", fake_materialize)

    result = platinum_rankings.main()

    assert result == 1
    assert calls == ["alpha", "zeta"]


def test_main_returns_zero_when_no_ranking_enabled_strategies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(platinum_rankings, "_resolve_strategy_candidates", lambda _dsn: [])
    monkeypatch.setattr(
        platinum_rankings,
        "materialize_strategy_rankings",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("materialize should not be called")),
    )
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")

    assert platinum_rankings.main() == 0


def test_main_requires_postgres_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    with pytest.raises(ValueError, match="POSTGRES_DSN is required"):
        platinum_rankings.main()
