from __future__ import annotations

import pytest

import tasks.ranking.platinum_rankings as platinum_rankings


class _FakeStrategyRepository:
    def __init__(self, _dsn: str) -> None:
        pass

    def list_strategies(self) -> list[dict[str, str]]:
        return [
            {"name": "zeta"},
            {"name": "alpha"},
            {"name": "duplicate"},
            {"name": "duplicate"},
            {"name": "missing-schema"},
            {"name": "blank-schema"},
            {"name": ""},
            {},
        ]

    def get_strategy(self, name: str) -> dict[str, object] | None:
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


def test_resolve_strategy_names_filters_invalid_configs_and_sorts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platinum_rankings, "StrategyRepository", _FakeStrategyRepository)
    monkeypatch.setattr(platinum_rankings, "RankingRepository", _FakeRankingRepository)

    names = platinum_rankings._resolve_strategy_names("postgresql://test")

    assert names == ["alpha", "duplicate", "zeta"]


def test_main_materializes_each_eligible_strategy_without_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(platinum_rankings, "_resolve_strategy_names", lambda _dsn: ["alpha", "zeta"])
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
            },
        ),
        (
            "postgresql://test",
            {
                "strategy_name": "zeta",
                "triggered_by": "job",
            },
        ),
    ]


def test_main_returns_zero_when_no_ranking_enabled_strategies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(platinum_rankings, "_resolve_strategy_names", lambda _dsn: [])
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
