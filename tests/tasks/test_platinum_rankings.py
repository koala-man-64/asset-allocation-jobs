from __future__ import annotations

import pytest

import tasks.ranking.platinum_rankings as platinum_rankings


class _FakeRankingRepository:
    work_items: list[dict[str, object]] = []
    claims: list[str | None] = []
    completions: list[tuple[str, str, str | None]] = []
    failures: list[tuple[str, str]] = []

    def __init__(self, _dsn: str) -> None:
        type(self).claims = []
        type(self).completions = []
        type(self).failures = []

    def claim_next_refresh(self, *, execution_name: str | None = None) -> dict[str, object] | None:
        type(self).claims.append(execution_name)
        if not type(self).work_items:
            return None
        return type(self).work_items.pop(0)

    def complete_refresh(
        self,
        strategy_name: str,
        *,
        claim_token: str,
        run_id: str | None = None,
        dependency_fingerprint: str | None = None,
        dependency_state: dict[str, object] | None = None,
    ) -> dict[str, object]:
        _ = dependency_fingerprint, dependency_state
        type(self).completions.append((strategy_name, claim_token, run_id))
        return {"status": "ok"}

    def fail_refresh(self, strategy_name: str, *, claim_token: str, error: str) -> dict[str, object]:
        _ = claim_token
        type(self).failures.append((strategy_name, error))
        return {"status": "ok"}


def test_main_claims_and_materializes_each_pending_window(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str]] = []
    markers: list[dict[str, object]] = []

    _FakeRankingRepository.work_items = [
        {
            "strategyName": "alpha",
            "claimToken": "claim-1",
            "startDate": "2026-03-01",
            "endDate": "2026-03-03",
            "dependencyFingerprint": "fp-1",
            "dependencyState": {"domains": {}},
        },
        {
            "strategyName": "zeta",
            "claimToken": "claim-2",
            "startDate": "2026-03-04",
            "endDate": "2026-03-05",
            "dependencyFingerprint": "fp-2",
            "dependencyState": {"domains": {}},
        },
    ]
    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(platinum_rankings, "RankingRepository", _FakeRankingRepository)
    monkeypatch.setattr(
        platinum_rankings,
        "write_system_health_marker",
        lambda **kwargs: markers.append(kwargs) or True,
    )
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "job-run-1")

    def fake_materialize(dsn: str, **kwargs: object) -> dict[str, object]:
        calls.append((dsn, str(kwargs["strategy_name"]), str(kwargs["start_date"])))
        return {
            "strategyName": str(kwargs["strategy_name"]),
            "rankingSchemaName": "quality",
            "outputTableName": "platinum_table",
            "rowCount": 1,
            "dateCount": 1,
            "runId": f"run-{kwargs['strategy_name']}",
            "status": "success",
            "startDate": str(kwargs["start_date"]),
            "endDate": str(kwargs["end_date"]),
            "previousWatermark": None,
            "currentWatermark": str(kwargs["end_date"]),
            "reason": None,
        }

    monkeypatch.setattr(platinum_rankings, "materialize_strategy_rankings", fake_materialize)

    result = platinum_rankings.main()

    assert result == 0
    assert calls == [
        ("postgresql://test", "alpha", "2026-03-01"),
        ("postgresql://test", "zeta", "2026-03-04"),
    ]
    assert _FakeRankingRepository.claims == ["job-run-1", "job-run-1", "job-run-1"]
    assert _FakeRankingRepository.completions == [
        ("alpha", "claim-1", "run-alpha"),
        ("zeta", "claim-2", "run-zeta"),
    ]
    assert markers == [
        {
            "layer": "platinum",
            "domain": "rankings",
            "job_name": "platinum-rankings-job",
            "metadata": {"completedCount": 2},
        }
    ]


def test_main_marks_failed_claim_and_returns_non_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeRankingRepository.work_items = [
        {
            "strategyName": "alpha",
            "claimToken": "claim-1",
            "startDate": "2026-03-01",
            "endDate": "2026-03-03",
            "dependencyFingerprint": "fp-1",
            "dependencyState": {"domains": {}},
        }
    ]
    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(platinum_rankings, "RankingRepository", _FakeRankingRepository)
    monkeypatch.setattr(platinum_rankings, "write_system_health_marker", lambda **_kwargs: True)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(
        platinum_rankings,
        "materialize_strategy_rankings",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    result = platinum_rankings.main()

    assert result == 1
    assert _FakeRankingRepository.failures == [("alpha", "boom")]


def test_main_returns_zero_when_no_refresh_work_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeRankingRepository.work_items = []
    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.setattr(platinum_rankings, "RankingRepository", _FakeRankingRepository)
    monkeypatch.setattr(
        platinum_rankings,
        "materialize_strategy_rankings",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("materialize should not be called")),
    )
    monkeypatch.setattr(platinum_rankings, "write_system_health_marker", lambda **_kwargs: True)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")

    assert platinum_rankings.main() == 0


def test_main_requires_postgres_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platinum_rankings, "_configure_job_logging", lambda: None)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    with pytest.raises(ValueError, match="POSTGRES_DSN is required"):
        platinum_rankings.main()
