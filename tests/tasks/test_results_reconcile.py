from __future__ import annotations

from dataclasses import dataclass

import pytest

from tasks import results_reconcile


@dataclass(frozen=True)
class _Result:
    dryRun: bool = False
    rankingDirtyCount: int = 0
    rankingNoopCount: int = 0
    canonicalEnqueuedCount: int = 0
    canonicalUpToDateCount: int = 0
    canonicalSkippedCount: int = 0
    publicationSignalsProcessedCount: int = 0
    publicationSignalsErrorCount: int = 0
    errorCount: int = 0
    errors: list[str] | None = None


def test_env_flag_accepts_only_explicit_boolean_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RESULTS_RECONCILE_DRY_RUN", "yes")
    assert results_reconcile._env_flag("RESULTS_RECONCILE_DRY_RUN") is True

    monkeypatch.setenv("RESULTS_RECONCILE_DRY_RUN", "no")
    assert results_reconcile._env_flag("RESULTS_RECONCILE_DRY_RUN") is False

    monkeypatch.setenv("RESULTS_RECONCILE_DRY_RUN", "maybe")
    with pytest.raises(ValueError, match="RESULTS_RECONCILE_DRY_RUN must be a boolean value"):
        results_reconcile._env_flag("RESULTS_RECONCILE_DRY_RUN")


def test_main_defaults_to_live_reconcile_and_returns_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []

    class _Repo:
        def reconcile(self, *, dry_run: bool = False) -> _Result:
            calls.append(dry_run)
            return _Result(errors=[])

    monkeypatch.delenv("RESULTS_RECONCILE_DRY_RUN", raising=False)
    monkeypatch.setattr(results_reconcile, "ResultsRepository", lambda: _Repo())

    assert results_reconcile.main() == 0
    assert calls == [False]


def test_main_forwards_dry_run_and_returns_nonzero_on_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []

    class _Repo:
        def reconcile(self, *, dry_run: bool = False) -> _Result:
            calls.append(dry_run)
            return _Result(
                dryRun=dry_run,
                publicationSignalsProcessedCount=1,
                publicationSignalsErrorCount=1,
                errorCount=1,
                errors=["publication:regime:boom"],
            )

    monkeypatch.setenv("RESULTS_RECONCILE_DRY_RUN", "true")
    monkeypatch.setattr(results_reconcile, "ResultsRepository", lambda: _Repo())

    assert results_reconcile.main() == 1
    assert calls == [True]


def test_main_propagates_repository_validation_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Repo:
        def reconcile(self, *, dry_run: bool = False) -> _Result:
            raise ValueError("Results reconcile response was not a JSON object.")

    monkeypatch.delenv("RESULTS_RECONCILE_DRY_RUN", raising=False)
    monkeypatch.setattr(results_reconcile, "ResultsRepository", lambda: _Repo())

    with pytest.raises(ValueError, match="Results reconcile response was not a JSON object"):
        results_reconcile.main()
