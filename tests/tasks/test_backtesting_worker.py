from __future__ import annotations

import pytest

from asset_allocation_contracts.backtest import BacktestReconcileResponse
from tasks.backtesting import reconcile as reconcile_task
from tasks.backtesting import worker


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self):
        return (1,)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_preflight_dependencies_checks_transport_and_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor()
    transport_closed: list[bool] = []
    transport_calls: list[tuple[str, str]] = []
    messages: list[str] = []

    class _FakeTransport:
        def probe(self, path: str) -> None:
            transport_calls.append(("GET", path))

        def close(self) -> None:
            transport_closed.append(True)

    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport())
    monkeypatch.setattr(worker, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(worker.logger, "info", lambda message, *args: messages.append(message % args))

    worker.preflight_dependencies(
        dsn="postgresql://test:test@localhost:5432/asset_allocation",
        execution_name="exec-1",
        explicit_run_id="run-123",
    )

    assert cursor.executed == ["SELECT 1"]
    assert transport_calls == [("GET", "/api/internal/backtests/ready")]
    assert transport_closed == [True]
    assert any("phase=preflight_postgres_ok" in message for message in messages)
    assert any("phase=preflight_control_plane_ok" in message for message in messages)
    assert any("phase=preflight_ok" in message for message in messages)
    assert messages.index(next(message for message in messages if "phase=preflight_control_plane_ok" in message)) < messages.index(
        next(message for message in messages if "phase=preflight_ok" in message)
    )


def test_main_returns_failure_without_claiming_when_preflight_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("BACKTEST_RUN_ID", "run-123")
    monkeypatch.setattr(worker, "connect", lambda _dsn: _FakeConnection(_FakeCursor()))

    class _FakeTransport:
        def __init__(self) -> None:
            self.closed = False

        def probe(self, path: str) -> None:
            raise RuntimeError("control plane ready probe failed")

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(worker.ControlPlaneTransport, "from_env", lambda: _FakeTransport())

    class _UnexpectedRepo:
        def __init__(self, _dsn: str) -> None:
            raise AssertionError("worker should not build a repository after preflight failure")

    monkeypatch.setattr(worker, "BacktestRepository", _UnexpectedRepo)

    assert worker.main() == 1


def test_main_preserves_primary_failure_when_reporting_also_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("BACKTEST_RUN_ID", "run-123")
    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)
    messages: list[str] = []
    monkeypatch.setattr(worker.logger, "exception", lambda message, *args: messages.append(message % args))

    class _FakeRepo:
        def __init__(self, _dsn: str) -> None:
            self.failures: list[tuple[str, str]] = []

        def get_run(self, run_id: str):
            return {"run_id": run_id, "status": "running"}

        def fail_run(self, run_id: str, *, error: str) -> None:
            self.failures.append((run_id, error))
            raise RuntimeError("reporting failed")

    monkeypatch.setattr(worker, "BacktestRepository", _FakeRepo)
    monkeypatch.setattr(worker, "execute_backtest_run", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("runtime failed")))

    assert worker.main() == 1
    assert any("Backtest run failed: run_id=run-123" in message for message in messages)
    assert any("Backtest failure reporting failed: run_id=run-123" in message for message in messages)


def test_reconcile_task_returns_success_and_logs_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []

    class _FakeRepo:
        def reconcile_runs(self) -> BacktestReconcileResponse:
            return BacktestReconcileResponse(
                dispatchedCount=2,
                dispatchFailedCount=1,
                failedStaleRunningCount=1,
                skippedActiveCount=3,
                noActionCount=0,
                dispatchedRunIds=["run-1", "run-2"],
                dispatchFailedRunIds=["run-3"],
                failedRunIds=["run-4"],
            )

    monkeypatch.setattr(reconcile_task, "BacktestRepository", _FakeRepo)
    monkeypatch.setattr(reconcile_task.logger, "info", lambda message, *args: messages.append(message % args))

    assert reconcile_task.main() == 0
    assert any("phase=reconcile" in message for message in messages)
    assert any("dispatched=2" in message for message in messages)
