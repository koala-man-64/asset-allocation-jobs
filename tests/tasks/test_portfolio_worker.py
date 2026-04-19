from __future__ import annotations

from datetime import date

import pytest

from core.portfolio_contracts import PortfolioSnapshot
from core.portfolio_materialization import PortfolioMaterializationResult
from core.portfolio_repository import PortfolioMaterializationBundle, PortfolioMaterializationWorkItem
from tasks.portfolio import worker


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


def _bundle() -> PortfolioMaterializationBundle:
    return PortfolioMaterializationBundle(
        account=type(
            "Account",
            (),
            {
                "accountId": "acct-core",
                "name": "Core",
                "inceptionDate": date(2026, 1, 2),
            },
        )(),
        account_revision=None,
        active_assignment=None,
        portfolio=None,
        portfolio_revision=None,
        ledger_events=(),
        alerts=(),
        freshness=(),
        dependency_fingerprint="fp-1",
        dependency_state={"x": 1},
    )


def _result() -> PortfolioMaterializationResult:
    return PortfolioMaterializationResult(
        snapshot=PortfolioSnapshot(
            accountId="acct-core",
            accountName="Core",
            asOf="2026-04-19",
            nav=100.0,
            cash=100.0,
            grossExposure=0.0,
            netExposure=0.0,
            sinceInceptionPnl=0.0,
            sinceInceptionReturn=0.0,
            currentDrawdown=0.0,
            freshness=[],
            slices=[],
        ),
        history=(),
        positions=(),
        attribution=(),
        alerts=(),
        dependency_fingerprint="fp-1",
        dependency_state={"x": 1},
    )


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
        explicit_account_id="acct-core",
    )

    assert cursor.executed == ["SELECT 1"]
    assert transport_calls == [("GET", "/api/internal/portfolio-materializations/ready")]
    assert transport_closed == [True]
    assert any("phase=preflight_postgres_ok" in message for message in messages)
    assert any("phase=preflight_control_plane_ok" in message for message in messages)


def test_main_claims_materializes_persists_and_completes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "portfolio-job-7")
    monkeypatch.delenv("PORTFOLIO_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("PORTFOLIO_MATERIALIZATION_CLAIM_TOKEN", raising=False)

    calls: list[tuple[str, str]] = []
    markers: list[dict[str, object]] = []

    class _FakeRepo:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == "postgresql://test"
            self.transport = self

        def probe_ready(self) -> None:
            calls.append(("probe", "ready"))

        def close(self) -> None:
            calls.append(("transport", "close"))

        def claim_next_materialization(self, *, execution_name=None):
            calls.append(("claim", execution_name or ""))
            return PortfolioMaterializationWorkItem(account_id="acct-core", claim_token="claim-1")

        def start_materialization(self, account_id: str, *, claim_token: str, execution_name=None) -> None:
            calls.append(("start", account_id))

        def update_heartbeat(self, account_id: str, *, claim_token: str) -> None:
            calls.append(("heartbeat", account_id))

        def get_materialization_bundle(self, account_id: str, *, claim_token=None):
            calls.append(("bundle", account_id))
            return _bundle()

        def complete_materialization(self, account_id: str, **kwargs) -> dict[str, object]:
            calls.append(("complete", account_id))
            return {"status": "ok"}

        def fail_materialization(self, account_id: str, *, claim_token: str, error: str) -> dict[str, object]:
            calls.append(("fail", account_id))
            return {"status": "ok"}

    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "PortfolioMaterializationRepository", _FakeRepo)
    monkeypatch.setattr(worker, "materialize_portfolio_bundle", lambda *_args, **_kwargs: _result())
    monkeypatch.setattr(worker, "write_system_health_marker", lambda **kwargs: markers.append(kwargs) or True)
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.logger, "exception", lambda *args, **kwargs: None)

    assert worker.main() == 0
    assert ("claim", "portfolio-job-7") in calls
    assert ("complete", "acct-core") in calls
    assert markers == [
        {
            "layer": "platinum",
            "domain": "portfolio",
            "job_name": "portfolio-materialization-worker",
            "metadata": {"accountId": "acct-core", "asOf": "2026-04-19"},
        }
    ]


def test_main_reports_failure_when_materialization_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "portfolio-job-7")
    monkeypatch.delenv("PORTFOLIO_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("PORTFOLIO_MATERIALIZATION_CLAIM_TOKEN", raising=False)

    calls: list[tuple[str, str]] = []

    class _FakeRepo:
        def __init__(self, dsn: str | None = None) -> None:
            self.transport = self

        def close(self) -> None:
            return None

        def claim_next_materialization(self, *, execution_name=None):
            return PortfolioMaterializationWorkItem(account_id="acct-core", claim_token="claim-1")

        def start_materialization(self, account_id: str, *, claim_token: str, execution_name=None) -> None:
            calls.append(("start", account_id))

        def update_heartbeat(self, account_id: str, *, claim_token: str) -> None:
            calls.append(("heartbeat", account_id))

        def get_materialization_bundle(self, account_id: str, *, claim_token=None):
            return _bundle()

        def complete_materialization(self, account_id: str, **kwargs) -> dict[str, object]:
            calls.append(("complete", account_id))
            return {"status": "ok"}

        def fail_materialization(self, account_id: str, *, claim_token: str, error: str) -> dict[str, object]:
            calls.append(("fail", account_id))
            return {"status": "ok"}

    monkeypatch.setattr(worker, "preflight_dependencies", lambda **_kwargs: None)
    monkeypatch.setattr(worker, "PortfolioMaterializationRepository", _FakeRepo)
    monkeypatch.setattr(
        worker,
        "materialize_portfolio_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(worker.logger, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(worker.logger, "exception", lambda *args, **kwargs: None)

    assert worker.main() == 1
    assert ("fail", "acct-core") in calls
