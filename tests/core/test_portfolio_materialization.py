from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest

from core.portfolio_contracts import (
    PortfolioAccount,
    PortfolioAlert,
    PortfolioAssignment,
    PortfolioHistoryPoint,
    PortfolioPosition,
    PortfolioPositionContributor,
    PortfolioRevision,
    PortfolioSnapshot,
    PortfolioSleeveAllocation,
    StrategySliceAttribution,
    StrategyVersionReference,
)
from core.portfolio_materialization import (
    PortfolioMaterializationStaleDependencyError,
    PortfolioMaterializedSurfaces,
    PortfolioServingRepository,
    PortfolioStrategyDependency,
    PortfolioStrategyHistorySample,
    _persist_materialization,
    materialize_portfolio_bundle,
)
from core.portfolio_repository import PortfolioMaterializationBundle


class _DummyCursor:
    def __enter__(self) -> "_DummyCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _DummyConnection:
    def __enter__(self) -> "_DummyConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _DummyCursor:
        return _DummyCursor()


class _FakeCursor:
    def __init__(self, *, fetchone_rows: list[tuple[Any, ...] | None] | None = None) -> None:
        self.fetchone_rows = list(fetchone_rows or [])
        self.executed: list[tuple[str, object]] = []
        self.rowcount = 1

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> tuple[Any, ...] | None:
        if self.fetchone_rows:
            return self.fetchone_rows.pop(0)
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
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


class _ServingCursor:
    def __init__(self) -> None:
        self.last_sql = ""

    def __enter__(self) -> "_ServingCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.last_sql = sql

    def fetchone(self) -> tuple[Any, ...] | None:
        if "SELECT as_of_date FROM core.portfolio_latest_snapshot" in self.last_sql:
            return (date(2026, 4, 19),)
        if "FROM core.portfolio_latest_snapshot" in self.last_sql:
            return (
                "acct-core",
                "Core",
                date(2026, 4, 19),
                110.0,
                10.0,
                0.9,
                0.8,
                10.0,
                0.1,
                -0.02,
                -0.04,
                1,
                {
                    "assignmentId": "assign-1",
                    "accountId": "acct-core",
                    "accountVersion": 1,
                    "portfolioName": "Core Model",
                    "portfolioVersion": 1,
                    "effectiveFrom": "2026-01-02",
                    "status": "active",
                    "notes": "",
                },
                [],
            )
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        if "FROM core.portfolio_attribution" in self.last_sql:
            return [
                (
                    date(2026, 4, 19),
                    "sleeve-1",
                    "alpha",
                    3,
                    1.0,
                    1.0,
                    110.0,
                    0.9,
                    0.8,
                    10.0,
                    0.09,
                    -0.02,
                    0.01,
                    0.10,
                )
            ]
        if "FROM core.portfolio_history" in self.last_sql:
            return [
                (
                    date(2026, 4, 19),
                    110.0,
                    10.0,
                    0.9,
                    0.8,
                    10.0,
                    0.1,
                    10.0,
                    0.1,
                    -0.02,
                    0.01,
                    5.0,
                )
            ]
        if "FROM core.portfolio_positions" in self.last_sql:
            return [
                (
                    date(2026, 4, 19),
                    "AAPL",
                    1.0,
                    100.0,
                    0.909090909,
                    90.0,
                    100.0,
                    10.0,
                    5.0,
                    [
                        {
                            "sleeveId": "sleeve-1",
                            "strategyName": "alpha",
                            "strategyVersion": 3,
                            "quantity": 1.0,
                            "marketValue": 100.0,
                            "weight": 0.909090909,
                        }
                    ],
                )
            ]
        if "FROM core.portfolio_alerts" in self.last_sql:
            return [
                (
                    "alert-1",
                    "acct-core",
                    "warning",
                    "open",
                    "cash_residual_high",
                    "High Cash Residual",
                    "Cash is high",
                    datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
                    None,
                    None,
                    None,
                    date(2026, 4, 19),
                )
            ]
        return []


class _ServingConnection:
    def __enter__(self) -> "_ServingConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _ServingCursor:
        return _ServingCursor()


def _build_bundle(*, dependency_fingerprint: str | None = None, dependency_state: dict[str, Any] | None = None) -> PortfolioMaterializationBundle:
    return PortfolioMaterializationBundle(
        account=PortfolioAccount.model_validate(
            {
                "accountId": "acct-core",
                "name": "Core",
                "description": "",
                "status": "active",
                "mode": "internal_model_managed",
                "accountingDepth": "position_level",
                "cadenceMode": "strategy_native",
                "baseCurrency": "USD",
                "inceptionDate": "2026-01-02",
            }
        ),
        account_revision=None,
        active_assignment=PortfolioAssignment.model_validate(
            {
                "assignmentId": "assign-1",
                "accountId": "acct-core",
                "accountVersion": 1,
                "portfolioName": "Core Model",
                "portfolioVersion": 1,
                "effectiveFrom": "2026-01-02",
                "status": "active",
                "notes": "",
            }
        ),
        portfolio=None,
        portfolio_revision=PortfolioRevision.model_validate(
            {
                "portfolioName": "Core Model",
                "version": 1,
                "allocations": [
                    PortfolioSleeveAllocation(
                        sleeveId="sleeve-1",
                        sleeveName="Alpha",
                        strategy=StrategyVersionReference(strategyName="alpha", strategyVersion=3),
                        targetWeight=1.0,
                    ).model_dump(mode="json")
                ],
            }
        ),
        ledger_events=(),
        alerts=(),
        freshness=(),
        dependency_fingerprint=dependency_fingerprint,
        dependency_state=dependency_state or {},
        as_of=date(2026, 4, 19),
    )


def _dependency(run_id: str = "run-1") -> PortfolioStrategyDependency:
    history = (
        PortfolioStrategyHistorySample(
            as_of=date(2026, 4, 18),
            portfolio_value=100.0,
            cash=10.0,
            gross_exposure=0.9,
            net_exposure=0.8,
            period_return=0.0,
            cumulative_return=0.0,
            drawdown=0.0,
            turnover=0.0,
            commission=0.0,
            slippage_cost=0.0,
        ),
        PortfolioStrategyHistorySample(
            as_of=date(2026, 4, 19),
            portfolio_value=110.0,
            cash=10.0,
            gross_exposure=0.9,
            net_exposure=0.8,
            period_return=0.1,
            cumulative_return=0.1,
            drawdown=-0.02,
            turnover=0.01,
            commission=0.0,
            slippage_cost=0.0,
        ),
    )
    return PortfolioStrategyDependency(
        sleeve_id="sleeve-1",
        strategy_name="alpha",
        strategy_version=3,
        target_weight=1.0,
        run_id=run_id,
        canonical_target_id="target-1",
        canonical_fingerprint="canonical-fp-1",
        completed_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
        latest_as_of=date(2026, 4, 19),
        initial_portfolio_value=100.0,
        latest_portfolio_value=110.0,
        latest_cash=10.0,
        latest_gross_exposure=0.9,
        latest_net_exposure=0.8,
        latest_drawdown=-0.02,
        latest_period_return=0.1,
        latest_cumulative_return=0.1,
        latest_turnover=0.01,
        history=history,
        positions=(),
    )


def _surfaces() -> PortfolioMaterializedSurfaces:
    snapshot = PortfolioSnapshot(
        accountId="acct-core",
        accountName="Core",
        asOf=date(2026, 4, 19),
        nav=110.0,
        cash=10.0,
        grossExposure=0.9,
        netExposure=0.8,
        sinceInceptionPnl=10.0,
        sinceInceptionReturn=0.1,
        currentDrawdown=-0.02,
        maxDrawdown=-0.04,
        openAlertCount=1,
        activeAssignment=PortfolioAssignment.model_validate(
            {
                "assignmentId": "assign-1",
                "accountId": "acct-core",
                "accountVersion": 1,
                "portfolioName": "Core Model",
                "portfolioVersion": 1,
                "effectiveFrom": "2026-01-02",
                "status": "active",
                "notes": "",
            }
        ),
        freshness=[],
        slices=[],
    )
    return PortfolioMaterializedSurfaces(
        snapshot=snapshot,
        history=(
            PortfolioHistoryPoint(
                asOf=date(2026, 4, 19),
                nav=110.0,
                cash=10.0,
                grossExposure=0.9,
                netExposure=0.8,
                periodPnl=10.0,
                periodReturn=0.1,
                cumulativePnl=10.0,
                cumulativeReturn=0.1,
                drawdown=-0.02,
                turnover=0.01,
                costDragBps=5.0,
            ),
        ),
        positions=(
            PortfolioPosition(
                asOf=date(2026, 4, 19),
                symbol="AAPL",
                quantity=1.0,
                marketValue=100.0,
                weight=0.909090909,
                averageCost=90.0,
                lastPrice=100.0,
                unrealizedPnl=10.0,
                realizedPnl=5.0,
                contributors=[
                    PortfolioPositionContributor(
                        sleeveId="sleeve-1",
                        strategyName="alpha",
                        strategyVersion=3,
                        quantity=1.0,
                        marketValue=100.0,
                        weight=0.909090909,
                    )
                ],
            ),
        ),
        attribution=(
            StrategySliceAttribution(
                asOf=date(2026, 4, 19),
                sleeveId="sleeve-1",
                strategyName="alpha",
                strategyVersion=3,
                targetWeight=1.0,
                actualWeight=1.0,
                marketValue=110.0,
                grossExposure=0.9,
                netExposure=0.8,
                pnlContribution=10.0,
                returnContribution=0.09,
                drawdownContribution=-0.02,
                turnover=0.01,
                sinceInceptionReturn=0.1,
            ),
        ),
        alerts=(
            PortfolioAlert(
                alertId="alert-1",
                accountId="acct-core",
                severity="warning",
                status="open",
                code="cash_residual_high",
                title="High Cash Residual",
                description="Cash is high",
                detectedAt=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
                asOf=date(2026, 4, 19),
            ),
        ),
    )


def test_materialize_portfolio_bundle_raises_on_stale_dependency_fingerprint(monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = _build_bundle(dependency_fingerprint="expected-fingerprint")

    monkeypatch.setattr("core.portfolio_materialization.connect", lambda _dsn: _DummyConnection())
    monkeypatch.setattr("core.portfolio_materialization._load_latest_strategy_dependency", lambda *args, **kwargs: _dependency())

    with pytest.raises(PortfolioMaterializationStaleDependencyError, match="Dependency fingerprint drifted"):
        materialize_portfolio_bundle("postgresql://test", bundle)


def test_persist_materialization_uses_staged_apply_without_truncate(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor(fetchone_rows=[("off",), (False,)])
    copied_tables: list[str] = []

    monkeypatch.setattr("core.portfolio_materialization.connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(
        "core.portfolio_materialization.copy_rows",
        lambda _cur, *, table, columns, rows: copied_tables.append(str(table)),
    )

    _persist_materialization(
        "postgresql://test",
        bundle=_build_bundle(),
        result=_surfaces(),
        dependency_fingerprint="fp-1",
        dependency_state={"sleeveRuns": []},
    )

    assert copied_tables == [
        "pg_temp.portfolio_stage_portfolio_history",
        "pg_temp.portfolio_stage_portfolio_positions",
        "pg_temp.portfolio_stage_portfolio_attribution",
        "pg_temp.portfolio_stage_portfolio_alerts",
    ]
    assert any("DELETE FROM core.portfolio_history AS target" in sql for sql, _ in cursor.executed)
    assert any("DELETE FROM core.portfolio_positions AS target" in sql for sql, _ in cursor.executed)
    assert any("DELETE FROM core.portfolio_attribution AS target" in sql for sql, _ in cursor.executed)
    assert any("DELETE FROM core.portfolio_alerts AS target" in sql for sql, _ in cursor.executed)
    assert all("TRUNCATE TABLE core.portfolio_history" not in sql for sql, _ in cursor.executed)


def test_persist_materialization_rolls_back_when_state_upsert_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    committed_actions: list[tuple[str, object]] = []

    monkeypatch.setattr(
        "core.portfolio_materialization.connect",
        lambda _dsn: _TransactionalConnection(committed_actions),
    )
    monkeypatch.setattr("core.portfolio_materialization._ensure_connection_is_writable", lambda _cur: None)
    monkeypatch.setattr(
        "core.portfolio_materialization._upsert_latest_snapshot",
        lambda cur, **_kwargs: cur.pending.append(("snapshot", "ok")),
    )
    monkeypatch.setattr(
        "core.portfolio_materialization._apply_serving_table",
        lambda cur, *, config, rows, scope_values: cur.pending.append(("table", config.table)),
    )

    def _failing_state(cur, **_kwargs) -> None:
        cur.pending.append(("state", "fail"))
        raise RuntimeError("state failure")

    monkeypatch.setattr("core.portfolio_materialization._upsert_materialization_state", _failing_state)

    with pytest.raises(RuntimeError, match="state failure"):
        _persist_materialization(
            "postgresql://test",
            bundle=_build_bundle(),
            result=_surfaces(),
            dependency_fingerprint="fp-1",
            dependency_state={"sleeveRuns": []},
        )

    assert committed_actions == []


def test_serving_repository_reads_all_portfolio_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("core.portfolio_materialization.connect", lambda _dsn: _ServingConnection())

    repo = PortfolioServingRepository("postgresql://test")
    snapshot = repo.get_latest_snapshot("acct-core")
    history = repo.get_history("acct-core")
    positions = repo.get_positions("acct-core")
    attribution = repo.get_attribution("acct-core")
    alerts = repo.get_alerts("acct-core")

    assert snapshot is not None
    assert snapshot.accountId == "acct-core"
    assert snapshot.slices[0].sleeveId == "sleeve-1"
    assert history[0].nav == 110.0
    assert positions[0].symbol == "AAPL"
    assert attribution[0].strategyName == "alpha"
    assert alerts[0].code == "cash_residual_high"
