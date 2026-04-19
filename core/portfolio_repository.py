from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport

from core.portfolio_contracts import (
    FreshnessStatus,
    PortfolioAccount,
    PortfolioAccountRevision,
    PortfolioAlert,
    PortfolioAssignment,
    PortfolioDefinition,
    PortfolioHistoryPoint,
    PortfolioLedgerEvent,
    PortfolioPosition,
    PortfolioRevision,
    PortfolioSnapshot,
)


def _parse_date(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


@dataclass(frozen=True)
class PortfolioMaterializationWorkItem:
    account_id: str
    claim_token: str
    dependency_fingerprint: str | None = None
    dependency_state: dict[str, Any] = field(default_factory=dict)
    as_of: date | None = None


@dataclass(frozen=True)
class PortfolioMaterializationBundle:
    account: PortfolioAccount
    account_revision: PortfolioAccountRevision | None
    active_assignment: PortfolioAssignment | None
    portfolio: PortfolioDefinition | None
    portfolio_revision: PortfolioRevision | None
    ledger_events: tuple[PortfolioLedgerEvent, ...]
    alerts: tuple[PortfolioAlert, ...]
    freshness: tuple[FreshnessStatus, ...]
    dependency_fingerprint: str | None = None
    dependency_state: dict[str, Any] = field(default_factory=dict)
    as_of: date | None = None
    claim_token: str | None = None
    strategy_dependencies: tuple[dict[str, Any], ...] = ()


class PortfolioMaterializationRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def claim_next_materialization(self, *, execution_name: str | None = None) -> PortfolioMaterializationWorkItem | None:
        payload = self.transport.request_json(
            "POST",
            "/api/internal/portfolio-materializations/claim",
            json_body={"executionName": execution_name},
        )
        work = payload.get("work") if isinstance(payload, dict) else None
        if not isinstance(work, dict):
            return None
        account_id = str(work.get("accountId") or "").strip()
        claim_token = str(work.get("claimToken") or "").strip()
        if not account_id or not claim_token:
            return None
        dependency_state = work.get("dependencyState")
        return PortfolioMaterializationWorkItem(
            account_id=account_id,
            claim_token=claim_token,
            dependency_fingerprint=str(work.get("dependencyFingerprint") or "").strip() or None,
            dependency_state=dict(dependency_state) if isinstance(dependency_state, dict) else {},
            as_of=_parse_date(work.get("asOf")),
        )

    def get_materialization_bundle(
        self,
        account_id: str,
        *,
        claim_token: str | None = None,
    ) -> PortfolioMaterializationBundle:
        params = {"claimToken": claim_token} if claim_token else None
        payload = self.transport.request_json(
            "GET",
            f"/api/internal/portfolio-materializations/accounts/{account_id}/bundle",
            params=params,
        )
        if not isinstance(payload, dict):
            raise ValueError("Portfolio materialization bundle response was not a JSON object.")

        account = PortfolioAccount.model_validate(payload.get("account") or {})
        account_revision = payload.get("accountRevision")
        active_assignment = payload.get("activeAssignment")
        portfolio = payload.get("portfolio")
        portfolio_revision = payload.get("portfolioRevision")
        ledger_events = payload.get("ledgerEvents")
        alerts = payload.get("alerts")
        freshness = payload.get("freshness")
        strategy_dependencies = payload.get("strategyDependencies")
        dependency_state = payload.get("dependencyState")

        return PortfolioMaterializationBundle(
            account=account,
            account_revision=(
                PortfolioAccountRevision.model_validate(account_revision)
                if isinstance(account_revision, dict)
                else None
            ),
            active_assignment=(
                PortfolioAssignment.model_validate(active_assignment)
                if isinstance(active_assignment, dict)
                else None
            ),
            portfolio=PortfolioDefinition.model_validate(portfolio) if isinstance(portfolio, dict) else None,
            portfolio_revision=(
                PortfolioRevision.model_validate(portfolio_revision)
                if isinstance(portfolio_revision, dict)
                else None
            ),
            ledger_events=tuple(
                PortfolioLedgerEvent.model_validate(item)
                for item in (ledger_events if isinstance(ledger_events, list) else [])
                if isinstance(item, dict)
            ),
            alerts=tuple(
                PortfolioAlert.model_validate(item)
                for item in (alerts if isinstance(alerts, list) else [])
                if isinstance(item, dict)
            ),
            freshness=tuple(
                FreshnessStatus.model_validate(item)
                for item in (freshness if isinstance(freshness, list) else [])
                if isinstance(item, dict)
            ),
            dependency_fingerprint=str(payload.get("dependencyFingerprint") or "").strip() or None,
            dependency_state=dict(dependency_state) if isinstance(dependency_state, dict) else {},
            as_of=_parse_date(payload.get("asOf")),
            claim_token=claim_token,
            strategy_dependencies=tuple(
                item
                for item in (strategy_dependencies if isinstance(strategy_dependencies, list) else [])
                if isinstance(item, dict)
            ),
        )

    def start_materialization(
        self,
        account_id: str,
        *,
        claim_token: str,
        execution_name: str | None = None,
    ) -> None:
        self.transport.request_json(
            "POST",
            f"/api/internal/portfolio-materializations/accounts/{account_id}/start",
            json_body={"claimToken": claim_token, "executionName": execution_name},
        )

    def update_heartbeat(self, account_id: str, *, claim_token: str) -> None:
        self.transport.request_json(
            "POST",
            f"/api/internal/portfolio-materializations/accounts/{account_id}/heartbeat",
            json_body={"claimToken": claim_token},
        )

    def complete_materialization(
        self,
        account_id: str,
        *,
        claim_token: str,
        snapshot: PortfolioSnapshot,
        history: list[PortfolioHistoryPoint],
        positions: list[PortfolioPosition],
        alerts: list[PortfolioAlert],
        dependency_fingerprint: str | None = None,
        dependency_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self.transport.request_json(
            "POST",
            f"/api/internal/portfolio-materializations/accounts/{account_id}/complete",
            json_body={
                "claimToken": claim_token,
                "dependencyFingerprint": dependency_fingerprint,
                "dependencyState": dependency_state,
                "snapshot": snapshot.model_dump(mode="json"),
                "history": [point.model_dump(mode="json") for point in history],
                "positions": [position.model_dump(mode="json") for position in positions],
                "alerts": [alert.model_dump(mode="json") for alert in alerts],
            },
        )
        return payload if isinstance(payload, dict) else {}

    def fail_materialization(self, account_id: str, *, claim_token: str, error: str) -> dict[str, Any]:
        payload = self.transport.request_json(
            "POST",
            f"/api/internal/portfolio-materializations/accounts/{account_id}/fail",
            json_body={"claimToken": claim_token, "error": error},
        )
        return payload if isinstance(payload, dict) else {}

    def probe_ready(self) -> None:
        self.transport.probe("/api/internal/portfolio-materializations/ready")

    def get_account(self, account_id: str) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/portfolio-accounts/{account_id}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None


__all__ = [
    "PortfolioMaterializationBundle",
    "PortfolioMaterializationRepository",
    "PortfolioMaterializationWorkItem",
]
