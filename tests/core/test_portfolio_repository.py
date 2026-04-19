from __future__ import annotations

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig

from core.portfolio_contracts import PortfolioSnapshot
from core.portfolio_repository import PortfolioMaterializationRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="http://asset-allocation-api-vnet", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_claim_next_materialization_unwraps_work_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/internal/portfolio-materializations/claim"
        return httpx.Response(200, json={"work": {"accountId": "acct-core", "claimToken": "claim-1"}})

    transport = _build_transport(handler)
    try:
        repo = PortfolioMaterializationRepository(transport=transport)
        work = repo.claim_next_materialization(execution_name="portfolio-job-1")
    finally:
        transport.close()

    assert work is not None
    assert work.account_id == "acct-core"
    assert work.claim_token == "claim-1"


def test_materialization_lifecycle_calls_expected_internal_paths() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.path.endswith("/bundle"):
            return httpx.Response(
                200,
                json={
                    "account": {
                        "accountId": "acct-core",
                        "name": "Core",
                        "description": "",
                        "status": "active",
                        "mode": "internal_model_managed",
                        "accountingDepth": "position_level",
                        "cadenceMode": "strategy_native",
                        "baseCurrency": "USD",
                        "inceptionDate": "2026-01-02",
                    },
                    "ledgerEvents": [],
                    "alerts": [],
                    "freshness": [],
                },
            )
        return httpx.Response(200, json={})

    transport = _build_transport(handler)
    snapshot = PortfolioSnapshot(
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
    )
    try:
        repo = PortfolioMaterializationRepository(transport=transport)
        repo.start_materialization("acct-core", claim_token="claim-1", execution_name="job-7")
        repo.update_heartbeat("acct-core", claim_token="claim-1")
        repo.get_materialization_bundle("acct-core", claim_token="claim-1")
        repo.complete_materialization(
            "acct-core",
            claim_token="claim-1",
            snapshot=snapshot,
            history=[],
            positions=[],
            alerts=[],
        )
        repo.fail_materialization("acct-core", claim_token="claim-1", error="boom")
    finally:
        transport.close()

    assert calls == [
        ("POST", "/api/internal/portfolio-materializations/accounts/acct-core/start"),
        ("POST", "/api/internal/portfolio-materializations/accounts/acct-core/heartbeat"),
        ("GET", "/api/internal/portfolio-materializations/accounts/acct-core/bundle"),
        ("POST", "/api/internal/portfolio-materializations/accounts/acct-core/complete"),
        ("POST", "/api/internal/portfolio-materializations/accounts/acct-core/fail"),
    ]


def test_get_materialization_bundle_tolerates_missing_optional_lists() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(
            200,
            json={
                "account": {
                    "accountId": "acct-core",
                    "name": "Core",
                    "description": "",
                    "status": "active",
                    "mode": "internal_model_managed",
                    "accountingDepth": "position_level",
                    "cadenceMode": "strategy_native",
                    "baseCurrency": "USD",
                    "inceptionDate": "2026-01-02",
                },
                "ledgerEvents": None,
                "alerts": None,
                "freshness": None,
                "strategyDependencies": None,
            },
        )

    transport = _build_transport(handler)
    try:
        repo = PortfolioMaterializationRepository(transport=transport)
        bundle = repo.get_materialization_bundle("acct-core", claim_token="claim-1")
    finally:
        transport.close()

    assert bundle.account.accountId == "acct-core"
    assert bundle.ledger_events == ()
    assert bundle.alerts == ()
    assert bundle.freshness == ()
    assert bundle.strategy_dependencies == ()
