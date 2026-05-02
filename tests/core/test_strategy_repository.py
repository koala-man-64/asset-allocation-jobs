from __future__ import annotations

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.strategy_engine import StrategyConfig
from asset_allocation_runtime_common.strategy_repository import StrategyRepository, normalize_strategy_config_document


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="http://asset-allocation-api-vnet", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_normalize_strategy_config_document_removes_disabled_structures() -> None:
    normalized = normalize_strategy_config_document(
        {
            "rebalance": "monthly",
            "regimePolicy": {
                "enabled": False,
                "modelName": "legacy-regime",
            },
            "exits": [
                {"enabled": False, "kind": "stop_loss"},
                {"enabled": True, "kind": "take_profit", "threshold": 0.1},
            ],
        }
    )

    assert "regimePolicy" not in normalized
    assert normalized["exits"] == [{"kind": "take_profit", "threshold": 0.1}]


def test_get_strategy_reads_http_detail_and_normalizes_config() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/internal/strategies/momentum"
        assert request.headers["Authorization"] == "Bearer test-token"
        return httpx.Response(
            200,
            json={
                "name": "momentum",
                "type": "configured",
                "config": {
                    "rebalance": "monthly",
                    "regimePolicy": {"enabled": True, "modelName": "steady"},
                    "exits": [
                        {"enabled": False, "kind": "stop_loss"},
                        {"enabled": True, "kind": "take_profit", "threshold": 0.2},
                    ],
                },
            },
        )

    transport = _build_transport(handler)
    try:
        repo = StrategyRepository(transport=transport)
        result = repo.get_strategy("momentum")
    finally:
        transport.close()

    assert result == {
        "name": "momentum",
        "type": "configured",
        "config": {
            "rebalance": "monthly",
            "regimePolicy": {"modelName": "steady"},
            "exits": [{"kind": "take_profit", "threshold": 0.2}],
        },
    }


def test_get_strategy_returns_none_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "Strategy 'missing' not found."})

    transport = _build_transport(handler)
    try:
        repo = StrategyRepository(transport=transport)
        assert repo.get_strategy("missing") is None
    finally:
        transport.close()


def test_get_strategy_revision_passes_version_query_param() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/strategies/momentum/revision"
        assert request.url.params["version"] == "4"
        return httpx.Response(200, json={"name": "momentum", "version": 4, "config": {"rebalance": "weekly"}})

    transport = _build_transport(handler)
    try:
        repo = StrategyRepository(transport=transport)
        result = repo.get_strategy_revision("momentum", version=4)
    finally:
        transport.close()

    assert result == {"name": "momentum", "version": 4, "config": {"rebalance": "weekly"}}


def test_get_strategy_revision_preserves_pins_and_resolved_snapshots() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/strategies/momentum/revision"
        return httpx.Response(
            200,
            json={
                "name": "momentum",
                "version": 8,
                "ranking_schema_name": "quality-ranking",
                "ranking_schema_version": 7,
                "universe_name": "large-cap-quality",
                "universe_version": 5,
                "regime_policy_name": "observe-default",
                "regime_policy_version": 2,
                "risk_policy_name": "balanced-risk",
                "risk_policy_version": 4,
                "exit_rule_set_name": "standard-exits",
                "exit_rule_set_version": 6,
                "config": {
                    "universeConfigName": "large-cap-quality",
                    "universeConfigVersion": 5,
                    "rankingSchemaName": "quality-ranking",
                    "rankingSchemaVersion": 7,
                    "regimePolicyConfigName": "observe-default",
                    "regimePolicyConfigVersion": 2,
                    "regimePolicy": {"modelName": "default-regime", "modelVersion": 3, "mode": "observe_only"},
                    "riskPolicyName": "balanced-risk",
                    "riskPolicyVersion": 4,
                    "strategyRiskPolicy": {
                        "scope": "strategy",
                        "stopLoss": {"thresholdPct": 8, "action": "reduce_exposure", "reductionPct": 50},
                    },
                    "exitRuleSetName": "standard-exits",
                    "exitRuleSetVersion": 6,
                    "intrabarConflictPolicy": "priority_order",
                    "exits": [{"id": "stop-8", "type": "stop_loss_fixed", "value": 0.08}],
                },
            },
        )

    transport = _build_transport(handler)
    try:
        repo = StrategyRepository(transport=transport)
        result = repo.get_strategy_revision("momentum")
    finally:
        transport.close()

    assert result is not None
    assert result["config"]["rankingSchemaVersion"] == 7
    assert result["config"]["regimePolicy"]["modelVersion"] == 3
    strategy_config = StrategyConfig.model_validate(result["config"])
    assert strategy_config.universeConfigVersion == 5
    assert strategy_config.regimePolicy is not None
    assert strategy_config.regimePolicy.modelVersion == 3
    assert strategy_config.exits[0].id == "stop-8"


def test_mutating_methods_are_blocked() -> None:
    transport = _build_transport(lambda request: httpx.Response(200, json={}))
    try:
        repo = StrategyRepository(transport=transport)
        for method in (repo.save_strategy, repo.delete_strategy):
            try:
                method("momentum")
            except NotImplementedError as exc:
                assert "does not mutate" in str(exc)
            else:
                raise AssertionError("Expected NotImplementedError for jobs-side mutation method.")
    finally:
        transport.close()
