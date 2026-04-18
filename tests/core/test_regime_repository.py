from __future__ import annotations

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.regime_repository import RegimeRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_get_active_regime_model_revision_reads_internal_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/regimes/models/default-regime/active"
        return httpx.Response(200, json={"name": "default-regime", "version": 2})

    transport = _build_transport(handler)
    try:
        repo = RegimeRepository(transport=transport)
        result = repo.get_active_regime_model_revision("default-regime")
    finally:
        transport.close()

    assert result == {"name": "default-regime", "version": 2}


def test_list_active_regime_model_revisions_reads_internal_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/regimes/models/active"
        return httpx.Response(200, json=[{"name": "default-regime", "version": 2}])

    transport = _build_transport(handler)
    try:
        repo = RegimeRepository(transport=transport)
        result = repo.list_active_regime_model_revisions()
    finally:
        transport.close()

    assert result == [{"name": "default-regime", "version": 2}]
