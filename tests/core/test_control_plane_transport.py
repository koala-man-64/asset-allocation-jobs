from __future__ import annotations

import httpx
import pytest

from asset_allocation_runtime_common.control_plane_transport import (
    ControlPlaneRequestError,
    ControlPlaneTransport,
    ControlPlaneTransportConfig,
)


def test_transport_normalizes_base_url_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api-vnet/api/")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://asset-allocation")

    transport = ControlPlaneTransport.from_env()

    try:
        assert transport.config.base_url == "http://asset-allocation-api-vnet"
        assert transport.config.api_scope == "api://asset-allocation"
    finally:
        transport.close()


def test_transport_adds_auth_and_caller_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "rankings-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "rankings-job-7")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.headers["X-Caller-Job"] == "rankings-job"
        assert request.headers["X-Caller-Execution"] == "rankings-job-7"
        assert request.url.path == "/api/internal/strategies"
        return httpx.Response(200, json=[{"name": "momentum"}])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    transport = ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="http://asset-allocation-api-vnet", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )

    try:
        payload = transport.request_json("GET", "/api/internal/strategies")
    finally:
        transport.close()

    assert payload == [{"name": "momentum"}]


def test_transport_probe_uses_authenticated_get_request() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        assert request.headers["Authorization"] == "Bearer test-token"
        return httpx.Response(204, content=b"")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    transport = ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="http://asset-allocation-api-vnet", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )

    try:
        assert transport.probe("/api/internal/backtests/ready") is None
    finally:
        transport.close()

    assert calls == [("GET", "/api/internal/backtests/ready")]


def test_transport_raises_control_plane_request_error_with_detail() -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(503, json={"detail": "Upstream unavailable"}))
    )
    transport = ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="http://asset-allocation-api-vnet", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )

    try:
        with pytest.raises(ControlPlaneRequestError) as exc_info:
            transport.request_json("GET", "/api/internal/regimes/current")
    finally:
        transport.close()

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Upstream unavailable"


def test_transport_returns_none_for_empty_response_body() -> None:
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(204, content=b"")))
    transport = ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="http://asset-allocation-api-vnet", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )

    try:
        assert transport.request_json("POST", "/api/internal/backtests/runs/run-1/heartbeat") is None
    finally:
        transport.close()
