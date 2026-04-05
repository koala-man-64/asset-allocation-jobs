from __future__ import annotations

import httpx

from core.backtest_repository import BacktestRepository
from core.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_claim_next_run_unwraps_run_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/internal/backtests/runs/claim"
        assert request.content.decode("utf-8") == '{"executionName":"worker-01"}'
        return httpx.Response(200, json={"run": {"run_id": "run-123", "status": "queued"}})

    transport = _build_transport(handler)
    try:
        repo = BacktestRepository(transport=transport)
        result = repo.claim_next_run(execution_name="worker-01")
    finally:
        transport.close()

    assert result == {"run_id": "run-123", "status": "queued"}


def test_run_lifecycle_calls_expected_internal_paths() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        return httpx.Response(204, content=b"")

    transport = _build_transport(handler)
    try:
        repo = BacktestRepository(transport=transport)
        repo.start_run("run-123", execution_name="worker-01")
        repo.update_heartbeat("run-123")
        repo.complete_run("run-123", summary={"sharpe": 1.2}, artifact_manifest_path="backtests/run-123/manifest.json")
        repo.fail_run("run-123", error="boom")
    finally:
        transport.close()

    assert calls == [
        ("POST", "/api/internal/backtests/runs/run-123/start"),
        ("POST", "/api/internal/backtests/runs/run-123/heartbeat"),
        ("POST", "/api/internal/backtests/runs/run-123/complete"),
        ("POST", "/api/internal/backtests/runs/run-123/fail"),
    ]
