from __future__ import annotations

import json

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client


def _parse_window_assignment(body: str, window_key: str) -> dict:
    prefix = f"window.{window_key} ="
    for line in body.splitlines():
        if not line.startswith(prefix):
            continue
        payload = line.split("=", 1)[1].strip()
        if payload.endswith(";"):
            payload = payload[:-1].strip()
        return json.loads(payload)
    raise AssertionError(f"Missing {prefix} assignment in /config.js response.")


@pytest.mark.asyncio
async def test_config_js_emits_fixed_api_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/config.js")

    assert resp.status_code == 200
    assert "application/javascript" in resp.headers.get("content-type", "")
    assert "no-store" in resp.headers.get("cache-control", "").lower()

    cfg = _parse_window_assignment(resp.text, "__API_UI_CONFIG__")

    assert "window.__BACKTEST_UI_CONFIG__" not in resp.text
    assert cfg["apiBaseUrl"] == "/api"
    assert cfg["oidcRedirectUri"] is None
    assert "backtestApiBaseUrl" not in cfg
    assert "apiKeyAuthConfigured" not in cfg


@pytest.mark.asyncio
async def test_config_js_ignores_ui_api_base_url_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")
    monkeypatch.setenv("UI_API_BASE_URL", "/api")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/config.js")

    assert resp.status_code == 200
    cfg = _parse_window_assignment(resp.text, "__API_UI_CONFIG__")
    assert cfg["apiBaseUrl"] == "/api"
    assert "backtestApiBaseUrl" not in cfg


@pytest.mark.asyncio
async def test_config_js_preserves_explicit_oidc_redirect_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")
    monkeypatch.setenv("UI_OIDC_CLIENT_ID", "spa-client-id")
    monkeypatch.setenv("UI_OIDC_AUTHORITY", "https://login.microsoftonline.com/tenant-id")
    monkeypatch.setenv("UI_OIDC_SCOPES", "api://asset-allocation-api/user_impersonation")
    monkeypatch.setenv(
        "UI_OIDC_REDIRECT_URI",
        "https://asset-allocation.example.com/auth/callback",
    )

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/config.js")

    assert resp.status_code == 200
    cfg = _parse_window_assignment(resp.text, "__API_UI_CONFIG__")
    assert cfg["oidcEnabled"] is True
    assert cfg["authRequired"] is True
    assert cfg["oidcClientId"] == "spa-client-id"
    assert cfg["oidcAuthority"] == "https://login.microsoftonline.com/tenant-id"
    assert cfg["oidcScopes"] == "api://asset-allocation-api/user_impersonation"
    assert cfg["oidcRedirectUri"] == "https://asset-allocation.example.com/auth/callback"

