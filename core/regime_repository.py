from __future__ import annotations

from typing import Any

from core.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


class RegimeRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def get_regime_model_revision(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json(
                "GET",
                f"/api/internal/regimes/models/{name}/revision",
                params={"version": version} if version is not None else None,
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def get_active_regime_model_revision(self, name: str) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/regimes/models/{name}/active")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def list_active_regime_model_revisions(self) -> list[dict[str, Any]]:
        payload = self.transport.request_json("GET", "/api/internal/regimes/models/active")
        return payload if isinstance(payload, list) else []

    def get_regime_latest(self, *, model_name: str, model_version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json(
                "GET",
                "/api/internal/regimes/current",
                params={
                    "modelName": model_name,
                    "modelVersion": model_version,
                },
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def save_regime_model(self, *args, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("Jobs repo does not mutate regime control-plane state directly.")
