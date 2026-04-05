from __future__ import annotations

from typing import Any

from core.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


class UniverseRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def get_universe_config(self, name: str) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/universes/{name}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def get_universe_config_revision(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json(
                "GET",
                f"/api/internal/universes/{name}/revision",
                params={"version": version} if version is not None else None,
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def list_universe_configs(self) -> list[dict[str, Any]]:
        raise NotImplementedError("Jobs repo does not list universe configs directly.")

    def save_universe_config(self, *args, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("Jobs repo does not mutate universe control-plane state directly.")

    def delete_universe_config(self, *args, **kwargs) -> bool:
        raise NotImplementedError("Jobs repo does not mutate universe control-plane state directly.")
