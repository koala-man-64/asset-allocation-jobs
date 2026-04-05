from __future__ import annotations

from typing import Any

from core.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


class RankingRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def get_ranking_schema(self, name: str) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/rankings/{name}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def list_ranking_schemas(self) -> list[dict[str, Any]]:
        payload = self.transport.request_json("GET", "/api/internal/rankings")
        return payload if isinstance(payload, list) else []

    def get_ranking_schema_revision(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json(
                "GET",
                f"/api/internal/rankings/{name}/revision",
                params={"version": version} if version is not None else None,
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def save_ranking_schema(self, *args, **kwargs) -> None:
        raise NotImplementedError("Jobs repo does not mutate ranking control-plane state directly.")

    def delete_ranking_schema(self, *args, **kwargs) -> bool:
        raise NotImplementedError("Jobs repo does not mutate ranking control-plane state directly.")
