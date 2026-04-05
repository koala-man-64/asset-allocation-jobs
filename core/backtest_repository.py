from __future__ import annotations

from typing import Any

from core.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


class BacktestRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/backtests/runs/{run_id}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def claim_next_run(self, *, execution_name: str | None = None) -> dict[str, Any] | None:
        payload = self.transport.request_json(
            "POST",
            "/api/internal/backtests/runs/claim",
            json_body={"executionName": execution_name},
        )
        if isinstance(payload, dict):
            run = payload.get("run")
            if isinstance(run, dict):
                return run
        return None

    def update_heartbeat(self, run_id: str) -> None:
        self.transport.request_json("POST", f"/api/internal/backtests/runs/{run_id}/heartbeat")

    def start_run(self, run_id: str, *, execution_name: str | None = None) -> None:
        self.transport.request_json(
            "POST",
            f"/api/internal/backtests/runs/{run_id}/start",
            json_body={"executionName": execution_name},
        )

    def complete_run(
        self,
        run_id: str,
        *,
        summary: dict[str, Any] | None = None,
        artifact_manifest_path: str | None = None,
    ) -> None:
        self.transport.request_json(
            "POST",
            f"/api/internal/backtests/runs/{run_id}/complete",
            json_body={
                "summary": summary or {},
                "artifactManifestPath": artifact_manifest_path,
            },
        )

    def fail_run(self, run_id: str, *, error: str) -> None:
        self.transport.request_json(
            "POST",
            f"/api/internal/backtests/runs/{run_id}/fail",
            json_body={"error": error},
        )
