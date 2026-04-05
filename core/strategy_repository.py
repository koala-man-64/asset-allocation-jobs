from __future__ import annotations

import json
from typing import Any

from core.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


def normalize_strategy_config_document(config: Any) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {}

    normalized = json.loads(json.dumps(config))

    regime_policy = normalized.get("regimePolicy")
    if isinstance(regime_policy, dict):
        if regime_policy.get("enabled") is False:
            normalized.pop("regimePolicy", None)
        else:
            regime_policy.pop("enabled", None)
            normalized["regimePolicy"] = regime_policy

    raw_exits = normalized.get("exits")
    if isinstance(raw_exits, list):
        cleaned_exits: list[dict[str, Any]] = []
        for raw_rule in raw_exits:
            if not isinstance(raw_rule, dict):
                continue
            if raw_rule.get("enabled") is False:
                continue
            cleaned_rule = dict(raw_rule)
            cleaned_rule.pop("enabled", None)
            cleaned_exits.append(cleaned_rule)
        normalized["exits"] = cleaned_exits

    return normalized


class StrategyRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def get_strategy_config(self, name: str) -> dict[str, Any] | None:
        strategy = self.get_strategy(name)
        if not strategy:
            return None
        return normalize_strategy_config_document(strategy.get("config"))

    def get_strategy(self, name: str) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/strategies/{name}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        if isinstance(payload, dict):
            payload["config"] = normalize_strategy_config_document(payload.get("config"))
            return payload
        return None

    def list_strategies(self) -> list[dict[str, Any]]:
        payload = self.transport.request_json("GET", "/api/internal/strategies")
        return payload if isinstance(payload, list) else []

    def get_strategy_revision(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json(
                "GET",
                f"/api/internal/strategies/{name}/revision",
                params={"version": version} if version is not None else None,
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        if isinstance(payload, dict):
            payload["config"] = normalize_strategy_config_document(payload.get("config"))
            return payload
        return None

    def save_strategy(self, *args, **kwargs) -> None:
        raise NotImplementedError("Jobs repo does not mutate strategy control-plane state directly.")

    def delete_strategy(self, *args, **kwargs) -> bool:
        raise NotImplementedError("Jobs repo does not mutate strategy control-plane state directly.")
