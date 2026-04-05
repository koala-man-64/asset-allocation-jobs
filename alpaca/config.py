from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional, Set

import yaml

_STRICT_ALLOWED_TOP_LEVEL_KEYS: Set[str] = {
    "alpaca",
    "execution",
}

_STRICT_ALLOWED_SECTIONS: Dict[str, Set[str]] = {
    "alpaca": {
        "env",
        "api_key_env",
        "api_secret_env",
        "trading_base_url",
        "trading_ws_url",
        "marketdata_feed",
        "marketdata_ws_url",
        "http",
        "reconcile",
    },
    "execution": {
        "allow_fractional_shares",
        "lot_size",
        "rounding_mode",
        "min_trade_notional",
        "min_trade_shares",
        "participation_cap",
        "default_order_type",
        "time_in_force",
    },
}


def validate_live_config_dict_strict(data: Dict[str, Any]) -> None:
    """
    Best-effort strict validation to catch YAML typos early for Live Config.
    """
    if not isinstance(data, dict):
        raise ValueError("LiveConfig must be an object.")

    unknown_top = set(data.keys()) - _STRICT_ALLOWED_TOP_LEVEL_KEYS
    if unknown_top:
        raise ValueError(f"Unknown top-level config field(s): {sorted(unknown_top)}")

    for section, allowed in _STRICT_ALLOWED_SECTIONS.items():
        if section not in data or data[section] is None:
            continue
        payload = data[section]
        if not isinstance(payload, dict):
            raise ValueError(f"{section} must be an object.")
        unknown = set(payload.keys()) - allowed
        if unknown:
            raise ValueError(f"Unknown {section} field(s): {sorted(unknown)}")


@dataclass(frozen=True)
class HttpConfig:
    timeout_s: float = 10.0
    max_retries: int = 5
    backoff_base_s: float = 0.25

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "HttpConfig":
        return HttpConfig(
            timeout_s=float(data.get("timeout_s", 10.0)),
            max_retries=int(data.get("max_retries", 5)),
            backoff_base_s=float(data.get("backoff_base_s", 0.25)),
        )


@dataclass(frozen=True)
class ReconcileConfig:
    poll_interval_s: float = 30.0
    full_resync_interval_s: float = 300.0

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ReconcileConfig":
        return ReconcileConfig(
            poll_interval_s=float(data.get("poll_interval_s", 30.0)),
            full_resync_interval_s=float(data.get("full_resync_interval_s", 300.0)),
        )


@dataclass(frozen=True)
class AlpacaConfig:
    env: Literal["paper", "live"]
    api_key_env: str
    api_secret_env: str
    trading_base_url: Optional[str] = None
    trading_ws_url: Optional[str] = None
    marketdata_feed: Literal["v2/iex", "v2/sip", "v2/delayed_sip"] = "v2/iex"
    marketdata_ws_url: Optional[str] = None
    http: HttpConfig = field(default_factory=HttpConfig)
    reconcile: ReconcileConfig = field(default_factory=ReconcileConfig)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "AlpacaConfig":
        env = data.get("env")
        if env not in {"paper", "live"}:
            raise ValueError("alpaca.env must be 'paper' or 'live'.")

        marketdata_feed = data.get("marketdata_feed", "v2/iex")
        if marketdata_feed not in {"v2/iex", "v2/sip", "v2/delayed_sip"}:
            raise ValueError("alpaca.marketdata_feed must be 'v2/iex', 'v2/sip', or 'v2/delayed_sip'.")

        return AlpacaConfig(
            env=env,  # type: ignore
            api_key_env=str(data.get("api_key_env", "ALPACA_KEY_ID")),
            api_secret_env=str(data.get("api_secret_env", "ALPACA_SECRET_KEY")),
            trading_base_url=data.get("trading_base_url"),
            trading_ws_url=data.get("trading_ws_url"),
            marketdata_feed=marketdata_feed,  # type: ignore
            marketdata_ws_url=data.get("marketdata_ws_url"),
            http=HttpConfig.from_dict(data.get("http") or {}),
            reconcile=ReconcileConfig.from_dict(data.get("reconcile") or {}),
        )

    def get_api_key(self) -> str:
        val = os.environ.get(self.api_key_env)
        if not val:
            raise ValueError(f"Environment variable {self.api_key_env} is not set.")
        return val

    def get_api_secret(self) -> str:
        val = os.environ.get(self.api_secret_env)
        if not val:
            raise ValueError(f"Environment variable {self.api_secret_env} is not set.")
        return val

    def get_trading_base_url(self) -> str:
        if self.trading_base_url:
            return self.trading_base_url
        return "https://paper-api.alpaca.markets" if self.env == "paper" else "https://api.alpaca.markets"

    def get_trading_ws_url(self) -> str:
        if self.trading_ws_url:
            return self.trading_ws_url
        return "wss://paper-api.alpaca.markets/stream" if self.env == "paper" else "wss://api.alpaca.markets/stream"


@dataclass(frozen=True)
class ExecutionConfig:
    allow_fractional_shares: bool = True
    lot_size: int = 1
    rounding_mode: Literal["toward_zero", "nearest", "floor", "ceil"] = "toward_zero"
    min_trade_notional: float = 5.0
    min_trade_shares: float = 0.0
    participation_cap: Optional[float] = None
    default_order_type: Literal["market", "limit"] = "market"
    time_in_force: Literal["day", "gtc", "opg"] = "day"

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ExecutionConfig":
        rounding_mode = data.get("rounding_mode", "toward_zero")
        if rounding_mode not in {"toward_zero", "nearest", "floor", "ceil"}:
            raise ValueError("execution.rounding_mode must be toward_zero, nearest, floor, or ceil.")
        
        default_order_type = data.get("default_order_type", "market")
        if default_order_type not in {"market", "limit"}:
             raise ValueError("execution.default_order_type must be market or limit.")

        time_in_force = data.get("time_in_force", "day")
        # Simple validation for now, Alpaca supports more
        if time_in_force not in {"day", "gtc", "opg", "ioc", "fok", "cls"}:
             pass 

        return ExecutionConfig(
            allow_fractional_shares=bool(data.get("allow_fractional_shares", True)),
            lot_size=int(data.get("lot_size", 1)),
            rounding_mode=rounding_mode, # type: ignore
            min_trade_notional=float(data.get("min_trade_notional", 5.0)),
            min_trade_shares=float(data.get("min_trade_shares", 0.0)),
            participation_cap=float(data["participation_cap"]) if data.get("participation_cap") else None,
            default_order_type=default_order_type, # type: ignore
            time_in_force=time_in_force, # type: ignore
        )


@dataclass(frozen=True)
class LiveConfig:
    alpaca: AlpacaConfig
    execution: ExecutionConfig

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "LiveConfig":
        if not isinstance(data, dict):
            raise ValueError("LiveConfig must be an object.")
        
        return LiveConfig(
            alpaca=AlpacaConfig.from_dict(data.get("alpaca") or {}),
            execution=ExecutionConfig.from_dict(data.get("execution") or {}),
        )

    @staticmethod
    def from_yaml(path: str, *, strict: bool = False) -> "LiveConfig":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        
        if strict:
            validate_live_config_dict_strict(data)
            
        return LiveConfig.from_dict(data)
