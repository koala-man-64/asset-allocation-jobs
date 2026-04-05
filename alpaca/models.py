from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

@dataclass
class AlpacaAccount:
    id: str
    account_number: str
    status: str
    currency: str
    cash: float
    equity: float
    buying_power: float
    daytrade_count: int
    created_at: datetime
    # Add other fields as needed, but keep it focused

    @staticmethod
    def from_api_dict(data: Dict[str, Any]) -> "AlpacaAccount":
        return AlpacaAccount(
            id=data["id"],
            account_number=data["account_number"],
            status=data["status"],
            currency=data["currency"],
            cash=float(data["cash"]),
            equity=float(data["equity"]),
            buying_power=float(data["buying_power"]),
            daytrade_count=int(data.get("daytrade_count", 0)),
            created_at=datetime.fromisoformat(data["created_at"].replace("Z", "+00:00")),
        )


@dataclass
class AlpacaPosition:
    symbol: str
    qty: float
    market_value: float
    avg_entry_price: float
    current_price: float
    change_today: float
    unrealized_pl: float
    side: Literal["long", "short"]

    @staticmethod
    def from_api_dict(data: Dict[str, Any]) -> "AlpacaPosition":
        qty = float(data["qty"])
        return AlpacaPosition(
            symbol=data["symbol"],
            qty=qty,
            market_value=float(data["market_value"]),
            avg_entry_price=float(data["avg_entry_price"]),
            current_price=float(data["current_price"]),
            change_today=float(data["change_today"]),
            unrealized_pl=float(data["unrealized_pl"]),
            side="long" if qty >= 0 else "short",
        )


@dataclass
class AlpacaOrder:
    id: str
    client_order_id: str
    symbol: str
    created_at: datetime
    updated_at: datetime
    submitted_at: datetime
    filled_at: Optional[datetime]
    expired_at: Optional[datetime]
    canceled_at: Optional[datetime]
    failed_at: Optional[datetime]
    asset_id: str
    asset_class: str
    qty: float
    filled_qty: float
    type: str # market, limit, etc.
    side: str # buy, sell
    time_in_force: str
    limit_price: Optional[float]
    stop_price: Optional[float]
    status: str # new, filled, canceled, etc.

    @staticmethod
    def from_api_dict(data: Dict[str, Any]) -> "AlpacaOrder":
        def parse_dt(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            return datetime.fromisoformat(s.replace("Z", "+00:00"))

        return AlpacaOrder(
            id=data["id"],
            client_order_id=data["client_order_id"],
            symbol=data["symbol"],
            created_at=parse_dt(data["created_at"]), # type: ignore
            updated_at=parse_dt(data["updated_at"]), # type: ignore
            submitted_at=parse_dt(data["submitted_at"]), # type: ignore
            filled_at=parse_dt(data.get("filled_at")),
            expired_at=parse_dt(data.get("expired_at")),
            canceled_at=parse_dt(data.get("canceled_at")),
            failed_at=parse_dt(data.get("failed_at")),
            asset_id=data["asset_id"],
            asset_class=data["asset_class"],
            qty=float(data.get("qty", 0.0)),
            filled_qty=float(data.get("filled_qty", 0.0)),
            type=data["type"],
            side=data["side"],
            time_in_force=data["time_in_force"],
            limit_price=float(data["limit_price"]) if data.get("limit_price") else None,
            stop_price=float(data["stop_price"]) if data.get("stop_price") else None,
            status=data["status"],
        )


@dataclass
class TradeUpdateEvent:
    event: str  # new, fill, partial_fill, canceled, etc.
    price: Optional[float]
    qty: Optional[float]
    timestamp: datetime
    order: AlpacaOrder
    execution_id: Optional[str] = None
    position_qty: Optional[float] = None


@dataclass
class PositionStateLike:
    """Matches the shape of internal backtest PositionState for compatibility."""
    symbol: str
    shares: float
    avg_entry_price: float
    entry_date: datetime
    last_fill_date: Optional[datetime]


@dataclass
class BrokerageState:
    account: AlpacaAccount
    positions: Dict[str, AlpacaPosition]
    open_orders: Dict[str, AlpacaOrder]
    position_states: Dict[str, PositionStateLike]
    last_update: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 0
