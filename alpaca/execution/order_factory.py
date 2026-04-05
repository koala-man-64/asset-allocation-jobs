import hashlib
from typing import Dict, Any

from alpaca.config import ExecutionConfig
from alpaca.execution.rebalance_planner import PlannedOrder

class OrderFactory:
    def __init__(self, config: ExecutionConfig):
        self._config = config

    def create_order_payload(
        self,
        plan_order: PlannedOrder,
        strategy_id: str,
        rebalance_id: str
    ) -> Dict[str, Any]:
        """
        Create dictionary for submit_order.
        """
        # Deterministic Client Order ID
        # Format: {strategy}-{rebalance}-{symbol}-{side} truncated if needed?
        # Alpaca client_order_id max length is 48.
        # We can hash it if too long, or use readable structure.
        
        raw_id = f"{strategy_id}|{rebalance_id}|{plan_order.symbol}|{plan_order.side}"
        if len(raw_id) <= 48:
            client_oid = raw_id
        else:
            # Hash suffix
            h = hashlib.md5(raw_id.encode()).hexdigest()[:12]
            # Try to keep some readable parts: symbol-side-hash
            prefix = f"{plan_order.symbol}-{plan_order.side}"
            client_oid = f"{prefix}-{h}"[:48]

        return {
            "symbol": plan_order.symbol,
            "qty": plan_order.qty,
            "side": plan_order.side,
            "type": self._config.default_order_type,
            "time_in_force": self._config.time_in_force,
            "client_order_id": client_oid,
            # We could add limit_price here if limit orders are supported
        }
