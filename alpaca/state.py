import logging
from datetime import datetime, timezone
from typing import List

from alpaca.models import (
    BrokerageState, 
    AlpacaAccount, 
    AlpacaPosition, 
    AlpacaOrder, 
    TradeUpdateEvent
)

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

class StateManager:
    def __init__(self, initial_state: BrokerageState):
        self._state = initial_state

    @property
    def state(self) -> BrokerageState:
        return self._state

    def update_account(self, account: AlpacaAccount):
        self._state.account = account
        self._state.last_update = _utc_now()
        self._state.version += 1

    def update_positions(self, positions: List[AlpacaPosition]):
        # Replace entire map or merge? Usually replace on full sync.
        new_map = {p.symbol: p for p in positions}
        self._state.positions = new_map
        self._state.last_update = _utc_now()
        self._state.version += 1
        
        # Also need to reconcile position_states? 
        # Ideally yes, but that requires history. We might just sync what we can.

    def update_open_orders(self, orders: List[AlpacaOrder]):
        new_map = {o.id: o for o in orders}
        self._state.open_orders = new_map
        self._state.last_update = _utc_now()
        self._state.version += 1

    def apply_trade_event(self, event: TradeUpdateEvent):
        """
        Apply a streaming event to the local state.
        This allows low-latency state updates between polls.
        """
        order = event.order
        
        # 1. Update Order State
        if event.event in ("new", "accepted", "pending_new"):
            self._state.open_orders[order.id] = order
        elif event.event in ("filled", "canceled", "expired", "rejected", "suspended"):
            # Remove from open orders if it was there
            if order.id in self._state.open_orders:
                # If filled, it's done. If partial_fill, it stays open (logic below).
                # Wait, 'filled' means completely filled.
                if event.event == "filled" or event.event == "canceled" or event.event == "expired" or event.event == "rejected":
                     del self._state.open_orders[order.id]
        elif event.event == "partial_fill":
            # Update the order in open_orders with new filled_qty
            self._state.open_orders[order.id] = order

        # 2. Update Position State (Approximate)
        # We rely on REST sync for authoritative match, but we can patch it here.
        if event.event in ("filled", "partial_fill"):
            symbol = order.symbol
            filled_price = event.price
            
            # This is complex to do perfectly without a full ledger, 
            # so usually we might wait for position push or rely on 'position_qty' if sent.
            if event.position_qty is not None:
                # Update position object
                current_pos = self._state.positions.get(symbol)
                if current_pos:
                     # We only know qty, not the new avg_entry_price perfectly without calc.
                     # But we can update qty.
                     current_pos.qty = event.position_qty
                else:
                    # New position
                    self._state.positions[symbol] = AlpacaPosition(
                        symbol=symbol,
                        qty=event.position_qty,
                        market_value=event.position_qty * (filled_price or 0.0),
                        avg_entry_price=filled_price or 0.0, # Approximate
                        current_price=filled_price or 0.0,
                        change_today=0.0,
                        unrealized_pl=0.0,
                        side="long" if event.position_qty > 0 else "short"
                    )
        
        self._state.last_update = _utc_now()
        self._state.version += 1
