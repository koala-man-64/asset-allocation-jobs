import logging
import math
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

from alpaca.config import ExecutionConfig
from alpaca.models import AlpacaPosition
from alpaca.price_cache import PriceCache

logger = logging.getLogger(__name__)

@dataclass
class PlannedOrder:
    symbol: str
    side: str
    qty: float
    estimated_price: float
    estimated_notional: float

@dataclass
class RebalancePlan:
    orders: List[PlannedOrder] = field(default_factory=list)
    skipped: List[Tuple[str, str]] = field(default_factory=list)
    valid: bool = True
    error: Optional[str] = None

class RebalancePlanner:
    def __init__(self, config: ExecutionConfig, price_cache: PriceCache):
        self._config = config
        self._prices = price_cache

    def plan(
        self,
        target_weights: Dict[str, float],
        current_positions: Dict[str, AlpacaPosition],
        equity: float
    ) -> RebalancePlan:
        plan = RebalancePlan()
        
        if equity <= 0:
            plan.valid = False
            plan.error = "Equity must be positive"
            return plan

        # 1. Normalize targets (assuming they sum <= 1.0 + margin)
        # We process each target independently against current position
        
        # Union of all symbols involved
        all_symbols = set(target_weights.keys()) | set(current_positions.keys())
        
        for symbol in all_symbols:
            target_weight = target_weights.get(symbol, 0.0)
            current_pos = current_positions.get(symbol)
            
            # Get Price
            # If we have a position, we might use current_price from it if cache misses?
            # Better to use unified price cache.
            price = self._prices.get_price(symbol)
            if price is None and current_pos:
                price = current_pos.current_price
            
            if price is None or price <= 0:
                plan.skipped.append((symbol, "Missing price"))
                logger.warning(f"Skipping {symbol}: No price available.")
                continue

            current_qty = current_pos.qty if current_pos else 0.0
            
            # Target Notional & Qty
            target_notional = equity * target_weight
            target_qty_raw = target_notional / price
            
            delta_qty_raw = target_qty_raw - current_qty
            
            # Filter small changes (optional config? broker config has min_trade but not min_change)
            # Usually we check if delta notional is significant.
            
            if abs(delta_qty_raw) < 1e-9:
                continue

            side = "buy" if delta_qty_raw > 0 else "sell"
            abs_delta_qty = abs(delta_qty_raw)
            
            # Apply Rounding
            rounded_qty = self._round_qty(abs_delta_qty)
            
            # Check Min Trade Shares
            if rounded_qty < self._config.min_trade_shares:
                 plan.skipped.append((symbol, f"Qty {rounded_qty} < min {self._config.min_trade_shares}"))
                 continue

            # Check Min Trade Notional
            est_notional = rounded_qty * price
            if est_notional < self._config.min_trade_notional:
                 # If closing a position locally, we might allow it? 
                 # Alpaca rejects small notionals on entry. On exit to 0, it might be allowed?
                 # For now, strict check.
                 # Exception: if we are selling EVERYTHING (target=0) and existing position is small, 
                 # we should probably close it regardless?
                 # Logic: if target_weight is 0 and current_qty > 0, we want to close.
                 is_close = (target_weight == 0.0 and current_qty != 0)
                 if not is_close:
                     plan.skipped.append((symbol, f"Notional {est_notional:.2f} < min {self._config.min_trade_notional}"))
                     continue
            
            # If closing, ensure we don't leave dust due to rounding?
            # If target is 0, we should ensure we sell exactly current_qty.
            if target_weight == 0.0 and current_pos:
                # Force exact close
                rounded_qty = abs(current_pos.qty)
                # But what if rounded_qty was calculated differently?
                # Re-check min notional? If it's a dust cleanup, we might need a different flag.
                # Alpaca allows closing small positions?
                pass 

            if rounded_qty <= 0:
                continue

            plan.orders.append(PlannedOrder(
                symbol=symbol,
                side=side,
                qty=rounded_qty,
                estimated_price=price,
                estimated_notional=rounded_qty * price
            ))
            
        return plan

    def _round_qty(self, qty: float) -> float:
        if self._config.allow_fractional_shares:
            # Maybe round to 9 decimals? Alpaca supports fractional.
            # But 'lot_size' might still apply if fractional is False.
            # config says allow_fractional_shares.
            return float(Decimal(str(qty)).quantize(Decimal("0.000000001"), rounding=ROUND_HALF_UP))
        else:
            # Integer rounding
            mode = self._config.rounding_mode
            d_qty = Decimal(str(qty))
            if mode == "toward_zero":
                # floor for positive numbers
                return int(d_qty) # default int cast is toward zero
            elif mode == "floor":
                 return int(math.floor(qty))
            elif mode == "ceil":
                 return int(math.ceil(qty))
            elif mode == "nearest":
                 return int(round(qty))
            return int(qty)
