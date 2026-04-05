import asyncio
import logging
from typing import Dict, List, Any

from alpaca.config import AlpacaConfig
from alpaca.execution.order_factory import OrderFactory
from alpaca.execution.rebalance_planner import RebalancePlanner, RebalancePlan
from alpaca.models import AlpacaOrder
from alpaca.price_cache import PriceCache
from alpaca.state import StateManager
from alpaca.trading_rest import AlpacaTradingClient

logger = logging.getLogger(__name__)

class ExecutionService:
    def __init__(
        self,
        config: AlpacaConfig,
        client: AlpacaTradingClient,
        state_manager: StateManager,
        price_cache: PriceCache
    ):
        self._config = config
        self._execution_config = config.execution # Helper
        self._client = client
        self._state_manager = state_manager
        self._price_cache = price_cache
        
        self._planner = RebalancePlanner(self._execution_config, price_cache)
        self._factory = OrderFactory(self._execution_config)

    async def rebalance_to_target_weights(
        self,
        target_weights: Dict[str, float],
        strategy_id: str,
        rebalance_id: str,
        wait_for_fills: bool = True,
        timeout_s: float = 300.0
    ) -> Dict[str, Any]:
        """
        Execute rebalance.
        """
        logger.info(f"Starting rebalance {rebalance_id} for strategy {strategy_id}")
        
        # 1. State Snapshot
        # We assume state is up to date via reconciler/stream, or we force sync?
        # Ideally we use what we have.
        current_state = self._state_manager.state
        current_positions = current_state.positions.copy()
        
        # 2. Equity
        equity = current_state.account.equity
        if equity <= 0:
            logger.error("Equity is zero or negative. Cannot rebalance.")
            return {"status": "failed", "reason": "No equity"}
            
        # 3. Plan
        plan: RebalancePlan = self._planner.plan(
            target_weights=target_weights, 
            current_positions=current_positions, 
            equity=equity
        )
        
        if not plan.valid:
            return {"status": "failed", "reason": plan.error}
            
        logger.info(f"Plan generated: {len(plan.orders)} orders. Skipped: {len(plan.skipped)}")
        
        # 4. Execute
        submitted_orders = []
        errors = []
        
        # Cancel open orders for symbols we are trading? Or all?
        # Usually smart rebalance cancels existing open orders for touched symbols first.
        # Implementation simplicity: Cancel all is safest but aggressive.
        # Let's cancel open orders for symbols in plan.
        # (Not implemented in M3 scope detailed in prompt, but good practice).
        
        # Submit
        for order_plan in plan.orders:
            payload = self._factory.create_order_payload(order_plan, strategy_id, rebalance_id)
            try:
                # Run sync submit in thread?
                loop = asyncio.get_running_loop()
                order = await loop.run_in_executor(None, lambda: self._client.submit_order(**payload))
                submitted_orders.append(order)
                logger.info(f"Submitted {order.side} {order.qty} {order.symbol} (id={order.id})")
            except Exception as e:
                logger.error(f"Failed to submit order for {order_plan.symbol}: {e}")
                errors.append((order_plan.symbol, str(e)))

        result = {
            "status": "submitted",
            "submitted_count": len(submitted_orders),
            "orders": [o.id for o in submitted_orders],
            "errors": errors,
            "plan_skipped": plan.skipped
        }
        
        if wait_for_fills and submitted_orders:
            logger.info(f"Waiting for {len(submitted_orders)} orders to fill (timeout={timeout_s}s)...")
            fills = await self._wait_for_orders(submitted_orders, timeout_s)
            result["status"] = "completed"
            result["filled_count"] = len(fills)
            result["fills"] = fills # List of order IDs that filled?
        
        return result

    async def _wait_for_orders(self, orders: List[AlpacaOrder], timeout_s: float) -> List[str]:
        # Simple polling wait
        start_time = asyncio.get_event_loop().time()
        pending_ids = {o.id for o in orders}
        filled_ids = set()
        
        while pending_ids:
            if asyncio.get_event_loop().time() - start_time > timeout_s:
                logger.warning("Timed out waiting for fills.")
                break
                
            # Check status of pending
            # We can check local state if stream is running!
            # Using state_manager is better than polling REST if stream is active.
            
            # Make a copy to iterate
            check_ids = list(pending_ids)
            for oid in check_ids:
                # Check local state first
                # If order is gone from open_orders (and was there), is it filled?
                # Or check if it is in positions?
                # Better: check status via REST if we want certainty, or trust state.
                # Let's poll REST for M3 simplicity and robustness.
                
                try:
                    loop = asyncio.get_running_loop()
                    o = await loop.run_in_executor(None, lambda: self._client.get_order(oid))
                    
                    if o.status == "filled":
                        filled_ids.add(oid)
                        pending_ids.remove(oid)
                    elif o.status in ("canceled", "expired", "rejected"):
                        pending_ids.remove(oid) # Terminal but not filled
                except Exception:
                    pass
            
            if pending_ids:
                await asyncio.sleep(1.0)
                
        return list(filled_ids)
