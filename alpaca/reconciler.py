import asyncio
import logging
import os
from typing import Optional

from alpaca.config import AlpacaConfig
from alpaca.state import StateManager
from alpaca.trading_rest import AlpacaTradingClient

logger = logging.getLogger(__name__)

def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_test_environment() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or _is_truthy(os.environ.get("TEST_MODE"))


class Reconciler:
    def __init__(
        self, 
        config: AlpacaConfig, 
        client: AlpacaTradingClient, 
        state_manager: StateManager
    ):
        self._config = config
        self._client = client
        self._state_manager = state_manager
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def bootstrap(self):
        """
        Perform initial full state synchronization.
        """
        logger.info("Bootstrapping brokerage state...")
        # 1. Get Account
        account = self._client.get_account()
        self._state_manager.update_account(account)

        # 2. Get Positions
        positions = self._client.list_positions()
        self._state_manager.update_positions(positions)

        # 3. Get Open Orders
        open_orders = self._client.list_orders(status="open")
        self._state_manager.update_open_orders(open_orders)
        
        logger.info("Bootstrap complete. State version: %s", self._state_manager.state.version)

    async def start_polling(self):
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self):
        interval = self._config.reconcile.poll_interval_s
        logger.info(f"Starting reconcile loop (interval={interval}s)")
        
        while self._running:
            try:
                # We can do a lighter sync or full sync.
                # For safety, let's sync open orders and positions.
                # Account updates are less critical for high frequency but good to have.
                await self._sync_cycle()
            except Exception as e:
                logger.error(f"Error in reconcile loop: {e}", exc_info=True)
            
            await asyncio.sleep(interval)

    async def _sync_cycle(self):
        # The Alpaca client is synchronous (httpx.Client). In production we offload
        # to a threadpool; in test/sandbox environments threads can be restricted.
        if _is_test_environment():
            orders = self._client.list_orders(status="open")
            self._state_manager.update_open_orders(orders)

            positions = self._client.list_positions()
            self._state_manager.update_positions(positions)

            account = self._client.get_account()
            self._state_manager.update_account(account)
            return

        loop = asyncio.get_running_loop()

        orders = await loop.run_in_executor(None, lambda: self._client.list_orders(status="open"))
        self._state_manager.update_open_orders(orders)

        positions = await loop.run_in_executor(None, self._client.list_positions)
        self._state_manager.update_positions(positions)

        account = await loop.run_in_executor(None, self._client.get_account)
        self._state_manager.update_account(account)
