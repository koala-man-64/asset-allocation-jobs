"""Jobs-side facade for the shared backtest lifecycle client.

The lifecycle contract is authored in asset-allocation-contracts and implemented by
asset-allocation-control-plane. This repo consumes the published runtime-common client.
"""

from asset_allocation_runtime_common.backtest_repository import BacktestRepository

__all__ = ["BacktestRepository"]
