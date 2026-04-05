from typing import Dict, Optional

class PriceCache:
    def __init__(self):
        self._prices: Dict[str, float] = {}

    def update_price(self, symbol: str, price: float):
        self._prices[symbol] = price

    def get_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

    def snapshot(self) -> Dict[str, float]:
        return self._prices.copy()
