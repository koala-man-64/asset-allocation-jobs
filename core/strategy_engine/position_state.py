from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import Mapping

TemporalValue = date | datetime | str
FeatureValue = float | int | bool | None


@dataclass(frozen=True)
class PriceBar:
    date: TemporalValue
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    features: Mapping[str, FeatureValue] = field(default_factory=dict)

    def get_price(self, field_name: str) -> float | None:
        value = getattr(self, field_name, None)
        if value is None:
            return None
        return float(value)

    def get_feature(self, field_name: str) -> float | int | bool | None:
        return self.features.get(field_name)

    def anchor_high(self) -> float | None:
        prices = [value for value in (self.high, self.open, self.close) if value is not None]
        if not prices:
            return None
        return float(max(prices))

    def anchor_low(self) -> float | None:
        prices = [value for value in (self.low, self.open, self.close) if value is not None]
        if not prices:
            return None
        return float(min(prices))


@dataclass(frozen=True)
class PositionState:
    symbol: str
    entry_date: TemporalValue
    entry_price: float
    quantity: float
    position_id: str | None = None
    opened_at: TemporalValue | None = None
    average_cost: float | None = None
    commission_accrued: float = 0.0
    slippage_accrued: float = 0.0
    max_quantity: float | None = None
    resize_count: int = 0
    realized_pnl_accrued: float = 0.0
    realized_basis_accrued: float = 0.0
    bars_held: int = 0
    highest_since_entry: float | None = None
    lowest_since_entry: float | None = None

    def __post_init__(self) -> None:
        if self.opened_at is None:
            object.__setattr__(self, "opened_at", self.entry_date)
        if self.average_cost is None:
            object.__setattr__(self, "average_cost", float(self.entry_price))
        if self.max_quantity is None:
            object.__setattr__(self, "max_quantity", float(abs(self.quantity)))
        if self.position_id is None:
            object.__setattr__(self, "position_id", f"{self.symbol}:{self.entry_date}")

    def advance(self, bar: PriceBar) -> "PositionState":
        highest = self.highest_since_entry if self.highest_since_entry is not None else self.entry_price
        lowest = self.lowest_since_entry if self.lowest_since_entry is not None else self.entry_price

        bar_high = bar.anchor_high()
        bar_low = bar.anchor_low()
        if bar_high is not None:
            highest = max(highest, bar_high)
        if bar_low is not None:
            lowest = min(lowest, bar_low)

        return replace(
            self,
            bars_held=self.bars_held + 1,
            highest_since_entry=highest,
            lowest_since_entry=lowest,
        )
