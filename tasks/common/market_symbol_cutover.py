from __future__ import annotations

from typing import Literal

import pandas as pd

from core import core as mdc
from core import bronze_bucketing
from core import layer_bucketing
from core.market_symbols import REGIME_REQUIRED_MARKET_SYMBOLS

MarketLayer = Literal["bronze", "silver", "gold"]
_REGIME_REQUIRED_MARKET_SYMBOL_SET = frozenset(REGIME_REQUIRED_MARKET_SYMBOLS)


def _index_path(*, layer: MarketLayer) -> str:
    if layer == "bronze":
        return "system/bronze-index/market/latest.parquet"
    return f"system/{layer}-index/market/latest.parquet"


def _gold_watermark_path() -> str:
    return "system/watermarks/gold_market_features.json"


def _legacy_symbols_from_index(df: pd.DataFrame | None) -> tuple[str, ...]:
    if df is None or df.empty or "symbol" not in df.columns:
        return ()

    observed_symbols = {
        str(symbol).strip().upper()
        for symbol in df["symbol"].dropna().tolist()
        if str(symbol).strip().upper() in _REGIME_REQUIRED_MARKET_SYMBOL_SET
    }
    return tuple(symbol for symbol in REGIME_REQUIRED_MARKET_SYMBOLS if symbol in observed_symbols)


def prepare_provider_native_vix_cutover(*, layer: MarketLayer) -> bool:
    common_client = getattr(mdc, "common_storage_client", None)
    if common_client is None:
        return False

    if layer == "bronze":
        index_df = bronze_bucketing.load_symbol_index("market")
        index_path = _index_path(layer=layer)
    elif layer in {"silver", "gold"}:
        index_df = layer_bucketing.load_layer_symbol_index(layer=layer, domain="market")
        index_path = _index_path(layer=layer)
    else:
        raise ValueError(f"Unsupported market cutover layer={layer!r}")

    legacy_symbols = _legacy_symbols_from_index(index_df)
    if not legacy_symbols:
        return False

    deleted_index_blobs = int(common_client.delete_prefix(index_path) or 0)
    deleted_watermarks = 0
    if layer == "gold":
        deleted_watermarks = int(common_client.delete_prefix(_gold_watermark_path()) or 0)

    mdc.write_warning(
        "Provider-native VIX cutover reset: "
        f"layer={layer} legacy_symbols={list(legacy_symbols)} "
        f"deleted_index_blobs={deleted_index_blobs} deleted_watermarks={deleted_watermarks}"
    )
    return True
