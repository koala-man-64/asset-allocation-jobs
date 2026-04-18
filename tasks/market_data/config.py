from __future__ import annotations

from asset_allocation_runtime_common.foundation import config as _cfg
AZURE_FOLDER_MARKET = _cfg.AZURE_FOLDER_MARKET


def __getattr__(name: str):
    return getattr(_cfg, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_cfg)))


