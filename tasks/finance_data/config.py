from __future__ import annotations

from core import config as _cfg

AZURE_FOLDER_FINANCE = _cfg.AZURE_FOLDER_FINANCE


def __getattr__(name: str):
    return getattr(_cfg, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_cfg)))
