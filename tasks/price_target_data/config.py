from __future__ import annotations

import os

from core import config as _cfg

AZURE_FOLDER_TARGETS = _cfg.AZURE_FOLDER_TARGETS
NASDAQ_API_KEY = os.environ.get("NASDAQ_API_KEY")


def __getattr__(name: str):
    return getattr(_cfg, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_cfg)))
