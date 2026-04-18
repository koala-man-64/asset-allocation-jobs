from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any


def owner_module_for(target: Any) -> ModuleType:
    module_name = getattr(target, "__module__", None)
    if not module_name:
        raise TypeError(f"Cannot resolve owner module for {target!r}.")
    return importlib.import_module(module_name)
