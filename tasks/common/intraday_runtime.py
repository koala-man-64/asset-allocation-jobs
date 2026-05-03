from __future__ import annotations

from contextlib import contextmanager
import os
from typing import Iterator

from asset_allocation_runtime_common.market_data import core as mdc

MARKET_LAYER_LOCKS = {
    "bronze": "market-bronze",
    "silver": "market-silver",
    "gold": "market-gold",
}


def storage_lock_prerequisites_configured() -> bool:
    return bool(
        str(os.environ.get("AZURE_CONTAINER_COMMON") or "").strip()
        and (
            str(os.environ.get("AZURE_STORAGE_ACCOUNT_NAME") or "").strip()
            or str(os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or "").strip()
        )
    )


def require_intraday_lock_prerequisites(job_name: str) -> None:
    if not str(os.environ.get("AZURE_CONTAINER_COMMON") or "").strip():
        raise RuntimeError(f"{job_name} requires AZURE_CONTAINER_COMMON before claiming work.")
    if not (
        str(os.environ.get("AZURE_STORAGE_ACCOUNT_NAME") or "").strip()
        or str(os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or "").strip()
    ):
        raise RuntimeError(
            f"{job_name} requires AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING before claiming work."
        )


@contextmanager
def market_layer_lock(
    layer: str,
    *,
    conflict_policy: str = "wait_then_fail",
    wait_timeout_seconds: int = 90,
    enabled: bool | None = None,
) -> Iterator[str]:
    normalized = str(layer or "").strip().lower()
    lock_name = MARKET_LAYER_LOCKS.get(normalized)
    if not lock_name:
        raise ValueError(f"Unknown market layer lock: {layer!r}.")

    should_lock = storage_lock_prerequisites_configured() if enabled is None else enabled
    if not should_lock:
        yield "disabled"
        return

    kwargs: dict[str, object] = {"conflict_policy": conflict_policy}
    if wait_timeout_seconds is not None:
        kwargs["wait_timeout_seconds"] = int(wait_timeout_seconds)
    with mdc.JobLock(lock_name, **kwargs):
        yield "acquired"

