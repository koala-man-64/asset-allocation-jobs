from __future__ import annotations

from typing import Any

from asset_allocation_runtime_common.market_data import core as mdc
def should_log_bronze_success(
    count: int,
    *,
    sample_limit: int = 20,
    interval: int = 250,
) -> bool:
    try:
        normalized_count = int(count)
    except Exception:
        return False
    if normalized_count <= 0:
        return False

    sample_limit = max(0, int(sample_limit))
    interval = max(0, int(interval))
    if normalized_count <= sample_limit:
        return True
    return interval > 0 and normalized_count % interval == 0


def _format_context_value(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip()
    return text or "n/a"


def log_bronze_success(*, domain: str, operation: str, **context: Any) -> None:
    parts = [
        f"Bronze {str(domain).strip()} success:",
        f"operation={_format_context_value(operation)}",
    ]
    for key, value in context.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            continue
        parts.append(f"{normalized_key}={_format_context_value(value)}")
    mdc.write_line(" ".join(parts))
