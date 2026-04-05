# Transitional decomposition surface; implementation remains in the stable top-level finance job module.
from tasks.finance_data.bronze_finance_data import (
    _failure_bucket_key,
    _format_failure_reason,
    _is_recoverable_massive_error,
)

_COMPAT_EXPORTS = (
    _failure_bucket_key,
    _format_failure_reason,
    _is_recoverable_massive_error,
)
