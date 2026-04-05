# Transitional decomposition surface; implementation remains in the stable top-level finance job module.
from tasks.finance_data.silver_finance_data import (
    _run_finance_reconciliation,
)

_COMPAT_EXPORTS = (
    _run_finance_reconciliation,
)
