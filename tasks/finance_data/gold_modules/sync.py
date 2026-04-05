# Transitional decomposition surface; implementation remains in the stable top-level finance job module.
from tasks.finance_data.gold_finance_data import (
    _FINANCE_POSTGRES_SCHEMA_REMEDIATION_HINT,
    _load_existing_gold_finance_symbol_to_bucket_map,
)

_COMPAT_EXPORTS = (
    _FINANCE_POSTGRES_SCHEMA_REMEDIATION_HINT,
    _load_existing_gold_finance_symbol_to_bucket_map,
)
