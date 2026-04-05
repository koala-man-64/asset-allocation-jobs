# Transitional decomposition surface; implementation remains in the stable top-level finance job module.
from tasks.finance_data.bronze_finance_data import (
    _build_finance_bucket_row,
    _load_alpha26_finance_row_map,
    _parse_ingested_at,
    _remove_alpha26_finance_row,
    _upsert_alpha26_finance_row,
)

_COMPAT_EXPORTS = (
    _build_finance_bucket_row,
    _load_alpha26_finance_row_map,
    _parse_ingested_at,
    _remove_alpha26_finance_row,
    _upsert_alpha26_finance_row,
)
