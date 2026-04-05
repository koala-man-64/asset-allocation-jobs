# Transitional decomposition surface; implementation remains in the stable top-level finance job module.
from tasks.finance_data.gold_finance_data import (
    _build_missing_source_column_message,
    _parse_human_number,
    _preflight_feature_schema,
    _prepare_optional_table,
    _prepare_table,
    _require_column,
    _resolve_column,
    _safe_div,
    compute_features,
)

_COMPAT_EXPORTS = (
    _build_missing_source_column_message,
    _parse_human_number,
    _preflight_feature_schema,
    _prepare_optional_table,
    _prepare_table,
    _require_column,
    _resolve_column,
    _safe_div,
    compute_features,
)
