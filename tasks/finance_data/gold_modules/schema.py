# Transitional decomposition surface; implementation remains in the stable top-level finance job module.
from tasks.finance_data.gold_finance_data import (
    _GOLD_FINANCE_FLOAT_COLUMNS,
    _GOLD_FINANCE_PIOTROSKI_COLUMNS,
    _GOLD_FINANCE_PIOTROSKI_INTEGER_COLUMNS,
    _empty_gold_finance_bucket_frame,
    _gold_finance_alpha26_bucket_path,
    _load_gold_finance_bucket_template,
    _merge_symbol_to_bucket_map,
    _normalize_sub_domain,
    _project_gold_finance_piotroski_frame,
)

_COMPAT_EXPORTS = (
    _GOLD_FINANCE_FLOAT_COLUMNS,
    _GOLD_FINANCE_PIOTROSKI_COLUMNS,
    _GOLD_FINANCE_PIOTROSKI_INTEGER_COLUMNS,
    _empty_gold_finance_bucket_frame,
    _gold_finance_alpha26_bucket_path,
    _load_gold_finance_bucket_template,
    _merge_symbol_to_bucket_map,
    _normalize_sub_domain,
    _project_gold_finance_piotroski_frame,
)
