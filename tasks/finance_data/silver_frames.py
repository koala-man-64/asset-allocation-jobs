# Transitional compatibility wrapper; implementation lives in silver_modules.frames.
from tasks.finance_data.silver_modules.frames import (
    _align_finance_frame_to_contract,
    _finance_row_identity_columns,
    _finance_sub_domain,
    _prepare_finance_delta_write_frame,
    _repair_symbol_column_aliases,
    _split_finance_bucket_rows,
)

_COMPAT_EXPORTS = (
    _align_finance_frame_to_contract,
    _finance_row_identity_columns,
    _finance_sub_domain,
    _prepare_finance_delta_write_frame,
    _repair_symbol_column_aliases,
    _split_finance_bucket_rows,
)
