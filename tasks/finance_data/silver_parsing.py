# Transitional compatibility wrapper; implementation lives in silver_modules.parsing.
from tasks.finance_data.silver_modules.parsing import (
    _read_finance_json,
    _utc_today,
    resample_daily_ffill,
)

_COMPAT_EXPORTS = (
    _read_finance_json,
    _utc_today,
    resample_daily_ffill,
)
