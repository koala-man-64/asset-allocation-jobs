# Transitional decomposition surface; implementation remains in the stable top-level finance job module.
from tasks.finance_data.gold_finance_data import (
    FeatureJobConfig,
    _build_job_config,
)

_COMPAT_EXPORTS = (
    FeatureJobConfig,
    _build_job_config,
)
