from __future__ import annotations

import os
from typing import Final

from asset_allocation_contracts.paths import DataPaths, bucket_letter

try:
    from asset_allocation_contracts.quiver_signals import (
        QUIVER_DATASET_FAMILIES,
        QUIVER_FORWARD_LOOKING_COLUMNS,
        QUIVER_GOLD_FEATURE_DATASETS,
        QUIVER_PUBLIC_AVAILABILITY_FIELDS,
        QUIVER_SYMBOL_FIELD_HINTS,
        normalize_quiver_dataset,
    )
except Exception:
    QUIVER_DATASET_FAMILIES = (
        "political_trading",
        "government_contracts",
        "government_contracts_all",
        "insider_trading",
        "institutional_holdings",
        "institutional_holding_changes",
        "lobbying",
        "etf_holdings",
        "congress_holdings",
    )
    QUIVER_GOLD_FEATURE_DATASETS = (
        "political_trading",
        "government_contracts",
        "insider_trading",
        "institutional_holding_changes",
    )
    QUIVER_PUBLIC_AVAILABILITY_FIELDS = {
        "political_trading": "ReportDate",
        "government_contracts": "Date",
        "government_contracts_all": "action_date",
        "insider_trading": "uploaded",
        "institutional_holdings": "Date",
        "institutional_holding_changes": "Date",
        "lobbying": "Date",
        "etf_holdings": "",
        "congress_holdings": "",
    }
    QUIVER_SYMBOL_FIELD_HINTS = {
        "political_trading": ("Ticker",),
        "government_contracts": ("Ticker",),
        "government_contracts_all": ("Ticker",),
        "insider_trading": ("Ticker",),
        "institutional_holdings": ("Ticker",),
        "institutional_holding_changes": ("Ticker",),
        "lobbying": ("Ticker",),
        "etf_holdings": ("Holding Symbol", "ETF Symbol"),
        "congress_holdings": (),
    }
    QUIVER_FORWARD_LOOKING_COLUMNS = frozenset({"ExcessReturn", "PriceChange", "SPYChange", "excess_return", "price_change", "spy_change"})

    def normalize_quiver_dataset(dataset: str) -> str:
        return str(dataset or "").strip().lower().replace("-", "_").replace(" ", "_")


DOMAIN_SLUG: Final[str] = str(os.environ.get("AZURE_FOLDER_QUIVER") or "quiver-data").strip()
BRONZE_ROOT_PREFIX: Final[str] = f"{DOMAIN_SLUG}/runs"
DOMAIN_ARTIFACT_PATH: Final[str] = f"{DOMAIN_SLUG}/_metadata/domain.json"

BRONZE_JOB_NAME: Final[str] = "bronze-quiver-data-job"
SILVER_JOB_NAME: Final[str] = "silver-quiver-data-job"
GOLD_JOB_NAME: Final[str] = "gold-quiver-data-job"

SOURCE_DATASETS: Final[tuple[tuple[str, str], ...]] = (
    ("congress_trading_live", "political_trading"),
    ("congress_trading_historical", "political_trading"),
    ("senate_trading_live", "political_trading"),
    ("senate_trading_historical", "political_trading"),
    ("house_trading_live", "political_trading"),
    ("house_trading_historical", "political_trading"),
    ("government_contracts_live", "government_contracts"),
    ("government_contracts_historical", "government_contracts"),
    ("government_contracts_all_live", "government_contracts_all"),
    ("government_contracts_all_historical", "government_contracts_all"),
    ("insiders_live", "insider_trading"),
    ("sec13f_live", "institutional_holdings"),
    ("sec13fchanges_live", "institutional_holding_changes"),
    ("lobbying_live", "lobbying"),
    ("lobbying_historical", "lobbying"),
    ("etf_holdings_live", "etf_holdings"),
    ("congress_holdings_live", "congress_holdings"),
)


def bronze_run_prefix(run_id: str) -> str:
    return f"{BRONZE_ROOT_PREFIX}/{str(run_id or '').strip()}"


def bronze_raw_path(run_id: str, source_dataset: str, bucket: str) -> str:
    clean_dataset = str(source_dataset or "").strip().replace(" ", "_")
    return f"{bronze_run_prefix(run_id)}/raw/{clean_dataset}/buckets/{str(bucket or 'X').strip().upper()}.json"


def bronze_manifest_path(run_id: str) -> str:
    return f"{bronze_run_prefix(run_id)}/manifest.json"


def silver_table_path(dataset_family: str, bucket: str) -> str:
    helper = getattr(DataPaths, "get_silver_quiver_bucket_path", None)
    if callable(helper):
        return helper(dataset_family, bucket)
    clean_family = normalize_quiver_dataset(dataset_family)
    return f"quiver-data/{clean_family}/buckets/{str(bucket).strip().upper()}"


def gold_table_path(dataset_family: str, bucket: str) -> str:
    helper = getattr(DataPaths, "get_gold_quiver_bucket_path", None)
    if callable(helper):
        return helper(dataset_family, bucket)
    clean_family = normalize_quiver_dataset(dataset_family)
    return f"quiver/{clean_family}/buckets/{str(bucket).strip().upper()}"


def feature_table_path(dataset_family: str, bucket: str) -> str:
    return gold_table_path(f"{normalize_quiver_dataset(dataset_family)}_daily", bucket)


def dataset_family_for_source(source_dataset: str) -> str:
    mapping = dict(SOURCE_DATASETS)
    return mapping[str(source_dataset)]


def symbol_field_hints(dataset_family: str) -> tuple[str, ...]:
    return tuple(QUIVER_SYMBOL_FIELD_HINTS.get(normalize_quiver_dataset(dataset_family), ()))


def public_availability_field(dataset_family: str) -> str:
    return str(QUIVER_PUBLIC_AVAILABILITY_FIELDS.get(normalize_quiver_dataset(dataset_family), "") or "")


def normalize_bucket(symbol: str | None) -> str:
    return bucket_letter(str(symbol or "").strip()) if str(symbol or "").strip() else "X"
