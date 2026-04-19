from __future__ import annotations

import os
from typing import Final


DOMAIN_SLUG: Final[str] = str(os.environ.get("AZURE_FOLDER_ECONOMIC_CATALYST") or "economic-catalyst").strip()
BRONZE_ROOT_PREFIX: Final[str] = f"{DOMAIN_SLUG}/runs"
SILVER_ROOT_PREFIX: Final[str] = DOMAIN_SLUG
GOLD_ROOT_PREFIX: Final[str] = DOMAIN_SLUG
DOMAIN_ARTIFACT_PATH: Final[str] = f"{DOMAIN_SLUG}/_metadata/domain.json"
SILVER_STATE_ROOT_PREFIX: Final[str] = f"{SILVER_ROOT_PREFIX}/_state"

BRONZE_JOB_NAME: Final[str] = "bronze-economic-catalyst-job"
SILVER_JOB_NAME: Final[str] = "silver-economic-catalyst-job"
GOLD_JOB_NAME: Final[str] = "gold-economic-catalyst-job"

DEFAULT_BLS_ICS_URL: Final[str] = "https://www.bls.gov/schedule/news_release/bls.ics"
DEFAULT_BEA_SCHEDULE_URL: Final[str] = "https://www.bea.gov/news/schedule/"
DEFAULT_ECB_CALENDAR_URL: Final[str] = "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html"
DEFAULT_BOE_CALENDAR_URL: Final[str] = "https://www.bankofengland.co.uk/monetary-policy/upcoming-mpc-dates"
DEFAULT_BOJ_SCHEDULE_URL: Final[str] = "https://www.boj.or.jp/en/about/calendar/"
DEFAULT_TREASURY_AUCTIONS_URL: Final[str] = "https://www.treasurydirect.gov/xml/PendingAuctions.xml"

DEFAULT_FOMC_SCHEDULE_URLS: Final[tuple[str, ...]] = (
    "https://www.federalreserve.gov/newsevents/pressreleases/monetary20240809a.htm",
    "https://www.federalreserve.gov/newsevents/pressreleases/monetary20250905a.htm",
)

IMPORTANCE_TIERS: Final[frozenset[str]] = frozenset({"low", "medium", "high"})
TIME_PRECISION_VALUES: Final[frozenset[str]] = frozenset({"exact", "approximate", "date_only", "unknown"})
SCHEDULE_STATUS_VALUES: Final[frozenset[str]] = frozenset(
    {"scheduled", "released", "revised", "cancelled", "withdrawn", "unknown"}
)
ITEM_KIND_VALUES: Final[frozenset[str]] = frozenset({"event", "headline"})
ENTITY_TYPE_VALUES: Final[frozenset[str]] = frozenset(
    {"country", "region", "central_bank", "indicator", "currency", "symbol", "sector", "factor"}
)

OFFICIAL_SOURCES: Final[frozenset[str]] = frozenset(
    {
        "fred_releases",
        "bls_release_calendar",
        "bea_release_schedule",
        "fomc_schedule",
        "ecb_policy_calendar",
        "boe_mpc_calendar",
        "boj_release_schedule",
        "treasury_auction_schedule",
    }
)
STRUCTURED_VENDOR_SOURCES: Final[frozenset[str]] = frozenset({"nasdaq_tables"})
HEADLINE_SOURCES: Final[frozenset[str]] = frozenset(
    {"massive_news", "alpaca_news", "alpha_vantage_news"}
)
ALL_SOURCES: Final[tuple[str, ...]] = (
    "fred_releases",
    "bls_release_calendar",
    "bea_release_schedule",
    "fomc_schedule",
    "ecb_policy_calendar",
    "boe_mpc_calendar",
    "boj_release_schedule",
    "treasury_auction_schedule",
    "nasdaq_tables",
    "massive_news",
    "alpaca_news",
    "alpha_vantage_news",
)

EVENT_BASE_COLUMNS: Final[tuple[str, ...]] = (
    "event_key",
    "event_name",
    "event_group",
    "event_subgroup",
    "event_type",
    "importance_tier",
    "impact_domain",
    "country",
    "region",
    "currency",
    "source_name",
    "source_event_key",
    "official_source_name",
    "effective_at",
    "published_at",
    "source_updated_at",
    "ingested_at",
    "time_precision",
    "schedule_status",
    "is_confirmed",
    "actual_numeric",
    "actual_text",
    "consensus_numeric",
    "consensus_text",
    "previous_numeric",
    "previous_text",
    "revised_previous_numeric",
    "revised_previous_text",
    "surprise_abs",
    "surprise_pct",
    "unit",
    "period_label",
    "frequency",
    "market_sensitivity_tags_json",
    "sector_tags_json",
    "factor_tags_json",
    "is_high_impact",
    "is_routine",
    "is_revisionable",
    "withdrawal_flag",
    "source_hash",
    "provenance_json",
)
EVENT_COLUMNS: Final[tuple[str, ...]] = ("event_id", *EVENT_BASE_COLUMNS)
EVENT_VERSION_COLUMNS: Final[tuple[str, ...]] = (
    "version_id",
    "event_id",
    "version_seq",
    "version_kind",
    "version_observed_at",
    *EVENT_BASE_COLUMNS,
)

HEADLINE_BASE_COLUMNS: Final[tuple[str, ...]] = (
    "headline_key",
    "source_name",
    "source_item_id",
    "headline",
    "summary",
    "url",
    "author",
    "published_at",
    "source_updated_at",
    "ingested_at",
    "country",
    "region",
    "event_group",
    "importance_tier",
    "relevance_tier",
    "withdrawal_flag",
    "tags_json",
    "tickers_json",
    "channels_json",
    "source_hash",
    "provenance_json",
)
HEADLINE_COLUMNS: Final[tuple[str, ...]] = ("headline_id", *HEADLINE_BASE_COLUMNS)
HEADLINE_VERSION_COLUMNS: Final[tuple[str, ...]] = (
    "version_id",
    "headline_id",
    "version_seq",
    "version_kind",
    "version_observed_at",
    *HEADLINE_BASE_COLUMNS,
)

MENTION_COLUMNS: Final[tuple[str, ...]] = (
    "item_kind",
    "item_id",
    "entity_type",
    "entity_key",
    "relevance_tier",
    "confidence",
    "mapping_rule_version",
    "source_name",
    "published_at",
    "effective_at",
    "ingested_at",
)

QUARANTINE_COLUMNS: Final[tuple[str, ...]] = (
    "source_name",
    "dataset_name",
    "record_kind",
    "raw_identifier",
    "reason",
    "observed_at",
    "source_updated_at",
    "payload_preview",
    "source_hash",
)

ENTITY_DAILY_COLUMNS: Final[tuple[str, ...]] = (
    "as_of_date",
    "entity_type",
    "entity_key",
    "headline_count",
    "event_count",
    "high_impact_event_count",
    "release_count",
    "scheduled_count",
    "policy_event_count",
    "inflation_event_count",
    "labor_event_count",
    "growth_event_count",
    "rates_event_count",
    "last_published_at",
    "last_effective_at",
    "ingested_at",
)

INTERNAL_SOURCE_EVENT_COLUMNS: Final[tuple[str, ...]] = (
    "source_record_id",
    "source_name",
    "dataset_name",
    "source_event_key",
    "event_name",
    "event_group",
    "event_subgroup",
    "event_type",
    "importance_tier",
    "impact_domain",
    "country",
    "region",
    "currency",
    "effective_at",
    "published_at",
    "source_updated_at",
    "ingested_at",
    "time_precision",
    "schedule_status",
    "is_confirmed",
    "actual_numeric",
    "actual_text",
    "consensus_numeric",
    "consensus_text",
    "previous_numeric",
    "previous_text",
    "revised_previous_numeric",
    "revised_previous_text",
    "unit",
    "period_label",
    "frequency",
    "summary",
    "market_sensitivity_tags_json",
    "sector_tags_json",
    "factor_tags_json",
    "is_high_impact",
    "is_routine",
    "is_revisionable",
    "withdrawal_flag",
    "source_url",
    "source_hash",
    "raw_identifier",
)

INTERNAL_SOURCE_HEADLINE_COLUMNS: Final[tuple[str, ...]] = (
    "source_record_id",
    "source_name",
    "dataset_name",
    "source_item_id",
    "headline",
    "summary",
    "url",
    "author",
    "published_at",
    "source_updated_at",
    "ingested_at",
    "country",
    "region",
    "event_group",
    "importance_tier",
    "relevance_tier",
    "withdrawal_flag",
    "tags_json",
    "tickers_json",
    "channels_json",
    "source_hash",
    "raw_identifier",
)

SOURCE_PRIORITY: Final[dict[str, int]] = {
    "fred_releases": 10,
    "bls_release_calendar": 10,
    "bea_release_schedule": 10,
    "fomc_schedule": 10,
    "ecb_policy_calendar": 10,
    "boe_mpc_calendar": 10,
    "boj_release_schedule": 10,
    "treasury_auction_schedule": 10,
    "nasdaq_tables": 20,
    "massive_news": 10,
    "alpaca_news": 20,
    "alpha_vantage_news": 30,
}


def bronze_run_prefix(run_id: str) -> str:
    return f"{BRONZE_ROOT_PREFIX}/{str(run_id or '').strip()}"


def bronze_raw_path(run_id: str, source_name: str, dataset_name: str) -> str:
    clean_source = str(source_name or "").strip().replace(" ", "_")
    clean_dataset = str(dataset_name or "").strip().replace(" ", "_")
    return f"{bronze_run_prefix(run_id)}/raw/{clean_source}/{clean_dataset}.json"


def bronze_manifest_path(run_id: str) -> str:
    return f"{bronze_run_prefix(run_id)}/manifest.json"


def silver_table_path(table_name: str) -> str:
    return f"{SILVER_ROOT_PREFIX}/{str(table_name or '').strip()}.parquet"


def silver_state_table_path(table_name: str) -> str:
    return f"{SILVER_STATE_ROOT_PREFIX}/{str(table_name or '').strip()}.parquet"


def gold_table_path(table_name: str) -> str:
    return f"{GOLD_ROOT_PREFIX}/{str(table_name or '').strip()}.parquet"
