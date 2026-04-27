from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from tasks.economic_catalyst_data import constants


def _strip(value: object) -> str:
    return str(value or "").strip()


def _env_text(name: str, default: str = "") -> str:
    value = _strip(os.environ.get(name))
    return value or default


def _env_int(name: str, default: int) -> int:
    raw = _strip(os.environ.get(name))
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _csv_or_default(raw: str, default: tuple[str, ...]) -> tuple[str, ...]:
    text = _strip(raw)
    if not text:
        return tuple(default)
    return tuple(dict.fromkeys(part.strip() for part in text.split(",") if part.strip()))


def _json_list(raw: str) -> list[Any]:
    text = _strip(raw)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON payload: {exc}") from exc
    if not isinstance(parsed, list):
        raise ValueError("Expected a JSON list payload.")
    return parsed


@dataclass(frozen=True)
class NasdaqTableConfig:
    table: str
    dataset_name: str
    date_column: str | None = None
    title_column: str = "event_name"
    effective_at_column: str | None = None
    published_at_column: str | None = None
    updated_at_column: str | None = None
    country_column: str | None = None
    region_column: str | None = None
    currency_column: str | None = None
    actual_column: str | None = None
    consensus_column: str | None = None
    previous_column: str | None = None
    revised_previous_column: str | None = None
    unit_column: str | None = None
    period_column: str | None = None
    frequency_column: str | None = None
    status_column: str | None = None
    importance_column: str | None = None
    event_type: str = "macro_release"
    filters: dict[str, Any] = field(default_factory=dict)
    static_values: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "NasdaqTableConfig":
        table = _strip(data.get("table"))
        if not table:
            raise ValueError("Nasdaq table config requires 'table'.")
        dataset_name = _strip(data.get("dataset_name")) or table.replace("/", "_").lower()
        filters = data.get("filters") or {}
        static_values = data.get("static_values") or {}
        if not isinstance(filters, dict):
            raise ValueError("Nasdaq table config 'filters' must be an object.")
        if not isinstance(static_values, dict):
            raise ValueError("Nasdaq table config 'static_values' must be an object.")
        return NasdaqTableConfig(
            table=table,
            dataset_name=dataset_name,
            date_column=_strip(data.get("date_column")) or None,
            title_column=_strip(data.get("title_column")) or "event_name",
            effective_at_column=_strip(data.get("effective_at_column")) or None,
            published_at_column=_strip(data.get("published_at_column")) or None,
            updated_at_column=_strip(data.get("updated_at_column")) or None,
            country_column=_strip(data.get("country_column")) or None,
            region_column=_strip(data.get("region_column")) or None,
            currency_column=_strip(data.get("currency_column")) or None,
            actual_column=_strip(data.get("actual_column")) or None,
            consensus_column=_strip(data.get("consensus_column")) or None,
            previous_column=_strip(data.get("previous_column")) or None,
            revised_previous_column=_strip(data.get("revised_previous_column")) or None,
            unit_column=_strip(data.get("unit_column")) or None,
            period_column=_strip(data.get("period_column")) or None,
            frequency_column=_strip(data.get("frequency_column")) or None,
            status_column=_strip(data.get("status_column")) or None,
            importance_column=_strip(data.get("importance_column")) or None,
            event_type=_strip(data.get("event_type")) or "macro_release",
            filters={str(key): value for key, value in filters.items()},
            static_values={str(key): value for key, value in static_values.items()},
        )


@dataclass(frozen=True)
class EconomicCatalystConfig:
    bronze_container: str
    silver_container: str
    gold_container: str
    official_sources: tuple[str, ...]
    vendor_sources: tuple[str, ...]
    structured_lookback_days: int
    headline_lookback_days: int
    future_schedule_days: int
    general_poll_minutes: int
    fred_api_key: str
    alpha_vantage_api_key: str
    massive_api_key: str
    alpaca_key_id: str
    alpaca_secret_key: str
    nasdaq_api_key: str
    alpha_vantage_news_topics: str
    fomc_schedule_urls: tuple[str, ...]
    bls_ics_url: str
    bea_schedule_url: str
    ecb_calendar_url: str
    boe_calendar_url: str
    boj_schedule_url: str
    treasury_auctions_url: str
    nasdaq_table_configs: tuple[NasdaqTableConfig, ...]
    massive_base_url: str
    alpaca_news_base_url: str
    http_timeout_seconds: float

    @staticmethod
    def from_env() -> "EconomicCatalystConfig":
        official_sources = _csv_or_default(
            os.environ.get("ECONOMIC_CATALYST_OFFICIAL_SOURCES", ""),
            tuple(sorted(constants.OFFICIAL_SOURCES)),
        )
        vendor_sources = _csv_or_default(
            os.environ.get("ECONOMIC_CATALYST_VENDOR_SOURCES", ""),
            ("nasdaq_tables",),
        )
        nasdaq_table_configs = tuple(
            NasdaqTableConfig.from_dict(item)
            for item in _json_list(os.environ.get("ECONOMIC_CATALYST_NASDAQ_TABLES", ""))
            if isinstance(item, dict)
        )
        timeout_raw = _env_text("ECONOMIC_CATALYST_HTTP_TIMEOUT_SECONDS", "30")
        try:
            http_timeout_seconds = float(timeout_raw)
        except Exception:
            http_timeout_seconds = 30.0
        return EconomicCatalystConfig(
            bronze_container=_env_text("AZURE_CONTAINER_BRONZE", "bronze"),
            silver_container=_env_text("AZURE_CONTAINER_SILVER", "silver"),
            gold_container=_env_text("AZURE_CONTAINER_GOLD", "gold"),
            official_sources=official_sources,
            vendor_sources=vendor_sources,
            structured_lookback_days=_env_int("ECONOMIC_CATALYST_STRUCTURED_CORRECTION_LOOKBACK_DAYS", 90),
            headline_lookback_days=_env_int("ECONOMIC_CATALYST_HEADLINE_CORRECTION_LOOKBACK_DAYS", 14),
            future_schedule_days=_env_int("ECONOMIC_CATALYST_FUTURE_SCHEDULE_DAYS", 180),
            general_poll_minutes=_env_int("ECONOMIC_CATALYST_GENERAL_POLL_MINUTES", 15),
            fred_api_key=_env_text("FRED_API_KEY"),
            alpha_vantage_api_key=_env_text("ALPHA_VANTAGE_API_KEY"),
            massive_api_key=_env_text("MASSIVE_API_KEY"),
            alpaca_key_id=_env_text("ALPACA_KEY_ID"),
            alpaca_secret_key=_env_text("ALPACA_SECRET_KEY"),
            nasdaq_api_key=_env_text("NASDAQ_API_KEY"),
            alpha_vantage_news_topics=_env_text(
                "ECONOMIC_CATALYST_ALPHA_VANTAGE_TOPICS",
                "economy_macro,financial_markets,fiscal_policy,finance",
            ),
            fomc_schedule_urls=_csv_or_default(
                os.environ.get("ECONOMIC_CATALYST_FOMC_SCHEDULE_URLS", ""),
                constants.DEFAULT_FOMC_SCHEDULE_URLS,
            ),
            bls_ics_url=_env_text("ECONOMIC_CATALYST_BLS_ICS_URL", constants.DEFAULT_BLS_ICS_URL),
            bea_schedule_url=_env_text("ECONOMIC_CATALYST_BEA_SCHEDULE_URL", constants.DEFAULT_BEA_SCHEDULE_URL),
            ecb_calendar_url=_env_text("ECONOMIC_CATALYST_ECB_CALENDAR_URL", constants.DEFAULT_ECB_CALENDAR_URL),
            boe_calendar_url=_env_text("ECONOMIC_CATALYST_BOE_CALENDAR_URL", constants.DEFAULT_BOE_CALENDAR_URL),
            boj_schedule_url=_env_text("ECONOMIC_CATALYST_BOJ_SCHEDULE_URL", constants.DEFAULT_BOJ_SCHEDULE_URL),
            treasury_auctions_url=_env_text(
                "ECONOMIC_CATALYST_TREASURY_AUCTIONS_URL", constants.DEFAULT_TREASURY_AUCTIONS_URL
            ),
            nasdaq_table_configs=nasdaq_table_configs,
            massive_base_url=_env_text("MASSIVE_BASE_URL", "https://api.massive.com"),
            alpaca_news_base_url=_env_text("ALPACA_NEWS_BASE_URL", "https://data.alpaca.markets/v1beta1"),
            http_timeout_seconds=http_timeout_seconds,
        )

    def requested_sources(self) -> tuple[str, ...]:
        requested = [*self.official_sources, *self.vendor_sources]
        return tuple(dict.fromkeys(source for source in requested if source in constants.ALL_SOURCES))

    def enabled_sources(self) -> tuple[str, ...]:
        enabled: list[str] = []
        for source_name in self.requested_sources():
            if source_name == "fred_releases" and not self.fred_api_key:
                continue
            if source_name == "nasdaq_tables" and (not self.nasdaq_api_key or not self.nasdaq_table_configs):
                continue
            if source_name == "massive_news" and not self.massive_api_key:
                continue
            if source_name == "alpaca_news" and (not self.alpaca_key_id or not self.alpaca_secret_key):
                continue
            if source_name == "alpha_vantage_news" and not self.alpha_vantage_api_key:
                continue
            enabled.append(source_name)
        return tuple(enabled)

    def missing_credentials(self) -> dict[str, str]:
        missing: dict[str, str] = {}
        for source_name in self.requested_sources():
            if source_name == "fred_releases" and not self.fred_api_key:
                missing[source_name] = "FRED_API_KEY is not configured."
            elif source_name == "nasdaq_tables":
                if not self.nasdaq_api_key:
                    missing[source_name] = "NASDAQ_API_KEY is not configured."
                elif not self.nasdaq_table_configs:
                    missing[source_name] = "ECONOMIC_CATALYST_NASDAQ_TABLES is empty."
            elif source_name == "massive_news" and not self.massive_api_key:
                missing[source_name] = "MASSIVE_API_KEY is not configured."
            elif source_name == "alpaca_news" and (not self.alpaca_key_id or not self.alpaca_secret_key):
                missing[source_name] = "ALPACA_KEY_ID or ALPACA_SECRET_KEY is not configured."
            elif source_name == "alpha_vantage_news" and not self.alpha_vantage_api_key:
                missing[source_name] = "ALPHA_VANTAGE_API_KEY is not configured."
        return missing

    def structured_window_start(self, *, now: datetime | None = None) -> datetime:
        anchor = now or datetime.now(timezone.utc)
        return anchor - timedelta(days=max(self.structured_lookback_days, 1))

    def headline_window_start(self, *, now: datetime | None = None) -> datetime:
        anchor = now or datetime.now(timezone.utc)
        return anchor - timedelta(days=max(self.headline_lookback_days, 1))

    def future_window_end(self, *, now: datetime | None = None) -> datetime:
        anchor = now or datetime.now(timezone.utc)
        return anchor + timedelta(days=max(self.future_schedule_days, 1))
