from __future__ import annotations

from datetime import datetime, timezone

from tasks.economic_catalyst_data.bronze_economic_catalyst_data import _selected_sources
from tasks.economic_catalyst_data.config import EconomicCatalystConfig, NasdaqTableConfig


def _config() -> EconomicCatalystConfig:
    return EconomicCatalystConfig(
        bronze_container="bronze",
        silver_container="silver",
        gold_container="gold",
        official_sources=("fred_releases", "bls_release_calendar", "bea_release_schedule"),
        vendor_sources=("nasdaq_tables", "massive_news", "alpaca_news", "alpha_vantage_news"),
        structured_lookback_days=90,
        headline_lookback_days=14,
        future_schedule_days=180,
        general_poll_minutes=15,
        fred_api_key="fred",
        alpha_vantage_api_key="av",
        massive_api_key="massive",
        alpaca_key_id="alpaca-key",
        alpaca_secret_key="alpaca-secret",
        nasdaq_api_key="nasdaq",
        alpha_vantage_news_topics="economy_macro",
        fomc_schedule_urls=("https://example.com/fomc",),
        bls_ics_url="https://example.com/bls.ics",
        bea_schedule_url="https://example.com/bea",
        ecb_calendar_url="https://example.com/ecb",
        boe_calendar_url="https://example.com/boe",
        boj_schedule_url="https://example.com/boj",
        treasury_auctions_url="https://example.com/treasury.xml",
        nasdaq_table_configs=(NasdaqTableConfig(table="DB/TEST", dataset_name="macro_table"),),
        massive_base_url="https://api.massive.com",
        alpaca_news_base_url="https://data.alpaca.markets/v1beta1",
        http_timeout_seconds=30.0,
    )


def test_selected_sources_uses_general_poll_on_full_cycle() -> None:
    poll_mode, selected = _selected_sources(
        _config(),
        now=datetime(2026, 4, 18, 12, 30, tzinfo=timezone.utc),
    )

    assert poll_mode == "general"
    assert selected == _config().enabled_sources()


def test_selected_sources_limits_hot_window_runs_to_fast_sources() -> None:
    poll_mode, selected = _selected_sources(
        _config(),
        now=datetime(2026, 4, 18, 12, 31, tzinfo=timezone.utc),
    )

    assert poll_mode == "hot_window"
    assert selected == (
        "fred_releases",
        "nasdaq_tables",
        "massive_news",
        "alpaca_news",
        "alpha_vantage_news",
    )
