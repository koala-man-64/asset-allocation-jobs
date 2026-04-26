from __future__ import annotations

from datetime import datetime, timezone

from tasks.economic_catalyst_data.config import EconomicCatalystConfig, NasdaqTableConfig
from tasks.economic_catalyst_data.sources import RawSourceBatch, fetch_requested_sources
from tasks.economic_catalyst_data import sources as source_module


def _config(
    *,
    fred_api_key: str = "",
    massive_api_key: str = "massive",
    nasdaq_api_key: str = "nasdaq",
    nasdaq_tables: tuple[NasdaqTableConfig, ...] | None = None,
) -> EconomicCatalystConfig:
    return EconomicCatalystConfig(
        bronze_container="bronze",
        silver_container="silver",
        gold_container="gold",
        official_sources=("fred_releases",),
        vendor_sources=("massive_news", "nasdaq_tables"),
        structured_lookback_days=90,
        headline_lookback_days=14,
        future_schedule_days=180,
        general_poll_minutes=15,
        fred_api_key=fred_api_key,
        alpha_vantage_api_key="",
        massive_api_key=massive_api_key,
        alpaca_key_id="",
        alpaca_secret_key="",
        nasdaq_api_key=nasdaq_api_key,
        alpha_vantage_news_topics="economy_macro",
        fomc_schedule_urls=("https://example.com/fomc",),
        bls_ics_url="https://example.com/bls.ics",
        bea_schedule_url="https://example.com/bea",
        ecb_calendar_url="https://example.com/ecb",
        boe_calendar_url="https://example.com/boe",
        boj_schedule_url="https://example.com/boj",
        treasury_auctions_url="https://example.com/treasury.xml",
        nasdaq_table_configs=nasdaq_tables or (NasdaqTableConfig(table="DB/TEST", dataset_name="macro_table"),),
        massive_base_url="https://api.massive.com",
        alpaca_news_base_url="https://data.alpaca.markets/v1beta1",
        http_timeout_seconds=30.0,
    )


def test_fetch_requested_sources_respects_selected_sources_and_reports_missing_creds(
    monkeypatch,
) -> None:
    called: list[str] = []

    def _ok_fetcher(config: EconomicCatalystConfig, now: datetime) -> list[RawSourceBatch]:
        called.append("massive_news")
        return [
            RawSourceBatch(
                source_name="massive_news",
                dataset_name="benzinga_news",
                fetched_at=now.isoformat(),
                request_url="https://api.massive.com/benzinga/v2/news",
                payload_format="json",
                payload={"results": []},
                metadata={},
            )
        ]

    def _failing_fetcher(config: EconomicCatalystConfig, now: datetime) -> list[RawSourceBatch]:
        called.append("nasdaq_tables")
        raise RuntimeError("entitlement denied")

    monkeypatch.setitem(source_module._FETCHERS, "massive_news", _ok_fetcher)
    monkeypatch.setitem(source_module._FETCHERS, "nasdaq_tables", _failing_fetcher)
    monkeypatch.setattr(source_module.mdc, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(source_module.mdc, "write_error", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(source_module.mdc, "write_warning", lambda *_args, **_kwargs: None)

    batches, warnings, failures = fetch_requested_sources(
        _config(fred_api_key=""),
        now=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        source_names=("fred_releases", "massive_news", "nasdaq_tables"),
    )

    assert [batch.source_name for batch in batches] == ["massive_news"]
    assert any("fred_releases: FRED_API_KEY is not configured." in warning for warning in warnings)
    assert failures == ["nasdaq_tables: RuntimeError: entitlement denied"]
    assert called == ["massive_news", "nasdaq_tables"]


def test_fetch_requested_sources_sanitizes_failure_details(monkeypatch) -> None:
    def _failing_fetcher(config: EconomicCatalystConfig, now: datetime) -> list[RawSourceBatch]:
        raise RuntimeError("GET https://example.com/feed?api_key=secret-token failed with Bearer abc123")

    monkeypatch.setitem(source_module._FETCHERS, "massive_news", _failing_fetcher)
    monkeypatch.setattr(source_module.mdc, "write_warning", lambda *_args, **_kwargs: None)

    _, _, failures = fetch_requested_sources(
        _config(),
        now=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        source_names=("massive_news",),
    )

    assert failures == ["massive_news: RuntimeError: GET <url> failed with Bearer <redacted>"]
    assert "secret-token" not in failures[0]
