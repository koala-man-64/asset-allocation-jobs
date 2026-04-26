from __future__ import annotations

from datetime import datetime, timezone

from tasks.economic_catalyst_data import bronze_economic_catalyst_data as bronze
from tasks.economic_catalyst_data.config import EconomicCatalystConfig, NasdaqTableConfig
from tasks.economic_catalyst_data.sources import RawSourceBatch

_selected_sources = bronze._selected_sources


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


def test_source_failure_decision_warns_for_optional_outage_when_source_succeeds() -> None:
    should_fail, reason, failed, succeeded = bronze._source_failure_decision(
        selected_sources=("fred_releases", "massive_news", "nasdaq_tables"),
        failures=["nasdaq_tables: RuntimeError: entitlement denied"],
    )

    assert should_fail is False
    assert reason == "partial_source_outage"
    assert failed == {"nasdaq_tables"}
    assert succeeded == {"fred_releases", "massive_news"}


def test_source_failure_decision_fails_when_required_sources_all_fail() -> None:
    should_fail, reason, failed, succeeded = bronze._source_failure_decision(
        selected_sources=("fred_releases", "bls_release_calendar", "massive_news"),
        failures=[
            "fred_releases: RuntimeError: outage",
            "bls_release_calendar: RuntimeError: outage",
        ],
    )

    assert should_fail is True
    assert reason == "all_required_sources_failed"
    assert failed == {"fred_releases", "bls_release_calendar"}
    assert succeeded == {"massive_news"}


def test_main_downgrades_optional_source_failure_and_saves_success(monkeypatch) -> None:
    config = _config()
    batch = RawSourceBatch(
        source_name="massive_news",
        dataset_name="benzinga_news",
        fetched_at="2026-04-18T12:00:00Z",
        request_url="https://api.massive.com/benzinga/v2/news",
        payload_format="json",
        payload={"results": []},
        metadata={},
    )
    manifest_payloads: list[dict[str, object]] = []
    saved_success: list[dict[str, object]] = []

    monkeypatch.setattr(bronze.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(bronze.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(bronze.mdc, "write_error", lambda _message: None)
    monkeypatch.setattr(bronze.mdc, "write_warning", lambda _message: None)
    monkeypatch.setattr(bronze.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(bronze.EconomicCatalystConfig, "from_env", staticmethod(lambda: config))
    monkeypatch.setattr(
        bronze,
        "fetch_requested_sources",
        lambda *_args, **_kwargs: ([batch], [], ["nasdaq_tables: RuntimeError: entitlement denied"]),
    )
    monkeypatch.setattr(bronze, "_persist_source_batches", lambda **_kwargs: ["economic-catalyst/raw.json"])
    monkeypatch.setattr(
        bronze,
        "_persist_manifest",
        lambda **kwargs: manifest_payloads.append({"warnings": kwargs["warnings"], "failures": kwargs["failures"]})
        or {"manifest": "ok"},
    )
    monkeypatch.setattr(bronze, "write_domain_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(bronze, "save_last_success", lambda _key, metadata: saved_success.append(dict(metadata)))

    assert bronze.main() == 0
    assert manifest_payloads[0]["failures"] == []
    assert "optional_source_outage: nasdaq_tables: RuntimeError: entitlement denied" in manifest_payloads[0]["warnings"]
    assert saved_success and saved_success[0]["status"] == "succeededWithWarnings"


def test_main_does_not_save_last_success_on_failed_source_run(monkeypatch) -> None:
    config = EconomicCatalystConfig(
        **{**_config().__dict__, "official_sources": (), "vendor_sources": ()}
    )
    saved_success: list[dict[str, object]] = []

    monkeypatch.setattr(bronze.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(bronze.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(bronze.mdc, "write_error", lambda _message: None)
    monkeypatch.setattr(bronze.mdc, "write_warning", lambda _message: None)
    monkeypatch.setattr(bronze.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(bronze.EconomicCatalystConfig, "from_env", staticmethod(lambda: config))
    monkeypatch.setattr(bronze, "fetch_requested_sources", lambda *_args, **_kwargs: ([], [], []))
    monkeypatch.setattr(bronze, "_persist_source_batches", lambda **_kwargs: [])
    monkeypatch.setattr(bronze, "_persist_manifest", lambda **_kwargs: {})
    monkeypatch.setattr(bronze, "write_domain_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(bronze, "save_last_success", lambda _key, metadata: saved_success.append(dict(metadata)))

    assert bronze.main() == 1
    assert saved_success == []
