from __future__ import annotations

from tasks.common import symbol_availability


def test_normalize_massive_records_skips_invalid_provider_symbols(monkeypatch) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(symbol_availability.mdc, "write_warning", lambda message: warnings.append(str(message)))

    df = symbol_availability._normalize_massive_records(
        [
            {"ticker": "AAPL", "type": "CS"},
            {"ticker": "N/A", "type": "CS"},
            {"ticker": "UNKNOWN", "type": "CS"},
            {"ticker": "MSFT", "type": "CS"},
        ]
    )

    assert df["Symbol"].tolist() == ["AAPL", "MSFT"]
    assert df.attrs["alias_resolution_failure_count"] == 2
    assert any("Massive ticker sync skipped invalid symbol records: count=2" in message for message in warnings)


def test_sync_domain_availability_uses_tolerant_massive_normalizer(monkeypatch) -> None:
    warnings: list[str] = []
    original_normalizer = symbol_availability._owner._normalize_massive_records
    observed: dict[str, object] = {}

    monkeypatch.setattr(symbol_availability.mdc, "write_warning", lambda message: warnings.append(str(message)))

    def _fake_sync(domain: str):
        df = symbol_availability._owner._normalize_massive_records(
            [
                {"ticker": "SPY", "type": "ETF"},
                {"ticker": "NA", "type": "ETF"},
            ]
        )
        observed["domain"] = domain
        observed["symbols"] = df["Symbol"].tolist()
        observed["failures"] = df.attrs["alias_resolution_failure_count"]
        return symbol_availability.SyncResult(
            provider="massive",
            source_column="source_massive",
            listed_count=len(df),
            inserted_count=0,
            disabled_count=0,
            duration_ms=1,
            lock_wait_ms=0,
            alias_resolution_failure_count=int(df.attrs["alias_resolution_failure_count"]),
        )

    monkeypatch.setattr(symbol_availability._owner, "sync_domain_availability", _fake_sync)

    result = symbol_availability.sync_domain_availability("market")

    assert observed == {"domain": "market", "symbols": ["SPY"], "failures": 1}
    assert result.alias_resolution_failure_count == 1
    assert symbol_availability._owner._normalize_massive_records is original_normalizer
    assert any("count=1" in message for message in warnings)

