import json

import pandas as pd
import pytest

import pandas as pd
import pytest

from tasks.common import bronze_symbol_policy as policy


class _FakeCommonClient:
    def __init__(self, blobs: dict[str, bytes]) -> None:
        self._blobs = blobs
        self.deleted_paths: list[str] = []

    def delete_file(self, path: str) -> None:
        self.deleted_paths.append(path)
        self._blobs.pop(path, None)

    def list_blob_infos(self, name_starts_with=None) -> list[dict[str, str]]:
        prefix = str(name_starts_with or "")
        return [{"name": name} for name in sorted(self._blobs) if name.startswith(prefix)]


def _install_storage_stubs(monkeypatch):
    blobs: dict[str, bytes] = {}
    update_calls: list[tuple[str, str, object]] = []
    stored_csvs: dict[str, pd.DataFrame] = {}
    blacklist_rows: dict[str, list[str]] = {}

    def fake_read_raw_bytes(path, client=None, **kwargs):
        del client
        del kwargs
        if path not in blobs:
            raise FileNotFoundError(path)
        return blobs[path]

    def fake_store_raw_bytes(raw, path, client=None):
        del client
        blobs[path] = raw

    def fake_update_csv_set(path, symbol, client=None):
        update_calls.append((path, symbol, client))

    def fake_load_ticker_list(path, client=None):
        del client
        return list(blacklist_rows.get(path, []))

    def fake_store_csv(df, path, client=None):
        del client
        stored_csvs[path] = df.copy()
        if df.empty:
            blacklist_rows[path] = []
            return path
        column = "Symbol" if "Symbol" in df.columns else df.columns[0]
        blacklist_rows[path] = [str(value).strip().upper() for value in df[column].dropna().tolist()]
        return path

    monkeypatch.setattr(policy.mdc, "read_raw_bytes", fake_read_raw_bytes)
    monkeypatch.setattr(policy.mdc, "store_raw_bytes", fake_store_raw_bytes)
    monkeypatch.setattr(policy.mdc, "update_csv_set", fake_update_csv_set)
    monkeypatch.setattr(policy.mdc, "load_ticker_list", fake_load_ticker_list)
    monkeypatch.setattr(policy.mdc, "store_csv", fake_store_csv)
    return blobs, update_calls, stored_csvs, blacklist_rows


def test_record_invalid_symbol_candidate_creates_marker_without_blacklist_on_first_run(monkeypatch):
    blobs, update_calls, _, _ = _install_storage_stubs(monkeypatch)

    result = policy.record_invalid_symbol_candidate(
        common_client=object(),
        bronze_client=object(),
        domain="finance",
        symbol="aapl",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )

    assert result["promoted"] is False
    assert result["observedRunCount"] == 1
    assert update_calls == []

    marker_path = policy.invalid_candidate_marker_path(domain="finance", symbol="AAPL")
    stored = json.loads(blobs[marker_path].decode("utf-8"))
    assert stored["status"] == "candidate"
    assert stored["observedRunCount"] == 1
    assert stored["symbol"] == "AAPL"
    assert stored["blacklistPath"] is None


def test_record_invalid_symbol_candidate_dedupes_same_run_and_promotes_on_second_run(monkeypatch):
    blobs, update_calls, _, _ = _install_storage_stubs(monkeypatch)
    common_client = object()
    bronze_client = object()

    first = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )
    duplicate = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )
    promoted = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-2",
    )

    assert first["promoted"] is False
    assert duplicate["promoted"] is False
    assert duplicate["observedRunCount"] == 1
    assert promoted["promoted"] is True
    assert promoted["observedRunCount"] == 2
    assert update_calls == [("market-data/blacklist.csv", "MSFT", bronze_client)]

    marker_path = policy.invalid_candidate_marker_path(domain="market", symbol="MSFT")
    stored = json.loads(blobs[marker_path].decode("utf-8"))
    assert stored["status"] == "promoted"
    assert stored["observedRunCount"] == 2
    assert stored["blacklistPath"] == "market-data/blacklist.csv"
    assert stored["promotedAt"]


def test_clear_invalid_symbol_state_on_success_clears_candidate_marker(monkeypatch):
    blobs, _, _, _ = _install_storage_stubs(monkeypatch)
    common_client = _FakeCommonClient(blobs)
    candidate_path = policy.invalid_candidate_marker_path(domain="earnings", symbol="AAPL")
    blobs[candidate_path] = json.dumps(
        {
            "status": "candidate",
            "observedRunCount": 1,
        }
    ).encode("utf-8")

    result = policy.clear_invalid_symbol_state_on_success(
        common_client=common_client,
        bronze_client=object(),
        domain="earnings",
        symbol="AAPL",
    )

    assert result == {"cleared": True, "recovered": False, "blacklistPath": None}
    assert common_client.deleted_paths == [candidate_path]
    assert candidate_path not in blobs


def test_clear_invalid_symbol_state_on_success_recovers_promoted_symbol(monkeypatch):
    blobs, _, stored_csvs, blacklist_rows = _install_storage_stubs(monkeypatch)
    common_client = _FakeCommonClient(blobs)
    bronze_client = object()
    promoted_path = policy.invalid_candidate_marker_path(domain="finance", symbol="AAPL")
    blobs[promoted_path] = json.dumps(
        {
            "status": "promoted",
            "observedRunCount": 2,
            "blacklistPath": "finance-data/blacklist.csv",
        }
    ).encode("utf-8")
    blacklist_rows["finance-data/blacklist.csv"] = ["AAPL", "MSFT"]

    result = policy.clear_invalid_symbol_state_on_success(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="finance",
        symbol="AAPL",
    )

    assert result == {
        "cleared": True,
        "recovered": True,
        "blacklistPath": "finance-data/blacklist.csv",
    }
    assert common_client.deleted_paths == [promoted_path]
    assert promoted_path not in blobs
    assert stored_csvs["finance-data/blacklist.csv"]["Symbol"].tolist() == ["MSFT"]


def test_record_invalid_symbol_candidate_resets_count_when_reason_changes(monkeypatch):
    blobs, update_calls, _, _ = _install_storage_stubs(monkeypatch)
    common_client = object()
    bronze_client = object()

    first_reason = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_invalid_symbol",
        run_id="run-1",
    )
    second_reason_first_run = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_no_market_history",
        run_id="run-2",
    )
    second_reason_promoted = policy.record_invalid_symbol_candidate(
        common_client=common_client,
        bronze_client=bronze_client,
        domain="market",
        symbol="msft",
        provider="massive",
        reason_code="provider_no_market_history",
        run_id="run-3",
    )

    assert first_reason["promoted"] is False
    assert first_reason["observedRunCount"] == 1
    assert second_reason_first_run["promoted"] is False
    assert second_reason_first_run["observedRunCount"] == 1
    assert second_reason_promoted["promoted"] is True
    assert second_reason_promoted["observedRunCount"] == 2
    assert update_calls == [("market-data/blacklist.csv", "MSFT", bronze_client)]

    marker_path = policy.invalid_candidate_marker_path(domain="market", symbol="MSFT")
    stored = json.loads(blobs[marker_path].decode("utf-8"))
    assert stored["reasonCode"] == "provider_no_market_history"


def test_list_promoted_invalid_candidate_markers_filters_and_sorts_reprobe_priority(monkeypatch):
    blobs, _, _, _ = _install_storage_stubs(monkeypatch)
    blobs[policy.invalid_candidate_marker_path(domain="market", symbol="AAPL")] = json.dumps(
        {
            "status": "promoted",
            "symbol": "AAPL",
            "promotedAt": "2026-04-17T10:00:00+00:00",
        }
    ).encode("utf-8")
    blobs[policy.invalid_candidate_marker_path(domain="market", symbol="MSFT")] = json.dumps(
        {
            "status": "promoted",
            "symbol": "MSFT",
            "promotedAt": "2026-04-18T10:00:00+00:00",
            "lastReprobeAt": "2026-04-18T12:00:00+00:00",
            "reprobeAttemptCount": 1,
        }
    ).encode("utf-8")
    blobs[policy.invalid_candidate_marker_path(domain="market", symbol="GOOG")] = json.dumps(
        {
            "status": "candidate",
            "symbol": "GOOG",
            "promotedAt": None,
        }
    ).encode("utf-8")
    blobs[policy.invalid_candidate_marker_path(domain="market", symbol="TSLA")] = json.dumps(
        {
            "status": "promoted",
            "symbol": "TSLA",
            "promotedAt": "2026-04-16T10:00:00+00:00",
            "lastReprobeAt": "2026-04-18T11:00:00+00:00",
            "reprobeAttemptCount": 2,
        }
    ).encode("utf-8")

    common_client = _FakeCommonClient(blobs)

    markers = policy.list_promoted_invalid_candidate_markers(common_client=common_client, domain="market")

    assert [marker["symbol"] for marker in markers] == ["AAPL", "TSLA", "MSFT"]


def test_record_promoted_symbol_reprobe_attempt_updates_marker_metadata(monkeypatch):
    blobs, _, _, _ = _install_storage_stubs(monkeypatch)
    marker_path = policy.invalid_candidate_marker_path(domain="market", symbol="AAPL")
    blobs[marker_path] = json.dumps(
        {
            "status": "promoted",
            "symbol": "AAPL",
            "reprobeAttemptCount": 1,
        }
    ).encode("utf-8")

    updated = policy.record_promoted_symbol_reprobe_attempt(
        common_client=_FakeCommonClient(blobs),
        domain="market",
        symbol="AAPL",
        outcome="still_invalid_symbol",
    )

    assert updated["lastReprobeOutcome"] == "still_invalid_symbol"
    assert updated["reprobeAttemptCount"] == 2
    stored = json.loads(blobs[marker_path].decode("utf-8"))
    assert stored["reprobeAttemptCount"] == 2
    assert stored["lastReprobeAt"]


def test_list_promoted_invalid_candidate_markers_raises_on_corrupt_json(monkeypatch):
    blobs, _, _, _ = _install_storage_stubs(monkeypatch)
    marker_path = policy.invalid_candidate_marker_path(domain="market", symbol="AAPL")
    blobs[marker_path] = b"{not-json"

    with pytest.raises(RuntimeError, match="invalid JSON"):
        policy.list_promoted_invalid_candidate_markers(
            common_client=_FakeCommonClient(blobs),
            domain="market",
        )


def test_validate_bronze_storage_clients_requires_common_state() -> None:
    with pytest.raises(ValueError, match="AZURE_CONTAINER_COMMON"):
        policy.validate_bronze_storage_clients(
            bronze_container_name="bronze",
            common_container_name="",
            bronze_client=object(),
            common_client=object(),
        )

    with pytest.raises(RuntimeError, match="Common storage client is unavailable"):
        policy.validate_bronze_storage_clients(
            bronze_container_name="bronze",
            common_container_name="common",
            bronze_client=object(),
            common_client=None,
        )
