from __future__ import annotations

import pandas as pd

from tasks.common import bronze_alpha26_publish as publish


def _market_bucket_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "date": [pd.Timestamp("2026-01-02")],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
        }
    )


def test_normalize_bucket_frames_preserves_nonempty_frame_identity() -> None:
    frame = _market_bucket_frame()

    normalized = publish._normalize_bucket_frames(
        bucket_frames={"A": frame},
        bucket_columns=frame.columns,
    )

    assert normalized["A"] is frame
    assert list(normalized["B"].columns) == list(frame.columns)


def test_write_alpha26_bronze_bucket_defers_active_bucket_artifact_until_finalize(monkeypatch) -> None:
    stored_bytes: dict[str, bytes] = {}

    monkeypatch.setattr(
        publish.mdc,
        "store_raw_bytes",
        lambda payload, path, client=None: stored_bytes.__setitem__(str(path), bytes(payload)),
    )
    monkeypatch.setattr(publish.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(
        publish.domain_artifacts,
        "write_bucket_artifact",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("bucket artifact must be deferred")),
    )

    frame = _market_bucket_frame()
    session = publish.start_alpha26_bronze_publish(
        domain="market",
        root_prefix="market-data",
        bucket_columns=frame.columns,
        date_column="date",
        storage_client=object(),
        job_name="bronze-market-job",
        run_id="run-123",
    )

    entry = publish.write_alpha26_bronze_bucket(
        session,
        bucket="A",
        frame=frame,
        symbol_to_bucket={"AAPL": "A"},
    )

    assert entry["bucket"] == "A"
    assert entry["size"] > 0
    assert len(stored_bytes) == 1
    assert session.bucket_artifacts["A"]["dataPath"] == "market-data/runs/run-123/buckets/A.parquet"
    assert session.symbol_to_bucket == {"AAPL": "A"}
    assert session.total_bytes == entry["size"]


def test_finalize_alpha26_bronze_publish_returns_publish_result_contract(monkeypatch) -> None:
    saved_payloads: dict[str, dict[str, object]] = {}
    domain_artifact_calls: list[dict[str, object]] = []

    monkeypatch.setattr(publish.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(publish.mdc, "store_raw_bytes", lambda payload, path, client=None: None)
    monkeypatch.setattr(
        publish.mdc,
        "save_json_content",
        lambda data, file_path, client=None: saved_payloads.__setitem__(str(file_path), dict(data)),
    )
    monkeypatch.setattr(
        publish.domain_artifacts,
        "bucket_artifact_path",
        lambda **kwargs: f"metadata/{kwargs['bucket']}.json",
    )
    monkeypatch.setattr(publish.domain_artifacts, "root_prefix", lambda **_kwargs: "market-data")
    monkeypatch.setattr(
        publish.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: domain_artifact_calls.append(dict(kwargs)) or {"artifactPath": "market-data/_metadata/domain.json"},
    )
    monkeypatch.setattr(
        publish.bronze_bucketing,
        "write_symbol_index",
        lambda domain, symbol_to_bucket: "system/bronze-index/market/latest.parquet",
    )
    monkeypatch.setattr(
        publish.run_manifests,
        "create_bronze_alpha26_manifest",
        lambda **kwargs: {"manifestPath": "system/manifests/bronze/market/run-123.json"},
    )

    frame = _market_bucket_frame()
    session = publish.start_alpha26_bronze_publish(
        domain="market",
        root_prefix="market-data",
        bucket_columns=frame.columns,
        date_column="date",
        storage_client=object(),
        job_name="bronze-market-job",
        run_id="run-123",
    )
    publish.write_alpha26_bronze_bucket(
        session,
        bucket="A",
        frame=frame,
        symbol_to_bucket={"AAPL": "A"},
    )

    result = publish.finalize_alpha26_bronze_publish(session)

    assert result.run_id == "run-123"
    assert result.data_prefix == "market-data/runs/run-123"
    assert result.index_path == "system/bronze-index/market/latest.parquet"
    assert result.manifest_path == "system/manifests/bronze/market/run-123.json"
    assert result.written_symbols == 1
    assert result.file_count == 1
    assert saved_payloads["metadata/A.json"]["manifestPath"] == "system/manifests/bronze/market/run-123.json"
    assert domain_artifact_calls[0]["symbol_count_override"] == 1
    assert domain_artifact_calls[0]["file_count_override"] == 1


def test_publish_alpha26_bronze_domain_wrapper_remains_compatible(monkeypatch) -> None:
    stored_paths: list[str] = []
    saved_payloads: dict[str, dict[str, object]] = {}

    monkeypatch.setattr(publish.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(
        publish.mdc,
        "store_raw_bytes",
        lambda payload, path, client=None: stored_paths.append(str(path)),
    )
    monkeypatch.setattr(
        publish.mdc,
        "save_json_content",
        lambda data, file_path, client=None: saved_payloads.__setitem__(str(file_path), dict(data)),
    )
    monkeypatch.setattr(
        publish.domain_artifacts,
        "bucket_artifact_path",
        lambda **kwargs: f"metadata/{kwargs['bucket']}.json",
    )
    monkeypatch.setattr(publish.domain_artifacts, "root_prefix", lambda **_kwargs: "market-data")
    monkeypatch.setattr(
        publish.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: {"artifactPath": "market-data/_metadata/domain.json"},
    )
    monkeypatch.setattr(
        publish.bronze_bucketing,
        "write_symbol_index",
        lambda domain, symbol_to_bucket: "system/bronze-index/market/latest.parquet",
    )
    monkeypatch.setattr(
        publish.run_manifests,
        "create_bronze_alpha26_manifest",
        lambda **kwargs: {"manifestPath": "system/manifests/bronze/market/run-compat.json"},
    )

    frame = _market_bucket_frame()
    result = publish.publish_alpha26_bronze_domain(
        domain="market",
        root_prefix="market-data",
        bucket_frames={"A": frame},
        bucket_columns=frame.columns,
        date_column="date",
        symbol_to_bucket={"AAPL": "A"},
        storage_client=object(),
        job_name="bronze-market-job",
        run_id="run-compat",
    )

    assert result.run_id == "run-compat"
    assert result.file_count == 26
    assert result.written_symbols == 1
    assert len(stored_paths) == 26
    assert "market-data/runs/run-compat/buckets/A.parquet" in stored_paths
    assert saved_payloads["metadata/A.json"]["manifestPath"] == "system/manifests/bronze/market/run-compat.json"


def test_scoped_bronze_publish_merges_prior_index_and_preserves_untouched_manifest_paths(monkeypatch) -> None:
    stored_paths: list[str] = []
    saved_payloads: dict[str, dict[str, object]] = {}
    index_maps: list[dict[str, str]] = []
    manifest_bucket_paths: list[list[dict[str, object]]] = []

    monkeypatch.setattr(publish.mdc, "write_line", lambda _message: None)
    monkeypatch.setattr(
        publish.mdc,
        "store_raw_bytes",
        lambda payload, path, client=None: stored_paths.append(str(path)),
    )
    monkeypatch.setattr(
        publish.mdc,
        "save_json_content",
        lambda data, file_path, client=None: saved_payloads.__setitem__(str(file_path), dict(data)),
    )
    monkeypatch.setattr(
        publish.domain_artifacts,
        "bucket_artifact_path",
        lambda **kwargs: f"metadata/{kwargs['bucket']}.json",
    )
    monkeypatch.setattr(publish.domain_artifacts, "root_prefix", lambda **_kwargs: "market-data")
    monkeypatch.setattr(
        publish.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: {"artifactPath": "market-data/_metadata/domain.json"},
    )

    def _write_symbol_index(*, domain: str, symbol_to_bucket: dict[str, str]) -> str:
        index_maps.append(dict(symbol_to_bucket))
        return f"system/bronze-index/{domain}/latest.parquet"

    def _create_manifest(**kwargs):
        manifest_bucket_paths.append([dict(item) for item in kwargs["bucket_paths"]])
        return {"manifestPath": "system/manifests/bronze/market/run-scoped.json"}

    monkeypatch.setattr(publish.bronze_bucketing, "write_symbol_index", _write_symbol_index)
    monkeypatch.setattr(publish.run_manifests, "create_bronze_alpha26_manifest", _create_manifest)

    frame = _market_bucket_frame()
    session = publish.start_alpha26_bronze_publish(
        domain="market",
        root_prefix="market-data",
        bucket_columns=frame.columns,
        date_column="date",
        storage_client=object(),
        job_name="bronze-market-job",
        run_id="run-scoped",
        scope_mode="intraday",
        touched_buckets={"A"},
        active_symbol_to_bucket={"AAPL": "A", "AMZN": "A", "MSFT": "M"},
        active_bucket_paths=[
            {"bucket": "A", "name": "market-data/runs/prior/buckets/A.parquet", "size": 10},
            {"bucket": "M", "name": "market-data/runs/prior/buckets/M.parquet", "size": 20},
        ],
    )
    publish.write_alpha26_bronze_bucket(
        session,
        bucket="A",
        frame=frame,
        symbol_to_bucket={"AAPL": "A"},
    )
    skipped = publish.write_alpha26_bronze_bucket(
        session,
        bucket="M",
        frame=pd.DataFrame(
            {
                "symbol": ["MSFT"],
                "date": [pd.Timestamp("2026-01-02")],
                "open": [20.0],
                "high": [21.0],
                "low": [19.0],
                "close": [20.5],
            }
        ),
        symbol_to_bucket={"MSFT": "M"},
    )

    result = publish.finalize_alpha26_bronze_publish(session)

    assert skipped["skipped"] is True
    assert stored_paths == ["market-data/runs/run-scoped/buckets/A.parquet"]
    assert index_maps == [{"AAPL": "A", "AMZN": "A", "MSFT": "M"}]
    assert result.written_symbols == 3
    assert result.file_count == 2
    assert manifest_bucket_paths[0] == [
        {"bucket": "A", "name": "market-data/runs/run-scoped/buckets/A.parquet", "size": result.bucket_paths[0]["size"]},
        {"bucket": "M", "name": "market-data/runs/prior/buckets/M.parquet", "size": 20},
    ]
    assert set(saved_payloads) == {"metadata/A.json"}
