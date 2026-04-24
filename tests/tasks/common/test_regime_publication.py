from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from tasks.common import regime_publication


def test_finalize_regime_publication_reuses_shared_publish_state_across_surfaces(monkeypatch) -> None:
    messages: list[str] = []
    captured: dict[str, object] = {}
    marker_metadata: list[dict[str, object]] = []
    saved_watermarks: list[tuple[str, dict[str, object]]] = []
    saved_last_success: list[tuple[str, datetime, dict[str, object]]] = []

    monkeypatch.setattr(regime_publication.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        regime_publication.domain_artifacts,
        "publish_domain_artifact_payload",
        lambda *, payload, client=None: captured.update({"payload": dict(payload), "client": client})
        or {"artifactPath": "regime/_metadata/domain.json"},
    )
    monkeypatch.setattr(
        regime_publication,
        "save_watermarks",
        lambda key, items: saved_watermarks.append((key, dict(items))),
    )
    monkeypatch.setattr(
        regime_publication,
        "save_last_success",
        lambda key, *, when, metadata=None: saved_last_success.append((key, when, dict(metadata or {}))),
    )
    monkeypatch.setattr(
        regime_publication,
        "write_system_health_marker",
        lambda *, layer, domain, job_name="", metadata=None: marker_metadata.append(dict(metadata or {})) or True,
    )
    monkeypatch.setattr(regime_publication.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(regime_publication.mdc, "write_error", lambda msg: messages.append(str(msg)))

    publish_state = regime_publication.build_regime_publish_state(
        published_as_of_date="2026-03-19",
        input_as_of_date="2026-03-19",
        history_rows=2,
        latest_rows=1,
        transition_rows=1,
        active_models=[{"model_name": "default-regime", "model_version": 2}],
        downstream_triggered=True,
        warnings=["none"],
    )
    when = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

    result = regime_publication.finalize_regime_publication(
        gold_container="gold",
        inputs=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-18"), pd.Timestamp("2026-03-19")]}),
        history=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-18"), pd.Timestamp("2026-03-19")]}),
        latest=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-19")]}),
        transitions=pd.DataFrame({"effective_from_date": [pd.Timestamp("2026-03-19")]}),
        active_models=[{"name": "default-regime", "version": 2, "activated_at": "2026-03-20T00:00:00Z"}],
        publish_state=publish_state,
        job_name="gold-regime-job",
        watermark_key="gold_regime_features",
        when=when,
        write_marker_fn=regime_publication.write_system_health_marker,
        save_watermarks_fn=regime_publication.save_watermarks,
        save_last_success_fn=regime_publication.save_last_success,
    )

    assert result.status == "published"
    assert result.source_fingerprint == captured["payload"]["sourceCommit"]
    assert captured["payload"]["published_as_of_date"] == "2026-03-19"
    assert captured["payload"]["input_as_of_date"] == "2026-03-19"
    assert captured["payload"]["history_rows"] == 2
    assert captured["payload"]["downstream_triggered"] is True
    assert saved_watermarks == [("gold_regime_features", publish_state)]
    assert marker_metadata == [publish_state]
    assert saved_last_success == [("gold_regime_features", when, publish_state)]
    assert any("artifact_publication_status layer=gold domain=regime status=published" in message for message in messages)


def test_finalize_regime_publication_blocks_before_success_markers_when_reconcile_signal_fails(monkeypatch) -> None:
    messages: list[str] = []
    saved_watermarks: list[tuple[str, dict[str, object]]] = []
    marker_metadata: list[dict[str, object]] = []
    saved_last_success: list[tuple[str, datetime, dict[str, object]]] = []

    monkeypatch.setattr(regime_publication.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        regime_publication.domain_artifacts,
        "publish_domain_artifact_payload",
        lambda *, payload, client=None: {"artifactPath": "regime/_metadata/domain.json"},
    )
    monkeypatch.setattr(regime_publication.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(regime_publication.mdc, "write_error", lambda msg: messages.append(str(msg)))

    publish_state = regime_publication.build_regime_publish_state(
        published_as_of_date="2026-03-19",
        input_as_of_date="2026-03-19",
        history_rows=2,
        latest_rows=1,
        transition_rows=1,
        active_models=[{"model_name": "default-regime", "model_version": 2}],
        downstream_triggered=False,
    )

    def _raise_signal_error(_artifact_payload: dict[str, object], _published: dict[str, object]) -> None:
        raise RuntimeError("signal unavailable")

    result = regime_publication.finalize_regime_publication(
        gold_container="gold",
        inputs=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-19")]}),
        history=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-19")]}),
        latest=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-19")]}),
        transitions=pd.DataFrame({"effective_from_date": [pd.Timestamp("2026-03-19")]}),
        active_models=[{"name": "default-regime", "version": 2}],
        publish_state=publish_state,
        job_name="gold-regime-job",
        watermark_key="gold_regime_features",
        when=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
        write_marker_fn=(
            lambda *, layer, domain, job_name="", metadata=None: marker_metadata.append(dict(metadata or {})) or True
        ),
        save_watermarks_fn=lambda key, items: saved_watermarks.append((key, dict(items))),
        save_last_success_fn=(
            lambda key, *, when, metadata=None: saved_last_success.append((key, when, dict(metadata or {})))
        ),
        after_artifact_published_fn=_raise_signal_error,
    )

    assert result.status == "blocked"
    assert result.failure_mode == "finalization"
    assert saved_watermarks == []
    assert marker_metadata == []
    assert saved_last_success == []
    assert any("signal unavailable" in message for message in messages)


def test_finalize_regime_publication_blocks_when_health_marker_write_fails(monkeypatch) -> None:
    messages: list[str] = []
    last_success_calls = {"count": 0}

    monkeypatch.setattr(regime_publication.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        regime_publication.domain_artifacts,
        "publish_domain_artifact_payload",
        lambda *, payload, client=None: {"artifactPath": "regime/_metadata/domain.json"},
    )
    monkeypatch.setattr(regime_publication.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(regime_publication.mdc, "write_error", lambda msg: messages.append(str(msg)))

    publish_state = regime_publication.build_regime_publish_state(
        published_as_of_date="2026-03-19",
        input_as_of_date="2026-03-19",
        history_rows=2,
        latest_rows=1,
        transition_rows=1,
        active_models=[{"model_name": "default-regime", "model_version": 2}],
        downstream_triggered=False,
    )

    result = regime_publication.finalize_regime_publication(
        gold_container="gold",
        inputs=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-19")]}),
        history=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-19")]}),
        latest=pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-19")]}),
        transitions=pd.DataFrame({"effective_from_date": [pd.Timestamp("2026-03-19")]}),
        active_models=[{"name": "default-regime", "version": 2}],
        publish_state=publish_state,
        job_name="gold-regime-job",
        watermark_key="gold_regime_features",
        when=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
        write_marker_fn=lambda **kwargs: False,
        save_watermarks_fn=lambda key, items: None,
        save_last_success_fn=lambda key, *, when, metadata=None: last_success_calls.__setitem__(
            "count",
            last_success_calls["count"] + 1,
        ),
    )

    assert result.status == "blocked"
    assert result.failure_mode == "finalization"
    assert last_success_calls["count"] == 0
    assert any("artifact_publication_status layer=gold domain=regime status=blocked" in message for message in messages)
