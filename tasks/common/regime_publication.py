from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import domain_artifacts
from tasks.common.system_health_markers import write_system_health_marker
from tasks.common.watermarks import save_last_success, save_watermarks


@dataclass(frozen=True)
class RegimePublicationFinalizationResult:
    status: str
    reason: str
    failure_mode: str
    failed_finalization: int
    domain_artifact_path: str | None
    health_marker_written: bool
    publish_state: dict[str, Any]
    source_fingerprint: str | None = None


def build_regime_publish_state(
    *,
    published_as_of_date: str,
    input_as_of_date: str | None,
    history_rows: int,
    latest_rows: int,
    transition_rows: int,
    active_models: Sequence[dict[str, Any]],
    downstream_triggered: bool,
    warnings: Sequence[str] = (),
    status: str = "published",
    reason: str = "none",
    failure_mode: str = "none",
) -> dict[str, Any]:
    return {
        "as_of_date": published_as_of_date,
        "published_as_of_date": published_as_of_date,
        "input_as_of_date": input_as_of_date,
        "history_rows": int(history_rows),
        "latest_rows": int(latest_rows),
        "transition_rows": int(transition_rows),
        "active_models": [dict(item) for item in active_models],
        "downstream_triggered": bool(downstream_triggered),
        "warnings": list(warnings),
        "status": str(status or "").strip() or "published",
        "reason": str(reason or "").strip() or "none",
        "failure_mode": str(failure_mode or "").strip() or "none",
    }


def log_regime_publication_status(
    publish_state: dict[str, Any],
    *,
    failed_finalization: int = 0,
) -> None:
    status = str(publish_state.get("status") or "blocked").strip() or "blocked"
    reason = str(publish_state.get("reason") or "none").strip() or "none"
    failure_mode = str(publish_state.get("failure_mode") or "none").strip() or "none"
    mdc.write_line(
        "artifact_publication_status "
        f"layer=gold domain=regime status={status} reason={reason} "
        f"failure_mode={failure_mode} failed_finalization={max(int(failed_finalization), 0)} "
        f"published_as_of_date={publish_state.get('published_as_of_date') or '-'} "
        f"input_as_of_date={publish_state.get('input_as_of_date') or '-'} "
        f"history_rows={int(publish_state.get('history_rows') or 0)} "
        f"latest_rows={int(publish_state.get('latest_rows') or 0)} "
        f"transition_rows={int(publish_state.get('transition_rows') or 0)} "
        f"active_models={len(publish_state.get('active_models') or [])} "
        f"downstream_triggered={str(bool(publish_state.get('downstream_triggered'))).lower()}"
    )


def _history_date_range(history: pd.DataFrame) -> dict[str, Any] | None:
    if history.empty or "as_of_date" not in history.columns:
        return None
    parsed_dates = pd.to_datetime(history["as_of_date"], errors="coerce").dropna()
    if parsed_dates.empty:
        return None
    return {
        "min": parsed_dates.min().isoformat(),
        "max": parsed_dates.max().isoformat(),
        "column": "as_of_date",
        "source": "artifact",
    }


def _build_source_fingerprint(
    *,
    active_models: Sequence[dict[str, Any]],
    date_range: dict[str, Any] | None,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
) -> str:
    def _frame_digest(frame: pd.DataFrame) -> str:
        if frame.empty:
            return "empty"
        ordered = frame.reindex(sorted(frame.columns), axis=1)
        payload = ordered.to_json(orient="split", date_format="iso", default_handler=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    payload = {
        "activeModels": [
            {
                "name": model.get("model_name") or model.get("name"),
                "version": model.get("model_version") or model.get("version"),
                "activatedAt": model.get("activated_at"),
            }
            for model in active_models
        ],
        "dateRange": date_range,
        "content": {
            "inputs": _frame_digest(inputs),
            "history": _frame_digest(history),
            "latest": _frame_digest(latest),
            "transitions": _frame_digest(transitions),
        },
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _build_domain_artifact_payload(
    *,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
    active_models: Sequence[dict[str, Any]],
    publish_state: dict[str, Any],
    job_name: str,
) -> dict[str, Any]:
    date_range = _history_date_range(history)
    all_columns = sorted(set(inputs.columns) | set(history.columns) | set(latest.columns) | set(transitions.columns))
    now = datetime.now(timezone.utc).isoformat()
    artifact_path = domain_artifacts.domain_artifact_path(layer="gold", domain="regime")
    source_fingerprint = _build_source_fingerprint(
        active_models=active_models,
        date_range=date_range,
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
    )
    return {
        "version": 1,
        "scope": "domain",
        "layer": "gold",
        "domain": "regime",
        "rootPath": "regime",
        "artifactPath": artifact_path,
        "updatedAt": now,
        "computedAt": now,
        "publishedAt": now,
        "producerJobName": job_name,
        "sourceCommit": source_fingerprint,
        "symbolCount": 0,
        "columnCount": len(all_columns),
        "columns": all_columns,
        "dateRange": date_range,
        "affectedAsOfStart": date_range.get("min") if isinstance(date_range, dict) else None,
        "affectedAsOfEnd": date_range.get("max") if isinstance(date_range, dict) else None,
        "totalRows": int(len(history)),
        "fileCount": 4,
        **publish_state,
    }


def finalize_regime_publication(
    *,
    gold_container: str,
    inputs: pd.DataFrame,
    history: pd.DataFrame,
    latest: pd.DataFrame,
    transitions: pd.DataFrame,
    active_models: Sequence[dict[str, Any]],
    publish_state: dict[str, Any],
    job_name: str,
    watermark_key: str,
    when: datetime,
    write_marker_fn: Callable[..., bool] = write_system_health_marker,
    save_watermarks_fn: Callable[[str, dict[str, Any]], None] = save_watermarks,
    save_last_success_fn: Callable[..., None] = save_last_success,
    after_artifact_published_fn: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
) -> RegimePublicationFinalizationResult:
    try:
        client = mdc.get_storage_client(gold_container)
        if client is None:
            raise ValueError(f"Storage client unavailable for container '{gold_container}'.")

        artifact_payload = _build_domain_artifact_payload(
            inputs=inputs,
            history=history,
            latest=latest,
            transitions=transitions,
            active_models=active_models,
            publish_state=publish_state,
            job_name=job_name,
        )
        published = domain_artifacts.publish_domain_artifact_payload(payload=artifact_payload, client=client)
        if after_artifact_published_fn is not None:
            after_artifact_published_fn(artifact_payload, published or {})
        marker_written = write_marker_fn(
            layer="gold",
            domain="regime",
            job_name=job_name,
            metadata=dict(publish_state),
        )
        if not marker_written:
            raise RuntimeError("System-health marker write returned False for gold/regime.")
        save_watermarks_fn(watermark_key, dict(publish_state))
        save_last_success_fn(watermark_key, when=when, metadata=dict(publish_state))
        log_regime_publication_status(publish_state)
        return RegimePublicationFinalizationResult(
            status="published",
            reason=str(publish_state.get("reason") or "none"),
            failure_mode=str(publish_state.get("failure_mode") or "none"),
            failed_finalization=0,
            domain_artifact_path=str((published or {}).get("artifactPath") or "") or None,
            health_marker_written=True,
            publish_state=dict(publish_state),
            source_fingerprint=str(artifact_payload.get("sourceCommit") or "") or None,
        )
    except Exception as exc:
        blocked_state = dict(publish_state)
        blocked_state["status"] = "blocked"
        if str(blocked_state.get("reason") or "").strip() in {"", "none"}:
            blocked_state["reason"] = "failed_finalization"
        blocked_state["failure_mode"] = "finalization"
        log_regime_publication_status(blocked_state, failed_finalization=1)
        mdc.write_error(f"Gold regime publication finalization failed: {type(exc).__name__}: {exc}")
        return RegimePublicationFinalizationResult(
            status="blocked",
            reason=str(blocked_state["reason"]),
            failure_mode="finalization",
            failed_finalization=1,
            domain_artifact_path=None,
            health_marker_written=False,
            publish_state=blocked_state,
            source_fingerprint=None,
        )
