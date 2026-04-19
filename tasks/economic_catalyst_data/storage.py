from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc

from tasks.economic_catalyst_data import constants
from tasks.economic_catalyst_data.transform import read_parquet_frame


def computed_at_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_blob_infos(*, client: Any, prefix: str) -> list[dict[str, Any]]:
    if client is None:
        return []
    return [dict(item) for item in client.list_blob_infos(name_starts_with=str(prefix or "").strip("/"))]


def read_json_batches(*, client: Any, blob_infos: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for blob in blob_infos:
        name = str(blob.get("name") or "").strip()
        if not name:
            continue
        try:
            raw = mdc.read_raw_bytes(name, client=client)
            payload = json.loads(bytes(raw).decode("utf-8"))
        except Exception as exc:
            mdc.write_error(f"Failed to read economic catalyst bronze payload {name}: {type(exc).__name__}: {exc}")
            continue
        if isinstance(payload, dict):
            batches.append(payload)
    return batches


def load_parquet_snapshot(
    *,
    client: Any,
    path: str,
    columns: Sequence[str],
) -> pd.DataFrame:
    if client is None:
        return pd.DataFrame(columns=list(columns))
    try:
        raw = mdc.read_raw_bytes(path, client=client)
    except Exception:
        return pd.DataFrame(columns=list(columns))
    return read_parquet_frame(raw, columns=columns)


def write_parquet_snapshot(*, client: Any, path: str, frame: pd.DataFrame) -> None:
    if client is None:
        raise ValueError("Storage client is required.")
    client.write_parquet(path, frame if frame is not None else pd.DataFrame())


def write_domain_artifact(
    *,
    client: Any,
    layer: str,
    job_name: str,
    run_id: str,
    tables: dict[str, pd.DataFrame],
    warnings: Sequence[str] = (),
    extra_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        name: {
            "path": (
                constants.gold_table_path(name)
                if layer == "gold"
                else constants.silver_state_table_path(name)
                if name in {"source_events_raw", "source_headlines_raw", "quarantine"}
                else constants.silver_table_path(name)
                if layer == "silver"
                else None
            ),
            "rowCount": int(len(frame)) if frame is not None else 0,
        }
        for name, frame in tables.items()
    }
    payload = {
        "version": 1,
        "scope": "domain",
        "layer": str(layer or "").strip(),
        "domain": constants.DOMAIN_SLUG,
        "rootPath": constants.DOMAIN_SLUG,
        "artifactPath": constants.DOMAIN_ARTIFACT_PATH,
        "updatedAt": computed_at_iso(),
        "producerJobName": str(job_name or "").strip() or None,
        "jobRunId": str(run_id or "").strip() or None,
        "runId": str(run_id or "").strip() or None,
        "warnings": [str(item) for item in warnings if str(item).strip()],
        "tables": summary,
    }
    if extra_metadata:
        payload["metadata"] = dict(extra_metadata)
    mdc.save_json_content(payload, constants.DOMAIN_ARTIFACT_PATH, client=client)
    return payload

