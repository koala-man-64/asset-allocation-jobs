from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from typing import Any, Sequence

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc

from tasks.quiver_data import constants


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
            mdc.write_error(f"Failed to read Quiver bronze payload {name}: {type(exc).__name__}: {exc}")
            continue
        if isinstance(payload, dict):
            batches.append(payload)
    return batches


def load_parquet_snapshot(*, client: Any, path: str) -> pd.DataFrame:
    if client is None:
        return pd.DataFrame()
    try:
        raw = mdc.read_raw_bytes(path, client=client)
    except Exception:
        return pd.DataFrame()
    if not raw:
        return pd.DataFrame()
    return pd.read_parquet(io.BytesIO(bytes(raw)))


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
    extra_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
        "tables": {name: {"rowCount": int(len(frame))} for name, frame in tables.items()},
    }
    if extra_metadata:
        payload["metadata"] = dict(extra_metadata)
    mdc.save_json_content(payload, constants.DOMAIN_ARTIFACT_PATH, client=client)
    return payload
