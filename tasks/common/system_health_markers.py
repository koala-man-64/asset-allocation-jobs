from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core import core as mdc
from core.blob_storage import BlobStorageClient


DEFAULT_MARKER_PREFIX = "system/health_markers"
def _marker_container_name() -> str:
    explicit = os.environ.get("SYSTEM_HEALTH_MARKERS_CONTAINER")
    if explicit and explicit.strip():
        return explicit.strip()
    fallback = os.environ.get("AZURE_CONTAINER_COMMON")
    return fallback.strip() if fallback else ""


def _marker_prefix() -> str:
    raw = os.environ.get("SYSTEM_HEALTH_MARKERS_PREFIX", DEFAULT_MARKER_PREFIX)
    cleaned = str(raw or "").strip().strip("/")
    return cleaned or DEFAULT_MARKER_PREFIX


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _marker_blob_path(layer: str, domain: str) -> str:
    layer_slug = _slug(layer)
    domain_slug = _slug(domain)
    return f"{_marker_prefix()}/{layer_slug}/{domain_slug}.json"


def write_system_health_marker(
    *,
    layer: str,
    domain: str,
    job_name: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Writes a lightweight marker blob used by system-health freshness probes.

    This is best-effort and should not fail the parent job.
    """
    container_name = _marker_container_name()
    if not container_name:
        mdc.write_warning("Skipping system-health marker write: marker container is not configured.")
        return False

    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not account_name and not connection_string:
        mdc.write_warning(
            "Skipping system-health marker write: storage auth env is missing "
            "(AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING)."
        )
        return False

    payload: Dict[str, Any] = {
        "version": 1,
        "recordedAt": datetime.now(timezone.utc).isoformat(),
        "layer": _slug(layer),
        "domain": _slug(domain),
        "jobName": str(job_name or "").strip(),
        "source": "pipeline-job",
    }
    if metadata:
        payload["metadata"] = metadata

    blob_path = _marker_blob_path(layer, domain)
    try:
        client = BlobStorageClient(
            account_name=account_name,
            connection_string=connection_string,
            container_name=container_name,
            ensure_container_exists=False,
        )
        client.upload_data(
            blob_path,
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            overwrite=True,
        )
        mdc.write_line(
            "System-health marker updated: "
            f"container={container_name} path={blob_path} job={job_name} "
            f"metadata_keys={len(metadata or {})}"
        )
        return True
    except Exception as exc:
        mdc.write_warning(f"Failed to write system-health marker ({container_name}/{blob_path}): {exc}")
        return False
