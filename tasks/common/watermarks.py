from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation.datetime_utils import parse_utc_datetime, utc_isoformat


def _is_enabled() -> bool:
    return getattr(mdc, "common_storage_client", None) is not None


def _require_enabled(action: str) -> None:
    if _is_enabled():
        return
    message = f"{action} failed: common storage client is not initialized."
    mdc.write_error(message)
    raise RuntimeError(message)


def _watermark_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/{cleaned}.json"


def _run_checkpoint_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/runs/{cleaned}.json"


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return utc_isoformat(dt)


def _parse_iso(raw: Any) -> Optional[datetime]:
    return parse_utc_datetime(raw)


def normalize_watermark_blob_name(blob_name: str) -> str:
    text = str(blob_name or "").strip().strip("/")
    if not text:
        return ""

    parts = text.split("/")
    if len(parts) < 3 or parts[-2] != "buckets":
        return text
    if "runs" not in parts[:-2]:
        return text

    run_index = parts.index("runs")
    if run_index <= 0 or run_index != len(parts) - 4:
        return text
    return "/".join([*parts[:run_index], "buckets", parts[-1]])


def blob_last_modified_utc(blob: Dict[str, Any]) -> Optional[datetime]:
    return parse_utc_datetime(blob.get("last_modified"))


def build_blob_signature(blob: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        "etag": blob.get("etag"),
        "last_modified": _iso(blob_last_modified_utc(blob)),
    }


def signature_matches(prior: Dict[str, Any], current: Dict[str, Optional[str]]) -> bool:
    if not prior or not current:
        return False

    current_etag = current.get("etag")
    prior_etag = prior.get("etag")
    if current_etag and prior_etag:
        return current_etag == prior_etag

    current_lm = current.get("last_modified")
    prior_lm = prior.get("last_modified")
    if current_lm and prior_lm:
        return current_lm == prior_lm

    current_name = current.get("name")
    prior_name = prior.get("name")
    if current_name and prior_name:
        if current_name != prior_name:
            return False
        current_size = current.get("size")
        prior_size = prior.get("size")
        if current_size and prior_size:
            return current_size == prior_size
        return True

    current_size = current.get("size")
    prior_size = prior.get("size")
    if current_size and prior_size:
        return current_size == prior_size

    return False


def load_watermarks(key: str) -> Dict[str, Any]:
    _require_enabled("Watermark load")

    payload = mdc.get_common_json_content(_watermark_path(key)) or {}
    if isinstance(payload, dict) and isinstance(payload.get("items"), dict):
        return payload["items"]
    if isinstance(payload, dict):
        return payload
    return {}


def save_watermarks(key: str, items: Dict[str, Any]) -> None:
    _require_enabled("Watermark save")

    payload = {
        "version": 1,
        "updated_at": _iso(datetime.now(timezone.utc)),
        "items": items,
    }
    try:
        mdc.save_common_json_content(payload, _watermark_path(key))
    except Exception as exc:
        message = f"Failed to save watermarks: {exc}"
        mdc.write_error(message)
        raise RuntimeError(message) from exc

def load_last_success(key: str) -> Optional[datetime]:
    _require_enabled("Run checkpoint load")

    payload = mdc.get_common_json_content(_run_checkpoint_path(key))
    if not isinstance(payload, dict):
        return None

    for candidate_key in ("last_success", "last_success_at", "updated_at"):
        parsed = _parse_iso(payload.get(candidate_key))
        if parsed is not None:
            return parsed
    return None


def save_last_success(key: str, *, when: Optional[datetime] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
    _require_enabled("Run checkpoint save")

    last_success = when or datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "version": 1,
        "last_success": _iso(last_success),
        "updated_at": _iso(datetime.now(timezone.utc)),
    }
    if metadata:
        payload["metadata"] = metadata
    try:
        mdc.save_common_json_content(payload, _run_checkpoint_path(key))
    except Exception as exc:
        message = f"Failed to save run checkpoint: {exc}"
        mdc.write_error(message)
        raise RuntimeError(message) from exc


def should_process_blob_since_last_success(
    blob: Dict[str, Any],
    *,
    prior_signature: Optional[Dict[str, Any]],
    last_success_at: Optional[datetime],
    force_reprocess: bool = False,
) -> bool:
    if force_reprocess:
        return True

    if not prior_signature:
        return True

    current_signature = build_blob_signature(blob)
    if not signature_matches(prior_signature, current_signature):
        return True

    if last_success_at is None:
        return False

    blob_last_modified = blob_last_modified_utc(blob)
    if blob_last_modified is None:
        return False

    checkpoint = (
        last_success_at.replace(tzinfo=timezone.utc)
        if last_success_at.tzinfo is None
        else last_success_at.astimezone(timezone.utc)
    )
    return blob_last_modified > checkpoint


def check_blob_unchanged(blob: Dict[str, Any], prior: Optional[Dict[str, Any]]) -> Tuple[bool, Dict[str, Optional[str]]]:
    signature = build_blob_signature(blob)
    if not prior:
        return False, signature
    return signature_matches(prior, signature), signature
