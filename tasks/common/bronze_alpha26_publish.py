from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import bronze_bucketing
from asset_allocation_runtime_common.market_data import domain_artifacts
from asset_allocation_runtime_common.foundation import run_manifests
@dataclass
class PublishResult:
    run_id: str
    data_prefix: str
    bucket_paths: list[dict[str, Any]]
    index_path: Optional[str]
    manifest_path: Optional[str]
    written_symbols: int
    total_bytes: int
    file_count: int


@dataclass
class BronzeAlpha26PublishSession:
    domain: str
    root_prefix: str
    run_id: str
    run_prefix: str
    storage_client: Any
    job_name: str
    date_column: Optional[str]
    metadata: dict[str, Any]
    bucket_columns: tuple[str, ...]
    codec: str
    bucket_paths: list[dict[str, Any]] = field(default_factory=list)
    bucket_summaries: list[dict[str, Any]] = field(default_factory=list)
    bucket_artifacts: dict[str, dict[str, Any]] = field(default_factory=dict)
    symbol_to_bucket: dict[str, str] = field(default_factory=dict)
    written_buckets: set[str] = field(default_factory=set)
    total_bytes: int = 0
    scope_mode: str = "full_domain"
    touched_buckets: set[str] = field(default_factory=set)
    active_symbol_to_bucket: dict[str, str] = field(default_factory=dict)
    active_bucket_paths: list[dict[str, Any]] = field(default_factory=list)


def _normalize_publish_args(
    *,
    domain: str,
    root_prefix: str,
    run_id: str,
) -> tuple[str, str, str]:
    normalized_domain = str(domain or "").strip().lower().replace("_", "-")
    normalized_root_prefix = str(root_prefix or "").strip().strip("/")
    normalized_run_id = str(run_id or "").strip()
    if not normalized_domain:
        raise ValueError("domain is required")
    if not normalized_root_prefix:
        raise ValueError("root_prefix is required")
    if not normalized_run_id:
        raise ValueError("run_id is required")
    return normalized_domain, normalized_root_prefix, normalized_run_id


def _normalize_bucket_frames(
    *,
    bucket_frames: Dict[str, pd.DataFrame],
    bucket_columns: Iterable[str],
) -> dict[str, pd.DataFrame]:
    columns = [str(column) for column in bucket_columns]
    normalized: dict[str, pd.DataFrame] = {}
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        frame = bucket_frames.get(bucket)
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            normalized[bucket] = frame
        else:
            normalized[bucket] = pd.DataFrame(columns=columns)
    return normalized


def _aggregate_finance_subdomains(bucket_summaries: Iterable[dict[str, Any]]) -> Optional[dict[str, int]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for payload in bucket_summaries:
        if not isinstance(payload, dict):
            continue
        subdomains = payload.get("subdomains")
        if not isinstance(subdomains, dict):
            continue
        for key, value in subdomains.items():
            normalized_key = domain_artifacts.normalize_sub_domain(key)
            if normalized_key not in domain_artifacts.FINANCE_SUBDOMAINS or not isinstance(value, dict):
                continue
            grouped.setdefault(normalized_key, []).append(value)
    if not grouped:
        return None
    out: dict[str, int] = {}
    for key, payloads in grouped.items():
        summary = domain_artifacts.aggregate_summaries(payloads, date_column="date")
        out[key] = int(summary.get("symbolCount") or 0)
    return out or None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _bucket_artifact_payload(
    session: BronzeAlpha26PublishSession,
    *,
    bucket: str,
    summary: dict[str, Any],
    data_path: str,
    manifest_path: Optional[str],
) -> dict[str, Any]:
    artifact_path = domain_artifacts.bucket_artifact_path(layer="bronze", domain=session.domain, bucket=bucket)
    now = _utc_now_iso()
    date_range = summary.get("dateRange") if isinstance(summary, dict) else None
    affected_start = date_range.get("min") if isinstance(date_range, dict) else None
    affected_end = date_range.get("max") if isinstance(date_range, dict) else None
    return {
        "version": getattr(domain_artifacts, "ARTIFACT_VERSION", 1),
        "scope": "bucket",
        "layer": "bronze",
        "domain": session.domain,
        "subDomain": None,
        "bucket": bucket,
        "rootPath": domain_artifacts.root_prefix(layer="bronze", domain=session.domain),
        "artifactPath": artifact_path,
        "updatedAt": now,
        "computedAt": now,
        "publishedAt": now,
        "producerJobName": session.job_name or None,
        "jobRunId": session.run_id,
        "runId": session.run_id,
        "manifestPath": manifest_path,
        "activeDataPrefix": session.run_prefix,
        "dataPath": data_path,
        "sourceCommit": None,
        "affectedAsOfStart": affected_start,
        "affectedAsOfEnd": affected_end,
        **dict(summary or {}),
    }


def start_alpha26_bronze_publish(
    *,
    domain: str,
    root_prefix: str,
    bucket_columns: Iterable[str],
    date_column: Optional[str],
    storage_client: Any,
    job_name: str,
    run_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    scope_mode: str = "full_domain",
    touched_buckets: Optional[Iterable[str]] = None,
    active_symbol_to_bucket: Optional[Dict[str, str]] = None,
    active_bucket_paths: Optional[Iterable[dict[str, Any]]] = None,
) -> BronzeAlpha26PublishSession:
    normalized_domain, normalized_root_prefix, normalized_run_id = _normalize_publish_args(
        domain=domain,
        root_prefix=root_prefix,
        run_id=run_id,
    )
    session = BronzeAlpha26PublishSession(
        domain=normalized_domain,
        root_prefix=normalized_root_prefix,
        run_id=normalized_run_id,
        run_prefix=f"{normalized_root_prefix}/runs/{normalized_run_id}",
        storage_client=storage_client,
        job_name=str(job_name or "").strip(),
        date_column=date_column,
        metadata=dict(metadata or {}),
        bucket_columns=tuple(str(column) for column in bucket_columns),
        codec=bronze_bucketing.alpha26_codec(),
        scope_mode=str(scope_mode or "full_domain").strip().lower(),
        touched_buckets={
            str(bucket or "").strip().upper()
            for bucket in (touched_buckets or [])
            if str(bucket or "").strip().upper() in bronze_bucketing.ALPHABET_BUCKETS
        },
        active_symbol_to_bucket={
            str(symbol or "").strip().upper(): str(bucket or "").strip().upper()
            for symbol, bucket in (active_symbol_to_bucket or {}).items()
            if str(symbol or "").strip() and str(bucket or "").strip().upper() in bronze_bucketing.ALPHABET_BUCKETS
        },
        active_bucket_paths=[dict(entry) for entry in (active_bucket_paths or []) if isinstance(entry, dict)],
    )
    mdc.write_line(
        f"Bronze {normalized_domain} commit started: run_id={normalized_run_id} data_prefix={session.run_prefix}"
    )
    return session


def write_alpha26_bronze_bucket(
    session: BronzeAlpha26PublishSession,
    *,
    bucket: str,
    frame: Optional[pd.DataFrame],
    symbol_to_bucket: Optional[Dict[str, str]] = None,
) -> dict[str, Any]:
    clean_bucket = str(bucket or "").strip().upper()
    if clean_bucket not in bronze_bucketing.ALPHABET_BUCKETS:
        raise ValueError(f"Invalid bucket: {bucket!r}")
    if clean_bucket in session.written_buckets:
        raise ValueError(f"Bucket already written for session: {clean_bucket}")

    prepared_frame = frame if isinstance(frame, pd.DataFrame) and not frame.empty else pd.DataFrame(columns=list(session.bucket_columns))
    if session.scope_mode == "intraday" and session.touched_buckets and clean_bucket not in session.touched_buckets:
        session.written_buckets.add(clean_bucket)
        mdc.write_line(
            f"Bronze {session.domain} bucket write skipped for scoped run: "
            f"run_id={session.run_id} bucket={clean_bucket} reason=untouched_bucket"
        )
        return {"bucket": clean_bucket, "name": None, "size": 0, "skipped": True}

    payload = prepared_frame.to_parquet(index=False, compression=session.codec)
    path = bronze_bucketing.bucket_blob_path(session.run_prefix, clean_bucket)
    mdc.write_line(
        f"Bronze {session.domain} bucket write started: run_id={session.run_id} bucket={clean_bucket} rows={len(prepared_frame)}"
    )
    mdc.store_raw_bytes(payload, path, client=session.storage_client)
    entry = {
        "bucket": clean_bucket,
        "name": path,
        "size": len(payload),
    }
    summary = domain_artifacts.summarize_frame(
        prepared_frame,
        domain=session.domain,
        date_column=session.date_column,
    )
    session.bucket_artifacts[clean_bucket] = {
        "bucket": clean_bucket,
        "summary": summary,
        "dataPath": path,
    }
    session.bucket_paths.append(entry)
    session.bucket_summaries.append(summary)
    session.total_bytes += len(payload)
    session.written_buckets.add(clean_bucket)
    for symbol, assigned_bucket in (symbol_to_bucket or {}).items():
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_bucket = str(assigned_bucket or "").strip().upper()
        if normalized_symbol and normalized_bucket == clean_bucket:
            session.symbol_to_bucket[normalized_symbol] = clean_bucket
    mdc.write_line(
        "Bronze {domain} bucket write completed: run_id={run_id} bucket={bucket} rows={rows} bytes={bytes}".format(
            domain=session.domain,
            run_id=session.run_id,
            bucket=clean_bucket,
            rows=len(prepared_frame),
            bytes=len(payload),
        )
    )
    return entry


def _active_bucket_entries_by_bucket(session: BronzeAlpha26PublishSession) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for entry in session.active_bucket_paths:
        name = str(entry.get("name") or "").strip()
        bucket = bronze_bucketing.parse_bucket_from_blob_name(name)
        if bucket:
            out[bucket] = dict(entry)
    return out


def _manifest_bucket_paths(session: BronzeAlpha26PublishSession) -> list[dict[str, Any]]:
    if session.scope_mode != "intraday":
        return list(session.bucket_paths)

    by_bucket = _active_bucket_entries_by_bucket(session)
    for entry in session.bucket_paths:
        bucket = str(entry.get("bucket") or "").strip().upper()
        if bucket:
            by_bucket[bucket] = dict(entry)
    return [by_bucket[bucket] for bucket in bronze_bucketing.ALPHABET_BUCKETS if bucket in by_bucket]


def _effective_symbol_to_bucket(session: BronzeAlpha26PublishSession) -> dict[str, str]:
    if session.scope_mode != "intraday":
        return dict(session.symbol_to_bucket)
    if not session.active_symbol_to_bucket:
        raise RuntimeError(
            f"Bronze {session.domain} scoped publish blocked: prior active symbol index is missing."
        )
    merged = dict(session.active_symbol_to_bucket)
    merged.update(session.symbol_to_bucket)
    return merged


def finalize_alpha26_bronze_publish(session: BronzeAlpha26PublishSession) -> PublishResult:
    effective_symbol_to_bucket = _effective_symbol_to_bucket(session)
    manifest_bucket_paths = _manifest_bucket_paths(session)
    index_path = bronze_bucketing.write_symbol_index(
        domain=session.domain,
        symbol_to_bucket=effective_symbol_to_bucket,
    )
    aggregate_summary = domain_artifacts.aggregate_summaries(
        session.bucket_summaries,
        symbol_count_override=len(effective_symbol_to_bucket),
        date_column=session.date_column,
    )
    manifest_metadata = {
        **dict(session.metadata or {}),
        **aggregate_summary,
        "fileCount": len(manifest_bucket_paths),
        "totalBytes": session.total_bytes,
        "scopeMode": session.scope_mode,
        "touchedBuckets": sorted(session.touched_buckets),
    }
    finance_subfolder_counts = _aggregate_finance_subdomains(session.bucket_summaries)
    if finance_subfolder_counts:
        manifest_metadata["financeSubfolderSymbolCounts"] = finance_subfolder_counts

    manifest_result = run_manifests.create_bronze_alpha26_manifest(
        domain=session.domain,
        producer_job_name=session.job_name,
        data_prefix=session.run_prefix,
        bucket_paths=manifest_bucket_paths,
        index_path=index_path,
        metadata=manifest_metadata,
        run_id=session.run_id,
    )
    manifest_path = str((manifest_result or {}).get("manifestPath") or "").strip() or None

    published_bucket_artifacts: dict[str, dict[str, Any]] = {}
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        pending = session.bucket_artifacts.get(bucket)
        if not isinstance(pending, dict):
            continue
        artifact_payload = _bucket_artifact_payload(
            session,
            bucket=bucket,
            summary=dict(pending.get("summary") or {}),
            data_path=str(pending.get("dataPath") or "").strip(),
            manifest_path=manifest_path,
        )
        artifact_path = str(artifact_payload.get("artifactPath") or "").strip()
        if not artifact_path:
            continue
        mdc.save_json_content(artifact_payload, artifact_path, client=session.storage_client)
        published_bucket_artifacts[bucket] = artifact_payload
    session.bucket_artifacts = published_bucket_artifacts

    domain_artifacts.write_domain_artifact(
        layer="bronze",
        domain=session.domain,
        date_column=session.date_column,
        client=session.storage_client,
        symbol_count_override=len(effective_symbol_to_bucket),
        symbol_index_path=index_path,
        job_name=session.job_name,
        job_run_id=session.run_id,
        run_id=session.run_id,
        manifest_path=manifest_path,
        active_data_prefix=session.run_prefix,
        total_bytes_override=session.total_bytes,
        file_count_override=len(manifest_bucket_paths),
    )
    mdc.write_line(
        "Bronze {domain} commit completed: run_id={run_id} data_prefix={prefix} manifest_path={manifest_path} "
        "written_symbols={written_symbols} index_path={index_path}".format(
            domain=session.domain,
            run_id=session.run_id,
            prefix=session.run_prefix,
            manifest_path=manifest_path or "n/a",
            written_symbols=len(effective_symbol_to_bucket),
            index_path=index_path or "n/a",
        )
    )
    return PublishResult(
        run_id=session.run_id,
        data_prefix=session.run_prefix,
        bucket_paths=manifest_bucket_paths,
        index_path=index_path,
        manifest_path=manifest_path,
        written_symbols=len(effective_symbol_to_bucket),
        total_bytes=session.total_bytes,
        file_count=len(manifest_bucket_paths),
    )


def publish_alpha26_bronze_domain(
    *,
    domain: str,
    root_prefix: str,
    bucket_frames: Dict[str, pd.DataFrame],
    bucket_columns: Iterable[str],
    date_column: Optional[str],
    symbol_to_bucket: Dict[str, str],
    storage_client: Any,
    job_name: str,
    run_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    scope_mode: str = "full_domain",
    touched_buckets: Optional[Iterable[str]] = None,
    active_symbol_to_bucket: Optional[Dict[str, str]] = None,
    active_bucket_paths: Optional[Iterable[dict[str, Any]]] = None,
) -> PublishResult:
    prepared_frames = _normalize_bucket_frames(bucket_frames=bucket_frames, bucket_columns=bucket_columns)
    session = start_alpha26_bronze_publish(
        domain=domain,
        root_prefix=root_prefix,
        bucket_columns=bucket_columns,
        date_column=date_column,
        storage_client=storage_client,
        job_name=job_name,
        run_id=run_id,
        metadata=metadata,
        scope_mode=scope_mode,
        touched_buckets=touched_buckets,
        active_symbol_to_bucket=active_symbol_to_bucket,
        active_bucket_paths=active_bucket_paths,
    )
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        bucket_symbol_map = {
            str(symbol or "").strip().upper(): str(assigned_bucket or "").strip().upper()
            for symbol, assigned_bucket in symbol_to_bucket.items()
            if str(assigned_bucket or "").strip().upper() == bucket
        }
        write_alpha26_bronze_bucket(
            session,
            bucket=bucket,
            frame=prepared_frames[bucket],
            symbol_to_bucket=bucket_symbol_map,
        )
    return finalize_alpha26_bronze_publish(session)
