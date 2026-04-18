from __future__ import annotations

from dataclasses import dataclass, field
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
    artifact_payload = domain_artifacts.write_bucket_artifact(
        layer="bronze",
        domain=session.domain,
        bucket=clean_bucket,
        df=prepared_frame,
        date_column=session.date_column,
        client=session.storage_client,
        job_name=session.job_name,
        job_run_id=session.run_id,
        run_id=session.run_id,
        active_data_prefix=session.run_prefix,
        data_path=path,
    )
    if isinstance(artifact_payload, dict):
        session.bucket_artifacts[clean_bucket] = artifact_payload
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


def finalize_alpha26_bronze_publish(session: BronzeAlpha26PublishSession) -> PublishResult:
    index_path = bronze_bucketing.write_symbol_index(
        domain=session.domain,
        symbol_to_bucket=session.symbol_to_bucket,
    )
    aggregate_summary = domain_artifacts.aggregate_summaries(
        session.bucket_summaries,
        symbol_count_override=len(session.symbol_to_bucket),
        date_column=session.date_column,
    )
    manifest_metadata = {
        **dict(session.metadata or {}),
        **aggregate_summary,
        "fileCount": len(session.bucket_paths),
        "totalBytes": session.total_bytes,
    }
    finance_subfolder_counts = _aggregate_finance_subdomains(session.bucket_summaries)
    if finance_subfolder_counts:
        manifest_metadata["financeSubfolderSymbolCounts"] = finance_subfolder_counts

    manifest_result = run_manifests.create_bronze_alpha26_manifest(
        domain=session.domain,
        producer_job_name=session.job_name,
        data_prefix=session.run_prefix,
        bucket_paths=session.bucket_paths,
        index_path=index_path,
        metadata=manifest_metadata,
        run_id=session.run_id,
    )
    manifest_path = str((manifest_result or {}).get("manifestPath") or "").strip() or None

    if manifest_path:
        for artifact_payload in session.bucket_artifacts.values():
            artifact_path = str(artifact_payload.get("artifactPath") or "").strip()
            if not artifact_path:
                continue
            refreshed_payload = dict(artifact_payload)
            refreshed_payload["manifestPath"] = manifest_path
            mdc.save_json_content(refreshed_payload, artifact_path, client=session.storage_client)

    domain_artifacts.write_domain_artifact(
        layer="bronze",
        domain=session.domain,
        date_column=session.date_column,
        client=session.storage_client,
        symbol_count_override=len(session.symbol_to_bucket),
        symbol_index_path=index_path,
        job_name=session.job_name,
        job_run_id=session.run_id,
        run_id=session.run_id,
        manifest_path=manifest_path,
        active_data_prefix=session.run_prefix,
        total_bytes_override=session.total_bytes,
        file_count_override=len(session.bucket_paths),
    )
    mdc.write_line(
        "Bronze {domain} commit completed: run_id={run_id} data_prefix={prefix} manifest_path={manifest_path} "
        "written_symbols={written_symbols} index_path={index_path}".format(
            domain=session.domain,
            run_id=session.run_id,
            prefix=session.run_prefix,
            manifest_path=manifest_path or "n/a",
            written_symbols=len(session.symbol_to_bucket),
            index_path=index_path or "n/a",
        )
    )
    return PublishResult(
        run_id=session.run_id,
        data_prefix=session.run_prefix,
        bucket_paths=session.bucket_paths,
        index_path=index_path,
        manifest_path=manifest_path,
        written_symbols=len(session.symbol_to_bucket),
        total_bytes=session.total_bytes,
        file_count=len(session.bucket_paths),
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
