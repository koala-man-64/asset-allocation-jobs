from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import domain_artifacts
from asset_allocation_runtime_common.market_data import layer_bucketing
@dataclass(frozen=True)
class GoldCheckpointPublicationResult:
    symbol_to_bucket: dict[str, str]
    index_path: str
    domain_artifact_path: Optional[str]


@dataclass(frozen=True)
class GoldPublicationFinalizationResult:
    failed: int
    failed_symbols: int
    failed_buckets: int
    failed_finalization: int
    deferred_buckets: int
    failure_mode: str
    publication_reason: str
    index_path: Optional[str]
    domain_artifact_path: Optional[str]


def _metric_override(payload: Optional[dict[str, Any]], key: str) -> Optional[int]:
    value = payload.get(key) if isinstance(payload, dict) else None
    return value if isinstance(value, int) else None


def _coerce_counter(value: object) -> int:
    try:
        return max(int(value or 0), 0)
    except Exception:
        return 0


def resolve_failure_mode(
    *,
    failed_symbols: int,
    failed_buckets: int,
    failed_finalization: int,
) -> str:
    failure_modes: list[str] = []
    if _coerce_counter(failed_symbols) > 0:
        failure_modes.append("symbol")
    if _coerce_counter(failed_buckets) > 0:
        failure_modes.append("bucket")
    if _coerce_counter(failed_finalization) > 0:
        failure_modes.append("finalization")
    if not failure_modes:
        return "none"
    if len(failure_modes) == 1:
        return failure_modes[0]
    return "mixed"


def default_publication_reason(
    *,
    failed_symbols: int,
    failed_buckets: int,
    failed_finalization: int,
    deferred_buckets: int = 0,
) -> str:
    symbol_failure_count = _coerce_counter(failed_symbols)
    bucket_failure_count = _coerce_counter(failed_buckets)
    finalization_failure_count = _coerce_counter(failed_finalization)
    deferred_bucket_count = _coerce_counter(deferred_buckets)
    if symbol_failure_count == 0 and bucket_failure_count == 0 and finalization_failure_count == 0:
        if deferred_bucket_count > 0:
            return "retry_pending"
        return "none"
    if symbol_failure_count > 0 and bucket_failure_count == 0 and finalization_failure_count == 0:
        return "failed_symbols"
    if bucket_failure_count > 0 and symbol_failure_count == 0 and finalization_failure_count == 0:
        return "failed_buckets"
    if finalization_failure_count > 0 and symbol_failure_count == 0 and bucket_failure_count == 0:
        return "failed_finalization"
    return "mixed_failures"


def _emit_publication_failure_counter(
    *,
    domain: str,
    stage: str,
    reason: str,
    failed_symbols: int,
    failed_buckets: int,
    failed_finalization: int,
) -> None:
    mdc.write_line(
        "gold_publication_failure_counter "
        f"layer=gold domain={domain} stage={stage} reason={reason} "
        f"failed_symbols={_coerce_counter(failed_symbols)} "
        f"failed_buckets={_coerce_counter(failed_buckets)} "
        f"failed_finalization={_coerce_counter(failed_finalization)}"
    )


def publish_gold_checkpoint_aggregate(
    *,
    domain: str,
    bucket: str,
    symbol_to_bucket: dict[str, str],
    touched_symbol_to_bucket: dict[str, str],
    watermarks: dict[str, Any],
    watermarks_key: str,
    watermark_key: str,
    source_commit: Optional[float],
    date_column: Optional[str],
    job_name: str,
    save_watermarks_fn: Callable[[str, dict[str, Any]], None],
    job_run_id: Optional[str] = None,
    run_id: Optional[str] = None,
    publish_domain_artifact: bool = True,
    updated_at: Optional[datetime] = None,
) -> GoldCheckpointPublicationResult:
    clean_domain = domain_artifacts.normalize_domain(domain)
    clean_bucket = str(bucket or "").strip().upper()
    if source_commit is None:
        raise RuntimeError(f"Cannot persist gold {clean_domain} watermark for bucket={clean_bucket}: missing source commit.")

    checkpoint_time = updated_at or datetime.now(timezone.utc)
    updated_symbol_to_bucket = layer_bucketing.merge_symbol_to_bucket_map(
        symbol_to_bucket,
        touched_buckets={clean_bucket},
        touched_symbol_to_bucket=touched_symbol_to_bucket,
    )
    index_path = layer_bucketing.write_layer_symbol_index(
        layer="gold",
        domain=clean_domain,
        symbol_to_bucket=updated_symbol_to_bucket,
        updated_at=checkpoint_time,
    )
    if index_path is None:
        raise RuntimeError(f"Gold {clean_domain} symbol index write returned no path for bucket={clean_bucket}.")

    updated_watermarks = dict(watermarks)
    updated_watermarks[watermark_key] = {
        "silver_last_commit": source_commit,
        "updated_at": checkpoint_time.isoformat(),
    }
    save_watermarks_fn(watermarks_key, updated_watermarks)
    watermarks.clear()
    watermarks.update(updated_watermarks)

    domain_artifact_path: Optional[str] = None
    artifact_status = "skipped"
    if publish_domain_artifact:
        prior_artifact: Optional[dict[str, Any]] = None
        try:
            prior_artifact = domain_artifacts.load_domain_artifact(layer="gold", domain=clean_domain)
        except Exception as exc:
            mdc.write_warning(
                f"Gold {clean_domain} checkpoint artifact preload failed bucket={clean_bucket}: {exc}"
            )
        try:
            artifact = domain_artifacts.write_domain_artifact(
                layer="gold",
                domain=clean_domain,
                date_column=date_column,
                symbol_count_override=len(updated_symbol_to_bucket),
                symbol_index_path=index_path,
                job_name=job_name,
                job_run_id=job_run_id,
                run_id=run_id,
                total_bytes_override=_metric_override(prior_artifact, "totalBytes"),
                file_count_override=_metric_override(prior_artifact, "fileCount"),
                source_commit=source_commit,
                published_at=checkpoint_time.isoformat(),
            )
            domain_artifact_path = (
                str(artifact.get("artifactPath") or "").strip()
                if isinstance(artifact, dict)
                else None
            ) or None
            artifact_status = "published" if domain_artifact_path else "unavailable"
        except Exception as exc:
            artifact_status = "failed"
            mdc.write_warning(
                f"Gold {clean_domain} checkpoint metadata artifact write failed bucket={clean_bucket}: {exc}"
            )

    mdc.write_line(
        "gold_checkpoint_aggregate_publication "
        f"layer=gold domain={clean_domain} bucket={clean_bucket} status=published "
        f"symbol_count={len(updated_symbol_to_bucket)} index_path={index_path} "
        f"watermark_key={watermark_key} artifact_status={artifact_status} "
        f"artifact_path={domain_artifact_path or 'unavailable'}"
    )
    return GoldCheckpointPublicationResult(
        symbol_to_bucket=updated_symbol_to_bucket,
        index_path=index_path,
        domain_artifact_path=domain_artifact_path,
    )


def finalize_gold_publication(
    *,
    domain: str,
    symbol_to_bucket: dict[str, str],
    date_column: Optional[str],
    job_name: str,
    processed: int,
    skipped_unchanged: int,
    skipped_missing_source: int,
    failed_symbols: int,
    failed_buckets: int,
    failed_finalization: int = 0,
    deferred_buckets: int = 0,
    publication_reason: Optional[str] = None,
    index_path: Optional[str] = None,
    job_run_id: Optional[str] = None,
    run_id: Optional[str] = None,
    source_commit: Any = None,
) -> GoldPublicationFinalizationResult:
    clean_domain = domain_artifacts.normalize_domain(domain)
    clean_reason = str(publication_reason or "").strip() or None
    symbol_failure_count = _coerce_counter(failed_symbols)
    bucket_failure_count = _coerce_counter(failed_buckets)
    finalization_failure_count = _coerce_counter(failed_finalization)
    deferred_bucket_count = _coerce_counter(deferred_buckets)
    domain_artifact_path: Optional[str] = None

    if (
        symbol_failure_count == 0
        and bucket_failure_count == 0
        and finalization_failure_count == 0
        and deferred_bucket_count == 0
    ):
        if index_path is None:
            try:
                index_path = layer_bucketing.write_layer_symbol_index(
                    layer="gold",
                    domain=clean_domain,
                    symbol_to_bucket=symbol_to_bucket,
                )
            except Exception as exc:
                finalization_failure_count += 1
                clean_reason = "index_write_failed"
                _emit_publication_failure_counter(
                    domain=clean_domain,
                    stage="index_write",
                    reason=clean_reason,
                    failed_symbols=symbol_failure_count,
                    failed_buckets=bucket_failure_count,
                    failed_finalization=finalization_failure_count,
                )
                mdc.write_error(f"Gold {clean_domain} symbol index write failed: {exc}")

        if index_path is None and clean_reason is None:
            finalization_failure_count += 1
            clean_reason = "index_unavailable"
            _emit_publication_failure_counter(
                domain=clean_domain,
                stage="index_availability",
                reason=clean_reason,
                failed_symbols=symbol_failure_count,
                failed_buckets=bucket_failure_count,
                failed_finalization=finalization_failure_count,
            )

        if finalization_failure_count == 0 and index_path is not None:
            try:
                artifact = domain_artifacts.write_domain_artifact(
                    layer="gold",
                    domain=clean_domain,
                    date_column=date_column,
                    symbol_count_override=len(symbol_to_bucket),
                    symbol_index_path=index_path,
                    job_name=job_name,
                    job_run_id=job_run_id,
                    run_id=run_id,
                    source_commit=source_commit,
                )
                domain_artifact_path = (
                    str(artifact.get("artifactPath") or "").strip()
                    if isinstance(artifact, dict)
                    else None
                ) or None
            except Exception as exc:
                mdc.write_warning(f"Gold {clean_domain} metadata artifact write failed: {exc}")

    failed_total = symbol_failure_count + bucket_failure_count + finalization_failure_count
    failure_mode = resolve_failure_mode(
        failed_symbols=symbol_failure_count,
        failed_buckets=bucket_failure_count,
        failed_finalization=finalization_failure_count,
    )
    if clean_reason is None and failed_total > 0:
        clean_reason = default_publication_reason(
            failed_symbols=symbol_failure_count,
            failed_buckets=bucket_failure_count,
            failed_finalization=finalization_failure_count,
            deferred_buckets=deferred_bucket_count,
        )
    if clean_reason is None:
        clean_reason = "none"

    if failed_total == 0 and deferred_bucket_count == 0:
        mdc.write_line(
            "artifact_publication_status "
            f"layer=gold domain={clean_domain} status=published reason={clean_reason} "
            f"failure_mode={failure_mode} buckets_ok={int(processed)} failed=0 "
            "failed_symbols=0 failed_buckets=0 failed_finalization=0 "
            f"processed={int(processed)} skipped_unchanged={int(skipped_unchanged)} "
            f"skipped_missing_source={int(skipped_missing_source)} "
            f"deferred_buckets={deferred_bucket_count}"
        )
    elif failed_total == 0:
        mdc.write_line(
            "artifact_publication_status "
            f"layer=gold domain={clean_domain} status=retry_pending reason={clean_reason} "
            f"failure_mode={failure_mode} failed=0 failed_symbols=0 failed_buckets=0 "
            "failed_finalization=0 "
            f"processed={int(processed)} skipped_unchanged={int(skipped_unchanged)} "
            f"skipped_missing_source={int(skipped_missing_source)} "
            f"deferred_buckets={deferred_bucket_count}"
        )
    else:
        mdc.write_line(
            "artifact_publication_status "
            f"layer=gold domain={clean_domain} status=blocked reason={clean_reason} "
            f"failure_mode={failure_mode} failed={failed_total} "
            f"failed_symbols={symbol_failure_count} failed_buckets={bucket_failure_count} "
            f"failed_finalization={finalization_failure_count} processed={int(processed)} "
            f"skipped_unchanged={int(skipped_unchanged)} "
            f"skipped_missing_source={int(skipped_missing_source)} "
            f"deferred_buckets={deferred_bucket_count}"
        )

    return GoldPublicationFinalizationResult(
        failed=failed_total,
        failed_symbols=symbol_failure_count,
        failed_buckets=bucket_failure_count,
        failed_finalization=finalization_failure_count,
        deferred_buckets=deferred_bucket_count,
        failure_mode=failure_mode,
        publication_reason=clean_reason,
        index_path=index_path,
        domain_artifact_path=domain_artifact_path,
    )
