# Gold Publication Status

## Summary

All gold jobs now emit the same final `artifact_publication_status` contract, but there are two execution patterns:

- Bucketed jobs (`market`, `finance`, `earnings`, `price-target`) persist bucket state incrementally and publish root `domain.json` once at end-of-run.
- `regime` is a single-domain publication. It writes parquet surfaces first, then finalizes one shared publish-state payload across artifact metadata, watermarks, and health markers.

This applies to:

- `market`
- `finance`
- `earnings`
- `price-target`
- `regime`

## Final Log Fields

The final `artifact_publication_status` log now includes:

- `reason`
- `failure_mode`
- `failed`
- `failed_symbols`
- `failed_buckets`
- `failed_finalization`
- `processed`
- `skipped_unchanged`
- `skipped_missing_source`

Legacy fields remain for compatibility:

- `reason`
- `failed`
- `processed`

The regime job also persists a shared publish-state payload across:

- `regime/_metadata/domain.json`
- `system/watermarks/gold_regime_features.json`
- `system/watermarks/runs/gold_regime_features.json`
- `system/health_markers/gold/regime.json`

Shared regime publish-state fields:

- `published_as_of_date`
- `input_as_of_date`
- `history_rows`
- `latest_rows`
- `transition_rows`
- `active_models`
- `downstream_triggered`
- `status`
- `reason`
- `failure_mode`

## Failure Modes

- `none`: the run finalized successfully and the root artifact was published.
- `symbol`: the run wrote at least one bucket, but one or more symbol-level compute paths failed.
- `bucket`: one or more buckets failed at a hard bucket stage such as write or checkpoint.
- `finalization`: the bucket work completed, but final publication failed during end-of-run checks or final index publication.
- `mixed`: more than one failure class occurred in the same run.

## Operational Interpretation

- Treat root `domain.json` as final domain state only. It should not appear mid-run anymore.
- During a live run, bucket artifacts plus the gold symbol index remain the interim source of truth.
- For root-cause analysis, use the final publication log to classify the failure, then inspect earlier per-bucket logs to find the specific failing symbol or bucket.
- For `regime`, stale end-of-day inputs fail closed. The job emits `artifact_publication_status ... status=retry_pending reason=stale_eod_input` and does not advance success metadata or downstream work.

## Example

```text
artifact_publication_status layer=gold domain=market status=blocked reason=failed_buckets failure_mode=symbol failed=3 failed_symbols=3 failed_buckets=0 failed_finalization=0 processed=12 skipped_unchanged=5 skipped_missing_source=1
```

Interpretation:

- bucket writes were partially successful
- publication was blocked by symbol-level failures, not hard bucket failures
- the root artifact was intentionally withheld at finalization

## Rollback Note

This change does not require a storage migration.

Rollback is limited to:

- `tasks/common/gold_checkpoint_publication.py`
- the four gold job call sites
- test and doc updates

Reverting those files restores the prior publication timing and log contract.
