# Strategy Compute Jobs

## Classification

`strategy-compute` is the workflow category for jobs that compute strategy state rather than move data through the medallion pipeline.

| ACA job | category | key | role | trigger owner |
| --- | --- | --- | --- | --- |
| `gold-regime-job` | `strategy-compute` | `regime` | `publish` | `schedule` |
| `platinum-rankings-job` | `strategy-compute` | `rankings` | `materialize` | `control-plane` |
| `backtests-job` | `strategy-compute` | `backtests` | `execute` | `control-plane` |
| `backtests-reconcile-job` | `operational-support` | `backtests` | `reconcile` | `reconciler` |
| `results-reconcile-job` | `operational-support` | `results-reconcile` | `reconcile` | `reconciler` |

Regime writes gold outputs and rankings write platinum outputs, but neither job is classified as a medallion data-pipeline stage.

## Metadata Source

Each `deploy/job_*.yaml` manifest declares:

- `job-category`
- `job-key`
- `job-role`
- `trigger-owner`

Deploy rendering validates those tags before applying ACA resources. The control-plane reads ACA tags, validates them against the runtime-common catalog, and exposes normalized API fields: `jobCategory`, `jobKey`, `jobRole`, `triggerOwner`, `metadataSource`, and `metadataStatus`.

If tags are missing on an already-deployed cataloged job, system health uses `metadataStatus=fallback`. If tags are present but wrong, system health uses `metadataStatus=invalid` and deployment validation blocks the same manifest.

## Regime Publication Signal

`gold-regime-job` no longer uses `TRIGGER_NEXT_JOB_NAME` to chain into results reconciliation. After publishing regime DB rows and artifacts, it records a durable control-plane signal:

- key: `jobKey + sourceFingerprint`
- initial state: `pending`
- duplicate fingerprint: coalesces into the existing row
- prior `error` state: requeues to `pending`
- `processed` state: preserved

The signal metadata is typed and limited to the published regime window, row counts, active models, artifact path, and producer job name. The signal POST is retried a small bounded number of times by runtime-common; a final failure is not reported as clean job success and prevents success watermarks, health markers, and last-success markers from being written.

`results-reconcile-job` is the operational-support sweeper. It runs every 30 minutes, claims pending signals with a lease, marks successful batches `processed`, and schedules retry after transient errors.

## Rollback

Rollback does not require data rollback. ACA job names and operator aliases are unchanged.

If the new signal endpoint or scheduled reconciler is unhealthy:

1. Roll back the jobs image to the previous digest.
2. Keep the signal table in place; it is additive.
3. Start `results-reconcile-job` manually for repair with `python scripts\ops\trigger_job.py --job results-reconcile --resource-group AssetAllocationRG`.
4. Do not reintroduce `TRIGGER_NEXT_JOB_NAME` unless the whole release train is being rolled back to the pre-`strategy-compute` contract set.

## Validation

- `python -m pytest tests/test_workflow_runtime_ownership.py tests/test_workflow_scripts.py -q`
- `python -m pytest tests/tasks/common/test_regime_publication.py tests/tasks/test_gold_regime_data.py -q`
- `python -m pytest tests/test_postgres_migrations.py::test_strategy_publication_reconcile_migration_creates_idempotent_signal_table -q`
