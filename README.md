# Asset Allocation Jobs

Runtime-owned jobs repository for:
- `tasks/` batch jobs and backtesting worker runtime
- provider adapters in `alpha_vantage/`, `massive_provider/`, and `alpaca/`
- jobs-side `core/` runtime modules

Local development installs versioned shared packages rather than sibling repos:

```powershell
python -m pip install asset-allocation-contracts==1.1.0
python -m pip install asset-allocation-runtime-common==2.0.0
python -m pytest -q
```

Cross-repo control data is read from the control-plane over HTTP. Configure:

- `ASSET_ALLOCATION_API_BASE_URL`
- `ASSET_ALLOCATION_API_SCOPE`

## Operations

Canonical workflows live under `.github/workflows/`.

- `quality.yml` is the required validation path for PRs and `main`, and it also runs scheduled dependency audits and governance checks.
- `integration.yml` owns cross-repo compatibility validation and contracts adoption.
- `release.yml` builds the jobs image and writes `release-manifest.json`.
- `deploy-prod.yml` is the only workflow allowed to apply `deploy/job_*.yaml`.
- `scripts/ops/trigger_job.py` is the approved manual ACA job trigger entrypoint.
- `scripts/setup-env.ps1` builds repo-local `.env.web` using Azure and git discovery where possible.
- `scripts/sync-all-to-github.ps1` syncs the `.env.web` surface into repo vars and secrets.
- `DEPLOYMENT_SETUP.md` is the canonical deploy, operate, and rollback runbook.

## Backtesting

Backtest lifecycle state is owned by `asset-allocation-contracts` plus the control plane. This repo only owns the worker runtime, artifacts, and ACA job manifests.

- `deploy/job_backtests.yaml` remains the manual single-run worker job.
- `deploy/job_backtests_reconcile.yaml` is the scheduled recovery job that asks the control plane to redispatch stranded queued runs and fail stale running runs.
- `tasks/backtesting/worker.py` now performs fail-fast dependency preflight before looking up a targeted run or claiming queued work.
- `core/backtest_runtime.py` sends wall-clock heartbeats during long sections, writes Postgres-backed v4 results, and now publishes net and gross return metrics, cost drag, corrected `net_exposure`, trade lifecycle fields, and flat-to-flat closed-position analytics. There is no cross-run persistent cache.
- `scripts/profile_backtest_runtime.py` is the profiling harness for the multiprocessing gate. `BACKTEST_RANKING_MAX_WORKERS` remains a benchmark-only knob and defaults to `1`.
- Multiprocessing is intentionally disabled by default. Scale backtests by starting more ACA executions, not by turning one worker into a multi-run drain loop.
