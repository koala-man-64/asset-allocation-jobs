# Asset Allocation Jobs

Runtime-owned jobs repository for:
- `tasks/` batch jobs and backtesting worker runtime
- provider adapters in `alpha_vantage/`, `massive_provider/`, and `alpaca/`
- jobs-side `core/` runtime modules
- medallion pipelines for market, finance, earnings, price targets, regime state, and multi-source economic catalysts

Local development installs versioned shared packages rather than sibling repos:

```powershell
python -m pip install asset-allocation-contracts==2.1.0
python -m pip install asset-allocation-runtime-common==2.0.8
python scripts/run_quality_gate.py check-fast
```

Run the full suite separately when needed:

```powershell
python -m pytest -q
```

Refresh the shared package pins with Codex:

```powershell
.\scripts\refresh_shared_dependencies_with_codex.ps1
.\scripts\refresh_shared_dependencies_with_codex.ps1 -ExecutionMode full-auto
```

The wrapper stores the generated prompt, console log, and final Codex summary under `artifacts/codex/shared-dependency-refresh/<timestamp>/`.

Cross-repo control data is read from the control-plane over HTTP. Configure:

- `ASSET_ALLOCATION_API_BASE_URL`
- `ASSET_ALLOCATION_API_SCOPE`

In prod, set `ASSET_ALLOCATION_API_BASE_URL` to the internal control-plane service URL `http://asset-allocation-api-vnet`. Do not point jobs at a public ACA ingress FQDN.

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
- `docs/ops/networking-audit-2026-04-18.md` captures the live Azure networking posture observed on April 18, 2026 and the prioritized hardening backlog.
- `docs/ops/economic-catalyst-data.md` documents the economic catalyst Bronze/Silver/Gold pipeline, Postgres serving tables, source precedence, and replay expectations.

## Backtesting

Backtest lifecycle state is owned by `asset-allocation-contracts` plus the control plane. This repo only owns the worker runtime, artifacts, and ACA job manifests.

- `deploy/job_backtests.yaml` remains the manual single-run worker job.
- `deploy/job_backtests_reconcile.yaml` is the scheduled recovery job that asks the control plane to redispatch stranded queued runs and fail stale running runs.
- `tasks/backtesting/worker.py` now performs fail-fast dependency preflight before looking up a targeted run or claiming queued work.
- `core/backtest_runtime.py` sends wall-clock heartbeats during long sections, writes Postgres-backed v4 results, and now publishes net and gross return metrics, cost drag, corrected `net_exposure`, trade lifecycle fields, and flat-to-flat closed-position analytics. There is no cross-run persistent cache.
- `scripts/profile_backtest_runtime.py` is the profiling harness for the multiprocessing gate. `BACKTEST_RANKING_MAX_WORKERS` remains a benchmark-only knob and defaults to `1`.
- Multiprocessing is intentionally disabled by default. Scale backtests by starting more ACA executions, not by turning one worker into a multi-run drain loop.
