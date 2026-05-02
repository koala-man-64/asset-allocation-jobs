# Asset Allocation Jobs

Runtime-owned jobs repository for:
- `tasks/` batch jobs and backtesting worker runtime
- symbol cleanup and AI enrichment worker runtime
- provider adapters in `alpha_vantage/`, `massive_provider/`, and `alpaca/`
- jobs-side `core/` runtime modules
- medallion pipelines for market, finance, earnings, price targets, regime state, and multi-source economic catalysts

Local development installs versioned shared packages rather than sibling repos:

```powershell
python -m pip install asset-allocation-contracts==3.14.0
python -m pip install asset-allocation-runtime-common==3.5.4
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

In prod, set `ASSET_ALLOCATION_API_BASE_URL` to an internal control-plane service URL. The current restore target is `http://asset-allocation-api`; switch to `http://asset-allocation-api-vnet` only after the VNet-backed app exists and resolves from ACA Jobs. Do not point jobs at a public ACA ingress FQDN.

## Quiver Pipeline

Quiver runs as a control-plane-gated Bronze/Silver/Gold pipeline in this repo.

- Bronze stays API-backed for provider access in one ACA Job:
  - `incremental`: persisted hourly weekday schedule for global live feeds plus a rotating ticker slice
  - `historical_backfill`: operator-started one-off execution override for ticker-heavy historical feeds
- Bronze is disabled by default with `QUIVER_DATA_ENABLED=false`. A disabled run exits `0` before creating a Quiver client, writing artifacts, health markers, or triggering Silver.
- Quiver feed coverage includes live/global insider trading, Wall Street Bets, and patents, plus ticker-rotated historical Wall Street Bets and patents during manual backfills.
- The ticker universe is resolved directly from Postgres for both scheduled and manual runs.
- Bronze and Silver persist under `quiver-data/...`; Gold persists under `quiver/...`.

Key Quiver envs:

- `AZURE_FOLDER_QUIVER`
- `QUIVER_DATA_ENABLED`
- `QUIVER_DATA_JOB_MODE`
- `QUIVER_DATA_TICKER_BATCH_SIZE`
- `QUIVER_DATA_HISTORICAL_BATCH_SIZE`
- `QUIVER_DATA_SYMBOL_LIMIT`
- `QUIVER_DATA_PAGE_SIZE`
- `QUIVER_DATA_MAX_PAGES_PER_REQUEST`
- `QUIVER_DATA_SEC13F_TODAY_ONLY`

## Operations

Canonical workflows live under `.github/workflows/`.

- `quality.yml` is the required validation path for PRs and `main`, and it also runs scheduled dependency audits and governance checks.
- `release.yml` builds the jobs image and writes `release-manifest.json` after successful `quality.yml` runs on `main`, with manual dispatch retained for operator-approved rebuilds.
- `deploy-prod.yml` is the only workflow allowed to apply `deploy/job_*.yaml`.
- `scripts/ops/trigger_job.py` is the approved manual ACA job trigger entrypoint.
- `scripts/setup-env.ps1` builds repo-local `.env.web` using Azure and git discovery where possible.
- `scripts/sync-all-to-github.ps1` syncs the `.env.web` surface into repo vars and secrets.
- `DEPLOYMENT_SETUP.md` is the canonical deploy, operate, and rollback runbook.
- `docs/ops/bronze-runtime-hardening.md` documents the Bronze deployment drift checks, safe defaults, canary order, and Log Analytics alert queries.
- `docs/ops/networking-audit-2026-04-18.md` captures the live Azure networking posture observed on April 18, 2026 and the prioritized hardening backlog.
- `docs/ops/economic-catalyst-data.md` documents the economic catalyst Bronze/Silver/Gold pipeline, Postgres serving tables, source precedence, and replay expectations.

## Symbol Cleanup

Symbol enrichment lifecycle state is owned by the control-plane plus shared contracts. This repo only owns the worker runtime and the ACA job manifest.

- `deploy/job_symbol_cleanup.yaml` is the scheduled weekday worker job that runs at `23:00 UTC` (`0 23 * * 1-5`) and drains queued symbol cleanup work from the control-plane. Operators can still start it manually for repair or replay.
- `tasks.symbol_cleanup.worker` loads provider facts and current profile state from Postgres, applies deterministic normalization first, then asks the control-plane enrichment endpoint for the remaining AI-owned fields while draining a bounded serial pass of queued work per execution.
- The worker never streams `/api/ai/chat/stream` directly and never auto-overrides locked fields.

## Backtesting

Backtest lifecycle state is owned by `asset-allocation-contracts` plus the control plane. This repo only owns the worker runtime, artifacts, and ACA job manifests.

- `deploy/job_backtests.yaml` remains the manual single-run worker job.
- `deploy/job_backtests_reconcile.yaml` is the scheduled recovery job that asks the control plane to redispatch stranded queued runs and fail stale running runs.
- `deploy/job_intraday_monitor.yaml` is the scheduled intraday watchlist poller. It claims due runs from the control plane and posts symbol observations plus refresh candidates back to the internal intraday APIs.
- `deploy/job_intraday_market_refresh.yaml` is the scheduled targeted refresh worker. It drains queued intraday market batches and runs the existing selected-symbol Bronze/Silver/Gold market path in-process without chaining the full downstream job fan-out.
- `tasks/backtesting/worker.py` now performs fail-fast dependency preflight before looking up a targeted run or claiming queued work.
- `core/backtest_runtime.py` sends wall-clock heartbeats during long sections, writes Postgres-backed v5 results, and now publishes net and gross return metrics, cost drag, corrected `net_exposure`, trade lifecycle fields, and flat-to-flat closed-position analytics. There is no cross-run persistent cache.
- Default-regime backtest policy is observe-only under the published contracts. The runtime records regime trace rows but does not block entries or rescale exposure from default-regime state.
- `scripts/profile_backtest_runtime.py` is the profiling harness for the multiprocessing gate. `BACKTEST_RANKING_MAX_WORKERS` remains a benchmark-only knob and defaults to `1`.
- Multiprocessing is intentionally disabled by default. Scale backtests by starting more ACA executions, not by turning one worker into a multi-run drain loop.
