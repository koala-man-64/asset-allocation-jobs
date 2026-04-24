# Backtesting Runtime Remediation Ledger

## Strategy Compute Classification

`backtests-job` is classified as `strategy-compute` with `jobKey=backtests`, `jobRole=execute`, and `triggerOwner=control-plane`. `backtests-reconcile-job` is separate `operational-support` infrastructure with `jobKey=backtests` and `jobRole=reconcile`.

System health and the UI must use the API metadata fields, not the job name or medallion layer, when grouping these jobs.

## Purpose
- Single source of truth for decisions, actions, PRs, validation evidence, and rollout notes for the backtesting runtime v2-to-v4 remediation.
- Canonical file path: `C:/Users/rdpro/Projects/asset-allocation-jobs/docs/architecture/backtesting-runtime-remediation-ledger.md`.
- Update this file in every repo PR that changes scope, interfaces, status, or test evidence.

## Program Status
- Overall status: Implemented locally; pending commit and release choreography
- Program type: Cross-repo coordinated remediation
- Routing decision: This is a contracts-repo-first change.
- Repos in scope: `asset-allocation-contracts`, `asset-allocation-control-plane`, `asset-allocation-runtime-common`, `asset-allocation-jobs`

## Status Board

| Workstream | Repo | Status | PR / Branch | Test Evidence | Blockers / Notes |
| --- | --- | --- | --- | --- | --- |
| Contracts v4 fields | `asset-allocation-contracts` | Implemented locally | Uncommitted workspace | Pending re-run after local v4 edits | Package publication and downstream version adoption still pending |
| Control-plane readiness endpoint | `asset-allocation-control-plane` | Implemented locally | Uncommitted workspace | `29 passed`, reconcile smoke `1 passed`, targeted `ruff` passed | Uses local response synthesis until published contracts package is consumed |
| Runtime-common v4 persistence | `asset-allocation-runtime-common` | Implemented locally | Uncommitted workspace | Pending re-run after local v4 edits | Package publication still pending |
| Jobs runtime behavior | `asset-allocation-jobs` | Implemented locally | Uncommitted workspace | Pending re-run after local v4 edits | Live profiling not run because safe DB inputs were unavailable |
| Closed-position analytics surface | `asset-allocation-control-plane`, `asset-allocation-jobs` | Implemented locally | Uncommitted workspace | Pending re-run after local v4 edits | Requires additive Postgres migrations `0035` and `0036` in both repos |
| Worker preflight | `asset-allocation-jobs` | Implemented locally | Uncommitted workspace | Included in `tests/tasks/test_backtesting_worker.py` and runtime gate | Depends on control-plane readiness endpoint remaining stable |
| Dedicated runtime quality gate | `asset-allocation-jobs` | Implemented locally | Uncommitted workspace | `python scripts/run_quality_gate.py test-backtesting-runtime` passed | CI workflow updated but not exercised in GitHub Actions here |
| Docs and traceability | `asset-allocation-jobs` | Implemented locally | Uncommitted workspace | Docs updated; ledger evidence recorded below | PR links and release notes remain `TBD` until commit/publish |

## Execution Log

### 2026-04-17
- Summary: Extended the earlier v2 remediation into additive v3 and v4 result schemas: corrected `net_exposure`, added gross-return and cost-drag summary fields, added `position_id` and `trade_role` to trade rows, and added flat-to-flat closed-position analytics plus summary trade-quality metrics.

- Repo: `asset-allocation-jobs`
- Branch / PR: TBD
- Summary: Seeded the backtesting runtime v2 remediation ledger and linked it from the master design contract.
- Test evidence: N/A for this docs-only pass.
- Blockers: Upstream cross-repo implementation work is still pending.

- Repo: `asset-allocation-contracts`
- Branch / PR: TBD
- Summary: Added additive v2 backtest result metadata, `period_return`, `window_periods`, updated JSON schemas, TypeScript contracts, tests, and contract docs.
- Test evidence:
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python python -m pytest -q tests/python/test_contract_models.py tests/python/test_documented_configuration_examples.py` -> `22 passed`
  - `npm run typecheck` in `ts/` -> passed
  - `python -m ruff check python/asset_allocation_contracts/backtest.py tests/python/test_contract_models.py` -> passed
- Blockers: Published package/version bump work is not part of this workspace pass.

- Repo: `asset-allocation-runtime-common`
- Branch / PR: TBD
- Summary: Added `ControlPlaneTransport.probe(path)`, bumped backtest results schema version to `2`, added additive persistence support for `period_return` and `window_periods`, and removed eager record copying.
- Test evidence:
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/python/test_control_plane_transport.py tests/python/test_backtest_results.py` -> `8 passed`
  - `python -m ruff check python/asset_allocation_runtime_common/control_plane_transport.py python/asset_allocation_runtime_common/backtest_results.py tests/python/test_control_plane_transport.py tests/python/test_backtest_results.py` -> passed
- Blockers: Published package/version bump work is not part of this workspace pass.

- Repo: `asset-allocation-control-plane`
- Branch / PR: TBD
- Summary: Added authenticated non-mutating readiness endpoint, synthesized additive v2 backtest response metadata and compatibility fields, and aligned migrations/tests with `period_return` and `window_periods`.
- Test evidence:
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/api/test_backtests_endpoints.py tests/api/test_internal_endpoints.py tests/test_postgres_migrations.py` -> `29 passed`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/api/test_internal_backtest_reconcile_smoke.py` -> `1 passed`
  - `python -m ruff check api/endpoints/backtests.py api/endpoints/internal.py core/backtest_repository.py tests/api/test_backtests_endpoints.py tests/api/test_internal_endpoints.py tests/test_postgres_migrations.py` -> passed
- Blockers: This pass stops at local implementation; no package publication or control-plane PR metadata has been recorded yet.

- Repo: `asset-allocation-jobs`
- Branch / PR: TBD
- Summary: Preserved `PositionState` on rebalance resize, made metrics cadence-aware, switched worker preflight to authenticated readiness probing, added the runtime quality gate, added the local Postgres migration, and updated docs/ledger.
- Test evidence:
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/core/test_backtest_runtime.py tests/tasks/test_backtesting_worker.py tests/core/test_control_plane_transport.py tests/test_postgres_migrations.py` -> `26 passed`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python scripts/run_quality_gate.py test-backtesting-runtime` -> `12 passed`
  - `python -m ruff check core/backtest_runtime.py tasks/backtesting/worker.py tests/core/test_backtest_runtime.py tests/tasks/test_backtesting_worker.py tests/core/test_control_plane_transport.py scripts/run_quality_gate.py tests/test_postgres_migrations.py` -> passed
- Blockers: Safe `POSTGRES_DSN` and representative `run_id` were not available, so live profiling evidence remains open.

## Locked Decisions
- D-001: Introduce backtest results schema v2 instead of silently reusing v1 daily-only semantics.
Reason: v1 hard-codes `daily_return`, `window_days`, and schema version `1`, which is not correct for intraday runs and will otherwise force another migration later.
- D-002: Keep v2 additive and backward-compatible.
Decision: add `period_return`, `window_periods`, and response `metadata`; retain deprecated `daily_return` and `window_days` for daily-cadence compatibility and historical reads.
- D-003: Keep strategy scope long-only in this program.
Decision: do not redesign `net_exposure`, short exposure, borrow, or liquidity modeling here.
- D-004: Add a dedicated authenticated readiness endpoint in `asset-allocation-control-plane`.
Decision: worker preflight must not depend on claim, start, fail, or reconcile routes to prove auth and reachability.
- D-005: Add a dedicated `test-backtesting-runtime` CI gate instead of widening `test-fast`.
- D-006: Create this ledger file as the first repo mutation and link it from `docs/architecture/master-design-contract.md`.
- D-007: Keep the database default `results_schema_version` at `1` and stamp `2` when v2 results are published.
Reason: this avoids mislabeling historical or unpublished runs while `persist_backtest_results()` marks v2 output explicitly at publish time.

## Action Ledger
- A-001 `asset-allocation-contracts` Status: Implemented locally
Owner: contracts
Work: add `period_return` to timeseries, add `window_periods` and response `metadata`, deprecate `daily_return` and `window_days`, document `strategy_scope = long_only`, add v1 and v2 examples, and add model-validation tests.
Exit criteria: contract package is released and consumable by downstream repos.
- A-002 `asset-allocation-control-plane` Status: Implemented locally
Owner: control-plane
Work: add `GET /api/internal/backtests/ready`, adopt contracts v2, synthesize v1 rows to v2 read responses, expose `metadata.results_schema_version`, `metadata.bar_size`, `metadata.periods_per_year`, and `metadata.strategy_scope`, and add endpoint tests.
Exit criteria: worker can prove auth and readiness without mutating run state; v1 and v2 read paths both pass.
- A-003 `asset-allocation-runtime-common` Status: Implemented locally
Owner: runtime-common
Work: add `ControlPlaneTransport.probe(path)`, adopt contracts v2, bump `BACKTEST_RESULTS_SCHEMA_VERSION` to `2`, support `period_return` and `window_periods`, remove eager record copying, and keep legacy column support.
Exit criteria: probe tests and persistence v2 tests pass; downstream jobs repo can publish v2 rows.
- A-004 `asset-allocation-jobs` runtime Status: Implemented locally
Owner: jobs
Work: preserve `PositionState` on rebalance resize; only true opens create new state; derive `periods_per_year` from `bar_size`; compute summary and rolling metrics from cadence-aware period returns; stop `DataFrame.to_dict("records")` duplication before publish; keep long-only exposure behavior explicit.
Exit criteria: resize-state and intraday metric tests pass.
- A-005 `asset-allocation-jobs` worker Status: Implemented locally
Owner: jobs
Work: preflight order is env, Postgres, then `probe("/api/internal/backtests/ready")`; log `preflight_ok` only after all pass; reuse validated transport where practical; preserve the primary runtime exception if `fail_run()` also fails; add stage timing and row-count telemetry.
Exit criteria: auth, scope, base URL, timeout, 401, and 503 failure tests pass before claim or get-run; nested fail-run error handling passes.
- A-006 `asset-allocation-jobs` database Status: Implemented locally
Owner: jobs
Work: add migration `0034_backtest_results_postgres_cutover.sql` with nullable `period_return` and `window_periods`; keep legacy columns readable.
Exit criteria: migration tests pass and historical data remains queryable.
- A-007 CI and release Status: Implemented locally
Owner: jobs
Work: add `test-backtesting-runtime` to `scripts/run_quality_gate.py` and `.github/workflows/quality.yml`; keep `test-fast` lean.
Exit criteria: required workflow passes with the new gate.
- A-008 Docs and traceability Status: Implemented locally
Owner: jobs
Work: update `docs/architecture/master-design-contract.md`, `DEPLOYMENT_SETUP.md`, and this ledger with PR links, test evidence, blockers, and rollout notes.
Exit criteria: docs merge with code and all ledger sections are populated.

## Execution Order
1. Seed this ledger file and link it from `docs/architecture/master-design-contract.md`.
2. Ship `asset-allocation-contracts` v2 fields and docs.
3. Ship `asset-allocation-control-plane` readiness endpoint and v1-to-v2 read synthesis.
4. Ship `asset-allocation-runtime-common` probe and v2 persistence support.
5. Ship `asset-allocation-jobs` runtime, worker, migration, tests, CI, and docs.
6. Run coordinated end-to-end validation and record evidence in this ledger.

## Validation Ledger
- Required tests:
  - resize-in-place preserves `entry_date`, `entry_price`, `bars_held`, `highest_since_entry`, and `lowest_since_entry`
  - open, partial reduce, then exit does not reset exit-state logic
  - daily and intraday runs produce correct `period_return`, `periods_per_year`, `window_periods`, and annualized metrics
  - worker preflight fails on bad auth, bad scope, bad base URL, timeout, 401, and 503 before `claim_next_run()` or `get_run()`
  - nested `fail_run()` failure preserves the original runtime error
  - hermetic `execute_backtest_run()` fixture validates summary, timeseries, rolling metrics, trades, traces, heartbeat, and completion
  - v1 historical results remain readable through v2 API responses
- Optional evidence when safe inputs exist: run `scripts/profile_backtest_runtime.py` with a safe `run_id` and `POSTGRES_DSN` and record timings.

### Recorded Evidence

- `asset-allocation-contracts`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python python -m pytest -q tests/python/test_contract_models.py tests/python/test_documented_configuration_examples.py` -> `22 passed`
  - `npm run typecheck` in `ts/` -> passed
- `asset-allocation-runtime-common`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/python/test_control_plane_transport.py tests/python/test_backtest_results.py` -> `8 passed`
- `asset-allocation-control-plane`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/api/test_backtests_endpoints.py tests/api/test_internal_endpoints.py tests/test_postgres_migrations.py` -> `29 passed`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/api/test_internal_backtest_reconcile_smoke.py` -> `1 passed`
- `asset-allocation-jobs`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python -m pytest -q tests/core/test_backtest_runtime.py tests/tasks/test_backtesting_worker.py tests/core/test_control_plane_transport.py tests/test_postgres_migrations.py` -> `26 passed`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python;C:\Users\rdpro\Projects\asset-allocation-runtime-common\python python scripts/run_quality_gate.py test-backtesting-runtime` -> `12 passed`

## Ledger Update Rules
- Every status change must add date, repo, branch or PR link, summary of change, test evidence, and blockers.
- Every new decision must get a stable `D-###` id and a one-line reason.
- Every repo action must get a stable `A-###` id and explicit exit criteria.
- Do not mark any action `Done` until linked tests and docs evidence are recorded here.
