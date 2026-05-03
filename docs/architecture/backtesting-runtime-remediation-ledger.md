# Backtesting Runtime Remediation Ledger

## Strategy Compute Classification

`backtests-job` is classified as `strategy-compute` with `jobKey=backtests`, `jobRole=execute`, and `triggerOwner=control-plane`. `backtests-reconcile-job` is separate `operational-support` infrastructure with `jobKey=backtests` and `jobRole=reconcile`.

System health and the UI must use the API metadata fields, not the job name or medallion layer, when grouping these jobs.

## Purpose
- Single source of truth for decisions, actions, PRs, validation evidence, and rollout notes for the backtesting runtime v2-to-v7 remediation.
- Canonical file path: `C:/Users/rdpro/Projects/asset-allocation-jobs/docs/architecture/backtesting-runtime-remediation-ledger.md`.
- Update this file in every repo PR that changes scope, interfaces, status, or test evidence.

## Program Status
- Overall status: Implemented locally; pending commit, package publication, drift gate, and release choreography
- QA gate: No-go until shared packages `asset-allocation-contracts==3.15.1` and `asset-allocation-runtime-common==3.5.7` resolve in the intended Python 3.14 environment, control-plane migrations are applied, and the jobs shared dependency compatibility gate is green.
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
| Research-safe vNext contracts | `asset-allocation-contracts` | Implemented locally | `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-contracts` | `test_contract_models.py` -> `84 passed`; TS typecheck -> passed | Package publication still pending |
| Research-safe vNext persistence | `asset-allocation-runtime-common` | Implemented locally | `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-runtime-common` | targeted persistence/pin tests -> `14 passed` | Package publication still pending |
| Research-safe vNext control-plane read APIs | `asset-allocation-control-plane` | Implemented locally | `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-control-plane` | targeted repository/API/migration tests -> `53 passed` | Additive migration `0047_backtest_research_safe_v7.sql`; endpoint/repository tests updated |
| Research-safe vNext jobs runtime | `asset-allocation-jobs` | Implemented locally | `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-jobs` | `python -m pytest -q` -> `950 passed, 3 skipped`; quality gates green except published-package compatibility | Shared dependency compatibility gate fails until published `3.15.1` / `3.5.7` resolve |

## Execution Log

### 2026-05-03
- Cross-repo order: `asset-allocation-contracts` v7 contract first, then `asset-allocation-runtime-common` persistence, then `asset-allocation-control-plane` migration/read adoption, then `asset-allocation-jobs` runtime and pin adoption.
- Release status: no-go until `asset-allocation-contracts==3.15.1` and `asset-allocation-runtime-common==3.5.7` are published to the intended package index and `python scripts/workflows/validate_shared_dependency_compatibility.py --repo-root .` resolves them under Python 3.14.
- Trading labels: `execution_model` remains `simple_bps`, `execution_model_quality` remains `not_tca_grade`, and `approval_readiness` remains `research_only`. This remediation does not claim TCA-grade execution, live approval readiness, borrow/locate coverage, factor risk, liquidity concentration controls, or corporate-action reconciliation.

- Repo: `asset-allocation-contracts`
- Branch / PR: `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-contracts`
- Summary: Added authoritative v7 summary metadata and data-quality event response contracts; regenerated JSON schemas and TypeScript contracts; bumped Python and TypeScript manifests to `3.15.1`.
- Test evidence:
  - `PYTHONPATH=...\asset-allocation-contracts-backtest-runtime-remediation\python python -m pytest -q tests\python\test_contract_models.py` -> `84 passed`
  - `npm install` in `ts/` -> passed, `0 vulnerabilities`
  - `npm run typecheck` in `ts/` -> passed
- Blockers: publish `asset-allocation-contracts==3.15.1` before downstream package-index compatibility can pass.

- Repo: `asset-allocation-runtime-common`
- Branch / PR: `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-runtime-common`
- Summary: Bumped package to `3.5.7`, dependency to `asset-allocation-contracts==3.15.1`, and `BACKTEST_RESULTS_SCHEMA_VERSION` to `7`; extended `persist_backtest_results()` with `data_quality_event_rows`, v7 summary metadata, idempotent delete, copy, and row-count verification for `core.backtest_data_quality_events`.
- Test evidence:
  - `PYTHONPATH=...\asset-allocation-runtime-common-backtest-runtime-remediation\python;...\asset-allocation-contracts-backtest-runtime-remediation\python python -m pytest -q tests\python\test_backtest_results.py tests\python\test_verify_pinned_dependency.py` -> `14 passed`
- Blockers: publish after contracts `3.15.1`.

- Repo: `asset-allocation-control-plane`
- Branch / PR: `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-control-plane`
- Summary: Added migration `0047_backtest_research_safe_v7.sql`, v7 summary metadata reads, data-quality event repository methods, and `GET /api/backtests/{run_id}/data-quality-events`.
- Test evidence:
  - `PYTHONPATH=...\asset-allocation-control-plane-backtest-runtime-remediation;...\asset-allocation-runtime-common-backtest-runtime-remediation\python;...\asset-allocation-contracts-backtest-runtime-remediation\python python -m pytest -q tests\core\test_backtest_repository.py tests\api\test_backtests_endpoints.py tests\test_postgres_migrations.py` -> `53 passed`
- Blockers: apply migration before API rollout; install/pin contracts `3.15.1` and runtime-common `3.5.7`.

- Repo: `asset-allocation-jobs`
- Branch / PR: `agent/codex/backtest-runtime-remediation-20260503/asset-allocation-jobs`
- Summary: Pinned contracts/runtime-common to `3.15.1` / `3.5.7`, added runtime-common signature/schema contract test, added CI shared dependency compatibility check, and split strict slow data into session-stable date-only frames and per-bar timestamp-available frames cached by exact `bar_ts`.
- Test evidence:
  - `python scripts\run_quality_gate.py lint-python` with local v7 shared packages on `PYTHONPATH` -> passed
  - `python scripts\run_quality_gate.py test-fast` with local v7 shared packages on `PYTHONPATH` -> `74 passed`
  - `python scripts\run_quality_gate.py test-backtesting-runtime` with local v7 shared packages on `PYTHONPATH` -> `27 passed`
  - `python scripts\run_quality_gate.py test-runtime-common-compat` with local v7 shared packages on `PYTHONPATH` -> `14 passed`
  - `python -m pytest -q` with local v7 shared packages on `PYTHONPATH` -> `950 passed, 3 skipped`
  - `python scripts\workflows\validate_shared_dependency_compatibility.py --repo-root .` -> blocked because `asset-allocation-contracts==3.15.1` is not yet published to the configured index.
  - `python .codex\skills\code-drift-sentinel\scripts\codedrift_sentinel.py --mode audit --repo . --baseline-ref origin/main --skip-quality-gates` -> failed, `drift_score=110.0`, threshold `35.0`; quality gates were run separately above.
- Blockers: package-index compatibility remains blocked until shared packages are published; drift gate remains blocked until protected CI/config and behavioral/test drift receive explicit review/signoff or the drift score is reduced below `35`.

### 2026-05-02
- Repo: `asset-allocation-contracts`
- Branch / PR: TBD
- Summary: Added Research-safe vNext request mode, v7 metadata, execution-honesty labels, data-quality event contracts, JSON schemas, TypeScript contracts, and package version `3.15.1`.
- Test evidence:
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python python -m pytest tests\python -q` -> `100 passed`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-contracts\python python -m pytest tests\python\test_contract_models.py -q` -> `77 passed`
  - `npm install` in `ts/` -> passed, `0 vulnerabilities`
  - `npm run typecheck` in `ts/` -> passed
- Blockers: `asset-allocation-contracts==3.15.1` must be published before index-based downstream dependency gates can pass.

- Repo: `asset-allocation-runtime-common`
- Branch / PR: TBD
- Summary: Bumped `BACKTEST_RESULTS_SCHEMA_VERSION` to `7`, added run-summary metadata persistence, added `core.backtest_data_quality_events` copy support, and retained row-count verification.
- Test evidence:
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-runtime-common\python;C:\Users\rdpro\Projects\asset-allocation-contracts\python python -m pytest tests\python -q` -> `124 passed`
  - `PYTHONPATH=C:\Users\rdpro\Projects\asset-allocation-runtime-common\python;C:\Users\rdpro\Projects\asset-allocation-contracts\python python -m pytest tests\python\test_backtest_results.py -q` -> `1 passed`
- Blockers: `asset-allocation-runtime-common==3.5.7` must be published after the contracts package.

- Repo: `asset-allocation-control-plane`
- Branch / PR: TBD
- Summary: Added vNext metadata fields to backtest read responses, repository accessors for data-quality events, `GET /api/backtests/{run_id}/data-quality-events`, and additive migration `0047_backtest_research_safe_v7.sql`.
- Test evidence:
  - `PYTHONPATH=... python -m py_compile api\endpoints\backtests.py api\service\backtest_contracts_compat.py core\backtest_repository.py` -> passed
  - `PYTHONPATH=... python -m pytest tests\api\test_backtests_endpoints.py -q` -> blocked at collection because `oauthlib` is not installed in the local Python 3.13 environment.
- Blockers: Local test environment is missing control-plane runtime dependencies.

- Repo: `asset-allocation-jobs`
- Branch / PR: TBD
- Summary: Defaulted runs to strict research integrity, made date-only slow data prior-session-only, allowed timestamped slow data only by `available_at <= bar_ts`, failed strict runs before result publication on missing held/selected execution data, left residual cash when selected names are below `topN`, processed sells before buys, reserved simple flat-bps costs for buys, stamped research-only/simple-bps metadata, added local migration `0042_backtest_research_safe_vnext.sql`, and aligned shared package pins.
- Test evidence:
  - `PYTHONPATH=... python scripts\run_quality_gate.py lint-python` -> passed
  - `PYTHONPATH=... python scripts\run_quality_gate.py test-fast` -> `72 passed`
  - `PYTHONPATH=... python scripts\run_quality_gate.py test-backtesting-runtime` -> `23 passed`
  - `PYTHONPATH=... python scripts\run_quality_gate.py test-control-plane-compat` -> `22 passed`
  - `PYTHONPATH=... python scripts\run_quality_gate.py test-runtime-common-compat` -> `12 passed`
  - `PYTHONPATH=... python -m pytest tests\core\test_backtest_runtime.py tests\test_postgres_migrations.py -q` -> `37 passed`
  - `PYTHONPATH=... python -m pytest -q` -> `944 passed, 3 skipped`
  - `PYTHONPATH=... python scripts\workflows\validate_shared_dependency_compatibility.py --repo-root .` -> blocked because `3.15.1` / `3.5.7` needed publication and the local interpreter was Python 3.13 while shared packages require Python 3.14.
- Blockers: Publish contracts/runtime-common packages, then rerun dependency compatibility in the intended Python 3.14 environment.

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
- D-008: Research-safe vNext stamps schema version `7`, defaults new runs to `researchIntegrityMode=strict`, and labels the output `research_only` with `simple_bps` / `not_tca_grade`.
Reason: the runtime is now safer for deterministic research replay, but it still does not provide approval-grade TCA, OOS/walk-forward validation, factor attribution, or live risk controls.

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
Exit criteria: runtime gate remains green in CI.
- A-008 Research-safe vNext Status: Implemented locally
Owner: contracts/runtime-common/control-plane/jobs
Work: add v7 metadata and data-quality event contracts, persist/read diagnostics, enforce strict point-in-time and fail-fast data behavior in the jobs runtime, add additive database migrations, align shared dependency pins, and document research-only execution-quality caveats.
Exit criteria: `asset-allocation-contracts==3.15.1` and `asset-allocation-runtime-common==3.5.7` are published, downstream compatibility gates pass on Python 3.14, and control-plane endpoint tests pass with dependencies installed.
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
