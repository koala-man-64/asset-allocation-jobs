# Master Design Contract For `asset-allocation-jobs`

Status: Canonical current-state design contract for this repository.  
Audience: Engineers and agents changing jobs runtime code, deployment manifests, workflows, configuration surfaces, or cross-repo interfaces.  
Update rule: Update this document in the same PR as any material runtime, deploy, configuration, workflow, or interface-contract change.
Active remediation ledger: `docs/architecture/backtesting-runtime-remediation-ledger.md` for the coordinated backtesting runtime v2 program.

## 1. Executive Summary

`asset-allocation-jobs` is the jobs-side runtime for the broader Asset Allocation system. It owns scheduled and manually triggered batch workloads, provider adapters, job-side shared runtime modules, and Azure Container Apps Jobs manifests. It exists so ingestion, transformation, ranking, regime recompute, and backtesting work can evolve and deploy independently from the control-plane API and UI while still consuming shared contracts and runtime-common clients as versioned dependencies.

This repo is not the control plane, not the UI, and not the shared Azure bootstrap owner. Cross-repo control data is read over authenticated HTTP via runtime-common transport and repository clients rather than by importing control-plane source modules directly. The intended result is a jobs-only runtime with explicit repo boundaries, explicit deployment ownership, and executable contract tests around configuration, workflow ownership, and cross-repo interfaces.

### Evidence

- `README.md`
- `pyproject.toml`
- `DEPLOYMENT_SETUP.md`
- `docs/architecture/backtesting-runtime-remediation-ledger.md`
- `docs/architecture/original-monolith-and-five-repo-map.md`
- `core/control_plane_transport.py`

### Unverified / Needs confirmation

- The exact downstream consumer list for every gold or platinum output is not fully proven from this repo alone.

## 2. Repo Boundary

### In scope

- `tasks/`: deployable job entrypoints and domain-specific orchestration for bronze, silver, gold, regime, ranking, monitoring, and backtesting flows.
- `core/`: jobs-side shared runtime helpers, storage and bucketing utilities, logging/configuration helpers, provider gateway clients, repositories re-exported from runtime-common, and domain engines used by jobs.
- `alpaca/`, `alpha_vantage/`, `massive_provider/`: provider integration surfaces used by job flows.
- `monitoring/`: job-relevant readiness, resource health, control-plane wake-up, and system-health support modules used by runtime operations.
- `deploy/job_*.yaml`: Azure Container Apps Jobs resources owned and deployed by this repo.
- `.github/workflows/`: CI, release, and deploy workflows for the jobs runtime.
- `docs/ops/env-contract.*`: authoritative runtime configuration contract for repo variables and secrets.

### Out of scope

- Control-plane API implementation and operator-state ownership.
- UI implementation and UI deployment ownership.
- Shared Azure substrate provisioning and shared bootstrap scripts.
- Direct control-plane Python module ownership or sibling-source imports for normal runtime operation.
- Treating `tasks.common.*` compatibility wrappers as long-term ownership points.

### Boundary rules

- `tasks/` may depend on `core/`.
- `core/` must not import `tasks.*`.
- Shared interfaces should be sourced from `core/*` or published shared packages, not from ad hoc task-local wrappers.
- Jobs consume control-plane-owned control state over HTTP and authenticated clients, not by importing control-plane source.

### Evidence

- `README.md`
- `DEPLOYMENT_SETUP.md`
- `docs/architecture/adr-001-runtime-surfaces.md`
- `tests/test_workflow_runtime_ownership.py`
- `tests/test_azure_provisioning_scripts.py`
- `tests/test_multirepo_dependency_contract.py`
- `tests/architecture/test_python_module_boundaries.py`

### Unverified / Needs confirmation

- Whether every file currently under `monitoring/` is still actively exercised by present-day jobs versus retained for compatibility or future refactor targets.

## 3. Runtime / Component Map

### Domain job surfaces

- `tasks/market_data/`: bronze, silver, and gold market-data jobs.
- `tasks/finance_data/`: bronze, silver, and gold finance-data jobs, with decomposed `bronze_modules/`, `silver_modules/`, and `gold_modules/`.
- `tasks/earnings_data/`: bronze, silver, and gold earnings-data jobs.
- `tasks/price_target_data/`: bronze, silver, and gold price-target jobs.
- `tasks/regime_data/`: gold regime recomputation.
- `tasks/ranking/`: platinum ranking materialization.
- `tasks/backtesting/worker.py`: queued or explicitly targeted backtest execution.
- `tasks/monitoring/check_readiness.py`: readiness validation for runtime dependencies.

### Shared runtime modules

- `core/bronze_bucketing.py`, `core/layer_bucketing.py`, `core/run_manifests.py`: bucket layout, common indexes, and manifest publication.
- `core/blob_storage.py`: Azure Blob Storage client behavior and connection model.
- `core/logging_config.py`, `core/config.py`, `core/runtime_config.py`: runtime logging and configuration helpers.
- `core/control_plane_transport.py`: runtime-common transport re-export for control-plane HTTP access.
- `core/*repository*.py`: repository surfaces, many of which are runtime-common re-exports rather than locally owned implementations.
- `core/ranking_engine/*` and `core/strategy_engine/*`: jobs-side domain engines for ranking and strategy/backtest behavior.

### Provider and operations support

- Provider adapters live under `alpaca/`, `alpha_vantage/`, `massive_provider/`, and related gateway clients in `core/`.
- `monitoring/` contains health, ARM, Log Analytics, resource-health, and control-plane startup support used by runtime operations.

### Trigger modes

- Scheduled Azure Container Apps Jobs via `deploy/job_*.yaml`.
- Manual production triggering only through `python scripts/ops/trigger_job.py --job <job-key> --resource-group <resource-group>`.
- Local execution through `python -m tasks...` entrypoints.

### Evidence

- `tasks/`
- `core/`
- `monitoring/`
- `deploy/job_*.yaml`
- `scripts/ops/trigger_job.py`

### Unverified / Needs confirmation

- A full end-to-end lineage map for every individual domain output has not been exhaustively re-derived from every task module in this document.

## 4. Job Execution Contract

### Entrypoint contract

- Deployable jobs execute as `python -m tasks...` modules from `deploy/job_*.yaml`.
- The shared image built by this repo is expected to contain all deployable task modules.
- Local development and CI smoke behavior rely on the same module-entrypoint pattern.

### Runtime wrapper contract

- Deployable job entrypoints should wrap their main function with `tasks.common.job_entrypoint.run_logged_job`.
- The wrapper is responsible for:
  - logging startup context and timing,
  - normalizing exit codes,
  - executing success callbacks only after a zero exit code,
  - emitting structured failure information and re-raising unexpected exceptions.

### Logging and failure semantics

- Runtime context logging is part of the job contract.
- Deployed jobs are configured for machine-readable logging through manifest env values such as `LOG_FORMAT=JSON`.
- Jobs should fail honestly: success callbacks are skipped on non-zero exits, and exceptions are surfaced rather than silently masked.

### Chaining and startup behavior

- Some jobs declare downstream sequencing through env-driven names such as `TRIGGER_NEXT_JOB_NAME`.
- Job-side startup of dependent container apps is handled through controlled helpers such as `tasks/common/job_trigger.py` and must be treated as RBAC-governed behavior rather than ambient capability.
- Local runtime behavior differs from Azure runtime behavior: startup helpers can normalize local API addresses and runtime markers when ACA/Kubernetes environment variables are absent.

### Evidence

- `deploy/job_*.yaml`
- `Dockerfile`
- `tasks/common/job_entrypoint.py`
- `tasks/common/job_trigger.py`
- `tasks/backtesting/worker.py`
- `tasks/ranking/platinum_rankings.py`
- `.github/workflows/quality.yml`

### Unverified / Needs confirmation

- No reviewed test currently proves every `deploy/job_*.yaml` command maps to a valid task module and matching env surface.

## 5. Data / Storage Architecture

### Ownership planes

#### Blob / Delta data plane

- This repo owns medallion-style job outputs across bronze, silver, and gold storage layers.
- Container and folder names are deployment inputs, not hard-coded ownership choices:
  - `AZURE_CONTAINER_COMMON`
  - `AZURE_CONTAINER_BRONZE`
  - `AZURE_CONTAINER_SILVER`
  - `AZURE_CONTAINER_GOLD`
  - domain folders such as market, finance, earnings, and targets
- `deltalake` and `azure-storage-blob` are part of the runtime dependency set for this data plane.

#### Common metadata plane

- `common` storage is for manifests, symbol indexes, coverage markers, shared artifacts, metadata snapshots, and runtime coordination artifacts.
- Business datasets belong in bronze, silver, and gold layers rather than in `common`.

#### Postgres operational state

- `POSTGRES_DSN` is a required secret for workflows that need relational state.
- Verified uses include backtest run claim/failure tracking and ranking materialization inputs.
- Postgres should be described by owned use cases, not as a generic shared dump.

### Medallion lifecycle

- Bronze jobs ingest provider payloads and preserve source fidelity plus coverage and invalid-symbol metadata.
- Silver jobs normalize, parse, index, and reconcile domain data.
- Gold jobs materialize domain-ready outputs and sync or publication artifacts.
- Platinum rankings sit above gold data and depend on strategy and ranking metadata rather than raw provider payloads.

### Cross-repo control state

- Control-plane-owned control state is external to this repo.
- Jobs read that state over HTTP using runtime-common transport and repository clients.
- Future changes should assume operator-owned state stays outside this repo unless local code proves otherwise.

### Source-of-truth rule

- `core/*` is the long-term source of truth for shared job/runtime contracts.
- `tasks.common.*` wrappers are compatibility shims and should be treated as transitional.
- `docs/ops/env-contract.csv` is the canonical configuration matrix for repo variables and secrets.

### Evidence

- `docs/ops/env-contract.csv`
- `docs/ops/env-contract.md`
- `core/bronze_bucketing.py`
- `core/layer_bucketing.py`
- `core/run_manifests.py`
- `core/blob_storage.py`
- `tasks/common/run_manifests.py`
- `tasks/finance_data/`
- `tasks/backtesting/worker.py`
- `tasks/ranking/platinum_rankings.py`
- `DEPLOYMENT_SETUP.md`

### Unverified / Needs confirmation

- Exact Postgres schemas, tables, and ownership split across jobs-owned versus externally owned data are not fully documented here.
- Exact Delta partitioning, retention, and purge coverage should be treated as narrower subtopics until verified from implementation and SQL artifacts.

## 6. Cross-Repo Interfaces And Trust Boundaries

### Published package boundaries

- Shared schemas and contracts are consumed from `asset-allocation-contracts`.
- Shared transport and repository helpers are consumed from `asset-allocation-runtime-common`.
- This repo pins those dependencies in `pyproject.toml` and builds images from published packages rather than by copying sibling repositories into the runtime image.

### Backtesting v2 remediation routing

- Backtesting runtime v2 is contracts-repo-first. Any result-shape or serialized metadata change for backtesting must be introduced in `asset-allocation-contracts` before this repo adopts it.
- The planned v2 contract is additive and backward-compatible. This repo should preserve v1 reads and historical compatibility while the sibling repos publish and adopt the new fields.
- The jobs-side implementation should treat `period_return`, `window_periods`, response `metadata`, and readiness semantics as published contract inputs, not as locally invented shapes.

### Control-plane boundary

- Jobs require `ASSET_ALLOCATION_API_BASE_URL` and `ASSET_ALLOCATION_API_SCOPE`.
- Jobs call the control plane over authenticated HTTP via runtime-common transport and clients.
- Prod jobs target an internal control-plane service URL. The current restore target is `http://asset-allocation-api`; `http://asset-allocation-api-vnet` is the durable target after the VNet-backed app is deployed and reachable. Public ACA ingress FQDNs are out of contract for jobs runtime configuration.
- Jobs must not import control-plane Python modules directly for normal runtime behavior.
- Backtesting worker preflight depends on a dedicated authenticated readiness endpoint in the control plane before claim/start flow is allowed to proceed.
- Universe selection payloads now use stable public field ids at the contract edge. Jobs resolves those ids to warehouse columns locally inside `core/strategy_engine/universe.py` and `core/ranking_engine/service.py`; the warehouse mapping is not part of the external contract.

### Identity and secret boundaries

- Azure Container Apps Jobs use a user-assigned identity for runtime access, including ACR pull and control-plane token acquisition inputs.
- Secrets and deploy variables are distinct parts of the runtime contract.
- Sensitive values explicitly documented in the env contract include:
  - `ASSET_ALLOCATION_API_BASE_URL`
  - `ASSET_ALLOCATION_API_SCOPE`
  - `AZURE_STORAGE_CONNECTION_STRING`
  - `NASDAQ_API_KEY`
  - `POSTGRES_DSN`
- The repo-local bootstrap seeds the control-plane bootstrap pair without public ingress discovery: `ASSET_ALLOCATION_API_BASE_URL` defaults to the internal same-environment service URL and `ASSET_ALLOCATION_API_SCOPE` is auto-discovered from Azure when possible. Other secrets are reused from `.env.web` or securely prompted.

### Workflow- and runbook-gated mutating operations

- `deploy-prod.yml` is the only approved workflow that applies `deploy/job_*.yaml`.
- `scripts/ops/trigger_job.py` is the approved manual job-start path.
- Cross-job starts and control-plane wake-ups are RBAC-sensitive operations and must remain env-driven and runbook-controlled.
- Build-time private package configuration uses a BuildKit secret mount and must not be baked into source, image layers, or plain Docker build args.

### Logging boundary

- Logs may contain operational metadata such as job name, execution name, host, and normalized API base URL.
- Logs must not emit bearer tokens, DSNs, API keys, or raw secret values.

### Evidence

- `pyproject.toml`
- `Dockerfile`
- `README.md`
- `DEPLOYMENT_SETUP.md`
- `docs/ops/env-contract.csv`
- `docs/ops/env-contract.md`
- `core/control_plane_transport.py`
- `tasks/common/job_entrypoint.py`
- `tasks/common/job_trigger.py`
- `deploy/job_*.yaml`
- `.github/workflows/quality.yml`
- `.github/workflows/release.yml`
- `.github/workflows/deploy-prod.yml`
- `scripts/ops/trigger_job.py`

### Unverified / Needs confirmation

- Exact Azure RBAC grants for the runtime identity are not fully defined in the inspected files.
- Detailed token acquisition internals live in shared packages and are not re-specified here.

## 7. Deployment And Operations Contract

### Image and runtime model

- This repo builds one jobs image from `Dockerfile`.
- `release.yml` produces and publishes that image plus a release manifest describing version alignment.
- `deploy-prod.yml` deploys the latest successful release manifest for the selected branch, or an explicit digest supplied by `deploy_runtime` repository dispatch.

### Allowed workflows

- `quality.yml`: required validation path for PRs and `main`, plus scheduled dependency audit and governance checks.
- `release.yml`: image build, push, and release-manifest publication after successful `quality.yml` runs on `main`.
- `deploy-prod.yml`: apply and verify `deploy/job_*.yaml`.
- `scripts/ops/trigger_job.py`: the approved manual production job-start path.

### Deployment ownership

- This repo owns jobs-side runtime assets and job manifests only.
- Shared Azure bootstrap remains in the sibling control-plane repo.
- One jobs repo can own many ACA Jobs; the intended model is many jobs sharing a single image with env-driven behavior.

### Rollback and readiness

- Rollback is image-based redeploy using previously captured image references rather than ad hoc resource mutation.
- Manual deploy dispatch resolves the latest successful release artifact; targeted rollback uses `deploy_runtime` repository dispatch with an explicit previous digest.
- Readiness and diagnosability depend on structured logs, explicit env/config surfaces, workflow gates, storage access, Postgres access where required, and successful control-plane tokenized HTTP access.
- `tasks/monitoring/check_readiness.py` is part of the runtime readiness surface, but it is not the only operational check that matters.

### Evidence

- `README.md`
- `DEPLOYMENT_SETUP.md`
- `Dockerfile`
- `.github/workflows/quality.yml`
- `.github/workflows/release.yml`
- `.github/workflows/deploy-prod.yml`
- `scripts/ops/trigger_job.py`
- `tasks/monitoring/check_readiness.py`

### Unverified / Needs confirmation

- Whether all repo-local readiness or monitoring helpers are covered by active operational runbooks beyond the inspected deployment documentation.

## 8. Executable Contract And Validation

### Enforced in the required CI path today

- Workflow lint and Python lint.
- Environment contract tests centered on `docs/ops/env-contract.csv`.
- Workflow ownership and repo-boundary tests.
- Cross-repo control-plane transport and repository tests.
- Docker image build smoke validation.

### Executable contract layers

#### Required CI gate

- `.github/workflows/quality.yml` is the canonical validation path for PRs and `main`.
- Backtesting runtime v2 changes add a dedicated runtime quality gate (`test-backtesting-runtime`) so simulation correctness checks do not hide inside the generic fast gate.

#### Environment and deploy contract

- `tests/test_env_contract.py` validates the env matrix, workflow references, and repo-local env sync behavior.
- `tests/test_workflow_runtime_ownership.py` and `tests/test_azure_provisioning_scripts.py` validate workflow ownership and the absence of shared Azure provisioners in this repo.
- `tests/test_multirepo_dependency_contract.py` validates published-package boundaries and limits on sibling-repo checkout behavior.

#### Cross-repo interface contract

- `tests/core/test_control_plane_transport.py`
- `tests/core/test_strategy_repository.py`
- `tests/core/test_ranking_repository.py`
- `tests/core/test_universe_repository.py`
- `tests/core/test_regime_repository.py`
- `tests/core/test_backtest_repository.py`

These assert that jobs-side control data access flows through runtime-common clients and HTTP contracts rather than direct control-plane source coupling.

#### Architecture guardrails

- `tests/architecture/test_python_module_boundaries.py`
- `tests/architecture/test_system_facade_guard.py`

These encode design intent around runtime-surface boundaries and compatibility facades. They are part of the living design contract even though the current required CI workflow does not invoke them.

### Interpretation rule

- If code, tests, workflows, or deploy manifests disagree with prose, the prose loses and must be updated.
- This document should distinguish between:
  - enforced current behavior,
  - intended architectural guardrails carried forward,
  - historical lineage retained for context only.

### Evidence

- `.github/workflows/quality.yml`
- `tests/test_env_contract.py`
- `tests/test_workflow_runtime_ownership.py`
- `tests/test_azure_provisioning_scripts.py`
- `tests/test_multirepo_dependency_contract.py`
- `tests/core/`
- `tests/architecture/`
- `docs/architecture/adr-001-runtime-surfaces.md`

### Unverified / Needs confirmation

- No reviewed gate currently proves this master design contract stays synchronized with the executable contract.
- The architecture test suite is not currently part of the required CI path described in `quality.yml`.

## 9. Historical Lineage And Known Legacy References

### Historical lineage

- `docs/architecture/original-monolith-and-five-repo-map.md` explains how this repo emerged from the original monolith and how responsibilities were split across sibling repos.
- `docs/architecture/adr-001-runtime-surfaces.md` and the runtime-surface ledger/manifests capture refactor history, extraction intent, and compatibility strategies.

These files remain useful context, but they are not the canonical current-state contract for this repo.

### Known legacy or non-canonical references

- `docker-compose.yml` references API and UI surfaces that do not exist in the current top-level repo tree and must not be treated as authoritative current-state runtime ownership.
- `deploy/app_api.yaml` and `deploy/app_api_public.yaml` describe container app surfaces that conflict with the repo’s stated jobs-only ownership model; until separately reconciled, treat them as legacy or historical artifacts rather than active deploy surfaces for this repo.
- Older architecture docs may describe transitional or pre-split boundaries. Use them for lineage, not for current ownership decisions.

### Evidence

- `docs/architecture/original-monolith-and-five-repo-map.md`
- `docs/architecture/adr-001-runtime-surfaces.md`
- `docs/architecture/runtime-surface-extraction-manifest.md`
- `docs/architecture/runtime-surface-refactor-ledger.md`
- `docker-compose.yml`
- `deploy/app_api.yaml`
- `deploy/app_api_public.yaml`
- `README.md`
- `DEPLOYMENT_SETUP.md`

### Unverified / Needs confirmation

- Whether `docker-compose.yml` and `deploy/app_api*.yaml` are intentionally retained for narrow local or historical workflows rather than simply stale.

## 10. Update Rules For Future Agents

### Update triggers

Update this document in the same PR whenever any of the following change materially:

- `tasks/` job entrypoints, sequencing, or medallion flow.
- `core/` ownership boundaries or cross-repo client behavior.
- `deploy/job_*.yaml` or deployment ownership assumptions.
- `.github/workflows/*` that change validation, release, or deploy behavior.
- `scripts/ops/trigger_job.py` when the approved manual job-start path changes.
- `docs/ops/env-contract.*` or the secrets/vars split.
- provider integration behavior that changes runtime or data-flow expectations.
- architecture or contract tests that change the executable design boundary.

### Authoring rules

- Keep this file current-state-first.
- Put supporting history in the lineage section rather than mixing it into current ownership statements.
- End major sections with evidence paths.
- Put anything not directly verified under `Unverified / Needs confirmation`.
- Do not invent exact Postgres schemas, Delta retention policies, partition layouts, or ownership details not proven by implementation or docs.
- Prefer stable evidence from code, tests, workflows, manifests, and docs over inference.

### Review rules

- If a claim here conflicts with code or tests, update this file rather than explaining the mismatch away.
- If a change introduces a new ownership boundary or runtime invariant, update both the prose and the executable guardrail if one exists.
- If a historical artifact becomes current again, move it out of the legacy section only after code and workflow evidence prove that status.

### Evidence

- `README.md`
- `DEPLOYMENT_SETUP.md`
- `docs/ops/env-contract.csv`
- `.github/workflows/`
- `tests/`
- `docs/architecture/`

### Unverified / Needs confirmation

- No automated guardrail currently enforces that this document changes alongside all material contract changes.
