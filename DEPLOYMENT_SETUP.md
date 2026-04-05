# Deployment Setup

## Recommendation

Yes. This repo should have its own deployment to the shared Azure resource group.

Target shape:

- multiple Azure Container Apps Jobs
- same resource group: `AssetAllocationRG`
- same Container Apps environment: `asset-allocation-env`
- same ACR: `assetallocationacr`

This repo should not deploy the control-plane Container App or the UI.

## Current State

Jobs are already structurally closer to the target state than the other repos.

- the repo contains standalone job manifests under `deploy/job_*.yaml`
- `deploy/job_backtests.yaml` and the other job manifests target `Microsoft.App/jobs`
- the copied `.github/workflows/deploy.yml` still also deploys the API and UI, which is the wrong ownership boundary

## Deploy

Use only these workflow entry points:

1. `.github/workflows/ci.yml`
2. `.github/workflows/security.yml`
3. `.github/workflows/release.yml`
4. `.github/workflows/deploy-prod.yml`
5. `.github/workflows/control-plane-compat.yml`
6. `.github/workflows/trigger-jobs.yml`

`deploy-prod.yml` applies only `deploy/job_*.yaml`.

`trigger-jobs.yml` is the only manual job-start workflow.

## Operate

- Build exactly one jobs image digest with `release.yml`.
- Deploy that digest across all ACA Jobs with `deploy-prod.yml`.
- Run `control-plane-compat.yml` whenever the control-plane release dispatches `control_plane_released` or when validating an explicit control-plane ref manually.
- Use `trigger-jobs.yml` for ad hoc operator-driven starts after deployment.

## Shared Azure Foundation To Provision Once

Until infrastructure is moved into its own repo, use the bootstrap scripts from the original monorepo:

1. `powershell -ExecutionPolicy Bypass -File ..\\asset-allocation\\scripts\\provision_azure.ps1 -ProvisionPostgres`
2. `powershell -ExecutionPolicy Bypass -File ..\\asset-allocation\\scripts\\provision_entra_oidc.ps1`
3. `powershell -ExecutionPolicy Bypass -File ..\\asset-allocation\\scripts\\validate_azure_permissions.ps1`

Run this after the jobs exist if they need to start downstream jobs or wake apps:

4. `powershell -ExecutionPolicy Bypass -File ..\\asset-allocation\\scripts\\ensure_job_start_rbac.ps1 -ResourceGroup AssetAllocationRG -SubscriptionId <subscription-id>`

Those scripts currently provision or expect:

- resource group `AssetAllocationRG`
- storage account `assetallocstorage001`
- ACR `assetallocationacr`
- ACR pull identity `asset-allocation-acr-pull-mi`
- Log Analytics workspace `asset-allocation-law`
- Container Apps environment `asset-allocation-env`
- service account `asset-allocation-sa`
- Postgres Flexible Server `pg-asset-allocation`
- database `asset_allocation`

## Repo-Specific Inputs

GitHub secrets:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_STORAGE_CONNECTION_STRING`
- `ALPHA_VANTAGE_API_KEY`
- `NASDAQ_API_KEY`
- `POSTGRES_DSN`
- `ASSET_ALLOCATION_API_BASE_URL`
- `ASSET_ALLOCATION_API_SCOPE`

GitHub variables:

- `RESOURCE_GROUP=AssetAllocationRG`
- `ACR_NAME=assetallocationacr`
- `ACR_PULL_IDENTITY_NAME=asset-allocation-acr-pull-mi`
- `SERVICE_ACCOUNT_NAME=asset-allocation-sa`

## Deployment Steps

1. Publish the contracts repo first and pin the version consumed here.
2. Deploy the control-plane first. Jobs now read control data over HTTP and require a live API base URL.
3. Run the jobs test gates:
   - `python -m pytest tests/core/test_control_plane_transport.py tests/core/test_strategy_repository.py tests/core/test_ranking_repository.py tests/core/test_universe_repository.py tests/core/test_regime_repository.py tests/core/test_backtest_repository.py -q`
4. Build the jobs image from `Dockerfile`.
5. Deploy the job manifests you need from `deploy/job_*.yaml`.
6. Verify each job can:
   - pull from ACR
   - read storage
   - read Postgres where required
   - obtain a token for `ASSET_ALLOCATION_API_SCOPE`
   - call the control-plane at `ASSET_ALLOCATION_API_BASE_URL`

## Required Workflow Cleanup

Before calling this repo independently deployable, trim `.github/workflows/deploy.yml` so it only does jobs work:

1. keep image build and push for the jobs image
2. keep the `Microsoft.App/jobs` deployment steps
3. remove all Container App deployment steps for `asset-allocation-api`
4. remove all UI deployment steps
5. keep only the secrets and validations required by jobs

## Rollback

- Capture the pre-deploy image set from `artifacts/previous-job-images.json`.
- Roll back by rerunning `.github/workflows/deploy-prod.yml` with the previous known-good image digest.
- Re-trigger only the affected jobs after rollback, not the whole stack.

## Troubleshoot

- If `ci.yml` fails, verify the sibling contracts repo is available to the runner and that `ASSET_ALLOCATION_API_BASE_URL` and `ASSET_ALLOCATION_API_SCOPE` are set for the HTTP client tests.
- If `release.yml` fails to build the image, verify Docker is building from the shared workspace root and that the sibling contracts repo was checked out.
- If `deploy-prod.yml` fails during apply, inspect `artifacts/rendered/*` to confirm only `Microsoft.App/jobs` resources were rendered.
- If `deploy-prod.yml` verifies the wrong image, inspect `artifacts/previous-job-images.json` and the job image queries returned by Azure CLI.
- If `trigger-jobs.yml` fails, verify the selected job name exists in `AssetAllocationRG` and that the `prod` environment has valid Azure OIDC settings.

## Dependencies

- Sibling contracts repo for CI and release builds
- Control-plane API contract compatibility
- Azure OIDC credentials in GitHub variables
- `prod` GitHub environment for deploy and trigger workflows
- ACA Jobs in `AssetAllocationRG`

## Notes

- `core/control_plane_transport.py` now hard-requires `ASSET_ALLOCATION_API_BASE_URL` and `ASSET_ALLOCATION_API_SCOPE`.
- Jobs use bearer tokens to call the control-plane and should not import control-plane Python modules directly.
- One jobs repo can own many ACA Jobs. Do not create one Azure resource group per job.

## Evidence

- `.github/workflows/deploy.yml`
- `deploy/job_backtests.yaml`
- `deploy/job_bronze_market_data.yaml`
- `deploy/job_gold_regime_data.yaml`
- `core/control_plane_transport.py`
- `tests/core/test_control_plane_transport.py`
- `tests/core/test_backtest_repository.py`
- `..\\asset-allocation\\scripts\\provision_azure.ps1`
- `..\\asset-allocation\\scripts\\ensure_job_start_rbac.ps1`
- `..\\asset-allocation\\scripts\\validate_deploy_inputs.py`
