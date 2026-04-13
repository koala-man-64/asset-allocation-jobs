# Asset Allocation Jobs

Runtime-owned jobs repository for:
- `tasks/` batch jobs and backtesting worker runtime
- provider adapters in `alpha_vantage/`, `massive_provider/`, and `alpaca/`
- jobs-side `core/` runtime modules

Local development installs versioned shared packages rather than sibling repos:

```powershell
python -m pip install asset-allocation-contracts==0.1.0
python -m pip install asset-allocation-runtime-common==0.1.0
python -m pytest -q
```

Cross-repo control data is read from the control-plane over HTTP. Configure:

- `ASSET_ALLOCATION_API_BASE_URL`
- `ASSET_ALLOCATION_API_SCOPE`

## Operations

Canonical workflows live under `.github/workflows/`.

- `quality.yml` is the required validation path for PRs and `main`, and it also runs scheduled dependency audits and governance checks.
- `compatibility.yml` validates jobs against candidate or released control-plane and runtime-common refs.
- `release.yml` builds the jobs image and writes `release-manifest.json`.
- `deploy-prod.yml` is the only workflow allowed to apply `deploy/job_*.yaml`.
- `trigger-jobs.yml` is the only manual ACA job trigger workflow.
- `contracts-adoption.yml` pins released contracts versions into this repo's dependency manifests.
- `scripts/setup-env.ps1` builds repo-local `.env.web` using Azure and git discovery where possible.
- `scripts/sync-all-to-github.ps1` syncs the `.env.web` surface into repo vars and secrets.
- `DEPLOYMENT_SETUP.md` is the canonical deploy, operate, and rollback runbook.
