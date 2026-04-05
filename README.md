# Asset Allocation Jobs

Runtime-owned jobs repository for:
- `tasks/` batch jobs and backtesting worker runtime
- provider adapters in `alpha_vantage/`, `massive_provider/`, and `alpaca/`
- jobs-side `core/` runtime modules

Local development assumes the shared contracts package is available:

```powershell
python -m pip install -e ../asset-allocation-contracts/python
python -m pytest tests/tasks -q
```

Cross-repo control data is read from the control-plane over HTTP. Configure:

- `ASSET_ALLOCATION_API_BASE_URL`
- `ASSET_ALLOCATION_API_SCOPE`

## Operations

Canonical workflows live under `.github/workflows/`.

- `ci.yml` is the required validation path for PRs and `main`.
- `security.yml` runs dependency audits and dependency-governance checks.
- `release.yml` builds the jobs image and writes `release-manifest.json`.
- `deploy-prod.yml` is the only workflow allowed to apply `deploy/job_*.yaml`.
- `control-plane-compat.yml` validates jobs against a candidate or released control-plane ref.
- `trigger-jobs.yml` is the only manual ACA job trigger workflow.
- `scripts/setup-env.ps1` builds repo-local `.env.web` using Azure and git discovery where possible.
- `scripts/sync-all-to-github.ps1` syncs the `.env.web` surface into repo vars and secrets.
- `DEPLOYMENT_SETUP.md` is the canonical deploy, operate, and rollback runbook.
