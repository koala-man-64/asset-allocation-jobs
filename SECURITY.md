# Security Policy

## Reporting a Vulnerability

If this repository is hosted on GitHub, use the repository Security tab to report a vulnerability privately.

If GitHub Security Advisories are not available for this repo, report the issue to the repository owner or maintainer through your internal security process. Do not open a public issue with exploit details.

## Authentication and Authorization

- Jobs call the control-plane over HTTP using `ASSET_ALLOCATION_API_BASE_URL` and `ASSET_ALLOCATION_API_SCOPE`. Prod jobs are expected to use the internal service URL `http://asset-allocation-api-vnet`, not a public ingress endpoint.
- `core/api_gateway_auth.py` acquires bearer tokens with Azure credentials and should fail closed when required auth inputs are missing.
- GitHub Actions release and deploy workflows authenticate to Azure with OIDC variables `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_SUBSCRIPTION_ID`.

## Secrets and Identities

- Do not commit secrets. `.gitignore` excludes `.env` and `.env.*`, while `.env.template` documents only template-backed local inputs.
- `docs/ops/env-contract.csv` is the source of truth for GitHub variable and secret names, including workflow-only secrets that stay out of `.env.template`.
- Azure deployment uses a user-assigned managed identity for registry pulls and platform access.

## Runtime Hardening

- This repository does not own a browser surface or a local API service. Security-sensitive runtime code lives in jobs, transport, provider, and deployment paths only.
- `core/control_plane_transport.py` validates required control-plane configuration before outbound calls.
- `deploy/job_*.yaml` is the only deployable manifest surface owned by this repo.

## Dependency Hygiene

- Runtime dependencies are pinned in `pyproject.toml`, `requirements.txt`, and `requirements.lock.txt`.
- `quality.yml` consumes the lockfiles and the dependency governance script.
- Run `python3 scripts/dependency_governance.py check --report artifacts/dependency_governance_report.json` before merging dependency changes.

## Evidence

- `.gitignore`
- `.env.template`
- `docs/ops/env-contract.csv`
- `core/api_gateway_auth.py`
- `core/control_plane_transport.py`
- `deploy/job_backtests.yaml`
- `.github/workflows/quality.yml`
- `.github/workflows/deploy-prod.yml`
- `scripts/dependency_governance.py`
- `tests/test_env_contract.py`
- `tests/core/test_control_plane_transport.py`
