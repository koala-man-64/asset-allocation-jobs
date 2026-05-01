# Jobs Env Contract

This repo treats `.env.web` as the sync surface for template-backed GitHub variables and secrets.

Flow:

1. Review `docs/ops/env-contract.csv`.
2. Run `powershell -ExecutionPolicy Bypass -File scripts/setup-env.ps1`.
3. Inspect the preview or generated `.env.web`.
4. Run `powershell -ExecutionPolicy Bypass -File scripts/sync-all-to-github.ps1`.

Rules:

- `scripts/setup-env.ps1` only walks keys documented in `env-contract.csv` with `template=true`.
- Azure-backed identifiers are auto-discovered when `az` is installed and logged in.
- Repo slugs are derived from git where possible.
- The repo-local bootstrap seeds control-plane connectivity without depending on public ingress discovery: `ASSET_ALLOCATION_API_BASE_URL` defaults to the internal same-environment service URL `http://asset-allocation-api`, while `ASSET_ALLOCATION_API_SCOPE` is still resolved from Azure when possible. Move the base URL to `http://asset-allocation-api-vnet` only after that target is deployed and reachable from ACA Jobs.
- Other secrets are never fetched from Azure. Existing `.env.web` secrets are reused; otherwise the script prompts securely.
- Contract rows marked `template=false` are workflow-only entries. Keep them documented in `env-contract.csv`, but do not add them to `.env.template` or `.env.web`.
- Shared Azure provisioning lives in the sibling `asset-allocation-control-plane` repo, not here.
