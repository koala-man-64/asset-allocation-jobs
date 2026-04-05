# Env Contract

The checked-in source of truth is [env-contract.csv](env-contract.csv). It classifies every supported env key by:

- `class`: `secret`, `deploy_var`, `runtime_config`, `local_dev`, `constant`, or `deprecated`
- `github_storage`: `secret`, `var`, or `none`
- `source_of_truth`: where the value is supposed to come from
- `template`: whether the key belongs in [.env.template](../../.env.template)

## Ownership

- `secret`
  Source of truth: secret storage only.
  Examples: `POSTGRES_DSN`, `ALPHA_VANTAGE_API_KEY`, `MASSIVE_API_KEY`.
- `deploy_var`
  Source of truth: checked-in deploy config or a small GitHub Variables set for true environment identity/auth/public contract.
  Examples: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `ASSET_ALLOCATION_API_BASE_URL`, `SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID`.
- `runtime_config`
  Source of truth: code defaults plus Postgres `runtime_config`.
  These keys must not be synced to GitHub Variables.
- `local_dev`
  Source of truth: local `.env` files or local shell state only.
  These keys must not be expected in GitHub Actions.
- `constant`
  Source of truth: checked-in code or checked-in workflow/manifests.
  Examples: storage container names, header names, `/api`, `alpha26` layout names.
- `deprecated`
  Source of truth: none.
  Remove instead of reusing.

## Current policy

- GitHub Secrets are only for actual credentials and tokens.
- GitHub Variables are only for a narrow deploy contract.
- Runtime tuning belongs in code defaults and Postgres `runtime_config`, not GitHub Variables.
- Local UI and tooling env like `VITE_*` and `API_PORT` stay local.

## Adding or changing an env key

1. Add or update the row in [env-contract.csv](env-contract.csv).
2. If `template=true`, add or update the key in [.env.template](../../.env.template).
3. If `github_storage=var` or `github_storage=secret`, wire it in the workflow and keep the classification aligned.
4. If the key is a non-secret operational knob, prefer `runtime_config` before adding a new GitHub Variable.
5. If the value is invariant across environments, make it a checked-in constant instead of a new env key.

## Guardrails

- [sync-all-to-github.ps1](../../scripts/sync-all-to-github.ps1) reads the contract directly instead of classifying by regex.
- [test_env_contract.py](../../tests/test_env_contract.py) fails when:
  - the template and contract drift
  - workflows source vars/secrets outside the contract
  - runtime-config keys reappear in GitHub Variables
  - runtime code uses an undocumented env key
