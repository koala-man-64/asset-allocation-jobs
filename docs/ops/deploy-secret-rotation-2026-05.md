# Deploy Secret Rotation - May 2026

## Trigger

The prod jobs deploy workflow previously uploaded rendered ACA job manifests in the `jobs-deploy-support` artifact. Those rendered manifests contained deploy-time secret values. Treat retained `jobs-deploy-support` artifacts produced before this remediation as credential exposure candidates.

## Immediate Actions

1. Delete retained `jobs-deploy-support` artifacts from affected prod deploy runs where policy allows deletion.
2. Rotate these GitHub secrets and any backing provider credentials:
   - `ASSET_ALLOCATION_API_SCOPE`
   - `AZURE_STORAGE_CONNECTION_STRING`
   - `FRED_API_KEY`
   - `NASDAQ_API_KEY`
   - `MASSIVE_API_KEY`
   - `ALPHA_VANTAGE_API_KEY`
   - `ALPACA_KEY_ID`
   - `ALPACA_SECRET_KEY`
   - `POSTGRES_DSN`
3. Re-run the prod deploy after rotation.
4. Download the new `jobs-deploy-support` artifact and verify it contains only:
   - `artifacts/deploy-support/redacted-manifests/*`
   - `artifacts/deploy-support/deploy-provenance.json`
   - `artifacts/previous-job-images.json`

## Validation

Search the downloaded artifact for old and new credential sentinels. No manifest under `redacted-manifests` should contain secret values, connection strings, DSNs, API keys, bearer tokens, or API scopes. Secret-bearing manifests should remain only inside the ephemeral workflow workspace and must not be uploaded.

## Rollback Constraint

Rollback dispatches must include `image_digest`, `release_run_id`, and `release_git_sha` from a verified successful `jobs-release` artifact. Raw digest-only `deploy_runtime` dispatches are intentionally rejected.
