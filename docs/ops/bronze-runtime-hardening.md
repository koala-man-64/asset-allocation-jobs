# Bronze Runtime Hardening

This runbook covers the Bronze deployment controls added after the April 2026 live job drift audit.

## Deployment Contract

- Rendered job manifests remain the source of truth for ACA Jobs.
- `scripts/workflows/render_and_apply_job_manifests.py` rejects prod `ASSET_ALLOCATION_API_BASE_URL` values that point at public `*.azurecontainerapps.io` hosts unless `ALLOW_PUBLIC_ASSET_ALLOCATION_API_BASE_URL=true` is set for an approved emergency rollback.
- `scripts/workflows/verify_deployed_job_runtime.py` compares every rendered job manifest to the live ACA Job after deploy. It checks trigger type, cron, retry limit, timeout, image, env var presence and values, and secretRef names without printing env values.
- Safe Bronze defaults are:
  - `BRONZE_MARKET_ALPHA_VANTAGE_ENRICHMENT_ENABLED=false`
  - `ECONOMIC_CATALYST_VENDOR_SOURCES=nasdaq_tables`
  - `ECONOMIC_CATALYST_GENERAL_POLL_MINUTES=30`
  - `QUIVER_DATA_ENABLED=false`

Manual drift audit:

```powershell
python scripts/workflows/verify_deployed_job_runtime.py `
  --rendered-dir artifacts/rendered `
  --resource-group AssetAllocationRG `
  --expected-image <image-digest>
```

## Log Analytics Alert Queries

Use these as scheduled query rules or entries in `SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON`. Adjust table and column names only if the workspace schema differs.

Bronze failures in the last hour:

```kusto
ContainerAppConsoleLogs_CL
| where TimeGenerated > ago(1h)
| where Log_s has "Job completed:" and Log_s has "job=bronze-" and Log_s has "exit_code=1"
| summarize failures=count()
```

Bronze jobs without a recent success:

```kusto
let lookback = 26h;
ContainerAppConsoleLogs_CL
| where TimeGenerated > ago(lookback)
| where Log_s has "Job completed:" and Log_s has "job=bronze-" and Log_s has "exit_code=0"
| parse Log_s with * "job=" job_name " " *
| summarize last_success=max(TimeGenerated) by job_name
| where last_success < ago(lookback)
```

Public control-plane URL observed from job logs:

```kusto
ContainerAppConsoleLogs_CL
| where TimeGenerated > ago(24h)
| where Log_s has "ASSET_ALLOCATION_API_BASE_URL" or Log_s has "api_base_url="
| where Log_s has ".azurecontainerapps.io"
| summarize hits=count()
```

Secret-shaped text observed in job logs:

```kusto
ContainerAppConsoleLogs_CL
| where TimeGenerated > ago(24h)
| where Log_s matches regex @"(?i)(api[_\s-]?key|apikey|token|secret|password|authorization)\s*(=|:|\sas\s)\s*[A-Za-z0-9][A-Za-z0-9._~+/=-]{5,}"
| summarize hits=count()
```

## Canary Order

1. Run the deploy workflow and require runtime verification to pass.
2. Manually start `bronze-quiver-data-job` with `QUIVER_DATA_ENABLED=false`; expect exit `0`, no health marker, and no Silver trigger.
3. Manually start `bronze-market-job`; expect Massive-only logs and a Silver trigger after success.
4. Manually start `bronze-economic-catalyst-job`; expect a weekday 30-minute cadence and optional vendor failures recorded as warnings if required official source coverage remains available.
5. Manually start `bronze-earnings-job` only after Alpha Vantage listing-status returns non-empty symbols through the internal control plane.
6. Include `bronze-finance-job` and `bronze-price-target-job` in the same live status check to confirm no unrelated regression.

Rollback uses the existing `previous-job-images.json` artifact for image rollback. For config rollback, re-apply the previous rendered manifests and rerun runtime verification.
