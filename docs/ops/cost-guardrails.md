# Cost Guardrails

This repo keeps the cost guardrail template local to the jobs runtime and deploys it with Azure CLI instead of a repo-local provisioner script.

That split is intentional:

- `deploy/cost_guardrails.bicep` is repo-owned because the filters and budget categories are specific to the Asset Allocation jobs stack.
- Shared Azure bootstrap scripts stay out of this repo, so there is no `scripts/configure_cost_guardrails.ps1` here.
- The operational entrypoint is a documented `az deployment sub` run against the local template plus the example parameters file.

The template currently covers:

- monthly subscription-scope budgets filtered to `AssetAllocationRG` and split by meter category
- a daily subscription-scoped cost anomaly alert
- lightweight tagging defaults for Container Apps Jobs via the deploy manifests

## What Gets Deployed

`deploy/cost_guardrails.bicep` creates:

- one `Microsoft.Consumption/budgets` resource per configured meter-category group
- one `Microsoft.CostManagement/scheduledActions` resource of kind `InsightAlert`

`deploy/cost_guardrails.parameters.example.json` is the operator starting point for the subscription deployment.

## Defaults

The example parameter file assumes these starting monthly budgets:

- `Container Apps`: `150`
- `Azure Monitor`: `30`
- `Storage`: `40`
- `Container Registry`: `20`
- `Azure Database for PostgreSQL`: `50`

These are starting guardrails, not usage-derived targets. Tune them after confirming the exact meter-category names and the first month of actual spend.

## Run

1. Copy `deploy/cost_guardrails.parameters.example.json` to a local deployment parameters file and replace the example email addresses and dates.

2. Preview the deployment first:

```powershell
az deployment sub what-if `
  --location eastus `
  --template-file deploy/cost_guardrails.bicep `
  --parameters @deploy/cost_guardrails.parameters.example.json
```

3. Apply it:

```powershell
az deployment sub create `
  --location eastus `
  --template-file deploy/cost_guardrails.bicep `
  --parameters @deploy/cost_guardrails.parameters.example.json
```

## Important Limits

- Azure Cost Management budgets can be filtered by meter category and resource group, so those alerts are split the way requested.
- Azure cost anomaly alerts are subscription-scoped. The repo deploys a single anomaly alert for the subscription because Azure does not expose meter-category-scoped anomaly alerts.
- Native Cost Management budgets and anomaly alerts do not add Azure monitoring charges by themselves. Separate Azure Monitor action groups or downstream automation can add cost.

## Verification

After deployment, verify:

```powershell
az consumption budget list --query "[].name"
az resource list --namespace Microsoft.CostManagement --resource-type scheduledActions --query "[].name"
```

In the Azure portal, confirm:

- budgets appear under Cost Management for the subscription
- each budget filters to `AssetAllocationRG` plus the intended meter category
- the anomaly alert points to the configured notification email
