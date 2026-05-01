# Networking Audit - 2026-04-18

Status: Historical current-state live Azure networking review for `asset-allocation-jobs` as observed on April 18, 2026. Repo-local prod bootstrap now defaults to the internal same-environment control-plane URL `http://asset-allocation-api`; the Azure observations below remain pre-cutover evidence.
Scope: Subscription `eabd0bb1-8f36-4f27-ad86-8b33e02aaeb9`, resource group `AssetAllocationRG`.  
Routing decision: This is local-only and does not require contracts repo routing.

## Recommendation

Treat the current estate as public-by-default and flatter than it should be for a jobs-plus-control-plane deployment. Shared-network hardening should be routed through the sibling `asset-allocation-control-plane` repo because this repo does not own the shared Azure substrate. This repo should only adopt the resulting endpoint, identity, and configuration changes after that substrate work exists.

## Objective

Produce a networking review that separates facts, inferences, and recommendations for the deployed jobs runtime and the shared Azure resources it depends on.

## Assumptions

- The authoritative live scope is subscription `eabd0bb1-8f36-4f27-ad86-8b33e02aaeb9`, resource group `AssetAllocationRG`, observed on April 18, 2026.
- No hidden network resources outside `AssetAllocationRG` are fronting these services today. If later evidence shows a shared hub, reverse proxy, or private DNS path outside this resource group, update this audit to separate shared-network ownership from repo-owned runtime behavior.
- The required outcome is an audit and a prioritized remediation backlog, not immediate infrastructure mutation from this repo.

## Facts

### Repo-backed facts

- `deploy/job_*.yaml` injects `ASSET_ALLOCATION_API_BASE_URL` and `ASSET_ALLOCATION_API_SCOPE` into jobs that call the control plane over HTTP.
- `scripts/setup-env.ps1` now defaults `ASSET_ALLOCATION_API_BASE_URL` to `http://asset-allocation-api` and `JOB_STARTUP_API_CONTAINER_APPS` to `asset-allocation-api` instead of deriving a public ingress FQDN.
- `tasks/common/job_trigger.py` uses `SYSTEM_HEALTH_ARM_*` plus `https://management.azure.com/.default` to start container apps and downstream ACA jobs through Azure ARM.
- Job manifests use secret-based `AZURE_STORAGE_CONNECTION_STRING` and `POSTGRES_DSN` rather than an identity-only runtime path.
- `DEPLOYMENT_SETUP.md` is explicit that shared Azure bootstrap belongs to the sibling `asset-allocation-control-plane` repo, not to this repository.

### Live Azure facts observed on April 18, 2026

#### Shared ACA environment

- `asset-allocation-env` is in `East US`.
- `publicNetworkAccess` is `Enabled`.
- `vnetConfiguration` is `null`.
- `peerAuthentication.mtls.enabled` is `false`.
- `peerTrafficConfiguration.encryption.enabled` is `false`.
- The environment has a public default domain of `bluesea-887e7a19.eastus.azurecontainerapps.io`.

#### Public control-plane and UI ingress

- `asset-allocation-api` uses ACA ingress with `external=true`.
- `asset-allocation-ui` uses ACA ingress with `external=true`.
- Both apps run in single revision mode with `minReplicas=1`, `maxReplicas=1`, and no explicit scale rules.
- The repo-local wake-source audit for `asset-allocation-api` observed `Requests sum=12901`, `peak5m=54`, and `nonZeroBins=277` over the last 24 hours.

#### Data plane

- Storage account `assetallocstorage001` is in `eastus`.
- Storage `networkRuleSet.defaultAction` is `Allow`.
- Storage has no virtual network rules and no private endpoints.
- ACR `assetallocationacr` is in `eastus`.
- ACR `publicNetworkAccess` is `Enabled`.
- ACR has no private endpoints.
- PostgreSQL flexible server `pg-asset-allocation` is in `East US 2`.
- PostgreSQL `publicNetworkAccess` is `Enabled`.
- PostgreSQL `delegatedSubnetResourceId` is `null`.
- PostgreSQL `privateDnsZoneArmResourceId` is `null`.
- PostgreSQL has no private endpoints.
- PostgreSQL firewall rules include `allow-azure-services` (`0.0.0.0`) plus named public IP exceptions.

#### Identity and control plane

- The user-assigned identity `asset-allocation-acr-pull-mi` is attached to the API, UI, and ACA jobs.
- That shared principal currently has `Contributor` on the full `/subscriptions/eabd0bb1-8f36-4f27-ad86-8b33e02aaeb9/resourceGroups/AssetAllocationRG` scope.
- The same shared identity is used both for ACR image pulls and for runtime/auth flows that can reach ARM.

## Current Topology

```text
Internet
  |
  +--> asset-allocation-ui (ACA external ingress, East US)
  |
  +--> asset-allocation-api (ACA external ingress, East US)
          |
          +--> Azure ARM management plane
          +--> Blob/DFS public endpoints
          +--> PostgreSQL public FQDN (East US 2)

ACA Jobs (East US, same ACA environment)
  |
  +--> asset-allocation-api via ASSET_ALLOCATION_API_BASE_URL
  +--> Azure ARM management plane for app/job wake and status
  +--> Blob/DFS public endpoints
  +--> PostgreSQL public FQDN (East US 2)

ACR (public) <--- shared user-assigned identity --- API / UI / Jobs

No VNet
No private DNS
No private endpoints
No explicit NAT or firewall ownership
No ACA peer mTLS
No ACA peer traffic encryption
```

## Interfaces And Surfaces Under Review

No public API schema or shared-contract change is required for this audit.

Treat these deployment and runtime inputs as the networking interface surface:

- `ASSET_ALLOCATION_API_BASE_URL`
- `ASSET_ALLOCATION_API_SCOPE`
- `AZURE_STORAGE_CONNECTION_STRING`
- `POSTGRES_DSN`
- `JOB_STARTUP_API_CONTAINER_APPS`
- `SYSTEM_HEALTH_ARM_*`

## Traffic Classes

| Traffic class | Current path | Facts | Current risk |
| --- | --- | --- | --- |
| North-south ingress | Internet to `asset-allocation-api` and `asset-allocation-ui` over ACA external ingress | Both apps are `external=true` with public FQDNs and fixed `minReplicas=1` | Public edge exists with no private-only backend boundary in the current resource group |
| East-west control-plane calls | ACA jobs call the control plane via `ASSET_ALLOCATION_API_BASE_URL` | Repo bootstrap discovers the API base URL from the public ingress FQDN | Internal service-to-service traffic is configured against the public edge rather than an internal name or private endpoint |
| Data-plane storage and database access | API and jobs use Blob/DFS public endpoints plus PostgreSQL public FQDN | Storage is `defaultAction=Allow`; Postgres public access is enabled; both use secret-based credentials today | Sensitive data-plane paths are publicly addressable and rely on secrets instead of private connectivity or identity-first network posture |
| Management-plane ARM calls | API and jobs call `management.azure.com` to wake apps/jobs and collect status | Shared runtime identity has RG-wide `Contributor` and `tasks/common/job_trigger.py` can start apps/jobs through ARM | A compromise in one runtime surface inherits broad management-plane power over the whole resource group |

## Ranked Risks

### 1. Public ingress and public data-plane exposure

Facts:

- ACA environment public network access is enabled.
- API and UI ingress are external.
- Storage is allow-by-default and public.
- ACR is public.
- PostgreSQL public network access is enabled.

Why it matters:

- The current design exposes both the application edge and the data plane to public network paths.
- Even with auth at the application layer, this increases attack surface, misconfiguration risk, and incident blast radius.

### 2. Broad trust boundary from RG-wide `Contributor` on the shared runtime identity

Facts:

- `asset-allocation-acr-pull-mi` is shared by API, UI, and jobs.
- That identity has `Contributor` at the full resource-group scope.

Why it matters:

- A compromise in any attached runtime surface can mutate resources across the entire shared group.
- The current identity mixes image-pull, runtime, and control-plane responsibilities.

### 3. Secret-based storage and Postgres access instead of private and identity-first connectivity

Facts:

- Job manifests use `AZURE_STORAGE_CONNECTION_STRING` and `POSTGRES_DSN`.
- API runtime also carries storage and Postgres secrets.

Why it matters:

- Secret sprawl is higher than necessary.
- Network posture and credential posture are coupled: public endpoints stay reachable because the runtime expects to use shared secrets.

### 4. Implicit and shared egress with no explicit NAT or firewall ownership

Facts:

- The ACA environment is not VNet-integrated.
- No explicit NAT gateway, Azure Firewall, or egress ownership is present in this resource group.
- ACA apps expose a large shared outbound IP set.

Why it matters:

- Egress allowlisting and partner connectivity become difficult to reason about.
- Troubleshooting outbound failures or proving deterministic egress is harder than it needs to be.

### 5. No private networking, no private DNS, no private endpoints, and disabled ACA east-west encryption

Facts:

- ACA environment has no VNet configuration.
- ACA peer mTLS and peer traffic encryption are disabled.
- Storage, ACR, and PostgreSQL have no private endpoints.

Why it matters:

- There is no clear private trust zone for east-west or data-plane traffic.
- Future hardening will require substrate changes, not just repo-local variable updates.

## Inferences

- At audit time, jobs were configured to target the public API edge because `scripts/setup-env.ps1` resolved `ASSET_ALLOCATION_API_BASE_URL` from the public ACA ingress FQDN. The jobs repo now defaults to `http://asset-allocation-api` for same-environment internal restore; the shared Azure substrate still needs the durable VNet-backed runtime cutover.
- Because storage and Postgres use public endpoints plus shared secrets, the current design optimizes for deployment simplicity over segmentation.
- Because the shared runtime identity can reach ARM and already has RG-wide `Contributor`, control-plane orchestration is effectively an ambient privilege for multiple runtime surfaces.
- Because PostgreSQL is in `East US 2` while ACA compute is in `East US`, the current design also carries a cross-region latency and failure-domain dependency for relational traffic.

## Recommended Target State

- Keep only a deliberate public edge. Prefer internal-only API and UI backends behind an explicit ingress layer when public access is required.
- Move storage, ACR, and PostgreSQL to private connectivity or at minimum deny-by-default network rules with explicit exceptions.
- Split identities:
  - pull-only identity for ACR
  - least-privilege runtime identity for jobs
  - least-privilege runtime identity for API and UI
  - separate monitoring or control-plane identity if ARM access remains necessary
- Remove RG-wide `Contributor` from shared runtime identities and replace it with narrowly scoped roles.
- Replace secret-first storage and Postgres access with identity-first access where the platform supports it, or document why a secret path is still required.
- Make DNS and egress ownership explicit. If fixed egress or allowlisting matters, define NAT or firewall ownership instead of inheriting ACA shared outbound behavior.

## Prioritized Remediation Backlog

### P0 - Ownership and trust-boundary correction

- Open a shared Azure substrate work item in `asset-allocation-control-plane` for ACA environment networking, private endpoint strategy, and DNS ownership.
- Split the shared `asset-allocation-acr-pull-mi` identity into separate responsibilities and remove RG-wide `Contributor`.
- Decide whether public UI and API ingress are both truly required. If not, reduce the public edge first.

### P1 - Private network substrate

- Introduce a VNet-backed shared environment design for ACA or another private backend pattern owned by the shared-infra repo.
- Add private endpoints and private DNS for:
  - Blob/DFS
  - ACR
  - PostgreSQL
- Replace storage `defaultAction=Allow` with deny-by-default rules once private access is proven.
- Remove PostgreSQL public access and `allow-azure-services` after private reachability is in place.

### P2 - Repo adoption work after substrate exists

- Completed in this repo: repo-local bootstrap no longer discovers a public ingress FQDN for `ASSET_ALLOCATION_API_BASE_URL`. Remaining work is the shared-Azure cutover to the approved private or internal endpoint.
- Remove `AZURE_STORAGE_CONNECTION_STRING` and `POSTGRES_DSN` from manifests that can use the new identity-backed connectivity model.
- Narrow or redesign `JOB_STARTUP_API_CONTAINER_APPS` and `SYSTEM_HEALTH_ARM_*` so runtime surfaces no longer need broad ARM mutation rights.

### P3 - Hardening validation and rollback

- Prove private DNS resolution from jobs and apps before cutover.
- Validate that jobs still:
  - call the control plane
  - read and write storage
  - reach PostgreSQL
  - wake downstream jobs when required
- Keep the old endpoint and rollback path available until private cutover is proven under real workload conditions.

## Validation Plan

Reproduce current-state evidence with:

```powershell
az containerapp env show -g AssetAllocationRG -n asset-allocation-env -o json
az containerapp show -g AssetAllocationRG -n asset-allocation-api -o json
az containerapp show -g AssetAllocationRG -n asset-allocation-ui -o json
az containerapp job show -g AssetAllocationRG -n bronze-market-job -o json
az containerapp job show -g AssetAllocationRG -n backtests-job -o json
az storage account show -g AssetAllocationRG -n assetallocstorage001 -o json
az acr show -g AssetAllocationRG -n assetallocationacr -o json
az postgres flexible-server show -g AssetAllocationRG -n pg-asset-allocation -o json
az postgres flexible-server firewall-rule list -g AssetAllocationRG -n pg-asset-allocation -o json
az role assignment list --assignee-object-id 3595a9bf-1cf2-465b-bf01-1b77b4a1d0c4 --scope /subscriptions/eabd0bb1-8f36-4f27-ad86-8b33e02aaeb9/resourceGroups/AssetAllocationRG -o json
powershell -ExecutionPolicy Bypass -File .\scripts\audit_containerapp_wake_sources.ps1 -ResourceGroup AssetAllocationRG -LookbackHours 24
```

For follow-on hardening work, verify:

- ingress exposure matches the intended public versus private design
- private endpoint and subnet fields are populated where expected
- DNS resolution follows the intended private path
- jobs can still reach the control-plane API, storage, and PostgreSQL
- ARM-based wake and status flows still work after RBAC narrowing
- rollback keeps the previous endpoint path available until cutover is proven

## Watch-Outs

- PostgreSQL is in `East US 2` while ACA compute is in `East US`. That is a real latency and failure-domain boundary even if the security posture is improved later.
- The wake-source audit already observed ongoing public request traffic on `asset-allocation-api`, so hardening work needs an explicit caller inventory before ingress is changed.
- This repo should not try to solve shared-network posture by growing repo-local deployment scripts. The shared Azure foundation remains out of scope here.

## Evidence

Repo evidence:

- `deploy/job_*.yaml`
- `scripts/setup-env.ps1`
- `tasks/common/job_trigger.py`
- `DEPLOYMENT_SETUP.md`
- `docs/ops/env-contract.csv`

Live Azure evidence collected on April 18, 2026:

- `az account show -o json`
- `az resource list -g AssetAllocationRG -o json`
- `az containerapp env show -g AssetAllocationRG -n asset-allocation-env -o json`
- `az containerapp show -g AssetAllocationRG -n asset-allocation-api -o json`
- `az containerapp show -g AssetAllocationRG -n asset-allocation-ui -o json`
- `az storage account show -g AssetAllocationRG -n assetallocstorage001 -o json`
- `az acr show -g AssetAllocationRG -n assetallocationacr -o json`
- `az postgres flexible-server show -g AssetAllocationRG -n pg-asset-allocation -o json`
- `az postgres flexible-server firewall-rule list -g AssetAllocationRG -n pg-asset-allocation -o json`
- `az role assignment list --assignee-object-id 3595a9bf-1cf2-465b-bf01-1b77b4a1d0c4 --scope /subscriptions/eabd0bb1-8f36-4f27-ad86-8b33e02aaeb9/resourceGroups/AssetAllocationRG -o json`
- `scripts/audit_containerapp_wake_sources.ps1`
