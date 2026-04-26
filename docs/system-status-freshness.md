# System Status Freshness Contract

This document describes the jobs-side contribution to the system status experience. The endpoint and page implementation live outside this repo.

## Ownership

- `asset-allocation-control-plane` owns `/api/system/status-view`, `/api/system/health`, auth, and payload composition.
- `asset-allocation-ui` owns the page, query cadence, and client-side rendering.
- `asset-allocation-jobs` owns the job/resource data, artifacts, and logs that the control-plane reads when composing freshness and recent-job state.

## Jobs-Side Expectations

- Metadata-changing publishers in this repo must write through the shared artifact publish path so downstream snapshot documents stay current.
- Job executions and resource health signals emitted by jobs must remain consistent with the control-plane freshness overlay logic.
- Bronze finance remains a monitoring/runbook concern in this remediation: use the existing zero-write probe for `bronze-finance-job` to investigate processed-but-unwritten runs, and do not treat this work item as a finance runtime behavior change.
- Cross-repo references in this document are integration boundaries, not local implementation paths.

## Historical Note

Earlier versions of this document described the page and endpoint as if they lived in this repo. That is no longer true after the repo split.
