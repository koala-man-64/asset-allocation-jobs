# Strategy Exploration Release Signoff

Historical note: this signoff captured a cross-repo release before jobs, control-plane, and UI ownership was fully split.

Date: 2026-03-15
Status: historical reference only

## Current Ownership

- `asset-allocation-jobs` owns the gold column lookup seed and sync artifacts that feed downstream readers, including `core/gold_column_lookup_catalog.py`, `core/metadata/gold_column_lookup_seed.json`, and `scripts/sync_gold_column_lookup.py`.
- `asset-allocation-control-plane` owns the lookup API routes, auth, and API-side tests.
- `asset-allocation-ui` owns the Strategy Exploration page, client wiring, and UI tests.

## Boundary Decision

- Jobs keep the data-plane lookup source material and sync automation.
- Control-plane owns lookup read APIs and API contract tests.
- UI owns page behavior, rendering, and package or lockfile integrity checks.

## Follow-Up

- Live release notes and implementation details should live in the owner repos.
- Keep this document only as historical lineage for the split-repo migration.
