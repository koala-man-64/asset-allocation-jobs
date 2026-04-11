# Strategy Configuration Audit

Historical note: this audit described the pre-split control-plane strategy authoring stack. The API and UI implementation it referenced is now owned by sibling repos.

## Current Ownership

- `asset-allocation-jobs` owns runtime-facing strategy execution code and jobs-side consumers such as `core/strategy_engine/*` plus repository and transport adapters that read control data.
- `asset-allocation-control-plane` owns strategy authoring routes, auth, request validation, and live API documentation.
- `asset-allocation-ui` owns strategy authoring screens and UI interaction tests.

## Jobs-Side Implications

- Strategy configs consumed in this repo must arrive through published contracts or control-plane HTTP interfaces.
- Shared strategy payload shape changes are contracts-repo-first.
- This repo must not add local API or UI implementation paths for strategy authoring.

## Status

- Preserve this file only as historical context for the migration.
- Re-home live implementation guidance to the owner repos.
