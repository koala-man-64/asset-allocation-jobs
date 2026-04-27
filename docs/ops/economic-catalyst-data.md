# Economic Catalyst Data

The `economic_catalyst_data` domain ingests official macro calendars, structured vendor releases, and multi-vendor headline metadata into Bronze, Silver, Gold, and Postgres serving surfaces.

## Scope

- Official schedule and release sources: FRED, BLS, BEA, Federal Reserve, Treasury, ECB, BOE, BOJ.
- Structured vendor overlays: Nasdaq Data Link table mappings configured through `ECONOMIC_CATALYST_NASDAQ_TABLES`.
- Headline overlays: Massive primary, Alpaca and Alpha Vantage secondary.
- Production defaults enable only the structured vendor overlay (`ECONOMIC_CATALYST_VENDOR_SOURCES=nasdaq_tables`); headline vendors are opt-in until their credentials, entitlements, and rate limits are verified.
- Postgres stores structured event surfaces plus headline metadata only. Raw payloads and full article text remain in Bronze storage.

## Storage Layout

- Bronze raw payloads: `${AZURE_CONTAINER_BRONZE}/${AZURE_FOLDER_ECONOMIC_CATALYST}/runs/<run-id>/raw/...`
- Silver canonical snapshots: `${AZURE_CONTAINER_SILVER}/${AZURE_FOLDER_ECONOMIC_CATALYST}/*.parquet`
- Silver source-state snapshots: `${AZURE_CONTAINER_SILVER}/${AZURE_FOLDER_ECONOMIC_CATALYST}/_state/*.parquet`
- Gold serving snapshots: `${AZURE_CONTAINER_GOLD}/${AZURE_FOLDER_ECONOMIC_CATALYST}/*.parquet`
- Domain metadata artifact: `${AZURE_FOLDER_ECONOMIC_CATALYST}/_metadata/domain.json`

## Cadence

- Bronze job runs every 30 minutes on weekdays with `replicaRetryLimit: 0`.
- The Bronze runtime treats minutes divisible by `ECONOMIC_CATALYST_GENERAL_POLL_MINUTES` as full-source polls.
- Intervening runs are hot-window polls that refresh structured vendor and headline feeds plus FRED release dates.
- Silver and Gold are manual-trigger jobs chained from the prior layer.

## Postgres Tables

- `core.economic_catalyst_source_state`
- `gold.economic_catalyst_events`
- `gold.economic_catalyst_event_versions`
- `gold.economic_catalyst_headlines`
- `gold.economic_catalyst_headline_versions`
- `gold.economic_catalyst_mentions`
- `gold.economic_catalyst_entity_daily`

Serving views:

- `gold.economic_catalyst_calendar_by_date`
- `gold.economic_catalyst_releases_by_date`
- `gold.economic_catalyst_headlines_by_date`
- `gold.economic_catalyst_entity_daily_by_date`

## Precedence

- Official publishers win for event existence, schedule timing, cancellations, and official release values.
- Structured vendors win for consensus, previous, and revised-previous fields when present.
- Massive wins for headline metadata when duplicate coverage exists. Alpaca and Alpha Vantage remain secondary sources.
- Headlines never create structured macro events on their own.

## Operations

- Configure source mix and cadence from `.env.web` using the `ECONOMIC_CATALYST_*` contract surface.
- Validate `ECONOMIC_CATALYST_NASDAQ_TABLES` against the entitled table schema before enabling `nasdaq_tables`.
- Source failure details in logs and manifests are sanitized. Optional source outages are recorded as warnings when at least one selected source succeeds; the Bronze run fails only when no sources are enabled, all selected sources fail, or all selected official sources fail.
- Failed Bronze runs do not update the `bronze_economic_catalyst_data` last-success marker, so downstream monitoring should use the latest manifest and job exit code instead of assuming a fresh success watermark.
- Expect quarantine rows when a source payload cannot be normalized or reconciled. Quarantined rows stay out of Gold and Postgres.
- Gold uses staged Postgres apply. The serving replica advances only after all target tables apply successfully.
