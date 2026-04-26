# Bronze Market Alpha26 Memory Model

- Bronze market now processes alpha26 buckets sequentially. Each bucket is loaded, reconciled, written, and released before the next bucket starts, so the job no longer retains the full market universe in memory.
- Scheduled runs default to Massive-only ingestion. `BRONZE_MARKET_ALPHA_VANTAGE_ENRICHMENT_ENABLED=true` explicitly opts into Alpha Vantage dividend/split enrichment, and enrichment failures are warnings rather than blockers for Massive raw publication.
- Normal runs rewrite only the scheduled universe. Existing rows for unscheduled symbols are dropped as part of the full-domain rewrite.
- Debug runs preserve unscheduled existing rows while only recomputing the requested debug symbols. This avoids destructive partial rewrites during targeted investigations.
- Bucket parquet files are written during processing, but active bucket artifacts, the symbol index, manifest, and final domain artifact are written only during finalization after every bucket succeeds.
- If any bucket preload, transform, or write fails, finalization is aborted. Partial run-prefix data may exist, but downstream consumers should trust only runs with completed active artifacts, manifest, and symbol index.
- Post-rollout tuning rule: inspect peak memory and total duration on the next successful production runs before changing `MASSIVE_MAX_WORKERS`.
