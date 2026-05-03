# Ranking Materialization

Ranking materialization turns a saved strategy configuration plus a saved ranking schema into a strategy-specific platinum table in Postgres. Strategies and ranking schemas each resolve their own saved universe configuration, and the materializer ranks only the symbol/date rows that satisfy both universes.

## Control Plane Objects

- `core.strategies`
  - stores `config.universeConfigName`
  - stores `config.rankingSchemaName`
  - stores `output_table_name`
- `core.universe_configs`
  - stores reusable universe definitions referenced by strategies and ranking schemas
- `core.universe_config_revisions`
  - stores immutable saved universe revisions
- `core.ranking_schemas`
  - stores `config.universeConfigName`
  - stores the active schema definition
- `core.ranking_schema_revisions`
  - stores immutable saved revisions
- `core.ranking_runs`
  - stores run status, row counts, and errors
- `core.ranking_watermarks`
  - stores the latest ranked date per strategy

## Platinum Output

For each ranking-enabled strategy, the materializer writes to:

- `platinum.<strategy_output_table>`

Schema:

```sql
date DATE NOT NULL,
symbol TEXT NOT NULL,
rank INTEGER NOT NULL,
score DOUBLE PRECISION NOT NULL,
last_updated_date DATE NOT NULL
```

Primary key:

- `(date, symbol)`

Indexes:

- `(symbol, date DESC)`
- `(date DESC, rank)`

## API Flow

- Save ranking schema: `POST /api/rankings/`
- Save universe config: `POST /api/universes/`
- Attach schema to strategy: `POST /api/strategies/`
- Preview draft rankings for a strategy/date: `POST /api/rankings/preview`
- Materialize platinum rankings: `POST /api/rankings/materialize`

## Job Flow

Worker entrypoint:

- `python -m tasks.ranking.platinum_rankings`

ACA workflow metadata:

- `jobCategory=strategy-compute`
- `jobKey=rankings`
- `jobRole=materialize`
- `triggerOwner=control-plane`

This job writes platinum outputs, but it is not classified as a platinum medallion pipeline stage. See `docs/ops/strategy-compute-jobs.md`.

Relevant environment variables:

- `POSTGRES_DSN`
- `ASSET_ALLOCATION_API_BASE_URL`
- `ASSET_ALLOCATION_API_SCOPE`
- `RANKING_MATERIALIZATION_CHUNK_DATES` (optional runtime-common override; default `31`, valid range `1..366`)

Behavior:

- Production materialization logic lives in `asset-allocation-runtime-common`; this repo owns the ACA worker, package pin, Docker image, and control-plane claim loop.
- The worker claims one pending ranking refresh at a time, materializes the claimed strategy/date window, calls `complete` on success or `fail` on error, then continues until no work remains.
- If any claimed item fails, the worker exits non-zero after processing the rest of the claim stream.
- The system-health marker is written only after a clean invocation with at least one completed item. Mixed success/failure runs do not refresh freshness monitoring.
- If `start_date` and `end_date` are omitted, runtime-common defaults incrementally:
  - `start_date = ranking_watermark + 1 day` when a watermark exists
  - otherwise `start_date = earliest source-active date`
  - `end_date = latest contiguous complete source-ready date`
- If the ranking watermark is already current, the worker records a `noop` run and does not rewrite platinum rows or advance the watermark.
- Runtime-common requires every selected gold table to be ready for a date before materializing it. Non-null readiness is required for `missingValuePolicy="exclude"` ranking factors and universe operators that cannot match nulls; `missingValuePolicy="zero"` factors and `is_null` universe checks do not block readiness.
- Explicit claim windows fail fast when they contain source-active dates that are not fully ready. The worker does not silently clamp an incomplete claim and report success.
- If the referenced gold tables have no source dates, the worker fails explicitly and does not advance the watermark.
- Backfills are processed in bounded ready-date chunks so the worker does not load the full requested window into pandas at once.
- Each strategy run writes platinum rows, marks the run `success`, and updates the watermark in a single database transaction.
- Watermark updates are monotonic; historical backfills cannot lower `core.ranking_watermarks.last_ranked_date`.
- Universe configs cannot be deleted while they are still referenced by saved strategies or ranking schemas.

## Verification

List schemas:

```sql
SELECT name, version, updated_at
FROM core.ranking_schemas
ORDER BY name;
```

Check recent runs:

```sql
SELECT run_id, strategy_name, ranking_schema_name, status, row_count, date_count, started_at, finished_at
FROM core.ranking_runs
ORDER BY started_at DESC
LIMIT 20;
```

Inspect a platinum output:

```sql
SELECT date, symbol, rank, score, last_updated_date
FROM platinum.mom_spy_res
ORDER BY date DESC, rank ASC
LIMIT 50;
```

Check watermarks:

```sql
SELECT strategy_name, ranking_schema_name, ranking_schema_version, output_table_name, last_ranked_date, updated_at
FROM core.ranking_watermarks
ORDER BY strategy_name;
```

Local verification:

```powershell
python scripts/run_quality_gate.py test-platinum-rankings
python scripts/run_quality_gate.py test-fast
python -m pytest tests/tasks/test_platinum_rankings.py tests/core/ranking_engine/test_service.py tests/test_workflow_runtime_ownership.py -q
```
