BEGIN;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS core.economic_catalyst_source_state (
  source_name TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  state_type TEXT NOT NULL,
  cursor_value TEXT,
  source_commit TEXT,
  last_effective_at TIMESTAMPTZ,
  last_published_at TIMESTAMPTZ,
  last_source_updated_at TIMESTAMPTZ,
  last_ingested_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (source_name, dataset_name, state_type)
);

CREATE INDEX IF NOT EXISTS idx_core_economic_catalyst_source_state_updated_at
  ON core.economic_catalyst_source_state(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_core_economic_catalyst_source_state_last_ingested_at
  ON core.economic_catalyst_source_state(last_ingested_at DESC);

CREATE TABLE IF NOT EXISTS gold.economic_catalyst_events (
  event_id TEXT PRIMARY KEY,
  event_key TEXT NOT NULL,
  event_name TEXT NOT NULL,
  event_group TEXT NOT NULL CHECK (
    event_group IN (
      'Labor',
      'Inflation',
      'GrowthActivity',
      'Housing',
      'ConsumerSentiment',
      'TradeExternal',
      'RatesFiscal',
      'CentralBankPolicy',
      'CreditRegulatory'
    )
  ),
  event_subgroup TEXT,
  event_type TEXT NOT NULL,
  importance_tier TEXT NOT NULL CHECK (importance_tier IN ('low', 'medium', 'high')),
  impact_domain TEXT NOT NULL DEFAULT 'macro',
  country TEXT,
  region TEXT,
  currency TEXT,
  source_name TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  official_source_name TEXT,
  effective_at TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  source_updated_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ,
  time_precision TEXT NOT NULL CHECK (time_precision IN ('exact', 'approximate', 'date_only', 'unknown')),
  schedule_status TEXT NOT NULL CHECK (
    schedule_status IN ('scheduled', 'released', 'revised', 'cancelled', 'withdrawn', 'unknown')
  ),
  is_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  actual_numeric DOUBLE PRECISION,
  actual_text TEXT,
  consensus_numeric DOUBLE PRECISION,
  consensus_text TEXT,
  previous_numeric DOUBLE PRECISION,
  previous_text TEXT,
  revised_previous_numeric DOUBLE PRECISION,
  revised_previous_text TEXT,
  surprise_abs DOUBLE PRECISION,
  surprise_pct DOUBLE PRECISION,
  unit TEXT,
  period_label TEXT,
  frequency TEXT,
  market_sensitivity_tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  sector_tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  factor_tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  is_high_impact BOOLEAN NOT NULL DEFAULT FALSE,
  is_routine BOOLEAN NOT NULL DEFAULT TRUE,
  is_revisionable BOOLEAN NOT NULL DEFAULT FALSE,
  withdrawal_flag BOOLEAN NOT NULL DEFAULT FALSE,
  source_hash TEXT NOT NULL,
  provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_events_effective_at
  ON gold.economic_catalyst_events(effective_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_events_published_at
  ON gold.economic_catalyst_events(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_events_group_effective
  ON gold.economic_catalyst_events(event_group, effective_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_events_status_effective
  ON gold.economic_catalyst_events(schedule_status, effective_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_events_source_effective
  ON gold.economic_catalyst_events(source_name, effective_at DESC);

CREATE TABLE IF NOT EXISTS gold.economic_catalyst_event_versions (
  version_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL REFERENCES gold.economic_catalyst_events(event_id) ON DELETE CASCADE,
  version_seq INTEGER NOT NULL CHECK (version_seq >= 1),
  version_kind TEXT NOT NULL CHECK (
    version_kind IN ('schedule', 'release', 'revision', 'withdrawal', 'cancellation', 'correction')
  ),
  version_observed_at TIMESTAMPTZ NOT NULL,
  event_key TEXT NOT NULL,
  event_name TEXT NOT NULL,
  event_group TEXT NOT NULL CHECK (
    event_group IN (
      'Labor',
      'Inflation',
      'GrowthActivity',
      'Housing',
      'ConsumerSentiment',
      'TradeExternal',
      'RatesFiscal',
      'CentralBankPolicy',
      'CreditRegulatory'
    )
  ),
  event_subgroup TEXT,
  event_type TEXT NOT NULL,
  importance_tier TEXT NOT NULL CHECK (importance_tier IN ('low', 'medium', 'high')),
  impact_domain TEXT NOT NULL DEFAULT 'macro',
  country TEXT,
  region TEXT,
  currency TEXT,
  source_name TEXT NOT NULL,
  source_event_key TEXT NOT NULL,
  official_source_name TEXT,
  effective_at TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  source_updated_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ,
  time_precision TEXT NOT NULL CHECK (time_precision IN ('exact', 'approximate', 'date_only', 'unknown')),
  schedule_status TEXT NOT NULL CHECK (
    schedule_status IN ('scheduled', 'released', 'revised', 'cancelled', 'withdrawn', 'unknown')
  ),
  is_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  actual_numeric DOUBLE PRECISION,
  actual_text TEXT,
  consensus_numeric DOUBLE PRECISION,
  consensus_text TEXT,
  previous_numeric DOUBLE PRECISION,
  previous_text TEXT,
  revised_previous_numeric DOUBLE PRECISION,
  revised_previous_text TEXT,
  surprise_abs DOUBLE PRECISION,
  surprise_pct DOUBLE PRECISION,
  unit TEXT,
  period_label TEXT,
  frequency TEXT,
  market_sensitivity_tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  sector_tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  factor_tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  is_high_impact BOOLEAN NOT NULL DEFAULT FALSE,
  is_routine BOOLEAN NOT NULL DEFAULT TRUE,
  is_revisionable BOOLEAN NOT NULL DEFAULT FALSE,
  withdrawal_flag BOOLEAN NOT NULL DEFAULT FALSE,
  source_hash TEXT NOT NULL,
  provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_event_versions_event_id
  ON gold.economic_catalyst_event_versions(event_id, version_seq DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_event_versions_observed
  ON gold.economic_catalyst_event_versions(version_observed_at DESC);

CREATE TABLE IF NOT EXISTS gold.economic_catalyst_headlines (
  headline_id TEXT PRIMARY KEY,
  headline_key TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_item_id TEXT NOT NULL,
  headline TEXT NOT NULL,
  summary TEXT,
  url TEXT,
  author TEXT,
  published_at TIMESTAMPTZ,
  source_updated_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ,
  country TEXT,
  region TEXT,
  event_group TEXT,
  importance_tier TEXT NOT NULL CHECK (importance_tier IN ('low', 'medium', 'high')),
  relevance_tier TEXT NOT NULL CHECK (relevance_tier IN ('low', 'medium', 'high')),
  withdrawal_flag BOOLEAN NOT NULL DEFAULT FALSE,
  tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  tickers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  channels_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_hash TEXT NOT NULL,
  provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_headlines_published_at
  ON gold.economic_catalyst_headlines(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_headlines_group_published
  ON gold.economic_catalyst_headlines(event_group, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_headlines_source_published
  ON gold.economic_catalyst_headlines(source_name, published_at DESC);

CREATE TABLE IF NOT EXISTS gold.economic_catalyst_headline_versions (
  version_id TEXT PRIMARY KEY,
  headline_id TEXT NOT NULL REFERENCES gold.economic_catalyst_headlines(headline_id) ON DELETE CASCADE,
  version_seq INTEGER NOT NULL CHECK (version_seq >= 1),
  version_kind TEXT NOT NULL CHECK (version_kind IN ('publish', 'edit', 'withdrawal', 'correction')),
  version_observed_at TIMESTAMPTZ NOT NULL,
  headline_key TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_item_id TEXT NOT NULL,
  headline TEXT NOT NULL,
  summary TEXT,
  url TEXT,
  author TEXT,
  published_at TIMESTAMPTZ,
  source_updated_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ,
  country TEXT,
  region TEXT,
  event_group TEXT,
  importance_tier TEXT NOT NULL CHECK (importance_tier IN ('low', 'medium', 'high')),
  relevance_tier TEXT NOT NULL CHECK (relevance_tier IN ('low', 'medium', 'high')),
  withdrawal_flag BOOLEAN NOT NULL DEFAULT FALSE,
  tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  tickers_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  channels_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_hash TEXT NOT NULL,
  provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_headline_versions_headline_id
  ON gold.economic_catalyst_headline_versions(headline_id, version_seq DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_headline_versions_observed
  ON gold.economic_catalyst_headline_versions(version_observed_at DESC);

CREATE TABLE IF NOT EXISTS gold.economic_catalyst_mentions (
  item_kind TEXT NOT NULL CHECK (item_kind IN ('event', 'headline')),
  item_id TEXT NOT NULL,
  entity_type TEXT NOT NULL CHECK (
    entity_type IN ('country', 'region', 'central_bank', 'indicator', 'currency', 'symbol', 'sector', 'factor')
  ),
  entity_key TEXT NOT NULL,
  relevance_tier TEXT NOT NULL CHECK (relevance_tier IN ('low', 'medium', 'high')),
  confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
  mapping_rule_version TEXT NOT NULL,
  source_name TEXT,
  published_at TIMESTAMPTZ,
  effective_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ,
  PRIMARY KEY (item_kind, item_id, entity_type, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_mentions_entity
  ON gold.economic_catalyst_mentions(entity_type, entity_key, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_mentions_item
  ON gold.economic_catalyst_mentions(item_kind, item_id);

CREATE TABLE IF NOT EXISTS gold.economic_catalyst_entity_daily (
  as_of_date DATE NOT NULL,
  entity_type TEXT NOT NULL CHECK (
    entity_type IN ('country', 'region', 'central_bank', 'indicator', 'currency', 'symbol', 'sector', 'factor')
  ),
  entity_key TEXT NOT NULL,
  headline_count INTEGER NOT NULL DEFAULT 0,
  event_count INTEGER NOT NULL DEFAULT 0,
  high_impact_event_count INTEGER NOT NULL DEFAULT 0,
  release_count INTEGER NOT NULL DEFAULT 0,
  scheduled_count INTEGER NOT NULL DEFAULT 0,
  policy_event_count INTEGER NOT NULL DEFAULT 0,
  inflation_event_count INTEGER NOT NULL DEFAULT 0,
  labor_event_count INTEGER NOT NULL DEFAULT 0,
  growth_event_count INTEGER NOT NULL DEFAULT 0,
  rates_event_count INTEGER NOT NULL DEFAULT 0,
  last_published_at TIMESTAMPTZ,
  last_effective_at TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ,
  PRIMARY KEY (as_of_date, entity_type, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_entity_daily_entity
  ON gold.economic_catalyst_entity_daily(entity_type, entity_key, as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_gold_economic_catalyst_entity_daily_as_of
  ON gold.economic_catalyst_entity_daily(as_of_date DESC);

CREATE OR REPLACE VIEW gold.economic_catalyst_calendar_by_date AS
SELECT *
FROM gold.economic_catalyst_events
WHERE NOT withdrawal_flag
  AND schedule_status IN ('scheduled', 'cancelled', 'unknown');

CREATE OR REPLACE VIEW gold.economic_catalyst_releases_by_date AS
SELECT *
FROM gold.economic_catalyst_events
WHERE NOT withdrawal_flag
  AND schedule_status IN ('released', 'revised');

CREATE OR REPLACE VIEW gold.economic_catalyst_headlines_by_date AS
SELECT *
FROM gold.economic_catalyst_headlines
WHERE NOT withdrawal_flag;

CREATE OR REPLACE VIEW gold.economic_catalyst_entity_daily_by_date AS
SELECT *
FROM gold.economic_catalyst_entity_daily;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT USAGE ON SCHEMA core TO backtest_service;
    GRANT SELECT ON TABLE core.economic_catalyst_source_state TO backtest_service;

    GRANT USAGE ON SCHEMA gold TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_events TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_event_versions TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_headlines TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_headline_versions TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_mentions TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_entity_daily TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_calendar_by_date TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_releases_by_date TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_headlines_by_date TO backtest_service;
    GRANT SELECT ON TABLE gold.economic_catalyst_entity_daily_by_date TO backtest_service;
  END IF;
END $$;

COMMIT;
