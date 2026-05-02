BEGIN;

CREATE TABLE IF NOT EXISTS core.backtest_policy_events (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  event_seq INTEGER NOT NULL CHECK (event_seq >= 1),
  bar_ts TIMESTAMPTZ NOT NULL,
  scope TEXT NOT NULL,
  policy_type TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  symbol TEXT,
  position_id TEXT,
  policy_id TEXT,
  observed_value DOUBLE PRECISION,
  threshold_value DOUBLE PRECISION,
  action TEXT,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (run_id, event_seq)
);

CREATE INDEX IF NOT EXISTS idx_backtest_policy_events_run_bar_seq
  ON core.backtest_policy_events(run_id, bar_ts, event_seq);

ALTER TABLE core.backtest_run_summary
  ADD COLUMN IF NOT EXISTS research_integrity_status TEXT,
  ADD COLUMN IF NOT EXISTS execution_model TEXT,
  ADD COLUMN IF NOT EXISTS execution_model_quality TEXT,
  ADD COLUMN IF NOT EXISTS approval_readiness TEXT,
  ADD COLUMN IF NOT EXISTS data_quality_event_count INTEGER,
  ADD COLUMN IF NOT EXISTS policy_event_count INTEGER;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'backtest_run_summary_research_integrity_status_check'
      AND conrelid = 'core.backtest_run_summary'::regclass
  ) THEN
    ALTER TABLE core.backtest_run_summary
      ADD CONSTRAINT backtest_run_summary_research_integrity_status_check
      CHECK (research_integrity_status IS NULL OR research_integrity_status IN ('strict_passed', 'strict_failed', 'legacy_uncontrolled'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'backtest_run_summary_execution_model_check'
      AND conrelid = 'core.backtest_run_summary'::regclass
  ) THEN
    ALTER TABLE core.backtest_run_summary
      ADD CONSTRAINT backtest_run_summary_execution_model_check
      CHECK (execution_model IS NULL OR execution_model IN ('simple_bps'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'backtest_run_summary_execution_model_quality_check'
      AND conrelid = 'core.backtest_run_summary'::regclass
  ) THEN
    ALTER TABLE core.backtest_run_summary
      ADD CONSTRAINT backtest_run_summary_execution_model_quality_check
      CHECK (execution_model_quality IS NULL OR execution_model_quality IN ('not_tca_grade'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'backtest_run_summary_approval_readiness_check'
      AND conrelid = 'core.backtest_run_summary'::regclass
  ) THEN
    ALTER TABLE core.backtest_run_summary
      ADD CONSTRAINT backtest_run_summary_approval_readiness_check
      CHECK (approval_readiness IS NULL OR approval_readiness IN ('research_only'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'backtest_run_summary_event_counts_check'
      AND conrelid = 'core.backtest_run_summary'::regclass
  ) THEN
    ALTER TABLE core.backtest_run_summary
      ADD CONSTRAINT backtest_run_summary_event_counts_check
      CHECK (
        (data_quality_event_count IS NULL OR data_quality_event_count >= 0)
        AND (policy_event_count IS NULL OR policy_event_count >= 0)
      );
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS core.backtest_data_quality_events (
  run_id TEXT NOT NULL REFERENCES core.runs(run_id) ON DELETE CASCADE,
  event_seq INTEGER NOT NULL CHECK (event_seq >= 1),
  bar_ts TIMESTAMPTZ NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('warning', 'error', 'fatal')),
  table_name TEXT NOT NULL,
  symbol TEXT,
  field_name TEXT,
  reason_code TEXT NOT NULL,
  action TEXT NOT NULL,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (run_id, event_seq)
);

CREATE INDEX IF NOT EXISTS idx_backtest_data_quality_events_run_bar_seq
  ON core.backtest_data_quality_events(run_id, bar_ts, event_seq);

CREATE INDEX IF NOT EXISTS idx_backtest_data_quality_events_run_severity
  ON core.backtest_data_quality_events(run_id, severity);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'backtest_service') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_policy_events TO backtest_service;
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.backtest_data_quality_events TO backtest_service;
  END IF;
END $$;

COMMIT;
