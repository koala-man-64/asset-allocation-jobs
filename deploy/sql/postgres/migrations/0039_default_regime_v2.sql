BEGIN;

INSERT INTO core.regime_models (name, description, version, config, created_at, updated_at)
VALUES (
  'default-regime',
  'Canonical default gold-only regime monitor model',
  2,
  '{
    "trendPositiveThreshold": 0.02,
    "trendNegativeThreshold": -0.02,
    "curveContangoThreshold": 0.5,
    "curveInvertedThreshold": -0.5,
    "highVolEnterThreshold": 28.0,
    "highVolExitThreshold": 28.0,
    "bearVolMin": 15.0,
    "bearVolMaxExclusive": 25.0,
    "bullVolMaxExclusive": 15.0,
    "choppyVolMin": 10.0,
    "choppyVolMaxExclusive": 18.0,
    "haltVixThreshold": 32.0,
    "haltVixStreakDays": 2,
    "precedence": [
      "high_vol",
      "trending_bear",
      "trending_bull",
      "choppy_mean_reversion",
      "unclassified"
    ]
  }'::jsonb,
  NOW(),
  NOW()
)
ON CONFLICT (name) DO UPDATE
SET
  description = EXCLUDED.description,
  version = GREATEST(core.regime_models.version, EXCLUDED.version),
  config = EXCLUDED.config,
  updated_at = NOW();

INSERT INTO core.regime_model_revisions (
  model_name,
  version,
  description,
  config,
  status,
  config_hash,
  published_at,
  created_at
)
SELECT
  'default-regime',
  2,
  'Canonical default gold-only regime monitor model',
  '{
    "trendPositiveThreshold": 0.02,
    "trendNegativeThreshold": -0.02,
    "curveContangoThreshold": 0.5,
    "curveInvertedThreshold": -0.5,
    "highVolEnterThreshold": 28.0,
    "highVolExitThreshold": 28.0,
    "bearVolMin": 15.0,
    "bearVolMaxExclusive": 25.0,
    "bullVolMaxExclusive": 15.0,
    "choppyVolMin": 10.0,
    "choppyVolMaxExclusive": 18.0,
    "haltVixThreshold": 32.0,
    "haltVixStreakDays": 2,
    "precedence": [
      "high_vol",
      "trending_bear",
      "trending_bull",
      "choppy_mean_reversion",
      "unclassified"
    ]
  }'::jsonb,
  'published',
  md5('{
    "trendPositiveThreshold":0.02,
    "trendNegativeThreshold":-0.02,
    "curveContangoThreshold":0.5,
    "curveInvertedThreshold":-0.5,
    "highVolEnterThreshold":28.0,
    "highVolExitThreshold":28.0,
    "bearVolMin":15.0,
    "bearVolMaxExclusive":25.0,
    "bullVolMaxExclusive":15.0,
    "choppyVolMin":10.0,
    "choppyVolMaxExclusive":18.0,
    "haltVixThreshold":32.0,
    "haltVixStreakDays":2,
    "precedence":["high_vol","trending_bear","trending_bull","choppy_mean_reversion","unclassified"]
  }'),
  NOW(),
  NOW()
WHERE NOT EXISTS (
  SELECT 1
  FROM core.regime_model_revisions
  WHERE model_name = 'default-regime' AND version = 2
);

INSERT INTO core.regime_model_activations (model_name, model_version, activated_by, activated_at)
SELECT 'default-regime', 2, 'migration-0039', NOW()
WHERE NOT EXISTS (
  SELECT 1
  FROM core.regime_model_activations
  WHERE model_name = 'default-regime' AND model_version = 2
);

COMMIT;
