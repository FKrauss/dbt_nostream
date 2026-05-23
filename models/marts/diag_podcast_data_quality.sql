{{
  config(
    materialized = 'view',
    schema = 'marts'
  )
}}

-- Data quality diagnostics for podcast staging models.
-- Exposes dimensions where the data is thin, ambiguous, or inconsistent.
-- No assertions — just raw counts for human triage.

WITH podcast_events AS (
  SELECT * FROM {{ ref('stg_podcast_events') }}
),

podcast_zaps AS (
  SELECT * FROM {{ ref('stg_podcast_zaps') }}
),

-- 1. NULL GUID rates per field
null_rates AS (
  SELECT
    'episode_guid' AS field_name,
    COUNTIF(episode_guid IS NULL) AS null_count,
    COUNT(*) AS total_count
  FROM podcast_events
  UNION ALL
  SELECT
    'show_guid',
    COUNTIF(show_guid IS NULL),
    COUNT(*)
  FROM podcast_events
  UNION ALL
  SELECT
    'publisher_guid',
    COUNTIF(publisher_guid IS NULL),
    COUNT(*)
  FROM podcast_events
  UNION ALL
  SELECT
    'episode_guid (zaps)',
    COUNTIF(episode_guid IS NULL),
    COUNT(*)
  FROM podcast_zaps
  UNION ALL
  SELECT
    'show_guid (zaps)',
    COUNTIF(show_guid IS NULL),
    COUNT(*)
  FROM podcast_zaps
  UNION ALL
  SELECT
    'publisher_guid (zaps)',
    COUNTIF(publisher_guid IS NULL),
    COUNT(*)
  FROM podcast_zaps
),

-- 2. Content source inference: Fountain vs non-Fountain
inferred_source AS (
  SELECT
    CASE WHEN content LIKE '%fountain.fm%' OR content LIKE '%fountain%' THEN 'fountain'
         WHEN content LIKE '%podcastindex.org%' THEN 'podcastindex'
         WHEN content LIKE '%stablekraft%' OR content LIKE '%doerfelverse%' THEN 'stablekraft'
         WHEN content IS NULL THEN 'null'
         ELSE 'other'
    END AS inferred_app,
    COUNT(*) AS cnt,
    COUNT(DISTINCT npub) AS unique_authors
  FROM podcast_events
  WHERE kind = 1
  GROUP BY inferred_app
),

-- 3. Episode-to-show mapping: episodes seen with multiple shows
multi_show_episodes AS (
  SELECT
    episode_guid,
    COUNT(DISTINCT show_guid) AS distinct_show_count
  FROM podcast_events
  WHERE episode_guid IS NOT NULL
  GROUP BY episode_guid
  HAVING COUNT(DISTINCT show_guid) > 1
),

-- 4. Zap amount distribution
zap_amounts AS (
  SELECT
    APPROX_QUANTILES(amount_msats, 4) AS quartiles,
    COUNTIF(amount_msats IS NULL) AS null_amount_count,
    COUNTIF(amount_msats <= 0) AS non_positive_amount_count,
    COUNT(*) AS total_zap_rows
  FROM podcast_zaps
),

-- 5. Share/zap volume by date (spot activity gaps)
daily_volumes AS (
  SELECT
    DATE(created_at) AS dt,
    COUNTIF(kind = 1) AS shares,
    COUNTIF(kind = 9735) AS zaps,
    COUNTIF(kind = 1111) AS comments
  FROM podcast_events
  GROUP BY DATE(created_at)
  ORDER BY dt DESC
  LIMIT 14
)

-- Final: emit one row per diagnostic category
SELECT
  'null_guid_rate' AS checkpoint,
  field_name AS dimension,
  SAFE_DIVIDE(null_count, total_count) * 100 AS pct,
  null_count AS numerator,
  total_count AS denominator,
  'percent' AS unit,
  NULL AS sample_values
FROM null_rates

UNION ALL

SELECT
  'inferred_app_distribution',
  inferred_app,
  SAFE_DIVIDE(cnt, (SELECT SUM(cnt) FROM inferred_source)) * 100,
  cnt,
  (SELECT SUM(cnt) FROM inferred_source),
  'percent',
  CAST(unique_authors AS STRING)
FROM inferred_source

UNION ALL

SELECT
  'multi_show_episodes',
  CAST((SELECT COUNT(*) FROM multi_show_episodes) AS STRING),
  SAFE_DIVIDE((SELECT COUNT(*) FROM multi_show_episodes), (SELECT COUNT(DISTINCT episode_guid) FROM podcast_events WHERE episode_guid IS NOT NULL)) * 100,
  (SELECT COUNT(*) FROM multi_show_episodes),
  (SELECT COUNT(DISTINCT episode_guid) FROM podcast_events WHERE episode_guid IS NOT NULL),
  'percent',
  (SELECT STRING_AGG(episode_guid, ', ') FROM (SELECT episode_guid FROM multi_show_episodes LIMIT 5))

UNION ALL

SELECT
  'zap_amount_health',
  'null_or_nonpos',
  SAFE_DIVIDE(null_amount_count + non_positive_amount_count, total_zap_rows) * 100,
  null_amount_count + non_positive_amount_count,
  total_zap_rows,
  'percent',
  CONCAT('Q1=', CAST(quartiles[OFFSET(0)] AS STRING),
         ' Q2=', CAST(quartiles[OFFSET(1)] AS STRING),
         ' Q3=', CAST(quartiles[OFFSET(2)] AS STRING),
         ' Q4=', CAST(quartiles[OFFSET(3)] AS STRING))
FROM zap_amounts
