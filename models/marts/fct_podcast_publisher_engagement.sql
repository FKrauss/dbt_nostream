{{
  config(
    materialized = 'view',
    schema = 'marts'
  )
}}

-- Daily engagement metrics at the content creator (publisher) level.
-- Groups all share and zap activity by publisher_guid and event_date.

WITH episode_events AS (
  SELECT
    publisher_guid,
    DATE(created_at) AS event_date,
    COUNT(DISTINCT episode_guid) AS episode_count,
    COUNT(DISTINCT show_guid) AS show_count,
    COUNTIF(kind = 1) AS share_count,
    COUNTIF(kind = 1111) AS comment_count,
    COUNTIF(kind = 9735) AS zap_count,
    COUNT(DISTINCT npub) AS unique_interactors
  FROM {{ ref('stg_podcast_events') }}
  WHERE publisher_guid IS NOT NULL
  GROUP BY publisher_guid, DATE(created_at)
),

zap_events AS (
  SELECT
    publisher_guid,
    DATE(created_at) AS event_date,
    COUNT(*) AS zap_receipt_count,
    SAFE_DIVIDE(SUM(amount_msats), 1000.0) AS total_zap_sats,
    COUNT(DISTINCT npub) AS unique_zappers
  FROM {{ ref('stg_podcast_zaps') }}
  WHERE publisher_guid IS NOT NULL
  GROUP BY publisher_guid, DATE(created_at)
)

SELECT
  COALESCE(ee.publisher_guid, ze.publisher_guid) AS publisher_guid,
  COALESCE(ee.event_date, ze.event_date) AS event_date,
  COALESCE(ee.episode_count, 0) AS active_episode_count,
  COALESCE(ee.show_count, 0) AS active_show_count,
  COALESCE(ee.share_count, 0) AS share_count,
  COALESCE(ee.comment_count, 0) AS comment_count,
  COALESCE(ee.zap_count, 0) AS zap_event_count,
  COALESCE(ee.unique_interactors, 0) AS unique_interactors,
  COALESCE(ze.zap_receipt_count, 0) AS zap_receipt_count,
  ROUND(COALESCE(ze.total_zap_sats, 0), 2) AS total_zap_sats,
  COALESCE(ze.unique_zappers, 0) AS unique_zappers,
  COALESCE(ee.share_count, 0) + COALESCE(ee.comment_count, 0) + COALESCE(ze.zap_receipt_count, 0) AS total_events
FROM episode_events ee
FULL OUTER JOIN zap_events ze
  ON ee.publisher_guid = ze.publisher_guid
  AND ee.event_date = ze.event_date
