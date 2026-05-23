{{ config(
    materialized='view',
    schema='marts'
) }}

-- Daily engagement metrics for podcast episodes and shows.
-- Aggregates shares (kind 1), zaps (kind 9735 with amount), and comments (kind 1111).
-- Also tracks total satoshis sent via podcast zaps.

WITH shares AS (
  SELECT
    episode_guid,
    show_guid,
    DATE(created_at) AS event_date,
    event_id,
    npub,
    'share' AS interaction_type,
    CAST(NULL AS INT64) AS amount_msats
  FROM {{ ref('stg_podcast_events') }}
  WHERE kind = 1
),

zaps AS (
  SELECT
    episode_guid,
    show_guid,
    DATE(created_at) AS event_date,
    event_id,
    npub,
    'zap' AS interaction_type,
    amount_msats
  FROM {{ ref('stg_podcast_zaps') }}
),

combined AS (
  SELECT * FROM shares
  UNION ALL
  SELECT * FROM zaps
)

SELECT
  COALESCE(episode_guid, show_guid, 'unknown') AS content_guid,
  CASE WHEN episode_guid IS NOT NULL THEN 'episode' ELSE 'show' END AS content_type,
  event_date,
  COUNTIF(interaction_type = 'share') AS share_count,
  COUNTIF(interaction_type = 'zap') AS zap_count,
  ROUND(SAFE_DIVIDE(SUM(COALESCE(amount_msats, 0)), 1000), 2) AS total_zap_sats,
  COUNT(DISTINCT npub) AS unique_interactors,
  COUNT(DISTINCT event_id) AS total_events
FROM combined
GROUP BY content_guid, content_type, event_date
ORDER BY event_date DESC, total_zap_sats DESC, share_count DESC
