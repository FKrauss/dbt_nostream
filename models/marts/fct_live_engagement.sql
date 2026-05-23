{{ config(
    materialized='view',
    schema='marts'
) }}

-- Daily engagement metrics per live stream.
-- Combines kind 30311 event counts (metadata updates / status changes)
-- with kind 9735 Lightning zaps. Service-neutral.

WITH events AS (
  SELECT
    DATE(created_at) AS activity_date,
    stream_id,
    COUNT(*) AS event_count,
    COUNTIF(status = 'live') AS live_count,
    COUNTIF(status = 'ended') AS ended_count,
    COUNT(DISTINCT author_pubkey) AS unique_authors
  FROM {{ ref('stg_live_events') }}
  GROUP BY 1,2
),

zaps AS (
  SELECT
    DATE(created_at) AS activity_date,
    stream_id,
    COUNT(*) AS zap_count,
    COUNT(DISTINCT zapper_pubkey) AS unique_zappers,
    SUM(SAFE_DIVIDE(amount_msats, 1000)) AS total_zap_sats
  FROM {{ ref('stg_live_zaps') }}
  GROUP BY 1,2
)

SELECT
  COALESCE(e.activity_date, z.activity_date) AS activity_date,
  COALESCE(e.stream_id, z.stream_id) AS stream_id,

  -- stream metadata
  s.title,
  s.host_pubkey,
  s.service_url,

  -- events
  e.event_count,
  e.live_count,
  e.ended_count,
  e.unique_authors,

  -- zaps
  z.zap_count,
  z.unique_zappers,
  z.total_zap_sats

FROM events e
FULL OUTER JOIN zaps z
  USING (activity_date, stream_id)
LEFT JOIN {{ ref('int_live_streams') }} s
  USING (stream_id)
