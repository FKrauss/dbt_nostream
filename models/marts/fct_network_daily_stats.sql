{{
  config(
    materialized = 'view',
    schema = 'marts'
  )
}}

-- Daily network-wide statistics for nostr
-- Tracks aggregate activity across the entire network

SELECT
  DATE(Timestamp) AS activity_date,
  
  -- Event counts by type
  COUNTIF(kind = 0) AS profile_updates,
  COUNTIF(kind = 1) AS notes_published,
  COUNTIF(kind = 3) AS contact_list_updates,
  COUNTIF(kind = 6) AS reposts,
  COUNTIF(kind = 7) AS reactions,
  COUNTIF(kind = 9735) AS zaps,
  
  -- Total events
  COUNT(*) AS total_events,
  
  -- Unique participants
  COUNT(DISTINCT CASE WHEN kind = 0 THEN pubkey END) AS unique_profile_updates,
  COUNT(DISTINCT CASE WHEN kind = 1 THEN pubkey END) AS unique_note_authors,
  COUNT(DISTINCT CASE WHEN kind = 3 THEN pubkey END) AS unique_contact_updaters,
  COUNT(DISTINCT CASE WHEN kind = 7 THEN pubkey END) AS unique_reactors,
  COUNT(DISTINCT CASE WHEN kind = 9735 THEN pubkey END) AS unique_zappers,
  
  -- Total unique active users (any event type)
  COUNT(DISTINCT pubkey) AS daily_active_users,
  
  -- Zap metrics (from joined zap data)
  COALESCE(SUM(CASE WHEN kind = 9735 THEN amount_sats END), 0) AS total_zap_sats,
  AVG(CASE WHEN kind = 9735 THEN amount_sats END) AS avg_zap_sats

FROM (
  SELECT
    Timestamp,
    JSON_VALUE(payload, '$.pubkey') AS pubkey,
    CAST(JSON_VALUE(payload, '$.kind') AS INT64) AS kind,
    CAST(JSON_VALUE(payload, '$.tags[3][1]') AS INT64) / 1000 AS amount_sats
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
)
GROUP BY activity_date
ORDER BY activity_date DESC