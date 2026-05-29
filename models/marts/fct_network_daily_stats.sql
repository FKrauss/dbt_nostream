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
  COUNTIF(CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 0) AS profile_updates,
  COUNTIF(CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 1) AS notes_published,
  COUNTIF(CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 3) AS contact_list_updates,
  COUNTIF(CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 6) AS reposts,
  COUNTIF(CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 7) AS reactions,
  COUNTIF(CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735) AS zaps,
  
  -- Total events
  COUNT(*) AS total_events,
  
  -- Unique participants by event type
  COUNT(DISTINCT CASE WHEN CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 0 THEN JSON_VALUE(payload, '$.npub') END) AS unique_profile_updates,
  COUNT(DISTINCT CASE WHEN CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 1 THEN JSON_VALUE(payload, '$.npub') END) AS unique_note_authors,
  COUNT(DISTINCT CASE WHEN CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 3 THEN JSON_VALUE(payload, '$.npub') END) AS unique_contact_updaters,
  COUNT(DISTINCT CASE WHEN CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 7 THEN JSON_VALUE(payload, '$.npub') END) AS unique_reactors,
  COUNT(DISTINCT CASE WHEN CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735 THEN JSON_VALUE(payload, '$.npub') END) AS unique_zappers,
  
  -- Total unique active users (any event type) - using npub for user identification
  COUNT(DISTINCT JSON_VALUE(payload, '$.npub')) AS daily_active_users,
  
  -- Zap count only (we don't have reliable amount extraction yet)
  COUNTIF(CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735) AS total_zap_events

FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
GROUP BY activity_date
ORDER BY activity_date DESC