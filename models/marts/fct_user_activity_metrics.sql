{{
  config(
    materialized = 'view',
    schema = 'marts'
  )
}}

-- Daily activity metrics per user
-- Aggregates user actions (notes, reactions, reposts, zaps) by day

WITH base_events AS (
  SELECT
    JSON_VALUE(payload, '$.npub') AS npub,
    JSON_VALUE(payload, '$.pubkey') AS pubkey_hex,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    DATE(TIMESTAMP(JSON_VALUE(payload, '$.createdAt'))) AS activity_date,
    CAST(JSON_VALUE(payload, '$.kind') AS INT64) AS kind
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
),

-- Notes published (kind 1)
notes AS (
  SELECT
    COALESCE(npub, pubkey_hex) AS user_npub,
    activity_date,
    COUNT(*) AS notes_published
  FROM base_events
  WHERE kind = 1
  GROUP BY user_npub, activity_date
),

-- Reactions sent (kind 7)
reactions_sent AS (
  SELECT
    COALESCE(npub, pubkey_hex) AS user_npub,
    activity_date,
    COUNT(*) AS reactions_sent
  FROM base_events
  WHERE kind = 7
  GROUP BY user_npub, activity_date
),

-- Reposts made (kind 6)
reposts AS (
  SELECT
    COALESCE(npub, pubkey_hex) AS user_npub,
    activity_date,
    COUNT(*) AS reposts_made
  FROM base_events
  WHERE kind = 6
  GROUP BY user_npub, activity_date
),

-- Zaps sent (kind 9735)
zaps_sent AS (
  SELECT
    COALESCE(npub, pubkey_hex) AS user_npub,
    activity_date,
    COUNT(*) AS zaps_sent_count
  FROM base_events
  WHERE kind = 9735
  GROUP BY user_npub, activity_date
),

-- Profile updates (kind 0)
profile_updates AS (
  SELECT
    COALESCE(npub, pubkey_hex) AS user_npub,
    activity_date,
    COUNT(*) AS profile_updates
  FROM base_events
  WHERE kind = 0
  GROUP BY user_npub, activity_date
),

-- Contact list updates (kind 3)
contact_updates AS (
  SELECT
    COALESCE(npub, pubkey_hex) AS user_npub,
    activity_date,
    COUNT(*) AS contact_list_updates
  FROM base_events
  WHERE kind = 3
  GROUP BY user_npub, activity_date
),

-- All active users per day
all_user_days AS (
  SELECT DISTINCT user_npub, activity_date
  FROM (
    SELECT user_npub, activity_date FROM notes
    UNION ALL
    SELECT user_npub, activity_date FROM reactions_sent
    UNION ALL
    SELECT user_npub, activity_date FROM reposts
    UNION ALL
    SELECT user_npub, activity_date FROM zaps_sent
    UNION ALL
    SELECT user_npub, activity_date FROM profile_updates
    UNION ALL
    SELECT user_npub, activity_date FROM contact_updates
  )
)

SELECT
  aud.user_npub AS npub,
  aud.activity_date,
  COALESCE(n.notes_published, 0) AS notes_published,
  COALESCE(rs.reactions_sent, 0) AS reactions_sent,
  COALESCE(rp.reposts_made, 0) AS reposts_made,
  COALESCE(zs.zaps_sent_count, 0) AS zaps_sent_count,
  COALESCE(pu.profile_updates, 0) AS profile_updates,
  COALESCE(cu.contact_list_updates, 0) AS contact_list_updates,
  -- Total activity score (weighted)
  (COALESCE(n.notes_published, 0) * 2) +
  (COALESCE(rs.reactions_sent, 0) * 1) +
  (COALESCE(rp.reposts_made, 0) * 3) +
  (COALESCE(zs.zaps_sent_count, 0) * 5) +
  (COALESCE(pu.profile_updates, 0) * 1) +
  (COALESCE(cu.contact_list_updates, 0) * 1) AS activity_score
FROM all_user_days aud
LEFT JOIN notes n ON aud.user_npub = n.user_npub AND aud.activity_date = n.activity_date
LEFT JOIN reactions_sent rs ON aud.user_npub = rs.user_npub AND aud.activity_date = rs.activity_date
LEFT JOIN reposts rp ON aud.user_npub = rp.user_npub AND aud.activity_date = rp.activity_date
LEFT JOIN zaps_sent zs ON aud.user_npub = zs.user_npub AND aud.activity_date = zs.activity_date
LEFT JOIN profile_updates pu ON aud.user_npub = pu.user_npub AND aud.activity_date = pu.activity_date
LEFT JOIN contact_updates cu ON aud.user_npub = cu.user_npub AND aud.activity_date = cu.activity_date
