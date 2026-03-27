{{
  config(
    materialized = 'view',
    schema = 'marts'
  )
}}

-- Top engaged notes from the last 24 hours
-- Combines reactions, reposts, and zaps to find trending content

WITH note_events AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS note_id,
    JSON_VALUE(payload, '$.pubkey') AS author_pubkey,
    TIMESTAMP_SECONDS(CAST(JSON_VALUE(payload, '$.created_at') AS INT64)) AS published_at,
    JSON_VALUE(payload, '$.content') AS content_preview,
    1 AS note_count
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND JSON_VALUE(payload, '$.kind') = '1'
),

reactions AS (
  SELECT
    -- Reaction target is in tags[0][1]
    JSON_VALUE(payload, '$.tags[0][1]') AS target_note_id,
    COUNT(*) AS reaction_count,
    COUNT(DISTINCT JSON_VALUE(payload, '$.pubkey')) AS unique_reactors
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND JSON_VALUE(payload, '$.kind') = '7'
  GROUP BY target_note_id
),

reposts AS (
  SELECT
    -- Repost target is in tags[0][1]
    JSON_VALUE(payload, '$.tags[0][1]') AS target_note_id,
    COUNT(*) AS repost_count
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND JSON_VALUE(payload, '$.kind') = '6'
  GROUP BY target_note_id
),

zaps AS (
  SELECT
    -- Zapped note is in tags[0][1]
    JSON_VALUE(payload, '$.tags[0][1]') AS target_note_id,
    COUNT(*) AS zap_count,
    SUM(CAST(JSON_VALUE(payload, '$.tags[3][1]') AS INT64) / 1000) AS total_zap_sats
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND JSON_VALUE(payload, '$.kind') = '9735'
  GROUP BY target_note_id
),

engagement AS (
  SELECT
    n.note_id,
    n.author_pubkey,
    n.published_at,
    LEFT(n.content_preview, 200) AS content_preview,
    COALESCE(r.reaction_count, 0) AS reaction_count,
    COALESCE(r.unique_reactors, 0) AS unique_reactors,
    COALESCE(rp.repost_count, 0) AS repost_count,
    COALESCE(z.zap_count, 0) AS zap_count,
    COALESCE(z.total_zap_sats, 0) AS total_zap_sats,
    -- Engagement score: weighted combination
    (COALESCE(r.reaction_count, 0) * 1) +
    (COALESCE(rp.repost_count, 0) * 3) +
    (COALESCE(z.zap_count, 0) * 10) +
    (COALESCE(z.total_zap_sats, 0) / 100) AS engagement_score
  FROM note_events n
  LEFT JOIN reactions r ON n.note_id = r.target_note_id
  LEFT JOIN reposts rp ON n.note_id = rp.target_note_id
  LEFT JOIN zaps z ON n.note_id = z.target_note_id
)

SELECT *
FROM engagement
WHERE engagement_score > 0
ORDER BY engagement_score DESC
LIMIT 100