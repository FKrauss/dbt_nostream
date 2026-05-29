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
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS published_at,
    JSON_VALUE(payload, '$.content') AS content_preview,
    1 AS note_count
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 1
),

reactions AS (
  SELECT
    -- Tag "e" contains the target event ID
    JSON_VALUE(tag, '$[1]') AS target_note_id,
    COUNT(*) AS reaction_count,
    COUNT(DISTINCT JSON_VALUE(payload, '$.npub')) AS unique_reactors
  FROM `replit-gcp.Nostr.events`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 7
    AND JSON_VALUE(tag, '$[0]') = 'e'
  GROUP BY target_note_id
),

reposts AS (
  SELECT
    -- Tag "e" contains the reposted event ID
    JSON_VALUE(tag, '$[1]') AS target_note_id,
    COUNT(*) AS repost_count
  FROM `replit-gcp.Nostr.events`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 6
    AND JSON_VALUE(tag, '$[0]') = 'e'
  GROUP BY target_note_id
),

zaps AS (
  SELECT
    -- Tag "e" contains the zapped event ID
    JSON_VALUE(tag, '$[1]') AS target_note_id,
    COUNT(*) AS zap_count
  FROM `replit-gcp.Nostr.events`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735
    AND JSON_VALUE(tag, '$[0]') = 'e'
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
    -- Engagement score: weighted combination (reactions=1, reposts=3, zaps=10)
    (COALESCE(r.reaction_count, 0) * 1) +
    (COALESCE(rp.repost_count, 0) * 3) +
    (COALESCE(z.zap_count, 0) * 10) AS engagement_score
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