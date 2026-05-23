{{
  config(
    materialized = 'view',
    schema = 'marts'
  )
}}

-- Content engagement metrics for text notes (kind 1)
-- Aggregates reactions, reposts, and zaps per note

WITH note_events AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS note_id,
    JSON_VALUE(payload, '$.author') AS author_pubkey,
    JSON_VALUE(payload, '$.npub') AS author_npub,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS published_at,
    DATE(TIMESTAMP(JSON_VALUE(payload, '$.createdAt'))) AS published_date,
    LEFT(JSON_VALUE(payload, '$.content'), 280) AS content_preview
  FROM `replit-gcp.Nostr.events`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 1
),

-- Reactions to notes (kind 7 with tag "e" pointing to note)
reactions AS (
  SELECT
    JSON_VALUE(tag, '$[1]') AS target_note_id,
    COUNT(*) AS reaction_count,
    COUNT(DISTINCT JSON_VALUE(payload, '$.npub')) AS unique_reactors
  FROM `replit-gcp.Nostr.events`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 7
    AND JSON_VALUE(tag, '$[0]') = 'e'
  GROUP BY target_note_id
),

-- Reposts (kind 6 with tag "e" pointing to note)
reposts AS (
  SELECT
    JSON_VALUE(tag, '$[1]') AS target_note_id,
    COUNT(*) AS repost_count,
    COUNT(DISTINCT JSON_VALUE(payload, '$.npub')) AS unique_reposters
  FROM `replit-gcp.Nostr.events`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 6
    AND JSON_VALUE(tag, '$[0]') = 'e'
  GROUP BY target_note_id
),

-- Zaps to notes (kind 9735 with tag "e" pointing to note)
zaps AS (
  SELECT
    JSON_VALUE(tag, '$[1]') AS target_note_id,
    COUNT(*) AS zap_count,
    COUNT(DISTINCT JSON_VALUE(payload, '$.npub')) AS unique_zappers,
    -- Extract amount from zap request description if available
    SUM(
      SAFE_CAST(
        JSON_VALUE(
          (SELECT JSON_VALUE(t, '$[1]') 
           FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS t 
           WHERE JSON_VALUE(t, '$[0]') = 'description' 
           LIMIT 1),
          '$.tags[1][1]'
        ) AS INT64
      )
    ) AS total_amount_msats
  FROM `replit-gcp.Nostr.events`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735
    AND JSON_VALUE(tag, '$[0]') = 'e'
  GROUP BY target_note_id
),

-- Replies (kind 1 with tag "e" pointing to parent note)
-- Note: We count all notes with 'e' tags as potential replies since they reference another note
replies AS (
  SELECT
    JSON_VALUE(tag, '$[1]') AS parent_note_id,
    COUNT(*) AS reply_count,
    COUNT(DISTINCT JSON_VALUE(payload, '$.npub')) AS unique_repliers
  FROM `replit-gcp.Nostr.events`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 1
    AND JSON_VALUE(tag, '$[0]') = 'e'
  GROUP BY parent_note_id
),

-- Combine all engagement metrics
engagement AS (
  SELECT
    n.note_id,
    n.author_npub,
    n.author_pubkey,
    n.published_at,
    n.published_date,
    LEFT(n.content_preview, 280) AS content_preview,
    COALESCE(r.reaction_count, 0) AS reaction_count,
    COALESCE(r.unique_reactors, 0) AS unique_reactors,
    COALESCE(rp.repost_count, 0) AS repost_count,
    COALESCE(rp.unique_reposters, 0) AS unique_reposters,
    COALESCE(z.zap_count, 0) AS zap_count,
    COALESCE(z.unique_zappers, 0) AS unique_zappers,
    COALESCE(z.total_amount_msats, 0) AS total_zap_msats,
    SAFE_DIVIDE(COALESCE(z.total_amount_msats, 0), 1000) AS total_zap_sats,
    COALESCE(re.reply_count, 0) AS reply_count,
    COALESCE(re.unique_repliers, 0) AS unique_repliers,
    -- Engagement score: weighted combination
    -- reactions=1, reposts=3, replies=2, zaps=1 per count, plus 1 per 100 sats
    (COALESCE(r.reaction_count, 0) * 1.0) +
    (COALESCE(rp.repost_count, 0) * 3.0) +
    (COALESCE(re.reply_count, 0) * 2.0) +
    (COALESCE(z.zap_count, 0) * 1.0) +
    (SAFE_DIVIDE(COALESCE(z.total_amount_msats, 0), 1000) * 0.1) AS engagement_score
  FROM note_events n
  LEFT JOIN reactions r ON n.note_id = r.target_note_id
  LEFT JOIN reposts rp ON n.note_id = rp.target_note_id
  LEFT JOIN zaps z ON n.note_id = z.target_note_id
  LEFT JOIN replies re ON n.note_id = re.parent_note_id
)

SELECT
  note_id,
  author_npub,
  author_pubkey,
  published_at,
  published_date,
  content_preview,
  reaction_count,
  unique_reactors,
  repost_count,
  unique_reposters,
  zap_count,
  unique_zappers,
  total_zap_msats,
  ROUND(total_zap_sats, 2) AS total_zap_sats,
  reply_count,
  unique_repliers,
  ROUND(engagement_score, 2) AS engagement_score
FROM engagement
WHERE engagement_score > 0
ORDER BY engagement_score DESC
