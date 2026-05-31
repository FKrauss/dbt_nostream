-- Hashtag & Zap Analysis for Nostr Notes (kind=1) + Zap Events (kind=9735)
-- Date range: May 23–30, 2026
-- Extracts hashtags from NIP-12 "t" tags and inline #hashtag text in content.
-- Joins with zap events to compute total zap sats per hashtag.
-- NOTE: stg_zap_events.zapper_pubkey is currently NULL due to a column-name mismatch
--       in the raw events (key is 'npub' not 'pubkey'). This query pulls zapper from
--       stg_flat_events directly to work around the issue until the model is rebuilt.

WITH note_hashtags AS (
  -- Hashtags from NIP-12 tags (type "t")
  SELECT
    id        AS note_id,
    LOWER(TRIM(JSON_VALUE(t, '$[1]'))) AS hashtag
  FROM `replit-gcp.Nostr.stg_flat_events`,
  UNNEST(tags) AS t
  WHERE kind = 1
    AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND JSON_VALUE(t, '$[0]') = 't'

  UNION DISTINCT

  -- Inline hashtags extracted from content text (#word pattern)
  SELECT
    id        AS note_id,
    LOWER(TRIM(h)) AS hashtag
  FROM `replit-gcp.Nostr.stg_flat_events`,
  UNNEST(REGEXP_EXTRACT_ALL(content, r'#([A-Za-z0-9_\p{L}]+)')) AS h
  WHERE kind = 1
    AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND REGEXP_CONTAINS(content, r'#([A-Za-z0-9_\p{L}]+)')
),

zaps AS (
  -- Pull zap amount from the description tag JSON (zap request payload).
  SELECT
    id            AS zap_id,
    npub          AS zapper,
    JSON_VALUE(t, '$[1]') AS zapped_note_id,
    SAFE_CAST(
      JSON_VALUE(
        JSON_QUERY(
          SAFE.PARSE_JSON(
            (SELECT JSON_VALUE(t2, '$[1]')
             FROM UNNEST(tags) AS t2
             WHERE JSON_VALUE(t2, '$[0]') = 'description'
             LIMIT 1)
          ),
          '$.tags[1][1]'
        )
      ) AS INT64
    ) AS amount_msats
  FROM `replit-gcp.Nostr.stg_flat_events`,
  UNNEST(tags) AS t
  WHERE kind = 9735
    AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND JSON_VALUE(t, '$[0]') = 'e'
),

joined AS (
  SELECT
    h.hashtag,
    h.note_id        AS zapped_note_id,
    z.zapper,
    z.amount_msats
  FROM note_hashtags h
  INNER JOIN zaps z ON h.note_id = z.zapped_note_id
  WHERE z.amount_msats IS NOT NULL
),

hashtag_summary AS (
  SELECT
    hashtag,
    COUNT(DISTINCT zapped_note_id) AS zapped_note_count,
    COUNT(*)                       AS zap_count,
    COUNT(DISTINCT zapper)         AS unique_zappers,
    ROUND(SUM(amount_msats/1000), 2)     AS total_zap_sats,
    ROUND(AVG(amount_msats/1000), 2)       AS avg_zap_sats
  FROM joined
  GROUP BY hashtag
)

-- Top 30 hashtags by total zap sats
SELECT
  ROW_NUMBER() OVER (ORDER BY total_zap_sats DESC) AS rank,
  hashtag,
  zapped_note_count,
  zap_count,
  unique_zappers,
  total_zap_sats,
  avg_zap_sats
FROM hashtag_summary
ORDER BY total_zap_sats DESC
LIMIT 30
