-- Hashtag Zap Network Edges (hashtag → zapped_note → zapper → amount)
-- Date range: May 23–30, 2026
-- Output: one row per (hashtag, zapped_note, zapper, amount) for graph analysis.
-- NOTE: Uses stg_flat_events directly for zapper (see stg_zap_events column-name bug).

WITH note_hashtags AS (
  SELECT id AS note_id,
    LOWER(TRIM(JSON_VALUE(t, '$[1]'))) AS hashtag
  FROM `replit-gcp.Nostr.stg_flat_events`, UNNEST(tags) AS t
  WHERE kind = 1 AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND JSON_VALUE(t, '$[0]') = 't'

  UNION DISTINCT

  SELECT id AS note_id, LOWER(TRIM(h)) AS hashtag
  FROM `replit-gcp.Nostr.stg_flat_events`,
  UNNEST(REGEXP_EXTRACT_ALL(content, r'#([A-Za-z0-9_\p{L}]+)')) AS h
  WHERE kind = 1 AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND REGEXP_CONTAINS(content, r'#([A-Za-z0-9_\p{L}]+)')
),

zaps AS (
  SELECT
    id AS zap_id,
    npub AS zapper,
    JSON_VALUE(t, '$[1]') AS zapped_note_id,
    SAFE_CAST(
      JSON_VALUE(
        JSON_QUERY(
          SAFE.PARSE_JSON(
            (SELECT JSON_VALUE(t2, '$[1]') FROM UNNEST(tags) AS t2 WHERE JSON_VALUE(t2, '$[0]') = 'description' LIMIT 1)
          ), '$.tags[1][1]'
        )
      ) AS INT64
    ) AS amount_msats
  FROM `replit-gcp.Nostr.stg_flat_events`, UNNEST(tags) AS t
  WHERE kind = 9735 AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND JSON_VALUE(t, '$[0]') = 'e'
)

SELECT
  h.hashtag,
  z.zapped_note_id,
  z.zapper,
  z.zap_id,
  SAFE_DIVIDE(z.amount_msats, 1000) AS amount_sats
FROM note_hashtags h
INNER JOIN zaps z ON h.note_id = z.zapped_note_id
WHERE z.amount_msats IS NOT NULL
  AND h.hashtag IS NOT NULL
  AND LENGTH(h.hashtag) > 0
  AND z.zapped_note_id IS NOT NULL
