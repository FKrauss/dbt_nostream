{{ config(
    materialized='view',
    schema='staging'
) }}

-- Staging model for NIP-53 Live Event metadata events (kind 30311).
-- Service-neutral: captures stream metadata from all live event services.
-- Deduplicated across relays. 90-day lookback via _PARTITIONDATE.

WITH raw AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS event_id,
    JSON_VALUE(payload, '$.author') AS author_pubkey,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    JSON_VALUE(payload, '$.content') AS content,
    JSON_VALUE(payload, '$.relayUrl') AS relay_url,
    JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags')) AS tags
  FROM `replit-gcp.Nostr.events`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 30311
),

deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id ORDER BY created_at DESC, relay_url
    ) AS rn
  FROM raw
  QUALIFY rn = 1
),

tags_extracted AS (
  SELECT
    d.event_id,
    d.author_pubkey,
    d.created_at,
    d.content,
    d.relay_url,

    -- stream identifier (d-tag)
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'd'
             THEN JSON_VALUE(tag, '$[1]') END) AS stream_id,

    -- status
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'status'
             THEN JSON_VALUE(tag, '$[1]') END) AS status,

    -- title / summary
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'title'
             THEN JSON_VALUE(tag, '$[1]') END) AS title,
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'summary'
             THEN JSON_VALUE(tag, '$[1]') END) AS summary,

    -- media
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'image'
             THEN JSON_VALUE(tag, '$[1]') END) AS image_url,
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'thumb'
             THEN JSON_VALUE(tag, '$[1]') END) AS thumb_url,

    -- watch / service / streaming URLs
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'alt'
             THEN JSON_VALUE(tag, '$[1]') END) AS alt_url,
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'service'
             THEN JSON_VALUE(tag, '$[1]') END) AS service_url,
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'streaming'
             THEN JSON_VALUE(tag, '$[1]') END) AS streaming_url,

    -- start / end timestamps (_unix strings; callers can cast to TIMESTAMP)
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'starts'
             THEN JSON_VALUE(tag, '$[1]') END) AS starts_unix,
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'ends'
             THEN JSON_VALUE(tag, '$[1]') END) AS ends_unix,

    -- goal pubkey
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'goal'
             THEN JSON_VALUE(tag, '$[1]') END) AS goal_pubkey,

    -- host pubkey from p-tag with role=host
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'p'
              AND JSON_VALUE(tag, '$[3]') = 'host'
             THEN JSON_VALUE(tag, '$[1]') END) AS host_pubkey,

    -- all hashtags
    ARRAY_AGG(DISTINCT
      CASE WHEN JSON_VALUE(tag, '$[0]') = 't'
           THEN JSON_VALUE(tag, '$[1]') END
      IGNORE NULLS
    ) AS hashtags

  FROM deduped d
  CROSS JOIN UNNEST(d.tags) AS tag
  GROUP BY 1,2,3,4,5
)

SELECT * FROM tags_extracted
