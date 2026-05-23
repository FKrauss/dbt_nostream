{{ config(
    materialized='view',
    schema='staging'
) }}

-- Staging model for Lightning zap receipts (kind 9735) that reference podcasts.
-- Extends the generic zap pattern with podcast episode/show refs.
-- Single-pass on raw events for cost efficiency.

WITH raw AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS event_id,
    JSON_VALUE(payload, '$.npub') AS npub,
    JSON_VALUE(payload, '$.author') AS author_pubkey,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    JSON_VALUE(payload, '$.content') AS content,
    JSON_VALUE(payload, '$.relayUrl') AS relay_url,
    JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags')) AS tags
  FROM `replit-gcp.Nostr.events`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735
    AND EXISTS (
      SELECT 1
      FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
      WHERE JSON_VALUE(tag, '$[0]') = 'i'
        AND JSON_VALUE(tag, '$[1]') LIKE 'podcast:%'
    )
),

deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY created_at DESC, relay_url
    ) AS rn
  FROM raw
  QUALIFY rn = 1
)

SELECT
  d.event_id,
  d.npub,
  d.author_pubkey,
  d.created_at,
  d.content,
  d.relay_url,
  (SELECT REGEXP_EXTRACT(JSON_VALUE(tag, '$[1]'), r'^podcast:item:guid:(.*)$')
   FROM UNNEST(d.tags) AS tag
   WHERE JSON_VALUE(tag, '$[0]') = 'i' AND JSON_VALUE(tag, '$[1]') LIKE 'podcast:item:guid:%'
   LIMIT 1) AS episode_guid,
  (SELECT REGEXP_EXTRACT(JSON_VALUE(tag, '$[1]'), r'^podcast:guid:(.*)$')
   FROM UNNEST(d.tags) AS tag
   WHERE JSON_VALUE(tag, '$[0]') = 'i' AND JSON_VALUE(tag, '$[1]') LIKE 'podcast:guid:%'
   LIMIT 1) AS show_guid,
  (SELECT REGEXP_EXTRACT(JSON_VALUE(tag, '$[1]'), r'^podcast:publisher:guid:(.*)$')
   FROM UNNEST(d.tags) AS tag
   WHERE JSON_VALUE(tag, '$[0]') = 'i' AND JSON_VALUE(tag, '$[1]') LIKE 'podcast:publisher:guid:%'
   LIMIT 1) AS publisher_guid,
  (SELECT JSON_VALUE(tag, '$[1]')
   FROM UNNEST(d.tags) AS tag
   WHERE JSON_VALUE(tag, '$[0]') = 'p'
   LIMIT 1) AS recipient_pubkey,
  (SELECT JSON_VALUE(tag, '$[1]')
   FROM UNNEST(d.tags) AS tag
   WHERE JSON_VALUE(tag, '$[0]') = 'bolt11'
   LIMIT 1) AS bolt11_invoice,
  (SELECT JSON_VALUE(tag, '$[1]')
   FROM UNNEST(d.tags) AS tag
   WHERE JSON_VALUE(tag, '$[0]') = 'description'
   LIMIT 1) AS zap_request_json,
  SAFE_CAST(
    JSON_VALUE(
      (SELECT JSON_VALUE(tag, '$[1]')
       FROM UNNEST(d.tags) AS tag
       WHERE JSON_VALUE(tag, '$[0]') = 'description'
       LIMIT 1),
      '$.tags[1][1]'
    ) AS INT64
  ) AS amount_msats
FROM deduped d
