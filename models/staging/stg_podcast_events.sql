{{ config(
    materialized='view',
    schema='staging'
) }}

-- Staging model for podcast-referencing Nostr events.
-- Extracts episode, show, and publisher GUIDs from NIP-73 i-tags.
-- Deduplicated across relays (same event can appear on multiple relays).
-- Lookback: 90 days via _PARTITIONDATE for cost control.

WITH raw AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS event_id,
    JSON_VALUE(payload, '$.npub') AS npub,
    JSON_VALUE(payload, '$.author') AS author_pubkey,
    CAST(JSON_VALUE(payload, '$.kind') AS INT64) AS kind,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    JSON_VALUE(payload, '$.content') AS content,
    JSON_VALUE(payload, '$.relayUrl') AS relay_url,
    JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags')) AS tags
  FROM `replit-gcp.Nostr.events`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
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
),

itags AS (
  SELECT
    d.event_id,
    d.npub,
    d.author_pubkey,
    d.kind,
    d.created_at,
    d.content,
    d.relay_url,
    JSON_VALUE(tag, '$[0]') AS tag_key,
    JSON_VALUE(tag, '$[1]') AS tag_value,
    JSON_VALUE(tag, '$[2]') AS tag_url
  FROM deduped d
  CROSS JOIN UNNEST(d.tags) AS tag
  WHERE JSON_VALUE(tag, '$[0]') = 'i'
    AND JSON_VALUE(tag, '$[1]') LIKE 'podcast:%'
)

SELECT
  event_id,
  npub,
  author_pubkey,
  kind,
  created_at,
  content,
  relay_url,
  MAX(CASE WHEN tag_value LIKE 'podcast:item:guid:%'
           THEN REGEXP_EXTRACT(tag_value, r'^podcast:item:guid:(.*)$') END) AS episode_guid,
  MAX(CASE WHEN tag_value LIKE 'podcast:item:guid:%'
           THEN tag_url END) AS episode_url,
  MAX(CASE WHEN tag_value LIKE 'podcast:guid:%'
           THEN REGEXP_EXTRACT(tag_value, r'^podcast:guid:(.*)$') END) AS show_guid,
  MAX(CASE WHEN tag_value LIKE 'podcast:guid:%'
           THEN tag_url END) AS show_url,
  MAX(CASE WHEN tag_value LIKE 'podcast:publisher:guid:%'
           THEN REGEXP_EXTRACT(tag_value, r'^podcast:publisher:guid:(.*)$') END) AS publisher_guid,
  MAX(CASE WHEN tag_value LIKE 'podcast:publisher:guid:%'
           THEN tag_url END) AS publisher_url
FROM itags
GROUP BY event_id, npub, author_pubkey, kind, created_at, content, relay_url
