{{ config(
    materialized='view',
    schema='staging'
) }}

-- Staging model for NIP-88 Poll Response / Vote events (kind 1018).
-- Links votes to their parent poll via the first 'e' tag.
-- Deduplicated across relays. 90-day lookback via _PARTITIONDATE.

WITH raw AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS vote_event_id,
    JSON_VALUE(payload, '$.npub') AS npub,
    JSON_VALUE(payload, '$.author') AS author_pubkey,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    JSON_VALUE(payload, '$.content') AS content,
    JSON_VALUE(payload, '$.relayUrl') AS relay_url,
    JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags')) AS tags
  FROM `replit-gcp.Nostr.events`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 1018
),

deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY vote_event_id
      ORDER BY created_at DESC, relay_url
    ) AS rn
  FROM raw
  QUALIFY rn = 1
),

tags_extracted AS (
  SELECT
    d.vote_event_id,
    d.npub,
    d.author_pubkey,
    d.created_at,
    d.content,
    d.relay_url,

    -- First 'e' tag referencing the poll event
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'e'
             THEN JSON_VALUE(tag, '$[1]') END) AS poll_event_id,

    -- Optional label from 'l' tag
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'l'
             THEN JSON_VALUE(tag, '$[1]') END) AS label,

    -- If the vote embeds the option index in a specific tag
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'response'
             THEN JSON_VALUE(tag, '$[1]') END) AS response_tag_value

  FROM deduped d,
  UNNEST(d.tags) AS tag
  WHERE JSON_VALUE(tag, '$[0]') IN ('e', 'l', 'response')
  GROUP BY
    d.vote_event_id,
    d.npub,
    d.author_pubkey,
    d.created_at,
    d.content,
    d.relay_url
)

SELECT
  vote_event_id,
  npub,
  author_pubkey,
  created_at,
  -- Normalize the chosen option: prefer explicit response tag, fall back to content
  COALESCE(
    response_tag_value,
    SAFE_CAST(TRIM(content) AS STRING)
  ) AS selected_option,
  poll_event_id,
  label,
  relay_url
FROM tags_extracted
WHERE poll_event_id IS NOT NULL
ORDER BY created_at DESC
