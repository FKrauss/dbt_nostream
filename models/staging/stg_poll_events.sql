{{ config(
    materialized='view',
    schema='staging'
) }}

-- Staging model for NIP-88 Poll events (kind 1068).
-- Extracts poll question, options, type, and end time from tags.
-- Deduplicated across relays. 90-day lookback via _PARTITIONDATE.

WITH raw AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS poll_event_id,
    JSON_VALUE(payload, '$.npub') AS npub,
    JSON_VALUE(payload, '$.author') AS author_pubkey,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    JSON_VALUE(payload, '$.content') AS poll_question,
    JSON_VALUE(payload, '$.relayUrl') AS relay_url,
    JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags')) AS tags
  FROM `replit-gcp.Nostr.events`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 1068
),

deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY poll_event_id
      ORDER BY created_at DESC, relay_url
    ) AS rn
  FROM raw
  QUALIFY rn = 1
),

tags_extracted AS (
  SELECT
    d.poll_event_id,
    d.npub,
    d.author_pubkey,
    d.created_at,
    d.poll_question,
    d.relay_url,

    -- Poll type (singlechoice, multiplechoice)
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'polltype'
             THEN JSON_VALUE(tag, '$[1]') END) AS poll_type,

    -- End time as timestamp
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'endsAt'
             THEN TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(tag, '$[1]') AS INT64)) END) AS ends_at,

    -- Client that created the poll
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'client'
             THEN JSON_VALUE(tag, '$[1]') END) AS client,

    -- All option tags gathered as an array of structs
    ARRAY_AGG(
      STRUCT(
        SAFE_CAST(JSON_VALUE(tag, '$[1]') AS INT64) AS option_index,
        JSON_VALUE(tag, '$[2]') AS option_text
      )
      ORDER BY SAFE_CAST(JSON_VALUE(tag, '$[1]') AS INT64)
    ) AS poll_options

  FROM deduped d,
  UNNEST(d.tags) AS tag
  WHERE JSON_VALUE(tag, '$[0]') IN ('polltype', 'endsAt', 'client', 'option')
  GROUP BY
    d.poll_event_id,
    d.npub,
    d.author_pubkey,
    d.created_at,
    d.poll_question,
    d.relay_url
)

SELECT
  poll_event_id,
  npub,
  author_pubkey,
  created_at,
  poll_question,
  COALESCE(poll_type, 'unknown') AS poll_type,
  ends_at,
  client,
  poll_options,
  -- Number of available options
  ARRAY_LENGTH(poll_options) AS option_count,
  -- Is the poll still open?
  CASE WHEN ends_at IS NOT NULL AND ends_at > CURRENT_TIMESTAMP() THEN TRUE ELSE FALSE END AS is_open
FROM tags_extracted
ORDER BY created_at DESC
