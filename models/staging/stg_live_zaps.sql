{{ config(
    materialized='view',
    schema='staging'
) }}

-- Staging model for Lightning zap receipts (kind 9735) that target live streams.
-- Joins to stg_live_events on zapped_event_id to filter only live-stream zaps.
-- Service-neutral.

WITH raw AS (
  SELECT
    JSON_VALUE(payload, '$.id') AS event_id,
    JSON_VALUE(payload, '$.author') AS zapper_pubkey,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    JSON_VALUE(payload, '$.relayUrl') AS relay_url,
    JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags')) AS tags
  FROM `replit-gcp.Nostr.events`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735
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

zap_targets AS (
  SELECT
    d.event_id,
    d.zapper_pubkey,
    d.created_at,
    d.relay_url,

    -- e-tag = zapped stream event
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'e'
             THEN JSON_VALUE(tag, '$[1]') END) AS zapped_event_id,

    -- p-tag recipient
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'p'
             THEN JSON_VALUE(tag, '$[1]') END) AS recipient_pubkey,

    -- description = embedded zap request JSON
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'description'
             THEN JSON_VALUE(tag, '$[1]') END) AS zap_request_json,

    -- bolt11 invoice
    MAX(CASE WHEN JSON_VALUE(tag, '$[0]') = 'bolt11'
             THEN JSON_VALUE(tag, '$[1]') END) AS bolt11_invoice

  FROM deduped d
  CROSS JOIN UNNEST(d.tags) AS tag
  GROUP BY 1,2,3,4
)

SELECT
  z.event_id,
  z.zapper_pubkey,
  z.created_at,
  z.relay_url,
  z.zapped_event_id,
  z.recipient_pubkey,
  z.bolt11_invoice,

  SAFE_CAST(
    JSON_VALUE(z.zap_request_json, '$.tags[1][1]')
    AS INT64
  ) AS amount_msats,

  -- stream context from live event
  l.stream_id,
  l.title AS stream_title,
  l.host_pubkey AS stream_host,
  l.service_url AS stream_service_url,
  l.status AS stream_status_at_zap

FROM zap_targets z
INNER JOIN {{ ref('stg_live_events') }} l
  ON z.zapped_event_id = l.event_id
