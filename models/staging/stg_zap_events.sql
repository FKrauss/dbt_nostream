{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

-- Staging model for nostr zap events (kind 9735)
-- Parses Lightning zap receipts to extract sender, recipient, amount, and target note

SELECT
  -- Event identifiers
  JSON_VALUE(payload, '$.id') AS event_id,
  JSON_VALUE(payload, '$.pubkey') AS zapper_pubkey,
  TIMESTAMP_SECONDS(CAST(JSON_VALUE(payload, '$.created_at') AS INT64)) AS created_at,
  
  -- Zap target (the note or user being zapped)
  -- Tag format: ["e", <event_id>, <relay_url>, <marker>]
  JSON_VALUE(payload, '$.tags[0][1]') AS zapped_event_id,
  JSON_VALUE(payload, '$.tags[0][2]') AS zapped_relay_url,
  
  -- Recipient pubkey (tag "p")
  JSON_VALUE(payload, '$.tags[1][1]') AS recipient_pubkey,
  
  -- Zap request reference (tag "P" - the zap request event id)
  JSON_VALUE(payload, '$.tags[2][1]') AS zap_request_event_id,
  
  -- Zap amount in millisatoshis (tag "amount")
  CAST(JSON_VALUE(payload, '$.tags[3][1]') AS INT64) AS amount_msats,
  
  -- Convert to satoshis for readability
  CAST(CAST(JSON_VALUE(payload, '$.tags[3][1]') AS INT64) / 1000 AS INT64) AS amount_sats,
  
  -- Bolt11 invoice (if present in description tag, usually tag 5)
  JSON_VALUE(payload, '$.tags[5][1]') AS bolt11_invoice,
  
  -- Description (usually contains the zap request JSON)
  JSON_VALUE(payload, '$.content') AS description,
  
  -- Original payload for debugging
  payload AS raw_payload

FROM `replit-gcp.Nostr.events`
WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND JSON_VALUE(payload, '$.kind') = '9735'