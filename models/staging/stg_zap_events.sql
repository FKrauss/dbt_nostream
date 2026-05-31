{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

-- Staging model for nostr zap events (kind 9735)
-- Parses Lightning zap receipts to extract sender, recipient, amount, and target note

SELECT
  -- Event identifiers from the main payload
  JSON_VALUE(payload, '$.id') AS event_id,
  JSON_VALUE(payload, '$.npub') AS zapper_pubkey,
  TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
  
  -- Extract tags by type using UNNEST
  -- Tag "p" = recipient pubkey (who got zapped)
  (SELECT JSON_VALUE(tag, '$[1]') 
   FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag 
   WHERE JSON_VALUE(tag, '$[0]') = 'p' 
   LIMIT 1) AS recipient_pubkey,
  
  -- Tag "P" = zap request event ID
  (SELECT JSON_VALUE(tag, '$[1]') 
   FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag 
   WHERE JSON_VALUE(tag, '$[0]') = 'P' 
   LIMIT 1) AS zap_request_event_id,
  
  -- Tag "e" = zapped note/event ID
  (SELECT JSON_VALUE(tag, '$[1]') 
   FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag 
   WHERE JSON_VALUE(tag, '$[0]') = 'e' 
   LIMIT 1) AS zapped_event_id,
  
  -- Tag "bolt11" = Lightning invoice
  (SELECT JSON_VALUE(tag, '$[1]') 
   FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag 
   WHERE JSON_VALUE(tag, '$[0]') = 'bolt11' 
   LIMIT 1) AS bolt11_invoice,
  
  -- Tag "description" = JSON string containing zap request with amount
  (SELECT JSON_VALUE(tag, '$[1]') 
   FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag 
   WHERE JSON_VALUE(tag, '$[0]') = 'description' 
   LIMIT 1) AS zap_request_json,
  
  -- Extract amount from the zap request description JSON
  -- The description contains: {"tags":[["amount","11000"],...]}
  SAFE_CAST(
    JSON_VALUE(
      (SELECT JSON_VALUE(tag, '$[1]') 
       FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag 
       WHERE JSON_VALUE(tag, '$[0]') = 'description' 
       LIMIT 1),
      '$.tags[1][1]'
    ) AS INT64
  ) AS amount_msats,
  
  -- Content field (usually empty for zaps)
  JSON_VALUE(payload, '$.content') AS content,
  
  -- Original payload for debugging
  payload AS raw_payload

FROM `replit-gcp.Nostr.events`
WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 9735