
SELECT   
  JSON_VALUE(payload, '$.author')        AS author_npub,
  JSON_VALUE(payload, '$.content')       AS content,
  TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
  JSON_VALUE(payload, '$.id')            AS id,
  CAST(JSON_VALUE(payload, '$.kind') AS INT64) AS kind,
  JSON_VALUE(payload, '$.npub')          AS npub,
  JSON_VALUE(payload, '$.relayUrl')      AS relayUrl,
  JSON_VALUE(payload, '$.sig')           AS sig,
  JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'), '$') AS tags

FROM `replit-gcp.Nostr.events` e
