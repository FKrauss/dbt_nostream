
SELECT   
  JSON_VALUE(payload, '$.author')        AS author_npub,
  JSON_VALUE(payload, '$.content')       AS content,
  TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
  JSON_VALUE(payload, '$.id')            AS id,
  CAST(JSON_VALUE(payload, '$.kind') AS INT64) AS kind,
  JSON_VALUE(payload, '$.npub')          AS npub,
  JSON_VALUE(payload, '$.relayUrl')      AS relayUrl,
  JSON_VALUE(payload, '$.sig')           AS sig,
  JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'), '$') AS tags,
  km.name as kind_name

FROM `replit-gcp.Nostr.events` e
LEFT JOIN {{ ref('kind_meta_view') }} km ON CAST(JSON_VALUE(e.payload, '$.kind') AS INT64) = km.kind
