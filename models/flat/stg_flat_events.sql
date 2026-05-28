SELECT
  JSON_VALUE(payload, '$.id')            AS id,
  CAST(JSON_VALUE(payload, '$.kind') AS INT64) AS kind,
  JSON_VALUE(payload, '$.npub')          AS npub,
  JSON_VALUE(payload, '$.author')        AS author,
  TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
  JSON_VALUE(payload, '$.content')       AS content,
  JSON_VALUE(payload, '$.relayUrl')      AS relay_url,
  JSON_VALUE(payload, '$.sig')           AS sig,
  JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'), '$') AS tags,
  payload                                AS raw_payload,
  _PARTITIONDATE                         AS ingestion_date

FROM {{ source('nostr_raw', 'events') }}

{% if is_incremental() %}
  WHERE _PARTITIONDATE >= (SELECT COALESCE(MAX(ingestion_date), DATE('1970-01-01')) FROM {{ this }})
{% else %}
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('full_refresh_lookback_days', 90) }} DAY)
{% endif %}

