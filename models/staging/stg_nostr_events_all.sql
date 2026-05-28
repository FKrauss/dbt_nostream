SELECT
  id,
  kind,
  npub,
  author,
  created_at,
  content,
  relay_url,
  sig,
  tags,
  raw_payload,
  ingestion_date

FROM {{ ref('stg_flat_events') }}

WHERE ingestion_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days', 3) }} DAY)

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC) = 1