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

FROM {{ source('nostr_flat', 'events_flat') }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC) = 1
