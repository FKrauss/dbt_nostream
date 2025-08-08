with parsed_events as (
  select
    first_seen_at,
    topic,
    event_id,
    -- Extract JSON fields from payload
    json_value(payload, '$.accountId') as account_id,
    json_value(payload, '$.author') as author,
    json_value(payload, '$.content') as content,
    json_value(payload, '$.createdAt') as created_at,
    json_value(payload, '$.id') as nostr_id,
    cast(json_value(payload, '$.kind') as int64) as kind,
    json_value(payload, '$.relayUrl') as relay_url,
    json_value(payload, '$.sig') as signature,
    json_value(payload, '$.tags') as tags_raw,
    -- Keep original payload for reference
    payload as original_payload
  from {{ ref('nostream_raw_dedupped') }}
  where payload is not null
),
tag_extractions as (
  select
    *,
    -- Extract URL from tags (looking for 'r' tag)
    regexp_extract(tags_raw, r'\["r","([^"]+)"\]') as url,
    -- Extract image metadata
    regexp_extract(tags_raw, r'"size (\d+)"') as image_size,
    regexp_extract(tags_raw, r'"m ([^"]+)"') as mime_type,
    regexp_extract(tags_raw, r'"dim (\d+x\d+)"') as image_dimensions,
    regexp_extract(tags_raw, r'"alt "([^"]*)"') as alt_text,
    -- Extract blurhash if present
    regexp_extract(tags_raw, r'"blurhash ([^"]+)"') as blurhash
  from parsed_events
)
select
  first_seen_at,
  topic,
  event_id,
  account_id,
  author,
  content,
  created_at,
  nostr_id,
  kind,
  relay_url,
  signature,
  -- Extracted tag information
  url,
  cast(image_size as int64) as image_size_bytes,
  mime_type,
  image_dimensions,
  alt_text,
  blurhash,
  -- Original data
  tags_raw,
  original_payload
from tag_extractions 