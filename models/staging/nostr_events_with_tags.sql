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
  -- Extract URL from tags (looking for 'r' tag)
  regexp_extract(json_value(payload, '$.tags'), r'\["r","([^"]+)"\]') as url,
  -- Extract image metadata
  regexp_extract(json_value(payload, '$.tags'), r'"size (\d+)"') as image_size,
  regexp_extract(json_value(payload, '$.tags'), r'"m ([^"]+)"') as mime_type,
  regexp_extract(json_value(payload, '$.tags'), r'"dim (\d+x\d+)"') as image_dimensions,
  regexp_extract(json_value(payload, '$.tags'), r'"alt "([^"]*)"') as alt_text,
  -- Extract blurhash if present
  regexp_extract(json_value(payload, '$.tags'), r'"blurhash ([^"]+)"') as blurhash,
  -- Keep original payload for reference
  payload as original_payload
from {{ ref('nostream_raw_dedupped') }}
where payload is not null 