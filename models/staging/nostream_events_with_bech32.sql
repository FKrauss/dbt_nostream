
{{ config(materialized='view') }}

-- Apply bech32 encoding to event IDs from the raw deduped data
select
  first_seen_at,
  topic,
  event_id,
  {{ bech32_encode('event_id') }} as event_id_bech32,
  payload
from {{ ref('nostream_raw_dedupped') }}
