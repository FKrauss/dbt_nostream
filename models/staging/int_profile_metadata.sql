
with source_events as (
  select
    JSON_VALUE(payload, '$.npub') as npub,
    JSON_VALUE(payload, '$.pubkey') as pubkey_hex,
    JSON_VALUE(payload, '$.content') as content,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) as created_at,
    JSON_VALUE(payload, '$.id') as event_id
  from `replit-gcp.Nostr.events`
  where CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 0
),

latest_profiles as (
  select
    npub,
    pubkey_hex,
    content,
    created_at,
    event_id,
    row_number() over (partition by coalesce(pubkey_hex, npub) order by created_at desc) as rn
  from source_events
),

parsed_profiles as (
  select
    npub,
    pubkey_hex,
    event_id,
    created_at,
    JSON_VALUE(content, '$.name') as username,
    JSON_VALUE(content, '$.about') as description,
    JSON_VALUE(content, '$.display_name') as display_name,
    JSON_VALUE(content, '$.picture') as picture,
    JSON_VALUE(content, '$.nip05') as nip05
  from latest_profiles
  where rn = 1
)

select
  npub,
  pubkey_hex,
  event_id,
  created_at,
  coalesce(username, '') as username,
  coalesce(description, '') as description,
  display_name,
  picture,
  nip05
from parsed_profiles
