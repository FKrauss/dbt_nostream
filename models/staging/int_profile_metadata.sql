
with profile_events as (
  select
    npub,
    content,
    created_at,
    id as event_id
  from {{ ref('stg_nostr_events_all') }}
  where kind = 0
),

latest_profiles as (
  select
    npub,
    content,
    created_at,
    event_id,
    row_number() over (partition by npub order by created_at desc) as rn
  from profile_events
),

parsed_profiles as (
  select
    npub,
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
  event_id,
  created_at,
  coalesce(username, '') as username,
  coalesce(description, '') as description,
  display_name,
  picture,
  nip05
from parsed_profiles
