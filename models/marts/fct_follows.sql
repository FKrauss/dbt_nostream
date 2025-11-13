
with latest as (
  select * from {{ ref('int_state_contact_list_latest') }}
),

tag_rows as (
  select
    npub,
    event_id,
    created_at,
    tag_str
  from latest
  cross join unnest(tags) as tag_str
),

p_tags as (
  select
    npub,
    event_id,
    created_at,
    JSON_VALUE(tag_str, '$[0]') as tag_kind,              -- e.g. 'p'
    JSON_VALUE(tag_str, '$[1]') as followed_pubkey_hex,   -- 64-char hex
    JSON_VALUE(tag_str, '$[2]') as relay_hint             -- optional
  from tag_rows
  -- keep only well-formed "p" tags
  where JSON_VALUE(tag_str, '$[0]') = 'p'
)

select
  npub,
  followed_pubkey_hex,
  relay_hint,
  event_id as source_event_id,
  created_at,
  date(created_at) as created_date
from p_tags
where followed_pubkey_hex is not null