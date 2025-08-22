
-- models/staging/int_contact_list_latest.sql
{{ config(materialized='view') }}

with base as (
  select
    npub,                  
    id as event_id,
    created_at,
    content,                -- JSON of relay preferences
    tags,                   -- JSON array of Nostr tags
    relayUrl                -- where we saw it
  from {{ ref('stg_nostr_events_all') }}
  where kind = 3
),

ranked as (
  select
    *,
    row_number() over (
      partition by npub
      order by created_at desc, event_id desc
    ) as rn
  from base
)

select
  npub,
  event_id,
  created_at,
  content,
  tags,
  relayUrl
from ranked
where rn = 1
