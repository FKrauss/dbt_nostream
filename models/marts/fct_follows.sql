
-- models/marts/fct_follows.sql
{{ 
  config(
    materialized='table',
    partition_by = {'field': 'created_date', 'data_type': 'date'},
    cluster_by = ['follower_npub', 'followed_pubkey_hex']
  ) 
}}

with latest as (
  select * from {{ ref('int_contact_list_latest') }}
),

-- turn tags JSON into one row per tag
tag_rows as (
  select
    npub as follower_npub,
    event_id,
    created_at,
    t as tag_json
  from latest
  cross join unnest( JSON_QUERY_ARRAY(tags) ) as t
),

-- keep only "p" tags, pull values by index
p_tags as (
  select
    follower_npub,
    event_id,
    created_at,
    JSON_VALUE(tag_json, '$[0]') as tag_kind,
    JSON_VALUE(tag_json, '$[1]') as followed_pubkey_hex,   -- 64‑char hex
    JSON_VALUE(tag_json, '$[2]') as relay_hint             -- optional relay hint
  from tag_rows
  where JSON_VALUE(tag_json, '$[0]') = 'p'
)

select
  follower_npub,
  followed_pubkey_hex,
  relay_hint,
  event_id as source_event_id,
  created_at,
  date(created_at) as created_date
from p_tags
where followed_pubkey_hex is not null
