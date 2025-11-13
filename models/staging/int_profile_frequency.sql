
with profile_data as (
  select
    npub,
    username,
    description
  from {{ ref('int_profile_metadata') }}
),

follower_data as (
  select
    npub_hex,
    follower_count
  from {{ ref('int_follower_counts') }}
),

profile_with_followers as (
  select
    p.npub,
    p.username,
    p.description,
    coalesce(f.follower_count, 0) as follower_count
  from profile_data p
  left join follower_data f
    on p.npub = f.npub_hex
),

username_stats as (
  select
    username,
    count(*) as username_frequency,
    max(follower_count) as max_followers_with_username
  from profile_with_followers
  where username != ''
  group by username
),

description_stats as (
  select
    description,
    count(*) as description_frequency,
    max(follower_count) as max_followers_with_description
  from profile_with_followers
  where description != ''
  group by description
)

select
  p.npub,
  p.username,
  p.description,
  p.follower_count,
  coalesce(u.username_frequency, 1) as username_frequency,
  coalesce(u.max_followers_with_username, p.follower_count) as max_followers_with_username,
  coalesce(d.description_frequency, 1) as description_frequency,
  coalesce(d.max_followers_with_description, p.follower_count) as max_followers_with_description
from profile_with_followers p
left join username_stats u
  on p.username = u.username
left join description_stats d
  on p.description = d.description
