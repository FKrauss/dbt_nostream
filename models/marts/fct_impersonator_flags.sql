
{{
  config(
    materialized='table'
  )
}}

with profile_freq as (
  select * from {{ ref('int_profile_frequency') }}
),

thresholds as (
  select
    {{ var('username_frequency_threshold', 5) }} as username_freq_threshold,
    {{ var('description_frequency_threshold', 3) }} as description_freq_threshold,
    {{ var('follower_ratio_threshold', 0.10) }} as follower_ratio_threshold
),

flagged_profiles as (
  select
    p.*,
    t.username_freq_threshold,
    t.description_freq_threshold,
    t.follower_ratio_threshold,
    
    case 
      when p.max_followers_with_username > 0 
      then p.follower_count / p.max_followers_with_username 
      else 1.0 
    end as username_follower_ratio,
    
    case 
      when p.max_followers_with_description > 0 
      then p.follower_count / p.max_followers_with_description 
      else 1.0 
    end as description_follower_ratio,
    
    case 
      when p.username_frequency >= t.username_freq_threshold 
        and p.follower_count < (p.max_followers_with_username * t.follower_ratio_threshold)
        and p.username != ''
      then true 
      else false 
    end as is_username_impersonator,
    
    case 
      when p.description_frequency >= t.description_freq_threshold 
        and p.follower_count < (p.max_followers_with_description * t.follower_ratio_threshold)
        and p.description != ''
      then true 
      else false 
    end as is_description_impersonator
    
  from profile_freq p
  cross join thresholds t
),

risk_scoring as (
  select
    *,
    case 
      when is_username_impersonator or is_description_impersonator then
        (username_frequency * 1.0 / nullif(max_followers_with_username, 0)) * 0.5 +
        (description_frequency * 1.0 / nullif(max_followers_with_description, 0)) * 0.3 +
        (1 - username_follower_ratio) * 0.2
      else 0
    end as risk_score
  from flagged_profiles
)

select
  npub,
  username,
  description,
  follower_count,
  username_frequency,
  description_frequency,
  max_followers_with_username,
  max_followers_with_description,
  username_follower_ratio,
  description_follower_ratio,
  is_username_impersonator,
  is_description_impersonator,
  (is_username_impersonator or is_description_impersonator) as is_potential_impersonator,
  round(risk_score, 4) as risk_score
from risk_scoring
order by risk_score desc
