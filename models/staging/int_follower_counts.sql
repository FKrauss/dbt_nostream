
select
  followed_pubkey_hex as npub_hex,
  count(distinct npub) as follower_count
from {{ ref('fct_follows') }}
where followed_pubkey_hex is not null
group by followed_pubkey_hex
