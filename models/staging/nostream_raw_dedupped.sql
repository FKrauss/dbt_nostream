select
  min(timestamp) as first_seen_at,
  any_value(topic) as topic,
  json_value(payload, '$.id') as event_id,
  any_value(payload) as payload
from `replit-gcp.nostr.events`
where
  topic is not null
  and json_value(payload, '$.id') is not null
  and _partitiontime >= timestamp("2025-07-01")
group by event_id