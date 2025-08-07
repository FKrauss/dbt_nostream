
SELECT
  MIN(Timestamp) AS first_seen_at,
  ANY_VALUE(topic) AS topic,
  JSON_VALUE(Payload, '$.id') AS event_id,
  ANY_VALUE(Payload) AS Payload
FROM `replit-gcp.Nostr.events`
WHERE
  topic IS NOT NULL
  AND JSON_VALUE(Payload, '$.id') IS NOT NULL
  AND _PARTITIONTIME >= TIMESTAMP("2025-07-01")  -- or dynamic range
GROUP BY event_id
