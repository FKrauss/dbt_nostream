{{
  config(
    materialized = 'view',
    schema = 'marts'
  )
}}

-- Unfollow tracking: detects when a pubkey is removed from someone's contact list
-- Compares consecutive contact list events (kind 3) per user to find removed follows

WITH contact_list_events AS (
  -- Get all contact list events with their follows
  SELECT
    JSON_VALUE(payload, '$.npub') AS follower_npub,
    JSON_VALUE(payload, '$.id') AS event_id,
    TIMESTAMP(JSON_VALUE(payload, '$.createdAt')) AS created_at,
    DATE(TIMESTAMP(JSON_VALUE(payload, '$.createdAt'))) AS event_date,
    -- Extract all p tags (followed pubkeys) from this event
    ARRAY(
      SELECT JSON_VALUE(tag, '$[1]')
      FROM UNNEST(JSON_EXTRACT_ARRAY(JSON_VALUE(payload, '$.tags'))) AS tag
      WHERE JSON_VALUE(tag, '$[0]') = 'p'
    ) AS followed_pubkeys
  FROM `replit-gcp.Nostr.events`
  WHERE DATE(Timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND CAST(JSON_VALUE(payload, '$.kind') AS INT64) = 3
),

ranked_events AS (
  -- Rank contact list events per user by time
  SELECT
    *,
    LAG(followed_pubkeys) OVER (
      PARTITION BY follower_npub 
      ORDER BY created_at ASC, event_id ASC
    ) AS prev_followed_pubkeys,
    LAG(event_date) OVER (
      PARTITION BY follower_npub 
      ORDER BY created_at ASC, event_id ASC
    ) AS prev_event_date
  FROM contact_list_events
),

unfollows AS (
  -- Find pubkeys that were in previous list but not in current
  SELECT
    follower_npub,
    event_date,
    -- Unnested array of pubkeys that were removed
    unfollowed_pubkey
  FROM ranked_events,
  UNNEST(
    -- Array difference: prev list minus current list
    IFNULL(
      ARRAY(
        SELECT p FROM UNNEST(prev_followed_pubkeys) AS p
        WHERE p NOT IN UNNEST(followed_pubkeys)
      ),
      []
    )
  ) AS unfollowed_pubkey
  WHERE prev_followed_pubkeys IS NOT NULL  -- Skip first event (no previous to compare)
)

-- Aggregate: count unfollows per date per pubkey
SELECT
  event_date AS date,
  unfollowed_pubkey AS npub,
  COUNT(*) AS unfollows,
  ARRAY_AGG(DISTINCT follower_npub) AS unfollowed_by  -- Who unfollowed them
FROM unfollows
WHERE unfollowed_pubkey IS NOT NULL
GROUP BY event_date, unfollowed_pubkey
ORDER BY date DESC, unfollows DESC