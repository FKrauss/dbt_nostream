{{ config(
    materialized='view',
    schema='staging'
) }}

-- Episode/show dimension derived from podcast-referencing events.
-- Maps episode GUIDs to the most common show, publisher, and URLs.
-- Only includes episodes seen in the 90-day lookback of stg_podcast_events.

WITH refs AS (
  SELECT *
  FROM {{ ref('stg_podcast_events') }}
  WHERE episode_guid IS NOT NULL
)

SELECT
  episode_guid,
  MAX(show_guid) AS show_guid,
  MAX(publisher_guid) AS publisher_guid,
  MAX(episode_url) AS episode_url,
  MAX(show_url) AS show_url,
  MIN(created_at) AS first_seen_at,
  MAX(created_at) AS last_seen_at,
  COUNT(DISTINCT event_id) AS total_event_count,
  COUNTIF(kind = 1) AS share_count,
  COUNTIF(kind = 9735) AS zap_count,
  COUNTIF(kind = 1111) AS comment_count,
  COUNT(DISTINCT npub) AS unique_interactors
FROM refs
GROUP BY episode_guid
