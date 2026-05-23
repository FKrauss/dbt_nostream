{{ config(
    materialized='view',
    schema='staging'
) }}

-- Intermediate model: one row per live stream (d-tag).
-- Picks the latest metadata and rolls up counts across all
-- kind 30311 events for that stream. Service-neutral.

WITH latest_event AS (
  SELECT *
  FROM {{ ref('stg_live_events') }}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY stream_id ORDER BY created_at DESC
  ) = 1
),

agg AS (
  SELECT
    stream_id,
    MIN(created_at) AS first_seen_at,
    MAX(created_at) AS last_seen_at,
    COUNT(*) AS event_count,
    COUNTIF(status = 'live') AS live_event_count,
    COUNTIF(status = 'ended') AS ended_event_count,
    ARRAY_AGG(DISTINCT status IGNORE NULLS) AS statuses_seen
  FROM {{ ref('stg_live_events') }}
  GROUP BY stream_id
)

SELECT
  l.stream_id,

  -- latest metadata
  l.title,
  l.summary,
  l.image_url,
  l.thumb_url,
  l.alt_url,
  l.service_url,
  l.streaming_url,
  l.host_pubkey,
  l.goal_pubkey,
  l.hashtags,

  -- temporal rollup
  a.first_seen_at,
  a.last_seen_at,
  a.event_count,
  a.live_event_count,
  a.ended_event_count,
  a.statuses_seen

FROM latest_event l
INNER JOIN agg a ON l.stream_id = a.stream_id
