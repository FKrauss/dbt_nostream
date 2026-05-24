{{ config(
    materialized='view',
    schema='marts'
) }}

-- Daily poll metrics mart.
-- Combines poll creation metadata with aggregated vote counts and unique voter metrics.
-- One row per poll. Votes counted from stg_poll_votes.

WITH polls AS (
  SELECT
    poll_event_id,
    npub AS author_npub,
    author_pubkey,
    created_at AS poll_created_at,
    poll_question,
    poll_type,
    ends_at,
    client,
    option_count,
    is_open
  FROM {{ ref('stg_poll_events') }}
),

votes AS (
  SELECT
    poll_event_id,
    DATE(created_at) AS vote_date,
    COUNT(DISTINCT vote_event_id) AS vote_count,
    COUNT(DISTINCT author_pubkey) AS unique_voters,
    ARRAY_AGG(DISTINCT selected_option IGNORE NULLS) AS selected_options
  FROM {{ ref('stg_poll_votes') }}
  GROUP BY poll_event_id, DATE(created_at)
),

vote_totals AS (
  SELECT
    poll_event_id,
    COUNT(DISTINCT vote_event_id) AS total_votes,
    COUNT(DISTINCT author_pubkey) AS total_unique_voters
  FROM {{ ref('stg_poll_votes') }}
  GROUP BY poll_event_id
)

SELECT
  p.poll_event_id,
  p.author_npub,
  p.author_pubkey,
  p.poll_created_at,
  p.poll_question,
  p.poll_type,
  p.ends_at,
  p.client,
  p.option_count,
  p.is_open,
  COALESCE(vt.total_votes, 0) AS total_votes,
  COALESCE(vt.total_unique_voters, 0) AS total_unique_voters,
  -- Latest vote date to surface activity
  MAX(v.vote_date) AS latest_vote_date
FROM polls p
LEFT JOIN vote_totals vt ON vt.poll_event_id = p.poll_event_id
LEFT JOIN votes v ON v.poll_event_id = p.poll_event_id
GROUP BY
  p.poll_event_id,
  p.author_npub,
  p.author_pubkey,
  p.poll_created_at,
  p.poll_question,
  p.poll_type,
  p.ends_at,
  p.client,
  p.option_count,
  p.is_open,
  vt.total_votes,
  vt.total_unique_voters
ORDER BY p.poll_created_at DESC
