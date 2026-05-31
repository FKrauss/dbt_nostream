-- Link Analysis for Nostr Notes (kind=1)
-- Date range: May 23–30, 2026
-- Extracts URLs from content text (regex) and from tags (type "r"),
-- outputs top 20 domains, blossom-server share, and daily URL counts.

WITH urls_from_tags AS (
  SELECT ingestion_date, JSON_VALUE(t, '$[1]') AS raw_url
  FROM `replit-gcp.Nostr.stg_flat_events`, UNNEST(tags) AS t
  WHERE kind = 1
    AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND JSON_VALUE(t, '$[0]') = 'r'
),

urls_from_content AS (
  SELECT ingestion_date, url AS raw_url
  FROM `replit-gcp.Nostr.stg_flat_events`,
  UNNEST(REGEXP_EXTRACT_ALL(content, r'https?://[^[:space:]]+')) AS url
  WHERE kind = 1
    AND ingestion_date BETWEEN '2026-05-23' AND '2026-05-30'
    AND REGEXP_CONTAINS(content, r'https?://')
),

all_urls AS (
  SELECT ingestion_date, raw_url FROM urls_from_tags
  UNION ALL
  SELECT ingestion_date, raw_url FROM urls_from_content
),

cleaned AS (
  SELECT ingestion_date, raw_url, LOWER(NET.HOST(raw_url)) AS domain
  FROM all_urls
  WHERE raw_url IS NOT NULL AND LENGTH(raw_url) > 4 AND STARTS_WITH(LOWER(raw_url), 'http')
),

domain_counts AS (
  SELECT domain, COUNT(*) AS url_frequency
  FROM cleaned
  WHERE domain IS NOT NULL AND LENGTH(domain) > 0
  GROUP BY domain
),

daily_counts AS (
  SELECT ingestion_date, COUNT(*) AS url_count FROM cleaned GROUP BY ingestion_date ORDER BY ingestion_date
),

blossom_stats AS (
  SELECT
    COUNTIF(REGEXP_CONTAINS(domain, r'blossom')) AS blossom_count,
    COUNT(*) AS total_domains
  FROM cleaned WHERE domain IS NOT NULL AND LENGTH(domain) > 0
)

-- (a) Top 20 domains by frequency
SELECT
  ROW_NUMBER() OVER (ORDER BY url_frequency DESC) AS rank,
  domain,
  url_frequency
FROM domain_counts
ORDER BY url_frequency DESC
LIMIT 20
;

-- (b) Blossom server share
-- SELECT COUNTIF(REGEXP_CONTAINS(domain, r'blossom')) AS blossom_count,
--        COUNT(*) AS total_domains,
--        ROUND(100.0 * COUNTIF(REGEXP_CONTAINS(domain, r'blossom')) / COUNT(*), 2) AS blossom_pct
-- FROM cleaned WHERE domain IS NOT NULL AND LENGTH(domain) > 0;

-- (c) URL count per day
-- SELECT ingestion_date, url_count FROM daily_counts ORDER BY ingestion_date;
