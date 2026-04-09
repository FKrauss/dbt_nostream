# New Models Added - April 9, 2026

## Summary
Added two high-value analytics models to the dbt_nostream project based on the research document priorities.

## New Models

### 1. `fct_user_activity_metrics` ⭐
**Location:** `models/marts/fct_user_activity_metrics.sql`

**Purpose:** Daily activity metrics aggregated per user - combines multiple event types into a single view for analyzing user engagement and activity patterns.

**Key Features:**
- Tracks 6 activity types per user per day:
  - Notes published (kind 1)
  - Reactions sent (kind 7)
  - Reposts made (kind 6)
  - Zaps sent (kind 9735)
  - Profile updates (kind 0)
  - Contact list updates (kind 3)
- Weighted activity score: zaps=5pts, reposts=3pts, notes=2pts, reactions/profile/contact updates=1pt each
- Enables DAU/WAU/MAU calculations
- Useful for power user identification and user segmentation

**Use Cases:**
- Identify most active users on the network
- Track user engagement trends over time
- Calculate user-level KPIs
- Cohort analysis and user segmentation
- Detect changes in user activity patterns

---

### 2. `fct_content_engagement` ⭐
**Location:** `models/marts/fct_content_engagement.sql`

**Purpose:** Per-note engagement metrics aggregating reactions, reposts, zaps, and replies for comprehensive content performance analysis.

**Key Features:**
- Tracks engagement per note:
  - Reaction count and unique reactors
  - Repost count and unique reposters
  - Zap count, unique zappers, and total zap amount (sats)
  - Reply count and unique repliers
- Composite engagement score: reposts=3pts, replies=2pts, reactions/zaps=1pt + 0.1pt per 100 sats
- Content preview (first 280 chars)
- Sorted by engagement score (highest first)

**Use Cases:**
- Identify trending and viral content
- Find top-performing authors
- Analyze content performance patterns
- Content recommendation and discovery
- Track which types of content resonate most

---

## Schema Documentation
Updated `models/marts/schema.yml` with:
- Detailed descriptions for both models
- Column-level documentation for all fields
- Test definitions (not_null) for key columns:
  - `fct_user_activity_metrics.npub`
  - `fct_user_activity_metrics.activity_date`
  - `fct_content_engagement.note_id`
  - `fct_content_engagement.published_at`

---

## Testing
All models tested successfully:
```bash
dbt run --select fct_user_activity_metrics fct_content_engagement
dbt test --select fct_user_activity_metrics fct_content_engagement
```

**Results:**
- ✅ 2 models created (CREATE VIEW)
- ✅ 4/4 tests passed (not_null constraints)

---

## Example Queries

### Top 10 Most Active Users (Last 7 Days)
```sql
SELECT 
  npub,
  SUM(activity_score) as total_activity_score,
  SUM(notes_published) as total_notes,
  SUM(zaps_sent_count) as total_zaps
FROM `replit-gcp.dbt_nostream_stage.fct_user_activity_metrics`
WHERE activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY npub
ORDER BY total_activity_score DESC
LIMIT 10
```

### Trending Notes (Last 24 Hours)
```sql
SELECT 
  note_id,
  author_npub,
  content_preview,
  engagement_score,
  reaction_count,
  repost_count,
  zap_count,
  total_zap_sats
FROM `replit-gcp.dbt_nostream_stage.fct_content_engagement`
WHERE published_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY engagement_score DESC
LIMIT 20
```

### Daily Active Users Trend (Last 30 Days)
```sql
SELECT 
  activity_date,
  COUNT(DISTINCT npub) as dau
FROM `replit-gcp.dbt_nostream_stage.fct_user_activity_metrics`
WHERE activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY activity_date
ORDER BY activity_date DESC
```

---

## Implementation Notes

### Data Sources
Both models query raw data from `replit-gcp.Nostr.events` with:
- Date filtering: Last 90 days for performance
- Event kind filtering for specific event types

### Performance Considerations
- Materialized as views for fresh data
- Use DATE partitioning in WHERE clauses
- COALESCE for NULL handling
- SAFE_DIVIDE to prevent division by zero errors

### Dependencies
- `fct_user_activity_metrics`: Direct from raw events, no model dependencies
- `fct_content_engagement`: Direct from raw events, no model dependencies

---

## Next Steps (Future Enhancements)
Based on the research document, potential future models:
1. `fct_zap_flows` - Lightning payment flow analysis
2. `fct_social_graph` - Follower/following relationships over time
3. `fct_hashtag_trends` - Trending hashtags and topics
4. `int_user_influence_score` - Composite influence metrics

---

**Commit:** 985c5ec  
**Date:** 2026-04-09  
**Pushed to:** https://github.com/FKrauss/dbt_nostream
