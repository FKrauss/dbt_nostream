
SELECT 
  CAST(kind AS INT64) as kind,
  name,
  description,
  defined_in
FROM `replit-gcp.Nostr.seed_kind_meta`
LIMIT 1000;
