-- Simple test model to verify BigQuery connection
select 
  current_timestamp() as test_timestamp,
  'dbt_nostream_prod' as target_schema,
  'Connection test successful!' as status 