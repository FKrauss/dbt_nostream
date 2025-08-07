-- Placeholder staging model for supplies
-- Remove this file if you don't have supply data

{{ config(materialized='view') }}

select
    1 as supply_id,
    'placeholder' as supply_name

-- Delete this file if not needed for your project