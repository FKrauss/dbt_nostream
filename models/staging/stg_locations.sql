-- Placeholder staging model for locations
-- Remove this file if you don't have location data

{{ config(materialized='view') }}

select
    1 as location_id,
    'placeholder' as location_name

-- Delete this file if not needed for your project