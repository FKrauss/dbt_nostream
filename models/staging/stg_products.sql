-- Placeholder staging model for products
-- Remove this file if you don't have product data

{{ config(materialized='view') }}

select
    1 as product_id,
    'placeholder' as product_name

-- Delete this file if not needed for your project