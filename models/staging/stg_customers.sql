
-- Placeholder staging model for customers
-- Replace with your actual customer staging logic

{{ config(materialized='view') }}

select
    1 as customer_id,
    'placeholder' as customer_name
    
-- Remove this file if you don't have customer data
