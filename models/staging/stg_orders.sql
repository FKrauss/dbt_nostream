
-- Placeholder staging model for orders
-- Remove this file if you don't have order data

{{ config(materialized='view') }}

select
    1 as order_id,
    1 as customer_id,
    current_date as order_date

-- Delete this file if not needed for your project
