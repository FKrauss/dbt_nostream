-- Placeholder mart model for customers
-- Replace with your actual customer business logic

{{ config(materialized='table') }}

select
    customer_id,
    customer_name
from {{ ref('stg_customers') }}

-- Update this with your actual customer mart logic