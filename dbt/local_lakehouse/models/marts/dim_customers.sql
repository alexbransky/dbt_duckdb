{{ config(materialized='table') }}
select
  customer_id,
  customer_sk,
  first_name,
  last_name,
  first_name || ' ' || last_name as full_name,
  email,
  status,
  segment,
  state,
  created_at
from {{ ref('stg_customers') }}
