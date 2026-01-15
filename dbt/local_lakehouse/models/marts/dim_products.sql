{{ config(materialized='table') }}
select
  product_id,
  product_name,
  category,
  price,
  is_active
from {{ ref('stg_products') }}
