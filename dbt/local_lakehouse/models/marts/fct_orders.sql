{{ config(materialized='table') }}
select
  order_id,
  customer_id,
  min(order_ts_utc) as order_ts_utc,
  min(order_date) as order_date,
  max(order_status) as order_status,
  round(sum(line_total), 2) as order_total
from {{ ref('int_order_lines') }}
group by 1,2
