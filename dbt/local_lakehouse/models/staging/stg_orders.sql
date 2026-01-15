select
  cast(order_id as integer) as order_id,
  cast(customer_id as integer) as customer_id,
  cast(order_ts_utc as timestamp) as order_ts_utc,
  cast(order_ts_utc as date) as order_date,
  status
from {{ ref('bronze_orders') }}
