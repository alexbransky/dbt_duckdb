select
  oi.order_id,
  o.customer_id,
  o.order_ts_utc,
  o.order_date,
  o.status as order_status,
  oi.order_item_id,
  oi.product_id,
  p.category,
  oi.quantity,
  oi.unit_price,
  oi.discount_pct,
  round(oi.quantity * oi.unit_price * (1 - oi.discount_pct), 2) as line_total
from {{ ref('stg_order_items') }} oi
join {{ ref('stg_orders') }} o using (order_id)
join {{ ref('stg_products') }} p using (product_id)
