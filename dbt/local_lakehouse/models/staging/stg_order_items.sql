select
  cast(order_item_id as integer) as order_item_id,
  cast(order_id as integer) as order_id,
  cast(product_id as integer) as product_id,
  cast(quantity as integer) as quantity,
  cast(unit_price as double) as unit_price,
  cast(discount_pct as double) as discount_pct
from {{ ref('bronze_order_items') }}
