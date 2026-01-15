select
  cast(product_id as integer) as product_id,
  product_name,
  category,
  cast(price as double) as price,
  cast(is_active as boolean) as is_active
from {{ ref('bronze_products') }}
