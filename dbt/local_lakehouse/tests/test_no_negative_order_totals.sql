select *
from {{ ref('fct_orders') }}
where order_total < 0
