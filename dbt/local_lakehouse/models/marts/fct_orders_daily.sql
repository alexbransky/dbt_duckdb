select
  order_date as day,
  count(*) as orders,
  round(sum(order_total), 2) as revenue
from {{ ref('fct_orders') }}
where order_status = 'completed'
group by 1
