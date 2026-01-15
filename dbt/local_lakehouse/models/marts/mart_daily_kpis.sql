{{ config(materialized='table') }}

with orders as (
  select * from {{ ref('fct_orders_daily') }}
),
pageviews as (
  select * from {{ ref('fct_pageviews_daily') }}
),
all_days as (
  select day from orders
  union
  select day from pageviews
)

select
  d.day,
  coalesce(o.orders, 0) as orders,
  coalesce(o.revenue, 0.0) as revenue,
  coalesce(p.pageviews, 0) as pageviews,
  coalesce(p.known_visitors, 0) as known_visitors
from all_days d
left join orders o using (day)
left join pageviews p using (day)
order by d.day
