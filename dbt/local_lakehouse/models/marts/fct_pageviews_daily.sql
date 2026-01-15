{{ config(materialized='incremental', unique_key='day', incremental_strategy='delete+insert') }}

with base as (
  select
    event_date as day,
    count(*) as pageviews,
    count(distinct customer_id) as known_visitors
  from {{ ref('stg_pageviews') }}
  {% if is_incremental() %}
    where event_date >= (
      select coalesce(max(day), date '1970-01-01') from {{ this }}
    ) - interval 2 day
  {% endif %}
  group by 1
)

select * from base
