select
  cast(event_id as bigint) as event_id,
  cast(event_ts_utc as timestamp) as event_ts_utc,
  cast(event_ts_utc as date) as event_date,
  cast(customer_id as integer) as customer_id,
  url_path,
  referrer,
  user_agent,
  utm_source,
  utm_medium
from {{ ref('bronze_pageviews') }}
