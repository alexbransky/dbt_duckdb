select
  cast(customer_id as integer) as customer_id,
  first_name,
  last_name,
  email,
  status,
  segment,
  state,
  cast(created_at as date) as created_at,
  {{ surrogate_key(["customer_id", "email"]) }} as customer_sk
from {{ ref('bronze_customers') }}
