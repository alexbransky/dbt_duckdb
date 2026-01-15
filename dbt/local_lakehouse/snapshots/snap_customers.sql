{% snapshot snap_customers %}
{{
  config(
    unique_key='customer_id',
    strategy='check',
    check_cols=['email', 'status', 'segment', 'state']
  )
}}

select
  customer_id,
  email,
  status,
  segment,
  state
from {{ ref('stg_customers') }}

{% endsnapshot %}
