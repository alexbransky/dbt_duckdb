{{ config(materialized='table') }}
select *
from read_parquet('{{ var("raw_dir") }}/customers.parquet')
