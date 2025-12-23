{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='loan_id',
    on_schema_change='append_new_columns',
    file_format='delta'
) }}

select
  {{ dbt_utils.generate_surrogate_key(['loan_id']) }} as dim_loan_sk,
  loan_id,
  product,
  funded_date,
  loan_purpose,
  property_type,
  occupancy
from {{ ref('stg_dim_loan') }}