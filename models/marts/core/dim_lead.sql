
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='lead_id',
    on_schema_change='append_new_columns',
    file_format='delta'
) }}

select
  {{ dbt_utils.generate_surrogate_key(['lead_id']) }} as dim_lead_sk,
  lead_id,
  lead_source_id,
  associate_id,
  created_date,
  status,
  state
from {{ ref('stg_dim_lead') }}