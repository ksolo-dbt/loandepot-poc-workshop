{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='lead_source_id',
    on_schema_change='append_new_columns',
    file_format='delta'
) }}

select
  {{ dbt_utils.generate_surrogate_key(['lead_source_id']) }} as dim_lead_source_sk,
  lead_source_id,
  source_name,
  channel,
  cost_bucket,
  category
from {{ ref('stg_dim_lead_source') }}