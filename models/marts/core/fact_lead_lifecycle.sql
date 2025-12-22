{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['lead_id','lifecycle_stage','stage_date'],   -- event-grain upsert
    on_schema_change='append_new_columns',
    file_format='delta'
) }}
select
  -- Surrogate keys from dimensions
  dlead.dim_lead_sk,
  dassoc.dim_associate_sk,
  dsrc.dim_lead_source_sk,
  -- Natural keys + attributes
  f.fact_lead_lifecycle_id,
  f.lead_id,
  f.lifecycle_stage,
  f.associate_id,
  f.stage_date,
  f.lead_source_id
from {{ ref('stg_fact_lead_lifecycle') }} f
left join {{ ref('dim_lead') }}        dlead  on f.lead_id        = dlead.lead_id
left join {{ ref('dim_associate') }}   dassoc on f.associate_id   = dassoc.associate_id
left join {{ ref('dim_lead_source') }} dsrc   on f.lead_source_id = dsrc.lead_source_id
{% if is_incremental() %}
  -- Watermark: reprocess only data newer than what exists
  where f.stage_date > (
    select coalesce(max(f.stage_date), date('1900-01-01'))
    from {{ this }} f
  )
{% endif %}