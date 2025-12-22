
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['loan_id','funded_date'],     -- composite grain to avoid duplicates
    on_schema_change='append_new_columns',
    file_format='delta'
) }}

select
  -- Surrogate keys from dimensions
  dl.dim_loan_sk,
  dlead.dim_lead_sk,
  dassoc.dim_associate_sk,
  -- Natural keys + measures
  f.fact_loan_id,
  f.loan_id,
  f.lead_id,
  f.associate_id,
  f.funded_date,
  f.amount,
  f.interest_rate
from {{ ref('stg_fact_loan') }} f
left join {{ ref('dim_loan') }}       dl     on f.loan_id       = dl.loan_id
left join {{ ref('dim_lead') }}       dlead  on f.lead_id       = dlead.lead_id
left join {{ ref('dim_associate') }}  dassoc on f.associate_id  = dassoc.associate_id

{% if is_incremental() %}
  -- Watermark: only process rows newer than what's already in the target
  where f.funded_date > (
    select coalesce(max(f.funded_date), date('1900-01-01'))
    from {{ this }} f
  )
{% endif %}
