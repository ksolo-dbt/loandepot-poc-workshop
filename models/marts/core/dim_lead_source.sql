select
  lead_source_id,
  source_name,
  channel,
  cost_bucket,
  category
from {{ ref('stg_dim_lead_source') }