select
  l.lead_id,
  l.created_date,
  l.status,
  l.state,
  l.lead_source_id,
  l.associate_id
from {{ ref('stg_dim_lead') }} l
