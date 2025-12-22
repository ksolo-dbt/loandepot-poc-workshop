select
  l.lead_id,
  l.created_date,
  l.status,
  l.state
from {{ ref('stg_dim_lead') }} l
