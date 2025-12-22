select
  fact_lead_lifecycle_id,
  lead_id,
  lifecycle_stage,
  associate_id,
  stage_date,
  lead_source_id
from {{ ref('stg_fact_lead_lifecycle') }}
