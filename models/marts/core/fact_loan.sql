select
  fact_loan_id,
  loan_id,
  lead_id,         -- relationship to DimLead
  associate_id,    -- relationship to DimAssociate
  funded_date,
  amount,
  interest_rate
from {{ ref('stg_fact_loan') }}