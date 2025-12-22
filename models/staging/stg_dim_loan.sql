select
  loan_id,
  product,
  funded_date,
  loan_purpose,
  property_type,
  occupancy
from {{ ref('seed_dim_loan') }}