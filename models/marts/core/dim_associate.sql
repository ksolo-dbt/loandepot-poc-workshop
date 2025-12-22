select
  associate_id,
  associate_name,
  role,
  region,
  hire_date
from {{ ref('stg_dim_associate') }}
