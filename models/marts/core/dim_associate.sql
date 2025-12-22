select
  {{ dbt_utils.generate_surrogate_key(['associate_id']) }} as dim_associate_sk,
  associate_id,
  associate_name,
  role,
  region,
  hire_date
from {{ ref('stg_dim_associate') }}
