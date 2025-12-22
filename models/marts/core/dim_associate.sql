
select
  {{ dbt_utils.generate_surrogate_key(['associate_id','dbt_valid_from']) }} as dim_associate_sk
  , associate_id
  , associate_name
  , role
  , region
  , hire_date
  , dbt_valid_from
  , dbt_valid_to
  , case when dbt_valid_to is null then true else false end as is_current
from {{ ref('associate_scd2') }}