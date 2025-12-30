with source as (
    select * from {{ ref('raw_app_data') }}
)

select
    id,
    json_payload:user_id::int as user_id,
    json_payload:status::string as status,
    json_payload:meta.tier::string as tier
from source