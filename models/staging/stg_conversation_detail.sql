with source as (
    select * from {{ ref('seed_json_simple') }}
)

select
    id,
    json_payload:user_id::int as user_id,
    json_payload:action::string as action,
    json_payload:timestamp::string as timestamp
from source