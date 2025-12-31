with source as (
    select * from {{ ref('seed_json') }}
)

select
    payload:user_id::int as user_id,
    payload:action::string as action,
    payload:timestamp::string as timestamp,
    payload_nested:companyName::string as company_name,
    payload_nested:location::string as location,
    payload_nested:departments::string as department_detail,
    payload_nested:isActive::string as is_active

from source