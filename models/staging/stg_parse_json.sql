with source as (
    select * from {{ ref('seed_json') }}
)

select
    payload_nested:companyName::string as company_name,
    payload_nested:location::string as location,
    payload_nested:departments::string as department_detail,
    payload_nested:isActive::string as is_active

from source