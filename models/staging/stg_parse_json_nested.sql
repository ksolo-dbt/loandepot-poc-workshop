with source as (
    select * from {{ ref('seed_json_nested') }}
)

select
    payload:companyName::string as company_name,
    payload:location::string as location,
    payload:departments::string as department_detail,
    payload:isActive::string as is_active
from source