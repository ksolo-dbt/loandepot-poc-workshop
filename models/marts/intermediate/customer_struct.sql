{{
    config(
        materialized='table'
    )
}}

with customer as (
    select * from {{ ref('stg_tpch_customers') }}
),

orders as (
    select * from {{ ref('stg_tpch_orders') }}
),

final as (
    select
        customer.customer_key,
        -- We create the struct here
        struct(
            sum(orders.total_price) as lifetime_value,
            case 
                when sum(orders.total_price) <= 200000 then 'tier1'
                when sum(orders.total_price) > 2000000 then 'tier2'
                when sum(orders.total_price) between 1000000 and 1999999 then 'tier3'
                else 'tier4' 
            end as tier_name
        ) as customer_metrics
    from customer
    inner join orders
        on customer.customer_key = orders.customer_key
    group by 1
)

select * from final