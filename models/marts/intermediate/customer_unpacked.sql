with source_data as (
    select * from {{ ref('customer_struct') }}
),

final as (
    select
        customer_key,
        -- Accessing struct fields using dot notation
        customer_metrics.lifetime_value,
        customer_metrics.tier_name,
        
        -- You can also perform calculations on these fields
        (customer_metrics.lifetime_value * 0.10) as estimated_tax
    from source_data
)

select * from final