select
    -- Surrogate keys from dimensions
    dl.dim_loan_sk,
    dlead.dim_lead_sk,
    dassoc.dim_associate_sk,
    -- Fact natural keys and measures
    f.fact_loan_id,
    f.loan_id,
    f.lead_id,
    f.associate_id,
    f.funded_date,
    f.amount,
    f.interest_rate
from {{ ref('stg_fact_loan') }} f
left join {{ ref('dim_loan') }} dl
    on f.loan_id = dl.loan_id
left join {{ ref('dim_lead') }} dlead
    on f.lead_id = dlead.lead_id
left join {{ ref('dim_associate') }} dassoc
    on f.associate_id = dassoc.associate_id