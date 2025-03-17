
{{
    config(
        enabled=true,
        severity='warn',
        tags = ['ai_eval'],
        store_failures = true
    )
}}

with data as ( select * from {{ ref('movie_review_sentiment_analysis_eval') }} )

select *
from   data 
where  eval_score <= '{{ var("eval_accuracy_threshold") }}'
