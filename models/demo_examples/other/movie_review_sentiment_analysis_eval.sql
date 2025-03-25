with source as (

    select * from {{ ref('movie_review_sentiment_analysis') }}

),

rename as (

    select
    
        movie_review_id,
        movie_id,
        review_time,
        sentiment_score,
        actual_sentiment,
        '{{ var("eval_prompt") }}' as prompt,
        SNOWFLAKE.CORTEX.COMPLETE('snowflake-llama-3.3-70b', prompt) as eval_score
    from source

)

select * from rename