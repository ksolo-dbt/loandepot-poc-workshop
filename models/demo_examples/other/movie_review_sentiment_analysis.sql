with source as (

    select * from {{ ref('stg_movie_reviews') }}

),

rename as (

    select
    
        movie_review_id,
        movie_id,
        review_time,
        review_txt,
        SNOWFLAKE.CORTEX.SENTIMENT(review_txt) as sentiment_score,
        actual_sentiment

    from source

)

select * from rename