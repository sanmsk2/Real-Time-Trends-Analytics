{{ config(
    materialized='incremental',
    unique_key='date_source'
    )
}}

WITH sentiment_data AS (
    SELECT
        DATE(PUBLISHED_AT) AS date,
        SOURCE_ID,
        "title_sentiment_label" AS sentiment_label,
        COUNT(*) AS sentiment_count
    FROM {{ source('rss', 'fact_rss_sentiment') }}
    GROUP BY DATE(PUBLISHED_AT), SOURCE_ID, "title_sentiment_label"
),

daily_source_sentiment AS (
    SELECT
        date,
        SOURCE_ID,
        SUM(CASE WHEN sentiment_label = 'positive' THEN sentiment_count ELSE 0 END) AS positive_count,
        SUM(CASE WHEN sentiment_label = 'neutral' THEN sentiment_count ELSE 0 END) AS neutral_count,
        SUM(CASE WHEN sentiment_label = 'negative' THEN sentiment_count ELSE 0 END) AS negative_count,
        SUM(sentiment_count) AS total_count,
        ROUND(
            100.0 * SUM(CASE WHEN sentiment_label = 'positive' THEN sentiment_count ELSE 0 END) / NULLIF(SUM(sentiment_count),0),
            2
        ) AS positive_pct,
        ROUND(
            100.0 * SUM(CASE WHEN sentiment_label = 'neutral' THEN sentiment_count ELSE 0 END) / NULLIF(SUM(sentiment_count),0),
            2
        ) AS neutral_pct,
        ROUND(
            100.0 * SUM(CASE WHEN sentiment_label = 'negative' THEN sentiment_count ELSE 0 END) / NULLIF(SUM(sentiment_count),0),
            2
        ) AS negative_pct,
        CONCAT(TO_VARCHAR(date), '_', SOURCE_ID) AS date_source
    FROM sentiment_data
    GROUP BY date, SOURCE_ID
)

SELECT * FROM daily_source_sentiment

{% if is_incremental() %}
WHERE date_source > (
    SELECT COALESCE(MAX(date_source), '1900-01-01_0') 
    FROM {{ this }}
)
{% endif %}
