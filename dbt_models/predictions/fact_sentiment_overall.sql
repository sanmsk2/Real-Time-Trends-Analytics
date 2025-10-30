{{ config(
    materialized='incremental',
    unique_key='date_source'
    ) 
}}

WITH article_sentiment AS (
    SELECT
        DATE(PUBLISHED_AT) AS date,
        SOURCE_ID,
        CASE
            WHEN "title_sentiment_label" IS NOT NULL THEN "title_sentiment_label"
            ELSE "summary_sentiment_label"
        END AS overall_sentiment_label
    FROM {{ source('rss', 'fact_rss_sentiment') }}
),

daily_overall_sentiment AS (
    SELECT
        date,
        SOURCE_ID,
        SUM(CASE WHEN overall_sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_count,
        SUM(CASE WHEN overall_sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
        SUM(CASE WHEN overall_sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_count,
        COUNT(*) AS total_count,
        ROUND(
            100.0 * SUM(CASE WHEN overall_sentiment_label = 'positive' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
            2
        ) AS positive_pct,
        ROUND(
            100.0 * SUM(CASE WHEN overall_sentiment_label = 'neutral' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
            2
        ) AS neutral_pct,
        ROUND(
            100.0 * SUM(CASE WHEN overall_sentiment_label = 'negative' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
            2
        ) AS negative_pct,
        CONCAT(TO_VARCHAR(date), '_', SOURCE_ID) AS date_source
    FROM article_sentiment
    GROUP BY date, SOURCE_ID
)

SELECT * FROM daily_overall_sentiment

{% if is_incremental() %}
WHERE date_source > (
    SELECT COALESCE(MAX(date_source), '1900-01-01_0') 
    FROM {{ this }}
)
{% endif %}
