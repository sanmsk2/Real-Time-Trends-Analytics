{{ config(
    materialized='incremental',
    unique_key='date'
) }}

WITH sentiment_data AS (
    SELECT
        DATE(published_at) AS date,
        "title_sentiment_label" AS sentiment_label,
        COUNT(*) AS sentiment_count
    FROM {{ source('rss', 'fact_rss_sentiment') }}
    GROUP BY DATE(published_at), "title_sentiment_label"
),

daily_sentiment AS (
    SELECT
        date,
        SUM(CASE WHEN sentiment_label = 'positive' THEN sentiment_count ELSE 0 END) AS positive_count,
        SUM(CASE WHEN sentiment_label = 'neutral' THEN sentiment_count ELSE 0 END) AS neutral_count,
        SUM(CASE WHEN sentiment_label = 'negative' THEN sentiment_count ELSE 0 END) AS negative_count,
        SUM(sentiment_count) AS total_count,
        ROUND(
            100.0 * SUM(CASE WHEN sentiment_label = 'positive' THEN sentiment_count ELSE 0 END) / NULLIF(SUM(sentiment_count), 0),
            2
        ) AS positive_pct,
        ROUND(
            100.0 * SUM(CASE WHEN sentiment_label = 'neutral' THEN sentiment_count ELSE 0 END) / NULLIF(SUM(sentiment_count), 0),
            2
        ) AS neutral_pct,
        ROUND(
            100.0 * SUM(CASE WHEN sentiment_label = 'negative' THEN sentiment_count ELSE 0 END) / NULLIF(SUM(sentiment_count), 0),
            2
        ) AS negative_pct
    FROM sentiment_data
    GROUP BY date
)

SELECT * FROM daily_sentiment

{% if is_incremental() %}
WHERE date > (SELECT COALESCE(MAX(date), '1900-01-01') FROM {{ this }})
{% endif %}
