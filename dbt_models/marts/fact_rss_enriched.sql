{{ 
  config(
    materialized='incremental',
    unique_key='event_id'
  ) 
}}

-- Step 1: Base data from fact_rss_events
with base as (
    select
        f.event_id,
        f.title,
        f.summary,
        f.source_id,
        f.date_key,
        f.published_at,
        f.load_timestamp,

        -- Derived Features
        array_size(split(f.title, ' ')) as title_word_count,
        array_size(split(f.summary, ' ')) as summary_word_count,
        (array_size(split(f.title, ' ')) + array_size(split(f.summary, ' '))) / 100.0 as engagement_score,
        hour(f.published_at) as published_hour

    from {{ ref('fact_rss_events') }} f
)

-- Step 2: Incremental logic
select * from base

{% if is_incremental() %}
  where event_id not in (select event_id from {{ this }})
{% endif %}
