{{ config(materialized='table') }}

with base as (
    select * from {{ ref('rss_base') }}
),

deduped as (
    select
        *,
        row_number() over (partition by link order by load_timestamp desc) as rn
    from base
),

cleaned as (
    select
        id,                        -- original id
        md5(link) as clean_id,      -- unique surrogate key
        link,
        published_at,
        source,
        regexp_replace(summary_html, '<[^>]+>', '') as summary,
        title,
        load_timestamp
    from deduped
    where rn = 1
)

select * from cleaned
