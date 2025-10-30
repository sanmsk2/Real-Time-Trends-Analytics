{{ 
    config(
    materialized='incremental',
    unique_key='event_id'
    ) 
}}

with base as (

    select
        {{ dbt_utils.surrogate_key(['link', 'published_at']) }} as event_id,
        c.link,
        c.title,
        c.summary,
        c.source,
        c.published_at,
        c.load_timestamp,

        -- Word counts (count spaces + 1)
        case when c.title is not null then regexp_count(c.title, '\\s+') + 1 else 0 end as title_word_count,
        case when c.summary is not null then regexp_count(c.summary, '\\s+') + 1 else 0 end as summary_word_count

    from {{ ref('rss_clean') }} c
)

select
    b.event_id,
    s.source_id,
    d.date_key,
    b.link,
    b.title,
    b.summary,
    b.published_at,
    b.load_timestamp,
    b.title_word_count,
    b.summary_word_count
from base b
left join {{ ref('dim_rss_source') }} s
    on b.source = s.source_name
left join {{ ref('dim_rss_date') }} d
    on to_date(b.published_at) = d.full_date
