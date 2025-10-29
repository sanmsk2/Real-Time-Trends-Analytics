{{ config(materialized='table') }}

with src as (
    select *
    from {{ source('rss', 'rss_staging') }}
),

flattened as (
    select
        id,
        rss_entry:link::string       as link,
        coalesce(try_to_timestamp_tz(rss_entry:"published"::string), load_timestamp) as published_at,
        rss_entry:source::string     as source,
        rss_entry:summary::string    as summary_html,
        rss_entry:title::string      as title,
        load_timestamp
    from src
)

select * from flattened
