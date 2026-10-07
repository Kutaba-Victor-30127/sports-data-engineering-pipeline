select distinct
    to_char(event_time::date, 'YYYYMMDD')::integer as date_id,
    event_time::date as full_date,
    extract(year from event_time)::integer as year,
    extract(month from event_time)::integer as month,
    extract(day from event_time)::integer as day
from {{ ref('stg_events') }}